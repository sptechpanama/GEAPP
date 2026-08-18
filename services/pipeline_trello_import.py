from __future__ import annotations

"""Importador de una sola vez para exportaciones JSON reales de Trello."""

from dataclasses import dataclass
import json
from typing import Any, Mapping, Sequence

from services.pipeline_estrategico import (
    PipelineError,
    PipelineRepository,
    ROUTES,
    clean_text,
    normalize_key,
)


class TrelloImportError(PipelineError):
    pass


@dataclass(frozen=True)
class TrelloPreview:
    board_name: str
    total_cards: int
    eligible_cards: int
    archived_cards: int
    skipped_cards: int
    routes: dict[str, int]
    warnings: tuple[str, ...]


def load_trello_export(raw: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        payload = dict(raw)
    else:
        text_value = raw.decode("utf-8-sig", errors="replace") if isinstance(raw, bytes) else str(raw)
        if text_value.lstrip().lower().startswith(("<!doctype", "<html")):
            raise TrelloImportError(
                "El archivo es una página HTML de Trello, no una exportación JSON del tablero. "
                "En Trello usa Mostrar menú > Más > Imprimir y exportar > Exportar como JSON."
            )
        try:
            payload = json.loads(text_value)
        except json.JSONDecodeError as exc:
            raise TrelloImportError(
                f"El archivo no contiene JSON valido de Trello (linea {exc.lineno})."
            ) from exc
    if not isinstance(payload.get("cards"), list) or not isinstance(payload.get("lists"), list):
        raise TrelloImportError(
            "El JSON no contiene las colecciones 'cards' y 'lists' esperadas en una exportacion de Trello."
        )
    return payload


def _route_from_list(name: Any) -> str:
    normalized = normalize_key(name)
    rules = (
        ("fichas viejas", "fichas_viejas"),
        ("recien creadas", "fichas_recien_creadas"),
        ("homologaciones", "homologaciones_anunciadas"),
        ("solicitudes de creacion", "solicitudes_creacion"),
        ("creacion de fichas desde cero", "creacion_desde_cero"),
        ("desde cero", "creacion_desde_cero"),
    )
    for token, route_key in rules:
        if token in normalized:
            return route_key
    return ""


def _custom_field_values(board: Mapping[str, Any], card: Mapping[str, Any]) -> dict[str, str]:
    fields = {
        clean_text(field.get("id")): field
        for field in board.get("customFields", [])
        if isinstance(field, Mapping) and clean_text(field.get("id"))
    }
    output: dict[str, str] = {}
    for item in card.get("customFieldItems", []) or []:
        if not isinstance(item, Mapping):
            continue
        field = fields.get(clean_text(item.get("idCustomField")))
        if not field:
            continue
        name = clean_text(field.get("name"))
        value = item.get("value") if isinstance(item.get("value"), Mapping) else {}
        resolved = next(
            (
                clean_text(value.get(key))
                for key in ("text", "number", "date", "checked")
                if clean_text(value.get(key))
            ),
            "",
        )
        if not resolved and clean_text(item.get("idValue")):
            for option in field.get("options", []) or []:
                if clean_text(option.get("id")) != clean_text(item.get("idValue")):
                    continue
                option_value = option.get("value") if isinstance(option.get("value"), Mapping) else {}
                resolved = clean_text(option_value.get("text"))
                break
        if name and resolved and name not in output:
            output[name] = resolved
    return output


def _field(values: Mapping[str, str], *aliases: str) -> str:
    normalized = {normalize_key(key): clean_text(value) for key, value in values.items()}
    for alias in aliases:
        value = normalized.get(normalize_key(alias), "")
        if value:
            return value
    return ""


def _card_business_fields(
    board: Mapping[str, Any], card: Mapping[str, Any]
) -> dict[str, str]:
    values = _custom_field_values(board, card)
    combined = _field(values, "Proveedor/marca", "Proveedor marca", "Marca")
    provider = _field(values, "Proveedor")
    brand = combined
    if "/" in combined:
        combined_provider, combined_brand = [
            clean_text(part) for part in combined.split("/", 1)
        ]
        # El campo combinado representa la pareja de negocio y prevalece si
        # quedo un valor viejo o copiado en el campo simple Proveedor.
        provider = combined_provider or provider
        brand = combined_brand
    return {
        "ficha": _field(values, "Ficha Tecnica", "Ficha"),
        "nombre_ficha": _field(values, "Nombre ficha", "Descripcion ficha"),
        "producto": _field(values, "Producto") or clean_text(card.get("name")),
        "proveedor": provider,
        "marca": brand,
        "descripcion": _field(values, "Descripcion") or clean_text(card.get("desc")),
        "email": _field(values, "Correo Electronico", "Correo", "Email"),
        "whatsapp": _field(values, "Whatsapp o Wechat", "Whatsapp", "Wechat"),
    }


def _checklist_lookup(board: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        clean_text(checklist.get("id")): checklist
        for checklist in board.get("checklists", []) or []
        if isinstance(checklist, Mapping) and clean_text(checklist.get("id"))
    }


def _completed_keys(
    board: Mapping[str, Any], card: Mapping[str, Any], route_key: str
) -> list[str]:
    lookup = _checklist_lookup(board)
    completed_labels: set[str] = set()
    for checklist_id in card.get("idChecklists", []) or []:
        checklist = lookup.get(clean_text(checklist_id))
        if not checklist:
            continue
        for item in checklist.get("checkItems", []) or []:
            if clean_text(item.get("state")).lower() == "complete":
                completed_labels.add(normalize_key(item.get("name")))
    template = ROUTES[route_key].checklist
    keys: list[str] = []
    for key, label in template:
        normalized_label = normalize_key(label)
        if normalized_label in completed_labels:
            keys.append(key)
            continue
        # Tolerancia a diferencias menores de singular/plural o CSS/CT.
        if any(
            normalized_label in candidate or candidate in normalized_label
            for candidate in completed_labels
            if len(candidate) >= 12
        ):
            keys.append(key)
            continue
        break  # solo se importa el prefijo secuencial valido
    return keys


def preview_trello_export(board: Mapping[str, Any]) -> TrelloPreview:
    lists = {
        clean_text(item.get("id")): clean_text(item.get("name"))
        for item in board.get("lists", [])
        if isinstance(item, Mapping)
    }
    counts = {key: 0 for key in ROUTES}
    total = len(board.get("cards", []))
    archived = 0
    eligible = 0
    warnings: list[str] = []
    for card in board.get("cards", []):
        if not isinstance(card, Mapping):
            continue
        if bool(card.get("closed")):
            archived += 1
            continue
        route = _route_from_list(lists.get(clean_text(card.get("idList")), ""))
        if not route:
            warnings.append(f"Lista sin equivalencia: {lists.get(clean_text(card.get('idList')), 'desconocida')}")
            continue
        fields = _card_business_fields(board, card)
        if not fields["proveedor"] or not fields["marca"]:
            warnings.append(
                f"{clean_text(card.get('name'))}: falta proveedor o marca"
            )
            continue
        eligible += 1
        counts[route] += 1
    return TrelloPreview(
        board_name=clean_text(board.get("name")) or "Tablero Trello",
        total_cards=total,
        eligible_cards=eligible,
        archived_cards=archived,
        skipped_cards=max(0, total - eligible - archived),
        routes=counts,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def import_trello_board(
    repository: PipelineRepository,
    board: Mapping[str, Any],
    *,
    actor: str,
) -> dict[str, Any]:
    lists = {
        clean_text(item.get("id")): clean_text(item.get("name"))
        for item in board.get("lists", [])
        if isinstance(item, Mapping)
    }
    created = 0
    existing = 0
    skipped: list[str] = []
    for raw_card in board.get("cards", []):
        if not isinstance(raw_card, Mapping) or bool(raw_card.get("closed")):
            continue
        route_key = _route_from_list(lists.get(clean_text(raw_card.get("idList")), ""))
        if not route_key:
            skipped.append(f"{clean_text(raw_card.get('name'))}: lista no reconocida")
            continue
        fields = _card_business_fields(board, raw_card)
        ficha = fields["ficha"]
        producto = fields["producto"]
        proveedor = fields["proveedor"]
        marca = fields["marca"]
        if not proveedor or not marca:
            skipped.append(
                f"{clean_text(raw_card.get('name'))}: falta proveedor o marca"
            )
            continue
        external_id = clean_text(raw_card.get("id"))
        existing_card = repository.card_by_source("trello", external_id)
        if not existing_card:
            existing_card = repository.card_by_identity(
                ficha=ficha,
                producto=producto,
                proveedor=proveedor,
                marca=marca,
            )
        if existing_card:
            card = existing_card
            existing += 1
        else:
            try:
                card = repository.create_card(
                    ficha=ficha,
                    nombre_ficha=fields["nombre_ficha"],
                    producto=producto,
                    proveedor=proveedor,
                    marca=marca,
                    descripcion=fields["descripcion"],
                    route_key=route_key,
                    actor=actor,
                    responsable=clean_text(raw_card.get("idMembers", "")),
                    fecha_objetivo=clean_text(raw_card.get("due"))[:10],
                    source="trello",
                    source_external_id=external_id,
                )
                created += 1
            except PipelineError as exc:
                skipped.append(f"{clean_text(raw_card.get('name'))}: {exc}")
                continue
        repository.apply_imported_checkpoints(
            card["id"],
            _completed_keys(board, raw_card, card["route_key"]),
            actor=actor,
        )
        email = fields["email"]
        whatsapp = fields["whatsapp"]
        if (email or whatsapp) and not repository.contacts(card["id"]):
            repository.add_contact(
                card["id"], actor=actor, email=email, whatsapp_wechat=whatsapp, es_principal=True
            )
        existing_urls = {item.get("file_url", "") for item in repository.documents(card["id"])}
        for attachment in raw_card.get("attachments", []) or []:
            if not isinstance(attachment, Mapping):
                continue
            url = clean_text(attachment.get("url"))
            if not url or url in existing_urls:
                continue
            repository.add_document(
                card["id"],
                actor=actor,
                file_name=clean_text(attachment.get("name")) or "Adjunto de Trello",
                file_url=url,
                document_type="Migrado desde Trello",
                mime_type=clean_text(attachment.get("mimeType")),
                storage_provider="trello",
            )
            existing_urls.add(url)
    return {
        "created": created,
        "existing": existing,
        "skipped": len(skipped),
        "warnings": skipped,
    }


__all__ = [
    "TrelloImportError",
    "TrelloPreview",
    "import_trello_board",
    "load_trello_export",
    "preview_trello_export",
]
