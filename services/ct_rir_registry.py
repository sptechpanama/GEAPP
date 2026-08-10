from __future__ import annotations

"""Normalizacion del registro persistente de fichas vigiladas por CT_RIR.

La hoja historica tenia tres columnas (ficha, usuario y fecha). El formato
actual agrega ``Nombre ficha`` sin dejar de ser compatible con los scrapers,
que siguen leyendo el numero desde la primera columna.
"""

import re
import unicodedata
from collections.abc import Mapping, Sequence


REGISTRY_HEADERS = ["Ficha #", "Nombre ficha", "Actualizado por", "Actualizado"]


def normalize_ficha_code(value: object) -> str:
    text = str(value or "").strip()
    match = re.search(r"\d{3,8}", text)
    if not match:
        return ""
    return match.group(0).lstrip("0") or "0"


def _header_key(value: object) -> str:
    text = "".join(
        character
        for character in unicodedata.normalize("NFD", str(value or "").lower())
        if unicodedata.category(character) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def parse_registry_values(values: Sequence[Sequence[object]]) -> list[dict[str, str]]:
    """Lee tanto el esquema nuevo como el legado sin confundir usuario con nombre."""

    rows = [list(row) for row in values if row]
    if not rows:
        return []

    header = [_header_key(value) for value in rows[0]]
    has_header = bool(header and header[0] in {"ficha", "ficha #", "numero ficha"})
    data_rows = rows[1:] if has_header else rows

    name_index = -1
    user_index = -1
    updated_index = -1
    if has_header:
        for index, key in enumerate(header):
            if key in {"nombre ficha", "nombre generico", "nombre"}:
                name_index = index
            elif key in {"actualizado por", "usuario", "user"}:
                user_index = index
            elif key in {"actualizado", "fecha actualizacion", "updated at"}:
                updated_index = index

    records: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in data_rows:
        code = normalize_ficha_code(row[0] if row else "")
        if not code or code in seen:
            continue
        seen.add(code)

        def at(index: int) -> str:
            return str(row[index] or "").strip() if 0 <= index < len(row) else ""

        records.append(
            {
                "ficha": code,
                "nombre": at(name_index),
                "actualizado_por": at(user_index),
                "actualizado": at(updated_index),
            }
        )
    return records


def enrich_registry_names(
    records: Sequence[Mapping[str, object]],
    name_lookup: Mapping[str, object] | None = None,
) -> list[dict[str, str]]:
    lookup = {
        normalize_ficha_code(code): str(name or "").strip()
        for code, name in (name_lookup or {}).items()
        if normalize_ficha_code(code)
    }
    output: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in records:
        code = normalize_ficha_code(raw.get("ficha", ""))
        if not code or code in seen:
            continue
        seen.add(code)
        saved_name = str(raw.get("nombre", "") or "").strip()
        output.append(
            {
                "ficha": code,
                "nombre": lookup.get(code) or saved_name or f"Ficha tecnica {code}",
                "actualizado_por": str(raw.get("actualizado_por", "") or "").strip(),
                "actualizado": str(raw.get("actualizado", "") or "").strip(),
            }
        )
    return output


def merge_registry_tokens(
    records: Sequence[Mapping[str, object]],
    tokens: Sequence[object],
    *,
    remove: bool = False,
    name_lookup: Mapping[str, object] | None = None,
) -> list[dict[str, str]]:
    current = enrich_registry_names(records, name_lookup)
    targets = {normalize_ficha_code(token) for token in tokens}
    targets.discard("")
    if remove:
        return [record for record in current if record["ficha"] not in targets]

    existing = {record["ficha"] for record in current}
    lookup = name_lookup or {}
    for code in (normalize_ficha_code(token) for token in tokens):
        if not code or code in existing:
            continue
        existing.add(code)
        current.append(
            {
                "ficha": code,
                "nombre": str(lookup.get(code, "") or "").strip() or f"Ficha tecnica {code}",
                "actualizado_por": "",
                "actualizado": "",
            }
        )
    return current


def registry_sheet_values(
    records: Sequence[Mapping[str, object]],
    *,
    updated_by: str,
    updated_at: str,
) -> list[list[str]]:
    rows = [REGISTRY_HEADERS.copy()]
    for record in enrich_registry_names(records):
        rows.append(
            [
                record["ficha"],
                record["nombre"],
                str(updated_by or "").strip(),
                str(updated_at or "").strip(),
            ]
        )
    return rows
