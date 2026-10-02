"""Read-only PanamaCompra capture for the document worker (public HTTP API)."""
from __future__ import annotations

import base64
from datetime import datetime
import json
import re
from urllib.parse import unquote, urlparse

import fitz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from services.anestesia_docs import canonical_hash, file_hash, normalized, now_iso, public_registry_age

API = "https://apisv3.panamacompra.gob.pa"
MAX_PDF = 20 * 1024 * 1024


def route(url):
    parsed = urlparse(str(url).strip())
    if parsed.scheme != "https" or parsed.hostname not in {"www.panamacompra.gob.pa", "panamacompra.gob.pa"}:
        raise ValueError("Usa el enlace HTTPS oficial de PanamáCompra.")
    fragment = unquote(parsed.fragment).rstrip("/")
    if not any(part in fragment for part in ("/solicitud-de-cotizacion/", "/pliego-de-cargos/")):
        raise ValueError("El enlace debe abrir una solicitud de cotización o pliego de cargos.")
    number = re.search(r"\d{4}-\d+(?:-\d+)+-[A-Z]+-\d+", fragment)
    try:
        token = fragment.split("/")[-1][::-1]
        payload = json.loads(base64.b64decode(token + "=" * (-len(token) % 4)))
        flow, kind = int(payload["i"]), int(payload["tp"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("El identificador del enlace no es válido.") from exc
    if not number or flow <= 0 or kind <= 0:
        raise ValueError("Falta el número o identificador válido del acto.")
    return flow, kind, number[0]


def session():
    client = requests.Session()
    client.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "POST"))))
    client.headers.update({"User-Agent": "RIR-Document-Review/1.0", "Referer": "https://www.panamacompra.gob.pa/"})
    return client


def pdf_text(data: bytes):
    with fitz.open(stream=data, filetype="pdf") as doc:
        if doc.is_encrypted or len(doc) > 100:
            raise ValueError("Anexo cifrado o demasiado extenso para lectura automática; requiere revisión.")
        pages = [page.get_text() for page in doc]
        missing = [i + 1 for i, text in enumerate(pages) if len(text.strip()) < 60]
        return "\n".join(f"[Página {i}]\n{text}" for i, text in enumerate(pages, 1)), missing


def portal_delivery_term(source: dict) -> str:
    """Only the portal's delivery period; never the proposal closing date."""
    for key, value in source.get("info", {}).items():
        if normalized(key).rstrip(":") in {"termino de entrega", "plazo de entrega", "tiempo de entrega"}:
            term = " ".join(str(value or "").split())
            if term and normalized(term) not in {"no aplica", "n/a", "por definir", "ver anexo", "segun anexo"}:
                return term
    return ""


def tax_source_evidence(source: dict) -> list[dict]:
    """Keep raw tax observations without deciding the bidder's tax treatment."""
    evidence = []
    for key, value in source.get("info", {}).items():
        if normalized(key) in {"itbms", "impuesto", "impuestos", "tasa de itbms"} and value is not None and str(value).strip():
            evidence.append({"fuente": f"PanamáCompra: {key}", "valor": str(value)})
    for index, item in enumerate(source.get("items", []), 1):
        value = item.get("itbms")
        if value is not None and str(value).strip():
            evidence.append({"fuente": f"PanamáCompra, renglón {item.get('numRenglon', index)}: ITBMS", "valor": str(value)})
    for attachment in source.get("attachments", []):
        snippets = re.findall(r"\b(?:ITBMS|I\.T\.B\.M\.S\.|IMPUESTO)[^\r\n]{0,160}", attachment.get("text", ""), re.I)
        for snippet in snippets[:3]:
            evidence.append({"fuente": attachment.get("name", "Anexo oficial"), "valor": snippet.strip()})
    return evidence


def delivery_destination(source: dict) -> tuple[str, str]:
    """Use an explicit delivery field or its labelled annex cell, not buyer address."""
    for key, value in source.get("info", {}).items():
        if normalized(key).rstrip(":") in {"lugar de entrega", "direccion de entrega", "sitio de entrega"}:
            place = " ".join(str(value or "").split())
            if place and normalized(place) not in {"no aplica", "n/a", "por definir", "ver anexo", "segun anexo"}:
                return place, f"PanamáCompra: {key}"
    # OCR sometimes separates/reorders the label: DE / LUGAR / ENTREGA.
    label = re.compile(r"(?:lugar\s+(?:de\s+)?entrega|de\s+lugar\s+entrega)\s*[:\-]?", re.I)
    location = re.compile(r"\b(?:almacen|alm\.|hospital|policlinica|bodega|deposito|calle|avenida|ciudad|centro\s+de\s+salud)\b")
    stop = re.compile(r"^(?:requisitos|documentos|observacion|nota\b|ctni\b|forma\s+de\s+pago)")
    noise = re.compile(r"^(?:vigencia|presentacion|cantidad|dias|vencimiento|unidad|no aplica|tiempo|termino|forma|global|credito|total|parcial|\d)")
    for attachment in source.get("attachments", []):
        text = attachment.get("text", "")
        for match in label.finditer(text):
            lines = [" ".join(line.split()) for line in text[match.end():match.end() + 700].splitlines() if line.strip()]
            for index, line in enumerate(lines[:16]):
                norm = normalized(line)
                if stop.match(norm):
                    break
                if noise.match(norm) or label.fullmatch(line) or len(line) < 4:
                    continue
                # A value directly beside the label can be a short locality;
                # interleaved table columns require an identifiable destination.
                if location.search(norm) or (index == 0 and not line.endswith(":")):
                    parts = [line]
                    for continuation in lines[index + 1:index + 4]:
                        candidate = normalized(continuation)
                        if stop.match(candidate) or noise.match(candidate) or label.fullmatch(continuation):
                            break
                        if location.search(candidate) or re.match(r"^(?:piso|planta|edificio|local)\b", candidate):
                            parts.append(continuation)
                        else:
                            break
                    place = " ".join(parts)
                    place = re.split(r"\b(?:TIEMPO|T[ÉE]RMINO|FORMA)\s+DE\s+ENTREGA\s*:", place, maxsplit=1, flags=re.I)[0].strip()
                    return place, f"{attachment.get('name', 'Anexo oficial')}: lugar de entrega"
    return "", "No se identificó un lugar de entrega explícito en el acto o sus anexos."


def explicit_ficha_evidence(text):
    """Read explicit labels and the known CSS table layout after column-wise OCR."""
    plain = normalized(text)
    evidence = [{"ficha": m[1], "metodo": "Etiqueta explícita", "texto": m[0]}
        for m in re.finditer(r"(?:ctni|ficha\s*tecnica)\s*[:#.]?\s*(\d{4,7})\b", plain)]
    # OCR can read the value column (CTNI, award type, payment), then the labels.
    # Require the whole labelled table, not a bare code or a product name.
    pattern = (r"(?m)^\s*(\d{4,7})\s*\n\s*(?:global|parcial|por renglon)\s*\n"
               r"\s*(?:credito|contado)\s*\n(?P<delivery>[\s\S]{0,500}?)"
               r"\bctni\s*:\s*forma de adjudicacion\s*:\s*forma de pago\s*:")
    lines = '\n'.join(normalized(line) for line in str(text).splitlines() if line.strip())
    for m in re.finditer(pattern, lines):
        if all(label in m['delivery'] for label in ('tiempo de entrega', 'lugar de entrega', 'vigencia')):
            evidence.append({"ficha": m[1], "metodo": "Tabla CSS CTNI/adjudicación/pago con columnas OCR", "texto": m[0]})
    return evidence


def public_open_status(url, *, client=None):
    """Exact act AND flow in the official active listing; absence is inconclusive."""
    flow, kind, number = route(url)
    state = 8 if kind == 2 else 36
    response = (client or session()).post(API + '/busqueda/proceso-lista-publico',
        json={'registrosPorPagina': 50, 'valorSiguiente': '', 'filtro': {
            'idEstado': state, 'idTipoProceso': kind, 'numProceso': number, 'idProvincia': 0}}, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    if (not isinstance(payload, dict) or payload.get('status') != 1
            or not isinstance(payload.get('result'), dict)
            or not isinstance(payload['result'].get('registros'), list)):
        raise ValueError('No se pudo verificar el estado oficial del acto.')
    matched = [r for r in payload['result']['registros'] if isinstance(r, dict) and r.get('numProceso') == number
        and str(r.get('idProcesosContratacionFlujos')) == str(flow)
        and str(r.get('idTipoProceso')) == str(kind) and str(r.get('idEstado')) == str(state)]
    return {'number': number, 'flow': flow, 'process_type': kind, 'state_id': state if matched else None,
            'state': matched[0].get('nombreRealizado', '') if matched else '', 'checked_at': now_iso()}


def capture(url, storage, source_folder, *, client=None):
    flow, kind, number = route(url)
    client = client or session()
    response = client.get(f"{API}/procesos-configuracion/pagina-componentes-publico/{kind}/procesoVistaPliego/{flow}", timeout=(10, 40))
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != 1 or not isinstance(payload.get("result"), dict):
        raise ValueError("PanamáCompra no devolvió un detalle completo. Reintenta; se conserva el expediente anterior.")
    components = payload["result"].get("pageComponentes") or []
    info, items, attachments = {}, [], []
    for component in components:
        rows = component.get("value")
        if not isinstance(rows, list):
            continue
        if component.get("tipo", "").startswith("componentInfo"):
            for field in rows:
                if isinstance(field, dict) and "nombre" in field:
                    info[normalized(field["nombre"])] = str(field.get("value") or "")
        elif component.get("tipo") == "componentItems":
            items.extend(rows)
        elif component.get("tipo") == "componentFiles":
            attachments.extend(rows)
    reported = info.get("numero de proceso", "")
    if reported != number:
        raise ValueError("El número del enlace no coincide con el acto devuelto por PanamáCompra.")
    source = {"url": url, "number": number, "flow": flow, "process_type": kind, "captured_at": now_iso(),
              "title": info.get("titulo", ""), "entity": info.get("entidad", ""),
              "purchase_unit": info.get("unidad de compra", ""), "info": info,
              "items": items, "attachments": [], "blocking_errors": []}
    publication = re.search(r"\d{2}-\d{2}-\d{4}", info.get("fecha de publicacion", ""))
    source["publication"] = datetime.strptime(publication[0], "%d-%m-%Y").date().isoformat() if publication else ""
    source["closing"] = info.get("fecha y hora presentacion de cotizaciones", "") or info.get("fecha y hora de presentacion de propuestas", "")
    texts = [json.dumps(info, ensure_ascii=False), json.dumps(items, ensure_ascii=False)]
    ocr_folder = None
    for item in attachments:
        path = str(item.get("rutaCompleta", ""))
        if not path.startswith("/procesos-contratacion-archivos/") or ".." in path:
            source["blocking_errors"].append("Existe un adjunto sin ruta pública reconocida; revisar manualmente.")
            continue
        file_url = API + path
        with client.get(file_url, timeout=(10, 60), stream=True) as download:
            download.raise_for_status()
            data = bytearray()
            for chunk in download.iter_content(128 * 1024):
                data.extend(chunk)
                if len(data) > MAX_PDF:
                    raise ValueError("Un anexo supera 20 MB. Revisar su captura antes de generar.")
        data = bytes(data)
        name = str(item.get("nombreOriginal") or f"anexo_{item.get('id')}.pdf")
        if not data.startswith(b"%PDF"):
            source["blocking_errors"].append(f"Adjunto no PDF que debe interpretarse: {name}.")
            continue
        saved = storage.put(source_folder, name, data, "application/pdf")
        text, missing = pdf_text(data)
        if missing:
            try:
                ocr_folder = ocr_folder or storage.folder("Lectura OCR de anexos", source_folder)
                ocr = storage.convert_document(data, name, ocr_folder, pdf_input=True).decode("utf-8")
                if len(ocr.strip()) < 150:
                    raise ValueError("OCR insuficiente")
                text += "\n[OCR: verificar páginas visualmente en el original]\n" + ocr
            except Exception:
                source["blocking_errors"].append(f"No se pudieron interpretar las páginas {missing} de {name}. Revisar captura/OCR.")
        source["attachments"].append({**saved, "official_url": file_url, "official_id": item.get("id"),
                                      "text": text[:120000], "ocr_pages": missing})
        texts.append(text)
    if not attachments or len(source["attachments"]) != len(attachments):
        source["blocking_errors"].append("La captura de anexos oficiales está incompleta.")
    full_text = "\n".join(texts)
    source['ficha_evidence'] = [dict(e, origen=label) for label, text in
        [('Detalle del portal', texts[0] + '\n' + texts[1]),
         *((a['name'], a['text']) for a in source['attachments'])] for e in explicit_ficha_evidence(text)]
    source["explicit_fichas"] = sorted({e['ficha'] for e in source['ficha_evidence']})
    maximum, evidence = public_registry_age(full_text)
    source.update(registry_max_months=maximum, registry_rule=evidence)
    place, evidence = delivery_destination(source)
    source.update(delivery_place=place, delivery_place_evidence=evidence)
    source["fingerprint"] = canonical_hash({"components": components,
        "documents": [{"id": d["official_id"], "sha256": d["sha256"]} for d in source["attachments"]]})
    try:
        source['official_status'] = public_open_status(url, client=client)
    except (requests.RequestException, ValueError, TypeError):
        source['official_status'] = {'error': 'Estado actual no verificable; no equivale a acto cerrado.'}
    return source


def source_is_closed(source, *, now=None):
    from services.rir_supplier_research import _deadline, _local_timestamp
    from services.anestesia_docs import PANAMA
    clock = _local_timestamp(now or datetime.now(PANAMA).isoformat())
    status = normalized(source.get("info", {}).get("estado", "") or source.get("info", {}).get("estado del proceso", ""))
    if any(word in status for word in ("cancelad", "anulad", "suspendid", "adjudicad", "desierto", "finalizad")):
        return True, f"El estado oficial del acto no permite presentar una oferta: {status}."
    end, exact = _deadline(source.get("closing", ""))
    if end is None:
        return True, "No se pudo verificar el cierre oficial del acto."
    if (exact and end <= clock) or (not exact and end.date() < clock.date()):
        return True, f"El plazo publicado de presentación ya terminó: {source.get('closing', '')}."
    if not exact and end.date() == clock.date():
        official = source.get('official_status') or {}
        checked = _local_timestamp(official.get('checked_at'))
        fresh = checked is not None and 0 <= (clock - checked).total_seconds() <= 300
        same = (official.get('number') == source.get('number') and bool(source.get('flow'))
                and official.get('flow') == source.get('flow')
                and official.get('process_type') == source.get('process_type'))
        if fresh and same and official.get('state_id') == (8 if source.get('process_type') == 2 else 36):
            return False, "El portal confirma el acto abierto/vigente; no publica una hora exacta de cierre."
        return True, "El acto cierra hoy y el portal no indica hora. Hay que confirmar que siga abierto; se comprobará otra vez al generar."
    return False, ""


def needs_live_open_status(source, *, now=None):
    from services.rir_supplier_research import _deadline, _local_timestamp
    from services.anestesia_docs import PANAMA
    end, exact = _deadline(source.get('closing', ''))
    clock = _local_timestamp(now or datetime.now(PANAMA).isoformat())
    return end is not None and not exact and end.date() == clock.date()
