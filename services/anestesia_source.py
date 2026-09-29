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
    source["explicit_fichas"] = sorted(set(re.findall(r"(?:CTNI|FICHA\s*T[ÉE]CNICA)\s*[:#.]?\s*(\d{4,7})\b", full_text, re.I)))
    maximum, evidence = public_registry_age(full_text)
    source.update(registry_max_months=maximum, registry_rule=evidence)
    source["fingerprint"] = canonical_hash({"components": components,
        "documents": [{"id": d["official_id"], "sha256": d["sha256"]} for d in source["attachments"]]})
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
    if (exact and end <= clock) or (not exact and end.date() <= clock.date()):
        return True, "El acto está cerrado o cierra hoy sin hora exacta verificada. No se publica como listo para presentar."
    return False, ""
