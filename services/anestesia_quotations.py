"""Single-button quotations on the existing worker, Sheets index and Drive library."""
from __future__ import annotations

from datetime import datetime
from io import BytesIO
import json
from pathlib import Path
import uuid
import zipfile

import fitz

from services.anestesia_docs import (PANAMA, canonical_hash, file_hash, now_iso,
    parse_date, prepare_offer_config, totals, validate_package)
from services.anestesia_documents import quote_docx
from services.anestesia_delivery import DeliveryPublisher, original_pdf_set
from services.anestesia_health import certificate_content_check
from services.anestesia_source import capture, portal_delivery_term, route, source_is_closed


def location_fields(source):
    info = source.get("info", {})
    province = str(source.get("province") or info.get("provincia") or info.get("provincia de entrega") or "").strip()
    hospital = str(source.get("purchase_unit") or info.get("unidad de compra") or "").strip()
    return province, hospital


def prepare_quotation(source, values):
    """Bind UI attestations to this freshly scraped act, never another URL."""
    from services.rir_supplier_research import _deadline
    today = datetime.now(PANAMA).date()
    issued = parse_date(source.get("publication"))
    if issued is None or issued > today:
        raise ValueError("No se pudo verificar la fecha del pliego. No se sustituyó por la fecha de generación.")
    province, hospital = location_fields(source)
    if not province or not hospital:
        raise ValueError("PanamáCompra no devolvió Provincia y Unidad de compra completas. Reintenta la captura.")
    warehouse = str(values.get("warehouse") or "").strip()
    previous_location = values.get("location_source_hash")
    if values.get("location_reviewed") is True and previous_location and previous_location != source.get("fingerprint"):
        raise ValueError("El acto cambió desde la revisión del lugar de entrega. Verifica los adjuntos y vuelve a generar.")
    if values.get("location_reviewed") is not True and not warehouse:
        raise ValueError("Verifica los adjuntos o indica el Almacén/lugar específico de entrega.")
    closing, _ = _deadline(source.get("closing", ""))
    config = prepare_offer_config(source, {**values, "source_confirmed": True,
        "document_date": issued.isoformat(), "control_date": str(max(today, closing.date() if closing else today))})
    # An explicitly selected warehouse belongs to the form; do not replace it
    # with an unrelated address or an old annex's delivery field.
    config.update(province=province, hospital=hospital, warehouse=warehouse,
        delivery_place=" - ".join(part for part in (province, hospital, warehouse) if part),
        location_source_hash=source.get("fingerprint") if values.get("location_reviewed") else None)
    if values.get("delivery_use_portal") is True:
        previous = values.get("delivery_portal_source_hash")
        if previous and previous != source.get("fingerprint"):
            raise ValueError("El acto cambió desde la revisión de adjuntos. Revisa el plazo actualizado y vuelve a generar.")
        term = portal_delivery_term(source)
        if not term:
            raise ValueError("El portal no indica un término de entrega utilizable. Ingresa el plazo manualmente.")
        config.update(delivery=term, delivery_portal_value=term,
            delivery_portal_source_hash=source.get("fingerprint"))
    return config


def generate_quotation(storage, payload, *, execution_id, root: Path):
    """Called by the existing single-consumer manual queue; no email or submission."""
    ident = payload["request_id"]
    job = storage.job(ident)
    if job.get("last_execution") == execution_id and job.get("state") == "Documentos generados":
        return job
    values = payload.get("config") or job.get("config") or {}
    url = payload.get("url") or job.get("url")
    number = route(url)[2]
    quote = None
    storage.save_job({"id": ident, "state": "Procesando", "last_execution": execution_id,
        "detail": "Capturando el acto y verificando documentos vigentes.", "checks": []})
    try:
        # Sources and conversion files never contaminate the 12-PDF folder.
        histories = storage.folder("Expedientes", storage.root())
        history = storage.folder(f"{number} - {ident[:8]}", histories)
        sources = storage.folder("Fuentes oficiales - " + execution_id[:12], history)
        source = capture(url, storage, sources)
        source_file = storage.put(sources, "acto_y_anexos.json", json.dumps(source, ensure_ascii=False).encode(), "application/json")
        storage.save_job({"id": ident, "number": number, "url": url,
            "source_id": source_file["file_id"], "source_hash": source["fingerprint"]})
        # Preserve a manual setting even when preparing the rest of the data
        # reveals a blocking field; a retry should not lose the user's choices.
        storage.save_job({"id": ident, "config": values})
        config = prepare_quotation(source, values)
        storage.save_job({"id": ident, "config": config})
        closed, reason = source_is_closed(source)
        if closed:
            raise ValueError(reason)
        checks, selected = validate_package(source, config, storage.rows("ANESTESIA_DOCUMENTOS"))
        if any(c["estado"] != "Vigente documentalmente" for c in checks):
            return storage.save_job({"id": ident, "state": "Bloqueado", "checks": checks,
                "detail": "No se generaron documentos. Actualiza los originales o datos indicados."})
        originals = {}
        for kind, document in selected.items():
            data = storage.get_bytes(document["file_id"])
            if file_hash(data) != document["sha256"]:
                raise ValueError(f"El original de {kind} cambió en Drive. Registra su nueva versión.")
            validation = certificate_content_check(data, document,
                ocr_text=(document.get("content_validation") or {}).get("ocr_text", ""))
            if validation["errors"]:
                return storage.save_job({"id": ident, "state": "Bloqueado", "checks": [
                    {"documento": document.get("label", kind), "estado": "Bloqueado",
                     "motivo": " ".join(validation["errors"]), "enlace": document.get("url", "")}],
                    "detail": "No se generaron documentos. El certificado no pasa la verificación de contenido."})
            originals[kind] = data
        original_files, _ = original_pdf_set(originals, selected)
        if len(original_files) != 11:
            raise ValueError("El expediente no corresponde a los 11 adjuntos del ejemplo. Revisa sus requisitos específicos.")
        quote = storage.ensure_quotation(number, url)
        config["quotation_number"] = quote["quotation_number"]
        storage.save_job({"id": ident, "config": config, "quotation_id": quote["id"],
            "quotation_number": quote["quotation_number"], "quotation_root_url": quote["root_url"]})
        version = storage.folder("Versiones y comprobaciones", quote["folder_id"])
        stage = storage.folder(execution_id + " - " + uuid.uuid4().hex[:8], version)
        docx = quote_docx(source, config, root / "assets/cotizacion_base")
        word = storage.put(stage, "01_Cotizacion.docx", docx,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        conversion = storage.folder("Conversión Word a PDF", stage)
        pdf = storage.convert_document(docx, "01_Cotizacion.docx", conversion)
        with fitz.open(stream=pdf, filetype="pdf") as document:
            text = " ".join(page.get_text() for page in document)
            if not len(document) or number not in text or quote["quotation_number"] not in text:
                raise ValueError("El PDF no contiene el acto y número de cotización correctos. No se publicó.")
        files = [storage.put(stage, "01_Cotizacion.pdf", pdf, "application/pdf")]
        for original in original_files:
            files.append(storage.put(stage, original["name"], original["data"], "application/pdf"))
        manifest = {"request_id": ident, "quotation_number": quote["quotation_number"],
            "source": source, "config": config, "checks": checks, "documents": selected,
            "files": files, "word": word, "created_at": now_iso(),
            "review": "Comprobaciones automáticas; no representa una auditoría independiente de ChatGPT."}
        manifest_hash = canonical_hash(manifest)
        storage.put(stage, "manifest.json", json.dumps({**manifest, "manifest_hash": manifest_hash}, ensure_ascii=False).encode(), "application/json")
        # Verify that expiry/replacement has not happened during conversion.
        latest_checks, latest = validate_package(source, config, storage.rows("ANESTESIA_DOCUMENTOS"))
        if any(c["estado"] != "Vigente documentalmente" for c in latest_checks) or {
                k: v["id"] for k, v in selected.items()} != {k: v["id"] for k, v in latest.items()}:
            raise ValueError("La biblioteca cambió durante la generación. Vuelve a generar con los documentos actualizados.")
        verification = storage.folder("Verificación oficial antes de publicar", stage)
        fresh = capture(url, storage, verification)
        if fresh["fingerprint"] != source["fingerprint"]:
            saved = storage.put(verification, "acto_y_anexos.json", json.dumps(fresh, ensure_ascii=False).encode(), "application/json")
            storage.save_job({"id": ident, "source_id": saved["file_id"], "source_hash": fresh["fingerprint"]})
            raise ValueError("El acto o sus anexos cambiaron durante la generación. Revisa la nueva captura y vuelve a generar.")
        closed, reason = source_is_closed(fresh)
        if closed:
            raise ValueError(reason)
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for file in files:
                archive.writestr(file["name"], storage.get_bytes(file["file_id"]))
        zipped = storage.put(stage, quote["quotation_number"] + ".zip", buffer.getvalue(), "application/zip")
        amounts = totals(source["items"][0]["cantidad"], config["price"], config["tax_mode"], config.get("tax_rate", 7))
        published = DeliveryPublisher(storage, root=quote["folder_id"],
            folder_name="Documentos para presentar - 12 PDF").publish(files,
                request_id=ident, number=number, manifest_hash=manifest_hash)
        folder_url = f"https://drive.google.com/drive/folders/{published['folder_id']}"
        storage.save_quotation({**quote, "state": "Documentos generados", "final_url": folder_url,
            "delivery_folder_id": published["folder_id"], "word_url": word["url"], "zip_url": zipped["url"],
            "config": config, "amounts": amounts, "manifest_hash": manifest_hash, "request_id": ident})
        return storage.save_job({"id": ident, "state": "Documentos generados", "detail": "12 PDF y cotización Word generados.",
            "final_url": folder_url, "folder_url": quote["folder_url"], "folder_id": quote["folder_id"],
            "word_url": word["url"], "zip_url": zipped["url"], "delivery_folder_id": published["folder_id"],
            "manifest_hash": manifest_hash, "published_manifest": manifest_hash, "delivery_pdf_count": 12,
            "checks": checks, "config": config})
    except Exception as exc:
        if quote is not None:
            storage.save_quotation({"id": quote["id"], "state": "Error", "detail": str(exc)[:1500]})
        storage.save_job({"id": ident, "state": "Bloqueado" if isinstance(exc, ValueError) else "Error",
            "detail": str(exc)[:1500], "last_execution": execution_id})
        raise
