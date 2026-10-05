"""Background document workflow, invoked by the existing orchestrator's manual queue."""
from __future__ import annotations

from datetime import datetime
from io import BytesIO
import json
from pathlib import Path
import re
import zipfile

import fitz

from services.anestesia_docs import (PANAMA, canonical_hash, file_hash, now_iso, review_errors,
                                    prepare_offer_config, review_prompt, totals, validate_package)
from services.anestesia_documents import quote_docx
from services.anestesia_source import capture, source_is_closed
from services.anestesia_health import certificate_content_check
from services.anestesia_delivery import DeliveryPublisher, original_pdf_set


def write_json(storage, folder, name, value):
    return storage.put(folder, name, json.dumps(value, ensure_ascii=False, indent=2).encode(), "application/json")


def run_request(storage, payload, *, execution_id, root: Path):
    action = payload.get("action")
    ident = str(payload.get("request_id", ""))
    if not re.fullmatch(r"[a-f0-9]{32}", ident):
        raise ValueError("Identificador de expediente inválido.")
    job = storage.job(ident)
    if not job:
        raise ValueError("No se encontró el expediente en el índice.")
    if action == "generate_quotation":
        from services.anestesia_quotations import generate_quotation
        return generate_quotation(storage, payload, execution_id=execution_id, root=root)
    if action == "finalize" and job.get("state") == "Listo para entregar" and job.get("published_manifest") == job.get("manifest_hash"):
        return job
    if job.get("last_execution") == execution_id and job.get("state") in {"Datos capturados", "Bloqueado", "Pendiente de revisión", "Listo para entregar"}:
        return job
    storage.save_job({"id": ident, "state": "Procesando", "detail": "El orquestador está verificando el expediente."})
    try:
        root_folder = storage.folder("Expedientes", storage.root())
        folder = job.get("folder_id") or storage.folder(f"{job.get('number', ident)} - {ident[:8]}", root_folder)
        base = {"id": ident, "folder_id": folder, "folder_url": f"https://drive.google.com/drive/folders/{folder}", "last_execution": execution_id}
        if action == "capture":
            source_folder = storage.folder("Fuentes oficiales - " + execution_id[:8], folder)
            source = capture(payload["url"], storage, source_folder)
            saved = write_json(storage, source_folder, "acto_y_anexos.json", source)
            return storage.save_job({**base, "number": source["number"], "source_id": saved["file_id"],
                "source_hash": source["fingerprint"], "manifest_id": "", "manifest_hash": "", "final_url": "", "zip_url": "",
                "state": "Datos capturados", "detail": "Revisa datos, anexos, calendario y requisitos; después comprueba la biblioteca."})
        if not job.get("source_id"):
            raise ValueError("Primero captura el acto y sus anexos.")
        source = storage.json_file(job["source_id"])
        if action == "generate":
            config = prepare_offer_config(source, payload["config"])
            library = storage.rows("ANESTESIA_DOCUMENTOS")
            checks, selected = validate_package(source, config, library)
            blocked = [c for c in checks if c["estado"] != "Vigente documentalmente"]
            if blocked:
                return storage.save_job({**base, "state": "Bloqueado", "detail": "Actualiza los documentos o datos indicados antes de generar.",
                    "checks": checks, "config": config})
            # Re-check source before generating; old drafts cannot silently carry amendments.
            verify_folder = storage.folder("Comprobación de fuentes - " + execution_id[:8], folder)
            fresh = capture(source["url"], storage, verify_folder)
            if fresh["fingerprint"] != source["fingerprint"]:
                saved = write_json(storage, verify_folder, "acto_y_anexos.json", fresh)
                return storage.save_job({**base, "source_id": saved["file_id"], "state": "Datos capturados",
                    "detail": "El acto o sus anexos cambiaron. Revisa la nueva captura y confirma los datos antes de generar."})
            closed, reason = source_is_closed(fresh)
            if closed:
                raise ValueError(reason)
            # Validate actual originals before producing even the quotation.
            originals = {}
            for kind, document in selected.items():
                data = storage.get_bytes(document["file_id"])
                if file_hash(data) != document["sha256"]:
                    raise ValueError(f"El original de {kind} cambió en Drive. Registra la versión actualizada.")
                validation = certificate_content_check(data, document,
                    ocr_text=(document.get('content_validation') or {}).get('ocr_text', ''))
                if validation["errors"]:
                    return storage.save_job({**base, "state": "Bloqueado", "config": config,
                        "detail": "Actualiza o corrige el certificado antes de generar.",
                        "checks": [{"documento": document.get("label", kind), "estado": "Bloqueado",
                                    "motivo": " ".join(validation["errors"]), "enlace": document.get("url", "")}]})
                originals[kind] = data
            draft_folder = storage.folder("Borradores - " + execution_id[:8], folder)
            conversion = storage.folder("Conversión Word a PDF", draft_folder)
            files = []
            # This is the proposal stage. The entity's bilateral pact belongs
            # to the later award procedure in the supplied example annexes.
            outputs = [("01_Cotizacion.docx", quote_docx(source, config, root / "assets/cotizacion_base"))]
            for name, data in outputs:
                original = storage.put(draft_folder, name, data, "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                files.append({**original, "deliverable": True})
                pdf = storage.convert_document(data, name, conversion)
                with fitz.open(stream=pdf, filetype="pdf") as doc:
                    if not len(doc) or source["number"] not in " ".join(page.get_text() for page in doc):
                        raise ValueError("El PDF no contiene el número del acto. Revisar la conversión.")
                files.append({**storage.put(draft_folder, name.replace(".docx", ".pdf"), pdf, "application/pdf"), "deliverable": True})
            original_files, delivery_notes = original_pdf_set(originals, selected)
            for original in original_files:
                saved = storage.put(draft_folder, original['name'], original['data'], "application/pdf")
                files.append({**saved, "deliverable": True,
                    **{key: value for key, value in original.items() if key not in {'name', 'data'}}})
            amounts = totals(source["items"][0]["cantidad"], config["price"], config["tax_mode"], config.get("tax_rate", 7))
            content = {"request_id": ident, "version": execution_id, "source": source, "config": config,
                "amounts": amounts, "checks": checks, "documents": selected, "files": files, "created_at": now_iso(),
                "delivery_pdf_count": sum(f['mime'] == 'application/pdf' for f in files), "delivery_notes": delivery_notes,
                "folder_url": f"https://drive.google.com/drive/folders/{draft_folder}"}
            manifest = {**content, "manifest_hash": canonical_hash(content)}
            stored = write_json(storage, draft_folder, "manifest.json", manifest)
            prompt = storage.put(draft_folder, "LEEME_revision_ChatGPT.md", review_prompt(manifest).encode(), "text/markdown")
            return storage.save_job({**base, "state": "Pendiente de revisión", "detail": "Borradores preparados. Revisa en ChatGPT y adjunta el JSON de auditoría; aún no están habilitados para entregar.",
                "manifest_id": stored["file_id"], "manifest_hash": manifest["manifest_hash"], "prompt_url": prompt["url"],
                "draft_url": content["folder_url"], "checks": checks, "config": config,
                "delivery_pdf_count": content['delivery_pdf_count'], "delivery_notes": delivery_notes})
        if action == "finalize":
            if not job.get("manifest_id"):
                raise ValueError("No hay borradores revisables.")
            manifest = storage.json_file(job["manifest_id"])
            unsigned = {k: v for k, v in manifest.items() if k != "manifest_hash"}
            if canonical_hash(unsigned) != manifest["manifest_hash"]:
                raise ValueError("El manifiesto cambió después de su generación.")
            reviews = [r for r in storage.rows("ANESTESIA_REVISIONES") if r.get("id") == payload.get("review_id")]
            if not reviews:
                raise ValueError("No se encontró una revisión registrada.")
            problems = review_errors(reviews[0], manifest)
            if problems:
                raise ValueError(" ".join(problems))
            if payload.get("user_confirmed") is not True:
                raise ValueError("Falta confirmar la revisión final del expediente.")
            checks, selected = validate_package(source, manifest["config"], storage.rows("ANESTESIA_DOCUMENTOS"))
            if any(c["estado"] != "Vigente documentalmente" for c in checks):
                return storage.save_job({**base, "state": "Bloqueado", "detail": "Un documento dejó de cumplir antes de publicar. Actualízalo y genera otra versión.", "checks": checks})
            if {k: v["id"] for k, v in selected.items()} != {k: v["id"] for k, v in manifest["documents"].items()}:
                raise ValueError("La biblioteca tiene nuevas versiones. Genera y revisa el expediente actualizado.")
            verify_folder = storage.folder("Verificación final - " + execution_id[:8], folder)
            fresh = capture(source["url"], storage, verify_folder)
            if fresh["fingerprint"] != manifest["source"]["fingerprint"]:
                raise ValueError("Cambió el acto o un anexo. Vuelve a capturar y revisar el expediente.")
            closed, reason = source_is_closed(fresh)
            if closed:
                raise ValueError(reason)
            verified = []
            for file in manifest["files"]:
                if file.get("deliverable"):
                    data = storage.get_bytes(file["file_id"])
                    if file_hash(data) != file["sha256"]:
                        raise ValueError(f"Cambió el archivo {file['name']} después de la revisión.")
                    verified.append((file, data))
            # Keep the exact approved PDFs as an immutable per-case snapshot.
            # The common delivery folder is replaced only after this set is complete.
            import uuid
            final_folder = storage.folder(f"Entrega {source['number']} - PDF para presentar - {uuid.uuid4().hex[:12]}", folder)
            audit_folder = storage.folder("Revisión y descargables - " + manifest["version"], folder)
            pdf_files = []
            buffer = BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipout:
                for file, data in verified:
                    if file['mime'] == 'application/pdf':
                        pdf_files.append(storage.put(final_folder, file["name"], data, file["mime"]))
                        zipout.writestr(file["name"], data)
            archive = storage.put(audit_folder, f"Expediente_{source['number']}.zip", buffer.getvalue(), "application/zip")
            write_json(storage, audit_folder, "revision_registrada.json", reviews[0])
            write_json(storage, audit_folder, "manifest.json", manifest)
            published = DeliveryPublisher(storage).publish(pdf_files, request_id=ident,
                number=source['number'], manifest_hash=manifest['manifest_hash'])
            return storage.save_job({**base, "state": "Listo para entregar",
                "detail": f"{published['count']} PDF revisados publicados en Entrega actual. Se reemplazó el conjunto anterior y se conservó su historial.",
                "final_url": f"https://drive.google.com/drive/folders/{published['folder_id']}",
                "delivery_folder_id": published['folder_id'], "delivery_pdf_count": published['count'],
                "archive_url": f"https://drive.google.com/drive/folders/{final_folder}",
                "zip_url": archive["url"], "published_at": now_iso(), "published_manifest": manifest["manifest_hash"]})
        raise ValueError("Acción documental desconocida.")
    except Exception as exc:
        storage.save_job({"id": ident, "state": "Error", "detail": str(exc)[:1500], "last_execution": execution_id})
        raise
