"""Drive originals + Sheets index, using the application's existing credentials."""
from __future__ import annotations

from io import BytesIO
import json
import re
import time
import uuid

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

from services.anestesia_docs import file_hash, now_iso

SHEET_ID = "1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
DRIVE_PARENT = "0AOB-QlptrUHYUk9PVA"
FOLDER = "application/vnd.google-apps.folder"
TABLES = {
    "ANESTESIA_DOCUMENTOS": ["id", "documento", "catalogo", "emision", "vencimiento", "enlace", "registrado_por", "registrado_en", "datos_json"],
    "ANESTESIA_EXPEDIENTES": ["id", "acto", "estado", "detalle", "carpeta", "actualizado_en", "datos_json"],
    "ANESTESIA_REVISIONES": ["id", "expediente", "decision", "manifest_hash", "revisor", "fecha", "datos_json"],
}


def escape(value):
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def safe_name(value):
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]', "-", str(value)).strip(" .")[:180] or "documento"


class AnestesiaStorage:
    def __init__(self, drive, sheets, *, sheet_id=SHEET_ID, parent_id=DRIVE_PARENT):
        self.drive, self.sheets = drive, sheets
        self.sheet_id, self.parent_id = sheet_id, parent_id

    def ensure_tables(self):
        try:
            metadata = self.sheets.spreadsheets().get(spreadsheetId=self.sheet_id,
                fields="sheets.properties").execute()
        except HttpError as exc:
            # PC_MANUAL_SHEET_ID can be a historical XLSX. Reuse the existing
            # native orchestrator book; never convert it or create a second queue.
            office = exc.resp.status == 400 and "must not be an office file" in str(exc).lower()
            if not office or self.sheet_id == SHEET_ID:
                raise
            metadata = self.sheets.spreadsheets().get(spreadsheetId=SHEET_ID,
                fields="sheets.properties").execute()
            titles = {s["properties"]["title"] for s in metadata.get("sheets", [])}
            if not {"pc_config", "pc_manual"}.issubset(titles):
                raise ValueError("El libro alternativo no contiene la cola del orquestador. No se modificó ningún archivo.") from exc
            self.sheet_id = SHEET_ID
        existing = {s["properties"]["title"] for s in metadata.get("sheets", [])}
        missing = [name for name in TABLES if name not in existing]
        if missing:
            self.sheets.spreadsheets().batchUpdate(spreadsheetId=self.sheet_id, body={"requests": [
                {"addSheet": {"properties": {"title": name, "gridProperties": {"rowCount": 2000,
                    "columnCount": len(TABLES[name]), "frozenRowCount": 1}}}} for name in missing]}).execute()
            self.sheets.spreadsheets().values().batchUpdate(spreadsheetId=self.sheet_id,
                body={"valueInputOption": "RAW", "data": [{"range": f"'{name}'!A1", "values": [TABLES[name]]}
                    for name in missing]}).execute()

    def rows(self, name):
        if name not in TABLES:
            raise ValueError("Índice documental desconocido")
        end = chr(64 + len(TABLES[name]))
        values = self.sheets.spreadsheets().values().get(spreadsheetId=self.sheet_id,
            range=f"'{name}'!A1:{end}20000").execute().get("values", [])
        if not values or values[0] != TABLES[name]:
            raise ValueError(f"Los encabezados de {name} no coinciden. No se sobrescribió la hoja.")
        out = []
        for n, row in enumerate(values[1:], 2):
            if not any(row):
                continue
            data = dict(zip(values[0], row + [""] * (len(values[0]) - len(row))))
            raw = data.get("datos_json", "")
            try:
                decoded = json.loads(raw) if raw else {}
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Registro {n} de {name} ilegible. Revisar su historial; no se ignoró.") from exc
            out.append({**decoded, "_row": n})
        return out

    def _write(self, name, columns, data, row=None):
        end = chr(64 + len(TABLES[name]))
        headers = self.sheets.spreadsheets().values().get(spreadsheetId=self.sheet_id,
            range=f"'{name}'!A1:{end}1").execute().get("values", [])
        if headers != [TABLES[name]]:
            raise ValueError(f"Cambió la estructura de {name}. No se escribió sobre columnas ajenas.")
        encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        if len(encoded) > 45000:
            raise ValueError("El índice excede el límite de una celda; guarda el contenido en Drive.")
        record = {**columns, "datos_json": encoded}
        values = [[str(record.get(h, "")) for h in TABLES[name]]]
        api = self.sheets.spreadsheets().values()
        if row:
            api.update(spreadsheetId=self.sheet_id, range=f"'{name}'!A{row}", valueInputOption="RAW", body={"values": values}).execute()
        else:
            api.append(spreadsheetId=self.sheet_id, range=f"'{name}'!A1", valueInputOption="RAW",
                insertDataOption="INSERT_ROWS", body={"values": values}).execute()

    def folder(self, name, parent):
        query = f"trashed=false and mimeType='{FOLDER}' and name='{escape(name)}' and '{escape(parent)}' in parents"
        files = self.drive.files().list(q=query, fields="files(id,name)", pageSize=10,
            supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
        if files:
            return files[0]["id"]
        return self.drive.files().create(body={"name": name, "mimeType": FOLDER, "parents": [parent]},
            fields="id", supportsAllDrives=True).execute()["id"]

    def root(self):
        rir = self.folder("RIR", self.parent_id)
        return self.folder("Anestesia-Docs", rir)

    def put(self, parent, name, data: bytes, mime):
        if not data or len(data) > 30 * 1024 * 1024:
            raise ValueError("El archivo debe contener datos y no exceder 30 MB.")
        media = MediaIoBaseUpload(BytesIO(data), mimetype=mime, resumable=False)
        result = self.drive.files().create(body={"name": safe_name(name), "parents": [parent],
            "appProperties": {"sha256": file_hash(data), "module": "anestesia_docs"}},
            media_body=media, fields="id,name,mimeType,size,webViewLink", supportsAllDrives=True).execute()
        return {"file_id": result["id"], "name": result["name"], "mime": result.get("mimeType", mime),
                "url": result.get("webViewLink") or f"https://drive.google.com/file/d/{result['id']}/view",
                "sha256": file_hash(data), "size": len(data)}

    def get_bytes(self, file_id, *, max_size=30 * 1024 * 1024):
        meta = self.drive.files().get(fileId=file_id, fields="id,size,mimeType", supportsAllDrives=True).execute()
        if int(meta.get("size", 0)) > max_size:
            raise ValueError("El documento supera el tamaño permitido.")
        buffer = BytesIO()
        request = self.drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        downloader = MediaIoBaseDownload(buffer, request, chunksize=1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk(num_retries=2)
            if buffer.tell() > max_size:
                raise ValueError("El documento supera el tamaño permitido.")
        return buffer.getvalue()

    def json_file(self, file_id):
        return json.loads(self.get_bytes(file_id, max_size=8 * 1024 * 1024).decode("utf-8"))

    def delivery_status(self, folder_id):
        """Read publication identity before linking a shared, replaceable PDF set."""
        result = self.drive.files().get(fileId=folder_id, fields="id,name,mimeType,trashed,appProperties",
                                       supportsAllDrives=True).execute()
        props = result.get('appProperties', {})
        if result.get('trashed') or result.get('mimeType') != FOLDER or props.get('role') != 'anestesia_current_delivery':
            raise ValueError('No se pudo verificar la carpeta de entrega actual.')
        return props

    def _validated_metadata(self, data, metadata):
        from services.anestesia_health import certificate_content_check
        validation = certificate_content_check(data, metadata)
        if metadata.get('kind') in {'css', 'dgi'} and validation['unreadable_pages']:
            try:
                folder = self.folder('Lectura de certificados', self.root())
                text = self.convert_document(data, f"{metadata['kind']}_{file_hash(data)[:12]}.pdf", folder, pdf_input=True).decode('utf8')
                validation = certificate_content_check(data, metadata, ocr_text=text)
            except Exception:
                validation['errors'].append('No se pudo completar la lectura OCR. El original se conserva pendiente; reintenta su verificación.')
        metadata = {**metadata, "content_validation": validation}
        if validation["errors"]:
            metadata["verified"] = False
        return metadata

    def upload_document(self, name, data, metadata, *, actor):
        metadata = self._validated_metadata(data, metadata)
        ident = uuid.uuid4().hex
        folder = self.folder("Biblioteca de documentos", self.root())
        file = self.put(folder, f"{metadata['kind'].replace(':', '-')}_{ident[:8]}_{safe_name(name)}", data, "application/pdf")
        document = {**metadata, **file, "id": ident, "created_at": now_iso(), "actor": actor}
        self._write("ANESTESIA_DOCUMENTOS", {"id": ident, "documento": metadata.get("label", metadata["kind"]),
            "catalogo": metadata.get("catalogs", ""), "emision": metadata.get("issued", ""),
            "vencimiento": metadata.get("expires", ""), "enlace": file["url"], "registrado_por": actor,
            "registrado_en": document["created_at"]}, document)
        return document

    def revise_document(self, original, metadata, *, actor):
        """Append verified metadata without replacing the original PDF or old record."""
        data = self.get_bytes(original["file_id"])
        if file_hash(data) != original["sha256"]:
            raise ValueError("El PDF cambió en Drive. Carga su nueva versión antes de verificarlo.")
        checked = self._validated_metadata(data, {**original, **metadata})
        metadata = {**metadata, 'verified': checked.get('verified', False), 'content_validation': checked['content_validation']}
        ident = uuid.uuid4().hex
        document = {**original, **metadata, "id": ident, "created_at": now_iso(), "actor": actor,
                    "previous_id": original["id"]}
        document.pop("_row", None)
        self._write("ANESTESIA_DOCUMENTOS", {"id": ident, "documento": document.get("label", document["kind"]),
            "catalogo": document.get("catalogs", ""), "emision": document.get("issued", ""),
            "vencimiento": document.get("expires", ""), "enlace": document["url"], "registrado_por": actor,
            "registrado_en": document["created_at"]}, document)
        return document

    def job(self, ident):
        return next((r for r in self.rows("ANESTESIA_EXPEDIENTES") if r.get("id") == ident), None)

    def queue_request(self, ident):
        """Read this module's request without changing the shared queue."""
        values = self.sheets.spreadsheets().values().get(spreadsheetId=self.sheet_id,
            range="'pc_manual'!A1:K20000").execute().get("values", [])
        if not values or not {"id", "job", "status"}.issubset(values[0]):
            raise ValueError("No se pudo comprobar la cola del orquestador: encabezados inesperados.")
        for row in values[1:]:
            request = dict(zip(values[0], row))
            if request.get("id") == ident and request.get("job") == "anestesia_docs":
                return request
        return None

    def save_job(self, data):
        current = self.job(data["id"])
        saved = {**(current or {}), **data, "updated_at": now_iso()}
        saved.pop("_row", None)
        self._write("ANESTESIA_EXPEDIENTES", {"id": saved["id"], "acto": saved.get("number", ""),
            "estado": saved.get("state", ""), "detalle": saved.get("detail", ""), "carpeta": saved.get("folder_url", ""),
            "actualizado_en": saved["updated_at"]}, saved, row=current.get("_row") if current else None)
        return saved

    def save_review(self, review, actor):
        data = {**review, "id": uuid.uuid4().hex, "registered_by": actor, "registered_at": now_iso()}
        self._write("ANESTESIA_REVISIONES", {"id": data["id"], "expediente": data.get("request_id", ""),
            "decision": data.get("decision", ""), "manifest_hash": data.get("manifest_hash", ""),
            "revisor": data.get("revisor", ""), "fecha": data.get("reviewed_at", "")}, data)
        return data

    def convert_document(self, data, name, parent, *, pdf_input=False):
        """Google Docs renders DOCX to PDF; PDF input uses Drive OCR to obtain text.

        Keep conversion intermediates inside the request's folder, never alter originals.
        """
        mime = "application/pdf" if pdf_input else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        target = "text/plain" if pdf_input else "application/pdf"
        converted = self.drive.files().create(body={"name": "Conversión - " + safe_name(name),
            "mimeType": "application/vnd.google-apps.document", "parents": [parent]},
            media_body=MediaIoBaseUpload(BytesIO(data), mimetype=mime, resumable=False),
            ocrLanguage="es", fields="id", supportsAllDrives=True).execute()
        for attempt in range(3):
            try:
                output = self.drive.files().export_media(fileId=converted["id"], mimeType=target).execute()
                if not output or (not pdf_input and not output.startswith(b"%PDF")):
                    raise ValueError("La conversión no devolvió un archivo completo.")
                return output
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1 + attempt * 2)

    def register_worker(self, *, python_path, script_path):
        """Register a manual-only job without changing another job or any header."""
        api = self.sheets.spreadsheets().values()
        raw = api.batchGet(spreadsheetId=self.sheet_id, ranges=["'pc_config'!A1:F200", "'pc_manual'!A1:K1"]).execute()["valueRanges"]
        cfg = raw[0].get("values", [])
        manual_headers = (raw[1].get("values") or [[]])[0]
        if not cfg or cfg[0] != ["name", "python", "script", "days", "times", "enabled"]:
            raise ValueError("Revisar configuración del orquestador: encabezados inesperados.")
        expected = ["id", "job", "requested_by", "requested_at", "status", "notes", "payload", "result_file_id", "result_file_url", "result_file_name", "result_error"]
        if manual_headers != expected:
            raise ValueError("Revisar cola manual del orquestador: encabezados inesperados.")
        if not any(row and row[0] == "anestesia_docs" for row in cfg[1:]):
            api.append(spreadsheetId=self.sheet_id, range="'pc_config'!A1:F1", valueInputOption="RAW",
                body={"values": [["anestesia_docs", python_path, script_path, "", "", "si"]]}).execute()

    def enqueue(self, payload, *, actor, python_path, script_path):
        """Append a request to the same manual queue as LP Generator, never rewrite headers."""
        self.register_worker(python_path=python_path, script_path=script_path)
        ident = uuid.uuid4().hex
        api = self.sheets.spreadsheets().values()
        payload = {**payload, "sheet_id": self.sheet_id, "parent_id": self.parent_id}
        api.append(spreadsheetId=self.sheet_id, range="'pc_manual'!A1:K1", valueInputOption="RAW", body={"values": [
            [ident, "anestesia_docs", actor, now_iso(), "pending", "", json.dumps(payload, ensure_ascii=False), "", "", "", ""]]}).execute()
        return ident
