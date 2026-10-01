"""PDF delivery sets; immutable reviews plus a recoverable current Drive folder.

Called only by the orchestrator's serial worker, never from the Streamlit view.
"""
from __future__ import annotations

import json
import uuid

import fitz

from services.anestesia_docs import file_hash, now_iso

CURRENT_NAME = "Entrega actual - PDF para presentar"
UPDATING_NAME = "Entrega actual - ACTUALIZANDO, no presentar"
ROLE = "anestesia_current_delivery"
FOLDER_MIME = "application/vnd.google-apps.folder"


def original_pdf_set(originals, selected):
    """One original per attachment, exactly as in the user's example offers."""
    labels = {
        "dgi": "Paz_y_salvo_DGI", "css": "Paz_y_salvo_CSS", "registro_publico": "Registro_publico",
        "oferente": "Certificado_de_oferentes", "inscripcion_producto": "Catalogo_de_oferentes",
        "criterio_tecnico": "Criterio_tecnico", "catalogo": "Catalogo_del_producto",
        "cedula": "Cedula", "aviso_operacion": "Aviso_de_operacion",
        "licencia_minsa": "Licencia_de_operaciones_MINSA", "metodo_destruccion": "Metodo_de_destruccion",
    }
    result = []
    for kind, data in originals.items():
        label = labels.get(kind, kind.replace(":", "_"))
        result.append({"name": f"{len(result)+2:02d}_{label}.pdf", "data": data,
                       "kind": kind, "library_ids": [selected[kind]["id"]],
                       "original_hashes": {kind: file_hash(data)}})
    return result, []


class DeliveryPublisher:
    """Replace only this module's PDFs and keep rollback information in Drive.

    Drive has no multi-file transaction. The folder is marked as updating until
    every PDF is read back and verified. Interrupted publication is rolled back
    before a retry; a failed rollback remains visibly unavailable.
    """
    def __init__(self, storage):
        self.storage = storage
        self.api = storage.drive.files()

    def _list(self, query):
        result, token = [], None
        while True:
            response = self.api.list(q=query, pageSize=100, pageToken=token,
                fields="nextPageToken,files(id,name,mimeType,appProperties,parents)",
                supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            result.extend(response.get("files", []))
            token = response.get("nextPageToken")
            if not token:
                return result

    def _children(self, ident):
        from services.anestesia_storage import escape
        return self._list(f"trashed=false and '{escape(ident)}' in parents")

    def _update(self, ident, **body):
        return self.api.update(fileId=ident, body=body, fields="id,name,mimeType,appProperties",
                               supportsAllDrives=True).execute()

    def _move(self, file, origin, destination):
        self.api.update(fileId=file["id"], addParents=destination, removeParents=origin,
                        fields="id", supportsAllDrives=True).execute()

    def _current(self, root):
        from services.anestesia_storage import escape
        matches = self._list(f"trashed=false and '{escape(root)}' in parents and "
                             f"appProperties has {{ key='role' and value='{ROLE}' }}")
        if len(matches) > 1:
            raise ValueError("Hay dos carpetas de entrega actual. No se sobrescribió ninguna; revisa la duplicidad.")
        if matches:
            return matches[0]
        return self.api.create(body={"name": CURRENT_NAME, "mimeType": FOLDER_MIME, "parents": [root],
            "appProperties": {"module": "anestesia_docs", "role": ROLE, "state": "empty", "count": "0"}},
            fields="id,name,mimeType,appProperties", supportsAllDrives=True).execute()

    @staticmethod
    def _owned(files):
        if any(f.get("mimeType") != "application/pdf" or f.get("appProperties", {}).get("delivery") != ROLE for f in files):
            raise ValueError("La carpeta de entrega contiene archivos ajenos al conjunto generado. No se eliminaron ni sobrescribieron.")

    def _restore(self, current):
        props = current.get("appProperties", {})
        journal_id = props.get("rollback")
        if not journal_id:
            raise ValueError("Falta el respaldo de una publicación interrumpida. La carpeta sigue marcada como no disponible.")
        journal = self.storage.json_file(journal_id)
        if journal.get("folder_id") != current["id"]:
            raise ValueError("El respaldo no corresponde a la carpeta de entrega.")
        old = {f["id"]: f for f in journal["files"]}
        actual = self._children(current["id"])
        self._owned(actual)
        for file in actual:
            if file["id"] not in old:
                if file.get("appProperties", {}).get("manifest") != props.get("pending"):
                    raise ValueError("Se encontró una modificación ajena a la publicación pendiente.")
                # Reversible trash, scoped to copies created by this transaction.
                self._update(file["id"], trashed=True)
        backed_up = self._children(journal["backup_id"])
        for file in backed_up:
            if file["id"] in old:
                self._move(file, journal["backup_id"], current["id"])
        restored = self._children(current["id"])
        if {f['id'] for f in restored} != set(old):
            raise ValueError("No se completó la recuperación; la carpeta permanece como no disponible.")
        for file in restored:
            if file_hash(self.storage.get_bytes(file["id"])) != old[file["id"]]["appProperties"]["sha256"]:
                raise ValueError("Un PDF anterior cambió; revisa el historial antes de publicar.")
        return self._update(current["id"], name=journal["name"],
                            appProperties={**journal["properties"], "rollback": None, "pending": None})

    def publish(self, pdfs, *, request_id, number, manifest_hash):
        """pdfs are already approved immutable Drive files; only PDF copies go live."""
        names = [f["name"] for f in pdfs]
        if not names or len(names) != len(set(names)) or any(f.get("mime") != "application/pdf" for f in pdfs):
            raise ValueError("La entrega requiere PDF completos con nombres únicos.")
        # Validate staged originals before touching the previous delivery.
        for file in pdfs:
            data = self.storage.get_bytes(file["file_id"])
            if file_hash(data) != file["sha256"]:
                raise ValueError("Cambió un PDF preparado para publicación.")
            with fitz.open(stream=data, filetype="pdf") as doc:
                if doc.is_encrypted or not len(doc):
                    raise ValueError("No se puede publicar un PDF vacío o cifrado.")
        root = self.storage.root()
        current = self._current(root)
        if current.get("appProperties", {}).get("state") == "updating":
            current = self._restore(current)
        props = current.get("appProperties", {})
        old = self._children(current["id"])
        self._owned(old)
        # A retry after a successful Drive commit must not duplicate the set.
        if props.get("manifest") == manifest_hash and props.get("state") == "ready":
            expected = {f['name']: f['sha256'] for f in pdfs}
            if len(old) != len(pdfs) or {f['name']: file_hash(self.storage.get_bytes(f['id'])) for f in old} != expected:
                raise ValueError("Los PDF publicados cambiaron. No se marca el expediente como listo.")
            return {"folder_id": current["id"], "count": len(old)}
        for file in old:
            if file_hash(self.storage.get_bytes(file["id"])) != file.get("appProperties", {}).get("sha256"):
                raise ValueError("Un PDF de la entrega anterior cambió. Se conserva para revisión.")
        history = self.storage.folder("Historial de entregas reemplazadas", root)
        backup = self.storage.folder(now_iso().replace(":", "-") + " - " + uuid.uuid4().hex[:8], history)
        journal = {"folder_id": current["id"], "backup_id": backup, "name": current["name"],
                   "properties": props, "files": old}
        saved = self.storage.put(backup, "recuperacion.json", json.dumps(journal).encode(), "application/json")
        pending = {**props, "state": "updating", "pending": manifest_hash, "rollback": saved["file_id"]}
        current = self._update(current["id"], name=UPDATING_NAME, appProperties=pending)
        try:
            for file in old:
                self._move(file, current["id"], backup)
            for file in pdfs:
                self.api.copy(fileId=file["file_id"], body={"name": file["name"], "parents": [current["id"]],
                    "appProperties": {"module": "anestesia_docs", "delivery": ROLE,
                                      "sha256": file["sha256"], "manifest": manifest_hash}},
                    fields="id", supportsAllDrives=True).execute()
            actual = self._children(current["id"])
            expected = {f['name']: f['sha256'] for f in pdfs}
            if len(actual) != len(pdfs) or {f['name']: file_hash(self.storage.get_bytes(f['id'])) for f in actual} != expected:
                raise ValueError("La copia de los PDF en Drive quedó incompleta; se recuperará la entrega anterior.")
            self._update(current["id"], name=CURRENT_NAME, appProperties={"module": "anestesia_docs", "role": ROLE,
                "state": "ready", "manifest": manifest_hash, "request": request_id, "act": number,
                "count": str(len(pdfs)), "rollback": None, "pending": None})
            return {"folder_id": current["id"], "count": len(pdfs)}
        except Exception as exc:
            try:
                self._restore(current)
            except Exception as recovery:
                raise RuntimeError("No se completó la publicación ni su recuperación. La carpeta está marcada ACTUALIZANDO; "
                                   "no uses esos PDF. Reintenta para recuperar el respaldo. " + str(recovery)) from exc
            raise RuntimeError("No se completó la publicación. Se restauró la entrega anterior; reintenta. " + str(exc)) from exc
