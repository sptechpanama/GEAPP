from __future__ import annotations

"""Almacenamiento de adjuntos del Pipeline Estrategico en Google Drive."""

from datetime import datetime
from io import BytesIO
import re
from typing import Any

from googleapiclient.http import MediaIoBaseUpload

from services.pipeline_estrategico import clean_text, normalize_ficha


FOLDER_MIME = "application/vnd.google-apps.folder"


def _escape_query(value: Any) -> str:
    return clean_text(value).replace("\\", "\\\\").replace("'", "\\'")


def _safe_folder_name(value: Any, *, fallback: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "-", clean_text(value)).strip(" .-")
    return (name or fallback)[:120]


def _find_folder(drive, *, name: str, parent_id: str) -> str:
    query = (
        f"mimeType='{FOLDER_MIME}' and trashed=false and "
        f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents"
    )
    response = (
        drive.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = response.get("files") or []
    return str(files[0].get("id") or "") if files else ""


def _find_or_create_folder(drive, *, name: str, parent_id: str) -> str:
    existing = _find_folder(drive, name=name, parent_id=parent_id)
    if existing:
        return existing
    metadata = {
        "name": name,
        "mimeType": FOLDER_MIME,
        "parents": [parent_id],
    }
    created = (
        drive.files()
        .create(body=metadata, fields="id", supportsAllDrives=True)
        .execute()
    )
    folder_id = clean_text(created.get("id"))
    if not folder_id:
        raise RuntimeError("Google Drive no devolvio el ID de la carpeta creada.")
    return folder_id


class PipelineDriveStorage:
    def __init__(self, drive, *, root_folder_id: str) -> None:
        if drive is None:
            raise RuntimeError("No fue posible autenticar Google Drive.")
        self.drive = drive
        self.root_folder_id = clean_text(root_folder_id)
        if not self.root_folder_id:
            raise RuntimeError(
                "Configura DRIVE_PIPELINE_FOLDER_ID o DRIVE_TOPS_FOLDER_ID para adjuntar documentos."
            )

    @classmethod
    def from_config(
        cls,
        drive,
        *,
        pipeline_folder_id: str = "",
        parent_folder_id: str = "",
    ) -> "PipelineDriveStorage":
        exact = clean_text(pipeline_folder_id)
        if exact:
            return cls(drive, root_folder_id=exact)
        parent = clean_text(parent_folder_id)
        if not parent:
            raise RuntimeError(
                "Configura DRIVE_PIPELINE_FOLDER_ID o DRIVE_TOPS_FOLDER_ID para adjuntar documentos."
            )
        root = _find_or_create_folder(
            drive, name="Pipeline Estrategico", parent_id=parent
        )
        return cls(drive, root_folder_id=root)

    @property
    def folder_url(self) -> str:
        return f"https://drive.google.com/drive/folders/{self.root_folder_id}"

    def card_folder(self, card: dict[str, Any]) -> str:
        ficha = normalize_ficha(card.get("ficha")) or "POR-CREAR"
        provider = clean_text(card.get("proveedor")) or "Proveedor"
        brand = clean_text(card.get("marca")) or "Marca"
        folder_name = _safe_folder_name(
            f"{ficha} - {provider} - {brand}", fallback=f"Tarjeta-{card.get('id', '')[:8]}"
        )
        return _find_or_create_folder(
            self.drive, name=folder_name, parent_id=self.root_folder_id
        )

    def _filename_exists(self, *, folder_id: str, file_name: str) -> bool:
        query = (
            "trashed=false and "
            f"name='{_escape_query(file_name)}' and "
            f"'{_escape_query(folder_id)}' in parents"
        )
        response = (
            self.drive.files()
            .list(
                q=query,
                fields="files(id)",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        return bool(response.get("files"))

    def upload(
        self,
        *,
        card: dict[str, Any],
        file_name: str,
        data: bytes,
        mime_type: str,
    ) -> dict[str, Any]:
        folder_id = self.card_folder(card)
        final_name = _safe_folder_name(file_name, fallback="documento")
        if self._filename_exists(folder_id=folder_id, file_name=final_name):
            stem, dot, suffix = final_name.rpartition(".")
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            if dot:
                final_name = f"{stem}-{timestamp}.{suffix}"
            else:
                final_name = f"{final_name}-{timestamp}"
        media = MediaIoBaseUpload(
            BytesIO(data),
            mimetype=clean_text(mime_type) or "application/octet-stream",
            resumable=False,
        )
        result = (
            self.drive.files()
            .create(
                body={"name": final_name, "parents": [folder_id]},
                media_body=media,
                fields="id,name,mimeType,size,webViewLink,webContentLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        file_id = clean_text(result.get("id"))
        result["webViewLink"] = clean_text(result.get("webViewLink")) or (
            f"https://drive.google.com/file/d/{file_id}/view" if file_id else ""
        )
        result["folder_id"] = folder_id
        result["folder_url"] = f"https://drive.google.com/drive/folders/{folder_id}"
        return result


__all__ = ["PipelineDriveStorage"]
