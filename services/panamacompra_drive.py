"""Sincroniza archivos críticos desde Google Drive (panamacompra y planillas auxiliares)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

from core.config import APP_ROOT, DB_PATH
from services.auth_drive import DOMAIN_USER, get_service_account_credentials

_ENV_FILE_ID = "FINAPP_DRIVE_DB_FILE_ID"
_SECRETS_SECTION = "app"
_SECRETS_KEY = "DRIVE_PANAMACOMPRA_FILE_ID"
_DEFAULT_FILE_ID = "1TQYzsflXlE-5OwYKmTx0bTZ0rD9Ayivs"

_ENV_FICHAS_CTNI = "FINAPP_DRIVE_FICHAS_CTNI_ID"
_SECRET_FICHAS_CTNI = "DRIVE_FICHAS_CTNI_FILE_ID"
_DEFAULT_FICHAS_CTNI_ID = "1glsxTtLd_ZAF1WdLG0uEjLFwmPMOQgxL"
_FICHAS_CTNI_PATH = APP_ROOT / "fichas_ctni.xlsx"

_ENV_CRITERIOS = "FINAPP_DRIVE_CRITERIOS_ID"
_SECRET_CRITERIOS = "DRIVE_CRITERIOS_TECNICOS_FILE_ID"
_DEFAULT_CRITERIOS_ID = "15JoaLJ7Fq8TVob4UXR3kwNbDrXaRE4Vn"
_CRITERIOS_PATH = APP_ROOT / "criterios_tecnicos.xlsx"

_ENV_OFERENTES = "FINAPP_DRIVE_OFERENTES_ID"
_SECRET_OFERENTES = "DRIVE_OFERENTES_CATALOGOS_FILE_ID"
_DEFAULT_OFERENTES_ID = "1slEyEUUDAG8X0Uw94KEB6-WDHX96lFlf"
_OFERENTES_PATH = APP_ROOT / "oferentes_catalogos.xlsx"

_ENV_TOPS_FOLDER = "FINAPP_DRIVE_TOPS_FOLDER_ID"
_SECRET_TOPS_FOLDER = "DRIVE_TOPS_FOLDER_ID"
_DEFAULT_TOPS_FOLDER_ID = "0AMdsC0UugWLkUk9PVA"

def _notify(kind: str, message: str) -> None:
    printed = False
    func = getattr(st, kind, None)
    if callable(func):
        try:
            func(message)
        except Exception:
            pass
        else:
            printed = True
    print(f"[panamacompra-db][{kind.upper()}] {message}")


def _resolve_file_id(env_var: str, secret_key: str, default_value: Optional[str]) -> Optional[str]:
    env_value = os.environ.get(env_var)
    if env_value:
        return env_value.strip()
    try:
        app_conf = st.secrets.get(_SECRETS_SECTION, {})
        secret_value = app_conf.get(secret_key)
        return secret_value.strip() if secret_value else None
    except Exception:
        pass
    return default_value


def _build_drive_client():
    creds = get_service_account_credentials(subject=DOMAIN_USER)
    return build("drive", "v3", credentials=creds)


def _fetch_metadata(drive, file_id: str) -> dict:
    return (
        drive.files()
        .get(
            fileId=file_id,
            fields="id,name,modifiedTime,size",
            supportsAllDrives=True,
        )
        .execute()
    )


def _parse_remote_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _download_file(drive, file_id: str, dest: Path) -> None:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        while True:
            status, done = downloader.next_chunk()
            if done:
                break


def _ensure_drive_file(
    *,
    dest: Path,
    env_var: str,
    secret_key: str,
    default_id: Optional[str],
    label: str,
    force: bool = False,
) -> Optional[Path]:
    """
    Descarga un archivo desde Drive si no existe o está desactualizado.
    Retorna la ruta aun cuando la descarga falle, para que el llamador decida qué hacer.
    """
    dest = Path(dest)
    file_id = _resolve_file_id(env_var, secret_key, default_id)
    if not file_id:
        return dest if dest.exists() else None

    try:
        drive = _build_drive_client()
    except Exception as exc:
        _notify("error", f"No se pudo autenticar contra Drive para '{label}': {exc}")
        return dest if dest.exists() else None

    try:
        metadata = _fetch_metadata(drive, file_id)
    except HttpError as exc:
        _notify("error", f"No se pudo leer la metadata de '{label}' en Drive: {exc}")
        return dest if dest.exists() else None

    remote_ts = _parse_remote_ts(metadata.get("modifiedTime"))
    needs_download = force or not dest.exists()
    if not needs_download and remote_ts:
        local_ts = datetime.fromtimestamp(dest.stat().st_mtime, timezone.utc)
        needs_download = remote_ts > local_ts

    if not needs_download:
        return dest

    try:
        _download_file(drive, file_id, dest)
        if remote_ts:
            ts = remote_ts.timestamp()
            os.utime(dest, (ts, ts))
        _notify("info", f"Descargamos '{label}' desde Google Drive.")
    except Exception as exc:
        _notify("error", f"No se pudo descargar '{label}' desde Drive: {exc}")

    return dest if dest.exists() else None


def ensure_local_panamacompra_db(force: bool = False) -> Optional[Path]:
    return _ensure_drive_file(
        dest=DB_PATH,
        env_var=_ENV_FILE_ID,
        secret_key=_SECRETS_KEY,
        default_id=_DEFAULT_FILE_ID,
        label="panamacompra.db",
        force=force,
    )


def ensure_drive_fichas_ctni(force: bool = False) -> Optional[Path]:
    return _ensure_drive_file(
        dest=_FICHAS_CTNI_PATH,
        env_var=_ENV_FICHAS_CTNI,
        secret_key=_SECRET_FICHAS_CTNI,
        default_id=_DEFAULT_FICHAS_CTNI_ID,
        label="fichas_ctni.xlsx",
        force=force,
    )


def ensure_drive_criterios_tecnicos(force: bool = False) -> Optional[Path]:
    return _ensure_drive_file(
        dest=_CRITERIOS_PATH,
        env_var=_ENV_CRITERIOS,
        secret_key=_SECRET_CRITERIOS,
        default_id=_DEFAULT_CRITERIOS_ID,
        label="criterios_tecnicos.xlsx",
        force=force,
    )


def ensure_drive_oferentes_catalogos(force: bool = False) -> Optional[Path]:
    return _ensure_drive_file(
        dest=_OFERENTES_PATH,
        env_var=_ENV_OFERENTES,
        secret_key=_SECRET_OFERENTES,
        default_id=_DEFAULT_OFERENTES_ID,
        label="oferentes_catalogos.xlsx",
        force=force,
    )


def upload_tops_excel_to_drive(source: Path, *, label: str = "tops_panamacompra.xlsx") -> bool:
    """Sube o reemplaza el Excel de tops en la carpeta de Drive configurada."""
    source = Path(source)
    if not source.exists():
        _notify("error", f"No existe el archivo a subir: {source}")
        return False

    folder_id = _resolve_file_id(_ENV_TOPS_FOLDER, _SECRET_TOPS_FOLDER, _DEFAULT_TOPS_FOLDER_ID)
    if not folder_id:
        _notify("error", "No se configuró FINAPP_DRIVE_TOPS_FOLDER_ID / DRIVE_TOPS_FOLDER_ID.")
        return False

    try:
        drive = _build_drive_client()
    except Exception as exc:
        _notify("error", f"No se pudo autenticar contra Drive para subir '{label}': {exc}")
        return False

    query = (
        f"name = '{source.name}' and '{folder_id}' in parents and trashed = false"
    )
    try:
        result = (
            drive.files()
            .list(
                q=query,
                spaces="drive",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="files(id,name)",
            )
            .execute()
        )
        files = result.get("files", [])
    except HttpError as exc:
        _notify("error", f"No se pudo consultar el archivo '{label}' en Drive: {exc}")
        return False

    media = MediaFileUpload(
        str(source),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=False,
    )

    try:
        if files:
            file_id = files[0]["id"]
            drive.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
            _notify("info", f"Actualizamos '{source.name}' en la carpeta de tops.")
        else:
            metadata = {"name": source.name, "parents": [folder_id]}
            drive.files().create(
                body=metadata,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
            _notify("info", f"Subimos '{source.name}' a la carpeta de tops.")
    except HttpError as exc:
        _notify("error", f"No se pudo subir '{label}' a Drive: {exc}")
        return False

    return True


def ensure_drive_tops_excel(dest: Path, *, force: bool = False) -> Optional[Path]:
    """Descarga el Excel de tops desde la carpeta de Drive configurada."""
    dest = Path(dest)
    folder_id = _resolve_file_id(_ENV_TOPS_FOLDER, _SECRET_TOPS_FOLDER, _DEFAULT_TOPS_FOLDER_ID)
    if not folder_id:
        _notify("warning", "No hay carpeta de Drive configurada para los tops.")
        return dest if dest.exists() else None

    try:
        drive = _build_drive_client()
    except Exception as exc:
        _notify("error", f"No se pudo autenticar contra Drive para descargar tops: {exc}")
        return dest if dest.exists() else None

    query = (
        f"'{folder_id}' in parents and trashed = false and name = '{dest.name}'"
    )
    try:
        result = (
            drive.files()
            .list(
                q=query,
                spaces="drive",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                orderBy="modifiedTime desc",
                fields="files(id,name,modifiedTime)",
            )
            .execute()
        )
        files = result.get("files", [])
    except HttpError as exc:
        _notify("error", f"No se pudo buscar '{dest.name}' en la carpeta de tops: {exc}")
        return dest if dest.exists() else None

    if not files:
        _notify("warning", f"No se encontró '{dest.name}' en la carpeta de tops.")
        return dest if dest.exists() else None

    file_metadata = files[0]
    remote_ts = _parse_remote_ts(file_metadata.get("modifiedTime"))
    needs_download = force or not dest.exists()
    if not needs_download and remote_ts:
        local_ts = datetime.fromtimestamp(dest.stat().st_mtime, timezone.utc)
        needs_download = remote_ts > local_ts

    if not needs_download:
        return dest

    try:
        _download_file(drive, file_metadata["id"], dest)
        if remote_ts:
            ts = remote_ts.timestamp()
            os.utime(dest, (ts, ts))
        _notify("info", f"Descargamos '{dest.name}' desde la carpeta de tops.")
    except Exception as exc:
        _notify("error", f"No se pudo descargar '{dest.name}' desde Drive: {exc}")

    return dest if dest.exists() else None


__all__ = [
    "ensure_local_panamacompra_db",
    "ensure_drive_fichas_ctni",
    "ensure_drive_criterios_tecnicos",
    "ensure_drive_oferentes_catalogos",
    "ensure_drive_tops_excel",
    "upload_tops_excel_to_drive",
]
