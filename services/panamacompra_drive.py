"""Sincroniza la base panamacompra.db desde Google Drive."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from core.config import DB_PATH
from services.auth_drive import DOMAIN_USER, get_service_account_credentials

_ENV_FILE_ID = "FINAPP_DRIVE_DB_FILE_ID"
_SECRETS_SECTION = "app"
_SECRETS_KEY = "DRIVE_PANAMACOMPRA_FILE_ID"


def _notify(kind: str, message: str) -> None:
    func = getattr(st, kind, None)
    if callable(func):
        try:
            func(message)
            return
        except Exception:
            pass
    print(f"[panamacompra-db][{kind.upper()}] {message}")


def _resolve_file_id() -> Optional[str]:
    env_value = os.environ.get(_ENV_FILE_ID)
    if env_value:
        return env_value.strip()
    try:
        app_conf = st.secrets.get(_SECRETS_SECTION, {})
        secret_value = app_conf.get(_SECRETS_KEY)
        return secret_value.strip() if secret_value else None
    except Exception:
        return None


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


def ensure_local_panamacompra_db(force: bool = False) -> Optional[Path]:
    """
    Descarga la base desde Drive si no existe o esta desactualizada.
    Retorna la ruta local (aunque la descarga falle) para que la UI decida que hacer.
    """
    dest = Path(DB_PATH)
    file_id = _resolve_file_id()
    if not file_id:
        return dest if dest.exists() else None

    try:
        drive = _build_drive_client()
    except Exception as exc:
        _notify("error", f"No se pudo autenticar contra Drive: {exc}")
        return dest if dest.exists() else None

    try:
        metadata = _fetch_metadata(drive, file_id)
    except HttpError as exc:
        _notify("error", f"No se pudo leer la metadata de panamacompra.db en Drive: {exc}")
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
        _notify("info", "Descargamos panamacompra.db desde Google Drive.")
    except Exception as exc:
        _notify("error", f"No se pudo descargar panamacompra.db desde Drive: {exc}")

    return dest if dest.exists() else None


__all__ = ["ensure_local_panamacompra_db"]
