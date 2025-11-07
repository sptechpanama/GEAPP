"""Sincroniza archivos críticos desde Google Drive al entorno local."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from core.config import (
	DB_PATH,
	DRIVE_TODAS_FICHAS_PATH,
	DRIVE_OFERENTES_ACTIVOS_PATH,
)
from services.auth_drive import DOMAIN_USER, get_service_account_credentials


_SECRETS_SECTION = "app"


@dataclass(frozen=True)
class DriveAsset:
	name: str
	dest: Path
	env_var: str
	secret_key: str
	default_file_id: Optional[str] = None


PANAMACOMPRA_DB_ASSET = DriveAsset(
	name="panamacompra.db",
	dest=DB_PATH,
	env_var="FINAPP_DRIVE_DB_FILE_ID",
	secret_key="DRIVE_PANAMACOMPRA_FILE_ID",
	default_file_id="1TQYzsflXlE-5OwYKmTx0bTZ0rD9Ayivs",
)

TODAS_LAS_FICHAS_ASSET = DriveAsset(
	name="todas_las_fichas.xlsx",
	dest=DRIVE_TODAS_FICHAS_PATH,
	env_var="FINAPP_DRIVE_TODAS_FICHAS_ID",
	secret_key="DRIVE_TODAS_LAS_FICHAS_FILE_ID",
	default_file_id="1AxQPm7koNkgV1txDdWpMA9SK2CfyY23Z",
)

OFERENTES_ACTIVOS_ASSET = DriveAsset(
	name="oferentes_activos.xlsx",
	dest=DRIVE_OFERENTES_ACTIVOS_PATH,
	env_var="FINAPP_DRIVE_OFERENTES_ID",
	secret_key="DRIVE_OFERENTES_ACTIVOS_FILE_ID",
	default_file_id="18thVq_8AqQ7BvnRd3V5sYFFNccXWWj7E",
)


def _notify(kind: str, message: str) -> None:
	func = getattr(st, kind, None)
	if callable(func):
		try:
			func(message)
			return
		except Exception:
			pass
	print(f"[drive-assets][{kind.upper()}] {message}")


def _resolve_file_id(asset: DriveAsset) -> Optional[str]:
	env_value = os.environ.get(asset.env_var)
	if env_value:
		return env_value.strip()
	try:
		app_conf = st.secrets.get(_SECRETS_SECTION, {})
		secret_value = app_conf.get(asset.secret_key)
		if secret_value:
			return str(secret_value).strip()
	except Exception:
		pass
	return asset.default_file_id


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


def ensure_drive_asset(asset: DriveAsset, force: bool = False) -> Optional[Path]:
	dest = Path(asset.dest)
	file_id = _resolve_file_id(asset)
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
		_notify("error", f"No se pudo consultar '{asset.name}' en Drive: {exc}")
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
		_notify("info", f"Descargamos '{asset.name}' desde Google Drive.")
	except Exception as exc:
		_notify("error", f"No se pudo descargar '{asset.name}' desde Drive: {exc}")

	return dest if dest.exists() else None


def ensure_local_panamacompra_db(force: bool = False) -> Optional[Path]:
	return ensure_drive_asset(PANAMACOMPRA_DB_ASSET, force)


def ensure_drive_todas_las_fichas(force: bool = False) -> Optional[Path]:
	return ensure_drive_asset(TODAS_LAS_FICHAS_ASSET, force)


def ensure_drive_oferentes_activos(force: bool = False) -> Optional[Path]:
	return ensure_drive_asset(OFERENTES_ACTIVOS_ASSET, force)


__all__ = [
	"DriveAsset",
	"ensure_drive_asset",
	"ensure_local_panamacompra_db",
	"ensure_drive_todas_las_fichas",
	"ensure_drive_oferentes_activos",
]
