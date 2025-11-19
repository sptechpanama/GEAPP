# services/auth_drive.py
"""Helpers para autenticación delegada en Google Drive/Sheets."""

from __future__ import annotations

import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

from core.config import APP_ROOT

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

DOMAIN_USER = os.environ.get("FINAPP_DOMAIN_USER", "soporte@sptechpanama.com")

_CREDENTIAL_HINTS = [
    APP_ROOT / "credentials" / "service-account.json",
    APP_ROOT.parent / "scrapers_repo" / "credentials" / "service-account.json",
    Path.home() / "scrapers_repo" / "credentials" / "service-account.json",
]


def _load_credentials(subject: str | None = None):
    """Obtiene credenciales de la cuenta de servicio desde secrets o un JSON."""
    json_path = (
        os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    candidate_paths = []
    if json_path:
        candidate_paths.append(Path(json_path).expanduser())
    candidate_paths.extend(_CREDENTIAL_HINTS)
    for candidate in candidate_paths:
        if candidate and Path(candidate).exists():
            return service_account.Credentials.from_service_account_file(
                str(candidate),
                scopes=SCOPES,
                subject=subject,
            )

    try:  # Streamlit Cloud espera secrets vía st.secrets
        import streamlit as st  # type: ignore

        info = dict(st.secrets["google_service_account"])
    except Exception as exc:  # pragma: no cover - logging para diagnóstico
        raise RuntimeError(
            "No se encontró configuración de cuenta de servicio. "
            "Define FINAPP_SERVICE_ACCOUNT_FILE / GOOGLE_APPLICATION_CREDENTIALS "
            "o añade el bloque [google_service_account] en secrets."
        ) from exc

    private_key = info.get("private_key", "")
    if "\\n" in private_key and "\n" not in private_key:
        info["private_key"] = private_key.replace("\\n", "\n")

    return service_account.Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
        subject=subject,
    )


def get_service_account_credentials(subject: str | None = None):
    """Expone las credenciales de la cuenta de servicio para otros módulos."""
    return _load_credentials(subject=subject)


def get_drive_delegated():
    """Retorna un cliente de Drive actuando como el usuario de dominio."""
    try:
        creds = get_service_account_credentials(subject=DOMAIN_USER)
        drive = build("drive", "v3", credentials=creds)
        user = drive.about().get(fields="user").execute().get("user", {}).get("emailAddress")
        if user:
            print("Autenticado como:", user)
        return drive
    except Exception as e:  # pragma: no cover - logging para support remoto
        print("⚠️ Error en autenticación delegada:", e)
        return None
