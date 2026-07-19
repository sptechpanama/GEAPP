# services/auth_drive.py
"""Helpers para autenticación delegada en Google Drive/Sheets."""

from __future__ import annotations

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

DOMAIN_USER = os.environ.get("FINAPP_DOMAIN_USER", "soporte@sptechpanama.com")


def _load_credentials(subject: str | None = None):
    """Obtiene credenciales de la cuenta de servicio desde secrets o un JSON."""
    json_path = (
        os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    if json_path:
        return service_account.Credentials.from_service_account_file(
            json_path,
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


def _build_drive(credentials):
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def get_drive_service_account():
    """Retorna Drive usando directamente la cuenta de servicio.

    Este modo cubre archivos compartidos con ``client_email`` que no están
    compartidos también con el usuario de Workspace delegado.
    """
    try:
        drive = _build_drive(_load_credentials(subject=None))
        drive.about().get(fields="user").execute()
        return drive
    except Exception as exc:  # pragma: no cover - diagnóstico en Streamlit Cloud
        print("Error en autenticación directa de cuenta de servicio:", exc)
        return None


def get_drive_delegated():
    """Retorna un cliente de Drive actuando como el usuario de dominio."""
    try:
        creds = _load_credentials(subject=DOMAIN_USER)
        drive = _build_drive(creds)
        user = drive.about().get(fields="user").execute().get("user", {}).get("emailAddress")
        if user:
            print("Autenticado como:", user)
        return drive
    except Exception as exc:  # pragma: no cover - logging para soporte remoto
        print("Error en autenticación delegada; se usará la cuenta de servicio:", exc)
        return get_drive_service_account()
