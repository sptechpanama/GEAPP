# ===========================
# finapp/sheets.py
# ===========================

# Importa gspread para conectarte a Google Sheets
import gspread  # Cliente para Google Sheets
# Importa pandas para manipular DataFrames
import pandas as pd  # DataFrame y utilidades
# Creador de credenciales para cuentas de servicio
from google.oauth2.service_account import Credentials  # Credenciales (service account)
# Streamlit para leer secretos desde .streamlit/secrets.toml
import streamlit as st  # st.secrets
import time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import json
import os
from pathlib import Path

# Ruta local (fallback) a tu JSON. Se usará solo si NO hay secretos en st.secrets.
APP_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SA_FILE = APP_ROOT / "pure-beach-474203-p1-fdc9557f33d0.json"

SERVICE_ACCOUNT_PATH = (
    os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE")
    or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    or (str(_DEFAULT_SA_FILE) if _DEFAULT_SA_FILE.exists() else None)
)

# Scopes (permisos) que usará la cuenta de servicio
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


def get_client():
    # 1) Tomar SIEMPRE desde st.secrets
    try:
        info = dict(st.secrets["google_service_account"])
    except Exception:
        if SERVICE_ACCOUNT_PATH and Path(SERVICE_ACCOUNT_PATH).exists():
            with open(SERVICE_ACCOUNT_PATH, "r", encoding="utf-8") as fh:
                info = json.load(fh)
        else:
            keys = ", ".join(list(st.secrets.keys()))
            raise RuntimeError(
                "No se encontró el bloque [google_service_account] en los Secrets "
                f"de ESTE app. Claves disponibles: {keys}. "
                "Sube el JSON en Streamlit Secrets o define FINAPP_SERVICE_ACCOUNT_FILE/" 
                "GOOGLE_APPLICATION_CREDENTIALS apuntando al archivo."
            )

    # 2) Arreglar saltos de línea del private_key si vienen escapados
    pk = info.get("private_key", "")
    if "\\n" in pk and "\n" not in pk:
        info["private_key"] = pk.replace("\\n", "\n")

    # 3) Crear credenciales y autorizar gspread
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client, creds



def _retry(fn, tries=5, base_sleep=0.5):
    """Ejecuta fn() con reintentos exponenciales."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            last = e
            time.sleep(base_sleep * (2 ** i))
    # último intento fuera del bucle
    return fn()

def _make_unique_headers(raw_headers: list[str]) -> list[str]:
    unique, seen = [], {}
    for idx, header in enumerate(raw_headers):
        name = (header or "").strip() or f"col_{idx+1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        unique.append(name)
    return unique


def read_worksheet(client: gspread.Client, sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    raw_headers = ws.row_values(1)
    expected_headers = _make_unique_headers(raw_headers)

    def _get_all_values():
        return ws.get_all_values()

    values = _retry(_get_all_values)

    if not values:
        df = pd.DataFrame(columns=expected_headers)
    else:
        data_rows = values[1:] if len(values) > 1 else []
        width = len(expected_headers)
        padded_rows = [row[:width] + [""] * (width - len(row)) for row in data_rows]
        df = pd.DataFrame(padded_rows, columns=expected_headers)
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    if "Monto" in df.columns:
        df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce")
    return df

def write_worksheet(client: gspread.Client, sheet_id: str, worksheet_name: str, df: pd.DataFrame) -> None:
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    out = df.copy()
    if "Fecha" in out.columns:
        out["Fecha"] = pd.to_datetime(out["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "Monto" in out.columns:
        out["Monto"] = pd.to_numeric(out["Monto"], errors="coerce").fillna(0)

    rows = [list(out.columns)] + out.astype(str).fillna("").values.tolist()

    # 👇 también con retry por si hay cortes al escribir
    _retry(lambda: ws.clear())
    _retry(lambda: ws.update("A1", rows))

# añadir filas sin sobreescribir toda la hoja
def append_rows(client, sheet_id: str, ws_name: str, rows: list[dict]) -> None:
    """
    Lee la pestaña, agrega 'rows' (lista de dicts) y escribe de vuelta.
    Simple y suficiente para volúmenes pequeños/medios.
    """
    df = read_worksheet(client, sheet_id, ws_name)  # ya convierte tipos básicos
    df_new = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    write_worksheet(client, sheet_id, ws_name, df_new)
