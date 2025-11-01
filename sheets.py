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

import os  # 👈 necesario para detectar si el archivo existe

# Ruta local (fallback) a tu JSON. Se usará solo si NO hay secretos en st.secrets.
SERVICE_ACCOUNT_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    r"C:\Users\rodri\ge\finapp\pure-beach-474203-p1-fdc9557f33d0.json"
)

# Scopes (permisos) que usará la cuenta de servicio
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

def get_client():
    """
    Prioriza credenciales desde st.secrets['google_service_account'] (Streamlit Cloud).
    Si no existen, usa el archivo local SERVICE_ACCOUNT_PATH (entorno local).
    """
    creds = None

    # 1) Cloud / .streamlit/secrets.toml
    try:
        if "google_service_account" in st.secrets:
            info = dict(st.secrets["google_service_account"])
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception:
        # si falla secrets, probamos el archivo
        creds = None

    # 2) Local file (fallback)
    if creds is None:
        try:
            path = SERVICE_ACCOUNT_PATH
            if not os.path.isfile(path):
                # último intento: variable de entorno
                path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", SERVICE_ACCOUNT_PATH)
            creds = Credentials.from_service_account_file(path, scopes=SCOPES)
        except Exception as e:
            # re-lanzar con mensaje claro (Streamlit lo ocultará en Cloud, pero sirve localmente)
            raise FileNotFoundError(f"No se pudieron cargar credenciales: {e}")

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

def read_worksheet(client: gspread.Client, sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    # 👇 Usa wrapper con backoff para lecturas
    def _get_all_records():
        return ws.get_all_records()  # datos desde fila 2

    data = _retry(_get_all_records)

    df = pd.DataFrame(data)
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
