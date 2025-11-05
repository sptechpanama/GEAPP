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
from gspread.exceptions import APIError

import json
import os
from pathlib import Path

# Ruta local (fallback) a tu JSON. Se usará solo si NO hay secretos en st.secrets.
APP_ROOT = Path(__file__).resolve().parents[1]

def _first_service_account_file() -> Path | None:
    candidates = list(APP_ROOT.glob("pure-beach-*.json"))
    if not candidates:
        # Permite reutilizar el archivo del proyecto local anterior si existe.
        legacy_root = APP_ROOT.parent / "ge"
        if legacy_root.exists():
            candidates = list(legacy_root.glob("pure-beach-*.json"))
    return candidates[0] if candidates else None

_DEFAULT_SA_FILE = _first_service_account_file()
_DEFAULT_SA_PATH = str(_DEFAULT_SA_FILE) if (_DEFAULT_SA_FILE and _DEFAULT_SA_FILE.exists()) else None

SERVICE_ACCOUNT_PATH = (
    os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE")
    or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    or _DEFAULT_SA_PATH
)

# Scopes (permisos) que usará la cuenta de servicio
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


def get_client():
    # 1) Intentar credenciales desde st.secrets
    info = None
    try:
        info = dict(st.secrets["google_service_account"])
    except Exception:
        pass

    # 2) Fallback a archivo local
    if info is None:
        path = SERVICE_ACCOUNT_PATH
        if path and Path(path).exists():
            with open(path, "r", encoding="utf-8") as fh:
                info = json.load(fh)
        else:
            raise RuntimeError(
                "No encontramos credenciales de Google Sheets. "
                "Configura st.secrets['google_service_account'] o la variable FINAPP_SERVICE_ACCOUNT_FILE." 
            )

    # 2) Arreglar saltos de línea del private_key si vienen escapados
    pk = info.get("private_key", "")
    if "\\n" in pk and "\n" not in pk:
        info["private_key"] = pk.replace("\\n", "\n")

    # 3) Crear credenciales y autorizar gspread
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client, creds



def _retry(fn, tries=5, base_sleep=0.5, max_sleep=8.0):
    """Ejecuta fn() con reintentos exponenciales y manejo básico de cuotas."""
    last = None
    for attempt in range(tries):
        try:
            return fn()
        except APIError as api_err:
            response = getattr(api_err, "response", None)
            status = getattr(response, "status_code", None)
            if status in (429, 500, 503):
                retry_after = None
                if response is not None:
                    try:
                        retry_after = float(response.headers.get("Retry-After", ""))
                    except (TypeError, ValueError):
                        retry_after = None
                delay = retry_after if retry_after else min(max_sleep, base_sleep * (2 ** attempt))
                time.sleep(delay)
                last = api_err
                continue
            last = api_err
            break
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as exc:
            last = exc
            delay = min(max_sleep, base_sleep * (2 ** attempt))
            time.sleep(delay)
    if last:
        raise last
    raise RuntimeError("Retry loop exited without calling function")

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
