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

# Scopes (permisos) que usará la cuenta de servicio
SCOPES = [  # Conjunto de permisos requeridos
    "https://www.googleapis.com/auth/spreadsheets",  # Lectura/escritura de Sheets
    "https://www.googleapis.com/auth/drive",         # Acceso a Drive (abrir por ID)
]


def get_client():
    """
    Crea y devuelve el cliente gspread autenticado + las credenciales.
    Lee el bloque [google_service_account] del secrets.toml.
    """
    try:
        # Lee el bloque de credenciales como dict (secrets es un MappingProxy; lo convertimos a dict normal)
        info = dict(st.secrets["google_service_account"])  # Obtiene credenciales del secrets.toml
        # Crea el objeto Credentials a partir del dict y los SCOPES definidos arriba
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)  # Credenciales válidas
    except Exception:
        # Si falla la lectura de secrets, lanzamos un error claro (puedes habilitar fallback local si lo deseas)
        raise RuntimeError("No fue posible leer [google_service_account] en secrets.toml.")

    # Autoriza un cliente gspread usando las credenciales
    client = gspread.authorize(creds)  # Crea el cliente de Google Sheets

    # Devuelve el cliente y las credenciales (útiles para mostrar correo del SA en UI)
    return client, creds  # (gspread.Client, Credentials)


def read_worksheet(client: gspread.Client, sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    """
    Lee una Worksheet por nombre dentro de un Spreadsheet por ID y devuelve un DataFrame.
    Normaliza tipos en 'Fecha' (datetime) y 'Monto' (numérico).
    """
    # Abre el Spreadsheet por su ID
    sh = client.open_by_key(sheet_id)  # Archivo remoto
    # Selecciona la hoja por nombre
    ws = sh.worksheet(worksheet_name)  # Pestaña concreta
    # Descarga todas las filas como lista de dicts (encabezados = fila 1)
    data = ws.get_all_records()  # Datos desde fila 2
    # Convierte la lista de dicts a DataFrame
    df = pd.DataFrame(data)  # Estructura tabular

    # Si existe 'Fecha', conviértela a datetime
    if "Fecha" in df.columns:  # Validación de existencia
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")  # Convierte a fechas

    # Si existe 'Monto', conviértela a numérico
    if "Monto" in df.columns:  # Validación de existencia
        df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce")  # Convierte a float

    # Devuelve el DataFrame limpio
    return df  # DF normalizado


def write_worksheet(client: gspread.Client, sheet_id: str, worksheet_name: str, df: pd.DataFrame) -> None:
    """
    Sobrescribe COMPLETAMENTE la worksheet indicada con el DataFrame dado.
    Normaliza antes: Fecha -> 'YYYY-MM-DD', Monto -> numérico (NaN=0).
    """
    # Abre el Spreadsheet por ID
    sh = client.open_by_key(sheet_id)  # Archivo remoto
    # Selecciona la hoja por nombre
    ws = sh.worksheet(worksheet_name)  # Pestaña destino

    # Crea copia defensiva del DF
    out = df.copy()  # No alteramos el original

    # Normaliza 'Fecha' si existe
    if "Fecha" in out.columns:  # Chequea columna
        out["Fecha"] = pd.to_datetime(out["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")  # Formato ISO

    # Normaliza 'Monto' si existe
    if "Monto" in out.columns:  # Chequea columna
        out["Monto"] = pd.to_numeric(out["Monto"], errors="coerce").fillna(0)  # Asegura numérico

    # Prepara filas a subir (headers + datos) como lista de listas
    rows = [list(out.columns)] + out.astype(str).fillna("").values.tolist()  # Formato gspread

    # Limpia la hoja y sube el bloque completo empezando en A1
    ws.clear()        # Borra contenido anterior
    ws.update("A1", rows)  # Inserta todo desde la celda A1


# añadir filas sin sobreescribir toda la hoja

def append_rows(client, sheet_id: str, ws_name: str, rows: list[dict]) -> None:
    """
    Lee la pestaña, agrega 'rows' (lista de dicts) y escribe de vuelta.
    Simple y suficiente para volúmenes pequeños/medios.
    """
    df = read_worksheet(client, sheet_id, ws_name)  # ya convierte tipos básicos
    df_new = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    write_worksheet(client, sheet_id, ws_name, df_new)

