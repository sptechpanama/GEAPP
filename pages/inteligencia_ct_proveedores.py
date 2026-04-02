from __future__ import annotations

import json
import re
import sqlite3
import math
import uuid
import time
from collections.abc import Mapping
from datetime import date, datetime
import io
import os
import unicodedata
from html import unescape
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import streamlit_authenticator as stauth
import bcrypt
import requests
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine

from core.config import APP_ROOT, DB_PATH
from services.auth_drive import get_drive_delegated
from ui.theme import apply_global_theme


st.set_page_config(
    page_title="Inteligencia CT y Proveedores",
    page_icon="🧠",
    layout="wide",
)
apply_global_theme()


# Guard de autenticacion (mismo patron que otras paginas)
USERS = {
    "rsanchez": ("Rodrigo Sánchez", "Sptech-71"),
    "isanchez": ("Irvin Sánchez", "Sptech-71"),
    "igsanchez": ("Iris Grisel Sánchez", "Sptech-71"),
}


def _hash(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


@st.cache_data(show_spinner=False, ttl=86400)
def _hash_for_auth_cached(pw: str) -> str:
    # Evita recalcular bcrypt en cada rerun (costoso) sin cambiar la logica de auth.
    return _hash(pw)


credentials = {
    "usernames": {u: {"name": n, "password": _hash_for_auth_cached(p)} for u, (n, p) in USERS.items()}
}
COOKIE_NAME = "finapp_auth"
COOKIE_KEY = "finapp_key_123"
authenticator = stauth.Authenticate(credentials, COOKIE_NAME, COOKIE_KEY, 30)

try:
    authenticator.login(" ", location="sidebar", key="auth_intel_ct_silent")
    st.sidebar.empty()
except Exception:
    pass

if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")

authenticator.logout("Cerrar sesión", location="sidebar")




FICHA_TOKEN_RE = re.compile(r"\b\d{3,8}\*?\b")
FALLBACK_DB_PATH = Path(r"C:\Users\rodri\OneDrive\cl\panamacompra.db")
CTNI_CONSULTA_URL = "https://ctni.minsa.gob.pa/Home/ConsultarFichas"
CTNI_LOAD_FICHAS_URL = "https://ctni.minsa.gob.pa/Home/LoadFichas"
PANAMACOMPRA_BASE_URL = "https://www.panamacompra.gob.pa/Inicio/"
INTEL_WEIGHTS_PROFILE_VERSION = "defaults_38_32_12_10_8_v1"
INTEL_STUDY_DB_PATH = APP_ROOT / "data" / "inteligencia_ct_estudios.db"
INTEL_TRACKING_WORKSHEET = "ct_fichas_seguimiento"
INTEL_REMOTE_RUNS_WORKSHEET = "intel_study_runs_remote"
INTEL_REMOTE_DETAIL_WORKSHEET = "intel_study_detail_remote"
PC_MANUAL_WORKSHEET = "pc_manual"
PC_CONFIG_WORKSHEET = "pc_config"
PC_SHEET_ID_DEFAULT = "1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
PC_CONFIG_HEADERS = ["name", "python", "script", "days", "times", "enabled"]
PC_MANUAL_HEADERS = [
    "id",
    "job",
    "requested_by",
    "requested_at",
    "status",
    "notes",
    "payload",
    "result_file_id",
    "result_file_url",
    "result_file_name",
    "result_error",
]
INTEL_ORQ_JOB_NAME = "intel_estudio_ficha"
INTEL_ORQ_JOB_PY = r"C:\Users\rodri\scrapers_repo\.venv\Scripts\python.exe"
INTEL_ORQ_JOB_SCRIPT = r"C:\Users\rodri\scrapers_repo\orquestador\intel_ficha_worker.py"
INTEL_TRACKING_COLUMNS = [
    "ficha",
    "nombre_ficha",
    "clase_riesgo",
    "enlace_minsa",
    "score_inicial",
    "clasificacion",
    "actos",
    "actos_solo_ficha",
    "actos_con_otras_fichas",
    "monto_historico",
    "proponentes_promedio",
    "revision_proponentes",
    "top1_ganador",
    "top1_pct_ganadas",
    "top2_ganador",
    "top2_pct_ganadas",
    "top3_ganador",
    "top3_pct_ganadas",
    "estado",
    "fecha_ingreso",
    "notas",
    "created_at",
    "updated_at",
]
INTEL_BOOL_TRUE = {"1", "true", "si", "sí", "yes", "y", "t", "x", "on"}

RUN_STATUS_PENDING = "pendiente_consultas"
RUN_STATUS_COMPLETED = "completada"
RUN_STATUS_COMPLETED_OBS = "completada_con_observaciones"
RUN_STATUS_UPDATED = "actualizada"
INTEL_STUDY_DEFAULT_MAX_QUERIES = 12
INTEL_STUDY_MAX_QUERIES_HARD = 40
INTEL_STUDY_SQL_CACHE_TTL = 120

AP_STATE_STUDIED_NO_ANALYSIS = "estudiada_sin_analisis_proveedores"
AP_STATE_PENDING_JSON = "pendiente_json_proveedores"
AP_STATE_COMPLETED = "analisis_proveedores_completado"
AP_STATE_UPDATED = "analisis_proveedores_actualizado"

AP_HIST_COLUMNS = [
    "proveedor",
    "marca",
    "modelo",
    "pais_origen",
    "cantidad_actos_ganados",
    "precio_promedio_historico",
    "precio_minimo_historico",
    "precio_maximo_historico",
    "telefono",
    "contacto_email",
    "contacto_whatsapp",
    "canal_contacto_mas_probable",
    "correo_inicial_listo",
    "whatsapp_inicial_listo",
    "observaciones",
]
AP_GAMA_COLUMNS = [
    "proveedor_o_fabricante",
    "marca",
    "modelo",
    "pais_origen",
    "sitio_web",
    "telefono",
    "contacto_email",
    "contacto_whatsapp",
    "canal_contacto_mas_probable",
    "razon_clasificacion",
    "correo_inicial_listo",
    "whatsapp_inicial_listo",
    "observaciones",
]
AP_PRECIO_COLUMNS = [
    "proveedor_o_fabricante",
    "marca",
    "modelo",
    "pais_origen",
    "sitio_web",
    "telefono",
    "contacto_email",
    "contacto_whatsapp",
    "canal_contacto_mas_probable",
    "rango_precio_referencial",
    "razon_clasificacion",
    "correo_inicial_listo",
    "whatsapp_inicial_listo",
    "observaciones",
]

INTEL_STUDY_SHEETS_TABLE_MAP: dict[str, str] = {
    "estudio_runs": "intel_estudio_runs",
    "estudio_detalle": "intel_estudio_detalle",
    "estudio_consultas": "intel_estudio_consultas",
    "estudio_resumen_ficha": "intel_estudio_resumen",
    "analisis_proveedores_contexto": "intel_ap_contexto",
    "analisis_proveedores_version": "intel_ap_version",
    "analisis_proveedores_hist_panama": "intel_ap_hist_panama",
    "analisis_proveedores_mejor_gama": "intel_ap_mejor_gama",
    "analisis_proveedores_mejor_precio": "intel_ap_mejor_precio",
    "analisis_proveedores_comentarios": "intel_ap_comentarios",
}


def _normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _parse_number(value: object) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace("$", "").replace("USD", "").replace("us$", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "null", "n/a", "<na>"}:
        return ""
    return text


def _canonical_party_name(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    text = re.sub(r"^[\*\#\-\s]+", "", text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.upper()
    text = text.replace("&", " Y ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""

    tokens = text.split(" ")
    merged: list[str] = []
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        if token == "S" and idx + 1 < len(tokens) and tokens[idx + 1] == "A":
            merged.append("SA")
            idx += 2
            continue
        merged.append(token)
        idx += 1
    return " ".join(merged).strip()


def _normalize_ficha_identifier(value: object) -> str:
    """
    Normaliza el identificador de ficha evitando el error comun de convertir
    '43358.0' en '433580' al remover no-digitos.
    """
    if value is None:
        return ""

    # Casos numericos puros.
    try:
        if isinstance(value, int):
            return str(int(value))
        if isinstance(value, float):
            if math.isnan(value) or not math.isfinite(value):
                return ""
            int_val = int(value)
            if abs(value - int_val) < 1e-9:
                return str(int_val)
            value = f"{value:.15g}"
    except Exception:
        pass

    text = _clean_text(value)
    if not text:
        return ""

    # Patron excel / csv: 43358.0 -> 43358
    m = re.fullmatch(r"\s*(\d+)(?:\.0+)\s*", text)
    if m:
        return m.group(1)

    compact = text.replace(",", "").strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", compact):
        try:
            num = float(compact)
            if math.isfinite(num):
                int_val = int(num)
                if abs(num - int_val) < 1e-9:
                    return str(int_val)
        except Exception:
            pass

    tokens = FICHA_TOKEN_RE.findall(text)
    if tokens:
        return re.sub(r"\D", "", tokens[0])

    return re.sub(r"\D", "", text)


def _compute_proponentes_por_acto(df: pd.DataFrame) -> pd.Series:
    proponent_cols = [
        col for col in df.columns if re.fullmatch(r"Proponente\s+\d+", str(col).strip(), flags=re.IGNORECASE)
    ]
    if not proponent_cols:
        return pd.Series([0] * len(df), index=df.index, dtype="int64")

    # Cache local para evitar normalizar miles de veces los mismos nombres.
    norm_cache: dict[str, str] = {}

    def _norm_cached(raw_value: object) -> str:
        raw = _clean_text(raw_value)
        if not raw:
            return ""
        if raw in norm_cache:
            return norm_cache[raw]
        normalized = _canonical_party_name(raw)
        norm_cache[raw] = normalized
        return normalized

    counts: list[int] = []
    for row_values in df[proponent_cols].itertuples(index=False, name=None):
        seen: set[str] = set()
        for value in row_values:
            normalized = _norm_cached(value)
            if normalized:
                seen.add(normalized)
        counts.append(len(seen))

    return pd.Series(counts, index=df.index, dtype="int64")


def _normalize_column_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_any_date(value: object) -> pd.Timestamp:
    text = _clean_text(value)
    if not text:
        return pd.NaT
    try:
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            parsed = pd.to_datetime(text, errors="coerce")
        return parsed
    except Exception:
        return pd.NaT


def _normalize_minsa_link(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""

    url_match = re.search(r"https?://[^\s\"'<>]+", text, flags=re.IGNORECASE)
    if url_match:
        return url_match.group(0).rstrip(".,);")

    path_match = re.search(
        r"/Utilities/LoadFicha/\?idficha=\d+[^\s\"'<>]*",
        text,
        flags=re.IGNORECASE,
    )
    if path_match:
        path = path_match.group(0)
        return f"https://ctni.minsa.gob.pa{path}"

    id_match = re.search(r"idficha\s*=\s*(\d+)", text, flags=re.IGNORECASE)
    if id_match:
        return f"https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha={id_match.group(1)}&idparam=0"

    print_match = re.search(r"print\s*\(\s*(\d+)\s*\)", text, flags=re.IGNORECASE)
    if print_match:
        return f"https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha={print_match.group(1)}&idparam=0"

    id_match_2 = re.search(r"IdFicha\s*=\s*(\d+)", text, flags=re.IGNORECASE)
    if id_match_2:
        return f"https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha={id_match_2.group(1)}&idparam=0"

    return ""


def _safe_minsa_link(value: object) -> str:
    direct = _normalize_minsa_link(value)
    if direct:
        return direct
    # Fallback seguro: lleva al portal oficial de consulta (sin idficha inválido).
    return CTNI_CONSULTA_URL


def _get_study_data_rev() -> int:
    return int(st.session_state.get("intel_study_data_rev", 0) or 0)


def _bump_study_data_rev() -> None:
    st.session_state["intel_study_data_rev"] = _get_study_data_rev() + 1


@st.cache_data(show_spinner=False, ttl=43200)
def _load_ctni_num_to_id_map() -> dict[str, str]:
    out: dict[str, str] = {}
    start = 0
    length = 1000
    draw = 1
    max_pages = 40
    pages = 0

    while pages < max_pages:
        payload = {
            "draw": str(draw),
            "start": str(start),
            "length": str(length),
            "All": "1",
            "IdSubComite": "0",
            "IdSubGrupo": "0",
            "IdTipoProducto": "0",
            "Especialidad": "0",
            "IdCriterio": "0",
            "Filtro": "",
        }
        try:
            response = requests.post(CTNI_LOAD_FICHAS_URL, data=payload, timeout=30)
            response.raise_for_status()
            body = response.json()
        except Exception:
            break

        data = body.get("data", []) or []

        for row in data:
            ficha_num = re.sub(r"\D", "", str((row or {}).get("numFicha", "") or ""))
            id_ficha = str((row or {}).get("id", "") or "").strip()
            if ficha_num and id_ficha and ficha_num not in out:
                out[ficha_num] = id_ficha

        pages += 1
        start += length
        draw += 1
        if not data:
            break
        if len(data) < length:
            break

    return out


def _is_specific_minsa_link(url: str) -> bool:
    text = str(url or "").strip()
    if not text:
        return False
    return bool(re.search(r"/Utilities/LoadFicha/\?idficha=\d+", text, flags=re.IGNORECASE))


def _specific_minsa_link(ficha_value: object, raw_link: object, num_to_id_map: dict[str, str]) -> str:
    direct = _normalize_minsa_link(raw_link)
    if _is_specific_minsa_link(direct):
        return direct
    ficha_num = re.sub(r"\D", "", str(ficha_value or ""))
    if ficha_num:
        id_ficha = str((num_to_id_map or {}).get(ficha_num, "") or "").strip()
        if id_ficha:
            return f"https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha={id_ficha}&idparam=0"
    return _safe_minsa_link(direct)


def _resolve_column_by_alias(columns: list[str], aliases: list[str]) -> str:
    if not columns:
        return ""
    normalized = {_normalize_column_key(col): col for col in columns}
    for alias in aliases:
        hit = normalized.get(_normalize_column_key(alias))
        if hit:
            return hit
    for alias in aliases:
        alias_norm = _normalize_column_key(alias)
        if not alias_norm:
            continue
        for norm_col, real_col in normalized.items():
            if alias_norm in norm_col:
                return real_col
    return ""


def _is_registro_sanitario_required(value: object) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    norm = _normalize_column_key(raw)
    if not norm:
        return False
    negative_markers = [
        "no",
        "no aplica",
        "no requiere",
        "sin rs",
        "sin registro sanitario",
        "no rs",
    ]
    for marker in negative_markers:
        if norm == marker or norm.startswith(marker + " "):
            return False

    positive_markers = [
        "si",
        "sí",
        "requiere",
        "con rs",
        "registro sanitario",
        "rs lcrsp",
        "lcrsp",
    ]
    return any(marker in norm for marker in positive_markers)


def _candidate_fichas_paths() -> list[Path]:
    raw_candidates = [
        APP_ROOT / "fichas_ctni_con_enlace.xlsx",
        APP_ROOT / "fichas_ctni.xlsx",
        APP_ROOT / "data" / "fichas_ctni_con_enlace.xlsx",
        APP_ROOT / "data" / "fichas_ctni.xlsx",
        Path.cwd() / "fichas_ctni_con_enlace.xlsx",
        Path.cwd() / "fichas_ctni.xlsx",
        Path.cwd() / "data" / "fichas_ctni_con_enlace.xlsx",
        Path.cwd() / "data" / "fichas_ctni.xlsx",
    ]
    unique: list[Path] = []
    for path in raw_candidates:
        try:
            normalized = path.expanduser().resolve()
        except Exception:
            normalized = path.expanduser()
        if normalized not in unique:
            unique.append(normalized)
    return unique


def _candidate_criterios_paths() -> list[Path]:
    raw_candidates = [
        APP_ROOT / "criterios_tecnicos.xlsx",
        APP_ROOT / "data" / "criterios_tecnicos.xlsx",
        Path.cwd() / "criterios_tecnicos.xlsx",
        Path.cwd() / "data" / "criterios_tecnicos.xlsx",
    ]
    unique: list[Path] = []
    for path in raw_candidates:
        try:
            normalized = path.expanduser().resolve()
        except Exception:
            normalized = path.expanduser()
        if normalized not in unique:
            unique.append(normalized)
    return unique


def _candidate_oferentes_catalogos_paths() -> list[Path]:
    raw_candidates = [
        APP_ROOT / "oferentes_catalogos.xlsx",
        APP_ROOT / "oferentes_catalogos.csv",
        APP_ROOT / "oferentes_Catálogos.xlsx",
        APP_ROOT / "data" / "oferentes_catalogos.xlsx",
        APP_ROOT / "data" / "oferentes_catalogos.csv",
        APP_ROOT / "data" / "oferentes_Catálogos.xlsx",
        Path.cwd() / "oferentes_catalogos.xlsx",
        Path.cwd() / "oferentes_catalogos.csv",
        Path.cwd() / "oferentes_Catálogos.xlsx",
        Path.cwd() / "data" / "oferentes_catalogos.xlsx",
        Path.cwd() / "data" / "oferentes_catalogos.csv",
        Path.cwd() / "data" / "oferentes_Catálogos.xlsx",
        Path(r"C:\Users\rodri\scrapers_repo\minsa_scraper\outputs\oferentes_catalogos.xlsx"),
        Path(r"C:\Users\rodri\scrapers_repo\minsa_scraper\outputs\oferentes_catalogos.csv"),
        Path(r"C:\Users\rodri\scrapers_repo\minsa_scraper\outputs\oferentes_Catálogos.xlsx"),
    ]
    unique: list[Path] = []
    for path in raw_candidates:
        try:
            normalized = path.expanduser().resolve()
        except Exception:
            normalized = path.expanduser()
        if normalized not in unique:
            unique.append(normalized)
    return unique


def _candidate_db_paths() -> list[Path]:
    raw_candidates = [
        Path(DB_PATH),
        APP_ROOT / "panamacompra.db",
        APP_ROOT / "data" / "panamacompra.db",
        APP_ROOT / "data" / "db" / "panamacompra_drive.db",
        Path.cwd() / "panamacompra.db",
        Path.cwd() / "data" / "panamacompra.db",
        Path.cwd() / "data" / "db" / "panamacompra_drive.db",
        FALLBACK_DB_PATH,
    ]
    unique: list[Path] = []
    for path in raw_candidates:
        try:
            normalized = path.expanduser().resolve()
        except Exception:
            normalized = path.expanduser()
        if normalized not in unique:
            unique.append(normalized)
    return unique


def _panamacompra_drive_file_id() -> str:
    try:
        app_cfg = st.secrets.get("app", {})
    except Exception:
        app_cfg = {}
    for key in (
        "DRIVE_PANAMACOMPRA_FILE_ID",
        "DRIVE_PANAMACOMPRA_DB_FILE_ID",
        "DRIVE_DB_PANAMACOMPRA_FILE_ID",
    ):
        value = app_cfg.get(key) if isinstance(app_cfg, dict) else None
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    try:
        for key in (
            "DRIVE_PANAMACOMPRA_FILE_ID",
            "DRIVE_PANAMACOMPRA_DB_FILE_ID",
            "DRIVE_DB_PANAMACOMPRA_FILE_ID",
        ):
            value = st.secrets.get(key)
            if value and str(value).strip():
                return _normalize_drive_file_id(str(value).strip())
    except Exception:
        pass
    for key in (
        "DRIVE_PANAMACOMPRA_FILE_ID",
        "DRIVE_PANAMACOMPRA_DB_FILE_ID",
        "DRIVE_DB_PANAMACOMPRA_FILE_ID",
    ):
        value = os.environ.get(key)
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    return ""


def _oferentes_catalogos_drive_file_id() -> str:
    try:
        app_cfg = st.secrets.get("app", {})
    except Exception:
        app_cfg = {}
    for key in (
        "DRIVE_OFERENTES_CATALOGOS_FILE_ID",
        "DRIVE_OFERENTES_CATALOGO_FILE_ID",
        "DRIVE_OFERENTES_FILE_ID",
    ):
        value = app_cfg.get(key) if isinstance(app_cfg, dict) else None
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    for key in (
        "DRIVE_OFERENTES_CATALOGOS_FILE_ID",
        "DRIVE_OFERENTES_CATALOGO_FILE_ID",
        "DRIVE_OFERENTES_FILE_ID",
    ):
        value = os.environ.get(key)
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    return ""


def _normalize_drive_file_id(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        return value

    parsed = urlparse(value)
    qs = parse_qs(parsed.query)
    if "id" in qs and qs["id"]:
        return qs["id"][0]

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", parsed.path)
    if match:
        return match.group(1)
    return value


def _find_panamacompra_db_file_id_in_drive() -> str:
    try:
        drive, mode = _get_drive_client()
        if drive is None:
            st.session_state["intel_db_status"] = f"Drive no disponible para busqueda por nombre ({mode})."
            return ""
        response = (
            drive.files()
            .list(
                q="name='panamacompra.db' and trashed=false",
                fields="files(id,name,modifiedTime,parents)",
                pageSize=5,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", []) or []
        if not files:
            return ""
        # Toma el primer match (el API suele devolver primero el más relevante/reciente).
        return str(files[0].get("id", "")).strip()
    except Exception:
        return ""


def _get_drive_client() -> tuple[object | None, str]:
    try:
        drive = get_drive_delegated()
        if drive is not None:
            return drive, "delegated"
    except Exception:
        pass

    # fallback: direct service account (without domain delegation)
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    json_path = os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    try:
        if json_path:
            creds = service_account.Credentials.from_service_account_file(json_path, scopes=scopes)
        else:
            info = dict(st.secrets["google_service_account"])
            private_key = info.get("private_key", "")
            if "\\n" in private_key and "\n" not in private_key:
                info["private_key"] = private_key.replace("\\n", "\n")
            creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        drive = build("drive", "v3", credentials=creds)
        return drive, "service_account"
    except Exception as exc:
        return None, f"auth_error:{exc}"


def _download_panamacompra_db_from_drive(file_id: str) -> tuple[bytes | None, str]:
    try:
        drive, mode = _get_drive_client()
        if drive is None:
            return None, mode
        request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        stream = io.BytesIO()
        downloader = MediaIoBaseDownload(stream, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return stream.getvalue(), mode
    except Exception as exc:
        return None, f"download_error:{exc}"


def _fichas_drive_file_id() -> str:
    try:
        app_cfg = st.secrets.get("app", {})
    except Exception:
        app_cfg = {}
    for key in (
        "DRIVE_FICHAS_CTNI_CON_ENLACE_FILE_ID",
        "DRIVE_FICHAS_CON_ENLACE_FILE_ID",
        "DRIVE_FICHAS_CTNI_FILE_ID",
        "DRIVE_FICHAS_TECNICAS_FILE_ID",
    ):
        value = app_cfg.get(key) if isinstance(app_cfg, dict) else None
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    for key in (
        "DRIVE_FICHAS_CTNI_CON_ENLACE_FILE_ID",
        "DRIVE_FICHAS_CON_ENLACE_FILE_ID",
        "DRIVE_FICHAS_CTNI_FILE_ID",
        "DRIVE_FICHAS_TECNICAS_FILE_ID",
    ):
        value = os.environ.get(key)
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    return ""


def _download_drive_file_bytes(file_id: str) -> tuple[bytes | None, str]:
    try:
        drive, mode = _get_drive_client()
        if drive is None:
            return None, mode
        request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        stream = io.BytesIO()
        downloader = MediaIoBaseDownload(stream, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return stream.getvalue(), mode
    except Exception as exc:
        return None, f"download_error:{exc}"


def _find_drive_file_id_by_names(names: list[str]) -> str:
    target_names = [str(name or "").strip() for name in names if str(name or "").strip()]
    if not target_names:
        return ""
    try:
        drive, _ = _get_drive_client()
        if drive is None:
            return ""
        escaped = [name.replace("'", "\\'") for name in target_names]
        query = "trashed=false and (" + " or ".join([f"name='{n}'" for n in escaped]) + ")"
        response = (
            drive.files()
            .list(
                q=query,
                fields="files(id,name,modifiedTime)",
                pageSize=10,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                corpora="allDrives",
            )
            .execute()
        )
        files = response.get("files", []) or []
        if not files:
            return ""
        files.sort(key=lambda item: str(item.get("modifiedTime", "")), reverse=True)
        return str(files[0].get("id", "")).strip()
    except Exception:
        return ""


def _table_exists_sqlite(db_path: Path, table_name: str) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)=lower(?)",
                (table_name,),
            ).fetchone()
            return bool(row)
    except Exception:
        return False


def _load_reference_df_from_backend() -> pd.DataFrame:
    # 1) SQLite local/runtime
    db_path = _resolve_db_path()
    sqlite_tables = ["fichas_tecnicas", "fichas_ctni", "criterios_tecnicos"]
    if db_path and db_path.exists():
        for table in sqlite_tables:
            if _table_exists_sqlite(db_path, table):
                try:
                    with sqlite3.connect(db_path) as conn:
                        return pd.read_sql_query(f"SELECT * FROM {table}", conn)
                except Exception:
                    continue

    # 2) Postgres/Supabase
    db_url = _supabase_db_url()
    if db_url:
        pg_tables = ["fichas_tecnicas", "fichas_ctni", "criterios_tecnicos"]
        for table in pg_tables:
            try:
                engine = create_engine(db_url, pool_pre_ping=True)
                with engine.connect() as conn:
                    df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
                    if not df.empty:
                        return df
            except Exception:
                continue
    return pd.DataFrame()


def _build_ficha_reference_map_from_df(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    if df.empty:
        return {}
    columns = list(df.columns)
    ficha_col = _resolve_column_by_alias(
        columns,
        [
            "numero ficha",
            "número ficha",
            "ficha",
            "ficha tecnica",
            "codigo ficha",
            "id ficha",
        ],
    )
    nombre_col = _resolve_column_by_alias(
        columns,
        [
            "nombre generico",
            "nombre genérico",
            "nombre ficha",
            "nombre",
            "descripcion",
        ],
    )
    registro_col = _resolve_column_by_alias(
        columns,
        [
            "registro sanitario",
            "registro_sanitario",
            "reg sanitario",
            "tiene registro sanitario",
            "rs",
        ],
    )
    riesgo_col = _resolve_column_by_alias(
        columns,
        [
            "clase de riesgo",
            "clase riesgo",
            "clase",
            "riesgo",
            "nivel de riesgo",
            "risk class",
        ],
    )
    enlace_col = _resolve_column_by_alias(
        columns,
        [
            "enlace_ficha_tecnica",
            "enlace ficha tecnica",
            "enlace ficha",
            "enlace minsa",
            "link minsa",
            "url",
            "enlace",
            "acciones",
            "accion",
        ],
    )
    if not ficha_col:
        return {}

    mapping: dict[str, dict[str, object]] = {}
    selected_cols = [ficha_col]
    if nombre_col:
        selected_cols.append(nombre_col)
    if registro_col and registro_col not in selected_cols:
        selected_cols.append(registro_col)
    if riesgo_col and riesgo_col not in selected_cols:
        selected_cols.append(riesgo_col)
    if enlace_col and enlace_col not in selected_cols:
        selected_cols.append(enlace_col)

    for _, row in df[selected_cols].iterrows():
        raw_name = str(row.get(nombre_col, "")).strip() if nombre_col else ""
        if raw_name.lower() in {"nan", "none", "null"}:
            raw_name = ""
        raw_risk = str(row.get(riesgo_col, "")).strip() if riesgo_col else ""
        if raw_risk.lower() in {"nan", "none", "null"}:
            raw_risk = ""
        raw_link = str(row.get(enlace_col, "")).strip() if enlace_col else ""
        if raw_link.lower() in {"nan", "none", "null"}:
            raw_link = ""
        raw_link = _normalize_minsa_link(raw_link)
        if not raw_link:
            for cell_val in row.tolist():
                raw_link = _normalize_minsa_link(cell_val)
                if raw_link:
                    break
        rs_required = _is_registro_sanitario_required(row.get(registro_col, "")) if registro_col else False
        tokens = _extract_ficha_tokens(row.get(ficha_col, ""))
        for token in tokens:
            ficha_num = re.sub(r"\D", "", token or "")
            if not ficha_num:
                continue
            current = mapping.setdefault(
                ficha_num,
                {"nombre_ficha": "", "rs_requerido": False, "clase_riesgo": "", "enlace_minsa": ""},
            )
            if raw_name and not str(current.get("nombre_ficha", "")).strip():
                current["nombre_ficha"] = raw_name
            if rs_required:
                current["rs_requerido"] = True
            if raw_risk and not str(current.get("clase_riesgo", "")).strip():
                current["clase_riesgo"] = raw_risk
            if raw_link and not str(current.get("enlace_minsa", "")).strip():
                current["enlace_minsa"] = raw_link
    return mapping


def _merge_reference_maps(
    base_map: dict[str, dict[str, object]],
    new_map: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    merged = dict(base_map)
    for ficha, payload in (new_map or {}).items():
        if not ficha:
            continue
        current = merged.setdefault(
            str(ficha),
            {"nombre_ficha": "", "rs_requerido": False, "clase_riesgo": "", "enlace_minsa": ""},
        )
        name_val = str(payload.get("nombre_ficha", "") or "").strip()
        risk_val = str(payload.get("clase_riesgo", "") or "").strip()
        link_val = str(payload.get("enlace_minsa", "") or "").strip()
        rs_val = bool(payload.get("rs_requerido", False))

        if name_val and not str(current.get("nombre_ficha", "")).strip():
            current["nombre_ficha"] = name_val
        if risk_val and not str(current.get("clase_riesgo", "")).strip():
            current["clase_riesgo"] = risk_val
        if link_val and not str(current.get("enlace_minsa", "")).strip():
            current["enlace_minsa"] = link_val
        if rs_val:
            current["rs_requerido"] = True
    return merged


@st.cache_data(show_spinner=False, ttl=900)
def _load_ficha_reference_map() -> dict[str, dict[str, object]]:
    merged_map: dict[str, dict[str, object]] = {}

    # 1) Fichas locales
    for path in _candidate_fichas_paths():
        try:
            if path.exists() and path.is_file() and path.stat().st_size > 0:
                local_df = pd.read_excel(path)
                mapped = _build_ficha_reference_map_from_df(local_df)
                if mapped:
                    merged_map = _merge_reference_maps(merged_map, mapped)
        except Exception:
            continue

    # 2) Criterios locales (fallback para clase de riesgo)
    for path in _candidate_criterios_paths():
        try:
            if path.exists() and path.is_file() and path.stat().st_size > 0:
                crit_df = pd.read_excel(path)
                mapped = _build_ficha_reference_map_from_df(crit_df)
                if mapped:
                    merged_map = _merge_reference_maps(merged_map, mapped)
        except Exception:
            continue

    # 3) Fichas desde Drive
    file_id = _fichas_drive_file_id() or _find_drive_file_id_by_names(
        ["fichas_ctni_con_enlace.xlsx", "fichas_ctni.xlsx"]
    )
    if file_id:
        raw, _ = _download_drive_file_bytes(file_id)
        if raw:
            try:
                drive_df = pd.read_excel(io.BytesIO(raw))
                mapped = _build_ficha_reference_map_from_df(drive_df)
                if mapped:
                    merged_map = _merge_reference_maps(merged_map, mapped)
            except Exception:
                pass

    # 4) Criterios desde Drive (fallback por nombre)
    criterios_drive_id = _find_drive_file_id_by_names(["criterios_tecnicos.xlsx"])
    if criterios_drive_id:
        raw, _ = _download_drive_file_bytes(criterios_drive_id)
        if raw:
            try:
                crit_drive_df = pd.read_excel(io.BytesIO(raw))
                mapped = _build_ficha_reference_map_from_df(crit_drive_df)
                if mapped:
                    merged_map = _merge_reference_maps(merged_map, mapped)
            except Exception:
                pass

    # 5) Tabla de referencia en DB (sqlite/postgres), util para completar enlaces faltantes.
    try:
        backend_df = _load_reference_df_from_backend()
        if not backend_df.empty:
            mapped = _build_ficha_reference_map_from_df(backend_df)
            if mapped:
                merged_map = _merge_reference_maps(merged_map, mapped)
    except Exception:
        pass

    return merged_map


@st.cache_data(show_spinner=False, ttl=1800)
def _load_oferentes_catalogos_df() -> pd.DataFrame:
    # 1) Local files
    for path in _candidate_oferentes_catalogos_paths():
        try:
            if not (path.exists() and path.is_file() and path.stat().st_size > 0):
                continue
            if path.suffix.lower() == ".csv":
                df = pd.read_csv(path)
            else:
                df = pd.read_excel(path)
            if not df.empty:
                st.session_state["intel_catalogo_status"] = f"Catalogo cargado local: {path}"
                return df
        except Exception:
            continue

    # 2) Drive (fallback)
    file_id = _oferentes_catalogos_drive_file_id() or _find_drive_file_id_by_names(
        ["oferentes_catalogos.xlsx", "oferentes_catalogos.csv", "oferentes_Catálogos.xlsx"]
    )
    if file_id:
        raw, mode = _download_drive_file_bytes(file_id)
        if raw:
            try:
                # Intenta excel primero; luego csv.
                try:
                    df = pd.read_excel(io.BytesIO(raw))
                except Exception:
                    df = pd.read_csv(io.BytesIO(raw))
                if not df.empty:
                    st.session_state["intel_catalogo_status"] = f"Catalogo cargado desde Drive ({mode})."
                    return df
            except Exception:
                pass

    st.session_state["intel_catalogo_status"] = "Catalogo de oferentes no disponible (local/Drive)."
    return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=1800)
def _build_catalog_provider_map() -> tuple[dict[tuple[str, str], dict[str, str]], dict[str, dict[str, str]]]:
    df = _load_oferentes_catalogos_df()
    if df.empty:
        return {}, {}

    cols = list(df.columns)
    proveedor_col = _resolve_column_by_alias(cols, ["Oferente::Oferente", "Oferente", "razon social", "proveedor"])
    # Evita confundir "Numero de Oferente" con la razon social real del proveedor.
    if proveedor_col and "numero" in _normalize_column_key(proveedor_col):
        for candidate in cols:
            norm_c = _normalize_column_key(candidate)
            if "oferente" in norm_c and "numero" not in norm_c:
                proveedor_col = candidate
                break
    ficha_col = _resolve_column_by_alias(
        cols,
        [
            "N° Ficha CTNI",
            "No Ficha CTNI",
            "N Ficha CTNI",
            "Numero Ficha",
            "Número Ficha",
            "ficha",
        ],
    )
    marca_col = _resolve_column_by_alias(cols, ["Marca", "Catálogo::Marca"])
    modelo_col = _resolve_column_by_alias(
        cols,
        [
            "Modelo / Sitio Web",
            "Modelo",
            "N° de Catálogo o Modelo, Sitio Web",
            "N de Catalogo o Modelo",
            "Catálogo::N° de Catálogo o Modelo, Sitio Web",
        ],
    )
    pais_col = _resolve_column_by_alias(cols, ["País Origen", "Pais Origen", "Catálogo::País Origen", "pais de origen"])

    if not proveedor_col:
        return {}, {}

    use_cols = [proveedor_col]
    for col in (ficha_col, marca_col, modelo_col, pais_col):
        if col and col not in use_cols:
            use_cols.append(col)
    work = df[use_cols].copy()
    work["proveedor_norm"] = work[proveedor_col].map(_canonical_party_name)
    work = work[work["proveedor_norm"].astype(str).str.strip() != ""].copy()
    if work.empty:
        return {}, {}

    if ficha_col:
        work["ficha_norm"] = work[ficha_col].map(_normalize_ficha_identifier).fillna("")
    else:
        work["ficha_norm"] = ""

    def _mode_or_first(series: pd.Series) -> str:
        s = series.astype(str).map(_clean_text)
        s = s[s != ""]
        if s.empty:
            return ""
        mode = s.mode()
        if not mode.empty:
            return str(mode.iloc[0])
        return str(s.iloc[0])

    key_map: dict[tuple[str, str], dict[str, str]] = {}
    provider_map: dict[str, dict[str, str]] = {}

    for prov_norm, part in work.groupby("proveedor_norm", dropna=False):
        prov_key = str(prov_norm or "").strip()
        if not prov_key:
            continue
        provider_map[prov_key] = {
            "marca": _mode_or_first(part[marca_col]) if marca_col else "",
            "modelo": _mode_or_first(part[modelo_col]) if modelo_col else "",
            "pais_origen": _mode_or_first(part[pais_col]) if pais_col else "",
        }

        if ficha_col:
            by_ficha = part.copy()
            by_ficha = by_ficha[by_ficha["ficha_norm"].astype(str).str.strip() != ""]
            for ficha_norm, chunk in by_ficha.groupby("ficha_norm", dropna=False):
                ficha_key = str(ficha_norm or "").strip()
                if not ficha_key:
                    continue
                key_map[(ficha_key, prov_key)] = {
                    "marca": _mode_or_first(chunk[marca_col]) if marca_col else "",
                    "modelo": _mode_or_first(chunk[modelo_col]) if modelo_col else "",
                    "pais_origen": _mode_or_first(chunk[pais_col]) if pais_col else "",
                }

    return key_map, provider_map


def _lookup_catalog_provider_payload(ficha: str, proveedor: str) -> dict[str, str]:
    ficha_norm = _normalize_ficha_identifier(ficha)
    prov_norm = _canonical_party_name(proveedor)
    if not ficha_norm or not prov_norm:
        return {"marca": "", "modelo": "", "pais_origen": ""}

    key_map, provider_map = _build_catalog_provider_map()
    payload = key_map.get((ficha_norm, prov_norm))
    if payload:
        return {
            "marca": _clean_text(payload.get("marca", "")),
            "modelo": _clean_text(payload.get("modelo", "")),
            "pais_origen": _clean_text(payload.get("pais_origen", "")),
        }
    payload = provider_map.get(prov_norm, {})
    return {
        "marca": _clean_text(payload.get("marca", "")),
        "modelo": _clean_text(payload.get("modelo", "")),
        "pais_origen": _clean_text(payload.get("pais_origen", "")),
    }


def _resolve_db_path() -> Path | None:
    candidates = _candidate_db_paths()
    for candidate in candidates:
        path = candidate.expanduser()
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            return path

    file_id = _panamacompra_drive_file_id()
    if not file_id:
        file_id = _find_panamacompra_db_file_id_in_drive()
    if file_id:
        raw, mode = _download_panamacompra_db_from_drive(file_id)
        if raw:
            runtime_path = APP_ROOT / "data" / "db" / "panamacompra_drive.db"
            try:
                runtime_path.parent.mkdir(parents=True, exist_ok=True)
                runtime_path.write_bytes(raw)
                st.session_state["intel_db_status"] = (
                    f"DB descargada desde Drive ({mode}) -> {runtime_path}"
                )
                return runtime_path
            except Exception:
                pass
        else:
            st.session_state["intel_db_status"] = (
                f"No se pudo descargar DB de Drive. file_id={file_id} ({mode})"
            )
    else:
        st.session_state["intel_db_status"] = (
            "Sin file_id de Drive para panamacompra.db y sin hallazgo por nombre."
        )
    return None


def _supabase_db_url() -> str:
    try:
        app_cfg = st.secrets.get("app", {})
    except Exception:
        app_cfg = {}
    candidates = [
        app_cfg.get("SUPABASE_DB_URL") if isinstance(app_cfg, dict) else None,
        app_cfg.get("DATABASE_URL") if isinstance(app_cfg, dict) else None,
        os.environ.get("SUPABASE_DB_URL"),
        os.environ.get("DATABASE_URL"),
    ]
    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    st.session_state["intel_db_status"] = (
        "No hay SUPABASE_DB_URL/DATABASE_URL configurado en secrets/app ni env."
    )
    return ""


def _load_actos_postgres_df() -> tuple[pd.DataFrame, str]:
    db_url = _supabase_db_url()
    if not db_url:
        return pd.DataFrame(), ""
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            tables_df = pd.read_sql_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
                conn,
            )
            tables = [str(x) for x in tables_df["table_name"].tolist()]
            lower_map = {t.lower(): t for t in tables}
            table = ""
            for candidate in ("actos_publicos", "actos", "panamacompra_actos"):
                if candidate in lower_map:
                    table = lower_map[candidate]
                    break
            if not table:
                for t in tables:
                    if "acto" in t.lower():
                        table = t
                        break
            if not table:
                st.session_state["intel_db_status"] = (
                    "Postgres conectado pero no hay tabla de actos (actos_publicos/actos/panamacompra_actos)."
                )
                return pd.DataFrame(), "postgres"
            df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
            st.session_state["intel_db_status"] = f"DB OK (postgres): tabla={table}"
            return df, "postgres"
    except Exception as exc:
        st.session_state["intel_db_status"] = f"Error leyendo postgres: {exc}"
        return pd.DataFrame(), "postgres"


@st.cache_data(show_spinner=False, ttl=1800)
def _load_actos_db_df() -> tuple[pd.DataFrame, str]:
    db_path = _resolve_db_path()
    if db_path is None:
        pg_df, pg_source = _load_actos_postgres_df()
        if not pg_df.empty:
            return pg_df, pg_source
        current = str(st.session_state.get("intel_db_status", "")).strip()
        if current:
            st.session_state["intel_db_status"] = (
                "No se encontro panamacompra.db local/Drive. "
                f"Detalle final: {current}"
            )
        else:
            st.session_state["intel_db_status"] = (
                "No se encontro panamacompra.db local/Drive y tampoco se pudo leer postgres."
            )
        return pd.DataFrame(), ""
    try:
        with sqlite3.connect(db_path) as conn:
            tables_df = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'",
                conn,
            )
            tables = set(tables_df["name"].astype(str).tolist())
            actos_table = ""
            for candidate in ("actos_publicos", "actos", "panamacompra_actos"):
                if candidate in tables:
                    actos_table = candidate
                    break
            if not actos_table:
                st.session_state["intel_db_status"] = (
                    f"Se encontro DB en `{db_path}` pero no existe tabla de actos "
                    "(esperadas: actos_publicos, actos, panamacompra_actos)."
                )
                return pd.DataFrame(), str(db_path)
            df = pd.read_sql_query(f"SELECT * FROM {actos_table}", conn)
        st.session_state["intel_db_status"] = f"DB OK: {db_path}"
        return df, str(db_path)
    except Exception as exc:
        st.session_state["intel_db_status"] = f"Error leyendo DB `{db_path}`: {exc}"
        return pd.DataFrame(), str(db_path)


def _extract_ficha_tokens(raw_value: object) -> list[str]:
    tokens = FICHA_TOKEN_RE.findall(str(raw_value or ""))
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token not in seen:
            seen.add(token)
            unique.append(token)
    return unique


def _winner_price_from_row(row: pd.Series) -> float:
    winner = _normalize_text(row.get("razon_social", "")) or _normalize_text(row.get("nombre_comercial", ""))
    if winner:
        for idx in range(1, 15):
            proponente = _normalize_text(row.get(f"Proponente {idx}", ""))
            if proponente and proponente == winner:
                winner_price = _parse_number(row.get(f"Precio Proponente {idx}", ""))
                if winner_price > 0:
                    return winner_price
    return _parse_number(row.get("precio_referencia", 0))


def _build_top_winners_by_ficha(exploded: pd.DataFrame) -> pd.DataFrame:
    if exploded.empty or "ficha" not in exploded.columns or "id" not in exploded.columns:
        return pd.DataFrame()
    if "ganador_norm" not in exploded.columns:
        return pd.DataFrame()

    winner_display_col = "ganador"
    if winner_display_col not in exploded.columns:
        winner_display_col = "ganador_norm"

    base = exploded[["ficha", "id", "ganador_norm", winner_display_col]].copy()
    base["ficha"] = base["ficha"].astype(str).str.strip()
    base["ganador_norm"] = base["ganador_norm"].fillna("").astype(str).str.strip()
    base[winner_display_col] = base[winner_display_col].fillna("").astype(str).str.strip()
    base = base[(base["ficha"] != "")].drop_duplicates(subset=["ficha", "id", "ganador_norm"])
    if base.empty:
        return pd.DataFrame()

    total_actos = base.groupby("ficha", dropna=False)["id"].nunique().rename("actos_total")

    display_map = (
        base[base["ganador_norm"] != ""]
        .groupby(["ficha", "ganador_norm", winner_display_col], dropna=False)
        .size()
        .reset_index(name="freq")
        .sort_values(["ficha", "ganador_norm", "freq"], ascending=[True, True, False], kind="stable")
        .drop_duplicates(subset=["ficha", "ganador_norm"], keep="first")
        .rename(columns={winner_display_col: "ganador_display"})
    )

    wins = (
        base[base["ganador_norm"] != ""]
        .groupby(["ficha", "ganador_norm"], dropna=False)["id"]
        .nunique()
        .reset_index(name="victorias")
    )
    if wins.empty:
        return pd.DataFrame()

    wins = wins.merge(display_map[["ficha", "ganador_norm", "ganador_display"]], on=["ficha", "ganador_norm"], how="left")
    wins["ganador_display"] = wins["ganador_display"].fillna("").astype(str)
    wins = wins.sort_values(
        ["ficha", "victorias", "ganador_display"],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    wins["rank"] = wins.groupby("ficha").cumcount() + 1
    wins = wins[wins["rank"] <= 3].copy()
    wins = wins.merge(total_actos.reset_index(), on="ficha", how="left")
    wins["pct_ganadas"] = (100.0 * wins["victorias"] / wins["actos_total"].clip(lower=1)).round(2)

    rows: list[dict[str, object]] = []
    for ficha, chunk in wins.groupby("ficha", dropna=False):
        payload: dict[str, object] = {"ficha": ficha}
        for _, item in chunk.iterrows():
            rank = int(item.get("rank", 0))
            if rank <= 0 or rank > 3:
                continue
            payload[f"top{rank}_ganador"] = str(item.get("ganador_display", "") or "")
            payload[f"top{rank}_pct_ganadas"] = float(item.get("pct_ganadas", 0.0))
            payload[f"top{rank}_victorias"] = int(item.get("victorias", 0))
        rows.append(payload)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=1800)
def _build_ficha_universe() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    base_df, db_path = _load_actos_db_df()
    if base_df.empty:
        return pd.DataFrame(), pd.DataFrame(), db_path

    work = base_df.copy()
    if "id" not in work.columns:
        work["id"] = range(1, len(work) + 1)
    work["ficha_detectada"] = work.get("ficha_detectada", "").fillna("").astype(str)
    work["ficha_tokens"] = work["ficha_detectada"].map(_extract_ficha_tokens)
    # Fallback: if ficha_detectada is missing/empty in source, try extraction from key text fields.
    if work["ficha_tokens"].map(len).sum() == 0:
        fallback_cols = [
            col
            for col in ("ficha", "titulo", "descripcion", "item_1", "item_2", "observaciones")
            if col in work.columns
        ]
        if fallback_cols:
            merged_text = work[fallback_cols].fillna("").astype(str).agg(" ".join, axis=1)
            work["ficha_tokens"] = merged_text.map(_extract_ficha_tokens)

    # Cobertura temporal informativa (sin filtrar): prioriza fecha_adjudicacion y usa fallback.
    date_candidates = [c for c in ("fecha_adjudicacion", "fecha", "fecha_publicacion", "publicacion") if c in work.columns]
    if date_candidates:
        parsed_cols: list[str] = []
        for col in date_candidates:
            parsed_col = f"__parsed_{col}"
            work[parsed_col] = work[col].map(_parse_any_date)
            parsed_cols.append(parsed_col)
        work["fecha_referencia"] = work[parsed_cols[0]]
        for parsed_col in parsed_cols[1:]:
            work["fecha_referencia"] = work["fecha_referencia"].fillna(work[parsed_col])
        valid_dates = work["fecha_referencia"].dropna()
        if not valid_dates.empty:
            start = valid_dates.min().date().isoformat()
            end = valid_dates.max().date().isoformat()
            st.session_state["intel_time_window_status"] = (
                f"Historico completo (sin filtro temporal): {len(work):,} registros. "
                f"Cobertura fecha ref: {start} -> {end}."
            )
        else:
            st.session_state["intel_time_window_status"] = (
                f"Historico completo (sin filtro temporal): {len(work):,} registros. "
                "No se pudieron parsear fechas de referencia."
            )
    else:
        st.session_state["intel_time_window_status"] = (
            f"Historico completo (sin filtro temporal): {len(work):,} registros. "
            "Sin columnas de fecha reconocidas."
        )

    work = work[work["ficha_tokens"].map(len) > 0].copy()
    if work.empty:
        st.session_state["intel_db_status"] = (
            f"DB leida ({db_path}) pero no se detectaron fichas en columnas de referencia."
        )
        return pd.DataFrame(), pd.DataFrame(), db_path

    work["monto_estimado"] = work.apply(_winner_price_from_row, axis=1)
    work["num_participantes_num"] = work.get("num_participantes", 0).map(_parse_number)
    work["entidad"] = work.get("entidad", "").fillna("").astype(str).str.strip()
    work["ganador"] = work.get("razon_social", "").fillna("").astype(str).str.strip()
    if "nombre_comercial" in work.columns:
        no_winner_mask = work["ganador"].astype(str).str.strip() == ""
        work.loc[no_winner_mask, "ganador"] = (
            work.loc[no_winner_mask, "nombre_comercial"].fillna("").astype(str).str.strip()
        )
    work["ganador_norm"] = work["ganador"].map(_canonical_party_name)
    work["proponentes_en_acto"] = _compute_proponentes_por_acto(work)
    fallback_participantes = (
        pd.to_numeric(work.get("num_participantes_num", 0), errors="coerce").fillna(0).round().clip(lower=0).astype(int)
    )
    missing_mask = work["proponentes_en_acto"] <= 0
    work.loc[missing_mask, "proponentes_en_acto"] = fallback_participantes[missing_mask]

    exploded = work.explode("ficha_tokens").rename(columns={"ficha_tokens": "ficha_token"})
    exploded["ficha_token"] = exploded["ficha_token"].astype(str).str.strip()
    exploded = exploded[exploded["ficha_token"] != ""].copy()
    exploded["ficha"] = exploded["ficha_token"].str.replace(r"\D", "", regex=True)
    exploded = exploded[exploded["ficha"] != ""].copy()
    exploded = exploded.drop_duplicates(subset=["id", "ficha"]).reset_index(drop=True)
    fichas_por_acto = exploded.groupby("id", dropna=False)["ficha"].nunique()
    exploded["fichas_en_acto"] = exploded["id"].map(fichas_por_acto).fillna(0).astype(int)
    exploded["acto_tipo_ficha"] = exploded["fichas_en_acto"].map(
        lambda n: "solo_esa_ficha" if int(n) <= 1 else "con_otras_fichas"
    )
    ficha_reference_map = _load_ficha_reference_map()
    risk_count = sum(
        1
        for payload in ficha_reference_map.values()
        if str((payload or {}).get("clase_riesgo", "") or "").strip()
    )
    direct_link_count = sum(
        1
        for payload in ficha_reference_map.values()
        if _is_specific_minsa_link(str((payload or {}).get("enlace_minsa", "") or ""))
    )
    st.session_state["intel_risk_map_status"] = (
        f"Mapeo de fichas cargado: {len(ficha_reference_map):,} fichas, "
        f"{risk_count:,} con clase de riesgo, {direct_link_count:,} con enlace directo."
    )
    exploded["nombre_ficha"] = exploded["ficha"].astype(str).map(
        lambda x: str((ficha_reference_map.get(str(x)) or {}).get("nombre_ficha", "") or "")
    )
    exploded["clase_riesgo"] = exploded["ficha"].astype(str).map(
        lambda x: str((ficha_reference_map.get(str(x)) or {}).get("clase_riesgo", "") or "")
    )
    exploded["enlace_minsa"] = exploded["ficha"].astype(str).map(
        lambda x: str((ficha_reference_map.get(str(x)) or {}).get("enlace_minsa", "") or "")
    )
    ctni_id_map = _load_ctni_num_to_id_map()
    link_by_ficha: dict[str, str] = {}
    for ficha_key in exploded["ficha"].astype(str).dropna().unique().tolist():
        raw_link = str((ficha_reference_map.get(str(ficha_key)) or {}).get("enlace_minsa", "") or "")
        link_by_ficha[str(ficha_key)] = _specific_minsa_link(ficha_key, raw_link, ctni_id_map)
    exploded["enlace_minsa"] = exploded["ficha"].astype(str).map(link_by_ficha).fillna(CTNI_CONSULTA_URL)
    exploded["rs_requerido"] = exploded["ficha"].astype(str).map(
        lambda x: bool((ficha_reference_map.get(str(x)) or {}).get("rs_requerido", False))
    )
    exploded = exploded[~exploded["rs_requerido"]].copy()
    if exploded.empty:
        st.session_state["intel_db_status"] = (
            f"DB leida ({db_path}) pero todas las fichas detectadas requieren registro sanitario."
        )
        return pd.DataFrame(), pd.DataFrame(), db_path

    grouped = exploded.groupby("ficha", dropna=False)
    base_unique = exploded[["ficha", "id", "fichas_en_acto"]].drop_duplicates()
    actos_solo = (
        base_unique[base_unique["fichas_en_acto"] <= 1]
        .groupby("ficha", dropna=False)["id"]
        .nunique()
    )
    actos_multi = (
        base_unique[base_unique["fichas_en_acto"] > 1]
        .groupby("ficha", dropna=False)["id"]
        .nunique()
    )
    ficha_metrics = pd.DataFrame(
        {
            "ficha": grouped["ficha"].first(),
            "actos": grouped["id"].nunique(),  # todos los actos donde aparece la ficha
            "monto_historico": grouped["monto_estimado"].sum(),
            "ganadores_distintos": grouped["ganador_norm"].apply(lambda s: s[s.astype(str).str.strip() != ""].nunique()),
            "proponentes_promedio": grouped["proponentes_en_acto"].mean(),
        }
    ).reset_index(drop=True)
    ficha_metrics["pct_actos_proponentes_cero"] = (
        grouped["proponentes_en_acto"]
        .apply(lambda s: float((pd.to_numeric(s, errors="coerce").fillna(0.0) <= 0).mean() * 100.0))
        .reset_index(drop=True)
        .round(2)
    )
    ficha_metrics["revision_proponentes"] = (
        pd.to_numeric(ficha_metrics["proponentes_promedio"], errors="coerce").fillna(0.0) <= 0
    )
    ficha_metrics["actos_solo_ficha"] = ficha_metrics["ficha"].map(actos_solo).fillna(0).astype(int)
    ficha_metrics["actos_con_otras_fichas"] = ficha_metrics["ficha"].map(actos_multi).fillna(0).astype(int)
    if "fecha_adjudicacion" in exploded.columns:
        ficha_metrics["ultima_fecha"] = grouped["fecha_adjudicacion"].max().reset_index(drop=True)
    else:
        ficha_metrics["ultima_fecha"] = ""

    ficha_metrics["nombre_ficha"] = ficha_metrics["ficha"].astype(str).map(
        lambda x: str((ficha_reference_map.get(str(x)) or {}).get("nombre_ficha", "") or "")
    )
    ficha_metrics["clase_riesgo"] = ficha_metrics["ficha"].astype(str).map(
        lambda x: str((ficha_reference_map.get(str(x)) or {}).get("clase_riesgo", "") or "")
    )
    ficha_metrics["enlace_minsa"] = ficha_metrics["ficha"].astype(str).map(
        lambda x: str(link_by_ficha.get(str(x), CTNI_CONSULTA_URL) or CTNI_CONSULTA_URL)
    )

    winners_df = _build_top_winners_by_ficha(exploded)
    if not winners_df.empty:
        ficha_metrics = ficha_metrics.merge(winners_df, on="ficha", how="left")
    for idx in (1, 2, 3):
        g_col = f"top{idx}_ganador"
        p_col = f"top{idx}_pct_ganadas"
        v_col = f"top{idx}_victorias"
        if g_col not in ficha_metrics.columns:
            ficha_metrics[g_col] = ""
        else:
            ficha_metrics[g_col] = ficha_metrics[g_col].fillna("").astype(str)
        if p_col not in ficha_metrics.columns:
            ficha_metrics[p_col] = 0.0
        else:
            ficha_metrics[p_col] = pd.to_numeric(ficha_metrics[p_col], errors="coerce").fillna(0.0).round(2)
        if v_col not in ficha_metrics.columns:
            ficha_metrics[v_col] = 0
        else:
            ficha_metrics[v_col] = pd.to_numeric(ficha_metrics[v_col], errors="coerce").fillna(0).astype(int)

    return ficha_metrics, exploded, db_path


def _minmax(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    smin = float(numeric.min())
    smax = float(numeric.max())
    if smax <= smin:
        return pd.Series([0.0] * len(numeric), index=numeric.index)
    return (numeric - smin) / (smax - smin)


def _winsorize_upper(series: pd.Series, quantile: float = 0.95) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if numeric.empty:
        return numeric
    cap = float(numeric.quantile(quantile))
    return numeric.clip(lower=0.0, upper=cap)


def _feature_high_is_better(series: pd.Series, use_log: bool = False) -> pd.Series:
    transformed = _winsorize_upper(series, quantile=0.95)
    if use_log:
        transformed = transformed.map(lambda v: math.log1p(max(float(v), 0.0)))
    return _minmax(transformed)


def _feature_low_is_better(series: pd.Series, use_log: bool = False) -> pd.Series:
    return 1.0 - _feature_high_is_better(series, use_log=use_log)


def _classify_score(score: float) -> str:
    if score >= 75:
        return "atacar ya"
    if score >= 55:
        return "prometedor"
    if score >= 35:
        return "observacion"
    return "baja prioridad"


def _default_weights() -> dict[str, float]:
    return {
        "actos": 32.0,
        "monto": 38.0,
        "ganadores": 12.0,
        "proponentes": 10.0,
        "riesgo": 8.0,
    }


def _risk_to_score(value: object) -> float:
    # Reglas solicitadas:
    # A = 100% del peso (1.00)
    # B = 66% del peso (0.66)
    # C = 33% del peso (0.33)
    # D = 0% del peso (0.00)
    # "No aplica" se trata como A.
    norm = _normalize_column_key(value)
    if not norm:
        return 0.5

    tokens = set(norm.split())
    if "clase" in tokens:
        tokens.discard("clase")

    if "no aplica" in norm or "na" == norm or "n a" == norm or "n/a" in str(value or "").lower():
        return 1.0

    if "a" in tokens or "i" in tokens or "1" in tokens or "bajo" in tokens:
        return 1.0
    if "b" in tokens or "ii" in tokens or "2" in tokens or ("bajo" in tokens and "moderado" in tokens):
        return 0.66
    if "c" in tokens or "iii" in tokens or "3" in tokens or ("alto" in tokens and "moderado" in tokens):
        return 0.33
    if "d" in tokens or "iv" in tokens or "4" in tokens or "alto" in tokens or "critico" in tokens:
        return 0.0

    return 0.5


def _normalize_weights(stored: dict[str, float] | None, defaults: dict[str, float]) -> dict[str, float]:
    stored = stored or {}
    normalized: dict[str, float] = {}
    for key, default_val in defaults.items():
        raw = stored.get(key, default_val)
        try:
            normalized[key] = float(raw)
        except Exception:
            normalized[key] = float(default_val)
    return normalized


def _ensure_default_weights_profile() -> dict[str, float]:
    defaults = _default_weights()
    profile_version = str(st.session_state.get("intel_weights_profile_version", "") or "")
    if profile_version != INTEL_WEIGHTS_PROFILE_VERSION:
        st.session_state["intel_weights"] = defaults.copy()
        st.session_state["intel_weights_profile_version"] = INTEL_WEIGHTS_PROFILE_VERSION
        return defaults.copy()

    normalized = _normalize_weights(st.session_state.get("intel_weights", {}), defaults)
    st.session_state["intel_weights"] = normalized
    return normalized


def _score_fichas(ficha_df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    if ficha_df.empty:
        return ficha_df
    df = ficha_df.copy()
    # Opcion 2: winsorizacion p95 + log1p + minmax.
    # Direccion de negocio:
    # - actos, monto: mas alto = mejor
    # - ganadores_distintos, proponentes_promedio: mas bajo = mejor
    df["f_actos"] = _feature_high_is_better(df["actos"], use_log=True)
    df["f_monto"] = _feature_high_is_better(df["monto_historico"], use_log=True)
    df["f_ganadores"] = _feature_low_is_better(df["ganadores_distintos"], use_log=False)
    df["f_proponentes"] = _feature_low_is_better(df["proponentes_promedio"], use_log=False)
    df["f_riesgo"] = df["clase_riesgo"].map(_risk_to_score)
    if "revision_proponentes" not in df.columns:
        df["revision_proponentes"] = (
            pd.to_numeric(df.get("proponentes_promedio", 0), errors="coerce").fillna(0.0) <= 0
        )

    total_weight = sum(weights.values()) or 1.0
    weighted = (
        weights["actos"] * df["f_actos"]
        + weights["monto"] * df["f_monto"]
        + weights["ganadores"] * df["f_ganadores"]
        + weights["proponentes"] * df["f_proponentes"]
        + weights["riesgo"] * df["f_riesgo"]
    )
    df["score_total"] = (100.0 * weighted / total_weight).round(2)
    df["clasificacion"] = df["score_total"].map(_classify_score)
    return df.sort_values(["score_total", "actos", "monto_historico"], ascending=[False, False, False]).reset_index(drop=True)


def _ensure_study_state(sync_remote: bool = False) -> list[dict]:
    """
    Carga estado de seguimiento con estrategia rapida por defecto.
    - sync_remote=False: usa SQLite local para entrada rapida.
    - sync_remote=True: sincroniza con Sheets antes de operar.
    """
    loaded = bool(st.session_state.get("intel_tracking_loaded", False))
    remote_synced = bool(st.session_state.get("intel_tracking_remote_synced", False))

    if not loaded:
        if sync_remote:
            try:
                records, backend, status = _load_tracking_records_persistent()
                remote_synced = True
            except Exception as exc:
                records = _read_tracking_records_from_sqlite()
                backend = "sqlite"
                status = f"Seguimiento cargado localmente (sync remoto fallo: {exc})"
        else:
            records = _read_tracking_records_from_sqlite()
            backend = "sqlite"
            status = "Seguimiento cargado rapido desde SQLite local."

        st.session_state["intel_fichas_estudio"] = _normalize_tracking_records(records)
        st.session_state["intel_tracking_backend"] = backend
        st.session_state["intel_tracking_status"] = status
        st.session_state["intel_tracking_loaded"] = True
        st.session_state["intel_tracking_remote_synced"] = remote_synced
    elif sync_remote and not remote_synced:
        try:
            records, backend, status = _load_tracking_records_persistent()
            st.session_state["intel_fichas_estudio"] = _normalize_tracking_records(records)
            st.session_state["intel_tracking_backend"] = backend
            st.session_state["intel_tracking_status"] = status
            st.session_state["intel_tracking_remote_synced"] = True
        except Exception as exc:
            base_status = str(st.session_state.get("intel_tracking_status", "Seguimiento local activo")).strip()
            st.session_state["intel_tracking_status"] = (
                f"{base_status} (sync remoto pendiente: {exc})"
            ).strip()

    if "intel_fichas_estudio" not in st.session_state:
        st.session_state["intel_fichas_estudio"] = []
    return st.session_state["intel_fichas_estudio"]


def _ensure_discarded_state() -> list[str]:
    if "intel_fichas_descartadas" not in st.session_state:
        st.session_state["intel_fichas_descartadas"] = []
    # normaliza a lista de strings únicos
    raw = st.session_state.get("intel_fichas_descartadas", [])
    seen: set[str] = set()
    out: list[str] = []
    for item in raw if isinstance(raw, list) else []:
        key = str(item or "").strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    st.session_state["intel_fichas_descartadas"] = out
    return out


def _add_ficha_to_study(row: pd.Series) -> bool:
    # Antes de mutar, sincroniza para evitar sobrescribir seguimiento remoto.
    current = _ensure_study_state(sync_remote=True)
    ficha = str(row.get("ficha", "")).strip()
    if not ficha:
        return False
    if any(str(item.get("ficha", "")).strip() == ficha for item in current):
        return False
    now_iso = _utc_now_iso()
    current.append(
        {
            "ficha": ficha,
            "nombre_ficha": str(row.get("nombre_ficha", "")).strip(),
            "clase_riesgo": str(row.get("clase_riesgo", "")).strip(),
            "enlace_minsa": str(row.get("enlace_minsa", "")).strip(),
            "score_inicial": float(row.get("score_total", 0.0)),
            "clasificacion": str(row.get("clasificacion", "")),
            "actos": int(row.get("actos", 0)),
            "actos_solo_ficha": int(row.get("actos_solo_ficha", 0)),
            "actos_con_otras_fichas": int(row.get("actos_con_otras_fichas", 0)),
            "monto_historico": float(row.get("monto_historico", 0.0)),
            "proponentes_promedio": float(row.get("proponentes_promedio", 0.0)),
            "revision_proponentes": bool(row.get("revision_proponentes", False)),
            "top1_ganador": str(row.get("top1_ganador", "")).strip(),
            "top1_pct_ganadas": float(row.get("top1_pct_ganadas", 0.0)),
            "top2_ganador": str(row.get("top2_ganador", "")).strip(),
            "top2_pct_ganadas": float(row.get("top2_pct_ganadas", 0.0)),
            "top3_ganador": str(row.get("top3_ganador", "")).strip(),
            "top3_pct_ganadas": float(row.get("top3_pct_ganadas", 0.0)),
            "estado": "pendiente de estudio profundo",
            "fecha_ingreso": date.today().isoformat(),
            "notas": "",
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    )
    st.session_state["intel_fichas_estudio"] = _normalize_tracking_records(current)
    ok, msg = _persist_tracking_records(st.session_state["intel_fichas_estudio"])
    st.session_state["intel_tracking_status"] = msg
    st.session_state["intel_tracking_backend"] = "sheets+sqlite" if ok else "sqlite"
    return True


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return float(default)
            # Acepta formatos locales comunes: "1.234,56" y "1234.56"
            text = text.replace("B/.", "").replace("B/", "").replace("$", "").replace("USD", "").strip()
            if "," in text and "." in text:
                if text.rfind(",") > text.rfind("."):
                    text = text.replace(".", "").replace(",", ".")
                else:
                    text = text.replace(",", "")
            elif "," in text:
                text = text.replace(",", ".")
            return float(text)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: object) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _json_dumps(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "[]"


def _build_ficha_label_map(
    df: pd.DataFrame,
    ficha_col: str = "ficha",
    nombre_col: str = "nombre_ficha",
) -> dict[str, str]:
    labels: dict[str, str] = {}
    if df is None or df.empty or ficha_col not in df.columns:
        return labels
    for _, row in df.iterrows():
        ficha = _clean_text(row.get(ficha_col, ""))
        if not ficha:
            continue
        nombre = _clean_text(row.get(nombre_col, "")) if nombre_col in df.columns else ""
        if nombre:
            labels[ficha] = f"{ficha} - {nombre}"
        elif ficha not in labels:
            labels[ficha] = ficha
    return labels


def _intel_sheet_id() -> str:
    for key in ("SHEET_ID", "PC_MANUAL_SHEET_ID", "PC_CONFIG_SHEET_ID"):
        env_val = _clean_text(os.getenv(key, ""))
        if env_val:
            return env_val

        try:
            app_cfg = st.secrets.get("app", {})
            if isinstance(app_cfg, Mapping):
                app_val = _clean_text(app_cfg.get(key, ""))
                if app_val:
                    return app_val
        except Exception:
            pass

        try:
            root_val = _clean_text(st.secrets.get(key, ""))
            if root_val:
                return root_val
        except Exception:
            pass
    return ""


def _pc_manual_sheet_id() -> str:
    env_manual = _clean_text(os.getenv("PC_MANUAL_SHEET_ID", ""))
    if env_manual:
        return env_manual
    env_sheet = _clean_text(os.getenv("SHEET_ID", ""))
    if env_sheet:
        return env_sheet

    try:
        app_cfg = st.secrets.get("app", {})
        if isinstance(app_cfg, Mapping):
            return (
                _clean_text(app_cfg.get("PC_MANUAL_SHEET_ID", ""))
                or _clean_text(app_cfg.get("SHEET_ID", ""))
                or _intel_sheet_id()
            )
    except Exception:
        pass

    try:
        root_manual = _clean_text(st.secrets.get("PC_MANUAL_SHEET_ID", ""))
        if root_manual:
            return root_manual
        root_sheet = _clean_text(st.secrets.get("SHEET_ID", ""))
        if root_sheet:
            return root_sheet
    except Exception:
        pass

    return _intel_sheet_id()


def _pc_config_sheet_id() -> str:
    env_cfg = _clean_text(os.getenv("PC_CONFIG_SHEET_ID", ""))
    if env_cfg:
        return env_cfg
    env_manual = _clean_text(os.getenv("PC_MANUAL_SHEET_ID", ""))
    if env_manual:
        return env_manual
    env_sheet = _clean_text(os.getenv("SHEET_ID", ""))
    if env_sheet:
        return env_sheet

    try:
        app_cfg = st.secrets.get("app", {})
        if isinstance(app_cfg, Mapping):
            return (
                _clean_text(app_cfg.get("PC_CONFIG_SHEET_ID", ""))
                or _clean_text(app_cfg.get("PC_MANUAL_SHEET_ID", ""))
                or _clean_text(app_cfg.get("SHEET_ID", ""))
                or _intel_sheet_id()
            )
    except Exception:
        pass

    try:
        root_cfg = _clean_text(st.secrets.get("PC_CONFIG_SHEET_ID", ""))
        if root_cfg:
            return root_cfg
        root_manual = _clean_text(st.secrets.get("PC_MANUAL_SHEET_ID", ""))
        if root_manual:
            return root_manual
        root_sheet = _clean_text(st.secrets.get("SHEET_ID", ""))
        if root_sheet:
            return root_sheet
    except Exception:
        pass

    return _intel_sheet_id()


def _uniq_nonempty(values: list[object]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _sheet_id_candidates(kind: str) -> list[str]:
    key_map = {
        "config": ["PC_CONFIG_SHEET_ID", "PC_MANUAL_SHEET_ID", "SHEET_ID"],
        "manual": ["PC_MANUAL_SHEET_ID", "SHEET_ID", "PC_CONFIG_SHEET_ID"],
        "intel": ["SHEET_ID", "PC_MANUAL_SHEET_ID", "PC_CONFIG_SHEET_ID"],
    }
    keys = key_map.get(kind, key_map["intel"])
    vals: list[object] = []

    # 1) env vars
    for key in keys:
        vals.append(os.getenv(key, ""))

    # 2) secrets.app
    try:
        app_cfg = st.secrets.get("app", {})
        if isinstance(app_cfg, Mapping):
            for key in keys:
                vals.append(app_cfg.get(key, ""))
    except Exception:
        pass

    # 3) secrets root
    try:
        for key in keys:
            vals.append(st.secrets.get(key, ""))
    except Exception:
        pass

    # 4) funciones existentes + default conocido
    vals.extend([_pc_config_sheet_id(), _pc_manual_sheet_id(), _intel_sheet_id(), PC_SHEET_ID_DEFAULT])
    return _uniq_nonempty(vals)


def _open_sheet_with_fallback(client, candidates: list[str], purpose: str) -> tuple[str, object]:
    # Cachea el ultimo sheet_id valido por proposito para evitar lecturas extras
    # de metadata al probar multiples IDs en cada polling.
    cache_key = f"intel_sheet_cache_{purpose}"
    cached_sid = _clean_text(st.session_state.get(cache_key, ""))
    if cached_sid:
        try:
            sh = client.open_by_key(cached_sid)
            return cached_sid, sh
        except Exception:
            st.session_state.pop(cache_key, None)

    attempted: list[str] = []
    last_exc: Exception | None = None
    for sid in _uniq_nonempty(candidates):
        attempted.append(sid)
        try:
            sh = client.open_by_key(sid)
            st.session_state[cache_key] = sid
            return sid, sh
        except Exception as exc:
            last_exc = exc
            continue
    attempted_str = ", ".join(attempted) if attempted else "(sin candidatos)"
    raise RuntimeError(
        f"No se pudo abrir hoja de {purpose}. IDs probados: {attempted_str}. "
        f"Ultimo error: {last_exc}"
    )


def _current_user() -> str:
    for key in ("username", "user", "email", "correo", "name", "nombre"):
        value = st.session_state.get(key)
        if value:
            return str(value)
    return "desconocido"


def _excel_column_letter(index: int) -> str:
    out: list[str] = []
    num = max(1, int(index))
    while num > 0:
        num, rem = divmod(num - 1, 26)
        out.append(chr(65 + rem))
    return "".join(reversed(out))


def _ensure_ws_headers(client, sheet_id: str, worksheet: str, headers: list[str]) -> None:
    from gspread.exceptions import WorksheetNotFound

    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet, rows=300, cols=max(len(headers), 8))
        ws.update("A1", [headers])
        return

    existing = [cell.strip() for cell in (ws.row_values(1) or [])]
    if existing[: len(headers)] != headers:
        last_col = _excel_column_letter(len(headers))
        ws.update(f"A1:{last_col}1", [headers])


def _ensure_intel_orquestador_job(client) -> None:
    # Evita releer pc_config en cada encolado dentro de la misma sesion.
    # Esto reduce picos de lectura en Sheets (429) sin afectar funcionalidad.
    cache_key = "intel_orq_job_ready"
    if bool(st.session_state.get(cache_key, False)):
        return

    sheet_id, sh = _open_sheet_with_fallback(client, _sheet_id_candidates("config"), "pc_config")
    _ensure_ws_headers(client, sheet_id, PC_CONFIG_WORKSHEET, PC_CONFIG_HEADERS)
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(PC_CONFIG_WORKSHEET)
    rows = ws.get_all_records() or []
    headers = [h.strip() for h in ws.row_values(1)]
    header_map = {h.lower(): idx + 1 for idx, h in enumerate(headers)}

    for idx, row in enumerate(rows, start=2):
        if str(row.get("name", "")).strip().lower() == INTEL_ORQ_JOB_NAME:
            updates = {
                "python": INTEL_ORQ_JOB_PY,
                "script": INTEL_ORQ_JOB_SCRIPT,
                "days": "",
                "times": "",
                "enabled": "si",
            }
            for key, value in updates.items():
                col = header_map.get(key)
                if not col:
                    continue
                current = str(row.get(key, "")).strip()
                if current != value:
                    ws.update_cell(idx, col, value)
            st.session_state[cache_key] = True
            return

    row_data = {
        "name": INTEL_ORQ_JOB_NAME,
        "python": INTEL_ORQ_JOB_PY,
        "script": INTEL_ORQ_JOB_SCRIPT,
        "days": "",
        "times": "",
        "enabled": "si",
    }
    ws.append_row([row_data.get(col, "") for col in PC_CONFIG_HEADERS], value_input_option="USER_ENTERED")
    st.session_state[cache_key] = True


def _append_intel_manual_request(client, payload: dict[str, object]) -> str:
    sheet_id, _ = _open_sheet_with_fallback(client, _sheet_id_candidates("manual"), "pc_manual")
    _ensure_ws_headers(client, sheet_id, PC_MANUAL_WORKSHEET, PC_MANUAL_HEADERS)
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(PC_MANUAL_WORKSHEET)

    request_id = uuid.uuid4().hex
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload_obj = dict(payload or {})
    payload_obj["request_id"] = request_id
    row_data = {
        "id": request_id,
        "job": INTEL_ORQ_JOB_NAME,
        "requested_by": _current_user(),
        "requested_at": now_local,
        "status": "pending",
        "notes": "",
        "payload": json.dumps(payload_obj, ensure_ascii=False),
        "result_file_id": "",
        "result_file_url": "",
        "result_file_name": "",
        "result_error": "",
    }
    ws.append_row([row_data.get(h, "") for h in PC_MANUAL_HEADERS], value_input_option="USER_ENTERED")
    return request_id


def _register_study_request_session(
    request_id: str,
    ficha: str,
    nombre_ficha: str,
    notes: str = "",
    *,
    set_current_if_empty: bool = True,
) -> None:
    req = _clean_text(request_id)
    if not req:
        return
    ficha_id = _clean_text(ficha)
    ficha_name = _clean_text(nombre_ficha)
    notes_txt = _clean_text(notes)

    meta_map = st.session_state.get("intel_study_request_meta_map", {})
    if not isinstance(meta_map, dict):
        meta_map = {}
    meta_map[req] = {
        "ficha": ficha_id,
        "nombre_ficha": ficha_name,
        "notes": notes_txt,
    }
    st.session_state["intel_study_request_meta_map"] = meta_map

    batch_ids_raw = st.session_state.get("intel_study_request_batch_ids", [])
    batch_ids = [str(x).strip() for x in (batch_ids_raw if isinstance(batch_ids_raw, list) else []) if str(x).strip()]
    if req not in batch_ids:
        batch_ids.append(req)
    st.session_state["intel_study_request_batch_ids"] = batch_ids

    current_req = _clean_text(st.session_state.get("intel_study_request_id", ""))
    if set_current_if_empty and not current_req:
        st.session_state["intel_study_request_id"] = req
        st.session_state["intel_study_request_ficha"] = ficha_id
        st.session_state["intel_study_request_ficha_name"] = ficha_name
        st.session_state["intel_study_request_notes"] = notes_txt
        st.session_state.pop("intel_study_request_synced_id", None)
        st.session_state["intel_manual_last_req_row"] = None
        st.session_state["intel_manual_last_poll_ts"] = 0.0
        st.session_state["intel_manual_poll_cooldown_until"] = 0.0


def _advance_study_request_queue(current_request_id: str) -> str:
    current = _clean_text(current_request_id)
    batch_ids_raw = st.session_state.get("intel_study_request_batch_ids", [])
    batch_ids = [str(x).strip() for x in (batch_ids_raw if isinstance(batch_ids_raw, list) else []) if str(x).strip()]
    if current in batch_ids:
        idx = batch_ids.index(current)
        batch_ids = batch_ids[idx + 1 :]
    else:
        batch_ids = [rid for rid in batch_ids if rid != current]
    st.session_state["intel_study_request_batch_ids"] = batch_ids

    next_id = batch_ids[0] if batch_ids else ""
    st.session_state["intel_study_request_id"] = next_id
    st.session_state.pop("intel_study_request_synced_id", None)
    st.session_state["intel_manual_last_req_row"] = None
    st.session_state["intel_manual_last_poll_ts"] = 0.0
    st.session_state["intel_manual_poll_cooldown_until"] = 0.0
    if not next_id:
        st.session_state["intel_study_request_ficha"] = ""
        st.session_state["intel_study_request_ficha_name"] = ""
        st.session_state["intel_study_request_notes"] = ""
        return ""

    meta_map = st.session_state.get("intel_study_request_meta_map", {})
    if not isinstance(meta_map, dict):
        meta_map = {}
    next_meta = meta_map.get(next_id, {}) if isinstance(meta_map, dict) else {}
    st.session_state["intel_study_request_ficha"] = _clean_text(next_meta.get("ficha", ""))
    st.session_state["intel_study_request_ficha_name"] = _clean_text(next_meta.get("nombre_ficha", ""))
    st.session_state["intel_study_request_notes"] = _clean_text(next_meta.get("notes", ""))
    return next_id


def _is_pending_study_state(state_value: object) -> bool:
    state = _normalize_column_key(_clean_text(state_value))
    if not state:
        return True
    if "pendiente" in state:
        return True
    return state in {"nuevo", "sin estudio"}


def _enqueue_auto_study_for_fichas(
    fichas: list[str],
    *,
    db_path: str,
    max_queries: int,
    notes: str,
) -> dict[str, object]:
    targets = [str(f).strip() for f in (fichas or []) if str(f).strip()]
    if not targets:
        return {"queued": 0, "already": 0, "errors": [], "request_ids": []}

    current = _ensure_study_state(sync_remote=True)
    records = _normalize_tracking_records(current)
    by_ficha = {str(r.get("ficha", "")).strip(): r for r in records}

    queued = 0
    already = 0
    errors: list[str] = []
    request_ids: list[str] = []
    now_iso = _utc_now_iso()

    from sheets import get_client

    client, _ = get_client()
    _ensure_intel_orquestador_job(client)

    for ficha in targets:
        rec = by_ficha.get(str(ficha).strip())
        if rec is None:
            errors.append(f"{ficha}: no esta en seguimiento")
            continue
        estado_actual = _clean_text(rec.get("estado", ""))
        if not _is_pending_study_state(estado_actual):
            already += 1
            continue

        req_notes = _clean_text(notes)
        try:
            request_id = _append_intel_manual_request(
                client,
                {
                    "ficha": str(ficha),
                    "nombre_ficha": _clean_text(rec.get("nombre_ficha", "")),
                    "db_path": str(db_path or ""),
                    "max_queries": int(max_queries),
                    "notes": req_notes,
                    "headless": False,
                },
            )
            request_ids.append(request_id)
            queued += 1

            rec["estado"] = "en estudio"
            rec["updated_at"] = now_iso
            existing_notes = _clean_text(rec.get("notas", ""))
            auto_marker = f"Auto-estudio encolado request_id={request_id}"
            rec["notas"] = f"{existing_notes} | {auto_marker}".strip(" |")

            _register_study_request_session(
                request_id=request_id,
                ficha=str(ficha),
                nombre_ficha=_clean_text(rec.get("nombre_ficha", "")),
                notes=req_notes,
                set_current_if_empty=True,
            )
        except Exception as exc:
            errors.append(f"{ficha}: {exc}")
            continue

    st.session_state["intel_fichas_estudio"] = _normalize_tracking_records(records)
    ok, msg = _persist_tracking_records(st.session_state["intel_fichas_estudio"])
    st.session_state["intel_tracking_status"] = msg
    st.session_state["intel_tracking_backend"] = "sheets+sqlite" if ok else "sqlite"

    return {
        "queued": queued,
        "already": already,
        "errors": errors,
        "request_ids": request_ids,
        "persist_ok": ok,
        "persist_msg": msg,
    }


def _fetch_intel_manual_request(client, request_id: str) -> dict[str, str] | None:
    sheet_id, _ = _open_sheet_with_fallback(client, _sheet_id_candidates("manual"), "pc_manual")
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(PC_MANUAL_WORKSHEET)
    values = ws.get_all_values()
    if not values:
        return None
    headers = [cell.strip() for cell in values[0]]
    for row in values[1:]:
        row_map = {headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(headers))}
        if str(row_map.get("id", "")).strip() == str(request_id).strip():
            return row_map
    return None


def _is_sheets_quota_error(exc: Exception) -> bool:
    txt = _normalize_column_key(str(exc))
    return (
        "429" in txt
        or "quota exceeded" in txt
        or "read requests per minute" in txt
        or "rate_limit_exceeded" in txt
    )


def _read_remote_study_data(request_id: str) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    try:
        from sheets import get_client, read_worksheet
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), f"No se pudo importar modulo Sheets: {exc}"

    try:
        client, _ = get_client()
        sheet_id, _ = _open_sheet_with_fallback(client, _sheet_id_candidates("intel"), "estudio remoto")
        runs = read_worksheet(client, sheet_id, INTEL_REMOTE_RUNS_WORKSHEET)
        detail = read_worksheet(client, sheet_id, INTEL_REMOTE_DETAIL_WORKSHEET)
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), f"No se pudieron leer hojas remotas de estudio: {exc}"

    if runs.empty:
        return pd.DataFrame(), pd.DataFrame(), "No hay corridas remotas en Sheets."
    if "request_id" not in runs.columns:
        return pd.DataFrame(), pd.DataFrame(), "La hoja de runs remotos no contiene columna request_id."

    runs = runs[runs["request_id"].astype(str).str.strip() == str(request_id).strip()].copy()
    if runs.empty:
        return pd.DataFrame(), pd.DataFrame(), f"No hay corrida remota para request_id={request_id}."

    sort_col = "fecha_fin" if "fecha_fin" in runs.columns else runs.columns[0]
    runs = runs.sort_values(sort_col, ascending=False)
    run_row = runs.head(1).copy()
    run_id_remote = _clean_text(run_row.iloc[0].get("run_id_remote", ""))
    if detail.empty:
        return run_row, pd.DataFrame(), ""
    if "run_id_remote" in detail.columns and run_id_remote:
        detail = detail[detail["run_id_remote"].astype(str).str.strip() == run_id_remote].copy()
    elif "request_id" in detail.columns:
        detail = detail[detail["request_id"].astype(str).str.strip() == str(request_id).strip()].copy()
    return run_row, detail, ""


def _is_desierto_value(value: object) -> bool:
    txt = _normalize_column_key(_clean_text(value))
    return "desierto" in txt


def _normalize_remote_detail_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return detail_df.copy() if isinstance(detail_df, pd.DataFrame) else pd.DataFrame()

    df = detail_df.copy()
    for col in [
        "ficha",
        "nombre_ficha",
        "detail_id",
        "acto_id",
        "acto_nombre",
        "acto_url",
        "entidad",
        "renglon_texto",
        "proveedor",
        "proveedor_ganador",
        "marca",
        "modelo",
        "pais_origen",
        "fecha_publicacion",
        "fecha_celebracion",
        "fecha_adjudicacion",
        "fecha_orden_compra",
        "tipo_flujo",
        "fuente_precio",
        "fuente_fecha",
        "enlace_evidencia",
        "unidad_medida",
        "observaciones",
        "estado_revision",
    ]:
        if col in df.columns:
            df[col] = df[col].map(_clean_text)

    if "acto_url" in df.columns:
        df["acto_url"] = df["acto_url"].map(_to_absolute_panamacompra_url)
    if "enlace_evidencia" in df.columns:
        df["enlace_evidencia"] = df["enlace_evidencia"].map(_to_absolute_panamacompra_url)
    if "acto_url" in df.columns and "enlace_evidencia" in df.columns:
        empty_ev = df["enlace_evidencia"].astype(str).str.strip() == ""
        df.loc[empty_ev, "enlace_evidencia"] = df.loc[empty_ev, "acto_url"]

    numeric_cols = [
        "cantidad",
        "dias_acto_a_oc",
        "dias_acto_a_oc_mas_entrega",
        "tiempo_entrega_dias",
        "nivel_certeza",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].map(lambda x: round(_safe_float(x, 0.0), 6))

    if "requiere_revision" in df.columns:
        df["requiere_revision"] = df["requiere_revision"].map(
            lambda x: 1 if _as_bool(x) or _safe_int(x) > 0 else 0
        )

    if "es_ganador" in df.columns:
        df["es_ganador"] = df["es_ganador"].map(lambda x: 1 if _as_bool(x) or _safe_int(x) > 0 else 0)

    for col in ["precio_unitario_participacion", "precio_unitario_referencia"]:
        if col not in df.columns:
            continue

        def _norm_price(v: object) -> object:
            if _is_desierto_value(v):
                return "desierto"
            txt = _clean_text(v)
            if not txt:
                return ""
            num = _safe_float(txt, 0.0)
            if num > 0:
                return round(num, 6)
            # Preserva ceros explícitos (0, 0.0, 0,00) y evita romper textos no vacíos.
            if txt in {"0", "0.0", "0,0", "0,00", "0.0000"}:
                return 0.0
            return txt

        df[col] = df[col].map(_norm_price)

    # Reglas robustas para corridas desiertas:
    # - unit y fecha OC deben quedar "desierto"
    # - referencia: numérica si existe; de lo contrario "desierto"
    # - no requiere revisión
    if "precio_unitario_participacion" in df.columns and "fecha_orden_compra" in df.columns:
        for idx, row in df.iterrows():
            is_desierto_row = (
                _is_desierto_value(row.get("estado_revision", ""))
                or _is_desierto_value(row.get("precio_unitario_participacion", ""))
                or _is_desierto_value(row.get("fecha_orden_compra", ""))
            )
            if not is_desierto_row:
                continue
            df.at[idx, "precio_unitario_participacion"] = "desierto"
            df.at[idx, "fecha_orden_compra"] = "desierto"
            if "precio_unitario_referencia" in df.columns:
                pref_val = row.get("precio_unitario_referencia", "")
                pref_num = _safe_float(pref_val, 0.0)
                df.at[idx, "precio_unitario_referencia"] = round(pref_num, 6) if pref_num > 0 else "desierto"
            if "estado_revision" in df.columns:
                df.at[idx, "estado_revision"] = "desierto"
            if "requiere_revision" in df.columns:
                df.at[idx, "requiere_revision"] = 0
            if "nivel_certeza" in df.columns:
                df.at[idx, "nivel_certeza"] = max(_safe_float(row.get("nivel_certeza", 0.0), 0.0), 0.99)

    return df


def _sync_remote_run_to_local(
    request_id: str,
    fallback_ficha: str,
    fallback_nombre: str,
    db_source: str,
    notes: str = "",
) -> tuple[bool, str, str]:
    run_df, detail_df, err = _read_remote_study_data(request_id)
    if err:
        return False, err, ""
    if run_df.empty:
        return False, "Run remoto vacio.", ""

    ficha = _clean_text(run_df.iloc[0].get("ficha", "")) or _clean_text(fallback_ficha)
    ficha_name = _clean_text(run_df.iloc[0].get("nombre_ficha", "")) or _clean_text(fallback_nombre)
    if not ficha:
        return False, "El run remoto no contiene ficha.", ""

    if detail_df.empty:
        detail_df = pd.DataFrame(
            columns=[
                "detail_id",
                "ficha",
                "nombre_ficha",
                "acto_id",
                "acto_nombre",
                "acto_url",
                "entidad",
                "renglon_texto",
                "proveedor",
                "proveedor_ganador",
                "es_ganador",
                "marca",
                "modelo",
                "pais_origen",
                "cantidad",
                "precio_unitario_participacion",
                "precio_unitario_referencia",
                "fecha_publicacion",
                "fecha_celebracion",
                "fecha_adjudicacion",
                "fecha_orden_compra",
                "dias_acto_a_oc",
                "dias_acto_a_oc_mas_entrega",
                "tipo_flujo",
                "fuente_precio",
                "fuente_fecha",
                "enlace_evidencia",
                "unidad_medida",
                "tiempo_entrega_dias",
                "observaciones",
                "estado_revision",
                "nivel_certeza",
                "requiere_revision",
            ]
        )

    keep_cols = [
        "detail_id",
        "ficha",
        "nombre_ficha",
        "acto_id",
        "acto_nombre",
        "acto_url",
        "entidad",
        "renglon_texto",
        "proveedor",
        "proveedor_ganador",
        "es_ganador",
        "marca",
        "modelo",
        "pais_origen",
        "cantidad",
        "precio_unitario_participacion",
        "precio_unitario_referencia",
        "fecha_publicacion",
        "fecha_celebracion",
        "fecha_adjudicacion",
        "fecha_orden_compra",
        "dias_acto_a_oc",
        "dias_acto_a_oc_mas_entrega",
        "tipo_flujo",
        "fuente_precio",
        "fuente_fecha",
        "enlace_evidencia",
        "unidad_medida",
        "tiempo_entrega_dias",
        "observaciones",
        "estado_revision",
        "nivel_certeza",
        "requiere_revision",
    ]
    for col in keep_cols:
        if col not in detail_df.columns:
            detail_df[col] = ""
    detail_df = detail_df[keep_cols].copy()
    detail_df = _normalize_remote_detail_df(detail_df)
    detail_df["ficha"] = detail_df["ficha"].astype(str).replace("", ficha)
    detail_df["nombre_ficha"] = detail_df["nombre_ficha"].astype(str).replace("", ficha_name)
    detail_df["detail_id"] = detail_df["detail_id"].astype(str)
    missing_detail = detail_df["detail_id"].str.strip() == ""
    if missing_detail.any():
        detail_df.loc[missing_detail, "detail_id"] = [str(uuid.uuid4()) for _ in range(int(missing_detail.sum()))]

    consultas_df = pd.DataFrame()
    run_id = _save_study_run(
        ficha=ficha,
        ficha_name=ficha_name,
        detail_df=detail_df,
        consultas_df=consultas_df,
        db_source=db_source,
        notes=(notes or f"Sincronizado desde orquestador request_id={request_id}"),
    )
    return True, "Resultados del orquestador sincronizados y guardados en local.", run_id


def _as_bool(value: object) -> bool:
    text = _clean_text(value).lower()
    return text in INTEL_BOOL_TRUE


def _normalize_tracking_record(record: dict[str, object]) -> dict[str, object]:
    now_iso = _utc_now_iso()
    out: dict[str, object] = {key: "" for key in INTEL_TRACKING_COLUMNS}
    out["ficha"] = _clean_text(record.get("ficha"))
    out["nombre_ficha"] = _clean_text(record.get("nombre_ficha"))
    out["clase_riesgo"] = _clean_text(record.get("clase_riesgo"))
    out["enlace_minsa"] = _clean_text(record.get("enlace_minsa"))
    out["score_inicial"] = round(_safe_float(record.get("score_inicial")), 4)
    out["clasificacion"] = _clean_text(record.get("clasificacion"))
    out["actos"] = _safe_int(record.get("actos"))
    out["actos_solo_ficha"] = _safe_int(record.get("actos_solo_ficha"))
    out["actos_con_otras_fichas"] = _safe_int(record.get("actos_con_otras_fichas"))
    out["monto_historico"] = round(_safe_float(record.get("monto_historico")), 6)
    out["proponentes_promedio"] = round(_safe_float(record.get("proponentes_promedio")), 6)
    raw_revision = record.get("revision_proponentes", False)
    if isinstance(raw_revision, str):
        out["revision_proponentes"] = _as_bool(raw_revision)
    else:
        out["revision_proponentes"] = bool(raw_revision)
    out["top1_ganador"] = _clean_text(record.get("top1_ganador"))
    out["top1_pct_ganadas"] = round(_safe_float(record.get("top1_pct_ganadas")), 4)
    out["top2_ganador"] = _clean_text(record.get("top2_ganador"))
    out["top2_pct_ganadas"] = round(_safe_float(record.get("top2_pct_ganadas")), 4)
    out["top3_ganador"] = _clean_text(record.get("top3_ganador"))
    out["top3_pct_ganadas"] = round(_safe_float(record.get("top3_pct_ganadas")), 4)
    out["estado"] = _clean_text(record.get("estado")) or "pendiente de estudio profundo"
    out["fecha_ingreso"] = _clean_text(record.get("fecha_ingreso")) or date.today().isoformat()
    out["notas"] = _clean_text(record.get("notas"))
    out["created_at"] = _clean_text(record.get("created_at")) or now_iso
    out["updated_at"] = _clean_text(record.get("updated_at")) or now_iso
    return out


def _normalize_tracking_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    by_ficha: dict[str, dict[str, object]] = {}
    for raw in records or []:
        norm = _normalize_tracking_record(raw or {})
        ficha = str(norm.get("ficha", "")).strip()
        if not ficha:
            continue
        prev = by_ficha.get(ficha)
        if prev is None:
            by_ficha[ficha] = norm
            continue
        prev_updated = str(prev.get("updated_at", "") or "")
        curr_updated = str(norm.get("updated_at", "") or "")
        by_ficha[ficha] = norm if curr_updated >= prev_updated else prev

    normalized = list(by_ficha.values())
    normalized.sort(key=lambda r: (str(r.get("fecha_ingreso", "")), str(r.get("ficha", ""))))
    return normalized


def _tracking_records_to_df(records: list[dict[str, object]]) -> pd.DataFrame:
    normalized = _normalize_tracking_records(records)
    if not normalized:
        return pd.DataFrame(columns=INTEL_TRACKING_COLUMNS)
    df = pd.DataFrame(normalized)
    for col in INTEL_TRACKING_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[INTEL_TRACKING_COLUMNS]


def _read_tracking_records_from_sqlite() -> list[dict[str, object]]:
    try:
        _ensure_study_db()
        with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM seguimiento_fichas ORDER BY datetime(fecha_ingreso) ASC, ficha ASC",
                conn,
            )
    except Exception:
        return []
    if df.empty:
        return []
    records = df.to_dict(orient="records")
    return _normalize_tracking_records(records)


def _write_tracking_records_to_sqlite(records: list[dict[str, object]]) -> None:
    _ensure_study_db()
    normalized = _normalize_tracking_records(records)
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        conn.execute("DELETE FROM seguimiento_fichas")
        if normalized:
            rows = [
                tuple(rec.get(col, "") for col in INTEL_TRACKING_COLUMNS)
                for rec in normalized
            ]
            placeholders = ",".join(["?"] * len(INTEL_TRACKING_COLUMNS))
            conn.executemany(
                f"INSERT INTO seguimiento_fichas ({','.join(INTEL_TRACKING_COLUMNS)}) VALUES ({placeholders})",
                rows,
            )
        conn.commit()


@st.cache_data(show_spinner=False, ttl=120)
def _read_tracking_records_from_sheets_cached(sheet_id: str) -> tuple[list[dict[str, object]], str]:
    if not sheet_id:
        return [], "SHEET_ID no configurado para seguimiento."
    try:
        from gspread.exceptions import WorksheetNotFound
        from sheets import get_client, read_worksheet, write_worksheet
    except Exception as exc:
        return [], f"Modulo Sheets no disponible: {exc}"

    try:
        client, _ = get_client()
        sh = client.open_by_key(sheet_id)
        try:
            sh.worksheet(INTEL_TRACKING_WORKSHEET)
        except WorksheetNotFound:
            sh.add_worksheet(title=INTEL_TRACKING_WORKSHEET, rows=200, cols=max(len(INTEL_TRACKING_COLUMNS), 10))
            write_worksheet(
                client,
                sheet_id,
                INTEL_TRACKING_WORKSHEET,
                pd.DataFrame(columns=INTEL_TRACKING_COLUMNS),
            )
        df = read_worksheet(client, sheet_id, INTEL_TRACKING_WORKSHEET)
        for col in INTEL_TRACKING_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        records = df[INTEL_TRACKING_COLUMNS].to_dict(orient="records")
        return _normalize_tracking_records(records), ""
    except Exception as exc:
        return [], str(exc)


def _read_tracking_records_from_sheets() -> tuple[list[dict[str, object]], str]:
    sheet_id = _intel_sheet_id()
    return _read_tracking_records_from_sheets_cached(sheet_id)


def _write_tracking_records_to_sheets(records: list[dict[str, object]]) -> str:
    sheet_id = _intel_sheet_id()
    if not sheet_id:
        return "SHEET_ID no configurado para seguimiento."
    try:
        from gspread.exceptions import WorksheetNotFound
        from sheets import get_client, write_worksheet
    except Exception as exc:
        return f"Modulo Sheets no disponible: {exc}"

    try:
        client, _ = get_client()
        sh = client.open_by_key(sheet_id)
        try:
            sh.worksheet(INTEL_TRACKING_WORKSHEET)
        except WorksheetNotFound:
            sh.add_worksheet(title=INTEL_TRACKING_WORKSHEET, rows=200, cols=max(len(INTEL_TRACKING_COLUMNS), 10))
        df = _tracking_records_to_df(records)
        write_worksheet(client, sheet_id, INTEL_TRACKING_WORKSHEET, df)
        _read_tracking_records_from_sheets_cached.clear()
        return ""
    except Exception as exc:
        return str(exc)


def _load_tracking_records_persistent() -> tuple[list[dict[str, object]], str, str]:
    sqlite_records = _read_tracking_records_from_sqlite()
    sheet_records, sheet_error = _read_tracking_records_from_sheets()
    merged = _normalize_tracking_records(sqlite_records + sheet_records)

    if merged and merged != sqlite_records:
        _write_tracking_records_to_sqlite(merged)

    backend = "sqlite"
    status = "Seguimiento cargado desde SQLite local."
    if not sheet_error:
        backend = "sheets+sqlite"
        status = "Seguimiento cargado desde Sheets y sincronizado localmente."
        if merged and merged != sheet_records:
            sync_err = _write_tracking_records_to_sheets(merged)
            if sync_err:
                status = f"Seguimiento cargado (local) pero Sheets no sincronizo: {sync_err}"
    elif sheet_error:
        status = f"Seguimiento cargado localmente (Sheets no disponible: {sheet_error})"

    return merged, backend, status


def _persist_tracking_records(records: list[dict[str, object]]) -> tuple[bool, str]:
    normalized = _normalize_tracking_records(records)
    try:
        _write_tracking_records_to_sqlite(normalized)
    except Exception as exc:
        return False, f"No se pudo guardar seguimiento en SQLite: {exc}"

    sheet_error = _write_tracking_records_to_sheets(normalized)
    if sheet_error:
        return False, f"Guardado en local (SQLite). Sheets no disponible: {sheet_error}"
    return True, "Guardado en Sheets y SQLite."


def _update_tracking_state(
    ficha: str,
    new_state: str,
    note_suffix: str = "",
) -> tuple[bool, str]:
    ficha_key = _clean_text(ficha)
    if not ficha_key:
        return False, "Ficha vacia para actualizar estado."
    records = _ensure_study_state(sync_remote=True)
    changed = False
    now_iso = _utc_now_iso()
    for rec in records:
        if _clean_text(rec.get("ficha", "")) != ficha_key:
            continue
        rec["estado"] = _clean_text(new_state) or rec.get("estado", "")
        rec["updated_at"] = now_iso
        if note_suffix:
            existing = _clean_text(rec.get("notas", ""))
            rec["notas"] = f"{existing} | {note_suffix}".strip(" |")
        changed = True
        break
    if not changed:
        return False, f"Ficha {ficha_key} no encontrada en seguimiento."
    st.session_state["intel_fichas_estudio"] = _normalize_tracking_records(records)
    ok, msg = _persist_tracking_records(st.session_state["intel_fichas_estudio"])
    st.session_state["intel_tracking_status"] = msg
    st.session_state["intel_tracking_backend"] = "sheets+sqlite" if ok else "sqlite"
    return ok, msg


def _sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    cols = [str(r[1]).strip() for r in rows if len(r) > 1 and str(r[1]).strip()]
    return cols


def _normalize_table_df(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    work = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    for col in columns:
        if col not in work.columns:
            work[col] = ""
    work = work[columns].copy()
    work = work.where(pd.notna(work), "")
    return work


def _safe_read_sqlite_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[str],
) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception:
        df = pd.DataFrame(columns=columns)
    return _normalize_table_df(df, columns)


def _replace_sqlite_table_rows(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    df: pd.DataFrame,
) -> None:
    conn.execute(f"DELETE FROM {table_name}")
    write_df = _normalize_table_df(df, columns)
    if not write_df.empty:
        write_df.to_sql(table_name, conn, if_exists="append", index=False)


def _latest_timestamp_in_df(df: pd.DataFrame) -> pd.Timestamp:
    if df is None or df.empty:
        return pd.NaT
    ts_cols = [
        "updated_at",
        "created_at",
        "ultima_actualizacion",
        "fecha_ultima_actualizacion",
        "fecha_contexto_generado",
        "fecha_carga",
        "fecha_inicio",
        "fecha_fin",
    ]
    latest = pd.NaT
    for col in ts_cols:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        if parsed.isna().all():
            continue
        candidate = parsed.max()
        if pd.isna(latest) or (not pd.isna(candidate) and candidate > latest):
            latest = candidate
    return latest


def _study_payload_meta(payload: dict[str, pd.DataFrame]) -> tuple[int, pd.Timestamp]:
    rows_total = 0
    latest = pd.NaT
    for df in payload.values():
        if not isinstance(df, pd.DataFrame):
            continue
        rows_total += int(len(df))
        candidate = _latest_timestamp_in_df(df)
        if pd.isna(latest) or (not pd.isna(candidate) and candidate > latest):
            latest = candidate
    return rows_total, latest


def _load_study_payload_from_sqlite(table_names: list[str]) -> dict[str, pd.DataFrame]:
    _ensure_study_db()
    out: dict[str, pd.DataFrame] = {}
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        for table_name in table_names:
            cols = _sqlite_table_columns(conn, table_name)
            out[table_name] = _safe_read_sqlite_table(conn, table_name, cols)
    return out


def _read_study_payload_from_sheets(table_names: list[str]) -> tuple[dict[str, pd.DataFrame], str, str]:
    try:
        from gspread.exceptions import WorksheetNotFound
        from sheets import get_client, read_worksheet
    except Exception as exc:
        return {}, "", f"Modulo Sheets no disponible: {exc}"

    try:
        client, _ = get_client()
        sheet_id, sh = _open_sheet_with_fallback(
            client,
            _sheet_id_candidates("intel"),
            "intel_estudios_persistencia",
        )
    except Exception as exc:
        return {}, "", str(exc)

    out: dict[str, pd.DataFrame] = {}
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        for table_name in table_names:
            ws_name = INTEL_STUDY_SHEETS_TABLE_MAP.get(table_name, "")
            cols = _sqlite_table_columns(conn, table_name)
            if not ws_name:
                out[table_name] = pd.DataFrame(columns=cols)
                continue
            try:
                sh.worksheet(ws_name)
            except WorksheetNotFound:
                out[table_name] = pd.DataFrame(columns=cols)
                continue
            try:
                df = read_worksheet(client, sheet_id, ws_name)
            except Exception:
                df = pd.DataFrame(columns=cols)
            out[table_name] = _normalize_table_df(df, cols)
    return out, sheet_id, ""


def _write_study_payload_to_sheets(table_names: list[str]) -> tuple[bool, str]:
    if not table_names:
        return True, "Sin tablas para sincronizar."
    try:
        from sheets import get_client, write_worksheet
    except Exception as exc:
        return False, f"Modulo Sheets no disponible: {exc}"

    try:
        client, _ = get_client()
        sheet_id, _ = _open_sheet_with_fallback(
            client,
            _sheet_id_candidates("intel"),
            "intel_estudios_persistencia",
        )
    except Exception as exc:
        return False, str(exc)

    sync_tables = [t for t in table_names if t in INTEL_STUDY_SHEETS_TABLE_MAP]
    if not sync_tables:
        return True, "Sin tablas mapeadas para sincronizar."

    try:
        with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
            for table_name in sync_tables:
                ws_name = INTEL_STUDY_SHEETS_TABLE_MAP[table_name]
                cols = _sqlite_table_columns(conn, table_name)
                _ensure_ws_headers(client, sheet_id, ws_name, cols)
                df = _safe_read_sqlite_table(conn, table_name, cols)
                write_worksheet(client, sheet_id, ws_name, df)
    except Exception as exc:
        return False, str(exc)
    return True, f"Persistencia en Sheets completada ({len(sync_tables)} tablas)."


def _replace_local_study_payload(payload: dict[str, pd.DataFrame], table_names: list[str]) -> None:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        for table_name in table_names:
            cols = _sqlite_table_columns(conn, table_name)
            df = payload.get(table_name, pd.DataFrame(columns=cols))
            _replace_sqlite_table_rows(conn, table_name, cols, df)
        conn.commit()


def _sync_study_storage_with_sheets() -> tuple[bool, str]:
    _ensure_study_db()
    table_names = list(INTEL_STUDY_SHEETS_TABLE_MAP.keys())
    local_payload = _load_study_payload_from_sqlite(table_names)
    local_rows, local_latest = _study_payload_meta(local_payload)

    remote_payload, _, remote_err = _read_study_payload_from_sheets(table_names)
    if remote_err:
        return False, f"Persistencia estudios local (Sheets no disponible): {remote_err}"
    remote_rows, remote_latest = _study_payload_meta(remote_payload)

    if local_rows <= 0 and remote_rows <= 0:
        return True, "Persistencia estudios habilitada (sin datos aun)."

    if remote_rows <= 0 < local_rows:
        ok, msg = _write_study_payload_to_sheets(table_names)
        if ok:
            return True, "Estudios locales sembrados en Sheets."
        return False, f"Estudios locales en SQLite; no se pudo sembrar en Sheets: {msg}"

    if local_rows <= 0 < remote_rows:
        _replace_local_study_payload(remote_payload, table_names)
        _bump_study_data_rev()
        return True, "Estudios cargados desde Sheets (persistente)."

    pull_remote = False
    push_local = False
    if pd.isna(local_latest) and not pd.isna(remote_latest):
        pull_remote = True
    elif pd.isna(remote_latest) and not pd.isna(local_latest):
        push_local = True
    elif not pd.isna(local_latest) and not pd.isna(remote_latest):
        if remote_latest > local_latest:
            pull_remote = True
        elif local_latest > remote_latest:
            push_local = True
        elif remote_rows > local_rows:
            pull_remote = True
        elif local_rows > remote_rows:
            push_local = True

    if pull_remote:
        _replace_local_study_payload(remote_payload, table_names)
        _bump_study_data_rev()
        return True, "Estudios sincronizados desde Sheets."

    if push_local:
        ok, msg = _write_study_payload_to_sheets(table_names)
        if ok:
            return True, "Estudios sincronizados hacia Sheets."
        return False, f"Estudios locales actualizados; fallo sync a Sheets: {msg}"

    return True, "Estudios ya sincronizados entre SQLite y Sheets."


def _persist_study_tables_after_write(table_names: list[str], reason: str = "") -> None:
    unique_tables = list(dict.fromkeys([t for t in table_names if t in INTEL_STUDY_SHEETS_TABLE_MAP]))
    if not unique_tables:
        return
    ok, msg = _write_study_payload_to_sheets(unique_tables)
    prefix = f"{reason}: " if reason else ""
    if ok:
        st.session_state["intel_study_persist_status"] = prefix + msg
        st.session_state["intel_study_persist_backend"] = "sheets+sqlite"
    else:
        st.session_state["intel_study_persist_status"] = prefix + f"Persistencia parcial (SQLite): {msg}"
        st.session_state["intel_study_persist_backend"] = "sqlite"


def _bootstrap_study_storage_once() -> None:
    if st.session_state.get("intel_study_storage_loaded", False):
        return
    ok, msg = _sync_study_storage_with_sheets()
    st.session_state["intel_study_persist_status"] = msg
    st.session_state["intel_study_persist_backend"] = "sheets+sqlite" if ok else "sqlite"
    st.session_state["intel_study_storage_loaded"] = True


def _ensure_study_db() -> None:
    INTEL_STUDY_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS estudio_runs (
                run_id TEXT PRIMARY KEY,
                ficha TEXT NOT NULL,
                nombre_ficha TEXT,
                estado_run TEXT NOT NULL,
                fecha_inicio TEXT,
                fecha_fin TEXT,
                fuente_db TEXT,
                version_modelo TEXT,
                total_items INTEGER DEFAULT 0,
                total_consultas INTEGER DEFAULT 0,
                consultas_resueltas INTEGER DEFAULT 0,
                resumen_ia TEXT,
                notas TEXT,
                is_current INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS estudio_detalle (
                detail_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                ficha TEXT NOT NULL,
                nombre_ficha TEXT,
                acto_id TEXT,
                acto_nombre TEXT,
                acto_url TEXT,
                entidad TEXT,
                renglon_texto TEXT,
                proveedor TEXT,
                proveedor_ganador TEXT,
                es_ganador INTEGER DEFAULT 0,
                marca TEXT,
                modelo TEXT,
                pais_origen TEXT,
                cantidad REAL,
                precio_unitario_participacion REAL,
                precio_unitario_referencia REAL,
                fecha_publicacion TEXT,
                fecha_celebracion TEXT,
                fecha_adjudicacion TEXT,
                fecha_orden_compra TEXT,
                dias_acto_a_oc REAL,
                dias_acto_a_oc_mas_entrega REAL,
                tipo_flujo TEXT,
                fuente_precio TEXT,
                fuente_fecha TEXT,
                enlace_evidencia TEXT,
                unidad_medida TEXT,
                tiempo_entrega_dias REAL,
                observaciones TEXT,
                estado_revision TEXT DEFAULT 'pendiente',
                nivel_certeza REAL DEFAULT 0.0,
                requiere_revision INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS estudio_consultas (
                consulta_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                ficha TEXT NOT NULL,
                detail_id TEXT,
                acto_id TEXT,
                campo_dudoso TEXT,
                evidencia TEXT,
                opciones_json TEXT,
                respuesta_seleccionada TEXT,
                valor_manual TEXT,
                estado TEXT DEFAULT 'pendiente',
                obligatoria INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS estudio_resumen_ficha (
                ficha TEXT PRIMARY KEY,
                run_id_vigente TEXT,
                estado_estudio TEXT,
                ultima_actualizacion TEXT,
                total_actos INTEGER DEFAULT 0,
                total_renglones INTEGER DEFAULT 0,
                empresas_ganadoras INTEGER DEFAULT 0,
                marcas_json TEXT,
                modelos_json TEXT,
                paises_json TEXT,
                precio_participacion_prom REAL,
                precio_participacion_min REAL,
                fecha_precio_min TEXT,
                precio_participacion_max REAL,
                fecha_precio_max TEXT,
                precio_referencia_prom REAL,
                top_menor_precio_json TEXT,
                resumen_ia TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seguimiento_fichas (
                ficha TEXT PRIMARY KEY,
                nombre_ficha TEXT,
                clase_riesgo TEXT,
                enlace_minsa TEXT,
                score_inicial REAL,
                clasificacion TEXT,
                actos INTEGER,
                actos_solo_ficha INTEGER,
                actos_con_otras_fichas INTEGER,
                monto_historico REAL,
                proponentes_promedio REAL,
                revision_proponentes INTEGER DEFAULT 0,
                top1_ganador TEXT,
                top1_pct_ganadas REAL,
                top2_ganador TEXT,
                top2_pct_ganadas REAL,
                top3_ganador TEXT,
                top3_pct_ganadas REAL,
                estado TEXT,
                fecha_ingreso TEXT,
                notas TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_estudio_runs_ficha ON estudio_runs(ficha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_estudio_detalle_run ON estudio_detalle(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_estudio_consultas_run ON estudio_consultas(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_seguimiento_fichas_estado ON seguimiento_fichas(estado)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analisis_proveedores_contexto (
                ficha TEXT PRIMARY KEY,
                run_id_estudio TEXT,
                nombre_ficha TEXT,
                enlace_ficha_tecnica TEXT,
                descripcion_producto TEXT,
                palabras_clave TEXT,
                marcas_detectadas_historicamente TEXT,
                modelos_detectados_historicamente TEXT,
                paises_detectados_historicamente TEXT,
                proveedores_historicos_detectados TEXT,
                resumen_estudio_breve TEXT,
                contexto_texto TEXT,
                prompt_texto TEXT,
                estado_analisis TEXT,
                analisis_id_activo TEXT,
                fecha_contexto_generado TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analisis_proveedores_version (
                analisis_id TEXT PRIMARY KEY,
                ficha TEXT NOT NULL,
                version_num INTEGER DEFAULT 1,
                metadata_consulta_json TEXT,
                contexto_ficha_json TEXT,
                resumen_ejecutivo TEXT,
                json_raw TEXT,
                estado_version TEXT,
                fecha_carga TEXT,
                fecha_ultima_actualizacion TEXT,
                created_at TEXT,
                updated_at TEXT,
                is_active INTEGER DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analisis_proveedores_hist_panama (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analisis_id TEXT NOT NULL,
                ficha TEXT NOT NULL,
                proveedor TEXT,
                marca TEXT,
                modelo TEXT,
                pais_origen TEXT,
                cantidad_actos_ganados REAL,
                precio_promedio_historico REAL,
                precio_minimo_historico REAL,
                precio_maximo_historico REAL,
                telefono TEXT,
                contacto_email TEXT,
                contacto_whatsapp TEXT,
                canal_contacto_mas_probable TEXT,
                correo_inicial_listo TEXT,
                whatsapp_inicial_listo TEXT,
                observaciones TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analisis_proveedores_mejor_gama (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analisis_id TEXT NOT NULL,
                ficha TEXT NOT NULL,
                proveedor_o_fabricante TEXT,
                marca TEXT,
                modelo TEXT,
                pais_origen TEXT,
                sitio_web TEXT,
                telefono TEXT,
                contacto_email TEXT,
                contacto_whatsapp TEXT,
                canal_contacto_mas_probable TEXT,
                razon_clasificacion TEXT,
                correo_inicial_listo TEXT,
                whatsapp_inicial_listo TEXT,
                observaciones TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analisis_proveedores_mejor_precio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analisis_id TEXT NOT NULL,
                ficha TEXT NOT NULL,
                proveedor_o_fabricante TEXT,
                marca TEXT,
                modelo TEXT,
                pais_origen TEXT,
                sitio_web TEXT,
                telefono TEXT,
                contacto_email TEXT,
                contacto_whatsapp TEXT,
                canal_contacto_mas_probable TEXT,
                rango_precio_referencial TEXT,
                razon_clasificacion TEXT,
                correo_inicial_listo TEXT,
                whatsapp_inicial_listo TEXT,
                observaciones TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analisis_proveedores_comentarios (
                comentario_id TEXT PRIMARY KEY,
                ficha TEXT NOT NULL,
                analisis_id TEXT,
                run_id_estudio TEXT,
                usuario TEXT,
                comentario TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_contexto_estado ON analisis_proveedores_contexto(estado_analisis)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_version_ficha ON analisis_proveedores_version(ficha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_version_active ON analisis_proveedores_version(ficha, is_active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_hist_analisis ON analisis_proveedores_hist_panama(analisis_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_gama_analisis ON analisis_proveedores_mejor_gama(analisis_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_precio_analisis ON analisis_proveedores_mejor_precio(analisis_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ap_comentarios_ficha ON analisis_proveedores_comentarios(ficha, created_at)")

        # Backward compatibility: agrega columnas nuevas si la DB ya existía.
        existing_cols = {
            str(row[1] or "").strip().lower()
            for row in conn.execute("PRAGMA table_info(estudio_detalle)").fetchall()
        }
        required_cols = [
            ("dias_acto_a_oc_mas_entrega", "REAL"),
            ("tipo_flujo", "TEXT"),
            ("fuente_precio", "TEXT"),
            ("fuente_fecha", "TEXT"),
            ("enlace_evidencia", "TEXT"),
            ("unidad_medida", "TEXT"),
            ("tiempo_entrega_dias", "REAL"),
        ]
        for col_name, col_type in required_cols:
            if col_name.lower() not in existing_cols:
                conn.execute(f"ALTER TABLE estudio_detalle ADD COLUMN {col_name} {col_type}")

        ap_ctx_cols = {
            str(row[1] or "").strip().lower()
            for row in conn.execute("PRAGMA table_info(analisis_proveedores_contexto)").fetchall()
        }
        ap_ctx_required = [
            ("analisis_id_activo", "TEXT"),
            ("fecha_contexto_generado", "TEXT"),
            ("estado_analisis", "TEXT"),
            ("prompt_texto", "TEXT"),
        ]
        for col_name, col_type in ap_ctx_required:
            if col_name.lower() not in ap_ctx_cols:
                conn.execute(f"ALTER TABLE analisis_proveedores_contexto ADD COLUMN {col_name} {col_type}")

        conn.commit()


def _pick_alias_value(row: pd.Series, aliases: list[str]) -> str:
    if row is None:
        return ""
    for alias in aliases:
        if alias in row.index:
            value = _clean_text(row.get(alias))
            if value:
                return value
    normalized_cols = {_normalize_column_key(c): c for c in row.index.tolist()}
    for alias in aliases:
        hit = normalized_cols.get(_normalize_column_key(alias))
        if hit:
            value = _clean_text(row.get(hit))
            if value:
                return value
    return ""


def _extract_estudio_dates(row: pd.Series) -> dict[str, str]:
    fecha_publicacion = _pick_alias_value(row, ["fecha_publicacion", "publicacion", "fecha"])
    fecha_celebracion = _pick_alias_value(row, ["fecha_celebracion", "celebracion", "fecha_acto"])
    fecha_adjudicacion = _pick_alias_value(row, ["fecha_adjudicacion", "adjudicacion"])
    fecha_oc = _pick_alias_value(
        row,
        [
            "fecha_orden_compra",
            "fecha orden compra",
            "orden_compra_fecha",
            "fecha_oc",
            "fecha de orden de compra",
            "fecha_posterior",
        ],
    )
    tiempo_entrega_dias = _extract_delivery_days(
        _pick_alias_value(
            row,
            [
                "termino_entrega",
                "término_entrega",
                "tiempo_entrega",
                "tiempo de entrega",
            ],
        )
    )

    d_cele = _parse_any_date(fecha_celebracion)
    d_oc = _parse_any_date(fecha_oc)
    dias = 0.0
    if not pd.isna(d_cele) and not pd.isna(d_oc):
        dias = float((d_oc - d_cele).days)
    return {
        "fecha_publicacion": fecha_publicacion,
        "fecha_celebracion": fecha_celebracion,
        "fecha_adjudicacion": fecha_adjudicacion,
        "fecha_orden_compra": fecha_oc,
        "dias_acto_a_oc": dias,
        "tiempo_entrega_dias": float(tiempo_entrega_dias),
        "dias_acto_a_oc_mas_entrega": float(dias + max(0.0, float(tiempo_entrega_dias))),
    }


def _recompute_days_act_to_oc(dates: dict[str, str]) -> dict[str, str]:
    d_cele = _parse_any_date(dates.get("fecha_celebracion", ""))
    d_oc = _parse_any_date(dates.get("fecha_orden_compra", ""))
    if not pd.isna(d_cele) and not pd.isna(d_oc):
        dates["dias_acto_a_oc"] = float((d_oc - d_cele).days)
    else:
        dates["dias_acto_a_oc"] = 0.0
    entrega = float(_safe_float(dates.get("tiempo_entrega_dias", 0.0), 0.0))
    dates["dias_acto_a_oc_mas_entrega"] = float(_safe_float(dates.get("dias_acto_a_oc", 0.0), 0.0) + max(0.0, entrega))
    return dates


def _extract_labeled_value(lines: list[str], labels: list[str]) -> str:
    if not lines:
        return ""
    label_norms = [_normalize_column_key(lbl) for lbl in labels]
    for idx, line in enumerate(lines):
        norm = _normalize_column_key(line)
        for lbl in label_norms:
            if not lbl:
                continue
            if norm.startswith(lbl):
                # Caso: "Etiqueta: valor"
                parts = re.split(r":|-", line, maxsplit=1)
                if len(parts) > 1:
                    val = _clean_text(parts[1])
                    if val:
                        return val
                # Caso: etiqueta en línea y valor en línea siguiente
                if idx + 1 < len(lines):
                    next_val = _clean_text(lines[idx + 1])
                    if next_val and _normalize_column_key(next_val) != lbl:
                        return next_val
    return ""


def _extract_dates_from_text(text: str, context_labels: list[str]) -> str:
    if not text:
        return ""
    # Busca una fecha cercana a una etiqueta contextual.
    for lbl in context_labels:
        pattern = re.compile(
            rf"{re.escape(lbl)}[^0-9]{{0,30}}(\d{{1,2}}[/-]\d{{1,2}}[/-]\d{{2,4}})",
            flags=re.IGNORECASE,
        )
        m = pattern.search(text)
        if m:
            return _clean_text(m.group(1))
    return ""


def _extract_all_date_tokens(text: str) -> list[str]:
    raw = str(text or "")
    if not raw:
        return []
    return re.findall(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", raw)


def _latest_date_token(text: str) -> str:
    tokens = _extract_all_date_tokens(text)
    if not tokens:
        return ""
    parsed: list[tuple[pd.Timestamp, str]] = []
    for token in tokens:
        d = _parse_any_date(token)
        if not pd.isna(d):
            parsed.append((d, token))
    if not parsed:
        return _clean_text(tokens[-1])
    parsed.sort(key=lambda x: x[0])
    return _clean_text(parsed[-1][1])


def _extract_date_by_labels(lines: list[str], labels: list[str]) -> str:
    if not lines:
        return ""
    label_norms = [_normalize_column_key(x) for x in labels if _normalize_column_key(x)]
    for idx, line in enumerate(lines):
        norm = _normalize_column_key(line)
        if not norm:
            continue
        if any(lbl in norm for lbl in label_norms):
            # 1) fecha en la misma línea
            token = _latest_date_token(line)
            if token:
                return token
            # 2) fecha en la siguiente línea
            if idx + 1 < len(lines):
                token = _latest_date_token(lines[idx + 1])
                if token:
                    return token
    return ""


def _extract_delivery_days(value: object) -> float:
    text = _clean_text(value).lower()
    if not text:
        return 0.0
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(dias|d[ií]as)", text)
    if not m:
        return 0.0
    return float(_parse_number(m.group(1)))


def _to_absolute_panamacompra_url(raw_href: object) -> str:
    href = _clean_text(raw_href)
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("#/"):
        return f"{PANAMACOMPRA_BASE_URL}{href}"
    if href.startswith("/Inicio/#/"):
        return f"https://www.panamacompra.gob.pa{href}"
    if href.startswith("/#/"):
        return f"{PANAMACOMPRA_BASE_URL}{href[1:]}"
    return href


def _normalize_html_to_lines(html_text: str) -> list[str]:
    if not html_text:
        return []
    work = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    work = re.sub(r"(?is)<style.*?>.*?</style>", " ", work)
    work = re.sub(r"(?i)<br\\s*/?>", "\n", work)
    work = re.sub(r"(?i)</(p|div|tr|li|h\\d)>", "\n", work)
    work = re.sub(r"(?is)<[^>]+>", " ", work)
    work = unescape(work)
    lines = [re.sub(r"\s+", " ", x).strip() for x in work.splitlines()]
    return [x for x in lines if x]


def _extract_table_candidates_from_html(html_text: str) -> dict[str, str]:
    """
    Parser formal (deterministico) de tablas HTML.
    Intenta extraer campos clave sin IA.
    """
    if not html_text:
        return {}

    extracted: dict[str, str] = {}
    try:
        tables = pd.read_html(io.StringIO(html_text))
    except Exception:
        tables = []

    def _set_if_empty(key: str, value: object) -> None:
        if key in extracted and _clean_text(extracted.get(key)):
            return
        text = _clean_text(value)
        if text:
            extracted[key] = text

    for table in tables:
        try:
            df = table.copy()
        except Exception:
            continue
        if df.empty:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        norm_cols = {_normalize_column_key(c): c for c in df.columns}

        # Caso 1: tabla tipo etiqueta-valor.
        if len(df.columns) >= 2:
            first_col = df.columns[0]
            second_col = df.columns[1]
            for _, row in df[[first_col, second_col]].fillna("").iterrows():
                label = _normalize_column_key(row[first_col])
                value = _clean_text(row[second_col])
                if not label or not value:
                    continue
                if "marca" in label:
                    _set_if_empty("marca", value)
                elif "modelo" in label or "catalogo" in label:
                    _set_if_empty("modelo", value)
                elif "pais de origen" in label or "origen" == label or "pais origen" in label:
                    _set_if_empty("pais_origen", value)
                elif "precio unitario de referencia" in label or ("precio" in label and "referencia" in label):
                    _set_if_empty("precio_unitario_referencia", value)
                elif "precio unitario" in label or ("precio ofertado" in label and "unit" in label):
                    _set_if_empty("precio_unitario_participacion", value)
                elif "cantidad" in label:
                    _set_if_empty("cantidad", value)
                elif "fecha de celebracion" in label or "fecha celebracion" in label:
                    _set_if_empty("fecha_celebracion", value)
                elif "fecha de adjudicacion" in label or "fecha adjudicacion" in label:
                    _set_if_empty("fecha_adjudicacion", value)
                elif "fecha de publicacion" in label or "fecha publicacion" in label:
                    _set_if_empty("fecha_publicacion", value)
                elif "orden de compra" in label or "fecha oc" in label:
                    _set_if_empty("fecha_orden_compra", value)

        # Caso 2: tabla con columnas explícitas.
        col_price = None
        col_ref = None
        col_qty = None
        col_brand = None
        col_model = None
        col_country = None
        for norm_col, raw_col in norm_cols.items():
            if col_price is None and ("precio unitario" in norm_col or "precio ofertado" in norm_col):
                col_price = raw_col
            if col_ref is None and ("precio" in norm_col and "referencia" in norm_col):
                col_ref = raw_col
            if col_qty is None and "cantidad" in norm_col:
                col_qty = raw_col
            if col_brand is None and "marca" in norm_col:
                col_brand = raw_col
            if col_model is None and ("modelo" in norm_col or "catalogo" in norm_col):
                col_model = raw_col
            if col_country is None and ("pais de origen" in norm_col or "origen" in norm_col):
                col_country = raw_col

        if col_price and not _clean_text(extracted.get("precio_unitario_participacion", "")):
            price_vals = pd.to_numeric(df[col_price], errors="coerce").dropna()
            if not price_vals.empty:
                _set_if_empty("precio_unitario_participacion", float(price_vals.iloc[0]))
        if col_ref and not _clean_text(extracted.get("precio_unitario_referencia", "")):
            ref_vals = pd.to_numeric(df[col_ref], errors="coerce").dropna()
            if not ref_vals.empty:
                _set_if_empty("precio_unitario_referencia", float(ref_vals.iloc[0]))
        if col_qty and not _clean_text(extracted.get("cantidad", "")):
            qty_vals = pd.to_numeric(df[col_qty], errors="coerce").dropna()
            if not qty_vals.empty:
                _set_if_empty("cantidad", float(qty_vals.iloc[0]))
        if col_brand and not _clean_text(extracted.get("marca", "")):
            brand_vals = df[col_brand].astype(str).map(_clean_text)
            brand_vals = brand_vals[brand_vals != ""]
            if not brand_vals.empty:
                _set_if_empty("marca", brand_vals.iloc[0])
        if col_model and not _clean_text(extracted.get("modelo", "")):
            model_vals = df[col_model].astype(str).map(_clean_text)
            model_vals = model_vals[model_vals != ""]
            if not model_vals.empty:
                _set_if_empty("modelo", model_vals.iloc[0])
        if col_country and not _clean_text(extracted.get("pais_origen", "")):
            country_vals = df[col_country].astype(str).map(_clean_text)
            country_vals = country_vals[country_vals != ""]
            if not country_vals.empty:
                _set_if_empty("pais_origen", country_vals.iloc[0])

    return extracted


def _renglon_match_confidence(ficha: str, ficha_name: str, renglon_texto: str, acto_nombre: str) -> float:
    """
    Puntaje 0..1 para decidir si la asociación acto-renglon-ficha es confiable.
    """
    score = 0.0
    text = f"{_clean_text(renglon_texto)} {_clean_text(acto_nombre)}".strip().lower()
    if not text:
        return 0.0

    ficha_token = str(ficha or "").strip().lower()
    if ficha_token and ficha_token in text:
        score += 0.55

    name = _clean_text(ficha_name).lower()
    if name:
        tokens = [t for t in re.findall(r"[a-z0-9]{4,}", name) if t not in {"ficha", "tecnica", "criterio"}]
        if tokens:
            hit = sum(1 for t in tokens if t in text)
            score += 0.45 * (hit / max(len(tokens), 1))

    return max(0.0, min(score, 1.0))


def _load_provider_historical_defaults(ficha: str) -> dict[str, dict[str, object]]:
    """
    Reutiliza conocimiento previo para reducir consultas manuales.
    """
    _ensure_study_db()
    try:
        with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
            df = pd.read_sql_query(
                """
                SELECT proveedor, marca, modelo, pais_origen, precio_unitario_participacion
                FROM estudio_detalle
                WHERE ficha=?
                  AND COALESCE(estado_revision, '') != 'excluido'
                """,
                conn,
                params=(str(ficha),),
            )
    except Exception:
        return {}

    if df.empty:
        return {}

    defaults: dict[str, dict[str, object]] = {}
    df["proveedor_norm"] = df["proveedor"].astype(str).map(_canonical_party_name)
    for prov_norm, part in df.groupby("proveedor_norm", dropna=False):
        key = str(prov_norm or "").strip()
        if not key:
            continue
        brand = (
            part["marca"]
            .astype(str)
            .map(_clean_text)
            .replace("", pd.NA)
            .dropna()
            .mode()
        )
        model = (
            part["modelo"]
            .astype(str)
            .map(_clean_text)
            .replace("", pd.NA)
            .dropna()
            .mode()
        )
        country = (
            part["pais_origen"]
            .astype(str)
            .map(_clean_text)
            .replace("", pd.NA)
            .dropna()
            .mode()
        )
        price_series = pd.to_numeric(part["precio_unitario_participacion"], errors="coerce").dropna()
        defaults[key] = {
            "marca": str(brand.iloc[0]) if not brand.empty else "",
            "modelo": str(model.iloc[0]) if not model.empty else "",
            "pais_origen": str(country.iloc[0]) if not country.empty else "",
            "precio_prom": float(price_series.mean()) if not price_series.empty else 0.0,
        }
    return defaults


def _append_query_limited(
    query_rows: list[dict[str, object]],
    query_keys: set[str],
    query_payload: dict[str, object],
    max_queries: int,
) -> bool:
    key = str(query_payload.get("_key", "") or "").strip()
    if not key:
        key = f"{query_payload.get('acto_id','')}|{query_payload.get('detail_id','')}|{query_payload.get('campo_dudoso','')}"
    if key in query_keys:
        return False
    if len(query_rows) >= max_queries:
        return False
    query_keys.add(key)
    payload = dict(query_payload)
    payload.pop("_key", None)
    query_rows.append(payload)
    return True


def _build_selenium_driver(headless: bool = True) -> tuple[object | None, str]:
    try:
        from selenium import webdriver  # type: ignore
        from selenium.webdriver.chrome.service import Service as ChromeService  # type: ignore
        from webdriver_manager.chrome import ChromeDriverManager  # type: ignore
    except Exception as exc:
        return None, f"selenium_unavailable:{exc}"

    try:
        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=es-PA")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(50)
        return driver, "ok"
    except Exception as exc:
        return None, f"driver_init_error:{exc}"


def _driver_open_and_get_html(driver: object, url: str, timeout: int = 35) -> str:
    if not driver or not url:
        return ""
    try:
        from selenium.webdriver.common.by import By  # type: ignore
        from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
        from selenium.webdriver.support import expected_conditions as EC  # type: ignore

        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "app-root")))
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        try:
            WebDriverWait(driver, min(timeout, 25)).until(
                lambda d: any(
                    token in _normalize_column_key(getattr(d.find_element(By.TAG_NAME, "body"), "text", ""))
                    for token in (
                        "informacion del proponente",
                        "aviso de convocatoria",
                        "procesos relacionados",
                        "archivos de la compra menor",
                        "cuadro de propuestas",
                    )
                )
            )
        except Exception:
            # Si no aparecen tokens, igual retorna el page_source actual.
            pass
        return str(getattr(driver, "page_source", "") or "")
    except Exception:
        return ""


def _extract_href_from_html(html_text: str, contains: str, exclude: str = "") -> str:
    if not html_text:
        return ""
    pattern = re.compile(r'href=["\']([^"\']+)["\']', flags=re.IGNORECASE)
    for href in pattern.findall(html_text):
        href_clean = _clean_text(href)
        if not href_clean:
            continue
        if contains.lower() not in href_clean.lower():
            continue
        if exclude and exclude.lower() in href_clean.lower():
            continue
        return _to_absolute_panamacompra_url(href_clean)
    return ""


def _extract_provider_from_info_tables(html_text: str) -> str:
    if not html_text:
        return ""
    try:
        tables = pd.read_html(io.StringIO(html_text))
    except Exception:
        tables = []
    for table in tables:
        if table.empty or len(table.columns) < 2:
            continue
        first_col = table.columns[0]
        second_col = table.columns[1]
        for _, row in table[[first_col, second_col]].fillna("").iterrows():
            label = _normalize_column_key(row[first_col])
            value = _clean_text(row[second_col])
            if not value:
                continue
            if "nombre comercial" in label:
                return value
            if "razon social" in label:
                return value
    return ""


def _extract_unit_data_from_html_tables(html_text: str, ficha: str) -> dict[str, object]:
    out = {
        "precio_unitario_participacion": 0.0,
        "precio_unitario_referencia": 0.0,
        "cantidad": 0.0,
        "unidad_medida": "",
        "precio_total_referencia": 0.0,
    }
    if not html_text:
        return out

    ficha_token = str(ficha or "").strip()
    try:
        tables = pd.read_html(io.StringIO(html_text))
    except Exception:
        tables = []

    for table in tables:
        if table.empty:
            continue
        df = table.copy()
        df.columns = [str(c).strip() for c in df.columns]
        norm_cols = {_normalize_column_key(c): c for c in df.columns}
        col_price = ""
        col_ref = ""
        col_qty = ""
        col_um = ""
        col_desc = ""
        for norm_col, raw_col in norm_cols.items():
            if not col_price and "precio unitario" in norm_col:
                col_price = raw_col
            if not col_ref and ("precio referencia" in norm_col or ("precio" in norm_col and "referencia" in norm_col)):
                col_ref = raw_col
            if not col_qty and "cantidad" in norm_col:
                col_qty = raw_col
            if not col_um and "unidad de medida" in norm_col:
                col_um = raw_col
            if not col_desc and (
                "descripcion" in norm_col
                or "especificaciones del comprador" in norm_col
                or "especificaciones del proponente" in norm_col
            ):
                col_desc = raw_col

        if not (col_price or col_ref):
            continue

        selected_idx = 0
        if ficha_token and col_desc:
            match_mask = df[col_desc].astype(str).str.contains(str(ficha_token), case=False, regex=False, na=False)
            if match_mask.any():
                selected_idx = int(df[match_mask].index[0])
        row = df.loc[selected_idx] if selected_idx in df.index else df.iloc[0]

        if col_price and float(out["precio_unitario_participacion"]) <= 0:
            unit = _parse_number(row.get(col_price, 0))
            if unit > 0:
                out["precio_unitario_participacion"] = float(unit)
        if col_qty and float(out["cantidad"]) <= 0:
            qty = _parse_number(row.get(col_qty, 0))
            if qty > 0:
                out["cantidad"] = float(qty)
        if col_um and not _clean_text(out["unidad_medida"]):
            out["unidad_medida"] = _clean_text(row.get(col_um, ""))
        if col_ref and float(out["precio_unitario_referencia"]) <= 0:
            ref_raw = _parse_number(row.get(col_ref, 0))
            if ref_raw > 0:
                out["precio_total_referencia"] = float(ref_raw)
                qty_ref = float(out["cantidad"]) if float(out["cantidad"]) > 0 else _parse_number(row.get(col_qty, 0))
                if qty_ref > 0:
                    out["precio_unitario_referencia"] = float(ref_raw / qty_ref)
                else:
                    out["precio_unitario_referencia"] = float(ref_raw)
    return out


def _extract_oc_date_from_docs_tables(html_text: str) -> str:
    if not html_text:
        return ""
    best: pd.Timestamp | None = None
    best_raw = ""
    try:
        tables = pd.read_html(io.StringIO(html_text))
    except Exception:
        tables = []
    for table in tables:
        if table.empty:
            continue
        df = table.copy()
        df.columns = [str(c).strip() for c in df.columns]
        norm_cols = {_normalize_column_key(c): c for c in df.columns}
        col_tipo = ""
        col_desc = ""
        col_fecha = ""
        for norm_col, raw_col in norm_cols.items():
            if not col_tipo and norm_col == "tipo":
                col_tipo = raw_col
            if not col_desc and norm_col == "descripcion":
                col_desc = raw_col
            if not col_fecha and norm_col == "fecha":
                col_fecha = raw_col
        if not col_fecha:
            continue
        for _, row in df.fillna("").iterrows():
            tipo = _normalize_column_key(row.get(col_tipo, "")) if col_tipo else ""
            desc = _normalize_column_key(row.get(col_desc, "")) if col_desc else ""
            if "orden de compra" not in f"{tipo} {desc}":
                continue
            date_raw = _latest_date_token(str(row.get(col_fecha, "")))
            d = _parse_any_date(date_raw)
            if pd.isna(d):
                continue
            if best is None or d > best:
                best = d
                best_raw = date_raw
    return _clean_text(best_raw)


def _extract_act_date_from_page(lines: list[str]) -> str:
    labels_priority = [
        "fecha y hora de apertura de propuestas",
        "fecha y hora presentacion de propuestas",
        "fecha y hora presentacion de cotizaciones",
        "fecha y hora presentación de propuestas",
        "fecha y hora presentación de cotizaciones",
        "fecha de celebracion",
        "fecha de celebración",
    ]
    return _extract_date_by_labels(lines, labels_priority)


def _extract_cuadro_lowest_offer_from_driver(driver: object, cuadro_url: str, ficha: str) -> dict[str, object]:
    out = {
        "proveedor": "",
        "precio_unitario_participacion": 0.0,
        "cantidad": 0.0,
        "unidad_medida": "",
        "evidencia": "",
    }
    if not driver or not cuadro_url:
        return out
    try:
        from selenium.webdriver.common.by import By  # type: ignore
        from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
        from selenium.webdriver.support import expected_conditions as EC  # type: ignore
    except Exception:
        return out

    try:
        driver.get(cuadro_url)
        WebDriverWait(driver, 35).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        tables = driver.find_elements(By.CSS_SELECTOR, "table.caption-top")
        if not tables:
            return out

        candidates: list[dict[str, object]] = []
        ficha_token = str(ficha or "").strip()

        for tb in tables:
            provider = ""
            try:
                provider = _clean_text(tb.find_element(By.XPATH, "./caption//a[1]").text)
            except Exception:
                provider = ""

            headers = [_normalize_column_key(x.text) for x in tb.find_elements(By.CSS_SELECTOR, "thead th")]
            if not headers:
                continue
            idx_price = next((i for i, h in enumerate(headers) if "precio unitario" in h), -1)
            idx_qty = next((i for i, h in enumerate(headers) if "cantidad propuesta" in h or h == "cantidad"), -1)
            idx_um = next((i for i, h in enumerate(headers) if "unidad de medida" in h), -1)
            idx_desc = next((i for i, h in enumerate(headers) if "descripcion del bien" in h or "especificaciones del comprador" in h), -1)

            if idx_price < 0:
                continue

            rows = tb.find_elements(By.CSS_SELECTOR, "tbody tr")
            chosen_cells = None
            for row in rows:
                cells = row.find_elements(By.CSS_SELECTOR, "th,td")
                if not cells:
                    continue
                if ficha_token and idx_desc >= 0 and idx_desc < len(cells):
                    if ficha_token in _clean_text(cells[idx_desc].text):
                        chosen_cells = cells
                        break
                if chosen_cells is None:
                    chosen_cells = cells
            if not chosen_cells:
                continue

            unit_price = _parse_number(chosen_cells[idx_price].text if idx_price < len(chosen_cells) else 0)
            qty = _parse_number(chosen_cells[idx_qty].text if idx_qty >= 0 and idx_qty < len(chosen_cells) else 0)
            um = _clean_text(chosen_cells[idx_um].text) if idx_um >= 0 and idx_um < len(chosen_cells) else ""

            total = 0.0
            try:
                total_rows = tb.find_elements(By.CSS_SELECTOR, "tfoot tr")
                for tr in total_rows:
                    row_text = _normalize_column_key(tr.text)
                    if "total" in row_text:
                        cells = tr.find_elements(By.CSS_SELECTOR, "th,td")
                        if cells:
                            total = max(total, _parse_number(cells[-1].text))
            except Exception:
                total = 0.0

            if unit_price > 0:
                candidates.append(
                    {
                        "proveedor": provider,
                        "precio_unitario_participacion": float(unit_price),
                        "cantidad": float(qty) if qty > 0 else 0.0,
                        "unidad_medida": um,
                        "total": float(total),
                    }
                )

        if not candidates:
            return out

        candidates.sort(
            key=lambda x: (
                0 if float(x.get("total", 0.0) or 0.0) > 0 else 1,
                float(x.get("total", 0.0) or 0.0) if float(x.get("total", 0.0) or 0.0) > 0 else float(x.get("precio_unitario_participacion", 0.0) or 0.0),
            )
        )
        chosen = candidates[0]
        out.update(chosen)
        out["evidencia"] = f"cuadro_min_total|{_clean_text(chosen.get('proveedor', ''))}|{float(chosen.get('total', 0.0) or 0.0):.2f}"
        return out
    except Exception:
        return out


@st.cache_data(show_spinner=False, ttl=3600)
def _fetch_acto_html(url: str) -> str:
    try:
        response = requests.get(
            url,
            timeout=25,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
            },
        )
        response.raise_for_status()
        return str(response.text or "")
    except Exception:
        return ""


def _get_openai_api_key() -> str:
    key = _clean_text(os.getenv("OPENAI_API_KEY"))
    if key:
        return key
    try:
        key = _clean_text(st.secrets.get("OPENAI_API_KEY", ""))
    except Exception:
        key = ""
    return key


def _parse_json_block(text: str) -> dict[str, object]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        pass
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def _ai_extract_fields_from_text(
    text_excerpt: str,
    ficha: str,
    ficha_name: str,
    acto_nombre: str,
) -> dict[str, object]:
    api_key = _get_openai_api_key()
    if not api_key or not text_excerpt:
        return {}
    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return {}
    try:
        client = OpenAI(api_key=api_key)
        prompt = (
            "Extrae solo datos si aparecen explícitos. No inventes. "
            "Responde JSON con claves: marca, modelo, pais_origen, fecha_celebracion, "
            "fecha_orden_compra, fecha_publicacion, fecha_adjudicacion, "
            "precio_unitario_participacion, precio_unitario_referencia, cantidad, certeza, evidencia. "
            f"Ficha: {ficha} | Nombre ficha: {ficha_name} | Acto: {acto_nombre}. "
            "Si falta algo deja cadena vacía."
        )
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": "Eres extractor estricto de datos públicos. Nunca inventes."},
                {"role": "user", "content": prompt + "\n\nContenido:\n" + text_excerpt[:12000]},
            ],
            max_output_tokens=800,
        )
        output_text = _clean_text(getattr(response, "output_text", ""))
        return _parse_json_block(output_text)
    except Exception:
        return {}


def _extract_live_fields_from_acto_url(
    acto_url: str,
    ficha: str,
    ficha_name: str,
    acto_nombre: str,
    driver: object | None = None,
    use_ai_fallback: bool = True,
) -> dict[str, object]:
    if not acto_url:
        return {}
    url = _to_absolute_panamacompra_url(acto_url)
    html_text = _driver_open_and_get_html(driver, url) if driver is not None else _fetch_acto_html(url)
    if not html_text:
        return {}

    lines = _normalize_html_to_lines(html_text)
    text_flat = " | ".join(lines)
    table_fields = _extract_table_candidates_from_html(html_text)
    has_info = "informacion del proponente" in _normalize_column_key(text_flat)

    # Base común para ambos tipos
    extracted = {
        "tipo_flujo": "tipo_1_info_proponente" if has_info else "tipo_2_cuadro_propuestas",
        "proveedor": "",
        "precio_unitario_participacion": 0.0,
        "precio_unitario_referencia": 0.0,
        "cantidad": 0.0,
        "unidad_medida": "",
        "fecha_celebracion": "",
        "fecha_orden_compra": "",
        "fecha_publicacion": _extract_date_by_labels(lines, ["fecha de publicacion", "fecha de publicación"]),
        "fecha_adjudicacion": _extract_date_by_labels(lines, ["fecha de adjudicacion", "fecha de adjudicación"]),
        "tiempo_entrega_dias": 0.0,
        "fuente_precio": "",
        "fuente_fecha": "",
        "enlace_evidencia": url,
        "evidencia": "",
    }

    extracted["tiempo_entrega_dias"] = _extract_delivery_days(text_flat)

    # Información de tablas del propio acto (cuando exista)
    unit_payload = _extract_unit_data_from_html_tables(html_text, ficha)
    if float(unit_payload.get("precio_unitario_participacion", 0.0) or 0.0) > 0:
        extracted["precio_unitario_participacion"] = float(unit_payload["precio_unitario_participacion"])
    if float(unit_payload.get("precio_unitario_referencia", 0.0) or 0.0) > 0:
        extracted["precio_unitario_referencia"] = float(unit_payload["precio_unitario_referencia"])
    if float(unit_payload.get("cantidad", 0.0) or 0.0) > 0:
        extracted["cantidad"] = float(unit_payload["cantidad"])
    extracted["unidad_medida"] = _clean_text(unit_payload.get("unidad_medida", ""))

    # Fecha OC desde anexos/documentos
    extracted["fecha_orden_compra"] = _extract_oc_date_from_docs_tables(html_text) or _extract_date_by_labels(
        lines,
        ["fecha de orden de compra", "orden de compra", "fecha oc"],
    )

    if has_info:
        # Tipo 1: proveedor + precios desde mismo acto.
        extracted["proveedor"] = _extract_provider_from_info_tables(html_text)
        if float(extracted.get("precio_unitario_participacion", 0.0) or 0.0) > 0:
            extracted["fuente_precio"] = "acto_info_proponente"
        if float(extracted.get("precio_unitario_referencia", 0.0) or 0.0) > 0:
            extracted["fuente_precio"] = extracted["fuente_precio"] or "acto_info_proponente"

        # Fecha del acto: proceso original.
        original_link = _extract_href_from_html(html_text, "/proceso-original/")
        if original_link and driver is not None:
            original_html = _driver_open_and_get_html(driver, original_link)
        elif original_link:
            original_html = _fetch_acto_html(original_link)
        else:
            original_html = ""
        original_lines = _normalize_html_to_lines(original_html)
        extracted["fecha_celebracion"] = _extract_act_date_from_page(original_lines) if original_lines else ""
        if extracted["fecha_celebracion"]:
            extracted["fuente_fecha"] = "proceso_original"
            extracted["enlace_evidencia"] = original_link or extracted["enlace_evidencia"]
        else:
            fallback_date = _extract_act_date_from_page(lines)
            if fallback_date:
                extracted["fecha_celebracion"] = fallback_date
                extracted["fuente_fecha"] = extracted["fuente_fecha"] or "acto_fallback"
    else:
        # Tipo 2: fecha en acto + precio por cuadro de propuestas.
        extracted["fecha_celebracion"] = _extract_act_date_from_page(lines)
        if extracted["fecha_celebracion"]:
            extracted["fuente_fecha"] = "acto_apertura"

        cuadro_link = _extract_href_from_html(html_text, "/cuadro-de-propuestas/", exclude="/ver-propuesta/")
        if cuadro_link and driver is not None:
            cuadro_data = _extract_cuadro_lowest_offer_from_driver(driver, cuadro_link, ficha)
            extracted["proveedor"] = _clean_text(cuadro_data.get("proveedor", ""))
            if float(cuadro_data.get("precio_unitario_participacion", 0.0) or 0.0) > 0:
                extracted["precio_unitario_participacion"] = float(cuadro_data["precio_unitario_participacion"])
                extracted["fuente_precio"] = "cuadro_propuestas_min_total"
                extracted["enlace_evidencia"] = cuadro_link
            if float(cuadro_data.get("cantidad", 0.0) or 0.0) > 0:
                extracted["cantidad"] = float(cuadro_data["cantidad"])
            if _clean_text(cuadro_data.get("unidad_medida", "")):
                extracted["unidad_medida"] = _clean_text(cuadro_data["unidad_medida"])
            extracted["evidencia"] = _clean_text(cuadro_data.get("evidencia", ""))
        elif cuadro_link:
            cuadro_html = _fetch_acto_html(cuadro_link)
            if cuadro_html:
                unit_payload_cuadro = _extract_unit_data_from_html_tables(cuadro_html, ficha)
                if float(unit_payload_cuadro.get("precio_unitario_participacion", 0.0) or 0.0) > 0:
                    extracted["precio_unitario_participacion"] = float(unit_payload_cuadro["precio_unitario_participacion"])
                    extracted["fuente_precio"] = "cuadro_propuestas_parser"
                    extracted["enlace_evidencia"] = cuadro_link
                if float(unit_payload_cuadro.get("cantidad", 0.0) or 0.0) > 0:
                    extracted["cantidad"] = float(unit_payload_cuadro["cantidad"])
                if _clean_text(unit_payload_cuadro.get("unidad_medida", "")):
                    extracted["unidad_medida"] = _clean_text(unit_payload_cuadro["unidad_medida"])

    # Fallback estricto opcional (solo fechas/precios/cantidad, no marca-modelo-pais).
    if use_ai_fallback:
        core_missing = (
            float(extracted.get("precio_unitario_participacion", 0.0) or 0.0) <= 0
            and not _clean_text(extracted.get("fecha_celebracion", ""))
        )
        if core_missing:
            ai_payload = _ai_extract_fields_from_text(text_flat[:12000], ficha, ficha_name, acto_nombre)
            if float(extracted.get("precio_unitario_participacion", 0.0) or 0.0) <= 0:
                extracted["precio_unitario_participacion"] = _parse_number(ai_payload.get("precio_unitario_participacion", 0))
                if float(extracted.get("precio_unitario_participacion", 0.0) or 0.0) > 0:
                    extracted["fuente_precio"] = extracted["fuente_precio"] or "ai_fallback"
            if float(extracted.get("precio_unitario_referencia", 0.0) or 0.0) <= 0:
                extracted["precio_unitario_referencia"] = _parse_number(ai_payload.get("precio_unitario_referencia", 0))
            if float(extracted.get("cantidad", 0.0) or 0.0) <= 0:
                extracted["cantidad"] = _parse_number(ai_payload.get("cantidad", 0))
            if not _clean_text(extracted.get("fecha_celebracion", "")):
                extracted["fecha_celebracion"] = _clean_text(ai_payload.get("fecha_celebracion", ""))
                if extracted["fecha_celebracion"]:
                    extracted["fuente_fecha"] = extracted["fuente_fecha"] or "ai_fallback"

    # fallback final con parser formal de tablas
    if float(extracted.get("precio_unitario_participacion", 0.0) or 0.0) <= 0:
        extracted["precio_unitario_participacion"] = _parse_number(table_fields.get("precio_unitario_participacion", 0))
    if float(extracted.get("precio_unitario_referencia", 0.0) or 0.0) <= 0:
        extracted["precio_unitario_referencia"] = _parse_number(table_fields.get("precio_unitario_referencia", 0))
    if float(extracted.get("cantidad", 0.0) or 0.0) <= 0:
        extracted["cantidad"] = _parse_number(table_fields.get("cantidad", 0))
    if not _clean_text(extracted.get("fecha_celebracion", "")):
        extracted["fecha_celebracion"] = _clean_text(table_fields.get("fecha_celebracion", ""))
    if not _clean_text(extracted.get("fecha_orden_compra", "")):
        extracted["fecha_orden_compra"] = _clean_text(table_fields.get("fecha_orden_compra", ""))
    return extracted


def _build_study_payload(
    ficha: str,
    ficha_name: str,
    acts_df: pd.DataFrame,
    max_queries: int = INTEL_STUDY_DEFAULT_MAX_QUERIES,
    use_browser_extractor: bool = True,
    use_ai_fallback: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    if acts_df.empty:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            {
                "acts_processed": 0,
                "detail_rows": 0,
                "queries_generated": 0,
                "queries_skipped_limit": 0,
                "auto_resolved_price": 0,
                "auto_resolved_country": 0,
                "auto_resolved_brand_model": 0,
                "tipo1_detectados": 0,
                "tipo2_detectados": 0,
                "runs_con_browser": 0,
            },
        )

    max_queries = max(1, min(int(_safe_int(max_queries or INTEL_STUDY_DEFAULT_MAX_QUERIES)), INTEL_STUDY_MAX_QUERIES_HARD))
    detail_rows: list[dict[str, object]] = []
    consultas_rows: list[dict[str, object]] = []
    query_keys: set[str] = set()
    provider_defaults = _load_provider_historical_defaults(ficha)
    _ = _build_catalog_provider_map()  # warm cache
    stats = {
        "acts_processed": 0,
        "detail_rows": 0,
        "queries_generated": 0,
        "queries_skipped_limit": 0,
        "auto_resolved_price": 0,
        "auto_resolved_country": 0,
        "auto_resolved_brand_model": 0,
        "tipo1_detectados": 0,
        "tipo2_detectados": 0,
        "runs_con_browser": 0,
    }

    driver = None
    if use_browser_extractor:
        driver, mode = _build_selenium_driver(headless=True)
        if driver is not None:
            stats["runs_con_browser"] = 1
        st.session_state["intel_study_browser_status"] = mode

    try:
        for _, row in acts_df.iterrows():
            stats["acts_processed"] += 1
            acto_id = str(row.get("id", "") or "").strip()
            acto_nombre = _clean_text(row.get("titulo")) or f"Acto {acto_id}"
            acto_url = _clean_text(row.get("enlace"))
            entidad = _clean_text(row.get("entidad"))
            renglon_texto = " | ".join(
                [t for t in [_clean_text(row.get("item_1")), _clean_text(row.get("item_2")), _clean_text(row.get("descripcion"))] if t]
            )
            dates = _extract_estudio_dates(row)
            proveedor_ganador = _clean_text(row.get("ganador")) or _clean_text(row.get("razon_social"))
            proveedor_ganador_norm = _canonical_party_name(proveedor_ganador)
            precio_ref = _parse_number(row.get("precio_referencia", 0))
            cantidad = _parse_number(row.get("cantidad", 0))
            unidad_medida_base = _pick_alias_value(row, ["unidad_medida", "unidad de medida"])

            live_fields = (
                _extract_live_fields_from_acto_url(
                    acto_url,
                    ficha,
                    ficha_name,
                    acto_nombre,
                    driver=driver,
                    use_ai_fallback=bool(use_ai_fallback),
                )
                if acto_url
                else {}
            )
            match_conf = _renglon_match_confidence(ficha, ficha_name, renglon_texto, acto_nombre)

            tipo_flujo = _clean_text(live_fields.get("tipo_flujo", ""))
            if tipo_flujo.startswith("tipo_1"):
                stats["tipo1_detectados"] += 1
            elif tipo_flujo.startswith("tipo_2"):
                stats["tipo2_detectados"] += 1

            if _clean_text(dates.get("fecha_celebracion", "")) == "":
                dates["fecha_celebracion"] = _clean_text(live_fields.get("fecha_celebracion", ""))
            if _clean_text(dates.get("fecha_orden_compra", "")) == "":
                dates["fecha_orden_compra"] = _clean_text(live_fields.get("fecha_orden_compra", ""))
            if _clean_text(dates.get("fecha_publicacion", "")) == "":
                dates["fecha_publicacion"] = _clean_text(live_fields.get("fecha_publicacion", ""))
            if _clean_text(dates.get("fecha_adjudicacion", "")) == "":
                dates["fecha_adjudicacion"] = _clean_text(live_fields.get("fecha_adjudicacion", ""))

            tiempo_entrega_live = _safe_float(live_fields.get("tiempo_entrega_dias", 0.0), 0.0)
            if tiempo_entrega_live > 0 and _safe_float(dates.get("tiempo_entrega_dias", 0.0), 0.0) <= 0:
                dates["tiempo_entrega_dias"] = float(tiempo_entrega_live)

            dates = _recompute_days_act_to_oc(dates)
            if precio_ref <= 0:
                precio_ref = _parse_number(live_fields.get("precio_unitario_referencia", 0))
            if cantidad <= 0:
                cantidad = _parse_number(live_fields.get("cantidad", 0))
            if not unidad_medida_base:
                unidad_medida_base = _clean_text(live_fields.get("unidad_medida", ""))

            proponent_candidates: list[tuple[str, float]] = []
            available_prices: list[float] = []
            for idx in range(1, 15):
                p_col = f"Proponente {idx}"
                price_col = f"Precio Proponente {idx}"
                if p_col in row.index:
                    p_name = _clean_text(row.get(p_col))
                    if not p_name:
                        continue
                    p_price = _parse_number(row.get(price_col, 0))
                    if p_price > 0:
                        available_prices.append(p_price)
                    proponent_candidates.append((p_name, p_price))

            if not proponent_candidates and _clean_text(live_fields.get("proveedor", "")):
                proponent_candidates.append(
                    (
                        _clean_text(live_fields.get("proveedor", "")),
                        _parse_number(live_fields.get("precio_unitario_participacion", 0)),
                    )
                )
            if not proponent_candidates and proveedor_ganador:
                proponent_candidates.append((proveedor_ganador, _parse_number(row.get("monto_estimado", 0))))
            if len(proponent_candidates) == 1 and _parse_number(proponent_candidates[0][1]) <= 0:
                live_part_price = _parse_number(live_fields.get("precio_unitario_participacion", 0))
                if live_part_price > 0:
                    proponent_candidates[0] = (proponent_candidates[0][0], live_part_price)
                    stats["auto_resolved_price"] += 1

            if int(_safe_int(row.get("fichas_en_acto", 1))) > 1 and match_conf < 0.60:
                _append_query_limited(
                    consultas_rows,
                    query_keys,
                    {
                        "_key": f"{acto_id}|renglon_correspondencia_ficha|",
                        "consulta_id": str(uuid.uuid4()),
                        "ficha": ficha,
                        "detail_id": "",
                        "acto_id": acto_id,
                        "campo_dudoso": "renglon_correspondencia_ficha",
                        "evidencia": renglon_texto or acto_nombre,
                        "opciones_json": _json_dumps(
                            [
                                {"label": "Confirmar asociación por ficha detectada", "value": "confirmar_asociacion"},
                                {"label": "Excluir este acto para la ficha", "value": "excluir_acto"},
                                {"label": "Resolver manualmente", "value": "manual"},
                            ]
                        ),
                        "respuesta_seleccionada": "",
                        "valor_manual": "",
                        "estado": "pendiente",
                        "obligatoria": 1,
                    },
                    max_queries=max_queries,
                )

            for proponente, precio_uni in proponent_candidates:
                detail_id = str(uuid.uuid4())
                proponente_norm = _canonical_party_name(proponente)
                es_ganador = 1 if proponente_norm and proponente_norm == proveedor_ganador_norm else 0
                provider_default = provider_defaults.get(proponente_norm, {})
                requiere_revision = 0
                certeza = 0.9
                observ = []

                catalog_payload = _lookup_catalog_provider_payload(ficha, proponente)
                marca_item = _clean_text(catalog_payload.get("marca", ""))
                modelo_item = _clean_text(catalog_payload.get("modelo", ""))
                pais_item = _clean_text(catalog_payload.get("pais_origen", ""))
                if marca_item or modelo_item:
                    stats["auto_resolved_brand_model"] += 1
                if pais_item:
                    stats["auto_resolved_country"] += 1

                if precio_uni <= 0:
                    live_price = _parse_number(live_fields.get("precio_unitario_participacion", 0))
                    if live_price > 0:
                        precio_uni = live_price
                        stats["auto_resolved_price"] += 1
                    elif len(set([round(x, 6) for x in available_prices if x > 0])) == 1:
                        precio_uni = float([x for x in available_prices if x > 0][0])
                        stats["auto_resolved_price"] += 1
                    else:
                        hist_price = _safe_float(provider_default.get("precio_prom", 0.0))
                        if hist_price > 0:
                            precio_uni = hist_price
                            certeza = min(certeza, 0.78)
                            observ.append("Precio imputado por promedio historico del proveedor.")
                            stats["auto_resolved_price"] += 1

                if precio_uni <= 0 and es_ganador == 1:
                    requiere_revision = 1
                    certeza = min(certeza, 0.55)
                    observ.append("Precio unitario de participación no identificado.")
                    opciones_precio = [{"label": f"${p:,.2f}", "value": f"{p:.6f}"} for p in sorted(set(available_prices)) if p > 0]
                    opciones_precio.append({"label": "Dejar vacío", "value": "vacio"})
                    _append_query_limited(
                        consultas_rows,
                        query_keys,
                        {
                            "_key": f"{acto_id}|{detail_id}|precio_unitario_participacion",
                            "consulta_id": str(uuid.uuid4()),
                            "ficha": ficha,
                            "detail_id": detail_id,
                            "acto_id": acto_id,
                            "campo_dudoso": "precio_unitario_participacion",
                            "evidencia": f"Proveedor: {proponente}. Precios detectados: {', '.join([str(round(x,2)) for x in available_prices]) or 'ninguno'}",
                            "opciones_json": _json_dumps(opciones_precio),
                            "respuesta_seleccionada": "",
                            "valor_manual": "",
                            "estado": "pendiente",
                            "obligatoria": 1,
                        },
                        max_queries=max_queries,
                    )

                if not pais_item and es_ganador == 1:
                    certeza = min(certeza, 0.70)
                    observ.append("País de origen no encontrado en catálogo para proveedor+ficha.")

                if match_conf < 0.45 and es_ganador == 1:
                    requiere_revision = 1
                    certeza = min(certeza, 0.60)
                    observ.append("Asociacion ficha-renglon con baja confianza.")

                detail_rows.append(
                    {
                        "detail_id": detail_id,
                        "ficha": ficha,
                        "nombre_ficha": ficha_name,
                        "acto_id": acto_id,
                        "acto_nombre": acto_nombre,
                        "acto_url": acto_url,
                        "entidad": entidad,
                        "renglon_texto": renglon_texto,
                        "proveedor": proponente,
                        "proveedor_ganador": proveedor_ganador,
                        "es_ganador": es_ganador,
                        "marca": marca_item,
                        "modelo": modelo_item,
                        "pais_origen": pais_item,
                        "cantidad": float(cantidad) if float(cantidad) > 0 else _parse_number(live_fields.get("cantidad", 0)),
                        "precio_unitario_participacion": float(precio_uni),
                        "precio_unitario_referencia": float(precio_ref),
                        "fecha_publicacion": dates["fecha_publicacion"],
                        "fecha_celebracion": dates["fecha_celebracion"],
                        "fecha_adjudicacion": dates["fecha_adjudicacion"],
                        "fecha_orden_compra": dates["fecha_orden_compra"],
                        "dias_acto_a_oc": dates["dias_acto_a_oc"],
                        "dias_acto_a_oc_mas_entrega": dates.get("dias_acto_a_oc_mas_entrega", 0.0),
                        "tipo_flujo": tipo_flujo,
                        "fuente_precio": _clean_text(live_fields.get("fuente_precio", "")),
                        "fuente_fecha": _clean_text(live_fields.get("fuente_fecha", "")),
                        "enlace_evidencia": _clean_text(live_fields.get("enlace_evidencia", "")) or acto_url,
                        "unidad_medida": unidad_medida_base,
                        "tiempo_entrega_dias": float(_safe_float(dates.get("tiempo_entrega_dias", 0.0), 0.0)),
                        "observaciones": " | ".join([x for x in (observ + [_clean_text(live_fields.get("evidencia", ""))]) if _clean_text(x)]),
                        "estado_revision": "pendiente" if requiere_revision else "ok",
                        "nivel_certeza": certeza,
                        "requiere_revision": requiere_revision,
                    }
                )
                stats["detail_rows"] += 1
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    detail_df = pd.DataFrame(detail_rows)
    consultas_df = pd.DataFrame(consultas_rows)
    if not consultas_df.empty:
        consultas_df["_qkey"] = (
            consultas_df["acto_id"].astype(str)
            + "|"
            + consultas_df["campo_dudoso"].astype(str)
            + "|"
            + consultas_df["detail_id"].astype(str)
        )
        consultas_df = consultas_df.drop_duplicates(subset=["_qkey"], keep="first")
        consultas_df["obligatoria"] = pd.to_numeric(consultas_df.get("obligatoria", 1), errors="coerce").fillna(1).astype(int)
        consultas_df = consultas_df.sort_values(["obligatoria"], ascending=[False], kind="stable")
        dropped_df = pd.DataFrame()
        if len(consultas_df) > max_queries:
            dropped_df = consultas_df.iloc[max_queries:].copy()
            stats["queries_skipped_limit"] += int(len(dropped_df))
            consultas_df = consultas_df.head(max_queries).copy()
        consultas_df = consultas_df.drop(columns=["_qkey"])

        if not dropped_df.empty and not detail_df.empty:
            dropped_ids = {
                str(x).strip()
                for x in dropped_df["detail_id"].astype(str).tolist()
                if str(x).strip()
            }
            if dropped_ids:
                mask = detail_df["detail_id"].astype(str).isin(dropped_ids)
                if mask.any():
                    detail_df.loc[mask, "requiere_revision"] = 0
                    detail_df.loc[mask, "estado_revision"] = "ok"
                    detail_df.loc[mask, "observaciones"] = (
                        detail_df.loc[mask, "observaciones"].fillna("").astype(str)
                        + " | Consulta omitida por limite automatico"
                    ).str.strip(" |")
    stats["queries_generated"] = int(len(consultas_df))

    return detail_df, consultas_df, stats


def _compute_study_summary(ficha: str, run_id: str, detail_df: pd.DataFrame, estado: str, resumen_ia: str) -> dict[str, object]:
    if detail_df.empty:
        return {
            "ficha": ficha,
            "run_id_vigente": run_id,
            "estado_estudio": estado,
            "ultima_actualizacion": _utc_now_iso(),
            "total_actos": 0,
            "total_renglones": 0,
            "empresas_ganadoras": 0,
            "marcas_json": "[]",
            "modelos_json": "[]",
            "paises_json": "[]",
            "precio_participacion_prom": 0.0,
            "precio_participacion_min": 0.0,
            "fecha_precio_min": "",
            "precio_participacion_max": 0.0,
            "fecha_precio_max": "",
            "precio_referencia_prom": 0.0,
            "top_menor_precio_json": "[]",
            "resumen_ia": resumen_ia,
        }

    valid = detail_df.copy()
    if "estado_revision" in valid.columns:
        valid = valid[valid["estado_revision"].astype(str).str.lower() != "excluido"].copy()
    valid_price = valid[pd.to_numeric(valid["precio_unitario_participacion"], errors="coerce").fillna(0.0) > 0].copy()
    valid_ref = valid[pd.to_numeric(valid["precio_unitario_referencia"], errors="coerce").fillna(0.0) > 0].copy()

    precio_prom = float(pd.to_numeric(valid_price["precio_unitario_participacion"], errors="coerce").mean()) if not valid_price.empty else 0.0
    precio_min = float(pd.to_numeric(valid_price["precio_unitario_participacion"], errors="coerce").min()) if not valid_price.empty else 0.0
    precio_max = float(pd.to_numeric(valid_price["precio_unitario_participacion"], errors="coerce").max()) if not valid_price.empty else 0.0

    fecha_min = ""
    fecha_max = ""
    if not valid_price.empty:
        idx_min = pd.to_numeric(valid_price["precio_unitario_participacion"], errors="coerce").idxmin()
        idx_max = pd.to_numeric(valid_price["precio_unitario_participacion"], errors="coerce").idxmax()
        fecha_min = _clean_text(valid_price.loc[idx_min, "fecha_adjudicacion"]) or _clean_text(valid_price.loc[idx_min, "fecha_publicacion"])
        fecha_max = _clean_text(valid_price.loc[idx_max, "fecha_adjudicacion"]) or _clean_text(valid_price.loc[idx_max, "fecha_publicacion"])

    top_menor_precio = []
    if not valid_price.empty:
        agg = (
            valid_price.groupby(["proveedor", "marca"], dropna=False)["precio_unitario_participacion"]
            .mean()
            .reset_index(name="precio_prom")
            .sort_values("precio_prom", ascending=True)
            .head(10)
        )
        top_menor_precio = agg.to_dict(orient="records")

    marcas = sorted([x for x in valid["marca"].fillna("").astype(str).str.strip().unique().tolist() if x])
    modelos = sorted([x for x in valid["modelo"].fillna("").astype(str).str.strip().unique().tolist() if x])
    paises = sorted([x for x in valid["pais_origen"].fillna("").astype(str).str.strip().unique().tolist() if x])
    empresas_ganadoras = int(valid[valid["es_ganador"] == 1]["proveedor"].astype(str).str.strip().replace("", pd.NA).dropna().nunique())
    ref_prom = float(pd.to_numeric(valid_ref["precio_unitario_referencia"], errors="coerce").mean()) if not valid_ref.empty else 0.0

    return {
        "ficha": ficha,
        "run_id_vigente": run_id,
        "estado_estudio": estado,
        "ultima_actualizacion": _utc_now_iso(),
        "total_actos": int(valid["acto_id"].astype(str).nunique()),
        "total_renglones": int(len(valid)),
        "empresas_ganadoras": empresas_ganadoras,
        "marcas_json": _json_dumps(marcas),
        "modelos_json": _json_dumps(modelos),
        "paises_json": _json_dumps(paises),
        "precio_participacion_prom": round(precio_prom, 6),
        "precio_participacion_min": round(precio_min, 6),
        "fecha_precio_min": fecha_min,
        "precio_participacion_max": round(precio_max, 6),
        "fecha_precio_max": fecha_max,
        "precio_referencia_prom": round(ref_prom, 6),
        "top_menor_precio_json": _json_dumps(top_menor_precio),
        "resumen_ia": resumen_ia,
    }


def _build_ai_study_summary(ficha: str, ficha_name: str, detail_df: pd.DataFrame, summary_payload: dict[str, object]) -> str:
    if detail_df.empty:
        return f"Ficha {ficha} ({ficha_name}): no se detectaron renglones para estudio en el histórico disponible."

    total_rows = int(summary_payload.get("total_renglones", 0) or 0)
    total_acts = int(summary_payload.get("total_actos", 0) or 0)
    empresas = int(summary_payload.get("empresas_ganadoras", 0) or 0)
    pmin = float(summary_payload.get("precio_participacion_min", 0.0) or 0.0)
    pprom = float(summary_payload.get("precio_participacion_prom", 0.0) or 0.0)
    pmax = float(summary_payload.get("precio_participacion_max", 0.0) or 0.0)
    review_count = int(pd.to_numeric(detail_df.get("requiere_revision", 0), errors="coerce").fillna(0).sum())

    return (
        f"Ficha {ficha} ({ficha_name}): {total_rows} renglones en {total_acts} actos. "
        f"Empresas ganadoras históricas: {empresas}. "
        f"Precio unitario participación min/prom/max: ${pmin:,.2f} / ${pprom:,.2f} / ${pmax:,.2f}. "
        f"Registros con revisión pendiente: {review_count}."
    )


def _save_study_run(
    ficha: str,
    ficha_name: str,
    detail_df: pd.DataFrame,
    consultas_df: pd.DataFrame,
    db_source: str,
    notes: str = "",
) -> str:
    _ensure_study_db()
    run_id = str(uuid.uuid4())
    now = _utc_now_iso()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        # Política solicitada: conservar solo la última corrida por ficha.
        old_rows = conn.execute("SELECT run_id FROM estudio_runs WHERE ficha=?", (ficha,)).fetchall()
        old_run_ids = [str(r[0]) for r in old_rows if str(r[0] or "").strip()]
        if old_run_ids:
            placeholders = ",".join(["?"] * len(old_run_ids))
            conn.execute(f"DELETE FROM estudio_detalle WHERE run_id IN ({placeholders})", tuple(old_run_ids))
            conn.execute(f"DELETE FROM estudio_consultas WHERE run_id IN ({placeholders})", tuple(old_run_ids))
            conn.execute("DELETE FROM estudio_runs WHERE ficha=?", (ficha,))
        conn.execute("UPDATE estudio_runs SET is_current=0 WHERE ficha=?", (ficha,))
        conn.execute(
            """
            INSERT INTO estudio_runs (
                run_id, ficha, nombre_ficha, estado_run, fecha_inicio, fecha_fin, fuente_db, version_modelo,
                total_items, total_consultas, consultas_resueltas, resumen_ia, notas, is_current, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                ficha,
                ficha_name,
                RUN_STATUS_PENDING if not consultas_df.empty else RUN_STATUS_COMPLETED,
                now,
                now,
                db_source,
                "v1_estudio_fichas",
                int(len(detail_df)),
                int(len(consultas_df)),
                0,
                "",
                notes,
                1,
                now,
                now,
            ),
        )

        if not detail_df.empty:
            d = detail_df.copy()
            d["run_id"] = run_id
            d.to_sql("estudio_detalle", conn, if_exists="append", index=False)
        if not consultas_df.empty:
            q = consultas_df.copy()
            q["run_id"] = run_id
            q["created_at"] = now
            q["updated_at"] = now
            q.to_sql("estudio_consultas", conn, if_exists="append", index=False)

        summary_payload = _compute_study_summary(
            ficha=ficha,
            run_id=run_id,
            detail_df=detail_df,
            estado=RUN_STATUS_PENDING if not consultas_df.empty else RUN_STATUS_COMPLETED,
            resumen_ia="",
        )
        summary_payload["resumen_ia"] = _build_ai_study_summary(ficha, ficha_name, detail_df, summary_payload)
        conn.execute(
            """
            INSERT INTO estudio_resumen_ficha (
                ficha, run_id_vigente, estado_estudio, ultima_actualizacion, total_actos, total_renglones, empresas_ganadoras,
                marcas_json, modelos_json, paises_json, precio_participacion_prom, precio_participacion_min, fecha_precio_min,
                precio_participacion_max, fecha_precio_max, precio_referencia_prom, top_menor_precio_json, resumen_ia
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ficha) DO UPDATE SET
                run_id_vigente=excluded.run_id_vigente,
                estado_estudio=excluded.estado_estudio,
                ultima_actualizacion=excluded.ultima_actualizacion,
                total_actos=excluded.total_actos,
                total_renglones=excluded.total_renglones,
                empresas_ganadoras=excluded.empresas_ganadoras,
                marcas_json=excluded.marcas_json,
                modelos_json=excluded.modelos_json,
                paises_json=excluded.paises_json,
                precio_participacion_prom=excluded.precio_participacion_prom,
                precio_participacion_min=excluded.precio_participacion_min,
                fecha_precio_min=excluded.fecha_precio_min,
                precio_participacion_max=excluded.precio_participacion_max,
                fecha_precio_max=excluded.fecha_precio_max,
                precio_referencia_prom=excluded.precio_referencia_prom,
                top_menor_precio_json=excluded.top_menor_precio_json,
                resumen_ia=excluded.resumen_ia
            """,
            (
                summary_payload["ficha"],
                summary_payload["run_id_vigente"],
                summary_payload["estado_estudio"],
                summary_payload["ultima_actualizacion"],
                summary_payload["total_actos"],
                summary_payload["total_renglones"],
                summary_payload["empresas_ganadoras"],
                summary_payload["marcas_json"],
                summary_payload["modelos_json"],
                summary_payload["paises_json"],
                summary_payload["precio_participacion_prom"],
                summary_payload["precio_participacion_min"],
                summary_payload["fecha_precio_min"],
                summary_payload["precio_participacion_max"],
                summary_payload["fecha_precio_max"],
                summary_payload["precio_referencia_prom"],
                summary_payload["top_menor_precio_json"],
                summary_payload["resumen_ia"],
            ),
        )
        conn.execute(
            "UPDATE estudio_runs SET resumen_ia=?, updated_at=? WHERE run_id=?",
            (summary_payload["resumen_ia"], now, run_id),
        )
        conn.commit()
    _bump_study_data_rev()
    _persist_study_tables_after_write(
        ["estudio_runs", "estudio_detalle", "estudio_consultas", "estudio_resumen_ficha"],
        reason="estudio_consultas",
    )
    _persist_study_tables_after_write(
        ["estudio_runs", "estudio_detalle", "estudio_consultas", "estudio_resumen_ficha"],
        reason="estudio_fichas",
    )
    return run_id


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_runs_df_cached(_rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            """
            SELECT run_id, ficha, nombre_ficha, estado_run, fecha_inicio, fecha_fin, total_items,
                   total_consultas, consultas_resueltas, is_current, updated_at
            FROM estudio_runs
            WHERE COALESCE(is_current, 1) = 1
            ORDER BY datetime(updated_at) DESC
            """,
            conn,
        )


def _load_runs_df() -> pd.DataFrame:
    return _load_runs_df_cached(_get_study_data_rev())


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_run_detail_df_cached(run_id: str, _rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT * FROM estudio_detalle WHERE run_id=? ORDER BY acto_id, proveedor",
            conn,
            params=(run_id,),
        )


def _load_run_detail_df(run_id: str) -> pd.DataFrame:
    return _load_run_detail_df_cached(str(run_id or ""), _get_study_data_rev())


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_run_queries_df_cached(run_id: str, _rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT * FROM estudio_consultas WHERE run_id=? ORDER BY estado, acto_id, campo_dudoso",
            conn,
            params=(run_id,),
        )


def _load_run_queries_df(run_id: str) -> pd.DataFrame:
    return _load_run_queries_df_cached(str(run_id or ""), _get_study_data_rev())


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_resumen_estudiadas_df_cached(_rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            """
            SELECT r.*, s.nombre_ficha
            FROM estudio_resumen_ficha r
            LEFT JOIN estudio_runs s ON r.run_id_vigente = s.run_id
            ORDER BY datetime(r.ultima_actualizacion) DESC
            """,
            conn,
        )


def _load_resumen_estudiadas_df() -> pd.DataFrame:
    return _load_resumen_estudiadas_df_cached(_get_study_data_rev())


def _apply_query_resolution(run_id: str, responses: list[dict[str, str]]) -> None:
    _ensure_study_db()
    now = _utc_now_iso()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        for item in responses:
            consulta_id = str(item.get("consulta_id", "")).strip()
            if not consulta_id:
                continue
            respuesta = str(item.get("respuesta_seleccionada", "")).strip()
            valor_manual = str(item.get("valor_manual", "")).strip()
            estado = "resuelta" if (respuesta or valor_manual) else "pendiente"

            conn.execute(
                """
                UPDATE estudio_consultas
                SET respuesta_seleccionada=?, valor_manual=?, estado=?, updated_at=?
                WHERE consulta_id=? AND run_id=?
                """,
                (respuesta, valor_manual, estado, now, consulta_id, run_id),
            )

            query_row = conn.execute(
                "SELECT campo_dudoso, detail_id, acto_id FROM estudio_consultas WHERE consulta_id=?",
                (consulta_id,),
            ).fetchone()
            if not query_row:
                continue
            campo_dudoso, detail_id, acto_id = query_row
            campo_dudoso = str(campo_dudoso or "").strip().lower()

            if campo_dudoso == "renglon_correspondencia_ficha" and respuesta == "excluir_acto":
                conn.execute(
                    """
                    UPDATE estudio_detalle
                    SET estado_revision='excluido', requiere_revision=1,
                        observaciones=COALESCE(observaciones,'') || ' | Excluido por validación manual'
                    WHERE run_id=? AND acto_id=?
                    """,
                    (run_id, str(acto_id or "")),
                )
            elif campo_dudoso == "precio_unitario_participacion" and detail_id:
                candidate = valor_manual or respuesta
                if str(candidate).strip().lower() != "vacio":
                    parsed = _parse_number(candidate)
                    if parsed > 0:
                        conn.execute(
                            """
                            UPDATE estudio_detalle
                            SET precio_unitario_participacion=?, requiere_revision=0,
                                estado_revision='ok', nivel_certeza=0.9
                            WHERE detail_id=? AND run_id=?
                            """,
                            (parsed, detail_id, run_id),
                        )
            elif campo_dudoso == "pais_origen" and detail_id:
                candidate = valor_manual or respuesta
                if str(candidate).strip().lower() not in {"", "vacio", "manual"}:
                    conn.execute(
                        """
                        UPDATE estudio_detalle
                        SET pais_origen=?, requiere_revision=0, estado_revision='ok'
                        WHERE detail_id=? AND run_id=?
                        """,
                        (candidate, detail_id, run_id),
                    )

        pending_count = conn.execute(
            "SELECT COUNT(*) FROM estudio_consultas WHERE run_id=? AND obligatoria=1 AND estado!='resuelta'",
            (run_id,),
        ).fetchone()[0]
        resolved_count = conn.execute(
            "SELECT COUNT(*) FROM estudio_consultas WHERE run_id=? AND estado='resuelta'",
            (run_id,),
        ).fetchone()[0]

        detail_df = pd.read_sql_query("SELECT * FROM estudio_detalle WHERE run_id=?", conn, params=(run_id,))
        run_row = conn.execute("SELECT ficha, nombre_ficha FROM estudio_runs WHERE run_id=?", (run_id,)).fetchone()
        if run_row:
            ficha, nombre = run_row
            status = RUN_STATUS_PENDING if int(pending_count) > 0 else RUN_STATUS_COMPLETED
            unresolved_review = int(pd.to_numeric(detail_df.get("requiere_revision", 0), errors="coerce").fillna(0).sum())
            if status == RUN_STATUS_COMPLETED and unresolved_review > 0:
                status = RUN_STATUS_COMPLETED_OBS

            summary_payload = _compute_study_summary(str(ficha), run_id, detail_df, status, "")
            summary_payload["resumen_ia"] = _build_ai_study_summary(str(ficha), str(nombre or ""), detail_df, summary_payload)

            conn.execute(
                """
                UPDATE estudio_runs
                SET estado_run=?, consultas_resueltas=?, resumen_ia=?, fecha_fin=?, updated_at=?
                WHERE run_id=?
                """,
                (status, int(resolved_count), summary_payload["resumen_ia"], now, now, run_id),
            )
            conn.execute(
                """
                INSERT INTO estudio_resumen_ficha (
                    ficha, run_id_vigente, estado_estudio, ultima_actualizacion, total_actos, total_renglones, empresas_ganadoras,
                    marcas_json, modelos_json, paises_json, precio_participacion_prom, precio_participacion_min, fecha_precio_min,
                    precio_participacion_max, fecha_precio_max, precio_referencia_prom, top_menor_precio_json, resumen_ia
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ficha) DO UPDATE SET
                    run_id_vigente=excluded.run_id_vigente,
                    estado_estudio=excluded.estado_estudio,
                    ultima_actualizacion=excluded.ultima_actualizacion,
                    total_actos=excluded.total_actos,
                    total_renglones=excluded.total_renglones,
                    empresas_ganadoras=excluded.empresas_ganadoras,
                    marcas_json=excluded.marcas_json,
                    modelos_json=excluded.modelos_json,
                    paises_json=excluded.paises_json,
                    precio_participacion_prom=excluded.precio_participacion_prom,
                    precio_participacion_min=excluded.precio_participacion_min,
                    fecha_precio_min=excluded.fecha_precio_min,
                    precio_participacion_max=excluded.precio_participacion_max,
                    fecha_precio_max=excluded.fecha_precio_max,
                    precio_referencia_prom=excluded.precio_referencia_prom,
                    top_menor_precio_json=excluded.top_menor_precio_json,
                    resumen_ia=excluded.resumen_ia
                """,
                (
                    summary_payload["ficha"],
                    summary_payload["run_id_vigente"],
                    summary_payload["estado_estudio"],
                    summary_payload["ultima_actualizacion"],
                    summary_payload["total_actos"],
                    summary_payload["total_renglones"],
                    summary_payload["empresas_ganadoras"],
                    summary_payload["marcas_json"],
                    summary_payload["modelos_json"],
                    summary_payload["paises_json"],
                    summary_payload["precio_participacion_prom"],
                    summary_payload["precio_participacion_min"],
                    summary_payload["fecha_precio_min"],
                    summary_payload["precio_participacion_max"],
                    summary_payload["fecha_precio_max"],
                    summary_payload["precio_referencia_prom"],
                    summary_payload["top_menor_precio_json"],
                    summary_payload["resumen_ia"],
                ),
            )
        conn.commit()
    _bump_study_data_rev()

def _loads_json_obj(raw: object) -> dict[str, object]:
    text = _clean_text(raw)
    if not text:
        return {}
    try:
        loaded = json.loads(text)
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        pass
    parsed = _parse_json_block(text)
    return parsed if isinstance(parsed, dict) else {}


def _loads_json_list(raw: object) -> list[object]:
    text = _clean_text(raw)
    if not text:
        return []
    try:
        loaded = json.loads(text)
        if isinstance(loaded, list):
            return loaded
    except Exception:
        pass
    return []


def _to_clean_str_list(raw: object, max_items: int = 50) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    values: list[object]
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, str):
        lst = _loads_json_list(raw)
        if lst:
            values = lst
        else:
            values = re.split(r"[|,;\n]", raw)
    else:
        values = [raw]
    for item in values:
        txt = _clean_text(item)
        if not txt or txt in seen:
            continue
        seen.add(txt)
        out.append(txt)
        if len(out) >= max_items:
            break
    return out


def _extract_keywords_context(*parts: object, max_items: int = 20) -> list[str]:
    joined = " ".join([_clean_text(p) for p in parts if _clean_text(p)])
    if not joined:
        return []
    tokens = re.findall(r"[A-Za-zÁÉÍÓÚÑáéíóúñ0-9]{4,}", joined)
    stop = {
        "para",
        "con",
        "del",
        "los",
        "las",
        "por",
        "que",
        "ficha",
        "tecnica",
        "juego",
        "unidad",
        "baja",
        "alta",
    }
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        tk = token.strip()
        if not tk:
            continue
        key = _normalize_column_key(tk)
        if key in stop or key in seen:
            continue
        seen.add(key)
        out.append(tk.upper())
        if len(out) >= max_items:
            break
    return out


def _parse_ficha_batch_input(raw: object) -> list[str]:
    text = _clean_text(raw)
    if not text:
        return []
    chunks = [c for c in re.split(r"[,;\n]+", text) if _clean_text(c)]
    out: list[str] = []
    seen: set[str] = set()
    for chunk in chunks:
        c = _clean_text(chunk)
        if not c:
            continue
        m = re.search(r"\d{3,8}", c)
        ficha = m.group(0) if m else re.sub(r"\D", "", c)
        ficha = _clean_text(ficha)
        if not ficha or ficha in seen:
            continue
        seen.add(ficha)
        out.append(ficha)
    return out


def _build_ficha_lookup_maps(ranked_df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    name_map: dict[str, str] = {}
    link_map: dict[str, str] = {}
    if not ranked_df.empty:
        for _, row in ranked_df.iterrows():
            ficha = _clean_text(row.get("ficha"))
            if not ficha:
                continue
            if ficha not in name_map:
                name_map[ficha] = _clean_text(row.get("nombre_ficha"))
            if ficha not in link_map:
                link_map[ficha] = _clean_text(row.get("enlace_minsa"))
    ref_map = _load_ficha_reference_map()
    for ficha, payload in (ref_map or {}).items():
        key = _clean_text(ficha)
        if not key:
            continue
        if key not in name_map:
            name_map[key] = _clean_text((payload or {}).get("nombre_ficha", ""))
        if key not in link_map:
            link_map[key] = _clean_text((payload or {}).get("enlace_minsa", ""))
    for key, link in list(link_map.items()):
        link_map[key] = _safe_minsa_link(link)
    return name_map, link_map


def _build_context_payload_from_study(
    ficha: str,
    nombre_ficha: str,
    enlace_ficha: str,
    resumen_row: pd.Series,
    detail_df: pd.DataFrame,
) -> dict[str, object]:
    marcas = _to_clean_str_list(resumen_row.get("marcas_json", "[]"))
    modelos = _to_clean_str_list(resumen_row.get("modelos_json", "[]"))
    paises = _to_clean_str_list(resumen_row.get("paises_json", "[]"))
    resumen_ia = _clean_text(resumen_row.get("resumen_ia", ""))

    proveedores_hist: list[str] = []
    if not detail_df.empty:
        if "proveedor_ganador" in detail_df.columns:
            proveedores_hist.extend(
                [
                    _clean_text(x)
                    for x in detail_df["proveedor_ganador"].dropna().astype(str).tolist()
                    if _clean_text(x)
                ]
            )
        if not proveedores_hist and "proveedor" in detail_df.columns:
            proveedores_hist.extend(
                [
                    _clean_text(x)
                    for x in detail_df["proveedor"].dropna().astype(str).tolist()
                    if _clean_text(x)
                ]
            )
    proveedores_hist = _to_clean_str_list(proveedores_hist)

    proveedores_historicos_detalle: list[dict[str, object]] = []
    if not detail_df.empty:
        profiles: dict[str, dict[str, object]] = {}
        for _, drow in detail_df.iterrows():
            prov = (
                _clean_text(drow.get("proveedor_ganador", ""))
                or _clean_text(drow.get("proveedor", ""))
            )
            if not prov:
                continue
            prov_key = _canonical_party_name(prov) or _normalize_column_key(prov)
            base = profiles.setdefault(
                prov_key,
                {
                    "proveedor": prov,
                    "_marcas": set(),
                    "_modelos": set(),
                    "_paises": set(),
                    "_actos": set(),
                    "_precios": [],
                },
            )
            if not _clean_text(base.get("proveedor", "")):
                base["proveedor"] = prov
            marca = _clean_text(drow.get("marca", ""))
            modelo = _clean_text(drow.get("modelo", ""))
            pais = _clean_text(drow.get("pais_origen", ""))
            if marca:
                base["_marcas"].add(marca)
            if modelo:
                base["_modelos"].add(modelo)
            if pais:
                base["_paises"].add(pais)
            acto_ref = _clean_text(drow.get("acto_id", "")) or _clean_text(drow.get("acto_nombre", ""))
            if acto_ref:
                base["_actos"].add(acto_ref)
            precio = _safe_float(drow.get("precio_unitario_participacion", 0.0), 0.0)
            if precio > 0:
                base["_precios"].append(float(precio))

        for entry in profiles.values():
            price_vals = [float(v) for v in entry.get("_precios", []) if float(v) > 0]
            proveedores_historicos_detalle.append(
                {
                    "proveedor": _clean_text(entry.get("proveedor", "")),
                    "marcas": sorted([str(x) for x in entry.get("_marcas", set()) if _clean_text(x)]),
                    "modelos": sorted([str(x) for x in entry.get("_modelos", set()) if _clean_text(x)]),
                    "paises_origen": sorted([str(x) for x in entry.get("_paises", set()) if _clean_text(x)]),
                    "actos_relacionados": int(len(entry.get("_actos", set()))),
                    "precio_unitario_min": (round(min(price_vals), 6) if price_vals else 0.0),
                    "precio_unitario_prom": (round(sum(price_vals) / len(price_vals), 6) if price_vals else 0.0),
                    "precio_unitario_max": (round(max(price_vals), 6) if price_vals else 0.0),
                }
            )
        proveedores_historicos_detalle = sorted(
            proveedores_historicos_detalle,
            key=lambda x: (_normalize_column_key(x.get("proveedor", "")), x.get("proveedor", "")),
        )

    descripcion = _clean_text(nombre_ficha)
    if not detail_df.empty and "renglon_texto" in detail_df.columns:
        cand = (
            detail_df["renglon_texto"]
            .fillna("")
            .astype(str)
            .map(_clean_text)
            .loc[lambda s: s != ""]
            .head(1)
            .tolist()
        )
        if cand:
            descripcion = cand[0]
    palabras = _extract_keywords_context(
        nombre_ficha,
        descripcion,
        " ".join(marcas),
        " ".join(modelos),
        " ".join(proveedores_hist),
    )

    contexto = {
        "ficha_id": ficha,
        "nombre_ficha": nombre_ficha,
        "enlace_ficha_tecnica": enlace_ficha or CTNI_CONSULTA_URL,
        "descripcion_producto": descripcion,
        "palabras_clave": palabras,
        "marcas_detectadas_historicamente": marcas,
        "modelos_detectados_historicamente": modelos,
        "paises_detectados_historicamente": paises,
        "proveedores_historicos_detectados": proveedores_hist,
        "proveedores_historicos_detalle": proveedores_historicos_detalle,
        "resumen_breve_estudio_historico": resumen_ia,
    }
    return contexto


def _build_prompt_for_chatgpt(contexto: dict[str, object]) -> str:
    ficha_id = _clean_text(contexto.get("ficha_id", ""))
    link = _safe_minsa_link(contexto.get("enlace_ficha_tecnica", ""))
    json_context = json.dumps(contexto, ensure_ascii=False, indent=2)
    return (
        "Use SOLO el contexto suministrado para estructurar un analisis de proveedores.\n"
        "Responde UNICAMENTE JSON valido (sin markdown) con esta estructura minima:\n"
        "{\n"
        '  "metadata_consulta": {...},\n'
        '  "contexto_ficha": {"ficha_id":"...","nombre_ficha":"...","enlace_ficha_tecnica":"..."},\n'
        '  "proveedores_historicos_panama": [ ... ],\n'
        '  "proveedores_externos_clasificados": {"mejor_gama":[...], "mejor_precio":[...]},\n'
        '  "resumen_ejecutivo": "..." \n'
        "}\n"
        "Reglas:\n"
        "- incluir entre 5 y 10 proveedores externos adicionales confiables cuando sea posible.\n"
        "- por cada proveedor incluir telefono, contacto_email, contacto_whatsapp, canal_contacto_mas_probable,\n"
        "  correo_inicial_listo y whatsapp_inicial_listo.\n"
        "- Usa `proveedores_historicos_detalle` para mantener asociacion proveedor-marca-modelo-pais.\n"
        "- No mezclar marcas/paises entre proveedores distintos; si falta dato, dejar vacio.\n"
        "- El correo_inicial_listo DEBE iniciar exactamente con:\n"
        "\"My name is Rodrigo Sánchez and I represent RIR Medical Engineering, a company based in Panama focused on supplying medical products to hospitals and public institutions.\"\n"
        f"- El correo y whatsapp deben referenciar la ficha {ficha_id} y el enlace {link}.\n"
        "- Cerrar correo con:\n"
        "\"If so, we would appreciate receiving your quotation, as we are interested in distributing your products in Panama.\"\n"
        "- WhatsApp corto, directo y con solicitud de cumplimiento + cotizacion.\n\n"
        f"CONTEXTO:\n{json_context}"
    )


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_ap_context_df_cached(_rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT * FROM analisis_proveedores_contexto ORDER BY datetime(updated_at) DESC, ficha ASC",
            conn,
        )


def _load_ap_context_df() -> pd.DataFrame:
    return _load_ap_context_df_cached(_get_study_data_rev())


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_ap_active_versions_df_cached(_rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            """
            SELECT *
            FROM analisis_proveedores_version
            WHERE COALESCE(is_active, 1)=1
            ORDER BY datetime(updated_at) DESC, ficha ASC
            """,
            conn,
        )


def _load_ap_active_versions_df() -> pd.DataFrame:
    return _load_ap_active_versions_df_cached(_get_study_data_rev())


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_ap_versions_df_cached(_rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            """
            SELECT *
            FROM analisis_proveedores_version
            ORDER BY ficha ASC, version_num DESC, datetime(updated_at) DESC
            """,
            conn,
        )


def _load_ap_versions_df() -> pd.DataFrame:
    return _load_ap_versions_df_cached(_get_study_data_rev())


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_ap_comments_df_cached(_rev: int) -> pd.DataFrame:
    _ensure_study_db()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            """
            SELECT *
            FROM analisis_proveedores_comentarios
            ORDER BY datetime(created_at) DESC, ficha ASC
            """,
            conn,
        )


def _load_ap_comments_df() -> pd.DataFrame:
    return _load_ap_comments_df_cached(_get_study_data_rev())


def _save_ap_comment(ficha: str, comentario: str) -> tuple[bool, str]:
    _ensure_study_db()
    ficha = _clean_text(ficha)
    comentario_txt = _clean_text(comentario)
    if not ficha:
        return False, "Falta ficha para guardar comentario."
    if not comentario_txt:
        return False, "El comentario está vacío."

    now = _utc_now_iso()
    comentario_id = str(uuid.uuid4())
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        ctx_row = conn.execute(
            """
            SELECT analisis_id_activo, run_id_estudio
            FROM analisis_proveedores_contexto
            WHERE ficha=?
            """,
            (ficha,),
        ).fetchone()
        analisis_id = _clean_text(ctx_row[0]) if ctx_row and len(ctx_row) > 0 else ""
        run_id_estudio = _clean_text(ctx_row[1]) if ctx_row and len(ctx_row) > 1 else ""

        conn.execute(
            """
            INSERT INTO analisis_proveedores_comentarios (
                comentario_id, ficha, analisis_id, run_id_estudio, usuario, comentario, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comentario_id,
                ficha,
                analisis_id,
                run_id_estudio,
                _current_user(),
                comentario_txt,
                now,
                now,
            ),
        )
        conn.commit()

    _bump_study_data_rev()
    _persist_study_tables_after_write(
        ["analisis_proveedores_comentarios"],
        reason="ap_comentarios",
    )
    return True, "Comentario guardado."


def _ensure_provider_analysis_contexts(resumen_df: pd.DataFrame, ranked_df: pd.DataFrame) -> tuple[int, int]:
    _ensure_study_db()
    if resumen_df.empty:
        return 0, 0

    name_map, link_map = _build_ficha_lookup_maps(ranked_df)
    now = _utc_now_iso()
    created = 0
    updated = 0
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        existing_ctx_df = pd.read_sql_query("SELECT * FROM analisis_proveedores_contexto", conn)
        existing_ctx: dict[str, dict[str, object]] = {}
        if not existing_ctx_df.empty:
            existing_ctx = {
                _clean_text(r.get("ficha", "")): r
                for r in existing_ctx_df.to_dict(orient="records")
                if _clean_text(r.get("ficha", ""))
            }

        active_versions_df = pd.read_sql_query(
            "SELECT ficha, analisis_id, version_num FROM analisis_proveedores_version WHERE COALESCE(is_active,1)=1",
            conn,
        )
        active_map: dict[str, dict[str, object]] = {}
        if not active_versions_df.empty:
            active_map = {
                _clean_text(r.get("ficha", "")): r
                for r in active_versions_df.to_dict(orient="records")
                if _clean_text(r.get("ficha", ""))
            }

        for _, row in resumen_df.iterrows():
            ficha = _clean_text(row.get("ficha", ""))
            if not ficha:
                continue
            run_id = _clean_text(row.get("run_id_vigente", ""))
            nombre = _clean_text(row.get("nombre_ficha", "")) or _clean_text(name_map.get(ficha, ""))
            enlace = _safe_minsa_link(link_map.get(ficha, "") or row.get("enlace_minsa", ""))
            detail_df = pd.DataFrame()
            if run_id:
                try:
                    detail_df = pd.read_sql_query(
                        "SELECT * FROM estudio_detalle WHERE run_id=? ORDER BY acto_id, proveedor",
                        conn,
                        params=(run_id,),
                    )
                except Exception:
                    detail_df = _load_run_detail_df(run_id)
            contexto_obj = _build_context_payload_from_study(ficha, nombre, enlace, row, detail_df)
            contexto_texto = json.dumps(contexto_obj, ensure_ascii=False, indent=2)
            prompt_texto = _build_prompt_for_chatgpt(contexto_obj)

            current = existing_ctx.get(ficha, {})
            has_active = ficha in active_map
            prev_state = _clean_text(current.get("estado_analisis", ""))
            if prev_state == AP_STATE_PENDING_JSON:
                desired_state = AP_STATE_PENDING_JSON
            elif has_active:
                version_num = int(_safe_int(active_map[ficha].get("version_num", 1)))
                desired_state = AP_STATE_UPDATED if version_num > 1 else AP_STATE_COMPLETED
            else:
                desired_state = AP_STATE_PENDING_JSON
            active_id = _clean_text((active_map.get(ficha) or {}).get("analisis_id", ""))

            if not current:
                conn.execute(
                    """
                    INSERT INTO analisis_proveedores_contexto (
                        ficha, run_id_estudio, nombre_ficha, enlace_ficha_tecnica, descripcion_producto, palabras_clave,
                        marcas_detectadas_historicamente, modelos_detectados_historicamente, paises_detectados_historicamente,
                        proveedores_historicos_detectados, resumen_estudio_breve, contexto_texto, prompt_texto,
                        estado_analisis, analisis_id_activo, fecha_contexto_generado, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ficha,
                        run_id,
                        nombre,
                        enlace,
                        _clean_text(contexto_obj.get("descripcion_producto", "")),
                        json.dumps(contexto_obj.get("palabras_clave", []), ensure_ascii=False),
                        json.dumps(contexto_obj.get("marcas_detectadas_historicamente", []), ensure_ascii=False),
                        json.dumps(contexto_obj.get("modelos_detectados_historicamente", []), ensure_ascii=False),
                        json.dumps(contexto_obj.get("paises_detectados_historicamente", []), ensure_ascii=False),
                        json.dumps(contexto_obj.get("proveedores_historicos_detectados", []), ensure_ascii=False),
                        _clean_text(contexto_obj.get("resumen_breve_estudio_historico", "")),
                        contexto_texto,
                        prompt_texto,
                        desired_state,
                        active_id,
                        now,
                        now,
                        now,
                    ),
                )
                created += 1
                continue

            needs_update = False
            current_ctx_payload = _loads_json_obj(current.get("contexto_texto", ""))
            if _clean_text(current.get("run_id_estudio", "")) != run_id:
                needs_update = True
            if _clean_text(current.get("prompt_texto", "")) == "":
                needs_update = True
            if _clean_text(current.get("contexto_texto", "")) == "":
                needs_update = True
            # Fuerza regeneracion cuando el contexto guardado no trae la asociacion
            # proveedor -> marcas/modelos/paises.
            if "proveedores_historicos_detalle" not in current_ctx_payload:
                needs_update = True
            if _clean_text(current.get("estado_analisis", "")) != desired_state:
                needs_update = True
            if _clean_text(current.get("analisis_id_activo", "")) != active_id:
                needs_update = True
            if not needs_update:
                continue

            conn.execute(
                """
                UPDATE analisis_proveedores_contexto
                SET run_id_estudio=?, nombre_ficha=?, enlace_ficha_tecnica=?, descripcion_producto=?, palabras_clave=?,
                    marcas_detectadas_historicamente=?, modelos_detectados_historicamente=?, paises_detectados_historicamente=?,
                    proveedores_historicos_detectados=?, resumen_estudio_breve=?, contexto_texto=?, prompt_texto=?,
                    estado_analisis=?, analisis_id_activo=?, updated_at=?
                WHERE ficha=?
                """,
                (
                    run_id,
                    nombre,
                    enlace,
                    _clean_text(contexto_obj.get("descripcion_producto", "")),
                    json.dumps(contexto_obj.get("palabras_clave", []), ensure_ascii=False),
                    json.dumps(contexto_obj.get("marcas_detectadas_historicamente", []), ensure_ascii=False),
                    json.dumps(contexto_obj.get("modelos_detectados_historicamente", []), ensure_ascii=False),
                    json.dumps(contexto_obj.get("paises_detectados_historicamente", []), ensure_ascii=False),
                    json.dumps(contexto_obj.get("proveedores_historicos_detectados", []), ensure_ascii=False),
                    _clean_text(contexto_obj.get("resumen_breve_estudio_historico", "")),
                    contexto_texto,
                    prompt_texto,
                    desired_state,
                    active_id,
                    now,
                    ficha,
                ),
            )
            updated += 1

        conn.commit()
    if created or updated:
        _bump_study_data_rev()
        _persist_study_tables_after_write(
            ["analisis_proveedores_contexto"],
            reason="analisis_contexto",
        )
    return created, updated


def _validate_provider_analysis_json(raw_text: str, ficha_open: str) -> tuple[dict[str, object], list[str], list[str]]:
    payload = _parse_json_block(raw_text)
    errors: list[str] = []
    warnings: list[str] = []
    if not payload:
        errors.append("JSON inválido o vacío.")
        return {}, errors, warnings

    metadata = payload.get("metadata_consulta")
    if not isinstance(metadata, dict):
        errors.append("Falta `metadata_consulta` (objeto).")

    contexto_ficha = payload.get("contexto_ficha")
    if not isinstance(contexto_ficha, dict):
        errors.append("Falta `contexto_ficha` (objeto).")
        contexto_ficha = {}

    ficha_json = _clean_text(contexto_ficha.get("ficha_id", "")) or _clean_text(payload.get("ficha_id", ""))
    if not ficha_json:
        errors.append("Falta `ficha_id` en el JSON.")
    if ficha_json and _clean_text(ficha_open) and ficha_json != _clean_text(ficha_open):
        errors.append(f"El `ficha_id` del JSON ({ficha_json}) no coincide con la ficha abierta ({ficha_open}).")

    historicos = payload.get("proveedores_historicos_panama")
    if not isinstance(historicos, list):
        errors.append("Falta `proveedores_historicos_panama` (lista).")
        historicos = []

    externos = payload.get("proveedores_externos_clasificados")
    if not isinstance(externos, dict):
        errors.append("Falta `proveedores_externos_clasificados` (objeto).")
        externos = {}
    mejor_gama = externos.get("mejor_gama")
    if not isinstance(mejor_gama, list):
        errors.append("Falta `proveedores_externos_clasificados.mejor_gama` (lista).")
        mejor_gama = []
    mejor_precio = externos.get("mejor_precio")
    if not isinstance(mejor_precio, list):
        errors.append("Falta `proveedores_externos_clasificados.mejor_precio` (lista).")
        mejor_precio = []

    if "resumen_ejecutivo" not in payload:
        errors.append("Falta `resumen_ejecutivo`.")

    total_ext = len(mejor_gama) + len(mejor_precio)
    if total_ext < 5:
        warnings.append(
            "Se detectaron menos de 5 proveedores externos entre gama+precio. "
            "Se permite guardar, pero revisa cobertura."
        )

    return payload, errors, warnings


def _rows_from_payload_list(payload_list: object, expected_cols: list[str], ficha: str, analisis_id: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    data = payload_list if isinstance(payload_list, list) else []
    for item in data:
        if not isinstance(item, dict):
            continue
        row: dict[str, object] = {"analisis_id": analisis_id, "ficha": ficha}
        for col in expected_cols:
            value = item.get(col, "")
            if col in {
                "cantidad_actos_ganados",
                "precio_promedio_historico",
                "precio_minimo_historico",
                "precio_maximo_historico",
            }:
                row[col] = _safe_float(value, 0.0)
            else:
                row[col] = _clean_text(value)
        rows.append(row)
    base_cols = ["analisis_id", "ficha"] + expected_cols
    if not rows:
        return pd.DataFrame(columns=base_cols)
    return pd.DataFrame(rows)[base_cols]


def _save_provider_analysis_payload(ficha: str, payload: dict[str, object]) -> tuple[str, int, str]:
    _ensure_study_db()
    ficha = _clean_text(ficha)
    now = _utc_now_iso()
    metadata = payload.get("metadata_consulta", {})
    contexto_ficha = payload.get("contexto_ficha", {})
    resumen_ejecutivo = _clean_text(payload.get("resumen_ejecutivo", ""))
    externos = payload.get("proveedores_externos_clasificados", {})
    historicos = payload.get("proveedores_historicos_panama", [])
    mejor_gama = externos.get("mejor_gama", []) if isinstance(externos, dict) else []
    mejor_precio = externos.get("mejor_precio", []) if isinstance(externos, dict) else []

    analisis_id = str(uuid.uuid4())
    version_num = 1
    state_out = AP_STATE_COMPLETED

    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(version_num), 0) FROM analisis_proveedores_version WHERE ficha=?",
            (ficha,),
        ).fetchone()
        prev_max = int(row[0] or 0)
        version_num = prev_max + 1
        state_out = AP_STATE_UPDATED if prev_max > 0 else AP_STATE_COMPLETED

        # Mantener SOLO la version mas reciente por ficha:
        # antes de guardar, elimina analisis previos y sus tablas hijas.
        prev_ids = [
            _clean_text(r[0])
            for r in conn.execute(
                "SELECT analisis_id FROM analisis_proveedores_version WHERE ficha=?",
                (ficha,),
            ).fetchall()
            if _clean_text(r[0])
        ]
        if prev_ids:
            placeholders = ",".join(["?"] * len(prev_ids))
            conn.execute(
                f"DELETE FROM analisis_proveedores_hist_panama WHERE analisis_id IN ({placeholders})",
                tuple(prev_ids),
            )
            conn.execute(
                f"DELETE FROM analisis_proveedores_mejor_gama WHERE analisis_id IN ({placeholders})",
                tuple(prev_ids),
            )
            conn.execute(
                f"DELETE FROM analisis_proveedores_mejor_precio WHERE analisis_id IN ({placeholders})",
                tuple(prev_ids),
            )
            conn.execute(
                f"DELETE FROM analisis_proveedores_version WHERE analisis_id IN ({placeholders})",
                tuple(prev_ids),
            )
        conn.execute(
            """
            INSERT INTO analisis_proveedores_version (
                analisis_id, ficha, version_num, metadata_consulta_json, contexto_ficha_json, resumen_ejecutivo,
                json_raw, estado_version, fecha_carga, fecha_ultima_actualizacion, created_at, updated_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                analisis_id,
                ficha,
                int(version_num),
                json.dumps(metadata, ensure_ascii=False),
                json.dumps(contexto_ficha, ensure_ascii=False),
                resumen_ejecutivo,
                json.dumps(payload, ensure_ascii=False),
                state_out,
                now,
                now,
                now,
                now,
            ),
        )

        hist_df = _rows_from_payload_list(historicos, AP_HIST_COLUMNS, ficha, analisis_id)
        gama_df = _rows_from_payload_list(mejor_gama, AP_GAMA_COLUMNS, ficha, analisis_id)
        precio_df = _rows_from_payload_list(mejor_precio, AP_PRECIO_COLUMNS, ficha, analisis_id)
        if not hist_df.empty:
            hist_df.to_sql("analisis_proveedores_hist_panama", conn, if_exists="append", index=False)
        if not gama_df.empty:
            gama_df.to_sql("analisis_proveedores_mejor_gama", conn, if_exists="append", index=False)
        if not precio_df.empty:
            precio_df.to_sql("analisis_proveedores_mejor_precio", conn, if_exists="append", index=False)

        conn.execute(
            """
            UPDATE analisis_proveedores_contexto
            SET estado_analisis=?, analisis_id_activo=?, updated_at=?
            WHERE ficha=?
            """,
            (state_out, analisis_id, now, ficha),
        )
        conn.commit()

    _bump_study_data_rev()
    _persist_study_tables_after_write(
        [
            "analisis_proveedores_contexto",
            "analisis_proveedores_version",
            "analisis_proveedores_hist_panama",
            "analisis_proveedores_mejor_gama",
            "analisis_proveedores_mejor_precio",
        ],
        reason="analisis_proveedores",
    )
    return analisis_id, int(version_num), state_out


def _mark_provider_analysis_pending(ficha: str) -> None:
    _ensure_study_db()
    now = _utc_now_iso()
    ficha = _clean_text(ficha)
    if not ficha:
        return
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE analisis_proveedores_contexto
            SET estado_analisis=?, updated_at=?
            WHERE ficha=?
            """,
            (AP_STATE_PENDING_JSON, now, ficha),
        )
        conn.commit()
    _bump_study_data_rev()
    _persist_study_tables_after_write(
        ["analisis_proveedores_contexto"],
        reason="analisis_pendiente_json",
    )


@st.cache_data(show_spinner=False, ttl=INTEL_STUDY_SQL_CACHE_TTL)
def _load_ap_table_by_analisis_cached(table_name: str, analisis_id: str, _rev: int) -> pd.DataFrame:
    _ensure_study_db()
    if not _clean_text(analisis_id):
        return pd.DataFrame()
    with sqlite3.connect(INTEL_STUDY_DB_PATH) as conn:
        return pd.read_sql_query(
            f"SELECT * FROM {table_name} WHERE analisis_id=? ORDER BY id ASC",
            conn,
            params=(analisis_id,),
        )


def _load_ap_table_by_analisis(table_name: str, analisis_id: str) -> pd.DataFrame:
    return _load_ap_table_by_analisis_cached(table_name, _clean_text(analisis_id), _get_study_data_rev())


def _empty_table(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _placeholder_block(title: str, text: str, columns: list[str] | None = None) -> None:
    st.markdown(f"#### {title}")
    st.info(text)
    if columns:
        st.caption("Formato esperado de tabla (Fase 1, sin datos):")
        st.dataframe(_empty_table(columns), use_container_width=True, hide_index=True)


def _render_sidebar() -> None:
    st.sidebar.markdown("### 🎛️ Filtros globales")
    st.sidebar.selectbox("Ficha", ["Todas"], index=0)
    st.sidebar.selectbox("Estado ficha", ["Todos"], index=0)
    st.sidebar.selectbox("Prioridad", ["Todas"], index=0)
    st.sidebar.selectbox("Proveedor", ["Todos"], index=0)
    st.sidebar.selectbox("País", ["Todos"], index=0)
    st.sidebar.selectbox("Clasificación de contacto", ["Todas"], index=0)
    st.sidebar.checkbox("Solo con contacto encontrado", value=False)
    st.sidebar.checkbox("Solo con seguimiento vencido", value=False)
    st.sidebar.checkbox("Solo viable: prov. en conv.", value=False)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚡ Acciones rápidas")
    st.sidebar.button("Recalcular scoring", disabled=True)
    st.sidebar.button("Actualizar tablero", disabled=True)
    st.sidebar.button("Exportar resumen", disabled=True)
    st.sidebar.button("Limpiar filtros", disabled=True)
    st.sidebar.caption("Fase 1: botones visuales (sin ejecución).")


def _render_kpis(ranked_df: pd.DataFrame) -> None:
    st.markdown("### Centro de control")
    total_fichas = int(len(ranked_df))
    top_ataque = int((ranked_df.get("clasificacion", pd.Series(dtype=str)) == "atacar ya").sum()) if not ranked_df.empty else 0
    prometedoras = int((ranked_df.get("clasificacion", pd.Series(dtype=str)) == "prometedor").sum()) if not ranked_df.empty else 0
    # Carga local rapida para no bloquear la entrada por sincronizacion remota.
    fichas_estudio = _ensure_study_state(sync_remote=False)
    total_en_seguimiento = len(fichas_estudio)
    total_en_estudio = sum(1 for x in fichas_estudio if str(x.get("estado", "")).strip().lower() == "en estudio")

    cols = st.columns(5)
    cols[0].metric("Fichas det. con actos", f"{total_fichas:,}")
    cols[1].metric("Fichas en seg.", f"{total_en_seguimiento:,}")
    cols[2].metric("Fichas en est.", f"{total_en_estudio:,}")
    cols[3].metric("Seg. vencidos", "0")
    cols[4].metric("Correos por env.", "0")

    cols2 = st.columns(5)
    cols2[0].metric("Viable: prov. en conv.", "0")
    cols2[1].metric("Estudio: pend. contacto", "0")
    cols2[2].metric("Estudio: sin proveedor", "0")
    cols2[3].metric("Contactada no rentable", "0")
    cols2[4].metric("Justif. no rent. pend.", "0")

    st.caption(
        f"Captacion actual (DB): {top_ataque} fichas en 'atacar ya' y {prometedoras} en 'prometedor'. "
        "Estados de seguimiento/contacto se habilitan en la siguiente fase."
    )

def _render_tab_dashboard(ranked_df: pd.DataFrame, db_path: str) -> None:
    st.markdown("### Dashboard Ejecutivo")
    db_status = str(st.session_state.get("intel_db_status", "")).strip()
    risk_status = str(st.session_state.get("intel_risk_map_status", "")).strip()
    time_window_status = str(st.session_state.get("intel_time_window_status", "")).strip()
    if db_status:
        st.caption(f"Estado fuente: {db_status}")
    if risk_status:
        st.caption(risk_status)
    if time_window_status:
        st.caption(time_window_status)
    if not ranked_df.empty and "enlace_minsa" in ranked_df.columns:
        direct_links = (
            ranked_df["enlace_minsa"]
            .fillna("")
            .astype(str)
            .str.contains(r"/Utilities/LoadFicha/\?idficha=", case=False, regex=True)
            .sum()
        )
        st.caption(
            f"Enlaces MINSA: {int(direct_links):,} directos, "
            f"{int(len(ranked_df) - int(direct_links)):,} con fallback a consulta CTNI."
        )
    if ranked_df.empty:
        st.warning("No hay fichas detectadas en la base para construir el dashboard.")
        return

    st.info(
        f"Base utilizada: `{db_path}`. Captacion inicial por ficha detectada "
        "(incluye fichas con y sin asterisco, normalizadas a numero base)."
    )

    _placeholder_block(
        "Alertas y tareas del dia",
        "Aqui se mostraran alertas (vencimientos, fichas sin avance, contactos pendientes) y tareas recomendadas.",
        ["tipo_alerta", "ficha", "proveedor", "prioridad", "fecha_limite", "accion_sugerida"],
    )

    st.markdown("#### Top fichas por score (captacion)")
    dash_view = ranked_df.copy()
    dash_view["ver_ficha_minsa"] = dash_view["enlace_minsa"]
    dash_cols = [
        "ficha",
        "nombre_ficha",
        "ver_ficha_minsa",
        "score_total",
        "clasificacion",
        "actos",
        "actos_solo_ficha",
        "actos_con_otras_fichas",
        "monto_historico",
        "ganadores_distintos",
        "proponentes_promedio",
        "revision_proponentes",
        "clase_riesgo",
        "top1_ganador",
        "top1_pct_ganadas",
        "top2_ganador",
        "top2_pct_ganadas",
        "top3_ganador",
        "top3_pct_ganadas",
    ]
    show_dash_cols = [c for c in dash_cols if c in dash_view.columns]
    st.dataframe(
        dash_view[show_dash_cols].head(15),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ver_ficha_minsa": st.column_config.LinkColumn("Enlace MINSA", display_text="Ver ficha"),
            "revision_proponentes": st.column_config.CheckboxColumn(
                "Rev. prop. (dato)",
                help="Marcado cuando proponentes promedio es 0 y requiere revision de calidad de dato.",
            ),
        },
    )

def _render_tab_deteccion_ct(ficha_metrics_df: pd.DataFrame, ficha_acts_df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("### Deteccion automatica de fichas")
    sub1, sub2, sub3 = st.tabs(["Scoring", "Resultados", "Detalle ficha"])
    default_weights = _default_weights()

    weights = _ensure_default_weights_profile()

    if ficha_metrics_df.empty:
        with sub1:
            st.warning("No hay actos con ficha detectada en la base actual.")
        with sub2:
            st.info("Sin datos para ranking.")
        with sub3:
            st.info("Sin datos para detalle.")
        return pd.DataFrame()

    with sub1:
        st.markdown("#### Ajuste de pesos del score")
        st.caption(
            "Transformacion activa: winsorizacion p95 + log1p (actos/monto) + normalizacion min-max. "
            "Ganadores y proponentes se invierten (menos es mejor)."
        )
        c1, c2, c3 = st.columns(3)
        weights["actos"] = c1.slider(
            "Peso frecuencia (numero de actos)",
            0.0,
            100.0,
            float(weights["actos"]),
            1.0,
        )
        weights["monto"] = c1.slider(
            "Peso monto historico (dinero)",
            0.0,
            100.0,
            float(weights["monto"]),
            1.0,
        )
        weights["ganadores"] = c2.slider(
            "Peso ganadores distintos (competencia)",
            0.0,
            100.0,
            float(weights["ganadores"]),
            1.0,
        )
        weights["proponentes"] = c2.slider(
            "Peso proponentes por acto (competencia)",
            0.0,
            100.0,
            float(weights["proponentes"]),
            1.0,
        )
        weights["riesgo"] = c3.slider(
            "Peso clase de riesgo (complejidad)",
            0.0,
            100.0,
            float(weights["riesgo"]),
            1.0,
        )
        st.session_state["intel_weights"] = weights

        total_weights = sum(weights.values())
        st.caption(
            f"Suma de pesos: {total_weights:.1f}. "
            "El score se normaliza automaticamente con la suma actual."
        )

        b1, b2, b3, b4 = st.columns(4)
        if b1.button("Recalcular"):
            st.success("Scoring recalculado con los pesos actuales.")
        if b2.button("Restaurar default"):
            st.session_state["intel_weights"] = default_weights.copy()
            st.rerun()
        if b3.button("Normalizar a 100"):
            if total_weights > 0:
                factor = 100.0 / total_weights
                for key in list(weights.keys()):
                    weights[key] = round(float(weights[key]) * factor, 2)
                # ajuste fino para cerrar exactamente en 100
                diff = round(100.0 - sum(weights.values()), 2)
                weights["actos"] = round(weights["actos"] + diff, 2)
                st.session_state["intel_weights"] = weights
                st.rerun()
        b4.button("Guardar config (fase 2)", disabled=True)

    ranked_df = _score_fichas(ficha_metrics_df, weights)
    discarded = set(_ensure_discarded_state())
    if discarded:
        ranked_df = ranked_df[~ranked_df["ficha"].astype(str).isin(discarded)].reset_index(drop=True)

    with sub2:
        st.caption("Ranking completo de fichas detectadas en actos (ordenado por score).")
        st.caption(
            "Actos = todos los actos donde aparece la ficha. "
            "Actos_solo_ficha = actos donde solo aparece esa ficha. "
            "Actos_con_otras_fichas = actos donde comparte con otras fichas."
        )
        if discarded:
            st.caption(f"Fichas descartadas ocultas: {len(discarded)}")
        ranking_cols = [
            "N°",
            "ficha",
            "nombre_ficha",
            "ver_ficha_minsa",
            "score_total",
            "clasificacion",
            "actos",
            "actos_solo_ficha",
            "actos_con_otras_fichas",
            "monto_historico",
            "ganadores_distintos",
            "proponentes_promedio",
            "revision_proponentes",
            "clase_riesgo",
            "top1_ganador",
            "top1_pct_ganadas",
            "top2_ganador",
            "top2_pct_ganadas",
            "top3_ganador",
            "top3_pct_ganadas",
        ]
        view_df = ranked_df.sort_values(
            ["score_total", "actos", "monto_historico"],
            ascending=[False, False, False],
            kind="stable",
        ).reset_index(drop=True)
        view_df["N°"] = view_df.index + 1
        view_df["ver_ficha_minsa"] = view_df["enlace_minsa"]
        show_rank_cols = [c for c in ranking_cols if c in view_df.columns]
        st.dataframe(
            view_df[show_rank_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "ver_ficha_minsa": st.column_config.LinkColumn("Enlace MINSA", display_text="Ver ficha"),
                "revision_proponentes": st.column_config.CheckboxColumn(
                    "Rev. prop. (dato)",
                    help="Marcado cuando proponentes promedio es 0 y requiere revision de calidad de dato.",
                ),
            },
        )

        if view_df.empty:
            st.info("No hay fichas para mostrar (todas pueden estar descartadas).")
            if discarded and st.button("Restaurar fichas descartadas"):
                st.session_state["intel_fichas_descartadas"] = []
                st.rerun()
            return ranked_df

        st.markdown("#### Acciones sobre ficha seleccionada")
        ficha_opts = view_df["ficha"].astype(str).tolist()
        ficha_labels = _build_ficha_label_map(view_df, ficha_col="ficha", nombre_col="nombre_ficha")
        selected_action_ficha = st.selectbox(
            "Selecciona ficha",
            ficha_opts,
            index=0 if ficha_opts else None,
            key="intel_action_ficha",
            format_func=lambda x: ficha_labels.get(str(x), str(x)),
        )
        batch_input = st.text_input(
            "Fichas por lote (separadas por coma)",
            key="intel_action_ficha_batch",
            placeholder="Ej: 43358, 103169, 109726",
            help="Si completas este campo, se procesa el lote. Si queda vacio, usa la ficha seleccionada.",
        )
        a1, a2, a3 = st.columns(3)
        if a1.button("Ver actos de ficha", disabled=not bool(selected_action_ficha)):
            st.session_state["intel_selected_ficha"] = str(selected_action_ficha)
            st.rerun()
        if a2.button("Pasar ficha(s) a estudio", disabled=not bool(selected_action_ficha)):
            requested = _parse_ficha_batch_input(batch_input)
            if not requested:
                requested = [str(selected_action_ficha)]
            available = set(ranked_df["ficha"].astype(str).tolist())
            valid = [f for f in requested if f in available]
            invalid = [f for f in requested if f not in available]

            added = 0
            already = 0
            added_fichas: list[str] = []
            for ficha_req in valid:
                full_row = ranked_df[ranked_df["ficha"].astype(str) == str(ficha_req)].head(1)
                if full_row.empty:
                    continue
                if _add_ficha_to_study(full_row.iloc[0]):
                    added += 1
                    added_fichas.append(str(ficha_req))
                else:
                    already += 1

            if added:
                st.success(f"{added} ficha(s) enviadas a 'Fichas en seg.'.")
                auto_db_path = str(st.session_state.get("intel_db_path_cache", "") or "")
                try:
                    enqueue = _enqueue_auto_study_for_fichas(
                        added_fichas,
                        db_path=auto_db_path,
                        max_queries=int(st.session_state.get("intel_study_max_queries", INTEL_STUDY_DEFAULT_MAX_QUERIES)),
                        notes="Auto-estudio al pasar a seguimiento",
                    )
                    if int(enqueue.get("queued", 0)) > 0:
                        st.success(
                            f"Auto-estudio activado: {int(enqueue.get('queued', 0))} ficha(s) "
                            "encoladas en orquestador."
                        )
                    if int(enqueue.get("already", 0)) > 0:
                        st.info(
                            f"{int(enqueue.get('already', 0))} ficha(s) ya estaban con estado no pendiente "
                            "y no se reencolaron."
                        )
                    if enqueue.get("errors"):
                        st.warning("Auto-estudio con observaciones: " + "; ".join(enqueue.get("errors", [])))
                except Exception as exc:
                    st.warning(f"Fichas agregadas a seguimiento, pero no se pudo auto-encolar estudio: {exc}")
            if already:
                st.info(f"{already} ficha(s) ya estaban en seguimiento.")
            if invalid:
                st.warning("No encontradas en el ranking: " + ", ".join(invalid))
        if a3.button("Descartar ficha", disabled=not bool(selected_action_ficha)):
            discarded_now = _ensure_discarded_state()
            ficha_key = str(selected_action_ficha or "").strip()
            if ficha_key and ficha_key not in discarded_now:
                discarded_now.append(ficha_key)
                st.session_state["intel_fichas_descartadas"] = discarded_now
                st.success(f"Ficha {ficha_key} descartada. Ya no se mostrará en detección.")
                if str(st.session_state.get("intel_selected_ficha", "")) == ficha_key:
                    st.session_state["intel_selected_ficha"] = ""
                st.rerun()
            else:
                st.info("Esa ficha ya estaba descartada.")

        if discarded and st.button("Restaurar fichas descartadas"):
            st.session_state["intel_fichas_descartadas"] = []
            st.rerun()
        selected_name = (
            view_df.loc[view_df["ficha"].astype(str) == str(selected_action_ficha), "nombre_ficha"]
            .astype(str)
            .head(1)
            .tolist()
        )
        if selected_name and selected_name[0].strip():
            st.caption(f"Nombre de ficha: {selected_name[0]}")
        selected_link = (
            view_df.loc[view_df["ficha"].astype(str) == str(selected_action_ficha), "enlace_minsa"]
            .astype(str)
            .head(1)
            .tolist()
        )
        if selected_link and selected_link[0].strip():
            st.link_button("Ver ficha tecnica en MINSA", selected_link[0], use_container_width=False)

    with sub3:
        selected = st.session_state.get("intel_selected_ficha")
        if not selected:
            st.info("Selecciona una ficha en Resultados para ver su detalle.")
            return ranked_df

        row = ranked_df[ranked_df["ficha"].astype(str) == str(selected)]
        if row.empty:
            st.info("No hay detalle para la ficha seleccionada.")
            return ranked_df

        top_pos = 0
        top_matches = ranked_df.index[ranked_df["ficha"].astype(str) == str(selected)].tolist()
        if top_matches:
            top_pos = int(top_matches[0]) + 1

        row = row.iloc[0]
        nombre_ficha = str(row.get("nombre_ficha", "")).strip()
        detail_score = pd.DataFrame(
            [
                ["frecuencia", row["f_actos"], weights["actos"]],
                ["monto_historico", row["f_monto"], weights["monto"]],
                ["ganadores", row["f_ganadores"], weights["ganadores"]],
                ["proponentes_promedio", row["f_proponentes"], weights["proponentes"]],
                ["clase_riesgo", row["f_riesgo"], weights["riesgo"]],
            ],
            columns=["factor", "valor_norm", "peso"],
        )
        detail_score["contribucion"] = detail_score["valor_norm"] * detail_score["peso"]
        if nombre_ficha:
            st.markdown(
                f"#### Top #{top_pos} | Score de ficha {selected} - {nombre_ficha}: "
                f"{row['score_total']:.2f} ({row['clasificacion']})"
            )
        else:
            st.markdown(
                f"#### Top #{top_pos} | Score de ficha {selected}: "
                f"{row['score_total']:.2f} ({row['clasificacion']})"
            )
        st.caption(
            f"Actos totales: {int(row.get('actos', 0))} | "
            f"Solo esa ficha: {int(row.get('actos_solo_ficha', 0))} | "
            f"Con otras fichas: {int(row.get('actos_con_otras_fichas', 0))} | "
            f"Actos con 0 proponentes: {float(row.get('pct_actos_proponentes_cero', 0.0)):.2f}%"
        )
        link_minsa = str(row.get("enlace_minsa", "") or "").strip()
        if link_minsa:
            st.link_button("Abrir ficha tecnica en MINSA", link_minsa, use_container_width=False)
        st.caption(
            "Ganadores top: "
            f"1) {str(row.get('top1_ganador', '') or '-')} ({float(row.get('top1_pct_ganadas', 0.0)):.2f}%) | "
            f"2) {str(row.get('top2_ganador', '') or '-')} ({float(row.get('top2_pct_ganadas', 0.0)):.2f}%) | "
            f"3) {str(row.get('top3_ganador', '') or '-')} ({float(row.get('top3_pct_ganadas', 0.0)):.2f}%)"
        )
        st.dataframe(detail_score, use_container_width=True, hide_index=True)

        st.markdown("#### Actos asociados")
        acts = ficha_acts_df[ficha_acts_df["ficha"].astype(str) == str(selected)].copy()
        acts = acts.rename(
            columns={
                "ganador": "proveedor_ganador",
                "num_participantes": "participantes",
                "proponentes_en_acto": "proponentes_detectados",
            }
        )
        show_cols = [
            "id",
            "ficha_token",
            "nombre_ficha",
            "clase_riesgo",
            "fichas_en_acto",
            "acto_tipo_ficha",
            "titulo",
            "entidad",
            "fecha",
            "fecha_adjudicacion",
            "proveedor_ganador",
            "participantes",
            "proponentes_detectados",
            "monto_estimado",
            "enlace",
        ]
        proponent_cols = [c for c in acts.columns if c.startswith("Proponente ")]
        price_cols = [c for c in acts.columns if c.startswith("Precio Proponente ")]
        show_cols.extend(proponent_cols + price_cols)
        present_cols = [c for c in show_cols if c in acts.columns]
        column_config: dict[str, object] = {}
        if "enlace" in present_cols:
            column_config["enlace"] = st.column_config.LinkColumn("Enlace Panamá Compra", display_text="Ver acto")
        st.dataframe(
            acts[present_cols].head(500),
            use_container_width=True,
            hide_index=True,
            column_config=column_config,
        )

    return ranked_df

def _render_tab_seguimiento_ct() -> None:
    st.markdown("### Fichas en seg.")
    fichas_estudio = _ensure_study_state(sync_remote=True)
    tracking_status = str(st.session_state.get("intel_tracking_status", "") or "").strip()
    if tracking_status:
        st.caption(f"Persistencia seguimiento: {tracking_status}")
    if not fichas_estudio:
        st.info("Todavia no has enviado fichas a seguimiento desde 'Detecc. fichas'.")
        return

    df_seg = pd.DataFrame(fichas_estudio)
    show_cols = [
        "ficha",
        "nombre_ficha",
        "enlace_minsa",
        "clase_riesgo",
        "score_inicial",
        "clasificacion",
        "actos",
        "actos_solo_ficha",
        "actos_con_otras_fichas",
        "monto_historico",
        "proponentes_promedio",
        "revision_proponentes",
        "top1_ganador",
        "top1_pct_ganadas",
        "top2_ganador",
        "top2_pct_ganadas",
        "top3_ganador",
        "top3_pct_ganadas",
        "estado",
        "fecha_ingreso",
        "notas",
    ]
    cols = [c for c in show_cols if c in df_seg.columns]
    df_seg_view = df_seg.copy()
    if "enlace_minsa" in df_seg_view.columns:
        df_seg_view["ver_ficha_minsa"] = df_seg_view["enlace_minsa"]
        cols = ["ver_ficha_minsa" if c == "enlace_minsa" else c for c in cols]
    st.dataframe(
        df_seg_view[cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "ver_ficha_minsa": st.column_config.LinkColumn("Enlace MINSA", display_text="Ver ficha"),
            "revision_proponentes": st.column_config.CheckboxColumn(
                "Rev. prop. (dato)",
                help="Marcado cuando proponentes promedio es 0 y requiere revision de calidad de dato.",
            ),
        },
    )

    c1, c2, c3 = st.columns([1.4, 1.8, 1.0])
    ficha_target_opts = df_seg["ficha"].astype(str).tolist()
    ficha_target_labels = _build_ficha_label_map(df_seg, ficha_col="ficha", nombre_col="nombre_ficha")
    target = c1.selectbox(
        "Ficha a gestionar",
        ficha_target_opts,
        key="intel_seg_target",
        format_func=lambda x: ficha_target_labels.get(str(x), str(x)),
    )
    new_state = c2.selectbox(
        "Nuevo estado",
        [
            "pendiente de estudio profundo",
            "en estudio",
            "listo para busqueda de proveedores",
            "pausado",
            "descartado",
        ],
        key="intel_seg_state",
    )
    if c3.button("Actualizar estado"):
        now_iso = _utc_now_iso()
        for item in fichas_estudio:
            if str(item.get("ficha", "")) == str(target):
                item["estado"] = new_state
                item["updated_at"] = now_iso
        st.session_state["intel_fichas_estudio"] = _normalize_tracking_records(fichas_estudio)
        ok, msg = _persist_tracking_records(st.session_state["intel_fichas_estudio"])
        st.session_state["intel_tracking_status"] = msg
        st.session_state["intel_tracking_backend"] = "sheets+sqlite" if ok else "sqlite"
        st.success(f"Estado actualizado para ficha {target}.")
        if _is_pending_study_state(new_state):
            try:
                auto_db_path = str(st.session_state.get("intel_db_path_cache", "") or "")
                enqueue = _enqueue_auto_study_for_fichas(
                    [str(target)],
                    db_path=auto_db_path,
                    max_queries=int(st.session_state.get("intel_study_max_queries", INTEL_STUDY_DEFAULT_MAX_QUERIES)),
                    notes="Auto-estudio por cambio de estado a pendiente",
                )
                if int(enqueue.get("queued", 0)) > 0:
                    st.success("Auto-estudio encolado para la ficha actualizada.")
            except Exception as exc:
                st.warning(f"No se pudo auto-encolar estudio tras actualizar estado: {exc}")

    if st.button("Quitar ficha seleccionada"):
        st.session_state["intel_fichas_estudio"] = [
            x for x in fichas_estudio if str(x.get("ficha", "")) != str(target)
        ]
        ok, msg = _persist_tracking_records(st.session_state["intel_fichas_estudio"])
        st.session_state["intel_tracking_status"] = msg
        st.session_state["intel_tracking_backend"] = "sheets+sqlite" if ok else "sqlite"
        st.success(f"Ficha {target} removida de seguimiento.")
        st.rerun()


def _auto_enqueue_study_backlog_once(db_path: str) -> None:
    # 2026-04: Se desactiva auto-estudio masivo de backlog historico para
    # evitar picos de lectura de Sheets (429) al abrir la seccion.
    # Regla vigente:
    # - SI auto-estudio para fichas nuevas que se agregan/cambian a pendiente.
    # - NO auto-estudio inicial de todas las fichas ya existentes.
    if bool(st.session_state.get("intel_auto_study_bootstrapped", False)):
        return
    st.session_state["intel_auto_study_bootstrapped"] = True
    st.session_state["intel_auto_study_bootstrap_report"] = {}
    st.session_state["intel_auto_study_bootstrap_error"] = ""


def _render_tab_estudio_profundo(
    ficha_acts_df: pd.DataFrame,
    ranked_df: pd.DataFrame,
    db_path: str,
) -> None:
    st.markdown("### Estudio de fichas")
    _ensure_study_db()
    persist_status = _clean_text(st.session_state.get("intel_study_persist_status", ""))
    if persist_status:
        st.caption(f"Persistencia estudios: {persist_status}")

    _auto_enqueue_study_backlog_once(str(db_path or ""))
    st.caption(
        "Auto-estudio inicial masivo desactivado para evitar limites de lectura (429) en Sheets. "
        "Se mantiene auto-estudio solo para fichas nuevas agregadas a seguimiento."
    )
    bootstrap_report = st.session_state.get("intel_auto_study_bootstrap_report")
    if isinstance(bootstrap_report, dict):
        if int(bootstrap_report.get("queued", 0)) > 0:
            st.success(
                f"Auto-estudio inicial: {int(bootstrap_report.get('queued', 0))} ficha(s) encoladas "
                "automaticamente en orquestador."
            )
        if bootstrap_report.get("errors"):
            st.warning("Auto-estudio inicial con observaciones: " + "; ".join(bootstrap_report.get("errors", [])))
        st.session_state["intel_auto_study_bootstrap_report"] = {}
    bootstrap_error = _clean_text(st.session_state.get("intel_auto_study_bootstrap_error", ""))
    if bootstrap_error:
        st.warning(f"No se pudo completar auto-estudio inicial de seguimiento: {bootstrap_error}")
        st.session_state["intel_auto_study_bootstrap_error"] = ""

    tab_pend, tab_run, tab_cons, tab_done, tab_ver = st.tabs(
        [
            "Pendientes de estudio",
            "Ejecuci?n de estudio",
            "Consultas finales",
            "Fichas estudiadas",
            "Versiones y actualizaci?n",
        ]
    )

    with tab_pend:
        last_stats = st.session_state.get("intel_last_study_stats", None)
        if isinstance(last_stats, dict) and last_stats:
            st.caption(
                "Ultimo run - auto-resolucion: "
                f"precio={int(last_stats.get('auto_resolved_price', 0))}, "
                f"pais={int(last_stats.get('auto_resolved_country', 0))}, "
                f"marca/modelo={int(last_stats.get('auto_resolved_brand_model', 0))}, "
                f"tipo1={int(last_stats.get('tipo1_detectados', 0))}, "
                f"tipo2={int(last_stats.get('tipo2_detectados', 0))}, "
                f"consultas omitidas por limite={int(last_stats.get('queries_skipped_limit', 0))}."
            )
        seg = pd.DataFrame(_ensure_study_state(sync_remote=True))
        if seg.empty:
            st.info("No hay fichas en seguimiento. Env?alas desde Detecc. fichas.")
        else:
            st.caption("Selecciona una ficha en seguimiento para correr estudio hist?rico completo.")
            cols = [c for c in ["ficha", "nombre_ficha", "estado", "clasificacion", "score_inicial"] if c in seg.columns]
            st.dataframe(seg[cols], use_container_width=True, hide_index=True)

            ficha_opts = seg["ficha"].astype(str).tolist()
            ficha_labels = _build_ficha_label_map(seg, ficha_col="ficha", nombre_col="nombre_ficha")
            target_ficha = st.selectbox(
                "Ficha a estudiar",
                ficha_opts,
                key="intel_study_target_ficha",
                format_func=lambda x: ficha_labels.get(str(x), str(x)),
            )
            target_row = seg[seg["ficha"].astype(str) == str(target_ficha)].head(1)
            target_name = str(target_row.iloc[0].get("nombre_ficha", "") if not target_row.empty else "")
            copt1, copt2 = st.columns([1.2, 1.0])
            max_queries = int(
                copt1.slider(
                    "Max consultas finales por run",
                    min_value=1,
                    max_value=INTEL_STUDY_MAX_QUERIES_HARD,
                    value=INTEL_STUDY_DEFAULT_MAX_QUERIES,
                    step=1,
                    key="intel_study_max_queries",
                    help="Limite para minimizar revision manual en el estudio remoto.",
                )
            )
            use_browser_extractor = bool(
                copt2.checkbox(
                    label="Modo fijo: Selenium local visible",
                    value=True,
                    key="intel_study_use_browser_extractor_forced",
                    disabled=True,
                    help="El estudio se ejecuta via orquestador local con Selenium visible (sin headless).",
                )
            )
            catalog_status = str(st.session_state.get("intel_catalogo_status", "") or "").strip()
            if catalog_status:
                st.caption(catalog_status)
            run_notes = st.text_area("Notas del estudio (opcional)", key="intel_study_notes", height=80)
            batch_study_input = st.text_input(
                "Fichas a estudiar por lote (coma separada)",
                key="intel_study_batch_fichas",
                placeholder="Ej: 43358, 103169, 109726",
                help=(
                    "Si completas este campo, se encolan varias fichas en orden. "
                    "El orquestador las ejecuta secuencialmente."
                ),
            )
            if st.button("Iniciar estudio de ficha(s)", type="primary"):
                try:
                    from sheets import get_client

                    client, _ = get_client()
                    _ensure_intel_orquestador_job(client)

                    requested = _parse_ficha_batch_input(batch_study_input)
                    if not requested:
                        requested = [str(target_ficha)]
                    valid_set = set(seg["ficha"].astype(str).tolist())
                    valid_fichas = [f for f in requested if f in valid_set]
                    invalid_fichas = [f for f in requested if f not in valid_set]

                    if not valid_fichas:
                        st.error("No hay fichas validas para encolar.")
                        return

                    seg_name_map = {
                        str(r.get("ficha", "")): _clean_text(r.get("nombre_ficha", ""))
                        for r in seg.to_dict(orient="records")
                    }

                    st.session_state["intel_study_request_batch_ids"] = []
                    st.session_state["intel_study_request_meta_map"] = {}
                    st.session_state["intel_study_request_id"] = ""
                    st.session_state["intel_study_request_ficha"] = ""
                    st.session_state["intel_study_request_ficha_name"] = ""
                    st.session_state["intel_study_request_notes"] = ""
                    st.session_state.pop("intel_study_request_synced_id", None)
                    st.session_state["intel_manual_last_req_row"] = None
                    st.session_state["intel_manual_last_poll_ts"] = 0.0
                    st.session_state["intel_manual_poll_cooldown_until"] = 0.0

                    request_ids: list[str] = []
                    for ficha_req in valid_fichas:
                        req_name = _clean_text(seg_name_map.get(str(ficha_req), "")) or target_name
                        request_id = _append_intel_manual_request(
                            client,
                            {
                                "ficha": str(ficha_req),
                                "nombre_ficha": req_name,
                                "db_path": str(db_path or ""),
                                "max_queries": int(max_queries),
                                "notes": _clean_text(run_notes),
                                "headless": False,
                            },
                        )
                        request_ids.append(request_id)
                        _register_study_request_session(
                            request_id=request_id,
                            ficha=str(ficha_req),
                            nombre_ficha=req_name,
                            notes=_clean_text(run_notes),
                            set_current_if_empty=True,
                        )

                    first_id = request_ids[0]
                    if len(request_ids) == 1:
                        st.success(
                            f"Solicitud enviada al orquestador (request_id={first_id[:10]}...). "
                            "Se ejecutara en tu PC con Selenium visible."
                        )
                    else:
                        st.success(
                            f"Se encolaron {len(request_ids)} fichas para estudio secuencial "
                            f"(primer request_id={first_id[:10]}...)."
                        )
                    if invalid_fichas:
                        st.warning("No encontradas en seguimiento: " + ", ".join(invalid_fichas))
                except Exception as exc:
                    st.error(f"No se pudo enviar solicitud al orquestador: {exc}")

            current_request_id = _clean_text(st.session_state.get("intel_study_request_id", ""))
            if current_request_id:
                now_ts = time.time()
                poll_interval_s = 10.0
                quota_cooldown_s = 65.0
                last_poll_ts = float(st.session_state.get("intel_manual_last_poll_ts", 0.0) or 0.0)
                cooldown_until = float(st.session_state.get("intel_manual_poll_cooldown_until", 0.0) or 0.0)
                req_row = None
                used_cached_status = False

                try:
                    if cooldown_until and now_ts < cooldown_until:
                        req_row = st.session_state.get("intel_manual_last_req_row")
                        used_cached_status = req_row is not None
                    elif last_poll_ts and (now_ts - last_poll_ts) < poll_interval_s:
                        req_row = st.session_state.get("intel_manual_last_req_row")
                        used_cached_status = req_row is not None
                    else:
                        from sheets import get_client

                        client_req, _ = get_client()
                        req_row = _fetch_intel_manual_request(client_req, current_request_id)
                        st.session_state["intel_manual_last_poll_ts"] = now_ts
                        if req_row is not None:
                            st.session_state["intel_manual_last_req_row"] = req_row
                        st.session_state["intel_manual_poll_cooldown_until"] = 0.0
                except Exception as exc:
                    if _is_sheets_quota_error(exc):
                        cooldown_until = now_ts + quota_cooldown_s
                        st.session_state["intel_manual_poll_cooldown_until"] = cooldown_until
                        req_row = st.session_state.get("intel_manual_last_req_row")
                        used_cached_status = req_row is not None
                        wait_s = int(max(1, round(cooldown_until - now_ts)))
                        st.warning(
                            "Google Sheets reporto limite de lectura (429). "
                            f"Usando ultimo estado disponible y reintentando en {wait_s}s."
                        )
                    else:
                        req_row = st.session_state.get("intel_manual_last_req_row")
                        used_cached_status = req_row is not None
                        if used_cached_status:
                            st.warning(
                                "No se pudo consultar estado en este intento; "
                                "mostrando ultimo estado disponible."
                            )
                        else:
                            st.error(f"No se pudo consultar estado de solicitud: {exc}")

                if req_row:
                    status = _clean_text(req_row.get("status", "")).lower()
                    note = _clean_text(req_row.get("notes", ""))
                    progress_value = {
                        "pending": 0.15,
                        "enqueued": 0.35,
                        "running": 0.75,
                        "done": 1.0,
                        "error": 1.0,
                    }.get(status, 0.1)
                    progress_text = {
                        "pending": "Solicitud recibida",
                        "enqueued": "En cola de ejecucion",
                        "running": "Scraping local en ejecucion",
                        "done": "Run completado",
                        "error": "Run con error",
                    }.get(status, "Procesando...")
                    st.progress(progress_value, text=f"Request {current_request_id[:10]}... | {progress_text}")
                    st.caption(f"Estado actual: `{status or 'desconocido'}`")
                    if used_cached_status:
                        st.caption("Estado mostrado desde cache temporal por control de cuota.")
                    if note:
                        st.caption(note)
                    req_error = _clean_text(req_row.get("result_error", ""))
                    if req_error:
                        st.error(req_error)

                    sync_now = st.button("Sincronizar resultado remoto", key="intel_sync_remote_now")
                    st.caption("Auto-actualizacion activa cada 5 segundos mientras el run este en proceso.")
                    batch_ids_raw = st.session_state.get("intel_study_request_batch_ids", [])
                    batch_ids = [str(x).strip() for x in (batch_ids_raw if isinstance(batch_ids_raw, list) else []) if str(x).strip()]
                    if batch_ids:
                        st.caption(f"Cola activa: {len(batch_ids)} solicitud(es).")

                    synced_id = _clean_text(st.session_state.get("intel_study_request_synced_id", ""))
                    if sync_now or (status == "done" and synced_id != current_request_id):
                        ok, msg, run_id = _sync_remote_run_to_local(
                            request_id=current_request_id,
                            fallback_ficha=_clean_text(st.session_state.get("intel_study_request_ficha", "")),
                            fallback_nombre=_clean_text(st.session_state.get("intel_study_request_ficha_name", "")),
                            db_source=str(db_path or ""),
                            notes=_clean_text(st.session_state.get("intel_study_request_notes", "")),
                        )
                        if ok:
                            st.session_state["intel_study_request_synced_id"] = current_request_id
                            st.session_state["intel_selected_run_id"] = run_id
                            ficha_done = _clean_text(st.session_state.get("intel_study_request_ficha", ""))
                            if ficha_done:
                                _update_tracking_state(
                                    ficha_done,
                                    "listo para busqueda de proveedores",
                                    note_suffix=f"Estudio completado run_id={run_id}",
                                )
                            st.success(msg + " Estado de seguimiento actualizado.")
                            next_id = _advance_study_request_queue(current_request_id)
                            if next_id:
                                st.info(f"Continuando automaticamente con siguiente request_id={next_id[:10]}...")
                            st.rerun()
                        else:
                            st.warning(msg)

                    if status == "error":
                        ficha_err = _clean_text(st.session_state.get("intel_study_request_ficha", ""))
                        if ficha_err:
                            _update_tracking_state(
                                ficha_err,
                                "pendiente de estudio profundo",
                                note_suffix=f"Error en request_id={current_request_id}",
                            )
                        next_id = _advance_study_request_queue(current_request_id)
                        if next_id:
                            st.warning(
                                f"Request con error ({current_request_id[:10]}...). "
                                f"Se continua con {next_id[:10]}..."
                            )
                            st.rerun()

                    auto_refresh = status in {"pending", "enqueued", "running"}
                    if auto_refresh:
                        time.sleep(5)
                        st.rerun()

    runs_df = _load_runs_df()

    with tab_run:
        if runs_df.empty:
            st.info("A?n no hay corridas de estudio.")
        else:
            st.caption("Corridas registradas (persistentes).")
            st.dataframe(runs_df, use_container_width=True, hide_index=True)
            run_options = runs_df["run_id"].astype(str).tolist()
            default_run = str(st.session_state.get("intel_selected_run_id", "") or "")
            default_idx = run_options.index(default_run) if default_run in run_options else 0
            selected_run = st.selectbox("Run a visualizar", run_options, index=default_idx, key="intel_study_run_view")
            st.session_state["intel_selected_run_id"] = selected_run
            detail_df = _load_run_detail_df(selected_run)
            if detail_df.empty:
                st.warning("Sin detalle para este run.")
            else:
                desierto_count = int(
                    (
                        detail_df.get("estado_revision", pd.Series(dtype=str)).astype(str).str.lower().str.contains("desierto")
                        | detail_df.get("precio_unitario_participacion", pd.Series(dtype=str)).astype(str).str.lower().str.contains("desierto")
                        | detail_df.get("fecha_orden_compra", pd.Series(dtype=str)).astype(str).str.lower().str.contains("desierto")
                    ).sum()
                )
                st.caption(
                    f"Detalle cargado: {len(detail_df)} renglones | "
                    f"Registros marcados como desierto: {desierto_count}"
                )
                show_cols = [
                    "ficha",
                    "acto_id",
                    "acto_nombre",
                    "acto_url",
                    "enlace_evidencia",
                    "entidad",
                    "tipo_flujo",
                    "fuente_precio",
                    "fuente_fecha",
                    "proveedor",
                    "proveedor_ganador",
                    "marca",
                    "modelo",
                    "pais_origen",
                    "cantidad",
                    "unidad_medida",
                    "precio_unitario_participacion",
                    "precio_unitario_referencia",
                    "fecha_publicacion",
                    "fecha_celebracion",
                    "fecha_adjudicacion",
                    "fecha_orden_compra",
                    "dias_acto_a_oc",
                    "tiempo_entrega_dias",
                    "dias_acto_a_oc_mas_entrega",
                    "nivel_certeza",
                    "requiere_revision",
                    "estado_revision",
                    "observaciones",
                ]
                present = [c for c in show_cols if c in detail_df.columns]
                col_cfg = {}
                if "acto_url" in present:
                    col_cfg["acto_url"] = st.column_config.LinkColumn("Enlace acto", display_text="Abrir acto")
                if "enlace_evidencia" in present:
                    col_cfg["enlace_evidencia"] = st.column_config.LinkColumn("Enlace evidencia", display_text="Abrir")
                if "requiere_revision" in present:
                    col_cfg["requiere_revision"] = st.column_config.CheckboxColumn("Rev.")
                st.dataframe(detail_df[present], use_container_width=True, hide_index=True, column_config=col_cfg)

                st.markdown("#### Gr?fica: todos los precios unitarios de participaci?n")
                price_df = detail_df.copy()
                price_df["precio_unitario_participacion"] = pd.to_numeric(
                    price_df["precio_unitario_participacion"], errors="coerce"
                ).fillna(0.0)
                price_df = price_df[price_df["precio_unitario_participacion"] > 0].copy()
                if price_df.empty:
                    st.info("No hay precios unitarios positivos para graficar.")
                else:
                    price_df["label_barra"] = (
                        price_df["proveedor"].fillna("").astype(str).str.slice(0, 35)
                        + " | Acto "
                        + price_df["acto_id"].fillna("").astype(str)
                    )
                    st.bar_chart(
                        price_df.set_index("label_barra")["precio_unitario_participacion"],
                        use_container_width=True,
                    )
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Precio unitario m?nimo", f"${price_df['precio_unitario_participacion'].min():,.2f}")
                    m2.metric("Precio unitario promedio", f"${price_df['precio_unitario_participacion'].mean():,.2f}")
                    m3.metric("Precio unitario m?ximo", f"${price_df['precio_unitario_participacion'].max():,.2f}")

                    agg = (
                        price_df.groupby("proveedor", dropna=False)
                        .agg(
                            precio_min=("precio_unitario_participacion", "min"),
                            precio_prom=("precio_unitario_participacion", "mean"),
                            precio_max=("precio_unitario_participacion", "max"),
                        )
                        .reset_index()
                        .sort_values("precio_prom", ascending=True)
                    )
                    min_date = (
                        price_df.sort_values("precio_unitario_participacion", ascending=True)
                        .drop_duplicates(subset=["proveedor"], keep="first")[["proveedor", "fecha_adjudicacion", "fecha_publicacion"]]
                        .rename(columns={"fecha_adjudicacion": "fecha_precio_min_adj", "fecha_publicacion": "fecha_precio_min_pub"})
                    )
                    max_date = (
                        price_df.sort_values("precio_unitario_participacion", ascending=False)
                        .drop_duplicates(subset=["proveedor"], keep="first")[["proveedor", "fecha_adjudicacion", "fecha_publicacion"]]
                        .rename(columns={"fecha_adjudicacion": "fecha_precio_max_adj", "fecha_publicacion": "fecha_precio_max_pub"})
                    )
                    agg = agg.merge(min_date, on="proveedor", how="left").merge(max_date, on="proveedor", how="left")
                    st.markdown("#### Resumen por proveedor (min / prom / max + fechas)")
                    st.dataframe(agg, use_container_width=True, hide_index=True)

    with tab_cons:
        st.markdown("#### Bitácora de seguimiento con proveedores")
        st.caption(
            "Usa este espacio para guardar hallazgos posteriores al contacto con proveedores. "
            "Cada comentario queda persistido en Sheets y asociado a la ficha."
        )
        ap_ctx_df = _load_ap_context_df()
        if ap_ctx_df.empty:
            st.info("Aún no hay fichas estudiadas preparadas para asociar comentarios.")
        else:
            ap_ctx_df = ap_ctx_df.copy()
            ap_ctx_df["ficha_label"] = ap_ctx_df.apply(
                lambda r: f"{_clean_text(r.get('ficha', ''))} - {_clean_text(r.get('nombre_ficha', ''))}".strip(" -"),
                axis=1,
            )
            ap_ctx_df = ap_ctx_df.sort_values(["updated_at", "ficha"], ascending=[False, True], kind="stable")
            ap_comment_options = ap_ctx_df["ficha"].astype(str).tolist()
            ap_comment_map = dict(zip(ap_ctx_df["ficha"].astype(str), ap_ctx_df["ficha_label"].astype(str)))
            selected_comment_ficha = st.selectbox(
                "Ficha para comentarios",
                ap_comment_options,
                key="intel_ap_comment_ficha",
                format_func=lambda x: ap_comment_map.get(str(x), str(x)),
            )
            selected_ctx_row = ap_ctx_df[ap_ctx_df["ficha"].astype(str) == str(selected_comment_ficha)].head(1)
            if not selected_ctx_row.empty:
                ctx_row = selected_ctx_row.iloc[0]
                st.caption(
                    f"Estado análisis: `{_clean_text(ctx_row.get('estado_analisis', '')) or '-'}` | "
                    f"Análisis activo: `{_clean_text(ctx_row.get('analisis_id_activo', ''))[:8] or '-'}`"
                )

            comments_df = _load_ap_comments_df()
            comments_view = comments_df[comments_df["ficha"].astype(str) == str(selected_comment_ficha)].copy() if not comments_df.empty else pd.DataFrame()

            comment_input_key = f"intel_ap_comment_input_{selected_comment_ficha}"
            new_comment = st.text_area(
                "Agregar comentario",
                key=comment_input_key,
                height=120,
                placeholder="Ej: Proveedor confirmó cumplimiento parcial, pidió ficha adicional, cotización pendiente, contacto por WhatsApp, etc.",
            )
            if st.button("Guardar comentario", key=f"intel_ap_comment_save_{selected_comment_ficha}"):
                ok, msg = _save_ap_comment(selected_comment_ficha, new_comment)
                if ok:
                    st.session_state[comment_input_key] = ""
                    st.success(msg)
                    st.rerun()
                else:
                    st.warning(msg)

            if comments_view.empty:
                st.info("Aún no hay comentarios guardados para esta ficha.")
            else:
                comments_view = comments_view.rename(
                    columns={
                        "created_at": "fecha",
                        "usuario": "usuario",
                        "comentario": "comentario",
                        "analisis_id": "analisis_id",
                    }
                )
                show_comment_cols = [c for c in ["fecha", "usuario", "analisis_id", "comentario"] if c in comments_view.columns]
                st.dataframe(
                    comments_view[show_comment_cols],
                    use_container_width=True,
                    hide_index=True,
                )

        st.markdown("---")
        st.markdown("#### Resolución de consultas del estudio")
        if runs_df.empty:
            st.info("Sin runs para consultas.")
        else:
            pending_runs = runs_df[runs_df["estado_run"].astype(str) == RUN_STATUS_PENDING].copy()
            if pending_runs.empty:
                st.success("No hay consultas finales pendientes.")
            else:
                run_options = pending_runs["run_id"].astype(str).tolist()
                selected_run = st.selectbox("Run pendiente", run_options, key="intel_pending_run")
                queries_df = _load_run_queries_df(selected_run)
                if queries_df.empty:
                    st.info("Este run no tiene consultas registradas.")
                else:
                    st.caption("Resuelve solo las ambig?edades reales detectadas al final del proceso.")
                    pending_df = queries_df[queries_df["estado"].astype(str) != "resuelta"].copy()
                    resolved_df = queries_df[queries_df["estado"].astype(str) == "resuelta"].copy()
                    st.write(f"Pendientes: {len(pending_df)} | Resueltas: {len(resolved_df)}")

                    responses = []
                    for i, row in pending_df.reset_index(drop=True).iterrows():
                        consulta_id = str(row.get("consulta_id", ""))
                        with st.expander(
                            f"Consulta {i+1} | Acto {row.get('acto_id', '')} | Campo: {row.get('campo_dudoso', '')}",
                            expanded=(i < 3),
                        ):
                            st.caption(f"Evidencia: {row.get('evidencia', '')}")
                            options_payload = []
                            try:
                                options_payload = json.loads(str(row.get("opciones_json", "[]") or "[]"))
                            except Exception:
                                options_payload = []
                            option_labels = [str(x.get("label", "")) for x in options_payload if str(x.get("label", "")).strip()]
                            label_to_value = {str(x.get("label", "")): str(x.get("value", "")) for x in options_payload}
                            selected_label = st.selectbox(
                                f"Opci?n sugerida ({i+1})",
                                [""] + option_labels,
                                key=f"intel_query_opt_{consulta_id}",
                            )
                            manual_value = st.text_input(
                                f"Valor manual ({i+1})",
                                key=f"intel_query_manual_{consulta_id}",
                                placeholder="Opcional, solo si ninguna opci?n aplica",
                            )
                            responses.append(
                                {
                                    "consulta_id": consulta_id,
                                    "respuesta_seleccionada": label_to_value.get(selected_label, ""),
                                    "valor_manual": manual_value.strip(),
                                }
                            )

                    csave, cfinal = st.columns(2)
                    if csave.button("Guardar progreso"):
                        _apply_query_resolution(selected_run, responses)
                        st.success("Progreso guardado.")
                        st.rerun()
                    if cfinal.button("Resolver y completar run"):
                        _apply_query_resolution(selected_run, responses)
                        st.success("Consultas procesadas. Se recalcul? resumen y estado del estudio.")
                        st.rerun()

    with tab_done:
        resumen_df = _load_resumen_estudiadas_df()
        if resumen_df.empty:
            st.info("No hay fichas estudiadas todav?a.")
        else:
            resumen_view = resumen_df.copy()
            if "ficha" in resumen_view.columns:
                def _fmt_ficha_row(row: pd.Series) -> str:
                    ficha_val = _clean_text(row.get("ficha", ""))
                    nombre_val = _clean_text(row.get("nombre_ficha", ""))
                    return f"{ficha_val} - {nombre_val}" if ficha_val and nombre_val else ficha_val

                resumen_view["ficha"] = resumen_view.apply(_fmt_ficha_row, axis=1)
            st.dataframe(resumen_view, use_container_width=True, hide_index=True)
            ficha_opts = resumen_df["ficha"].astype(str).tolist()
            ficha_labels = _build_ficha_label_map(resumen_df, ficha_col="ficha", nombre_col="nombre_ficha")
            selected_ficha = st.selectbox(
                "Ficha estudiada",
                ficha_opts,
                key="intel_done_ficha",
                format_func=lambda x: ficha_labels.get(str(x), str(x)),
            )
            row = resumen_df[resumen_df["ficha"].astype(str) == str(selected_ficha)].head(1)
            if not row.empty:
                row = row.iloc[0]
                st.markdown(f"**Resumen IA:** {row.get('resumen_ia', '')}")
                st.caption(
                    f"Total actos: {int(_safe_int(row.get('total_actos', 0)))} | "
                    f"Total renglones: {int(_safe_int(row.get('total_renglones', 0)))} | "
                    f"Empresas ganadoras: {int(_safe_int(row.get('empresas_ganadoras', 0)))}"
                )
                st.caption(
                    f"Precio participaci?n min/prom/max: "
                    f"${_safe_float(row.get('precio_participacion_min', 0.0)):,.2f} / "
                    f"${_safe_float(row.get('precio_participacion_prom', 0.0)):,.2f} / "
                    f"${_safe_float(row.get('precio_participacion_max', 0.0)):,.2f}"
                )
                run_id = str(row.get("run_id_vigente", "") or "")
                if run_id:
                    detail_df = _load_run_detail_df(run_id)
                    if not detail_df.empty:
                        st.markdown("#### Detalle persistido acto/rengl?n")
                        st.dataframe(detail_df, use_container_width=True, hide_index=True)

    with tab_ver:
        if runs_df.empty:
            st.info("Sin runs para versionado.")
        else:
            st.caption("Puedes re-ejecutar una ficha; se reemplaza por la nueva corrida vigente.")
            st.dataframe(runs_df, use_container_width=True, hide_index=True)
            ficha_opts = sorted(runs_df["ficha"].astype(str).unique().tolist())
            ficha_labels = _build_ficha_label_map(runs_df, ficha_col="ficha", nombre_col="nombre_ficha")
            chosen = st.selectbox(
                "Ficha para reestudio",
                ficha_opts,
                key="intel_restudy_ficha",
                format_func=lambda x: ficha_labels.get(str(x), str(x)),
            )
            if st.button("Reestudiar ficha seleccionada"):
                ref_row = ranked_df[ranked_df["ficha"].astype(str) == str(chosen)].head(1)
                ficha_name = str(ref_row.iloc[0].get("nombre_ficha", "") if not ref_row.empty else "")
                try:
                    from sheets import get_client

                    client, _ = get_client()
                    _ensure_intel_orquestador_job(client)
                    request_id = _append_intel_manual_request(
                        client,
                        {
                            "ficha": str(chosen),
                            "nombre_ficha": ficha_name,
                            "db_path": str(db_path or ""),
                            "max_queries": int(
                                st.session_state.get("intel_study_max_queries", INTEL_STUDY_DEFAULT_MAX_QUERIES)
                            ),
                            "notes": "Reestudio manual",
                            "headless": False,
                        },
                    )
                    _register_study_request_session(
                        request_id=request_id,
                        ficha=str(chosen),
                        nombre_ficha=ficha_name,
                        notes="Reestudio manual",
                        set_current_if_empty=True,
                    )
                    st.success(
                        f"Reestudio encolado en orquestador (request_id={request_id[:10]}...). "
                        "Sincroniza cuando el estado cambie a done."
                    )
                except Exception as exc:
                    st.error(f"No se pudo enviar reestudio al orquestador: {exc}")

def _render_tab_analisis_proveedores(ranked_df: pd.DataFrame) -> None:
    st.markdown("### ANALISIS_DE_PROVEEDORES")
    _ensure_study_db()

    resumen_df = _load_resumen_estudiadas_df()
    if resumen_df.empty:
        st.info("A?n no hay fichas estudiadas. Completa primero la etapa de estudio de fichas.")
        return

    created_ctx, updated_ctx = _ensure_provider_analysis_contexts(resumen_df, ranked_df)
    if created_ctx or updated_ctx:
        st.caption(
            f"Preparaci?n autom?tica ejecutada: contextos creados={created_ctx}, actualizados={updated_ctx}."
        )

    ctx_df = _load_ap_context_df()
    active_df = _load_ap_active_versions_df()
    versions_df = _load_ap_versions_df()
    if ctx_df.empty:
        st.info("No se pudo preparar contexto para an?lisis de proveedores.")
        return

    tab_pending, tab_loaded, tab_versions = st.tabs(
        [
            "Pendiente JSON proveedores",
            "An?lisis proveedores cargado",
            "Versiones y actualizaci?n",
        ]
    )

    with tab_pending:
        pending_states = {AP_STATE_STUDIED_NO_ANALYSIS, AP_STATE_PENDING_JSON}
        pend_df = ctx_df[
            ctx_df.get("estado_analisis", pd.Series(dtype=str)).astype(str).isin(pending_states)
        ].copy()
        if pend_df.empty:
            st.success("No hay fichas pendientes. Todas las fichas estudiadas tienen an?lisis de proveedores cargado.")
        else:
            pend_df = pend_df.sort_values(["updated_at", "ficha"], ascending=[False, True], kind="stable")
            pend_df["ficha_label"] = pend_df.apply(
                lambda r: f"{_clean_text(r.get('ficha', ''))} - {_clean_text(r.get('nombre_ficha', ''))}".strip(" -"),
                axis=1,
            )
            st.dataframe(
                pend_df[
                    [
                        "ficha",
                        "nombre_ficha",
                        "estado_analisis",
                        "fecha_contexto_generado",
                        "updated_at",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
            options = pend_df["ficha"].astype(str).tolist()
            label_map = dict(zip(pend_df["ficha"].astype(str), pend_df["ficha_label"].astype(str)))
            selected_ficha = st.selectbox(
                "Ficha pendiente",
                options,
                key="ap_pending_ficha",
                format_func=lambda x: label_map.get(str(x), str(x)),
            )
            selected_row = pend_df[pend_df["ficha"].astype(str) == str(selected_ficha)].head(1)
            if selected_row.empty:
                st.info("Selecciona una ficha pendiente.")
            else:
                row = selected_row.iloc[0]
                st.caption(
                    f"Estado: `{_clean_text(row.get('estado_analisis', ''))}` | "
                    f"Contexto generado: {_clean_text(row.get('fecha_contexto_generado', '')) or '-'}"
                )
                if _clean_text(row.get("analisis_id_activo", "")):
                    st.caption(
                        "Esta ficha tiene un an?lisis previo activo. "
                        "El nuevo JSON lo reemplazar? como versi?n activa."
                    )

                st.markdown("#### Prompt listo para ChatGPT")
                prompt_value = _clean_text(row.get("prompt_texto", ""))
                st.text_area(
                    "Prompt generado",
                    value=prompt_value,
                    key=f"ap_prompt_{selected_ficha}",
                    height=280,
                )
                prompt_js = json.dumps(prompt_value, ensure_ascii=False)
                components.html(
                    f"""
                    <div style="display:flex; align-items:center; gap:8px; margin:2px 0 8px 0;">
                      <button
                        style="background:#00a99d;color:white;border:none;border-radius:8px;padding:8px 14px;cursor:pointer;font-weight:600;"
                        onclick='navigator.clipboard.writeText({prompt_js}).then(() => {{
                          const el = document.getElementById("copy_status");
                          if (el) {{ el.textContent = "Prompt copiado."; }}
                        }}).catch(() => {{
                          const el = document.getElementById("copy_status");
                          if (el) {{ el.textContent = "No se pudo copiar automáticamente."; }}
                        }});'
                      >
                        Copiar prompt
                      </button>
                      <span id="copy_status" style="font-size:12px;color:#8fb9ff;"></span>
                    </div>
                    """,
                    height=44,
                )

                json_input = st.text_area(
                    "Pega aqu? el JSON devuelto por ChatGPT",
                    key=f"ap_json_input_{selected_ficha}",
                    height=260,
                )
                validate_key = f"ap_valid_payload_{selected_ficha}"
                err_key = f"ap_valid_errors_{selected_ficha}"
                warn_key = f"ap_valid_warnings_{selected_ficha}"

                c1, c2 = st.columns([1, 1])
                if c1.button("Validar JSON", key=f"ap_validate_{selected_ficha}"):
                    payload, errors, warnings = _validate_provider_analysis_json(json_input, selected_ficha)
                    st.session_state[validate_key] = payload if not errors else {}
                    st.session_state[err_key] = errors
                    st.session_state[warn_key] = warnings

                errors = st.session_state.get(err_key, [])
                warnings = st.session_state.get(warn_key, [])
                for w in warnings:
                    st.warning(w)
                for e in errors:
                    st.error(e)
                payload_ok = st.session_state.get(validate_key, {})
                can_save = isinstance(payload_ok, dict) and bool(payload_ok) and not errors

                if c2.button("Guardar an?lisis", key=f"ap_save_{selected_ficha}", disabled=not can_save):
                    analisis_id, version_num, new_state = _save_provider_analysis_payload(selected_ficha, payload_ok)
                    st.success(
                        f"An?lisis guardado. analisis_id={analisis_id[:8]}..., "
                        f"versi?n={version_num}, estado={new_state}."
                    )
                    st.rerun()

    with tab_loaded:
        loaded_df = ctx_df[
            ctx_df.get("estado_analisis", pd.Series(dtype=str)).astype(str).isin({AP_STATE_COMPLETED, AP_STATE_UPDATED})
        ].copy()
        loaded_df = loaded_df[
            loaded_df.get("analisis_id_activo", pd.Series(dtype=str)).astype(str).str.strip() != ""
        ].copy()
        if loaded_df.empty:
            st.info("A?n no hay an?lisis de proveedores cargados.")
        else:
            loaded_df["ficha_label"] = loaded_df.apply(
                lambda r: f"{_clean_text(r.get('ficha', ''))} - {_clean_text(r.get('nombre_ficha', ''))}".strip(" -"),
                axis=1,
            )
            options = loaded_df["ficha"].astype(str).tolist()
            label_map = dict(zip(loaded_df["ficha"].astype(str), loaded_df["ficha_label"].astype(str)))
            selected_ficha = st.selectbox(
                "Ficha con an?lisis",
                options,
                key="ap_loaded_ficha",
                format_func=lambda x: label_map.get(str(x), str(x)),
            )
            row = loaded_df[loaded_df["ficha"].astype(str) == str(selected_ficha)].head(1).iloc[0]
            analisis_id = _clean_text(row.get("analisis_id_activo", ""))

            vrow = active_df[active_df["ficha"].astype(str) == str(selected_ficha)].head(1)
            if not vrow.empty:
                v = vrow.iloc[0]
                st.caption(
                    f"Fecha de carga: {_clean_text(v.get('fecha_carga', '')) or '-'} | "
                    f"?ltima actualizaci?n: {_clean_text(v.get('fecha_ultima_actualizacion', '')) or '-'} | "
                    f"Versi?n: {int(_safe_int(v.get('version_num', 1)))}"
                )
                st.markdown("#### Resumen ejecutivo")
                st.info(_clean_text(v.get("resumen_ejecutivo", "")) or "Sin resumen ejecutivo.")

            hist_df = _load_ap_table_by_analisis("analisis_proveedores_hist_panama", analisis_id)
            gama_df = _load_ap_table_by_analisis("analisis_proveedores_mejor_gama", analisis_id)
            precio_df = _load_ap_table_by_analisis("analisis_proveedores_mejor_precio", analisis_id)

            st.markdown("#### 1) Proponentes hist?ricos en Panam?")
            show_hist = [c for c in AP_HIST_COLUMNS if c in hist_df.columns]
            st.dataframe(
                hist_df[show_hist] if show_hist else _empty_table(AP_HIST_COLUMNS),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("#### 2) Mejores por gama")
            show_gama = [c for c in AP_GAMA_COLUMNS if c in gama_df.columns]
            st.dataframe(
                gama_df[show_gama] if show_gama else _empty_table(AP_GAMA_COLUMNS),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("#### 3) Mejores por precio")
            show_precio = [c for c in AP_PRECIO_COLUMNS if c in precio_df.columns]
            st.dataframe(
                precio_df[show_precio] if show_precio else _empty_table(AP_PRECIO_COLUMNS),
                use_container_width=True,
                hide_index=True,
            )

            if st.button("Actualizar / Reemplazar an?lisis", key=f"ap_refresh_{selected_ficha}"):
                _mark_provider_analysis_pending(selected_ficha)
                st.success("La ficha qued? en pendiente_json_proveedores. Ya puedes pegar una nueva versi?n.")
                st.rerun()

    with tab_versions:
        if versions_df.empty:
            st.info("No hay versiones guardadas de an?lisis de proveedores.")
        else:
            st.dataframe(versions_df, use_container_width=True, hide_index=True)
            ficha_opts = sorted(versions_df["ficha"].astype(str).unique().tolist())
            sel_ficha = st.selectbox("Ficha para revisar versiones", ficha_opts, key="ap_versions_ficha")
            vf = versions_df[versions_df["ficha"].astype(str) == str(sel_ficha)].copy()
            if vf.empty:
                st.info("No hay versiones para esa ficha.")
            else:
                st.markdown("#### Historial de versiones")
                st.dataframe(vf, use_container_width=True, hide_index=True)
                sel_ids = vf["analisis_id"].astype(str).tolist()
                sel_id = st.selectbox("Versi?n a inspeccionar", sel_ids, key="ap_version_id")
                rr = vf[vf["analisis_id"].astype(str) == str(sel_id)].head(1)
                if not rr.empty:
                    raw = _clean_text(rr.iloc[0].get("json_raw", ""))
                    with st.expander("JSON crudo de la versi?n", expanded=False):
                        st.code(raw or "{}", language="json")
                if st.button("Solicitar nueva versi?n (abrir flujo JSON)", key=f"ap_versions_refresh_{sel_ficha}"):
                    _mark_provider_analysis_pending(sel_ficha)
                    st.success("La ficha fue movida a pendiente_json_proveedores.")
                    st.rerun()


def _render_tab_analisis_proveedores_v2(ranked_df: pd.DataFrame) -> None:
    st.markdown("### ANALISIS_DE_PROVEEDORES")
    _ensure_study_db()
    persist_status = _clean_text(st.session_state.get("intel_study_persist_status", ""))
    if persist_status:
        st.caption(f"Persistencia estudios: {persist_status}")

    resumen_df = _load_resumen_estudiadas_df()
    if resumen_df.empty:
        st.info("Aun no hay fichas estudiadas. Completa primero la etapa de estudio de fichas.")
        return

    created_ctx, updated_ctx = _ensure_provider_analysis_contexts(resumen_df, ranked_df)
    if created_ctx or updated_ctx:
        st.caption(
            f"Preparacion automatica ejecutada: contextos creados={created_ctx}, actualizados={updated_ctx}."
        )

    ctx_df = _load_ap_context_df()
    active_df = _load_ap_active_versions_df()
    if ctx_df.empty:
        st.info("No se pudo preparar contexto para analisis de proveedores.")
        return

    ctx_df = ctx_df.sort_values(["updated_at", "ficha"], ascending=[False, True], kind="stable").copy()
    ctx_df["ficha_label"] = ctx_df.apply(
        lambda r: f"{_clean_text(r.get('ficha', ''))} - {_clean_text(r.get('nombre_ficha', ''))}".strip(" -"),
        axis=1,
    )
    ctx_df["tiene_analisis"] = (
        ctx_df.get("analisis_id_activo", pd.Series(dtype=str)).astype(str).str.strip() != ""
    )

    st.dataframe(
        ctx_df[
            [
                "ficha",
                "nombre_ficha",
                "estado_analisis",
                "fecha_contexto_generado",
                "updated_at",
                "tiene_analisis",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    options = ctx_df["ficha"].astype(str).tolist()
    label_map = dict(zip(ctx_df["ficha"].astype(str), ctx_df["ficha_label"].astype(str)))
    selected_ficha = st.selectbox(
        "Ficha (estudiada)",
        options,
        key="ap_editor_ficha_v2",
        format_func=lambda x: label_map.get(str(x), str(x)),
    )
    selected_row = ctx_df[ctx_df["ficha"].astype(str) == str(selected_ficha)].head(1)
    if selected_row.empty:
        st.info("Selecciona una ficha.")
        return

    row = selected_row.iloc[0]
    st.caption(
        f"Estado actual: `{_clean_text(row.get('estado_analisis', ''))}` | "
        f"Contexto generado: {_clean_text(row.get('fecha_contexto_generado', '')) or '-'}"
    )
    if _clean_text(row.get("analisis_id_activo", "")):
        st.info("Esta ficha ya tiene analisis cargado. Si guardas un nuevo JSON, reemplazara al actual.")

    st.markdown("#### Analisis actual guardado")
    vrow = active_df[active_df["ficha"].astype(str) == str(selected_ficha)].head(1)
    if vrow.empty:
        st.info("Todavia no hay analisis guardado para esta ficha.")
    else:
        v = vrow.iloc[0]
        analisis_id = _clean_text(v.get("analisis_id", ""))
        st.caption(
            f"Fecha de carga: {_clean_text(v.get('fecha_carga', '')) or '-'} | "
            f"Ultima actualizacion: {_clean_text(v.get('fecha_ultima_actualizacion', '')) or '-'}"
        )
        st.markdown("#### Resumen ejecutivo")
        st.info(_clean_text(v.get("resumen_ejecutivo", "")) or "Sin resumen ejecutivo.")

        hist_df = _load_ap_table_by_analisis("analisis_proveedores_hist_panama", analisis_id)
        gama_df = _load_ap_table_by_analisis("analisis_proveedores_mejor_gama", analisis_id)
        precio_df = _load_ap_table_by_analisis("analisis_proveedores_mejor_precio", analisis_id)

        st.markdown("#### 1) Proponentes historicos en Panama")
        show_hist = [c for c in AP_HIST_COLUMNS if c in hist_df.columns]
        st.dataframe(
            hist_df[show_hist] if show_hist else _empty_table(AP_HIST_COLUMNS),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### 2) Mejores por gama")
        show_gama = [c for c in AP_GAMA_COLUMNS if c in gama_df.columns]
        st.dataframe(
            gama_df[show_gama] if show_gama else _empty_table(AP_GAMA_COLUMNS),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### 3) Mejores por precio")
        show_precio = [c for c in AP_PRECIO_COLUMNS if c in precio_df.columns]
        st.dataframe(
            precio_df[show_precio] if show_precio else _empty_table(AP_PRECIO_COLUMNS),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("---")
    st.markdown("#### Prompt listo para ChatGPT")
    prompt_value = _clean_text(row.get("prompt_texto", ""))
    st.text_area(
        "Prompt generado",
        value=prompt_value,
        key=f"ap_prompt_v2_{selected_ficha}",
        height=280,
    )
    prompt_js = json.dumps(prompt_value, ensure_ascii=False)
    copy_status_id = f"copy_status_{re.sub(r'[^a-zA-Z0-9_]', '_', str(selected_ficha))}_v2"
    components.html(
        f"""
        <div style="display:flex; align-items:center; gap:8px; margin:2px 0 8px 0;">
          <button
            style="background:#00a99d;color:white;border:none;border-radius:8px;padding:8px 14px;cursor:pointer;font-weight:600;"
            onclick='navigator.clipboard.writeText({prompt_js}).then(() => {{
              const el = document.getElementById("{copy_status_id}");
              if (el) {{ el.textContent = "Prompt copiado."; }}
            }}).catch(() => {{
              const el = document.getElementById("{copy_status_id}");
              if (el) {{ el.textContent = "No se pudo copiar automaticamente."; }}
            }});'
          >
            Copiar prompt
          </button>
          <span id="{copy_status_id}" style="font-size:12px;color:#8fb9ff;"></span>
        </div>
        """,
        height=44,
    )

    json_input = st.text_area(
        "Pega aqui el JSON devuelto por ChatGPT",
        key=f"ap_json_input_v2_{selected_ficha}",
        height=260,
    )
    validate_key = f"ap_valid_payload_v2_{selected_ficha}"
    err_key = f"ap_valid_errors_v2_{selected_ficha}"
    warn_key = f"ap_valid_warnings_v2_{selected_ficha}"

    c1, c2 = st.columns([1, 1])
    if c1.button("Validar JSON", key=f"ap_validate_v2_{selected_ficha}"):
        payload, errors, warnings = _validate_provider_analysis_json(json_input, selected_ficha)
        st.session_state[validate_key] = payload if not errors else {}
        st.session_state[err_key] = errors
        st.session_state[warn_key] = warnings

    errors = st.session_state.get(err_key, [])
    warnings = st.session_state.get(warn_key, [])
    for w in warnings:
        st.warning(w)
    for e in errors:
        st.error(e)
    payload_ok = st.session_state.get(validate_key, {})
    can_save = isinstance(payload_ok, dict) and bool(payload_ok) and not errors

    if c2.button("Guardar analisis (reemplaza anterior)", key=f"ap_save_v2_{selected_ficha}", disabled=not can_save):
        analisis_id, version_num, new_state = _save_provider_analysis_payload(selected_ficha, payload_ok)
        st.success(
            f"Analisis guardado. analisis_id={analisis_id[:8]}..., "
            f"version={version_num}, estado={new_state}. Se conserva solo la version mas reciente."
        )
        st.rerun()


def _render_tab_contacto_correos() -> None:
    st.markdown("### Contacto y correos")
    _placeholder_block(
        "Generador de correo inicial",
        "Aquí se generará el correo inicial usando variables del proveedor, ficha y producto.",
        ["proveedor", "ficha", "asunto", "cuerpo_correo", "canal_sugerido"],
    )
    st.caption("Acciones visuales previstas: copiar correo, abrir mailto, abrir WhatsApp, marcar enviado.")


def _render_tab_seguimiento_contacto() -> None:
    st.markdown("### Seguimiento de contacto (CRM)")
    _placeholder_block(
        "Matriz de seguimiento",
        "Aquí se mostrará estado por proveedor/contacto/canal y días desde último contacto.",
        [
            "ficha",
            "proveedor",
            "canal_usado",
            "fecha_primer_contacto",
            "correo_enviado",
            "whatsapp_enviado",
            "contacto_exitoso_correo",
            "contacto_exitoso_whatsapp",
            "respuesta_recibida",
            "estado_actual",
            "dias_desde_ultimo_contacto",
        ],
    )
    _placeholder_block(
        "Automatización de follow-up",
        "Aquí se verán reglas automáticas de 2do y 3er contacto, con próximos pasos sugeridos.",
    )


def _render_tab_resultado_final() -> None:
    st.markdown("### Resultado final por ficha")
    row1_col1, row1_col2 = st.columns(2)
    with row1_col1:
        _placeholder_block(
            "Ficha viable: prov. en conv.",
            "Aquí se listarán fichas viables con contacto activo y proveedor útil.",
            ["ficha", "proveedor", "marca", "modelo", "pais_origen", "email", "whatsapp", "estado_contacto", "precio", "observaciones"],
        )
    with row1_col2:
        _placeholder_block(
            "Ficha en est.: pend. contacto",
            "Aquí se listarán fichas que requieren primer contacto o siguiente acción inmediata.",
            ["ficha", "prioridad", "proveedor_objetivo", "canal_recomendado", "observaciones"],
        )

    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        _placeholder_block(
            "Ficha en est.: sin proveedor",
            "Aquí se listarán fichas con intentos agotados sin proveedor confirmado.",
            ["ficha", "intentos_realizados", "canales_usados", "motivo_actual", "proximo_paso"],
        )
    with row2_col2:
        _placeholder_block(
            "Ficha contactada no rentable",
            "Aquí se mostrará justificación económica con proveedores contactados y razones.",
            [
                "ficha",
                "proveedores_contactados",
                "precio_obtenido",
                "rango_objetivo",
                "diferencia_%",
                "razones_no_rentable",
            ],
        )


st.markdown("# 🧠 Inteligencia de Prospección CT y Proveedores")
st.caption("Fase 1.1: captacion operativa desde DB + arquitectura del embudo.")

def _tab_requires_universe(tab_name: str) -> bool:
    return tab_name in {"Dashboard", "Detecc. fichas", "Estudio de fichas", "AnÃ¡lisis proveedores"}


def _tab_requires_study_bootstrap(tab_name: str) -> bool:
    return tab_name in {"Fichas en seg.", "Estudio de fichas", "AnÃ¡lisis proveedores"}


def _weights_signature(weights: dict[str, float]) -> str:
    keys = sorted(_default_weights().keys())
    parts: list[str] = []
    for key in keys:
        parts.append(f"{key}:{float(weights.get(key, 0.0)):.4f}")
    return "|".join(parts)


def _get_universe_session_cached() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    if not st.session_state.get("intel_universe_cache_ready", False):
        ficha_metrics_df, ficha_acts_df, db_path = _build_ficha_universe()
        st.session_state["intel_ficha_metrics_df_cache"] = ficha_metrics_df
        st.session_state["intel_ficha_acts_df_cache"] = ficha_acts_df
        st.session_state["intel_db_path_cache"] = db_path
        st.session_state["intel_universe_cache_ready"] = True
    return (
        st.session_state.get("intel_ficha_metrics_df_cache", pd.DataFrame()),
        st.session_state.get("intel_ficha_acts_df_cache", pd.DataFrame()),
        str(st.session_state.get("intel_db_path_cache", "") or ""),
    )


def _get_ranked_session_cached(ficha_metrics_df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    if ficha_metrics_df.empty:
        st.session_state["intel_ranked_df_cache"] = pd.DataFrame()
        st.session_state["intel_ranked_weights_sig"] = _weights_signature(weights)
        st.session_state["intel_ranked_rows"] = 0
        return pd.DataFrame()

    sig = _weights_signature(weights)
    cached_sig = str(st.session_state.get("intel_ranked_weights_sig", "") or "")
    cached_rows = int(st.session_state.get("intel_ranked_rows", -1))
    cached_df = st.session_state.get("intel_ranked_df_cache", pd.DataFrame())
    if (
        isinstance(cached_df, pd.DataFrame)
        and cached_sig == sig
        and cached_rows == int(len(ficha_metrics_df))
    ):
        return cached_df

    ranked_df = _score_fichas(ficha_metrics_df, weights)
    st.session_state["intel_ranked_df_cache"] = ranked_df
    st.session_state["intel_ranked_weights_sig"] = sig
    st.session_state["intel_ranked_rows"] = int(len(ficha_metrics_df))
    return ranked_df


_ensure_default_weights_profile()
_render_sidebar()

TAB_OPTIONS = [
    "Dashboard",
    "Detecc. fichas",
    "Fichas en seg.",
    "Estudio de fichas",
    "Análisis proveedores",
    "Contacto y correos",
    "Seg. contacto",
    "Resultado ficha",
]
default_tab = "Fichas en seg."
if st.session_state.get("intel_active_tab") not in TAB_OPTIONS:
    st.session_state["intel_active_tab"] = default_tab

# Carga optimizada:
# con st.tabs Streamlit ejecuta todas las pestanas en cada rerun.
# con radio horizontal solo se renderiza la seccion activa.
active_tab = st.radio(
    "Secciones",
    TAB_OPTIONS,
    key="intel_active_tab",
    horizontal=True,
    label_visibility="collapsed",
)

needs_universe = _tab_requires_universe(active_tab)
needs_study_bootstrap = _tab_requires_study_bootstrap(active_tab)

if needs_study_bootstrap:
    _bootstrap_study_storage_once()

ficha_metrics_df = pd.DataFrame()
ficha_acts_df = pd.DataFrame()
db_path = ""
ranked_df = st.session_state.get("intel_ranked_df_cache", pd.DataFrame())
if not isinstance(ranked_df, pd.DataFrame):
    ranked_df = pd.DataFrame()

if needs_universe:
    if not st.session_state.get("intel_universe_cache_ready", False):
        with st.spinner("Cargando universo de fichas..."):
            ficha_metrics_df, ficha_acts_df, db_path = _get_universe_session_cached()
    else:
        ficha_metrics_df, ficha_acts_df, db_path = _get_universe_session_cached()
    ranked_df = _get_ranked_session_cached(ficha_metrics_df, st.session_state["intel_weights"])

if needs_study_bootstrap:
    _auto_enqueue_study_backlog_once(str(db_path or st.session_state.get("intel_db_path_cache", "") or ""))

_render_kpis(ranked_df)

if active_tab == "Dashboard":
    _render_tab_dashboard(ranked_df, db_path)
elif active_tab == "Detecc. fichas":
    _render_tab_deteccion_ct(ficha_metrics_df, ficha_acts_df)
elif active_tab == "Fichas en seg.":
    _render_tab_seguimiento_ct()
elif active_tab == "Estudio de fichas":
    _render_tab_estudio_profundo(ficha_acts_df, ranked_df, db_path)
elif active_tab == "Análisis proveedores":
    _render_tab_analisis_proveedores_v2(ranked_df)
elif active_tab == "Contacto y correos":
    _render_tab_contacto_correos()
elif active_tab == "Seg. contacto":
    _render_tab_seguimiento_contacto()
else:
    _render_tab_resultado_final()
