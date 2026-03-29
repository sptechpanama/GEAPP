from __future__ import annotations

import re
import sqlite3
from datetime import date
import io
import os
import unicodedata
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import streamlit as st
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


credentials = {
    "usernames": {u: {"name": n, "password": _hash(p)} for u, (n, p) in USERS.items()}
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


def _normalize_column_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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


@st.cache_data(show_spinner=False, ttl=43200)
def _load_ctni_num_to_id_map() -> dict[str, str]:
    out: dict[str, str] = {}
    start = 0
    length = 1000
    draw = 1
    total = None
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
        if total is None:
            try:
                total = int(body.get("recordsTotal", len(data)))
            except Exception:
                total = len(data)

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
        if total is not None and start >= total:
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


@st.cache_data(show_spinner=False, ttl=300)
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

    base = exploded[["ficha", "id", "ganador"]].copy()
    base["ficha"] = base["ficha"].astype(str).str.strip()
    base["ganador"] = base["ganador"].fillna("").astype(str).str.strip()
    base = base[(base["ficha"] != "")].drop_duplicates(subset=["ficha", "id", "ganador"])
    if base.empty:
        return pd.DataFrame()

    total_actos = base.groupby("ficha", dropna=False)["id"].nunique().rename("actos_total")
    wins = (
        base[base["ganador"] != ""]
        .groupby(["ficha", "ganador"], dropna=False)["id"]
        .nunique()
        .reset_index(name="victorias")
    )
    if wins.empty:
        return pd.DataFrame()

    wins = wins.sort_values(["ficha", "victorias", "ganador"], ascending=[True, False, True]).reset_index(drop=True)
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
            payload[f"top{rank}_ganador"] = str(item.get("ganador", "") or "")
            payload[f"top{rank}_pct_ganadas"] = float(item.get("pct_ganadas", 0.0))
            payload[f"top{rank}_victorias"] = int(item.get("victorias", 0))
        rows.append(payload)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=300)
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
            "ganadores_distintos": grouped["ganador"].apply(lambda s: s[s.str.strip() != ""].nunique()),
            "proponentes_promedio": grouped["num_participantes_num"].mean(),
        }
    ).reset_index(drop=True)
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
        "actos": 20.0,
        "monto": 25.0,
        "ganadores": 20.0,
        "proponentes": 20.0,
        "riesgo": 15.0,
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


def _score_fichas(ficha_df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    if ficha_df.empty:
        return ficha_df
    df = ficha_df.copy()
    df["f_actos"] = _minmax(df["actos"])
    df["f_monto"] = _minmax(df["monto_historico"])
    df["f_ganadores"] = _minmax(df["ganadores_distintos"])
    df["f_proponentes"] = _minmax(df["proponentes_promedio"])
    df["f_riesgo"] = df["clase_riesgo"].map(_risk_to_score)

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


def _ensure_study_state() -> list[dict]:
    if "intel_fichas_estudio" not in st.session_state:
        st.session_state["intel_fichas_estudio"] = []
    return st.session_state["intel_fichas_estudio"]


def _add_ficha_to_study(row: pd.Series) -> bool:
    current = _ensure_study_state()
    ficha = str(row.get("ficha", "")).strip()
    if not ficha:
        return False
    if any(str(item.get("ficha", "")).strip() == ficha for item in current):
        return False
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
            "top1_ganador": str(row.get("top1_ganador", "")).strip(),
            "top1_pct_ganadas": float(row.get("top1_pct_ganadas", 0.0)),
            "top2_ganador": str(row.get("top2_ganador", "")).strip(),
            "top2_pct_ganadas": float(row.get("top2_pct_ganadas", 0.0)),
            "top3_ganador": str(row.get("top3_ganador", "")).strip(),
            "top3_pct_ganadas": float(row.get("top3_pct_ganadas", 0.0)),
            "estado": "pendiente de estudio profundo",
            "fecha_ingreso": date.today().isoformat(),
            "notas": "",
        }
    )
    st.session_state["intel_fichas_estudio"] = current
    return True

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
    fichas_estudio = _ensure_study_state()
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
    if db_status:
        st.caption(f"Estado fuente: {db_status}")
    if risk_status:
        st.caption(risk_status)
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
        "clase_riesgo",
        "score_total",
        "clasificacion",
        "actos",
        "actos_solo_ficha",
        "actos_con_otras_fichas",
        "monto_historico",
        "ganadores_distintos",
        "proponentes_promedio",
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
        },
    )

def _render_tab_deteccion_ct(ficha_metrics_df: pd.DataFrame, ficha_acts_df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("### Deteccion automatica de fichas")
    sub1, sub2, sub3 = st.tabs(["Scoring", "Resultados", "Detalle ficha"])
    default_weights = _default_weights()

    stored_weights = st.session_state.get("intel_weights", {})
    weights = _normalize_weights(stored_weights, default_weights)
    st.session_state["intel_weights"] = weights

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
        c1, c2, c3 = st.columns(3)
        weights["actos"] = c1.slider("Peso frecuencia", 0.0, 100.0, float(weights["actos"]), 1.0)
        weights["monto"] = c1.slider("Peso monto historico", 0.0, 100.0, float(weights["monto"]), 1.0)
        weights["ganadores"] = c2.slider("Peso ganadores distintos", 0.0, 100.0, float(weights["ganadores"]), 1.0)
        weights["proponentes"] = c2.slider(
            "Peso proponentes promedio por acto", 0.0, 100.0, float(weights["proponentes"]), 1.0
        )
        weights["riesgo"] = c3.slider("Peso clase de riesgo", 0.0, 100.0, float(weights["riesgo"]), 1.0)
        st.session_state["intel_weights"] = weights

        total_weights = sum(weights.values())
        st.caption(f"Suma de pesos: {total_weights:.1f} (debe ser 100)")
        if abs(total_weights - 100.0) > 0.001:
            st.error("La suma de pesos debe ser exactamente 100 para calcular el score.")

        b1, b2, b3, b4 = st.columns(4)
        if b1.button("Recalcular"):
            if abs(total_weights - 100.0) > 0.001:
                st.warning("No se recalcula: ajusta los pesos para sumar 100.")
            else:
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

    if abs(sum(weights.values()) - 100.0) > 0.001:
        with sub2:
            st.warning("Ajusta los pesos para sumar 100 y ver el ranking.")
        with sub3:
            st.info("Ajusta pesos para habilitar detalle por ficha.")
        return pd.DataFrame()

    ranked_df = _score_fichas(ficha_metrics_df, weights)

    with sub2:
        st.caption("Ranking completo de fichas detectadas en actos (ordenado por score).")
        st.caption(
            "Actos = todos los actos donde aparece la ficha. "
            "Actos_solo_ficha = actos donde solo aparece esa ficha. "
            "Actos_con_otras_fichas = actos donde comparte con otras fichas."
        )
        ranking_cols = [
            "ficha",
            "nombre_ficha",
            "ver_ficha_minsa",
            "clase_riesgo",
            "score_total",
            "clasificacion",
            "actos",
            "actos_solo_ficha",
            "actos_con_otras_fichas",
            "monto_historico",
            "ganadores_distintos",
            "proponentes_promedio",
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
        view_df["ver_ficha_minsa"] = view_df["enlace_minsa"]
        show_rank_cols = [c for c in ranking_cols if c in view_df.columns]
        st.dataframe(
            view_df[show_rank_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "ver_ficha_minsa": st.column_config.LinkColumn("Enlace MINSA", display_text="Ver ficha"),
            },
        )

        st.markdown("#### Acciones sobre ficha seleccionada")
        ficha_opts = view_df["ficha"].astype(str).tolist()
        selected_action_ficha = st.selectbox(
            "Selecciona ficha",
            ficha_opts,
            index=0 if ficha_opts else None,
            key="intel_action_ficha",
        )
        a1, a2 = st.columns(2)
        if a1.button("Ver actos de ficha", disabled=not bool(selected_action_ficha)):
            st.session_state["intel_selected_ficha"] = str(selected_action_ficha)
            st.rerun()
        if a2.button("Pasar ficha a estudio", disabled=not bool(selected_action_ficha)):
            full_row = ranked_df[ranked_df["ficha"].astype(str) == str(selected_action_ficha)]
            if not full_row.empty and _add_ficha_to_study(full_row.iloc[0]):
                st.success(f"Ficha {selected_action_ficha} enviada a 'Fichas en seg.'")
            else:
                st.info(f"Ficha {selected_action_ficha} ya estaba en seguimiento.")
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
                f"#### Score de ficha {selected} - {nombre_ficha}: "
                f"{row['score_total']:.2f} ({row['clasificacion']})"
            )
        else:
            st.markdown(f"#### Score de ficha {selected}: {row['score_total']:.2f} ({row['clasificacion']})")
        st.caption(
            f"Actos totales: {int(row.get('actos', 0))} | "
            f"Solo esa ficha: {int(row.get('actos_solo_ficha', 0))} | "
            f"Con otras fichas: {int(row.get('actos_con_otras_fichas', 0))}"
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
        acts = acts.rename(columns={"ganador": "proveedor_ganador", "num_participantes": "participantes"})
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
            "monto_estimado",
            "enlace",
        ]
        proponent_cols = [c for c in acts.columns if c.startswith("Proponente ")]
        price_cols = [c for c in acts.columns if c.startswith("Precio Proponente ")]
        show_cols.extend(proponent_cols + price_cols)
        present_cols = [c for c in show_cols if c in acts.columns]
        st.dataframe(acts[present_cols].head(500), use_container_width=True, hide_index=True)

    return ranked_df

def _render_tab_seguimiento_ct() -> None:
    st.markdown("### Fichas en seg.")
    fichas_estudio = _ensure_study_state()
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
        },
    )

    c1, c2, c3 = st.columns([1.4, 1.8, 1.0])
    target = c1.selectbox("Ficha a gestionar", df_seg["ficha"].astype(str).tolist(), key="intel_seg_target")
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
        for item in fichas_estudio:
            if str(item.get("ficha", "")) == str(target):
                item["estado"] = new_state
        st.session_state["intel_fichas_estudio"] = fichas_estudio
        st.success(f"Estado actualizado para ficha {target}.")

    if st.button("Quitar ficha seleccionada"):
        st.session_state["intel_fichas_estudio"] = [
            x for x in fichas_estudio if str(x.get("ficha", "")) != str(target)
        ]
        st.success(f"Ficha {target} removida de seguimiento.")
        st.rerun()


def _render_tab_estudio_profundo() -> None:
    st.markdown("### Estudio profundo por ficha")
    st.selectbox("Selecciona ficha para estudio", ["(sin datos en Fase 1)"], index=0)
    _placeholder_block(
        "Acto por acto",
        "Aquí se mostrará el detalle completo de actos asociados a la ficha seleccionada.",
        [
            "fecha_publicacion",
            "fecha_adjudicacion",
            "dias_pub_a_adj",
            "tiempo_entrega",
            "entidad",
            "proveedor_participante",
            "proveedor_ganador",
            "marca",
            "modelo",
            "pais_origen",
            "precio_unitario_ofertado",
            "precio_unitario_ganador",
            "cantidad",
            "monto_total",
        ],
    )
    _placeholder_block(
        "KPIs consolidados y variación de precios",
        "Aquí se mostrarán promedio/min/max de adjudicación, marcas/paises frecuentes y variación % de precios.",
    )
    _placeholder_block(
        "Gráficos de análisis",
        "Aquí se mostrarán barras de precios por proveedor, frecuencia de victorias, marcas y países.",
    )


def _render_tab_proveedores_historicos_ia() -> None:
    st.markdown("### Proveedores históricos + IA")
    _placeholder_block(
        "Tabla histórica por proveedor",
        "Aquí se mostrarán solo proveedores con al menos una adjudicación en la ficha seleccionada.",
        [
            "proveedor",
            "participaciones",
            "victorias",
            "%_victorias",
            "precio_min",
            "precio_prom",
            "precio_max",
            "marcas",
            "modelos",
            "paises_origen",
            "entidades_donde_mas_gana",
        ],
    )
    _placeholder_block(
        "Bloque IA (interpretación ejecutiva)",
        "Aquí se mostrará el análisis IA: dominante, agresivo en precio, premium, concentración, posibilidad de entrada.",
    )


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


def _render_architecture_notes() -> None:
    with st.expander("🧱 Arquitectura funcional propuesta (Fase 1 - diseño)", expanded=False):
        st.markdown(
            """
            - Esta página está montada como **blueprint visual** para validar flujo y UX.
            - En Fase 2 se conectará con tablas de datos (determinístico + IA).
            - Diseño preparado para módulos:
              - `core/ct_scoring.py`
              - `core/ct_analytics.py`
              - `services/ct_repository.py`
              - `services/ct_automation.py`
              - `services/ct_ai_insights.py`
            """
        )


st.markdown("# 🧠 Inteligencia de Prospección CT y Proveedores")
st.caption("Fase 1.1: captacion operativa desde DB + arquitectura del embudo.")

st.session_state["intel_weights"] = _normalize_weights(
    st.session_state.get("intel_weights", {}),
    _default_weights(),
)

ficha_metrics_df, ficha_acts_df, db_path = _build_ficha_universe()
ranked_df = _score_fichas(ficha_metrics_df, st.session_state["intel_weights"])

_render_sidebar()
_render_kpis(ranked_df)
_render_architecture_notes()

tabs = st.tabs(
    [
        "Dashboard",
        "Detecc. fichas",
        "Fichas en seg.",
        "Estudio ficha",
        "Prov. hist. + IA",
        "Contacto y correos",
        "Seg. contacto",
        "Resultado ficha",
    ]
)

with tabs[0]:
    _render_tab_dashboard(ranked_df, db_path)
with tabs[1]:
    _render_tab_deteccion_ct(ficha_metrics_df, ficha_acts_df)
with tabs[2]:
    _render_tab_seguimiento_ct()
with tabs[3]:
    _render_tab_estudio_profundo()
with tabs[4]:
    _render_tab_proveedores_historicos_ia()
with tabs[5]:
    _render_tab_contacto_correos()
with tabs[6]:
    _render_tab_seguimiento_contacto()
with tabs[7]:
    _render_tab_resultado_final()
