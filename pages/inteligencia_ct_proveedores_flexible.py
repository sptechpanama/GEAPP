from __future__ import annotations

import io
import os
import re
import sqlite3
import time
import unicodedata
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy import create_engine

from core.config import APP_ROOT, DB_PATH
from services.access_control import build_authenticator, require_page_access
from services.auth_drive import get_drive_delegated
from services.inteligencia_ct_flexible import (
    build_detected_fichas_summary,
    build_difference_view,
    build_profile_comparison,
    build_rescued_acts_view,
)
from services.panama_compra_detection_v2 import (
    apply_detection_profiles_to_dataframe,
    flexible_output_col,
    get_detection_profile_labels,
    list_detection_profiles,
    normalize_detection_profile_key,
)
from ui.theme import apply_global_theme


st.set_page_config(
    page_title="Inteligencia CT Flexible",
    page_icon="🧠",
    layout="wide",
)
apply_global_theme()


authenticator = build_authenticator()

try:
    authenticator.login(" ", location="sidebar", key="auth_intel_ct_flexible_silent")
    st.sidebar.empty()
except Exception:
    pass

require_page_access("pages/inteligencia_ct_proveedores_flexible.py")

authenticator.logout("Cerrar sesión", location="sidebar")


FALLBACK_DB_PATH = Path(r"C:\Users\rodri\OneDrive\cl\panamacompra.db")
PROFILE_KEYS = list(list_detection_profiles())
PROFILE_LABELS = get_detection_profile_labels()
DEFAULT_PROFILE = "moderado"
DATE_CANDIDATE_COLUMNS = ("fecha", "publicacion", "fecha_actualizacion")
TEXT_SEARCH_COLUMNS = ("titulo", "descripcion", "entidad", "unidad_solic", "termino_entrega", "estado", "ficha_detectada")
DB_RELEVANT_COLUMN_KEYS = {
    "id",
    "fecha actualizacion",
    "publicacion",
    "enlace",
    "titulo",
    "precio referencia",
    "fecha",
    "entidad",
    "unidad solic",
    "unidad solicitante",
    "termino entrega",
    "ficha detectada",
    "estado",
    "descripcion",
}
DEFAULT_MAX_ROWS = 50
MAX_PROCESS_ROWS = 400


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    return text


def _normalize_text(value: object, *, strip_accents: bool = True) -> str:
    text = _clean_text(value).lower()
    if strip_accents:
        text = "".join(
            ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
        )
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_column_key(value: object) -> str:
    text = _normalize_text(value)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_any_date(value: object) -> pd.Timestamp:
    text = _clean_text(value)
    if not text:
        return pd.NaT
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, errors="coerce")
    return parsed


def _detected_mask(series: pd.Series) -> pd.Series:
    clean = series.fillna("").astype(str).str.strip()
    return clean.ne("") & clean.str.lower().ne("no detectada")


def _quote_identifier(identifier: str) -> str:
    return f'"{str(identifier).replace(chr(34), chr(34) * 2)}"'


def _quote_sqlite_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _select_projection_columns(columns: list[str] | tuple[str, ...]) -> list[str]:
    selected: list[str] = []
    for col in columns:
        normalized = _normalize_column_key(col)
        if normalized in DB_RELEVANT_COLUMN_KEYS:
            selected.append(str(col))
            continue
        if normalized.startswith("item "):
            selected.append(str(col))
            continue
        if normalized.startswith("item_"):
            selected.append(str(col))
            continue
        if re.fullmatch(r"item \d+", normalized):
            selected.append(str(col))
            continue
    if selected:
        return selected
    return [str(col) for col in columns]


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


def _get_drive_client() -> tuple[object | None, str]:
    try:
        drive = get_drive_delegated()
        if drive is not None:
            return drive, "delegated"
    except Exception:
        pass

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
        return build("drive", "v3", credentials=creds), "service_account"
    except Exception as exc:
        return None, f"auth_error:{exc}"


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
    for key in (
        "DRIVE_PANAMACOMPRA_FILE_ID",
        "DRIVE_PANAMACOMPRA_DB_FILE_ID",
        "DRIVE_DB_PANAMACOMPRA_FILE_ID",
    ):
        value = os.environ.get(key)
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    return ""


def _find_panamacompra_db_file_id_in_drive() -> str:
    try:
        drive, _ = _get_drive_client()
        if drive is None:
            return ""
        response = (
            drive.files()
            .list(
                q="name='panamacompra.db' and trashed=false",
                fields="files(id,name,modifiedTime)",
                pageSize=5,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", []) or []
        if not files:
            return ""
        return str(files[0].get("id", "")).strip()
    except Exception:
        return ""


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


def _resolve_db_path() -> Path | None:
    for candidate in _candidate_db_paths():
        if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
            return candidate

    file_id = _panamacompra_drive_file_id() or _find_panamacompra_db_file_id_in_drive()
    if not file_id:
        return None
    raw, _ = _download_panamacompra_db_from_drive(file_id)
    if not raw:
        return None
    runtime_path = APP_ROOT / "data" / "db" / "panamacompra_drive.db"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_bytes(raw)
    return runtime_path


def _supabase_db_url() -> str:
    try:
        app_cfg = st.secrets.get("app", {})
    except Exception:
        app_cfg = {}
    for value in (
        app_cfg.get("SUPABASE_DB_URL") if isinstance(app_cfg, dict) else None,
        app_cfg.get("DATABASE_URL") if isinstance(app_cfg, dict) else None,
        os.environ.get("SUPABASE_DB_URL"),
        os.environ.get("DATABASE_URL"),
    ):
        if value and str(value).strip():
            return str(value).strip()
    return ""


@st.cache_data(show_spinner=False, ttl=1800)
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
            selected_table = ""
            for candidate in ("actos_publicos", "actos", "panamacompra_actos"):
                if candidate in lower_map:
                    selected_table = lower_map[candidate]
                    break
            if not selected_table:
                for table in tables:
                    if "acto" in table.lower():
                        selected_table = table
                        break
            if not selected_table:
                return pd.DataFrame(), "postgres"
            cols_df = pd.read_sql_query(
                (
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema='public' AND table_name='{selected_table}' "
                    "ORDER BY ordinal_position"
                ),
                conn,
            )
            selected_cols = _select_projection_columns(cols_df["column_name"].astype(str).tolist())
            select_sql = ", ".join(_quote_identifier(col) for col in selected_cols) if selected_cols else "*"
            df = pd.read_sql_query(f'SELECT {select_sql} FROM "{selected_table}"', conn)
            return _prepare_reference_dates(df), f"postgres:{selected_table}"
    except Exception:
        return pd.DataFrame(), "postgres"


@st.cache_data(show_spinner=False, ttl=1800)
def _load_actos_db_df() -> tuple[pd.DataFrame, str]:
    db_path = _resolve_db_path()
    if db_path is not None:
        try:
            with sqlite3.connect(db_path) as conn:
                tables_df = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table'",
                    conn,
                )
                tables = set(tables_df["name"].astype(str).tolist())
                selected_table = ""
                for candidate in ("actos_publicos", "actos", "panamacompra_actos"):
                    if candidate in tables:
                        selected_table = candidate
                        break
                if not selected_table:
                    return pd.DataFrame(), str(db_path)
                schema_df = pd.read_sql_query(
                    f"SELECT name FROM pragma_table_info({_quote_sqlite_string(selected_table)})",
                    conn,
                )
                selected_cols = _select_projection_columns(schema_df["name"].astype(str).tolist())
                select_sql = ", ".join(_quote_identifier(col) for col in selected_cols) if selected_cols else "*"
                df = pd.read_sql_query(f"SELECT {select_sql} FROM {_quote_identifier(selected_table)}", conn)
            return _prepare_reference_dates(df), str(db_path)
        except Exception:
            pass
    return _load_actos_postgres_df()


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


@st.cache_data(show_spinner=False, ttl=1800)
def _load_detection_metadata_df() -> tuple[pd.DataFrame, str]:
    for path in _candidate_fichas_paths():
        if not path.exists():
            continue
        try:
            df = pd.read_excel(path)
            if not df.empty:
                return df, str(path)
        except Exception:
            continue
    return pd.DataFrame(), "sin_metadata_local"


def _prepare_reference_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["__fecha_ref__"] = pd.NaT
    for col in DATE_CANDIDATE_COLUMNS:
        if col not in out.columns:
            continue
        parsed = out[col].map(_parse_any_date)
        out["__fecha_ref__"] = out["__fecha_ref__"].fillna(parsed)
    return out


def _filter_source_rows(
    df: pd.DataFrame,
    *,
    only_without_previous: bool,
    states: tuple[str, ...],
    date_from: pd.Timestamp | None,
    date_to: pd.Timestamp | None,
    search_text: str,
    max_rows: int,
) -> pd.DataFrame:
    work = df.copy()
    if "__fecha_ref__" not in work.columns:
        work = _prepare_reference_dates(work)
    if only_without_previous and "ficha_detectada" in work.columns:
        work = work[~_detected_mask(work["ficha_detectada"])].copy()
    if states and "estado" in work.columns:
        state_set = {str(value).strip().lower() for value in states if str(value).strip()}
        work = work[work["estado"].fillna("").astype(str).str.strip().str.lower().isin(state_set)].copy()
    if date_from is not None:
        work = work[work["__fecha_ref__"].isna() | (work["__fecha_ref__"] >= date_from)].copy()
    if date_to is not None:
        work = work[work["__fecha_ref__"].isna() | (work["__fecha_ref__"] <= date_to)].copy()

    raw_search = _clean_text(search_text)
    if raw_search:
        terms = [_normalize_text(token) for token in raw_search.split() if _normalize_text(token)]
        if terms:
            text_cols = [col for col in TEXT_SEARCH_COLUMNS if col in work.columns]
            if text_cols:
                haystack = work[text_cols].fillna("").astype(str).agg(" ".join, axis=1).map(_normalize_text)
                mask = pd.Series(True, index=work.index)
                for term in terms:
                    mask = mask & haystack.str.contains(re.escape(term), regex=True, na=False)
                work = work[mask].copy()

    if "__fecha_ref__" in work.columns:
        work = work.sort_values(by=["__fecha_ref__"], ascending=[False], kind="stable")
    elif "id" in work.columns:
        work = work.sort_values(by=["id"], ascending=[False], kind="stable")

    if max_rows > 0:
        work = work.head(int(max_rows)).copy()
    return work.reset_index(drop=True)


def _result_signature(payload: dict[str, object]) -> tuple[tuple[str, object], ...]:
    return tuple(sorted(payload.items()))


@st.cache_data(show_spinner=False, ttl=1800)
def _run_flexible_detection_cached(
    subset_df: pd.DataFrame,
    profiles: tuple[str, ...],
    metadata_source_key: str,
) -> pd.DataFrame:
    metadata_df, _ = _load_detection_metadata_df()
    return apply_detection_profiles_to_dataframe(
        subset_df.copy(),
        metadata_df=metadata_df if not metadata_df.empty else None,
        profiles=profiles,
    )


def _render_metrics_row(result_df: pd.DataFrame, selected_profile: str) -> None:
    selected_det_col = flexible_output_col("ficha_detectada", selected_profile)
    selected_detected_mask = _detected_mask(result_df.get(selected_det_col, pd.Series("", index=result_df.index)))
    selected_detected_df = result_df.loc[selected_detected_mask].copy()
    rescued_df = build_rescued_acts_view(result_df, selected_profile)
    unique_fichas = set()
    if not selected_detected_df.empty:
        unique_fichas = set(selected_detected_df[selected_det_col].fillna("").astype(str).tolist())
    cols = st.columns(4)
    cols[0].metric("Actos evaluados", f"{len(result_df):,}")
    cols[1].metric(f"Detectados {PROFILE_LABELS[selected_profile]}", f"{len(selected_detected_df):,}")
    cols[2].metric("Rescatados vs actual", f"{len(rescued_df):,}")
    cols[3].metric("Fichas únicas", f"{len(unique_fichas):,}")


def _comparison_with_rescues(result_df: pd.DataFrame) -> pd.DataFrame:
    comparison = build_profile_comparison(result_df, PROFILE_KEYS)
    if comparison.empty:
        return comparison
    comparison["Rescates vs actual"] = [
        len(build_rescued_acts_view(result_df, profile))
        for profile in comparison["Perfil"].tolist()
    ]
    comparison["Perfil label"] = comparison["Perfil"].map(lambda key: PROFILE_LABELS.get(str(key), str(key)))
    ordered_cols = [
        "Perfil label",
        "Actos detectados",
        "Rescates vs actual",
        "Fichas únicas",
        "Score promedio",
        "Confianza dominante",
    ]
    return comparison[ordered_cols].rename(columns={"Perfil label": "Perfil"})


st.markdown("# 🧠 Inteligencia de Prospección CT Flexible")
st.caption(
    "Página paralela de rescate de falsos negativos. Reevalúa actos usando tres perfiles "
    "(`Estricto`, `Moderado`, `Muy Flexible`) sobre el texto disponible del acto, sin tocar la lógica actual."
)

base_df, db_source = _load_actos_db_df()
metadata_source = str(st.session_state.get("intel_flex_metadata_source", "") or "")

if base_df.empty:
    st.error("No se pudo cargar la base de actos públicos desde SQLite/Drive/Postgres.")
    st.stop()
available_states = []
if "estado" in base_df.columns:
    available_states = sorted({str(value).strip() for value in base_df["estado"].fillna("").astype(str).tolist() if str(value).strip()})

valid_dates = base_df["__fecha_ref__"].dropna() if "__fecha_ref__" in base_df.columns else pd.Series(dtype="datetime64[ns]")
if not valid_dates.empty:
    max_date = valid_dates.max().date()
    min_date = valid_dates.min().date()
    default_from = max(min_date, max_date - pd.Timedelta(days=60))
else:
    max_date = pd.Timestamp.today().date()
    min_date = max_date
    default_from = max_date

with st.expander("Configuración del barrido", expanded=True):
    st.caption(
        "Para mantener la página ágil, el barrido se ejecuta sobre un subconjunto filtrado. "
        "El enfoque por defecto prioriza actos sin ficha previa para rescatar oportunidades perdidas."
    )
    st.caption("Para una respuesta más fluida, empieza con 25-50 actos y sube solo si necesitas más cobertura.")
    c1, c2, c3 = st.columns([1.1, 1.4, 1.1])
    with c1:
        only_without_previous = st.checkbox(
            "Solo actos sin ficha previa",
            value=True,
            help="Enfoca el análisis en falsos negativos reales del flujo actual.",
        )
        max_rows = st.slider(
            "Máximo de actos a procesar",
            min_value=25,
            max_value=MAX_PROCESS_ROWS,
            value=DEFAULT_MAX_ROWS,
            step=25,
            help="Sube este valor si quieres más cobertura. También aumentará el tiempo de cálculo.",
        )
    with c2:
        state_default = tuple(available_states) if available_states else tuple()
        selected_states = st.multiselect(
            "Estados a incluir",
            options=available_states,
            default=state_default,
        )
        search_text = st.text_input(
            "Filtro textual adicional",
            placeholder="Ej: sutura, cilindro, lente, esterilizador",
        )
    with c3:
        date_range = st.date_input(
            "Rango fecha referencia",
            value=(default_from, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        run_detection = st.button("Procesar barrido flexible", type="primary")

date_from_ts: pd.Timestamp | None = None
date_to_ts: pd.Timestamp | None = None
if isinstance(date_range, tuple) and len(date_range) == 2:
    date_from_ts = pd.Timestamp(date_range[0])
    date_to_ts = pd.Timestamp(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
elif date_range:
    date_from_ts = pd.Timestamp(date_range)
    date_to_ts = pd.Timestamp(date_range) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

request_payload = {
    "only_without_previous": bool(only_without_previous),
    "states": tuple(selected_states),
    "date_from": date_from_ts.isoformat() if date_from_ts is not None else "",
    "date_to": date_to_ts.isoformat() if date_to_ts is not None else "",
    "search_text": _clean_text(search_text),
    "max_rows": int(max_rows),
}

current_signature = _result_signature(request_payload)
last_signature = st.session_state.get("intel_flex_last_signature")
should_run = bool(run_detection)

if should_run:
    subset_df = _filter_source_rows(
        base_df,
        only_without_previous=only_without_previous,
        states=tuple(selected_states),
        date_from=date_from_ts,
        date_to=date_to_ts,
        search_text=search_text,
        max_rows=max_rows,
    )
    st.session_state["intel_flex_last_signature"] = _result_signature(request_payload)
    st.session_state["intel_flex_subset_count"] = len(subset_df)
    st.session_state["intel_flex_last_request"] = request_payload
    if subset_df.empty:
        st.session_state["intel_flex_result_df"] = pd.DataFrame()
    else:
        started_at = time.time()
        with st.spinner("Recalculando fichas con los tres perfiles..."):
            if not metadata_source:
                _, metadata_source = _load_detection_metadata_df()
            result_df = _run_flexible_detection_cached(
                subset_df,
                profiles=tuple(PROFILE_KEYS),
                metadata_source_key=str(metadata_source or ""),
            )
        st.session_state["intel_flex_result_df"] = result_df
        st.session_state["intel_flex_runtime_s"] = round(time.time() - started_at, 2)
        st.session_state["intel_flex_metadata_source"] = str(metadata_source or "")

result_df = st.session_state.get("intel_flex_result_df", pd.DataFrame())
subset_count = int(st.session_state.get("intel_flex_subset_count", 0) or 0)
runtime_s = float(st.session_state.get("intel_flex_runtime_s", 0.0) or 0.0)

st.caption(f"Fuente actos: `{db_source or 'desconocida'}`")
st.caption(f"Fuente metadata fichas: `{metadata_source or 'pendiente al ejecutar barrido'}`")

if last_signature is not None and current_signature != last_signature and not run_detection:
    st.warning("Hay cambios de filtros pendientes. Pulsa `Procesar barrido flexible` para recalcular los resultados.")

if result_df is None or result_df.empty:
    st.info("Configura los filtros y pulsa `Procesar barrido flexible` para cargar resultados.")
    st.stop()

selected_profile = normalize_detection_profile_key(
    st.selectbox(
        "Perfil operativo principal",
        options=PROFILE_KEYS,
        index=PROFILE_KEYS.index(DEFAULT_PROFILE) if DEFAULT_PROFILE in PROFILE_KEYS else 0,
        format_func=lambda key: PROFILE_LABELS.get(str(key), str(key)),
        help="Este perfil controla las métricas principales y los resúmenes por ficha.",
    )
)

st.caption(
    f"Subconjunto procesado: {subset_count:,} acto(s) | tiempo cálculo: {runtime_s:.2f}s | "
    f"perfil principal: {PROFILE_LABELS.get(selected_profile, selected_profile)}"
)

_render_metrics_row(result_df, selected_profile)

comparison_df = _comparison_with_rescues(result_df)
selected_summary_df, selected_detected_df = build_detected_fichas_summary(result_df, selected_profile)
rescued_df = build_rescued_acts_view(result_df, selected_profile)
differences_df = build_difference_view(result_df, PROFILE_KEYS)

tab_dash, tab_fichas, tab_actos, tab_diffs = st.tabs(
    ["Dashboard", "Fichas detectadas", "Actos detectados", "Diferencias"]
)

with tab_dash:
    if comparison_df.empty:
        st.info("No hubo detecciones con los perfiles configurados.")
    else:
        st.markdown("### Comparativa de perfiles")
        st.dataframe(comparison_df, use_container_width=True, hide_index=True)
        chart_df = comparison_df.set_index("Perfil")[["Actos detectados", "Rescates vs actual"]]
        st.bar_chart(chart_df, use_container_width=True)

    st.markdown(f"### Top fichas detectadas ({PROFILE_LABELS.get(selected_profile, selected_profile)})")
    if selected_summary_df.empty:
        st.info("El perfil seleccionado no detectó fichas en el subconjunto procesado.")
    else:
        st.dataframe(
            selected_summary_df.head(25),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Enlace ficha MINSA": st.column_config.LinkColumn("Enlace ficha MINSA", display_text="Abrir ficha"),
                "Monto referencial total": st.column_config.NumberColumn("Monto referencial total", format="$ %.2f"),
                "Score promedio": st.column_config.NumberColumn("Score promedio", format="%.2f"),
            },
        )

with tab_fichas:
    ficha_search = st.text_input("Buscar ficha o nombre", key="intel_flex_ficha_search")
    fichas_view = selected_summary_df.copy()
    if ficha_search.strip() and not fichas_view.empty:
        needle = _normalize_text(ficha_search)
        mask = (
            fichas_view["Ficha #"].fillna("").astype(str).map(_normalize_text).str.contains(needle, regex=False, na=False)
            | fichas_view["Nombre ficha"].fillna("").astype(str).map(_normalize_text).str.contains(needle, regex=False, na=False)
            | fichas_view["Clase ficha"].fillna("").astype(str).map(_normalize_text).str.contains(needle, regex=False, na=False)
        )
        fichas_view = fichas_view[mask].copy()
    if fichas_view.empty:
        st.info("No hay fichas para mostrar con el filtro actual.")
    else:
        st.dataframe(
            fichas_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Enlace ficha MINSA": st.column_config.LinkColumn("Enlace ficha MINSA", display_text="Abrir ficha"),
                "Monto referencial total": st.column_config.NumberColumn("Monto referencial total", format="$ %.2f"),
                "Score promedio": st.column_config.NumberColumn("Score promedio", format="%.2f"),
            },
        )

with tab_actos:
    only_detected = st.checkbox("Mostrar solo actos detectados por el perfil principal", value=True)
    only_rescued = st.checkbox("Mostrar solo rescates vs lógica actual", value=False)
    acts_view = result_df.copy()
    selected_det_col = flexible_output_col("ficha_detectada", selected_profile)
    if only_detected and selected_det_col in acts_view.columns:
        acts_view = acts_view[_detected_mask(acts_view[selected_det_col])].copy()
    if only_rescued:
        acts_view = build_rescued_acts_view(acts_view, selected_profile)

    if acts_view.empty:
        st.info("No hay actos para mostrar con los filtros actuales.")
    else:
        compare_cols: list[str] = []
        for profile in PROFILE_KEYS:
            compare_cols.extend(
                [
                    flexible_output_col("ficha_detectada", profile),
                    flexible_output_col("nombre_ficha", profile),
                    flexible_output_col("score_ficha", profile),
                    flexible_output_col("confianza_ficha", profile),
                    flexible_output_col("posible_ficha", profile),
                ]
            )
        base_cols = [col for col in ["id", "fecha_actualizacion", "publicacion", "fecha", "estado", "entidad", "precio_referencia", "titulo", "descripcion", "ficha_detectada", "enlace"] if col in acts_view.columns]
        display_cols = base_cols + [col for col in compare_cols if col in acts_view.columns]
        st.dataframe(
            acts_view[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "enlace": st.column_config.LinkColumn("Enlace acto", display_text="Abrir acto"),
                **{
                    col: st.column_config.NumberColumn(col, format="%.2f")
                    for col in display_cols
                    if col.startswith("score_ficha_")
                },
            },
        )

with tab_diffs:
    st.caption(
        "Aquí ves los actos donde los perfiles no coinciden entre sí. "
        "Es la mejor vista para calibrar ruido vs cobertura antes de llevar la lógica a producción."
    )
    if differences_df.empty:
        st.success("No hubo diferencias entre perfiles en el subconjunto procesado.")
    else:
        diff_cols = [col for col in ["id", "fecha_actualizacion", "estado", "entidad", "titulo", "ficha_detectada", "enlace"] if col in differences_df.columns]
        for profile in PROFILE_KEYS:
            diff_cols.extend(
                [
                    flexible_output_col("ficha_detectada", profile),
                    flexible_output_col("nombre_ficha", profile),
                    flexible_output_col("score_ficha", profile),
                    flexible_output_col("confianza_ficha", profile),
                    flexible_output_col("posible_ficha", profile),
                ]
            )
        diff_cols = [col for col in diff_cols if col in differences_df.columns]
        st.dataframe(
            differences_df[diff_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "enlace": st.column_config.LinkColumn("Enlace acto", display_text="Abrir acto"),
                **{
                    col: st.column_config.NumberColumn(col, format="%.2f")
                    for col in diff_cols
                    if col.startswith("score_ficha_")
                },
            },
        )
