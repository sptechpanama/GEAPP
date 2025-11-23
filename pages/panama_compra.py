"""Vista PanamáCompra para GE FinApp."""

# pages/visualizador.py
import os
import re
import sqlite3
import time
from pathlib import Path
from collections import defaultdict
from typing import Any
import streamlit as st
import pandas as pd
import altair as alt
import uuid
import unicodedata
from datetime import date, timedelta, datetime, timezone
from openai import OpenAI

from core.config import DB_PATH
from core.panamacompra_tops import (
    SUPPLIER_TOP_CONFIG,
    SUPPLIER_TOP_DEFAULT_ROWS,
    TOPS_EXCEL_PATH,
    TOPS_EXCEL_FALLBACK,
    TOPS_METADATA_SHEET,
    sheet_name_for_top,
)
from sheets import get_client, read_worksheet
from services.panamacompra_drive import (
    ensure_drive_criterios_tecnicos,
    ensure_drive_fichas_ctni,
    ensure_drive_oferentes_catalogos,
    ensure_drive_tops_excel,
    ensure_local_panamacompra_db,
)


ROW_ID_COL = "__row__"
CHECKBOX_FLAG_NAMES = {
    "prioritario",
    "prioritarios",
    "descartar",
    "descarte",
}
TRUE_VALUES = {"true", "1", "si", "sí", "yes", "y", "t", "x", "on"}
DEFAULT_DATE_START = date(2024, 1, 1)
DATE_COLUMN_KEYWORDS = ("fecha", "date", "dia", "día", "time", "hora", "timestamp")
SUMMARY_TAB_LABEL = "Resumen general"
CHAT_HISTORY_KEY = "analysis_chat_history"
DB_PANEL_EXPANDED_KEY = "pc_db_section_open"
ANALYSIS_PANEL_EXPANDED_KEY = "pc_analysis_section_open"


def _default_date_range() -> tuple[date, date]:
    today = date.today()
    if today < DEFAULT_DATE_START:
        return (today, today)
    return (DEFAULT_DATE_START, today)

# Asegura que los archivos críticos locales estén sincronizados antes de usarlos.
TOPS_OUTPUT_FILE = TOPS_EXCEL_PATH
TOPS_FALLBACK_FILE = TOPS_EXCEL_FALLBACK
ensure_local_panamacompra_db()
LOCAL_FICHAS_CTNI = ensure_drive_fichas_ctni()
LOCAL_CRITERIOS_TECNICOS = ensure_drive_criterios_tecnicos()
LOCAL_OFERENTES_CATALOGOS = ensure_drive_oferentes_catalogos()
ensure_drive_tops_excel(TOPS_OUTPUT_FILE)


def _tops_cache_signature() -> tuple[tuple[str, int, int], ...]:
    signature: list[tuple[str, int, int]] = []
    for path in (TOPS_OUTPUT_FILE, TOPS_FALLBACK_FILE):
        if not path:
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        signature.append((str(path), int(stat.st_mtime_ns), stat.st_size))
    return tuple(signature)




def _format_currency(value: str | float | None) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value) if value not in (None, "", "None") else "-"


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        if isinstance(value, str) and not value.strip():
            return 0
        if isinstance(value, str):
            return int(float(value.replace(",", "")))
        return int(float(value))
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        if isinstance(value, str) and not value.strip():
            return 0.0
        if isinstance(value, str):
            return float(value.replace(",", ""))
        return float(value)
    except Exception:
        return 0.0

def _render_summary_table(rows: list[tuple[str, str]]) -> None:
    if not rows:
        st.info("Sin datos de resumen.")
        return
    df = pd.DataFrame(rows, columns=["Metrica", "Valor"])
    st.dataframe(df, hide_index=True, use_container_width=True)

def _render_precomputed_summary(summary: dict[str, Any]) -> None:
    if not summary:
        st.info("El archivo precalculado no contiene metadatos de resumen.")
        return

    def _fmt_range(start_key: str, end_key: str) -> str:
        start_val = summary.get(start_key) or "-"
        end_val = summary.get(end_key) or "-"
        return f"{start_val} - {end_val}"

    rows = [
        ("Periodo", summary.get("period_label") or summary.get("Periodo") or summary.get("period_id", "-")),
        ("Rango configurado", _fmt_range("fecha_inicio", "fecha_fin")),
        ("Rango con datos", _fmt_range("fecha_min_data", "fecha_max_data")),
        ("Total adjudicaciones", f"{_safe_int(summary.get('total_actos')):,}"),
        ("Monto total adjudicado", _format_currency(summary.get("total_monto"))),
        ("Actos con ficha", f"{_safe_int(summary.get('actos_con_ficha')):,}"),
        ("Monto con ficha", _format_currency(summary.get("monto_con_ficha"))),
        ("Actos sin ficha", f"{_safe_int(summary.get('actos_sin_ficha')):,}"),
        ("Monto sin ficha", _format_currency(summary.get("monto_sin_ficha"))),
        ("Actos CT sin RS", f"{_safe_int(summary.get('actos_ct_sin_rs')):,}"),
        ("Monto CT sin RS", _format_currency(summary.get("monto_ct_sin_rs"))),
        ("Proveedores distintos", f"{_safe_int(summary.get('proveedores_distintos')):,}"),
        ("Entidades distintas", f"{_safe_int(summary.get('entidades_distintas')):,}"),
        ("Fichas distintas", f"{_safe_int(summary.get('fichas_distintas')):,}"),
        (
            "Promedio de participantes",
            f"{_safe_float(summary.get('participantes_promedio')):,.2f}",
        ),
        ("Base utilizada", summary.get("db_path") or "-"),
        ("Archivo origen", summary.get("archivo") or summary.get("archivo_local") or "-"),
    ]
    _render_summary_table(rows)


def _legacy_metadata_to_row(metadata: dict[str, Any]) -> dict[str, Any]:
    if not metadata:
        return {}
    row = {
        "period_id": metadata.get("period_id", "global"),
        "period_label": metadata.get("period_label", metadata.get("label", "Todo el periodo")),
        "fecha_inicio": metadata.get("fecha_inicio", metadata.get("fecha_min", "")),
        "fecha_fin": metadata.get("fecha_fin", metadata.get("fecha_max", "")),
        "fecha_min_data": metadata.get("fecha_min", ""),
        "fecha_max_data": metadata.get("fecha_max", ""),
        "total_actos": _safe_int(metadata.get("total_adjudicaciones")),
        "total_monto": _safe_float(metadata.get("total_monto")),
        "actos_con_ficha": _safe_int(metadata.get("actos_con_ficha")),
        "actos_sin_ficha": _safe_int(metadata.get("actos_sin_ficha")),
        "monto_con_ficha": _safe_float(metadata.get("monto_con_ficha")),
        "monto_sin_ficha": _safe_float(metadata.get("monto_sin_ficha")),
        "actos_ct_sin_rs": _safe_int(metadata.get("actos_ct_sin_rs")),
        "monto_ct_sin_rs": _safe_float(metadata.get("monto_ct_sin_rs")),
        "proveedores_distintos": _safe_int(metadata.get("proveedores_distintos")),
        "entidades_distintas": _safe_int(metadata.get("entidades_distintas")),
        "fichas_distintas": _safe_int(metadata.get("fichas_distintas")),
        "participantes_promedio": _safe_float(metadata.get("participantes_promedio")),
        "generated_at": metadata.get("generated_at", metadata.get("generated")),
        "db_path": metadata.get("db_path", ""),
        "fichas_path": metadata.get("fichas_path", ""),
        "criterios_path": metadata.get("criterios_path", ""),
        "oferentes_path": metadata.get("oferentes_path", ""),
        "archivo": metadata.get("archivo", ""),
        "has_data": _safe_int(metadata.get("total_adjudicaciones")) > 0,
    }
    return row


def _format_period_option(record: dict[str, Any]) -> str:
    label = record.get("period_label") or record.get("Periodo") or record.get("period_id") or "Periodo"
    start = record.get("fecha_inicio") or record.get("fecha_min_data") or "?"
    end = record.get("fecha_fin") or record.get("fecha_max_data") or "?"
    suffix = "" if record.get("has_data", True) else " - sin datos"
    return f"{label} ({start} - {end}){suffix}"


def _filter_precomputed_by_period(df: pd.DataFrame | None, period_id: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if "period_id" not in df.columns:
        return df.copy()
    return df[df["period_id"] == period_id].copy()

def _render_runtime_summary(
    filtered_df: pd.DataFrame,
    start_ts: datetime,
    end_ts: datetime,
    supplier_meta: dict[str, dict[str, bool]],
) -> None:
    if filtered_df.empty:
        st.info("No hay adjudicaciones en el rango seleccionado.")
        return
    mask_ct = filtered_df["tiene_ct"]
    supplier_registro = filtered_df["supplier_key"].map(
        lambda key: supplier_meta.get(key, {}).get("has_registro", False)
    )
    mask_ct_sin_rs = mask_ct & ~supplier_registro
    fichas_distintas = (
        filtered_df["ct_label"]
        .replace("", pd.NA)
        .dropna()
        .nunique()
    )
    rows = [
        ("Total adjudicaciones", f"{len(filtered_df):,}"),
        ("Monto total adjudicado", _format_currency(filtered_df["precio_referencia"].sum())),
        ("Actos con ficha", f"{int(mask_ct.sum()):,}"),
        ("Actos sin ficha", f"{int(len(filtered_df) - mask_ct.sum()):,}"),
        ("Actos CT sin RS", f"{int(mask_ct_sin_rs.sum()):,}"),
        (
            "Monto CT sin RS",
            _format_currency(filtered_df.loc[mask_ct_sin_rs, "precio_referencia"].sum()),
        ),
        ("Proveedores distintos", f"{filtered_df['supplier_key'].nunique():,}"),
        ("Fichas distintas", f"{fichas_distintas:,}"),
        ("Rango aplicado", f"{start_ts.date()}  →  {end_ts.date()}"),
    ]
    _render_summary_table(rows)


def _render_analysis_chatbot() -> None:
    st.subheader("Asistente GPT para análisis")
    db_path = _preferred_db_path()
    try:
        chat_data = load_analysis_chat_dataframes(
            db_path,
            LOCAL_FICHAS_CTNI,
            LOCAL_CRITERIOS_TECNICOS,
            LOCAL_OFERENTES_CATALOGOS,
        )
    except Exception as exc:
        st.error(f"No se pudieron cargar los datos para el chat: {exc}")
        return
    api_key = os.getenv("OPENAI_API_KEY") or st.secrets.get("OPENAI_API_KEY")
    if not api_key:
        st.info("Configura la variable de entorno OPENAI_API_KEY para usar el asistente.")
        return
    history: list[dict[str, str]] = st.session_state.setdefault(CHAT_HISTORY_KEY, [])
    for message in history:
        st.chat_message(message["role"]).write(message["content"])
    user_prompt = st.chat_input("Haz una pregunta sobre los actos, fichas o oferentes.")
    if not user_prompt:
        return
    st.chat_message("user").write(user_prompt)
    history.append({"role": "user", "content": user_prompt})
    with st.spinner("Consultando GPT..."):
        answer = _answer_analysis_question(user_prompt, chat_data, api_key)
    st.chat_message("assistant").write(answer)
    history.append({"role": "assistant", "content": answer})


def _render_ct_without_reg_chart_section(
    df: pd.DataFrame,
    supplier_meta: dict[str, dict[str, bool]],
    *,
    show_controls: bool,
    key_prefix: str,
) -> None:
    if df is None or df.empty or "fecha_referencia" not in df.columns:
        st.info("No hay adjudicaciones disponibles para graficar actos con ficha sin registro sanitario.")
        return

    base_df = df.copy()
    min_ts = base_df["fecha_referencia"].min()
    max_ts = base_df["fecha_referencia"].max()
    if pd.isna(min_ts) or pd.isna(max_ts):
        st.info("No se detectaron fechas válidas para la gráfica.")
        return

    min_date = min_ts.date()
    max_date = max_ts.date()
    default_start = max(min_date, date(2025, 1, 1))
    start_range = default_start
    end_range = max_date
    apply_date_filter = False

    if show_controls:
        date_input = st.date_input(
            "Rango de adjudicación para la gráfica CT sin RS",
            value=(default_start, max_date),
            min_value=min_date,
            max_value=max_date,
            key=f"{key_prefix}_ct_trend_range",
        )
        if isinstance(date_input, (tuple, list)) and len(date_input) == 2:
            start_range, end_range = date_input
            apply_date_filter = True
        elif isinstance(date_input, date):
            start_range = end_range = date_input
            apply_date_filter = True
    else:
        st.caption("La gráfica usa el mismo rango de fechas seleccionado en los filtros superiores.")

    if apply_date_filter and isinstance(start_range, date) and isinstance(end_range, date):
        start_ts = datetime.combine(start_range, datetime.min.time())
        end_ts = datetime.combine(end_range, datetime.max.time())
        filtered_df = _filter_awards_by_range(
            base_df,
            start_ts.isoformat(),
            end_ts.isoformat(),
        )
    else:
        filtered_df = base_df

    if filtered_df.empty:
        st.info("En el rango seleccionado no se registran adjudicaciones.")
        return

    subset = filtered_df[
        filtered_df["tiene_ct"]
        & ~filtered_df["supplier_key"].map(lambda key: supplier_meta.get(key, {}).get("has_registro", False))
    ].copy()
    if subset.empty:
        st.info("En el rango seleccionado no se registran actos con ficha técnica y sin registro sanitario.")
        return

    subset["fecha_dia"] = subset["fecha_referencia"].dt.floor("D")
    trend_df = (
        subset.groupby("fecha_dia", as_index=False)
        .agg(
            monto_total=("precio_referencia", "sum"),
            actos=("supplier_key", "size"),
        )
        .sort_values("fecha_dia")
    )
    trend_df["fecha_dia"] = pd.to_datetime(trend_df["fecha_dia"])
    trend_df["monto_promedio"] = trend_df["monto_total"].rolling(window=7, min_periods=1).mean()
    trend_df["actos_promedio"] = trend_df["actos"].rolling(window=7, min_periods=1).mean()

    base_chart = alt.Chart(trend_df).encode(
        x=alt.X("fecha_dia:T", title="Fecha de adjudicación"),
        tooltip=[
            alt.Tooltip("fecha_dia:T", title="Fecha"),
            alt.Tooltip("monto_total:Q", title="Monto total", format=",.2f"),
            alt.Tooltip("monto_promedio:Q", title="Monto promedio (7d)", format=",.2f"),
            alt.Tooltip("actos:Q", title="Actos"),
            alt.Tooltip("actos_promedio:Q", title="Actos promedio (7d)", format=",.2f"),
        ],
    )
    amount_area = base_chart.mark_area(color="#2a9d8f", opacity=0.35).encode(
        y=alt.Y("monto_promedio:Q", title="Monto promedio (B/.)"),
    )
    count_line = base_chart.mark_line(color="#e76f51", opacity=0.9).encode(
        y=alt.Y("actos_promedio:Q", title="Cantidad de actos (promedio)"),
    )
    ct_trend_chart = (
        alt.layer(amount_area, count_line)
        .resolve_scale(y="independent")
        .properties(
            height=320,
            title="Evolución de actos con ficha técnica sin registro sanitario",
        )
    )
    st.altair_chart(ct_trend_chart, use_container_width=True)

def _apply_search_filter(df: pd.DataFrame, search_text: str) -> pd.DataFrame:
    if not search_text:
        return df
    term = search_text.strip().lower()
    if not term:
        return df
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        try:
            series = df[col].astype(str).str.lower()
        except Exception:
            series = df[col].map(lambda v: str(v).lower() if pd.notna(v) else "")
        mask |= series.str.contains(term, na=False)
    return df[mask]

@st.cache_data(ttl=300)
def load_precomputed_top_tables(signature: tuple[tuple[str, int, int], ...]) -> dict[str, pd.DataFrame]:
    """Carga los tops precomputados en Excel si estan disponibles."""
    if not signature:
        return {}

    xls = None
    selected_path: Path | None = None
    for path_str, _, _ in signature:
        path = Path(path_str)
        try:
            xls = pd.ExcelFile(path)
            selected_path = path
            break
        except Exception:
            continue
    if xls is None or selected_path is None:
        return {}

    tables: dict[str, pd.DataFrame] = {}
    for cfg in SUPPLIER_TOP_CONFIG:
        sheet_name = sheet_name_for_top(cfg['key'])
        if sheet_name not in xls.sheet_names:
            continue
        try:
            tables[cfg['key']] = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception:
            continue

    if TOPS_METADATA_SHEET in xls.sheet_names:
        try:
            meta_df = pd.read_excel(xls, sheet_name=TOPS_METADATA_SHEET)
            if not meta_df.empty and {"period_id", "period_label"}.issubset(meta_df.columns):
                meta_copy = meta_df.copy()
                meta_copy["archivo"] = str(selected_path)
                tables["__metadata_table__"] = meta_copy
                metadata: dict[str, str] = {}
                for key in ("generated_at", "db_path", "fichas_path", "criterios_path", "oferentes_path"):
                    if key in meta_copy.columns:
                        series = meta_copy[key].dropna()
                        if not series.empty:
                            metadata[key] = str(series.iloc[0])
                metadata["archivo"] = str(selected_path)
                tables["__metadata__"] = metadata
            elif not meta_df.empty and meta_df.shape[1] >= 2:
                metadata = dict(zip(meta_df.iloc[:, 0], meta_df.iloc[:, 1]))
                metadata.setdefault("archivo", str(selected_path))
                tables["__metadata__"] = metadata
        except Exception:
            pass
    return tables

def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return normalized.strip()


def _normalize_supplier_key(value: str | None) -> str:
    base = _normalize_text(value).upper()
    return re.sub(r"[^A-Z0-9]+", "", base)


def _select_supplier_name(row: pd.Series) -> str:
    for col in ("nombre_comercial", "razon_social"):
        value = str(row.get(col) or "").strip()
        if value:
            return value
    unidad = str(row.get("unidad_solic", "")).strip()
    return unidad or "Proveedor sin nombre"


def _detect_ct_flag(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = _normalize_text(text).lower()
    if not normalized or "no detect" in normalized or normalized in {"no", "sin ficha", "sin dato"}:
        return False
    return bool(re.search(r"\d", text))


def _extract_ficha_label(value: str | None) -> str:
    if not _detect_ct_flag(value):
        return "Sin ficha detectada"
    text = str(value or "").strip()
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    text = text.replace(", ,", ",").strip(",; ")
    return text or "Ficha detectada"


def _normalize_ct_code(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"\d+(\.0+)?", text):
        try:
            text = str(int(float(text)))
        except Exception:
            text = text.split(".", 1)[0]
    return text


def _normalize_ct_label(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("*", "")
    text = re.sub(r"[^A-Z0-9/.-]", "", text)
    return text.strip()


def _extract_ct_candidates(value: str | None) -> list[str]:
    tokens = re.findall(r"[A-Z0-9/.-]+", str(value or "").upper())
    candidates: list[str] = []
    for token in tokens:
        normalized = _normalize_ct_label(token)
        if normalized:
            candidates.append(normalized)
    return candidates


def _match_known_ct_code(label: str, known_codes: set[str]) -> str:
    candidates = _extract_ct_candidates(label)
    for candidate in candidates:
        if candidate in known_codes:
            return candidate
    return candidates[0] if candidates else ""


def _last_non_empty(values: pd.Series) -> str:
    for raw in reversed(values.tolist()):
        text = str(raw or "").strip()
        if text:
            return text
    return ""


def _yes_no(value: bool | str | int) -> str:
    return "Sí" if bool(value) else "No"


@st.cache_data(ttl=600)
def load_supplier_awards_df(db_path: str | None) -> pd.DataFrame | None:
    if not db_path:
        return None
    db_path = str(db_path)
    path = Path(db_path)
    if not path.exists() or path.stat().st_size == 0:
        return None

    query = """
        SELECT
            razon_social,
            nombre_comercial,
            precio_referencia,
            fecha_adjudicacion,
            publicacion,
            fecha_actualizacion,
            ficha_detectada,
            num_participantes,
            estado
        FROM actos_publicos
        WHERE estado = 'Adjudicado'
    """
    try:
        with _connect_sqlite(db_path) as conn:
            df = pd.read_sql_query(query, conn)
    except Exception:
        return None

    if df.empty:
        return df

    for col in ("fecha_adjudicacion", "publicacion", "fecha_actualizacion"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["fecha_referencia"] = (
        df["fecha_adjudicacion"]
        .combine_first(df["publicacion"])
        .combine_first(df["fecha_actualizacion"])
    )
    df = df[df["fecha_referencia"].notna()].copy()
    df["fecha_referencia"] = df["fecha_referencia"].dt.tz_localize(None)
    df["precio_referencia"] = pd.to_numeric(df["precio_referencia"], errors="coerce").fillna(0.0)
    df["num_participantes"] = (
        pd.to_numeric(df["num_participantes"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["supplier_name"] = df.apply(_select_supplier_name, axis=1)
    df["supplier_name"] = df["supplier_name"].astype(str).str.strip()
    df = df[df["supplier_name"].astype(bool)]
    df["supplier_key"] = df["supplier_name"].map(_normalize_supplier_key)
    df = df[df["supplier_key"].astype(bool)].copy()
    df["tiene_ct"] = df["ficha_detectada"].map(_detect_ct_flag)
    df["ct_label"] = df["ficha_detectada"].map(_extract_ficha_label)
    return df.reset_index(drop=True)


@st.cache_data(ttl=3600)
def load_oferente_metadata(
    file_path: Path | None,
) -> tuple[dict[str, dict[str, bool]], dict[str, int], dict[str, str]]:
    if not file_path:
        return {}, {}, {}
    path = Path(file_path)
    if not path.exists():
        return {}, {}, {}
    try:
        df = pd.read_excel(path)
    except Exception:
        return {}, {}, {}
    df = _clean_drive_dataframe(df)
    if df.empty:
        return {}, {}, {}

    normalized_cols = {
        col: _normalize_text(col).lower()
        for col in df.columns
    }
    name_col = next(
        (col for col, norm in normalized_cols.items() if "oferente" in norm or "proveedor" in norm),
        None,
    )
    reg_col = next(
        (col for col, norm in normalized_cols.items() if "reg" in norm and "san" in norm),
        None,
    )
    ficha_col = next(
        (col for col, norm in normalized_cols.items() if "ficha" in norm and "ctni" in norm),
        None,
    )
    crit_col = next(
        (col for col, norm in normalized_cols.items() if "criterio" in norm),
        None,
    )
    ct_name_col = next(
        (col for col, norm in normalized_cols.items() if "nombre" in norm and "gener" in norm),
        None,
    )
    if not name_col:
        return {}, {}, {}

    metadata: dict[str, dict[str, bool]] = {}
    ct_suppliers: dict[str, set[str]] = defaultdict(set)
    ct_name_lookup: dict[str, str] = {}
    for _, row in df.iterrows():
        supplier = str(row.get(name_col) or "").strip()
        if not supplier:
            continue
        key = _normalize_supplier_key(supplier)
        if not key:
            continue
        meta = metadata.setdefault(key, {"has_registro": False, "has_ct": False})
        if reg_col:
            reg_value = str(row.get(reg_col) or "").strip()
            if reg_value:
                meta["has_registro"] = True
        norm_label = ""
        if ficha_col:
            ct_value = _normalize_ct_code(row.get(ficha_col))
            norm_label = _normalize_ct_label(ct_value)
        if not norm_label and crit_col:
            crit_value = str(row.get(crit_col) or "").strip()
            if crit_value:
                norm_label = _normalize_ct_label(_extract_ficha_label(crit_value))
        if norm_label:
            meta["has_ct"] = True
            meta.setdefault("ct_labels", set()).add(norm_label)
            ct_suppliers[norm_label].add(key)
            if ct_name_col:
                label_name = str(row.get(ct_name_col) or "").strip()
                if label_name:
                    ct_name_lookup.setdefault(norm_label, label_name)

    for meta in metadata.values():
        if "ct_labels" in meta:
            meta["ct_labels"] = tuple(sorted(meta["ct_labels"]))
    ct_stats = {label: len(keys) for label, keys in ct_suppliers.items()}
    return metadata, ct_stats, ct_name_lookup


def _compute_supplier_ranking(
    df: pd.DataFrame,
    *,
    require_ct: bool,
    require_registro: bool | None,
    metric: str,
    metadata: dict[str, dict[str, bool]],
    ct_stats: dict[str, int],
) -> pd.DataFrame:
    subset = df[df["tiene_ct"] == require_ct]
    if subset.empty:
        return pd.DataFrame()

    if require_registro is not None:
        subset = subset[
            subset["supplier_key"].map(
                lambda key: metadata.get(key, {}).get("has_registro", False)
            )
            == require_registro
        ]
    if subset.empty:
        return pd.DataFrame()

    grouped = (
        subset.sort_values("fecha_referencia")
        .groupby(["supplier_key", "supplier_name"], as_index=False)
        .agg(
            actos=("supplier_key", "size"),
            monto=("precio_referencia", "sum"),
            participantes_prom=("num_participantes", "mean"),
            participantes_max=("num_participantes", "max"),
            ultima_ficha=("ct_label", _last_non_empty),
        )
    )
    grouped["Monto adjudicado"] = grouped["monto"].round(2)
    grouped["Actos ganados"] = grouped["actos"]
    grouped["Participantes promedio"] = grouped["participantes_prom"].round(2)
    grouped["Participantes máx."] = grouped["participantes_max"].fillna(0).astype(int)
    grouped["Ficha / Criterio más reciente"] = grouped["ultima_ficha"].replace("", "Sin ficha registrada")
    grouped["_has_registro"] = grouped["supplier_key"].map(
        lambda key: metadata.get(key, {}).get("has_registro", False)
    )
    grouped["Precio promedio acto"] = (
        grouped["Monto adjudicado"] / grouped["Actos ganados"].replace(0, pd.NA)
    ).fillna(0).round(2)
    if require_registro is not None:
        grouped = grouped[grouped["_has_registro"] == require_registro]
    if grouped.empty:
        return pd.DataFrame()

    grouped["Tiene CT"] = grouped["supplier_key"].map(lambda _: require_ct)
    grouped["Tiene Registro Sanitario"] = grouped["_has_registro"]
    known_ct_codes = set(ct_stats.keys())
    grouped["_ct_code"] = grouped["Ficha / Criterio más reciente"].map(
        lambda label: _match_known_ct_code(label, known_ct_codes)
    )
    grouped["Oferentes con esta ficha"] = grouped["_ct_code"].map(lambda code: ct_stats.get(code, 0))

    if metric == "amount":
        grouped = grouped.sort_values(
            ["Monto adjudicado", "Actos ganados"],
            ascending=[False, False],
        )
    else:
        grouped = grouped.sort_values(
            ["Actos ganados", "Monto adjudicado"],
            ascending=[False, False],
        )

    grouped = grouped.copy()
    grouped["Proveedor"] = grouped["supplier_name"]
    grouped["Tiene CT"] = grouped["Tiene CT"].map(_yes_no)
    grouped["Tiene Registro Sanitario"] = grouped["Tiene Registro Sanitario"].map(_yes_no)
    grouped = grouped.drop(columns=["_has_registro", "_ct_code"])
    display_cols = [
        "Proveedor",
        "Actos ganados",
        "Monto adjudicado",
        "Participantes promedio",
        "Participantes máx.",
        "Ficha / Criterio más reciente",
        "Tiene CT",
        "Tiene Registro Sanitario",
    ]
    if require_ct:
        display_cols.insert(3, "Precio promedio acto")
        display_cols.append("Oferentes con esta ficha")
    return grouped[display_cols]

@st.cache_data(ttl=3600)
def load_ct_name_map(file_path: Path | None) -> dict[str, str]:
    if not file_path:
        return {}
    path = Path(file_path)
    if not path.exists():
        return {}
    try:
        df = pd.read_excel(path)
    except Exception:
        return {}
    df = _clean_drive_dataframe(df)
    if df.empty:
        return {}

    normalized_cols = {
        col: _normalize_text(col).lower()
        for col in df.columns
    }
    ficha_col = next(
        (col for col, norm in normalized_cols.items() if "ficha" in norm and "ctni" in norm),
        None,
    )
    nombre_col = next(
        (col for col, norm in normalized_cols.items() if "nombre" in norm and "gener" in norm),
        None,
    )
    if not ficha_col or not nombre_col:
        return {}

    name_map: dict[str, str] = {}
    for _, row in df.iterrows():
        criterio = _normalize_ct_code(row.get(ficha_col))
        nombre = str(row.get(nombre_col) or "").strip()
        norm = _normalize_ct_label(criterio)
        if norm and nombre:
            name_map.setdefault(norm, nombre)
    return name_map


@st.cache_data(ttl=300)
def _filter_awards_by_range(
    df: pd.DataFrame,
    start_iso: str,
    end_iso: str,
) -> pd.DataFrame:
    start = pd.to_datetime(start_iso)
    end = pd.to_datetime(end_iso)
    mask = (df["fecha_referencia"] >= start) & (df["fecha_referencia"] <= end)
    return df.loc[mask].copy()


def _compute_ct_ranking(
    df: pd.DataFrame,
    *,
    require_registro: bool | None,
    metric: str,
    metadata: dict[str, dict[str, bool]],
    ct_stats: dict[str, int],
    ct_names: dict[str, str],
) -> pd.DataFrame:
    subset = df[df["tiene_ct"]]
    subset = subset[subset["ct_label"].astype(str).str.strip().astype(bool)]
    if subset.empty:
        return pd.DataFrame()

    if require_registro is not None:
        subset = subset[
            subset["supplier_key"].map(
                lambda key: metadata.get(key, {}).get("has_registro", False)
            )
            == require_registro
        ]
    if subset.empty:
        return pd.DataFrame()

    known_codes = set(ct_stats.keys()) | set(ct_names.keys())
    rows: list[dict[str, Any]] = []
    for label, group in subset.groupby("ct_label"):
        norm_label = _match_known_ct_code(label, known_codes)
        display_label = norm_label or label
        total_actos = len(group.index)
        total_monto = group["precio_referencia"].sum()
        avg_price = (total_monto / total_actos) if total_actos else 0.0
        participantes_prom = group["num_participantes"].mean()
        participantes_max = group["num_participantes"].max()
        supplier_breakdown = (
            group.groupby("supplier_name", as_index=False)
            .agg(
                actos=("supplier_key", "size"),
                monto=("precio_referencia", "sum"),
            )
            .sort_values(["monto", "actos"], ascending=[False, False])
        )
        top_amount = supplier_breakdown.nlargest(3, ["monto", "actos"])
        top_amount_str = ", ".join(
            f"{row.supplier_name} (${row.monto:,.0f})" for _, row in top_amount.iterrows()
        )
        top_actos = supplier_breakdown.nlargest(3, ["actos", "monto"])
        top_actos_str = ", ".join(
            f"{row.supplier_name} ({int(row.actos)} actos)" for _, row in top_actos.iterrows()
        )
        rows.append(
            {
                "Ficha / Criterio": display_label,
                "Nombre de la ficha": ct_names.get(norm_label, ""),
                "Actos ganados": total_actos,
                "Monto adjudicado": round(total_monto, 2),
                "Precio promedio acto": round(avg_price, 2),
                "Participantes promedio": round(participantes_prom or 0, 2),
                "Participantes máx.": int(participantes_max or 0),
                "Oferentes en catálogo": ct_stats.get(norm_label, 0),
                "Top 3 por monto": top_amount_str or "Sin datos",
                "Top 3 por actos": top_actos_str or "Sin datos",
            }
        )

    ranking_df = pd.DataFrame(rows)
    if ranking_df.empty:
        return ranking_df

    if metric == "amount":
        ranking_df = ranking_df.sort_values(
            ["Monto adjudicado", "Actos ganados"], ascending=[False, False]
        )
    else:
        ranking_df = ranking_df.sort_values(
            ["Actos ganados", "Monto adjudicado"], ascending=[False, False]
        )
    return ranking_df


def render_precomputed_top_panel(precomputed: dict[str, pd.DataFrame]) -> bool:
    """Muestra los tops precomputados si existen."""
    available = [cfg for cfg in SUPPLIER_TOP_CONFIG if cfg["key"] in precomputed]
    if not available:
        return False

    metadata = precomputed.get("__metadata__", {})
    metadata_table = precomputed.get("__metadata_table__")
    st.markdown("### Tops precomputados de adjudicaciones")

    period_records: list[dict[str, Any]] = []
    if isinstance(metadata_table, pd.DataFrame) and not metadata_table.empty:
        for _, row in metadata_table.iterrows():
            record = {col: row[col] for col in metadata_table.columns}
            record["period_id"] = str(record.get("period_id") or f"period_{len(period_records)}")
            record["has_data"] = bool(record.get("has_data", True))
            period_records.append(record)
    elif metadata:
        legacy_row = _legacy_metadata_to_row(metadata)
        if legacy_row:
            period_records.append(legacy_row)

    if not period_records:
        period_records.append({"period_id": "global", "period_label": "Todo el periodo", "has_data": False})

    period_lookup = {rec["period_id"]: rec for rec in period_records}
    options = [rec["period_id"] for rec in period_records]
    default_period_id = next((rec["period_id"] for rec in period_records if rec.get("has_data")), options[0])
    default_index = options.index(default_period_id)
    selected_period_id = st.selectbox(
        "Periodo precalculado",
        options=options,
        format_func=lambda pid: _format_period_option(period_lookup[pid]),
        index=default_index,
        key="pc_precomputed_period",
    )
    selected_summary = period_lookup[selected_period_id]

    generated_at = metadata.get("generated_at") or selected_summary.get("generated_at") or "sin fecha"
    origen = metadata.get("db_path") or selected_summary.get("db_path") or "panamacompra.db"
    archivo = metadata.get("archivo") or selected_summary.get("archivo")
    extra = f" - Archivo: {archivo}" if archivo else ""
    st.caption(f"Generado: {generated_at} - Fuente: {origen}{extra}")

    filtered_tables: dict[str, pd.DataFrame] = {}
    max_rows = 0
    for cfg in available:
        df_period = _filter_precomputed_by_period(precomputed.get(cfg["key"]), selected_period_id)
        filtered_tables[cfg["key"]] = df_period
        max_rows = max(max_rows, len(df_period))

    with st.expander("Resumen del periodo seleccionado", expanded=False):
        _render_precomputed_summary(selected_summary)

    if max_rows <= 0:
        st.info("No se encontraron filas precalculadas para el periodo seleccionado.")
        return True

    slider_min = max(1, min(5, max_rows))
    slider_value = min(SUPPLIER_TOP_DEFAULT_ROWS, max_rows)
    top_n = st.slider(
        "Numero maximo de filas por listado",
        min_value=slider_min,
        max_value=max_rows,
        value=slider_value,
        key="precomputed_top_rows",
    )

    tabs = st.tabs([cfg["tab_label"] for cfg in SUPPLIER_TOP_CONFIG])
    for cfg, tab in zip(SUPPLIER_TOP_CONFIG, tabs):
        with tab:
            df_period = filtered_tables.get(cfg["key"])
            if df_period is None or df_period.empty:
                st.info("Sin datos precalculados para este ranking en el periodo elegido.")
                continue
            search_value = st.text_input(
                "Buscar en este top",
                key=f"precomputed_search_{cfg['key']}",
                placeholder="Proveedor, ficha, entidad, etc.",
            )
            df_filtered = _apply_search_filter(df_period, search_value)
            if df_filtered.empty:
                st.info("Sin filas que coincidan con el criterio de busqueda.")
                continue
            st.caption(cfg["title"])
            display_df = df_filtered.drop(columns=["period_id", "fecha_inicio", "fecha_fin"], errors="ignore")
            st.dataframe(
                display_df.head(top_n),
                hide_index=True,
                use_container_width=True,
            )
    return True

def render_supplier_top_panel() -> None:
    _render_analysis_chatbot()
    tops_signature = _tops_cache_signature()
    fresh_tables = load_precomputed_top_tables(tops_signature)
    if fresh_tables:
        st.session_state["precomputed_top_tables"] = fresh_tables
    precomputed_tables = fresh_tables or st.session_state.get("precomputed_top_tables", {})

    precomputed_rendered = render_precomputed_top_panel(precomputed_tables)

    db_path = _preferred_db_path()
    awards_df = load_supplier_awards_df(str(db_path) if db_path else None)
    if awards_df is None or awards_df.empty:
        st.info(
            "Aún no hay adjudicaciones sincronizadas en `panamacompra.db` para mostrar el top de proveedores."
        )
        if precomputed_rendered:
            st.info("No se pudo generar la gráfica CT sin RS porque la base local está vacía.")
        return

    if precomputed_rendered:
        supplier_meta, _, _ = load_oferente_metadata(LOCAL_OFERENTES_CATALOGOS)
        _render_ct_without_reg_chart_section(
            awards_df,
            supplier_meta,
            show_controls=True,
            key_prefix="precomputed_ct_trend",
        )
        return

    st.warning(
        "No se encontraron tops precomputados en data/tops ni en outputs/tops. "
        "Mostrando un cálculo en vivo temporal mientras ejecutas scripts/genera_tops_panamacompra.py "
        "o scripts/build_panamacompra_aggregates.py para la próxima sesión."
    )

    supplier_meta, ct_stats, ct_names_oferentes = load_oferente_metadata(LOCAL_OFERENTES_CATALOGOS)
    ct_names_fichas = load_ct_name_map(LOCAL_FICHAS_CTNI)
    ct_names = ct_names_oferentes.copy()
    ct_names.update(ct_names_fichas)
    min_date = awards_df["fecha_referencia"].min().date()
    max_date = awards_df["fecha_referencia"].max().date()
    default_start = max(min_date, max_date - timedelta(days=90))

    st.markdown("### 🏆 Tops de proveedores adjudicados")
    st.caption(
        "Explora rápidamente qué proveedores dominan las adjudicaciones, separados por si cuentan o no con ficha técnica."
    )
    date_input = st.date_input(
        "Rango de adjudicación",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
        key="supplier_top_date_range",
    )
    if isinstance(date_input, (list, tuple)) and len(date_input) == 2:
        start_date, end_date = date_input
    else:
        start_date = end_date = date_input

    if isinstance(start_date, date) and isinstance(end_date, date):
        start_ts = datetime.combine(start_date, datetime.min.time())
        end_ts = datetime.combine(end_date, datetime.max.time())
    else:
        start_ts = awards_df["fecha_referencia"].min()
        end_ts = awards_df["fecha_referencia"].max()

    filtered_df = _filter_awards_by_range(
        awards_df,
        start_ts.isoformat(),
        end_ts.isoformat(),
    )
    if filtered_df.empty:
        st.warning("No se registran adjudicaciones en el rango seleccionado.")
        return

    top_n = st.slider(
        "Numero maximo de filas por listado",
        min_value=5,
        max_value=100,
        value=min(SUPPLIER_TOP_DEFAULT_ROWS, 100),
        step=1,
        key="supplier_top_rows",
    )
    st.caption(f"El rango seleccionado contiene {len(filtered_df)} adjudicaciones únicas.")

    tab_labels = [SUMMARY_TAB_LABEL] + [cfg["tab_label"] for cfg in SUPPLIER_TOP_CONFIG]
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        _render_runtime_summary(filtered_df, start_ts, end_ts, supplier_meta)

    column_config = {
        "Monto adjudicado": st.column_config.NumberColumn(format="$%0.2f", help="Suma de precio de referencia adjudicado"),
        "Actos ganados": st.column_config.NumberColumn(format="%d"),
        "Precio promedio acto": st.column_config.NumberColumn(
            format="$%0.2f",
            help="Monto promedio adjudicado por acto",
        ),
        "Participantes promedio": st.column_config.NumberColumn(format="%.2f", help="Promedio de participantes reportados"),
        "Participantes máx.": st.column_config.NumberColumn(format="%d"),
        "Ficha / Criterio más reciente": st.column_config.TextColumn(help="Última referencia detectada en el bot"),
        "Tiene CT": st.column_config.TextColumn(),
        "Tiene Registro Sanitario": st.column_config.TextColumn(),
        "Oferentes con esta ficha": st.column_config.NumberColumn(
            format="%d",
            help="Cantidad de oferentes que en catálogo listan la misma ficha",
        ),
    }
    ct_column_config = {
        "Nombre de la ficha": st.column_config.TextColumn(),
        "Monto adjudicado": st.column_config.NumberColumn(format="$%0.2f"),
        "Actos ganados": st.column_config.NumberColumn(format="%d"),
        "Precio promedio acto": st.column_config.NumberColumn(format="$%0.2f"),
        "Participantes promedio": st.column_config.NumberColumn(format="%.2f"),
        "Participantes máx.": st.column_config.NumberColumn(format="%d"),
        "Oferentes en catálogo": st.column_config.NumberColumn(format="%d"),
        "Top 3 por monto": st.column_config.TextColumn(help="Empresas con mayor monto adjudicado"),
        "Top 3 por actos": st.column_config.TextColumn(help="Empresas con más actos adjudicados"),
        "oferentes_disponibles_por_CT": st.column_config.NumberColumn(
            format="%d",
            help="Cantidad de oferentes con certificado vigente en catálogo para esta ficha.",
        ),
        "dias_promedio_pub_a_adj_por_CT": st.column_config.NumberColumn(
            format="%.2f",
            help="Promedio de días entre publicación y adjudicación de actos con esta ficha.",
        ),
    }

    for cfg, tab in zip(SUPPLIER_TOP_CONFIG, tabs):
        with tab:
            if cfg.get("mode") == "ct":
                ranking = _compute_ct_ranking(
                    filtered_df,
                    require_registro=cfg.get("require_registro"),
                    metric=cfg["metric"],
                    metadata=supplier_meta,
                    ct_stats=ct_stats,
                    ct_names=ct_names,
                )
                current_config = ct_column_config
            else:
                ranking = _compute_supplier_ranking(
                    filtered_df,
                    require_ct=cfg["require_ct"],
                    require_registro=cfg.get("require_registro"),
                    metric=cfg["metric"],
                    metadata=supplier_meta,
                    ct_stats=ct_stats,
                )
                current_config = column_config
            if ranking.empty:
                st.info("Sin adjudicaciones disponibles para este subgrupo en el rango seleccionado.")
                continue

            search_value = st.text_input(
                "Buscar en este top",
                key=f"runtime_search_{cfg['key']}",
                placeholder="Proveedor, ficha, país, etc.",
            )
            ranking = _apply_search_filter(ranking, search_value)
            display_df = ranking.head(top_n)

            st.caption(cfg["title"])
            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                column_config=current_config,
            )

    _render_ct_without_reg_chart_section(
        filtered_df,
        supplier_meta,
        show_controls=False,
        key_prefix="runtime_ct_trend",
    )

    ct_without_reg = filtered_df[
        filtered_df["tiene_ct"]
        & ~filtered_df["supplier_key"].map(lambda key: supplier_meta.get(key, {}).get("has_registro", False))
    ].copy()
    if not ct_without_reg.empty:
        ct_without_reg["fecha_dia"] = ct_without_reg["fecha_referencia"].dt.floor("D")
        trend_df = (
            ct_without_reg.groupby("fecha_dia", as_index=False)
            .agg(
                monto_total=("precio_referencia", "sum"),
                actos=("supplier_key", "size"),
            )
            .sort_values("fecha_dia")
        )
        trend_df["fecha_dia"] = pd.to_datetime(trend_df["fecha_dia"])

        base_chart = alt.Chart(trend_df).encode(
            x=alt.X("fecha_dia:T", title="Fecha de adjudicación"),
            tooltip=[
                alt.Tooltip("fecha_dia:T", title="Fecha"),
                alt.Tooltip("monto_total:Q", title="Monto total", format=",.2f"),
                alt.Tooltip("actos:Q", title="Actos"),
            ],
        )
        amount_area = base_chart.mark_area(color="#2a9d8f", opacity=0.35).encode(
            y=alt.Y("monto_total:Q", title="Monto total (B/.)"),
        )
        count_line = base_chart.mark_line(color="#e76f51", opacity=0.9).encode(
            y=alt.Y("actos:Q", title="Cantidad de actos"),
        )
        ct_trend_chart = (
            alt.layer(amount_area, count_line)
            .resolve_scale(y="independent")
            .properties(
                height=320,
                title="Evolución de actos con ficha técnica sin registro sanitario",
            )
        )
        st.altair_chart(ct_trend_chart, use_container_width=True)
    else:
        st.info("En el rango seleccionado no se registran actos con ficha y sin registro sanitario.")

def _require_authentication() -> None:
    status = st.session_state.get("authentication_status")
    if status is True:
        st.session_state.setdefault("username", st.session_state.get("username"))
        return
    if status is False:
        st.error("Credenciales inválidas. Vuelve a la portada para iniciar sesión.")
    else:
        st.warning("Debes iniciar sesión para entrar.")

    # Redirige al home, igual que otras páginas protegidas del multipage.
    try:
        st.switch_page("Inicio.py")
    except Exception:
        st.stop()
    st.stop()

PC_STATE_WORKSHEET = "pc_state"
PC_CONFIG_WORKSHEET = "pc_config"
PC_MANUAL_SHEET_ID = "1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
PC_MANUAL_WORKSHEET = "pc_manual"
JOB_NAME_LABELS = {
    "clrir": "Cotizaciones Programadas",
    "clv": "Cotizaciones Abiertas",
    "rir1": "Licitaciones",
}
JOB_NAME_ORDER = ["clrir", "clv", "rir1"]
STATUS_BADGES = {
    "success": ("🟢", "Éxito"),
    "running": ("🟡", "En curso"),
    "failed": ("🔴", "Error"),
    "error": ("🔴", "Error"),
}

HEADER_ALIASES = {
    "request_id": {"request_id", "id", "uuid", "solicitud_id"},
    "timestamp": {"timestamp", "requested_at", "fecha", "fecha_solicitud", "created_at"},
    "job_name": {"job_name", "job", "bot", "proceso"},
    "job_label": {"job_label", "job_desc", "descripcion", "descripción"},
    "requested_by": {"requested_by", "user", "usuario", "solicitado_por"},
    "note": {"note", "nota", "comentario", "observacion", "observación"},
    "status": {"status", "estado"},
}

FALLBACK_DB_PATH = Path(r"C:\Users\rodri\GEAPP\panamacompra.db")


def _candidate_db_paths() -> list[Path]:
    candidates: list[Path] = []
    for raw in (DB_PATH, FALLBACK_DB_PATH):
        if not raw:
            continue
        try:
            candidate = Path(raw).expanduser()
        except Exception:
            continue
        if candidate in candidates:
            continue
        candidates.append(candidate)
    return candidates


def _preferred_db_path() -> Path | None:
    candidates = _candidate_db_paths()
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else None


def _quote_identifier(identifier: str) -> str:
    return f"\"{identifier.replace('\"', '\"\"')}\""


def _connect_sqlite(db_path: str):
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


@st.cache_data(ttl=300)
def list_sqlite_tables(db_path: str) -> list[str]:
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]


@st.cache_data(ttl=300)
def count_sqlite_rows(db_path: str, table_name: str) -> int:
    identifier = _quote_identifier(table_name)
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(f"SELECT COUNT(1) FROM {identifier}")
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


@st.cache_data(ttl=300)
def describe_sqlite_table(db_path: str, table_name: str) -> list[tuple[str, str]]:
    identifier = _quote_identifier(table_name)
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(f"PRAGMA table_info({identifier})")
        rows = cur.fetchall()
    return [(row[1], row[2]) for row in rows]


@st.cache_data(ttl=300)
def load_sqlite_preview(
    db_path: str,
    table_name: str,
    limit: int,
    search_text: str | None = None,
) -> pd.DataFrame:
    identifier = _quote_identifier(table_name)
    limit = max(1, int(limit))
    params: list[Any] = []
    base_query = f"SELECT * FROM {identifier}"

    cleaned_search = (search_text or "").strip()
    if cleaned_search:
        columns = describe_sqlite_table(db_path, table_name)
        if columns:
            normalized = cleaned_search.lower()
            pattern_raw = f"%{cleaned_search}%"
            pattern_lower = f"%{normalized}%"
            like_clauses: list[str] = []
            for col_name, col_type in columns:
                norm_type = (col_type or "").lower()
                identifier_col = _quote_identifier(col_name)
                if not norm_type:
                    like_clauses.append(f"{identifier_col} LIKE ?")
                    params.append(pattern_raw)
                elif any(token in norm_type for token in ("char", "text", "clob", "string")):
                    like_clauses.append(f"LOWER({identifier_col}) LIKE ?")
                    params.append(pattern_lower)
                elif any(token in norm_type for token in ("int", "dec", "num", "real", "double", "float")):
                    like_clauses.append(f"CAST({identifier_col} AS TEXT) LIKE ?")
                    params.append(pattern_raw)
            if like_clauses:
                where_clause = " OR ".join(like_clauses)
                base_query += f" WHERE {where_clause}"

    base_query += " LIMIT ?"
    params.append(limit)

    with _connect_sqlite(db_path) as conn:
        return pd.read_sql_query(base_query, conn, params=tuple(params))


@st.cache_data(ttl=300)
def load_excel_file(file_path: str) -> pd.DataFrame:
    return pd.read_excel(file_path)


@st.cache_data(ttl=1800)
def load_analysis_chat_dataframes(
    db_path: Path | None,
    fichas_path: Path | None,
    criterios_path: Path | None,
    oferentes_path: Path | None,
) -> dict[str, pd.DataFrame]:
    actos_df = load_supplier_awards_df(str(db_path) if db_path else None)
    if actos_df is None:
        actos_df = pd.DataFrame()

    def _try_read_excel(path: Path | None) -> pd.DataFrame:
        if not path:
            return pd.DataFrame()
        path = Path(path)
        if not path.exists():
            return pd.DataFrame()
        try:
            df_local = pd.read_excel(path)
        except Exception:
            return pd.DataFrame()
        return _clean_drive_dataframe(df_local)

    fichas_df = _try_read_excel(fichas_path)
    criterios_df = _try_read_excel(criterios_path)
    oferentes_df = _try_read_excel(oferentes_path)
    return {
        "actos": actos_df,
        "fichas": fichas_df,
        "criterios": criterios_df,
        "oferentes": oferentes_df,
    }


def _filter_df_for_terms(
    df: pd.DataFrame,
    terms: list[str],
    *,
    limit: int = 10,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = list(df.columns[: min(6, len(df.columns))])
    work_df = df[cols]
    if not terms:
        return work_df.head(limit)
    mask = pd.Series(False, index=work_df.index)
    for col in cols:
        try:
            series = work_df[col].astype(str).str.lower()
        except Exception:
            series = work_df[col].map(lambda v: str(v).lower() if pd.notna(v) else "")
        for term in terms:
            mask = mask | series.str.contains(term.lower(), na=False)
    return work_df[mask].head(limit)


def _build_chat_context(question: str, dataframes: dict[str, pd.DataFrame]) -> str:
    terms = [tok.lower() for tok in re.findall(r"\d{3,}", question or "")]
    context_parts: list[str] = []
    for name, df in dataframes.items():
        if df is None or df.empty:
            continue
        context_parts.append(
            f"Tabla {name}: {len(df)} filas, columnas principales: {', '.join(df.columns[:5])}"
        )
        snippet = _filter_df_for_terms(df, terms, limit=8)
        if not snippet.empty:
            snippet_text = snippet.to_string(index=False)
            context_parts.append(f"Ejemplos en {name}:\n{snippet_text}")
    context_text = "\n\n".join(context_parts)
    return context_text[-4000:] if len(context_text) > 4000 else context_text


def _answer_analysis_question(
    question: str,
    dataframes: dict[str, pd.DataFrame],
    api_key: str,
) -> str:
    context = _build_chat_context(question, dataframes) or "No se encontraron coincidencias directas."
    try:
        client = OpenAI(api_key=api_key)
    except Exception as exc:
        return f"No se pudo inicializar el cliente de OpenAI: {exc}"
    system_prompt = (
        "Eres un analista que responde en español usando exclusivamente los datos proporcionados. "
        "Si la respuesta no está en el contexto, indica qué información falta. "
        "Los dataframes disponibles son: actos (actos públicos adjudicados con campos como ficha_detectada, ct_label, "
        "proveedores y fechas), fichas_ctni (número de ficha y nombre genérico), criterios_tecnicos y oferentes_catalogos. "
        "Cuando piden detalles de una CT específica, usa las filas correspondientes del contexto."
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Contexto:\n{context}\n\nPregunta del usuario:\n{question}"},
    ]
    try:
        response = client.responses.create(
            model="gpt-4o-mini",
            input=messages,
            max_output_tokens=500,
        )
        return response.output_text
    except Exception as exc:
        return f"No se pudo obtener respuesta de GPT: {exc}"


def _filter_dataframe(
    df: pd.DataFrame,
    search_text: str | None,
    date_range: tuple[date, date] | None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    filtered = df
    if search_text:
        needle = str(search_text).strip().lower()
        if needle:
            mask = pd.Series(False, index=filtered.index)
            for col in filtered.columns:
                series = filtered[col]
                try:
                    text_values = series.astype(str).str.lower()
                except Exception:
                    text_values = series.map(
                        lambda v: str(v).lower() if pd.notna(v) else ""
                    )
                mask = mask | text_values.str.contains(needle, na=False)
            filtered = filtered[mask]

    if date_range:
        if isinstance(date_range, date):
            date_range = (date_range, date_range)
        if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
            start, end = date_range
        if isinstance(start, date) and isinstance(end, date):
            start_dt = datetime.combine(start, datetime.min.time())
            end_dt = datetime.combine(end, datetime.max.time())
            date_masks: list[pd.Series] = []
            for col in filtered.columns:
                series = filtered[col]
                dt_series = None

                if pd.api.types.is_datetime64_any_dtype(series):
                    dt_series = series
                else:
                    col_name = str(col).lower()
                    if any(token in col_name for token in DATE_COLUMN_KEYWORDS):
                        dt_series = pd.to_datetime(series, errors="coerce")

                if dt_series is None:
                    continue
                date_masks.append(dt_series.between(start_dt, end_dt, inclusive="both"))

            if date_masks:
                combined = date_masks[0]
                for extra in date_masks[1:]:
                    combined = combined | extra
                filtered = filtered[combined.fillna(False)]

    return filtered


def _clean_drive_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Remueve columnas auxiliares y filas de paginación de los Excel."""
    if df is None or df.empty:
        return df

    cols_to_drop = [
        col for col in df.columns if str(col).strip().lower().startswith("unnamed")
    ]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop, errors="ignore")

    def _looks_like_pagination(row: pd.Series) -> bool:
        values: list[int] = []
        for value in row:
            if pd.isna(value):
                continue
            text = str(value).strip()
            if not text:
                continue
            if text.endswith("..."):
                text = text.rstrip(".").strip()
            if not text.isdigit():
                return False
            values.append(int(text))
        if len(values) < 3:
            return False
        expected = list(range(values[0], values[0] + len(values)))
        return values == expected

    mask = df.apply(_looks_like_pagination, axis=1)
    if mask.any():
        df = df[~mask].reset_index(drop=True)

    return df


PC_CONFIG_OVERRIDE_TTL_SECONDS = 180
PC_CONFIG_NAME_ALIASES = ("name", "bot", "job", "proceso")
PC_CONFIG_DAYS_ALIASES = ("days", "dias", "días")
PC_CONFIG_TIMES_ALIASES = ("times", "horas", "hora", "hours")


def _normalize_job_key(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _resolve_config_column(df: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    if df is None or df.empty:
        return None
    col_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in aliases:
        key = str(candidate).strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _pc_config_overrides() -> dict[str, dict[str, str]]:
    overrides = st.session_state.setdefault("pc_config_overrides", {})
    now = time.time()
    stale_keys = [
        key
        for key, payload in list(overrides.items())
        if now - float(payload.get("ts", now)) > PC_CONFIG_OVERRIDE_TTL_SECONDS
    ]
    for key in stale_keys:
        overrides.pop(key, None)
    return overrides


def _sanitize_config_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _canonicalize_schedule_text(value: str) -> str:
    """
    Normaliza listas separadas por comas/semicolons a un formato consistente.
    Mantiene el orden de entrada y elimina espacios duplicados.
    """
    text = _sanitize_config_value(value)
    if not text:
        return ""
    parts = [p.strip() for p in re.split(r"[;,]+", text) if p.strip()]
    if not parts:
        return ""
    return ", ".join(parts)


def _apply_pc_config_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    name_col = _resolve_config_column(df, PC_CONFIG_NAME_ALIASES)
    if not name_col:
        return df

    overrides = _pc_config_overrides()
    if not overrides:
        return df

    work_df = df.copy()
    name_keys = work_df[name_col].astype(str).str.strip().str.lower()
    days_column = _resolve_config_column(work_df, PC_CONFIG_DAYS_ALIASES)
    times_column = _resolve_config_column(work_df, PC_CONFIG_TIMES_ALIASES)
    resolved_keys: list[str] = []

    for key, payload in list(overrides.items()):
        job_key = _normalize_job_key(key)
        if not job_key:
            resolved_keys.append(key)
            continue

        mask = name_keys == job_key
        if not mask.any():
            resolved_keys.append(key)
            continue

        row_index = mask[mask].index[0]

        if days_column and "days" in payload:
            desired_value = _canonicalize_schedule_text(payload["days"])
            work_df.at[row_index, days_column] = desired_value
            payload["days"] = desired_value

        if times_column and "times" in payload:
            desired_value = _canonicalize_schedule_text(payload["times"])
            work_df.at[row_index, times_column] = desired_value
            payload["times"] = desired_value

    for key in resolved_keys:
        overrides.pop(key, None)

    # Conserva metadatos como los encabezados originales.
    work_df.attrs = dict(df.attrs)
    return work_df


def _manual_sheet_id() -> str | None:
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    manual_id = app_cfg.get("PC_MANUAL_SHEET_ID") if isinstance(app_cfg, dict) else None
    manual_id = manual_id or PC_MANUAL_SHEET_ID
    return manual_id or None


def _pc_config_sheet_id() -> str | None:
    """Obtiene el Sheet ID donde vive pc_config, con fallbacks sensatos."""
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    sheet_id = None
    if isinstance(app_cfg, dict):
        sheet_id = (
            app_cfg.get("PC_CONFIG_SHEET_ID")
            or app_cfg.get("PC_MANUAL_SHEET_ID")
            or app_cfg.get("SHEET_ID")
        )

    return sheet_id or _manual_sheet_id()


def _current_user() -> str:
    for key in ("username", "user", "email", "correo", "name", "nombre"):
        value = st.session_state.get(key)
        if value:
            return str(value)
    return "desconocido"


def append_manual_request(job_name: str, job_label: str, note: str) -> bool:
    sheet_id = _manual_sheet_id()
    if not sheet_id:
        st.error("No hay hoja configurada para registrar ejecuciones manuales.")
        return False

    client = get_gc()
    try:
        sh = client.open_by_key(sheet_id)
    except Exception as exc:
        st.error(f"No se pudo abrir la hoja manual: {exc}")
        return False

    ws = None
    try:
        ws = sh.worksheet(PC_MANUAL_WORKSHEET)
    except Exception:
        try:
            ws = sh.sheet1
        except Exception:
            st.error("No encontramos la pestaña pc_manual en la hoja configurada.")
            return False

    headers = ws.row_values(1)
    cleaned_headers = [h.strip() for h in headers if h.strip()]
    if not cleaned_headers:
        cleaned_headers = ["request_id", "timestamp", "job_name", "job_label", "requested_by", "note"]
        ws.update("A1", [cleaned_headers])

    payload_map = {
        "request_id": uuid.uuid4().hex,
        "timestamp": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "job_name": job_name,
        "job_label": job_label,
        "requested_by": _current_user(),
        "note": note.strip(),
        "status": "pending",
    }

    def _value_for_header(header: str) -> str:
        normalized = header.strip().lower()
        for key, aliases in HEADER_ALIASES.items():
            if normalized == key or normalized in aliases:
                return payload_map.get(key, "")
        return payload_map.get(normalized, "")

    row = [_value_for_header(header) for header in cleaned_headers]
    status_idx = None
    for idx, header in enumerate(cleaned_headers, start=1):
        header_norm = header.strip().lower()
        if header_norm in {"status", "estado"}:
            status_idx = idx
            break

    try:
        append_result = ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as exc:
        st.error(f"No se pudo registrar la ejecución manual: {exc}")
        return False

    if status_idx:
        updated_range = ""
        try:
            updated_range = (
                append_result.get("updates", {}).get("updatedRange", "")
                if isinstance(append_result, dict)
                else ""
            )
        except AttributeError:
            updated_range = ""

        row_number = None
        if updated_range:
            try:
                range_bounds = updated_range.split("!")[-1]
                start_cell = range_bounds.split(":")[0]
                row_number = int("".join(filter(str.isdigit, start_cell)))
            except Exception:
                row_number = None

        if not row_number:
            try:
                row_number = len(ws.col_values(1))
            except Exception:
                row_number = None

        if row_number:
            try:
                ws.update_cell(row_number, status_idx, payload_map.get("status", "pending"))
            except Exception as exc:
                st.warning(f"No se pudo actualizar el estado 'pending' en la fila {row_number}: {exc}")

    return True


@st.cache_data(ttl=180)
def load_pc_state() -> pd.DataFrame:
    """Carga el estado de los jobs PanamáCompra desde Finanzas Operativas."""
    try:
        sheet_id = st.secrets["app"]["SHEET_ID"]
    except Exception:
        return pd.DataFrame()

    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_STATE_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    keep_cols = [
        col
        for col in (
            "job_name",
            "status",
            "started_at",
            "finished_at",
            "duration_display",
            "duration_seconds",
        )
        if col in df.columns
    ]

    if not keep_cols:
        return pd.DataFrame()

    data = df[keep_cols].copy()
    if "job_name" not in data.columns:
        return pd.DataFrame()

    data["job_name"] = data["job_name"].astype(str).str.strip()

    if "status" in data.columns:
        data["status"] = data["status"].astype(str).str.strip()
    else:
        data["status"] = ""
    if "started_at" in data.columns:
        data["__started_ts"] = pd.to_datetime(data["started_at"], errors="coerce")
    else:
        data["__started_ts"] = pd.NaT

    # Conserva el último registro por job.
    data = data.sort_values("__started_ts", ascending=False)
    data = data.drop_duplicates(subset=["job_name"], keep="first")

    order_map = {name: idx for idx, name in enumerate(JOB_NAME_ORDER)}
    data["__order"] = data["job_name"].str.lower().map(order_map).fillna(len(order_map))
    data = data.sort_values(["__order", "__started_ts"], ascending=[True, False]).reset_index(drop=True)
    return data.drop(columns=["__started_ts", "__order"], errors="ignore")


@st.cache_data(ttl=180)
def load_pc_config() -> pd.DataFrame:
    """Obtiene la configuración de programación (días/horas) desde la hoja pc_config."""
    sheet_id = _pc_config_sheet_id()
    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_CONFIG_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    original_columns = list(df.columns)
    df = df.rename(columns=lambda col: str(col).strip().lower())
    df.attrs["__original_columns__"] = original_columns
    return _apply_pc_config_overrides(df)


def _format_pc_datetime(value) -> str:
    if value is None or value == "":
        return "—"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "—"
    return ts.strftime("%d/%m %H:%M")


def _format_pc_duration(row: pd.Series) -> str:
    display = str(row.get("duration_display", "")).strip()
    if display:
        return display
    seconds = row.get("duration_seconds")
    try:
        return f"{float(seconds):.0f} s"
    except Exception:
        return "—"


def render_pc_state_cards(
    pc_state_df: pd.DataFrame | None,
    pc_config_df: pd.DataFrame | None,
    suffix: str | None = None,
) -> None:
    """Renderiza tarjetas discretas con el estado de los bots de PanamáCompra."""
    if pc_state_df is None or pc_state_df.empty:
        return

    st.markdown(
        """
<div style="margin-top:14px;margin-bottom:6px;padding:8px 16px;background:rgba(34,52,82,0.55);border-radius:8px;border:1px solid rgba(120,170,255,0.15);font-size:1.12rem;font-weight:600;color:#f4f6fb;letter-spacing:0.01em;">
  🔧 Resumen de últimas actualizaciones y controles
</div>
""",
        unsafe_allow_html=True,
    )
    rows = pc_state_df.drop(columns=[col for col in pc_state_df.columns if col.startswith("__")]).copy()

    config_map = {}
    name_col = None
    days_col = None
    times_col = None
    if pc_config_df is not None and not pc_config_df.empty:
        name_col = _resolve_config_column(pc_config_df, PC_CONFIG_NAME_ALIASES)
        if name_col:
            tmp = pc_config_df.copy()
            tmp["__pc_key__"] = tmp[name_col].astype(str).str.strip().str.lower()
            days_col = _resolve_config_column(tmp, PC_CONFIG_DAYS_ALIASES)
            times_col = _resolve_config_column(tmp, PC_CONFIG_TIMES_ALIASES)
            for _, cfg_row in tmp.iterrows():
                config_map[cfg_row["__pc_key__"]] = cfg_row

    buffer = st.session_state.setdefault("pc_config_buffer", {})
    missing_config_jobs: set[str] = set()

    for start in range(0, len(rows), 3):
        chunk = rows.iloc[start : start + 3]
        cols = st.columns(len(chunk))
        for col_widget, (_, row) in zip(cols, chunk.iterrows()):
            job_raw = str(row.get("job_name", "")).strip()
            job_label = JOB_NAME_LABELS.get(job_raw.lower(), job_raw or "Job sin nombre")
            status_key = str(row.get("status", "")).strip().lower()
            icon, status_label = STATUS_BADGES.get(status_key, ("⚪", status_key.capitalize() or "Sin dato"))
            started_text = _format_pc_datetime(row.get("started_at"))
            duration_text = _format_pc_duration(row)

            job_key = job_raw.strip().lower()
            cfg = config_map.get(job_key) if config_map else None
            if cfg is not None:
                cfg_key = _sanitize_config_value(cfg.get("__pc_key__", job_key))
                job_key = cfg_key or job_key
            cfg_name_value = (
                _sanitize_config_value(cfg.get(name_col, job_raw))
                if (cfg is not None and name_col)
                else job_raw
            )
            key_suffix = f"_{suffix}" if suffix else ""
            days_key = f"pc_days_{job_raw.lower()}{key_suffix}"
            times_key = f"pc_times_{job_raw.lower()}{key_suffix}"
            days_value = (
                _canonicalize_schedule_text(cfg.get(days_col, ""))
                if cfg is not None and days_col
                else ""
            )
            times_value = (
                _canonicalize_schedule_text(cfg.get(times_col, ""))
                if cfg is not None and times_col
                else ""
            )

            card = f"""
<div style="border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:12px 14px;margin-top:8px;background-color:rgba(17,20,24,0.35);">
  <div style="font-weight:600;font-size:0.95rem;">{job_label}</div>
  <div style="font-size:0.8rem;color:#9aa0a6;margin-top:4px;">{icon} {status_label}</div>
  <div style="font-size:0.76rem;color:#d6d8dc;margin-top:10px;line-height:1.45;">
    <div>Fecha: {started_text}</div>
    <div>Duración: {duration_text}</div>
  </div>
</div>
"""
            col_widget.markdown(card, unsafe_allow_html=True)

            manual_col = col_widget.container()
            if manual_col.button(
                "▶ Actualización manual",
                key=f"pc_manual_btn_{job_raw.lower()}{key_suffix}",
                use_container_width=True,
            ):
                if append_manual_request(job_raw, job_label, ""):
                    st.session_state["pc_manual_feedback"] = job_label

            fields_col = col_widget.container()
            with fields_col:
                days_input = st.text_input(
                    f"Días programados ({job_label})",
                    value=days_value,
                    key=days_key,
                    placeholder="Días separados por comas",
                    label_visibility="collapsed",
                    help="Días separados por comas",
                )
                times_input = st.text_input(
                    f"Horas programadas ({job_label})",
                    value=times_value,
                    key=times_key,
                    placeholder="Horas separadas por comas",
                    label_visibility="collapsed",
                    help="Horas separadas por comas",
                )


            if cfg is None:
                missing_config_jobs.add(job_label)
                continue

            cleaned_days = _canonicalize_schedule_text(days_input) if days_col else ""
            cleaned_times = _canonicalize_schedule_text(times_input) if times_col else ""

            diff_days = bool(days_col and cleaned_days != days_value)
            diff_times = bool(times_col and cleaned_times != times_value)

            entry = buffer.get(job_key)

            if diff_days or diff_times:
                entry = buffer.setdefault(
                    job_key,
                    {"name": cfg_name_value, "label": job_label},
                )
                if diff_days:
                    entry["days"] = cleaned_days
                elif "days" in entry:
                    entry.pop("days", None)

                if diff_times:
                    entry["times"] = cleaned_times
                elif "times" in entry:
                    entry.pop("times", None)

                entry["ts"] = time.time()
            else:
                if entry:
                    entry.pop("days", None)
                    entry.pop("times", None)
                    if not any(k in entry for k in ("days", "times")):
                        buffer.pop(job_key, None)

            if entry and any(k in entry for k in ("days", "times")):
                col_widget.caption("⬆ Cambios pendientes")
            else:
                col_widget.caption("")

    if missing_config_jobs:
        st.warning(
            "No encontramos filas de configuración para: "
            + ", ".join(sorted(missing_config_jobs))
            + ". Revisa la hoja pc_config."
        )

    feedback_key = "pc_manual_feedback"
    if feedback_key in st.session_state:
        job_label = st.session_state.pop(feedback_key)
        st.success(f"Ejecución manual iniciada, scraping en curso para {job_label} (status: pending).")

    unsaved_jobs = [
        info.get("label") or key
        for key, info in buffer.items()
        if any(k in info for k in ("days", "times"))
    ]

    if unsaved_jobs:
        st.info(
            "Cambios pendientes: "
            + ", ".join(sorted(set(unsaved_jobs)))
        )

    sync_key = f"pc_sync_btn_{suffix or 'main'}"
    if st.button(
        "Sincronizar cambios",
        key=sync_key,
        type="primary",
        disabled=not unsaved_jobs,
        use_container_width=True,
    ):
        st.session_state["pc_config_sync_trigger"] = True

    if st.session_state.pop("pc_config_sync_trigger", False):
        success = sync_pc_config_updates(pc_config_df)
        if success:
            st.success("Se sincronizaron los cambios en pc_config.")
        else:
            st.warning("No se detectaron cambios para sincronizar.")


def sync_pc_config_updates(pc_config_df: pd.DataFrame | None) -> bool:
    """Sincroniza con Google Sheets los cambios almacenados en pc_config_buffer."""

    buffer = st.session_state.get("pc_config_buffer", {}) or {}
    pending: list[dict[str, str]] = []
    for job_key, info in buffer.items():
        payload = {"key": job_key}
        if info.get("name"):
            payload["name"] = info["name"]
        if "days" in info:
            payload["days"] = info["days"]
        if "times" in info:
            payload["times"] = info["times"]
        if len(payload) > 1:
            payload["label"] = info.get("label", job_key)
            pending.append(payload)

    if not pending:
        return False

    sheet_id = _pc_config_sheet_id()
    if not sheet_id:
        st.warning("No hay SHEET_ID configurado para sincronizar pc_config.")
        return False

    client = get_gc()
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(PC_CONFIG_WORKSHEET)
    except Exception as exc:
        st.error(f"No se pudo acceder a la hoja pc_config: {exc}")
        return False

    try:
        df = read_worksheet(client, sheet_id, PC_CONFIG_WORKSHEET)
    except Exception as exc:
        st.error(f"No se pudo leer pc_config: {exc}")
        return False

    if df is None or df.empty:
        st.warning("pc_config está vacía; no se aplicaron cambios.")
        return False

    headers = [str(c) for c in df.columns]
    work_df = df.copy()
    work_df.columns = [str(c).strip().lower() for c in work_df.columns]

    name_col = _resolve_config_column(work_df, PC_CONFIG_NAME_ALIASES)
    if not name_col:
        st.warning("No se encontró ninguna columna equivalente a 'name' en pc_config; no se aplicaron cambios.")
        return False

    days_col = _resolve_config_column(work_df, PC_CONFIG_DAYS_ALIASES)
    times_col = _resolve_config_column(work_df, PC_CONFIG_TIMES_ALIASES)

    def _column_index(lower_name: str | None) -> int | None:
        if not lower_name:
            return None
        target = lower_name.strip().lower()
        for idx, header in enumerate(headers, start=1):
            if str(header).strip().lower() == target:
                return idx
        return None

    col_idx_days = _column_index(days_col)
    col_idx_times = _column_index(times_col)

    overrides = _pc_config_overrides()
    missing_jobs: list[str] = []
    updated_any = False
    now = time.time()

    name_series = work_df[name_col].astype(str).str.strip().str.lower()

    for entry in pending:
        job_key = _normalize_job_key(entry.get("key") or entry.get("name"))
        if not job_key:
            continue

        mask = name_series == job_key
        if not mask.any():
            missing_jobs.append(entry.get("label", job_key))
            continue

        pos = mask[mask].index[0]
        row_number = int(pos) + 2  # +1 header row, +1 porque DataFrame inicia en 0

        override_entry = overrides.setdefault(job_key, {})
        override_entry["name"] = _sanitize_config_value(work_df.at[pos, name_col])

        if col_idx_days and "days" in entry and entry["days"] is not None:
            value = _canonicalize_schedule_text(entry["days"])
            try:
                ws.update_cell(row_number, col_idx_days, value)
            except Exception as exc:
                st.error(f"No se pudo actualizar los días para '{job_key}': {exc}")
                return
            work_df.at[pos, days_col] = value
            override_entry["days"] = value
            updated_any = True

        if col_idx_times and "times" in entry and entry["times"] is not None:
            value = _canonicalize_schedule_text(entry["times"])
            try:
                ws.update_cell(row_number, col_idx_times, value)
            except Exception as exc:
                st.error(f"No se pudo actualizar las horas para '{job_key}': {exc}")
                return
            work_df.at[pos, times_col] = value
            override_entry["times"] = value
            updated_any = True

        override_entry["pending"] = False
        override_entry["ts"] = now

    if missing_jobs:
        st.warning(
            "No se encontraron filas en pc_config para: "
            + ", ".join(sorted(missing_jobs))
        )

    if not updated_any:
        return False

    load_pc_config.clear()
    st.session_state["pc_config_buffer"] = {
        key: info
        for key, info in buffer.items()
        if key not in {entry["key"] for entry in pending}
    }
    return True


def _make_unique(headers):
    out, seen = [], {}
    for i, h in enumerate(headers):
        h = (h or "").strip() or f"col_{i+1}"
        if h in seen:
            seen[h] += 1
            h = f"{h}_{seen[h]}"
        else:
            seen[h] = 0
        out.append(h)
    return out


def _coerce_to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return value != 0
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in TRUE_VALUES:
            return True
        if norm in {"false", "0", "no", "n"}:
            return False
    return False


def _is_checkbox_target(col_name: str) -> bool:
    return col_name.strip().lower() in CHECKBOX_FLAG_NAMES


def _parse_sheet_date_column(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], index=series.index, dtype="datetime64[ns]", name=series.name)

    if pd.api.types.is_datetime64_any_dtype(series):
        tz_info = getattr(series.dt, "tz", None)
        return series.dt.tz_convert(None) if tz_info is not None else series

    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.replace({
        "": pd.NA,
        "none": pd.NA,
        "null": pd.NA,
        "nan": pd.NA,
        "nat": pd.NA,
        "n/a": pd.NA,
    }, regex=False)

    parsed = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)

    mask_serial = parsed.isna() & cleaned.str.fullmatch(r"\d+(\.0+)?", na=False)
    if mask_serial.any():
        serial = cleaned[mask_serial].astype(float)
        parsed.loc[mask_serial] = pd.to_datetime(
            serial,
            errors="coerce",
            origin="1899-12-30",
            unit="D",
        )

    mask_pattern = parsed.isna() & cleaned.notna()
    if mask_pattern.any():
        date_pattern = re.compile(r"(\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4})")

        def _last_match(text: str):
            matches = date_pattern.findall(text)
            if matches:
                return matches[-1]
            return text

        extracted = cleaned[mask_pattern].map(_last_match)
        parsed.loc[mask_pattern] = pd.to_datetime(extracted, errors="coerce", dayfirst=True)

    tz_info = getattr(parsed.dt, "tz", None)
    if tz_info is not None:
        parsed = parsed.dt.tz_convert(None)

    return parsed

st.set_page_config(page_title="Visualizador de Actos", layout="wide")
_require_authentication()
st.title("📋 Visualizador de Actos Panamá Compra")

# ---- Config ----
SHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"


DEFAULT_SHEET = "cl_abiertas_rir_sin_requisitos"

SHEET_LABELS = {
    "cl_abiertas_rir_sin_requisitos": "CL abiertas RIR sin requisitos",
    "cl_abiertas": "CL abiertas",
    "cl_abiertas_rir_con_ct": "CL abiertas RIR con CT",
    "cl_prog_sin_ficha": "CL programadas sin ficha",
    "cl_prog_sin_requisitos": "CL programadas sin requisitos",
    "cl_prog_con_ct": "CL programadas con CT",
    "cl_prioritarios": "CL prioritarios",
    "ap_con_ct": "AP con CT",
    "ap_sin_ficha": "AP sin ficha",
    "ap_sin_requisitos": "AP sin requisitos",
}

SHEET_GROUPS = {
    "Cotizaciones Abiertas": [
        "cl_abiertas_rir_sin_requisitos",
        "cl_abiertas",
        "cl_abiertas_rir_con_ct",
    ],
    "Cotizaciones Programadas": [
        "cl_prog_sin_ficha",
        "cl_prog_sin_requisitos",
        "cl_prog_con_ct",
    ],
    "Licitaciones": [
        "ap_con_ct",
        "ap_sin_ficha",
        "ap_sin_requisitos",
    ],
    "Prioritarias": [
        "cl_prioritarios",
    ],
}

CATEGORY_ORDER = [
    "Cotizaciones Abiertas",
    "Cotizaciones Programadas",
    "Licitaciones",
    "Prioritarias",
]


def _sheet_label(name: str) -> str:
    return SHEET_LABELS.get(name, name.replace("_", " ").title())

def get_gc():
    # Reutiliza las credenciales centralizadas en sheets.get_client()
    client, _ = get_client()
    return client


def apply_checkbox_updates(sheet_name: str, updates):
    if not updates:
        return

    ws = get_gc().open_by_key(SHEET_ID).worksheet(sheet_name)
    headers = _make_unique(ws.row_values(1))

    for row_number, column_name, value in updates:
        try:
            col_idx = headers.index(column_name) + 1
        except ValueError:
            continue
        ws.update_cell(int(row_number), col_idx, "TRUE" if value else "FALSE")

# --- reemplaza tu load_df y añade el helper _make_unique ---

@st.cache_data(ttl=300)
def load_df(sheet_name: str) -> pd.DataFrame:
    sh = get_gc().open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    raw_headers = ws.row_values(1)
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    if not any(c.strip() for c in raw_headers):
        header_row_idx = None
        for r in range(min(10, len(values))):
            non_empty = sum(1 for c in values[r] if c.strip())
            if non_empty >= 3:
                header_row_idx = r
                break

        if header_row_idx is None:
            return pd.DataFrame()

        headers = _make_unique(values[header_row_idx])
        width = len(headers)

        data_rows, row_numbers = [], []
        for idx, row in enumerate(values[header_row_idx + 1 :], start=header_row_idx + 2):
            trimmed = row[:width] + [""] * (width - len(row))
            data_rows.append(trimmed)
            row_numbers.append(idx)

        if not data_rows:
            df = pd.DataFrame(columns=headers)
            df[ROW_ID_COL] = pd.Series(dtype=int)
            return df

        df = pd.DataFrame(data_rows, columns=headers)
        df[ROW_ID_COL] = pd.Series(row_numbers, dtype=int)
    else:
        headers = _make_unique(raw_headers)
        width = len(headers)
        data_rows = values[1:] if len(values) > 1 else []
        padded_rows = [row[:width] + [""] * (width - len(row)) for row in data_rows]
        df = pd.DataFrame(padded_rows, columns=headers)
        if df.empty:
            df[ROW_ID_COL] = pd.Series(dtype=int)
        else:
            df[ROW_ID_COL] = pd.Series(range(2, len(df) + 2), dtype=int)

    df = df.replace("", pd.NA)
    data_cols = [c for c in df.columns if c != ROW_ID_COL]
    if data_cols:
        df = df.dropna(how="all", subset=data_cols)
    df = df.reset_index(drop=True)
    return df


def render_df(
    df: pd.DataFrame,
    sheet_name: str,
    pc_state_df: pd.DataFrame | None = None,
    pc_config_df: pd.DataFrame | None = None,
    suffix: str | None = None,
):
    keyp = f"{sheet_name}_"

    notice_key = keyp + "update_notice"
    if notice_key in st.session_state:
        count = st.session_state.pop(notice_key)
        if count:
            st.success(f"Se guardaron {count} cambio(s) en la hoja.")

    df = df.copy()
    displayable_columns = [c for c in df.columns if c != ROW_ID_COL]

    def _normalize_label(value: str) -> str:
        value = (value or "").strip().lower()
        for src, tgt in (
            ("á", "a"),
            ("é", "e"),
            ("í", "i"),
            ("ó", "o"),
            ("ú", "u"),
            ("ü", "u"),
        ):
            value = value.replace(src, tgt)
        value = re.sub(r"\s+", " ", value)
        return value

    def _is_date_header(name: str) -> bool:
        normalized = _normalize_label(name)
        if not normalized:
            return False
        if normalized.startswith("fecha"):
            return True
        if "publicacion" in normalized:
            return True
        return False

    date_columns = [c for c in df.columns if _is_date_header(c)]

    filter_date_col = None
    for col in date_columns:
        norm = _normalize_label(col)
        if "publicacion" in norm:
            filter_date_col = col
            break

    if filter_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha sola":
                filter_date_col = col
                break

    if filter_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha":
                filter_date_col = col
                break

    if filter_date_col is None and date_columns:
        filter_date_col = date_columns[0]

    event_date_col = None
    for col in date_columns:
        if _normalize_label(col) == "fecha sola":
            event_date_col = col
            break

    if event_date_col is None:
        for col in date_columns:
            norm = _normalize_label(col)
            if any(token in norm for token in ("acto", "celebr")):
                event_date_col = col
                break

    if event_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha":
                event_date_col = col
                break

    parsed_date_series = {}
    for col in date_columns:
        parsed = _parse_sheet_date_column(df[col])
        parsed_date_series[col] = parsed
        normalized_name = _normalize_label(col)
        keep_original = (col == filter_date_col) or (normalized_name == "fecha")
        if not keep_original:
            df[col] = parsed

    # Detectar columnas de monto/precio
    money_cols = [c for c in df.columns if any(x in c.lower() for x in ["precio", "monto", "estimado", "referencia"])]

    today = date.today()
    today_ts = pd.Timestamp(today)

    with st.expander("🔎 Filtros", expanded=True):
        if "Entidad" in df.columns:
            opciones = sorted([e for e in df["Entidad"].dropna().unique()])
            sel = st.multiselect("Entidad", opciones, key=keyp+"ent")
            if sel:
                df = df[df["Entidad"].isin(sel)]

        if "Estado" in df.columns:
            opciones = sorted([e for e in df["Estado"].dropna().unique()])
            sel = st.multiselect("Estado", opciones, key=keyp+"estado")
            if sel:
                df = df[df["Estado"].isin(sel)]

        date_filter_series = None
        if filter_date_col:
            date_filter_series = parsed_date_series.get(filter_date_col)
            if date_filter_series is not None and date_filter_series.notna().any():
                normalized_dates = date_filter_series.dt.normalize()
                valid_dates = normalized_dates.dropna()
                mind = valid_dates.min() if not valid_dates.empty else pd.Timestamp(today)
                maxd = valid_dates.max() if not valid_dates.empty else pd.Timestamp(today)
                default_fin = today
                default_ini = today - timedelta(days=30)
                if pd.notna(mind) and mind.date() > default_fin:
                    default_ini = mind.date()
                    default_fin = mind.date()
                if default_ini > default_fin:
                    default_ini = default_fin
                r = st.date_input(
                    "Rango de fechas",
                    value=(default_ini, default_fin),
                    key=keyp+"fecha",
                )
                if isinstance(r, tuple) and len(r) == 2:
                    ini = pd.Timestamp(r[0]).normalize()
                    fin = pd.Timestamp(r[1]).normalize()
                    normalized = date_filter_series.dt.normalize()
                    mask_valid = (normalized >= ini) & (normalized <= fin)
                    mask = normalized.isna() | mask_valid
                    df = df[mask]
            else:
                st.info("No encontramos fechas válidas en esa columna todavía.")

        if money_cols:
            colm = money_cols[0]
            v = pd.to_numeric(df[colm], errors="coerce")
            if v.notna().any():
                price_min, price_max = 1000.0, 2000000.0
                if price_min < price_max:
                    r = st.slider(
                        f"Rango de {colm}",
                        min_value=price_min,
                        max_value=price_max,
                        value=(price_min, price_max),
                        step=1000.0,
                        key=keyp+"monto",
                    )
                    df = df[(pd.to_numeric(df[colm], errors="coerce") >= r[0]) &
                            (pd.to_numeric(df[colm], errors="coerce") <= r[1])]

        q = st.text_input("Búsqueda rápida (todas las columnas)", key=keyp+"q",
                          placeholder="Palabra clave, CT, entidad, título…")
        if q:
            mask = df.astype(str).apply(lambda s: s.str.contains(q, case=False, na=False)).any(axis=1)
            df = df[mask]
        def _is_item_column(col_name: str) -> bool:
            normalized = col_name.strip().lower().replace("í", "i")
            return normalized.startswith("item")

        item_cols_sorted = [c for c in displayable_columns if _is_item_column(c)]

        cols_key = keyp + "cols"
        state_applied_key = keyp + "items_all_applied"

        if item_cols_sorted:
            non_item_cols = [c for c in displayable_columns if c not in item_cols_sorted]
            base_items = item_cols_sorted[:2] if len(item_cols_sorted) >= 2 else item_cols_sorted
            base_default = non_item_cols + base_items
            show_all = st.toggle("➡️ Mostrar todos los Item_n", key=keyp+"toggle_items")

            if cols_key not in st.session_state:
                st.session_state[cols_key] = base_default
                st.session_state[state_applied_key] = False
            st.session_state.setdefault(state_applied_key, False)

            if show_all and not st.session_state.get(state_applied_key, False):
                st.session_state[cols_key] = non_item_cols + item_cols_sorted
                st.session_state[state_applied_key] = True
            elif not show_all and st.session_state.get(state_applied_key, False):
                st.session_state[cols_key] = base_default
                st.session_state[state_applied_key] = False
        else:
            if cols_key not in st.session_state:
                st.session_state[cols_key] = displayable_columns

        selected_cols = st.multiselect(
            "Columnas a mostrar",
            options=displayable_columns,
            key=cols_key,
        )
        if selected_cols:
            cols = [c for c in displayable_columns if c in selected_cols]
        else:
            cols = displayable_columns

    parsed_filtered = {col: series.loc[df.index] for col, series in parsed_date_series.items()}

    df_base = df.copy()

    metric_state_key = keyp + "metric_filter"
    active_metric = st.session_state.get(metric_state_key)
    top_modal_key = keyp + "show_top_unidades"

    metrics_defs = []
    metrics_defs.append({
        "key": "total",
        "label": "Total de actos públicos",
        "count": int(len(df_base)),
        "filter": None,
    })

    public_col = next((c for c in df_base.columns if "public" in c.lower()), None)
    public_series = None
    public_series_normalized = None

    if public_col:
        public_series = _parse_sheet_date_column(df_base[public_col])
        count_public_today = 0
        if public_series.notna().any():
            public_series_normalized = public_series.dt.normalize()
            count_public_today = int((public_series_normalized == today_ts).sum())
        metrics_defs.append({
            "key": "publicados_hoy",
            "label": "Actos publicados hoy",
            "count": count_public_today,
            "filter": "publicados_hoy",
        })

    event_series = None
    event_series_normalized = None
    count_date_today = 0

    if event_date_col:
        event_series = parsed_filtered.get(event_date_col)
        if event_series is not None:
            event_series_normalized = event_series.dt.normalize()
            if event_series_normalized.notna().any():
                count_date_today = int((event_series_normalized == today_ts).sum())

    if event_date_col:
        metrics_defs.append({
            "key": "fecha_hoy",
            "label": "Actos a celebrarse hoy",
            "count": count_date_today,
            "filter": "fecha_hoy",
        })

    metrics_defs.append({
        "key": "top_unidades",
        "label": "Top unidades solicitantes (próximamente)",
        "count": None,
        "filter": None,
        "placeholder": True,
    })

    cols_metrics = st.columns(len(metrics_defs))
    for metric_col, metric in zip(cols_metrics, metrics_defs):
        with metric_col:
            label = metric["label"]
            if metric.get("count") is not None:
                label = f"{label}\n{metric['count']}"

            clicked = st.button(
                label,
                key=keyp + f"metric_btn_{metric['key']}",
                use_container_width=True,
            )

            if clicked:
                if metric["key"] == "total":
                    st.session_state[metric_state_key] = None
                elif metric.get("placeholder"):
                    st.session_state[top_modal_key] = not st.session_state.get(top_modal_key, False)
                else:
                    current = st.session_state.get(metric_state_key)
                    st.session_state[metric_state_key] = None if current == metric["filter"] else metric["filter"]

    active_metric = st.session_state.get(metric_state_key)

    df = df_base
    if active_metric == "fecha_hoy" and event_series_normalized is not None:
        mask = event_series_normalized == today_ts
        df = df_base.loc[mask.fillna(False)]
    elif active_metric == "publicados_hoy" and public_series_normalized is not None:
        mask = public_series_normalized == today_ts
        df = df_base.loc[mask.fillna(False)]

    if st.session_state.get(top_modal_key):
        st.info("Pronto mostraremos el top de unidades solicitantes con su suma de precio de referencia (últimos 7 días).")

    displayable_columns = [c for c in df.columns if c != ROW_ID_COL]

    table_columns = cols if cols else displayable_columns
    display_df = df[table_columns].copy()

    editable_cols = [c for c in display_df.columns if _is_checkbox_target(c)]
    for col in editable_cols:
        display_df[col] = display_df[col].map(_coerce_to_bool)

    col_cfg = {}
    for date_field in date_columns:
        if date_field in display_df.columns and pd.api.types.is_datetime64_any_dtype(display_df[date_field]):
            col_cfg[date_field] = st.column_config.DateColumn(date_field, help="Fecha")

    for c in money_cols:
        if c in display_df.columns:
            col_cfg[c] = st.column_config.NumberColumn(c, format="B/. %,.2f")

    link_col = next((c for c in display_df.columns if c.strip().lower() in {"enlace", "link", "url"}), None)
    if link_col:
        col_cfg[link_col] = st.column_config.LinkColumn(
            label="🔗",
            display_text="🔗",
            help="Abrir acto en PanamáCompra",
        )

    for col in editable_cols:
        col_cfg[col] = st.column_config.CheckboxColumn(col, help="Sincroniza al marcar", default=False)

    table_height = 620
    disabled_columns = [c for c in display_df.columns if c not in editable_cols]
    disabled_config = {"columns": disabled_columns} if disabled_columns else False

    editor_key = keyp + "editor"

    if editable_cols:
        original_display = display_df.copy()
        edited_df = st.data_editor(
            display_df,
            hide_index=True,
            width="stretch",
            height=table_height,
            column_config=col_cfg,
            disabled=disabled_config,
            key=editor_key,
        )

        changes = []
        for col in editable_cols:
            orig_series = original_display[col].fillna(False).astype(bool)
            new_series = edited_df[col].fillna(False).astype(bool)
            diff_mask = orig_series != new_series
            if diff_mask.any():
                for idx in edited_df.index[diff_mask]:
                    row_number = df.loc[idx, ROW_ID_COL]
                    if pd.isna(row_number):
                        continue
                    changes.append((int(row_number), col, bool(new_series.loc[idx])))

        if changes:
            apply_checkbox_updates(sheet_name, changes)
            st.session_state[notice_key] = len(changes)
            load_df.clear()
            st.rerun()

        df_view = edited_df
    else:
        df_view = display_df
        st.data_editor(
            df_view,
            hide_index=True,
            width="stretch",
            height=table_height,
            column_config=col_cfg,
            disabled=True,
            key=editor_key,
        )

    st.caption(f"Mostrando {len(df)} filas")

    render_pc_state_cards(pc_state_df, pc_config_df, suffix=suffix)


def render_panamacompra_db_panel() -> None:
    """Muestra una vista de las tablas disponibles en la base local panamacompra.db."""
    st.divider()
    st.subheader("Base panamacompra.db")

    db_path = _preferred_db_path()
    if db_path is None:
        st.info("No hay rutas configuradas para la base panamacompra.db.")
        return

    st.caption(f"Origen configurado: `{db_path}`")
    if not db_path.exists():
        st.warning(
            "No pudimos abrir el archivo. Verifica que la ruta "
            "`C:\\Users\\rodri\\GEAPP\\panamacompra.db` existe o define `FINAPP_DB_PATH` "
            "apuntando a una copia local."
        )
        return

    db_path_str = str(db_path)
    try:
        db_tables = list_sqlite_tables(db_path_str)
    except sqlite3.OperationalError as exc:
        st.error(f"No fue posible conectar a la base: {exc}")
        return
    except Exception as exc:
        st.error(f"No fue posible listar las tablas: {exc}")
        return

    if not db_tables:
        st.info("La base panamacompra.db no contiene tablas visibles.")
        return

    selected_table = st.selectbox(
        "Tabla disponible en la base",
        db_tables,
        key="pc_db_table_selector",
    )

    total_rows: int | None = None
    try:
        total_rows = count_sqlite_rows(db_path_str, selected_table)
    except sqlite3.OperationalError:
        pass
    except Exception:
        pass

    search_text = st.text_input(
        "Buscar en la tabla",
        placeholder="Ingresa texto a buscar…",
        key="pc_db_search",
    )
    date_range = st.date_input(
        "Rango de fechas",
        value=_default_date_range(),
        min_value=DEFAULT_DATE_START,
        max_value=date.today(),
        key="pc_db_date_range",
    )

    max_limit = total_rows if total_rows and total_rows > 0 else 5000
    max_limit = max(1, max_limit)
    min_limit = max(1, min(100, max_limit))
    default_limit = max(min_limit, min(1000, max_limit))

    limit = st.slider(
        "Límite de filas a mostrar",
        min_value=min_limit,
        max_value=max_limit,
        value=default_limit,
        step=max(1, min(100, max_limit // 5)),
        help="Amplía el límite si necesitas revisar más registros.",
    )

    try:
        preview_df = load_sqlite_preview(
            db_path_str,
            selected_table,
            limit,
            search_text=search_text,
        )
    except sqlite3.OperationalError as exc:
        st.error(f"No se pudo leer la tabla {selected_table}: {exc}")
        return
    except Exception as exc:
        st.error(f"Error al consultar {selected_table}: {exc}")
        return

    filtered_df = _filter_dataframe(preview_df, search_text, date_range)

    if filtered_df.empty:
        st.info("No hay filas que coincidan con los filtros aplicados.")
    else:
        st.dataframe(filtered_df, use_container_width=True, height=520)

    caption = f"Mostrando {len(filtered_df)} filas (tope configurado {limit})."
    if total_rows is not None:
        caption += f" Total en `{selected_table}`: {total_rows:,}."
    st.caption(caption)


def render_drive_excel_panel(title: str, file_path: Path | None, key_prefix: str) -> None:
    """Muestra una vista previa de un archivo Excel sincronizado desde Drive."""
    st.divider()
    st.subheader(title)

    if not file_path:
        st.info("No hay rutas configuradas para este archivo.")
        return

    file_path = Path(file_path)
    if not file_path.exists():
        st.warning(
            "No pudimos abrir el archivo. Verifica que la sincronización desde Drive "
            "esté funcionando o actualiza los secrets con el ID correcto."
        )
        return

    try:
        df = load_excel_file(str(file_path))
    except Exception as exc:
        st.error(f"No pudimos leer `{file_path.name}`: {exc}")
        return

    df = _clean_drive_dataframe(df)

    total_rows = len(df.index)
    if total_rows == 0:
        st.info("El archivo no contiene datos visibles.")
        return

    search_text = st.text_input(
        "Buscar en el archivo",
        placeholder="Ingresa texto a buscar…",
        key=f"{key_prefix}_search",
    )

    max_limit = total_rows if total_rows and total_rows > 0 else 5000
    max_limit = max(1, max_limit)
    min_limit = max(1, min(100, max_limit))
    default_limit = max(min_limit, min(1000, max_limit))

    slider_key = f"{key_prefix}_excel_limit"
    if slider_key in st.session_state:
        current_value = st.session_state[slider_key]
        if current_value < min_limit or current_value > max_limit:
            st.session_state[slider_key] = default_limit

    if max_limit <= min_limit:
        limit = max_limit
    else:
        step = max(1, min(100, max_limit // 5))
        limit = st.slider(
            "Límite de filas a mostrar",
            min_value=min_limit,
            max_value=max_limit,
            value=default_limit,
            step=step,
            key=slider_key,
        )

    preview_df = df.head(limit)
    filtered_df = _filter_dataframe(preview_df, search_text, None)

    if filtered_df.empty:
        st.info("No hay filas que coincidan con los filtros aplicados.")
    else:
        st.dataframe(filtered_df, use_container_width=True, height=520)
    st.caption(
        f"Mostrando {len(filtered_df)} filas (tope configurado {limit}). Total en el archivo: {total_rows:,}."
    )

# ---- UI: pestañas de categorías + desplegable de hojas ----
pc_state_df = load_pc_state()
pc_config_df = load_pc_config()
ordered_categories = [c for c in CATEGORY_ORDER if c in SHEET_GROUPS]
category_tabs = st.tabs(ordered_categories)

for tab, category_name in zip(category_tabs, ordered_categories):
    with tab:
        st.subheader(category_name)
        sheets = SHEET_GROUPS.get(category_name, [])
        if not sheets:
            st.info("Sin hojas configuradas para esta categoría.")
            continue

        selector_slug = re.sub(r"[^0-9a-z]+", "_", category_name.lower())
        selector_key = f"sheet_selector_{selector_slug.strip('_')}"
        tab_suffix = selector_slug.strip("_") or None

        if len(sheets) == 1:
            sheet_name = sheets[0]
            st.caption(_sheet_label(sheet_name))
        else:
            default_idx = sheets.index(DEFAULT_SHEET) if DEFAULT_SHEET in sheets else 0
            sheet_name = st.selectbox(
                "Seleccione la hoja",
                sheets,
                index=default_idx,
                key=selector_key,
                format_func=_sheet_label,
                label_visibility="collapsed",
            )

        df = load_df(sheet_name)
        if df.empty:
            st.info("Sin datos en esta pestaña.")
        else:
            render_df(df, sheet_name, pc_state_df, pc_config_df, suffix=tab_suffix)

analysis_expanded = st.session_state.get(ANALYSIS_PANEL_EXPANDED_KEY, False)
with st.expander("Análisis de actos públicos", expanded=analysis_expanded):
    st.session_state[ANALYSIS_PANEL_EXPANDED_KEY] = True
    render_supplier_top_panel()


db_panel_expanded = st.session_state.get(DB_PANEL_EXPANDED_KEY, False)
with st.expander(
    "Base de datos de actos publicos, fichas y oferentes",
    expanded=db_panel_expanded,
):
    st.session_state[DB_PANEL_EXPANDED_KEY] = True
    render_panamacompra_db_panel()
    render_drive_excel_panel(
        "Fichas tecnicas",
        LOCAL_FICHAS_CTNI,
        "fichas_ctni",
    )
    render_drive_excel_panel(
        "Criterios tecnicos",
        LOCAL_CRITERIOS_TECNICOS,
        "criterios_tecnicos",
    )
    render_drive_excel_panel(
        "Oferentes y Catalogos",
        LOCAL_OFERENTES_CATALOGOS,
        "oferentes_catalogos",
    )

