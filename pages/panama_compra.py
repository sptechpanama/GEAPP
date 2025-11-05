"""Vista PanamáCompra para GE FinApp."""

# pages/visualizador.py
import re
import streamlit as st
import pandas as pd
import uuid
from datetime import date, timedelta, datetime, timezone
from sheets import get_client, read_worksheet


ROW_ID_COL = "__row__"
CHECKBOX_FLAG_NAMES = {
    "prioritario",
    "prioritarios",
    "descartar",
    "descarte",
}
TRUE_VALUES = {"true", "1", "si", "sí", "yes", "y", "t", "x", "on"}

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
}


def _manual_sheet_id() -> str | None:
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    manual_id = app_cfg.get("PC_MANUAL_SHEET_ID") if isinstance(app_cfg, dict) else None
    manual_id = manual_id or PC_MANUAL_SHEET_ID
    return manual_id or None


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
    }

    def _value_for_header(header: str) -> str:
        normalized = header.strip().lower()
        for key, aliases in HEADER_ALIASES.items():
            if normalized == key or normalized in aliases:
                return payload_map.get(key, "")
        return payload_map.get(normalized, "")

    row = [_value_for_header(header) for header in cleaned_headers]

    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as exc:
        st.error(f"No se pudo registrar la ejecución manual: {exc}")
        return False

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
    try:
        sheet_id = st.secrets["app"]["SHEET_ID"]
    except Exception:
        return pd.DataFrame()

    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_CONFIG_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    return df.rename(columns=lambda col: str(col).strip().lower())


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
<div style="margin-top:14px;margin-bottom:6px;padding:6px 12px;background:rgba(63,142,252,0.16);border-radius:6px;font-size:1.12rem;font-weight:600;color:#ffffff;letter-spacing:0.01em;">
  🔧 Resumen de últimas actualizaciones y controles
</div>
""",
        unsafe_allow_html=True,
    )
    rows = pc_state_df.drop(columns=[col for col in pc_state_df.columns if col.startswith("__")]).copy()

    config_map = {}
    if pc_config_df is not None and not pc_config_df.empty and "name" in pc_config_df.columns:
        tmp = pc_config_df.copy()
        tmp["name"] = tmp["name"].astype(str).str.strip().str.lower()
        for _, cfg_row in tmp.iterrows():
            config_map[cfg_row["name"]] = cfg_row

    updates = []

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

            cfg = config_map.get(job_raw.lower()) if config_map else None
            key_suffix = f"_{suffix}" if suffix else ""
            days_key = f"pc_days_{job_raw.lower()}{key_suffix}"
            times_key = f"pc_times_{job_raw.lower()}{key_suffix}"
            days_value = str(cfg.get("days", "")).strip() if cfg is not None else ""
            times_value = str(cfg.get("times", "")).strip() if cfg is not None else ""

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

            days_input = col_widget.text_input(
                f"Días programados ({job_label})",
                value=days_value,
                key=days_key,
                placeholder="Días separados por comas",
                label_visibility="collapsed",
                help="Días separados por comas",
            )
            times_input = col_widget.text_input(
                f"Horas programadas ({job_label})",
                value=times_value,
                key=times_key,
                placeholder="Horas separadas por comas",
                label_visibility="collapsed",
                help="Horas separadas por comas",
            )

            if col_widget.button(
                f"▶ Actualización manual",
                key=f"pc_manual_btn_{job_raw.lower()}{key_suffix}",
                use_container_width=True,
            ):
                if append_manual_request(job_raw, job_label, ""):
                    st.session_state["pc_manual_feedback"] = job_label

            if cfg is not None and (
                days_input.strip() != days_value or times_input.strip() != times_value
            ):
                updates.append(
                    {
                        "name": cfg.get("name", job_raw),
                        "days": days_input.strip(),
                        "times": times_input.strip(),
                    }
                )

    if updates:
        st.session_state.setdefault("pc_config_pending_updates", []).extend(updates)

    feedback_key = "pc_manual_feedback"
    if feedback_key in st.session_state:
        job_label = st.session_state.pop(feedback_key)
        st.success(f"Ejecución manual iniciada, scraping en curso para {job_label} (status: pending).")


def sync_pc_config_updates(pc_config_df: pd.DataFrame | None) -> None:
    """Escribe en la hoja de configuración cualquier cambio realizado desde la UI."""
    pending = st.session_state.pop("pc_config_pending_updates", None)
    if not pending:
        return

    if pc_config_df is None or pc_config_df.empty:
        st.warning("No fue posible sincronizar pc_config: datos base vacíos.")
        return

    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}
    sheet_id = app_cfg.get("SHEET_ID")
    if not sheet_id:
        st.warning("No hay SHEET_ID configurado para sincronizar pc_config.")
        return

    client = get_gc()
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(PC_CONFIG_WORKSHEET)

    df = pc_config_df.copy()
    df.columns = df.columns.astype(str)
    name_col = None
    for col in df.columns:
        if col.lower() == "name":
            name_col = col
            break
    if name_col is None:
        st.warning("No se encontró columna 'name' en pc_config; no se aplicaron cambios.")
        return

    df[name_col] = df[name_col].astype(str).str.strip().str.lower()
    df.set_index(name_col, inplace=True)

    for payload in pending:
        job_name = str(payload.get("name", "")).strip().lower()
        if not job_name or job_name not in df.index:
            continue
        if "days" in payload:
            df.at[job_name, "days"] = payload["days"]
        if "times" in payload:
            df.at[job_name, "times"] = payload["times"]

    df.reset_index(inplace=True)
    headers = list(df.columns)
    values = [headers] + df.astype(str).fillna("").values.tolist()
    ws.update("A1", values)
    load_pc_config.clear()


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

            prefix = "✅ " if active_metric == metric.get("filter") else ""
            if metric["key"] == "total" and active_metric is None:
                prefix = "✅ "

            clicked = st.button(
                prefix + label,
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

sync_pc_config_updates(pc_config_df)
