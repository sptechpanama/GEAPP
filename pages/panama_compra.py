# pages/visualizador.py
import re
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from sheets import get_client


ROW_ID_COL = "__row__"
CHECKBOX_FLAG_NAMES = {
    "prioritario",
    "prioritarios",
    "descartar",
    "descarte",
}
TRUE_VALUES = {"true", "1", "si", "sí", "yes", "y", "t", "x", "on"}


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
st.title("📋 Visualizador cómodo de Actos (CL / RIR)")

# ---- Config ----
SHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"


DEFAULT_TAB = "cl_abiertas_rir_sin_requisitos"
_OTHER_TABS = [
    "ap_con_ct", "ap_sin_ficha", "ap_sin_requisitos",
    "cl_prog_sin_ficha", "cl_prog_sin_requisitos", "cl_prog_con_ct",
    "cl_abiertas", "cl_abiertas_rir_con_ct", "cl_prioritarios",
]
TABS = [DEFAULT_TAB] + [t for t in _OTHER_TABS if t != DEFAULT_TAB]

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


def render_df(df: pd.DataFrame, sheet_name: str):
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
    st.download_button(
        "⬇️ Descargar CSV",
        df_view.to_csv(index=False).encode("utf-8"),
        file_name=f"{sheet_name}.csv",
        mime="text/csv",
        key=keyp+"dl",
    )

# ---- UI en pestañas (más cómodo que selectbox) ----
tabs = st.tabs(TABS)
for tab, name in zip(tabs, TABS):
    with tab:
        st.subheader(name)
        df = load_df(name)
        if df.empty:
            st.info("Sin datos en esta pestaña.")
        else:
            render_df(df, name)
