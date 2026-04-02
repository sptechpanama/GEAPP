from __future__ import annotations

import uuid
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from core.finance_v2 import (
    GlobalFilters,
    apply_global_filters,
    build_analisis_gerencial,
    build_balance_general_simplificado,
    build_cashflow_actual,
    build_cashflow_proyectado,
    build_cuentas_por_cobrar,
    build_cuentas_por_pagar,
    build_estado_resultados,
    format_money_es,
    format_number_es,
    format_percent_es,
    get_filter_options,
    get_finance_sheet_config,
    load_finance_inputs,
    normalize_gastos,
    normalize_ingresos,
    split_real_vs_pending,
)
from core.finance_v2.constants import COL_FECHA
from core.finance_v2.constants import (
    COL_CATEGORIA,
    COL_CLIENTE_NOMBRE,
    COL_CONCEPTO,
    COL_DESC,
    COL_EMPRESA,
    COL_FECHA_COBRO,
    COL_MONTO,
    COL_POR_COBRAR,
    COL_POR_PAGAR,
    COL_PROVEEDOR,
    COL_PROYECTO,
)
from ui.theme import apply_global_theme


st.set_page_config(page_title="Panel Financiero Gerencial", page_icon="\U0001f4c8", layout="wide")
apply_global_theme()

if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")


F2_DEFAULTS_VERSION = "2026-04-02-defaults-v5"


def _ensure_v2_default_state() -> None:
    """
    Fuerza defaults de UX una sola vez por versión para evitar que session_state
    histórico deje selectores en valores viejos (ej: 'Mensual' en todas las secciones).
    """
    if st.session_state.get("f2_defaults_version") == F2_DEFAULTS_VERSION:
        return
    st.session_state["f2_modo_tiempo"] = "Periodo corriente (mes actual)"
    st.session_state["f2_period_cash_actual"] = "Mensual"
    st.session_state["f2_period_cash_proj"] = "Mensual"
    st.session_state["f2_period_results"] = "Semestral"
    st.session_state["f2_period_balance"] = "Mensual"
    st.session_state["f2_horizonte_proyeccion"] = 4
    st.session_state["f2_use_recommended_periods"] = True
    st.session_state["f2_window_cash_actual"] = "Mes corriente"
    st.session_state["f2_window_results"] = "Ultimo semestre cerrado"
    st.session_state["f2_window_balance"] = "Ultimo cierre mensual"
    st.session_state["f2_defaults_version"] = F2_DEFAULTS_VERSION


_ensure_v2_default_state()



def _safe_rerun() -> None:
    rerun = getattr(st, "rerun", None)
    if callable(rerun):
        rerun()
        return
    legacy = getattr(st, "experimental_rerun", None)
    if callable(legacy):
        legacy()



def _render_kpi_row(metrics: list[tuple[str, str]], cols: int = 5) -> None:
    if not metrics:
        return
    for i in range(0, len(metrics), cols):
        row = metrics[i : i + cols]
        columns = st.columns(len(row))
        for col, (label, value) in zip(columns, row):
            with col:
                st.metric(label, value)



def _line_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str = "#22c55e"):
    if df.empty:
        st.info("Sin datos para mostrar en este grafico.")
        return
    chart = (
        alt.Chart(df)
        .mark_line(point=True, color=color)
        .encode(
            x=alt.X(f"{x}:T", title="Fecha"),
            y=alt.Y(f"{y}:Q", title=title),
            tooltip=[alt.Tooltip(f"{x}:T", title="Fecha"), alt.Tooltip(f"{y}:Q", title=title, format=",.2f")],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)



def _bar_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str = "#0ea5e9"):
    if df.empty:
        st.info("Sin datos para mostrar en este grafico.")
        return
    chart = (
        alt.Chart(df)
        .mark_bar(color=color)
        .encode(
            x=alt.X(f"{x}:N", sort="-y", title=x.replace("_", " ").title()),
            y=alt.Y(f"{y}:Q", title=title),
            tooltip=[alt.Tooltip(f"{x}:N", title=x.title()), alt.Tooltip(f"{y}:Q", title=title, format=",.2f")],
        )
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)



def _series_to_csv_download(df: pd.DataFrame, filename: str, label: str):
    csv_data = df.to_csv(index=False).encode("utf-8")
    st.download_button(label, data=csv_data, file_name=filename, mime="text/csv")


PERIOD_OPTIONS_CASH = ["Mensual", "Semanal", "Diario"]
PERIOD_OPTIONS_RESULTS = ["Semestral", "Cuatrimestral", "Mensual", "Trimestral", "Anual"]
PERIOD_OPTIONS_BALANCE = ["Mensual", "Cuatrimestral", "Anual"]


def _freq_from_period_label(label: str) -> str:
    key = str(label or "").strip().lower()
    mapping = {
        "diario": "D",
        "semanal": "W",
        "mensual": "M",
        "semestral": "6MS",
        "trimestral": "Q",
        "cuatrimestral": "4MS",
        "anual": "Y",
    }
    return mapping.get(key, "M")


def _aggregate_cash_series(serie: pd.DataFrame, period_label: str) -> pd.DataFrame:
    if serie is None or serie.empty:
        return pd.DataFrame(columns=[COL_FECHA, "flujo", "saldo"])
    freq = _freq_from_period_label(period_label)
    base = serie.copy()
    date_col = COL_FECHA if COL_FECHA in base.columns else ("fecha" if "fecha" in base.columns else "fecha_evento")
    flow_col = "flujo" if "flujo" in base.columns else ("flujo_proyectado" if "flujo_proyectado" in base.columns else None)
    if date_col not in base.columns or flow_col is None:
        return pd.DataFrame(columns=[COL_FECHA, "flujo", "saldo"])

    base[date_col] = pd.to_datetime(base[date_col], errors="coerce")
    base[flow_col] = pd.to_numeric(base[flow_col], errors="coerce").fillna(0.0)
    base = base.dropna(subset=[date_col])
    if base.empty:
        return pd.DataFrame(columns=[COL_FECHA, "flujo", "saldo"])

    grouped = (
        base.set_index(date_col)[flow_col]
        .resample(freq)
        .sum()
        .reset_index()
        .rename(columns={date_col: COL_FECHA, flow_col: "flujo"})
    )
    grouped = grouped.dropna(subset=[COL_FECHA])
    grouped = grouped.sort_values(COL_FECHA)
    grouped["saldo"] = grouped["flujo"].cumsum()
    return grouped[[COL_FECHA, "flujo", "saldo"]]


def _aggregate_resultados_periodo(mensual_df: pd.DataFrame, period_label: str) -> pd.DataFrame:
    if mensual_df is None or mensual_df.empty:
        return pd.DataFrame(columns=["Periodo", "Ingresos", "Gastos", "Utilidad"])
    freq = _freq_from_period_label(period_label)
    work = mensual_df.copy()
    mes_col = "Mes" if "Mes" in work.columns else ("mes" if "mes" in work.columns else None)
    if mes_col is None:
        return pd.DataFrame(columns=["Periodo", "Ingresos", "Gastos", "Utilidad"])
    for col in ["Ingresos", "Gastos", "Utilidad"]:
        if col not in work.columns:
            work[col] = 0.0
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0.0)
    work[mes_col] = pd.to_datetime(work.get(mes_col), errors="coerce")
    work = work.dropna(subset=[mes_col])
    if work.empty:
        return pd.DataFrame(columns=["Periodo", "Ingresos", "Gastos", "Utilidad"])
    grouped = (
        work.set_index(mes_col)[["Ingresos", "Gastos", "Utilidad"]]
        .resample(freq)
        .sum()
        .reset_index()
        .rename(columns={mes_col: "Periodo"})
        .dropna(subset=["Periodo"])
        .sort_values("Periodo")
    )
    return grouped


def _filter_df_window(df: pd.DataFrame, fecha_inicio: date, fecha_fin: date) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=(df.columns if isinstance(df, pd.DataFrame) else []))
    out = df.copy()
    out[COL_FECHA] = pd.to_datetime(out.get(COL_FECHA), errors="coerce")
    return out[(out[COL_FECHA] >= pd.Timestamp(fecha_inicio)) & (out[COL_FECHA] <= pd.Timestamp(fecha_fin))].copy()


def _clip_window(fecha_inicio: date, fecha_fin: date, min_date: date, max_date: date) -> tuple[date, date]:
    start = max(min_date, fecha_inicio)
    end = min(max_date, fecha_fin)
    if start > end:
        start = min_date
        end = max_date
    return start, end


def _month_start(ref: date) -> date:
    return date(ref.year, ref.month, 1)


def _month_end(ref: date) -> date:
    return (pd.Timestamp(ref).to_period("M").to_timestamp(how="end")).date()


def _quarter_start(ref: date) -> date:
    quarter_month = ((ref.month - 1) // 3) * 3 + 1
    return date(ref.year, quarter_month, 1)


def _last_closed_month_window(ref: date) -> tuple[date, date]:
    prev_month_day = _month_start(ref) - timedelta(days=1)
    return _month_start(prev_month_day), _month_end(prev_month_day)


def _last_closed_semester_window(ref: date) -> tuple[date, date]:
    if ref.month <= 6:
        return date(ref.year - 1, 7, 1), date(ref.year - 1, 12, 31)
    return date(ref.year, 1, 1), date(ref.year, 6, 30)


def _last_closed_cuatrimester_window(ref: date) -> tuple[date, date]:
    if ref.month <= 4:
        return date(ref.year - 1, 9, 1), date(ref.year - 1, 12, 31)
    if ref.month <= 8:
        return date(ref.year, 1, 1), date(ref.year, 4, 30)
    return date(ref.year, 5, 1), date(ref.year, 8, 31)


def _last_closed_year_window(ref: date) -> tuple[date, date]:
    return date(ref.year - 1, 1, 1), date(ref.year - 1, 12, 31)


def _resolve_cash_window(label: str, today_ref: date, min_date: date, max_date: date) -> tuple[date, date]:
    if label == "Ultimos 30 dias":
        return _clip_window(today_ref - timedelta(days=29), today_ref, min_date, max_date)
    if label == "Mes anterior":
        prev_month_day = _month_start(today_ref) - timedelta(days=1)
        return _clip_window(_month_start(prev_month_day), _month_end(prev_month_day), min_date, max_date)
    if label == "Trimestre corriente":
        return _clip_window(_quarter_start(today_ref), today_ref, min_date, max_date)
    if label == "Año corriente":
        return _clip_window(date(today_ref.year, 1, 1), today_ref, min_date, max_date)
    return _clip_window(_month_start(today_ref), today_ref, min_date, max_date)


def _resolve_results_window(label: str, today_ref: date, min_date: date, max_date: date) -> tuple[date, date]:
    if label == "Ultimo cuatrimestre cerrado":
        return _clip_window(*_last_closed_cuatrimester_window(today_ref), min_date, max_date)
    if label == "Ultimo año cerrado":
        return _clip_window(*_last_closed_year_window(today_ref), min_date, max_date)
    return _clip_window(*_last_closed_semester_window(today_ref), min_date, max_date)


def _resolve_balance_window(label: str, today_ref: date, min_date: date, max_date: date) -> tuple[date, date]:
    if label == "Ultimo cierre cuatrimestral":
        return _clip_window(*_last_closed_cuatrimester_window(today_ref), min_date, max_date)
    if label == "Ultimo cierre anual":
        return _clip_window(*_last_closed_year_window(today_ref), min_date, max_date)
    return _clip_window(*_last_closed_month_window(today_ref), min_date, max_date)


def _projection_window(horizon_months: int, today_ref: date | None = None) -> tuple[date, date]:
    today_norm = pd.Timestamp(today_ref or date.today()).normalize()
    end_period = today_norm.to_period("M") + (int(horizon_months) - 1)
    horizon_end = end_period.to_timestamp(how="end").normalize().date()
    return today_norm.date(), horizon_end


def _prepare_expected_dates_for_balance(split: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    ing_p = split["ing_pend"].copy()
    ing_p["fecha_origen"] = pd.to_datetime(ing_p.get(COL_FECHA), errors="coerce")
    ing_p["fecha_esperada"] = pd.to_datetime(ing_p.get(COL_FECHA_COBRO), errors="coerce")
    ing_p["monto"] = pd.to_numeric(ing_p.get(COL_MONTO), errors="coerce").fillna(0.0)

    gas_p = split["gas_pend"].copy()
    gas_p["fecha_origen"] = pd.to_datetime(gas_p.get(COL_FECHA), errors="coerce")
    gas_p["fecha_esperada"] = pd.to_datetime(gas_p.get("__fecha_pago_estimada"), errors="coerce")
    gas_p.loc[gas_p["fecha_esperada"].isna(), "fecha_esperada"] = pd.to_datetime(
        gas_p.loc[gas_p["fecha_esperada"].isna(), COL_FECHA],
        errors="coerce",
    )
    gas_p["monto"] = pd.to_numeric(gas_p.get(COL_MONTO), errors="coerce").fillna(0.0)
    return ing_p, gas_p


def _build_balance_snapshots(
    cash_movimientos: pd.DataFrame,
    split: dict[str, pd.DataFrame],
    period_label: str,
    fecha_desde: date,
    fecha_hasta: date,
) -> pd.DataFrame:
    freq = _freq_from_period_label(period_label)
    start = pd.Timestamp(fecha_desde)
    end = pd.Timestamp(fecha_hasta)
    if start > end:
        return pd.DataFrame()

    # Corte por periodo. Ej.: anual / cuatrimestral / mensual.
    cutoffs = pd.date_range(start=start, end=end, freq=freq)
    if len(cutoffs) == 0 or cutoffs[-1] != end:
        cutoffs = cutoffs.append(pd.DatetimeIndex([end]))

    mov = cash_movimientos.copy()
    mov[COL_FECHA] = pd.to_datetime(mov.get(COL_FECHA), errors="coerce")
    mov["flujo"] = pd.to_numeric(mov.get("flujo"), errors="coerce").fillna(0.0)
    mov = mov.dropna(subset=[COL_FECHA])

    ing_p, gas_p = _prepare_expected_dates_for_balance(split)

    rows: list[dict[str, object]] = []
    for cutoff in cutoffs:
        efectivo = float(mov.loc[mov[COL_FECHA] <= cutoff, "flujo"].sum()) if not mov.empty else 0.0

        # Supuesto gerencial: una cuenta pendiente existe desde fecha origen
        # hasta su fecha esperada; si no tiene fecha esperada, se mantiene abierta.
        cxc_mask = (
            (ing_p["fecha_origen"].isna() | (ing_p["fecha_origen"] <= cutoff))
            & (ing_p["fecha_esperada"].isna() | (ing_p["fecha_esperada"] > cutoff))
        )
        cxp_mask = (
            (gas_p["fecha_origen"].isna() | (gas_p["fecha_origen"] <= cutoff))
            & (gas_p["fecha_esperada"].isna() | (gas_p["fecha_esperada"] > cutoff))
        )

        cxc = float(ing_p.loc[cxc_mask, "monto"].sum()) if not ing_p.empty else 0.0
        cxp = float(gas_p.loc[cxp_mask, "monto"].sum()) if not gas_p.empty else 0.0
        activos = float(efectivo + cxc)
        pasivos = float(cxp)
        patrimonio = float(activos - pasivos)
        capital_trabajo = float(activos - pasivos)

        rows.append(
            {
                "corte": cutoff,
                "efectivo": efectivo,
                "cuentas_por_cobrar": cxc,
                "cuentas_por_pagar": cxp,
                "activos_totales": activos,
                "pasivos_totales": pasivos,
                "patrimonio_neto": patrimonio,
                "capital_trabajo": capital_trabajo,
            }
        )

    return pd.DataFrame(rows)


def _build_verificar_por_corregir(
    ing_f: pd.DataFrame,
    gas_f: pd.DataFrame,
    split: dict[str, pd.DataFrame],
) -> tuple[list[dict[str, object]], pd.DataFrame]:
    checks: list[dict[str, object]] = []
    issues: list[dict[str, object]] = []

    ing_pending = split["ing_pend"].copy()
    gas_pending = split["gas_pend"].copy()

    miss_ing_due = int(pd.to_datetime(ing_pending.get(COL_FECHA_COBRO), errors="coerce").isna().sum()) if not ing_pending.empty else 0
    miss_gas_due = int(pd.to_datetime(gas_pending.get("__fecha_pago_estimada"), errors="coerce").isna().sum()) if not gas_pending.empty else 0
    checks.append(
        {
            "check": "Fechas esperadas pendientes",
            "status": "OK" if (miss_ing_due + miss_gas_due) == 0 else "REVISAR",
            "detalle": f"Ingresos por cobrar sin fecha: {miss_ing_due} | Gastos por pagar sin fecha: {miss_gas_due}",
        }
    )

    def _append_issue(df: pd.DataFrame, source: str, mask: pd.Series, problem: str, action: str) -> None:
        if df is None or df.empty or mask is None or not mask.any():
            return
        subset = df.loc[mask].copy()
        for _, row in subset.head(300).iterrows():
            issues.append(
                {
                    "fuente": source,
                    "rowid": str(row.get("RowID", "")),
                    "fecha": row.get(COL_FECHA, pd.NaT),
                    "empresa": str(row.get(COL_EMPRESA, "")),
                    "categoria": str(row.get(COL_CATEGORIA, "")),
                    "monto": float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
                    "problema": problem,
                    "accion_sugerida": action,
                }
            )

    ing_monto = pd.to_numeric(ing_f.get(COL_MONTO), errors="coerce").fillna(0.0)
    gas_monto = pd.to_numeric(gas_f.get(COL_MONTO), errors="coerce").fillna(0.0)
    ing_fecha = pd.to_datetime(ing_f.get(COL_FECHA), errors="coerce")
    gas_fecha = pd.to_datetime(gas_f.get(COL_FECHA), errors="coerce")

    _append_issue(ing_f, "Ingresos", ing_fecha.isna(), "Fecha faltante", "Completar fecha del registro.")
    _append_issue(gas_f, "Gastos", gas_fecha.isna(), "Fecha faltante", "Completar fecha del registro.")
    _append_issue(ing_f, "Ingresos", ing_monto <= 0, "Monto <= 0", "Corregir monto positivo.")
    _append_issue(gas_f, "Gastos", gas_monto <= 0, "Monto <= 0", "Corregir monto positivo.")
    _append_issue(
        ing_pending,
        "Ingresos",
        pd.to_datetime(ing_pending.get(COL_FECHA_COBRO), errors="coerce").isna(),
        "Por cobrar sin fecha esperada",
        "Asignar Fecha de cobro esperada.",
    )
    _append_issue(
        gas_pending,
        "Gastos",
        pd.to_datetime(gas_pending.get("__fecha_pago_estimada"), errors="coerce").isna(),
        "Por pagar sin fecha esperada",
        "Asignar Fecha esperada de pago.",
    )

    # Deteccion de posibles gastos fijos no cargados.
    # Debe tolerar hojas incompletas o fechas invalidas sin romper la pagina.
    gas_fecha_series = (
        pd.to_datetime(gas_f[COL_FECHA], errors="coerce")
        if COL_FECHA in gas_f.columns
        else pd.Series(index=gas_f.index, dtype="datetime64[ns]")
    )
    valid_gas_dates = gas_fecha_series.dropna()
    today_ts = pd.Timestamp(date.today())
    if valid_gas_dates.empty:
        start_m = today_ts.to_period("M")
        end_m = today_ts.to_period("M")
    else:
        start_m = valid_gas_dates.min().to_period("M")
        end_candidate = valid_gas_dates.max().to_period("M")
        end_m = max(end_candidate, today_ts.to_period("M"))

    month_range = pd.period_range(start=start_m, end=end_m, freq="M")

    desc_s = gas_f[COL_DESC].astype(str) if COL_DESC in gas_f.columns else pd.Series("", index=gas_f.index)
    conc_s = gas_f[COL_CONCEPTO].astype(str) if COL_CONCEPTO in gas_f.columns else pd.Series("", index=gas_f.index)
    cat_s = gas_f[COL_CATEGORIA].astype(str) if COL_CATEGORIA in gas_f.columns else pd.Series("", index=gas_f.index)
    gas_text = (desc_s + " " + conc_s + " " + cat_s).str.lower()
    gas_month = gas_fecha_series.dt.to_period("M")
    fixed_rules = {
        "Alquiler": ["alquiler", "arrendamiento", "rent"],
        "Salarios": ["salario", "planilla", "nomina", "nómina"],
        "Seguros": ["seguro", "poliza", "póliza", "aseguradora"],
    }
    for label, kws in fixed_rules.items():
        if gas_f.empty:
            matched = pd.Series(dtype=bool)
        else:
            matched = gas_text.str.contains("|".join(kws), na=False)
        months_with = set(gas_month[matched].dropna().tolist()) if not gas_f.empty else set()
        missing = [m for m in month_range if m not in months_with]
        status = "OK" if not missing else "REVISAR"
        checks.append(
            {
                "check": f"Gasto fijo potencial: {label}",
                "status": status,
                "detalle": "Cobertura mensual completa." if not missing else f"Meses sin registro detectados: {len(missing)}",
            }
        )
        if missing:
            issues.append(
                {
                    "fuente": "Gastos",
                    "rowid": "",
                    "fecha": pd.NaT,
                    "empresa": "",
                    "categoria": "Gastos fijos",
                    "monto": 0.0,
                    "problema": f"Posible gasto fijo faltante: {label}",
                    "accion_sugerida": "Verificar meses faltantes y registrar si aplica.",
                }
            )

    checks.append(
        {
            "check": "Completitud operativa",
            "status": "OK" if len(issues) == 0 else "REVISAR",
            "detalle": f"Registros por corregir detectados: {len(issues)}",
        }
    )

    issues_df = pd.DataFrame(issues)
    return checks, issues_df


st.title("Panel Financiero Gerencial")
st.caption(
    "Vista gerencial y analitica construida sobre los mismos datos de Finanzas 1. "
    "No reemplaza ni modifica el flujo operativo actual."
)

if "finanzas2_cache_token" not in st.session_state:
    st.session_state["finanzas2_cache_token"] = uuid.uuid4().hex

try:
    cfg = get_finance_sheet_config()
except Exception as exc:
    st.error(f"No se pudo leer configuracion de hojas: {exc}")
    st.stop()

with st.spinner("Cargando datos financieros..."):
    data = load_finance_inputs(
        cfg["sheet_id"],
        cfg["ws_ing"],
        cfg["ws_gas"],
        st.session_state["finanzas2_cache_token"],
    )

df_ing = normalize_ingresos(data.get("ingresos", pd.DataFrame()))
df_gas = normalize_gastos(data.get("gastos", pd.DataFrame()))

combined_dates = pd.concat([df_ing[COL_FECHA], df_gas[COL_FECHA]], ignore_index=True).dropna()
if combined_dates.empty:
    min_date = date(date.today().year, 1, 1)
    max_date = date.today()
else:
    min_date = combined_dates.min().date()
    max_date = combined_dates.max().date()

opts = get_filter_options(df_ing, df_gas)

with st.sidebar:
    st.markdown("### Filtros globales")
    today = date.today()
    mes_inicio = date(today.year, today.month, 1)
    default_desde = max(min_date, mes_inicio)
    default_hasta = min(max_date, today)
    if default_desde > default_hasta:
        default_desde = min_date
        default_hasta = max_date

    modo_tiempo = st.radio(
        "Modo de tiempo",
        ["Periodo corriente (mes actual)", "Rango personalizado"],
        index=0,
        key="f2_modo_tiempo",
    )

    if modo_tiempo == "Rango personalizado":
        fecha_desde = st.date_input("Desde", value=default_desde, min_value=min_date, max_value=max_date, key="f2_desde")
        fecha_hasta = st.date_input("Hasta", value=default_hasta, min_value=min_date, max_value=max_date, key="f2_hasta")
    else:
        fecha_desde = default_desde
        fecha_hasta = default_hasta
        st.caption(f"Periodo activo: {fecha_desde.isoformat()} -> {fecha_hasta.isoformat()}")

    empresa_opt = ["Todas"] + opts["empresas"]
    empresa = st.selectbox("Empresa", options=empresa_opt, index=0, key="f2_empresa")

    escenarios_opts = opts["escenarios"]
    escenarios_sel = st.multiselect(
        "Escenario",
        options=escenarios_opts,
        default=escenarios_opts,
        key="f2_escenarios",
    )

    search = st.text_input("Busqueda", key="f2_search", placeholder="cliente, proyecto, categoria, proveedor...")

    vista_modo = st.radio("Vista", ["Consolidado", "Por empresa"], horizontal=False, key="f2_vista")

    include_misc = st.toggle(
        "Incluir Miscelaneos en rentabilidad",
        value=False,
        help="Se aplica a Estado de resultados y analisis gerencial. Caja y balance mantienen todos los movimientos.",
        key="f2_include_misc",
    )
    horizonte_proy_meses = st.selectbox(
        "Horizonte de proyeccion",
        options=[4, 6, 9],
        index=0,
        key="f2_horizonte_proyeccion",
    )
    use_recommended_periods = st.toggle(
        "Usar periodos recomendados",
        value=True,
        help=(
            "Activa los periodos recomendados por defecto: Flujo actual/proyectado Mensual, "
            "Estado de resultados Semestral y Balance con ultimo cierre mensual."
        ),
        key="f2_use_recommended_periods",
    )
    custom_periods = not bool(use_recommended_periods)

    if use_recommended_periods:
        st.session_state["f2_period_cash_actual"] = "Mensual"
        st.session_state["f2_period_cash_proj"] = "Mensual"
        st.session_state["f2_period_results"] = "Semestral"
        st.session_state["f2_period_balance"] = "Mensual"

    if modo_tiempo != "Rango personalizado":
        st.markdown("#### Ventanas de analisis")
        cash_window_label = st.selectbox(
            "Flujo de caja actual",
            options=["Mes corriente", "Ultimos 30 dias", "Mes anterior", "Trimestre corriente", "Año corriente"],
            index=0,
            key="f2_window_cash_actual",
        )
        results_window_label = st.selectbox(
            "Estado de resultados",
            options=["Ultimo semestre cerrado", "Ultimo cuatrimestre cerrado", "Ultimo año cerrado"],
            index=0,
            key="f2_window_results",
        )
        balance_window_label = st.selectbox(
            "Balance general",
            options=["Ultimo cierre mensual", "Ultimo cierre cuatrimestral", "Ultimo cierre anual"],
            index=0,
            key="f2_window_balance",
        )
    else:
        cash_window_label = "Rango personalizado"
        results_window_label = "Rango personalizado"
        balance_window_label = "Rango personalizado"

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Actualizar tablero", key="f2_refresh"):
            st.session_state["finanzas2_cache_token"] = uuid.uuid4().hex
            _safe_rerun()
    with col_b:
        if st.button("Limpiar filtros", key="f2_clear"):
            for k in [
                "f2_empresa",
                "f2_search",
                "f2_escenarios",
                "f2_vista",
                "f2_include_misc",
                "f2_modo_tiempo",
                "f2_period_cash_actual",
                "f2_period_cash_proj",
                "f2_period_results",
                "f2_period_balance",
                "f2_horizonte_proyeccion",
                "f2_use_recommended_periods",
                "f2_window_cash_actual",
                "f2_window_results",
                "f2_window_balance",
            ]:
                st.session_state.pop(k, None)
            st.session_state["f2_desde"] = default_desde
            st.session_state["f2_hasta"] = default_hasta
            _safe_rerun()

if fecha_desde > fecha_hasta:
    st.warning("El rango de fechas es invalido.")
    st.stop()

# Base de trabajo: mismos filtros de negocio (empresa/escenario/busqueda), con historico completo.
scope_filters = GlobalFilters(
    fecha_desde=min_date,
    fecha_hasta=max_date,
    empresa=empresa,
    busqueda=search,
    escenarios=escenarios_sel,
)
ing_scope, gas_scope = apply_global_filters(df_ing, df_gas, scope_filters)

if modo_tiempo == "Rango personalizado":
    cash_desde, cash_hasta = fecha_desde, fecha_hasta
    resultados_desde, resultados_hasta = fecha_desde, fecha_hasta
    balance_desde, balance_hasta = fecha_desde, fecha_hasta
    results_period_default = st.session_state.get("f2_period_results", "Semestral")
    balance_period_default = st.session_state.get("f2_period_balance", "Mensual")
else:
    today_ref = date.today()
    cash_desde, cash_hasta = _resolve_cash_window(cash_window_label, today_ref, min_date, max_date)
    resultados_desde, resultados_hasta = _resolve_results_window(results_window_label, today_ref, min_date, max_date)
    balance_desde, balance_hasta = _resolve_balance_window(balance_window_label, today_ref, min_date, max_date)
    results_period_default = {
        "Ultimo semestre cerrado": "Semestral",
        "Ultimo cuatrimestre cerrado": "Cuatrimestral",
        "Ultimo año cerrado": "Anual",
    }.get(results_window_label, "Semestral")
    balance_period_default = {
        "Ultimo cierre mensual": "Mensual",
        "Ultimo cierre cuatrimestral": "Cuatrimestral",
        "Ultimo cierre anual": "Anual",
    }.get(balance_window_label, "Mensual")

# Flujos/KPIs del periodo operativo visible (default: mes actual).
ing_f = _filter_df_window(ing_scope, cash_desde, cash_hasta)
gas_f = _filter_df_window(gas_scope, cash_desde, cash_hasta)
split = split_real_vs_pending(ing_f, gas_f)

# Proyeccion: usa historico completo pendiente.
split_proj = split_real_vs_pending(ing_scope, gas_scope)

# Estado de resultados: periodo cerrado por defecto, editable por rango personalizado.
ing_res = _filter_df_window(ing_scope, resultados_desde, resultados_hasta)
gas_res = _filter_df_window(gas_scope, resultados_desde, resultados_hasta)

# Balance: usa el historico completo para calcular correctamente el corte seleccionado.
split_balance = split_real_vs_pending(ing_scope, gas_scope)

cash_actual = build_cashflow_actual(split["ing_real"], split["gas_real"])
cash_balance = build_cashflow_actual(split_balance["ing_real"], split_balance["gas_real"])
cxc_df = build_cuentas_por_cobrar(split["ing_pend"])
cxp_df, cxp_quality = build_cuentas_por_pagar(split["gas_pend"])
cxc_total = float(cxc_df["monto"].sum()) if not cxc_df.empty else 0.0
cxp_total = float(cxp_df["monto"].sum()) if not cxp_df.empty else 0.0

proyectado = build_cashflow_proyectado(
    split_proj["ing_pend"],
    split_proj["gas_pend"],
    saldo_inicial=cash_actual["metricas"]["efectivo_actual"],
    granularidad="D",
    horizon_months=int(horizonte_proy_meses),
)

estado = build_estado_resultados(ing_res, gas_res, include_miscelaneos=include_misc)
balance_snapshots_summary = _build_balance_snapshots(
    cash_movimientos=cash_balance["movimientos"],
    split=split_balance,
    period_label=balance_period_default,
    fecha_desde=balance_desde,
    fecha_hasta=balance_hasta,
)
latest_balance_summary = (
    balance_snapshots_summary.sort_values("corte").iloc[-1].to_dict()
    if not balance_snapshots_summary.empty
    else {}
)
balance = build_balance_general_simplificado(
    efectivo_actual=float(latest_balance_summary.get("efectivo", 0.0)),
    cuentas_por_cobrar=float(latest_balance_summary.get("cuentas_por_cobrar", 0.0)),
    cuentas_por_pagar=float(latest_balance_summary.get("cuentas_por_pagar", 0.0)),
)

analisis = build_analisis_gerencial(ing_f, gas_f, cxc_df, include_miscelaneos=include_misc)

metricas_resumen = [
    ("Efectivo actual", format_money_es(cash_actual["metricas"]["efectivo_actual"])),
    ("Flujo neto del periodo", format_money_es(cash_actual["metricas"]["flujo_neto"])),
    ("Cuentas por cobrar", format_money_es(cxc_total)),
    ("Cuentas por pagar", format_money_es(cxp_total)),
    ("Capital de trabajo", format_money_es(balance["metricas"]["capital_trabajo"])),
    ("Utilidad del periodo", format_money_es(estado["metricas"]["utilidad_operativa"])),
    ("Margen del periodo", format_percent_es(estado["metricas"]["margen_operativo"])),
    ("Saldo proyectado final", format_money_es(proyectado["metricas"]["saldo_proyectado_final"])),
    ("Posicion financiera neta", format_money_es(balance["metricas"]["posicion_financiera_neta"])),
]

checks_summary, issues_df = _build_verificar_por_corregir(ing_f, gas_f, split)
checks_df = pd.DataFrame(checks_summary)

# Periodicidad por defecto recomendada. Solo se edita si el usuario desactiva
# "Usar periodos recomendados".
period_cash_actual = "Mensual"
period_cash_proj = "Mensual"
period_results = str(results_period_default)
period_balance = str(balance_period_default)

st.markdown("## Resumen Ejecutivo")
_render_kpi_row(metricas_resumen, cols=3)

resumen_df = pd.DataFrame(metricas_resumen, columns=["Indicador", "Valor"])
with st.sidebar:
    _series_to_csv_download(resumen_df, "finanzas2_resumen.csv", "Exportar resumen")

st.caption(
    "Reglas base: flujo de caja en base a cobrado/pagado; estado de resultados gerencial (no contable completo); "
    "balance general simplificado con informacion disponible."
)

tab_a, tab_b, tab_c, tab_d, tab_e, tab_f, tab_g, tab_h, tab_i = st.tabs(
    [
        "A. Resumen Ejecutivo",
        "B. Flujo de Caja Actual",
        "C. Flujo de Caja Proyectado",
        "D. Estado de Resultados",
        "E. Balance General",
        "F. Cuentas por Cobrar y por Pagar",
        "G. Analisis Gerencial",
        "H. Verificar",
        "I. Por corregir",
    ]
)

with tab_a:
    st.markdown("### Estado general")
    st.write(
        f"Rango analizado: **{fecha_desde.isoformat()}** a **{fecha_hasta.isoformat()}** | "
        f"Empresa: **{empresa}** | Vista: **{vista_modo}**"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Tension de liquidez")
        tension = cxc_total - cxp_total
        st.metric("Cuentas por cobrar - por pagar", format_money_es(tension))
        if tension < 0:
            st.warning("La cartera pendiente no cubre las obligaciones pendientes en el periodo filtrado.")
        elif tension == 0:
            st.info("Cartera pendiente y obligaciones pendientes estan equilibradas.")
        else:
            st.success("La cartera pendiente supera las obligaciones pendientes.")

    with col2:
        st.markdown("#### Calidad de proyeccion")
        st.write(f"Pagos pendientes con fecha fallback: **{cxp_quality['con_fallback_fecha']}**")
        st.write(f"Pagos pendientes sin fecha usable: **{cxp_quality['sin_fecha']}**")
        if cxp_quality["con_fallback_fecha"] > 0:
            st.caption("Supuesto aplicado: cuando no existe fecha estimada de pago, se usa Fecha del registro del gasto.")

    revisar_count = int((checks_df.get("status", pd.Series(dtype=str)) == "REVISAR").sum()) if not checks_df.empty else 0
    if revisar_count > 0:
        st.warning(f"Hay {revisar_count} chequeo(s) gerenciales marcados para revision.")
    else:
        st.success("No se detectaron alertas en los chequeos gerenciales.")

with tab_b:
    st.markdown("### Flujo de caja actual")
    st.caption("Base caja: ingresos cobrados (Por_cobrar=No) y gastos pagados (Por_pagar=No).")
    st.caption(f"Ventana usada: {cash_desde.isoformat()} -> {cash_hasta.isoformat()}")

    if custom_periods:
        period_cash_actual = st.selectbox(
            "Periodo (flujo actual)",
            options=PERIOD_OPTIONS_CASH,
            index=PERIOD_OPTIONS_CASH.index("Mensual"),
            key="f2_period_cash_actual",
        )
    else:
        st.caption("Periodicidad por defecto aplicada: Mensual.")

    kpis_actual = [
        ("Entradas reales", format_money_es(cash_actual["metricas"]["entradas_reales"])),
        ("Salidas reales", format_money_es(cash_actual["metricas"]["salidas_reales"])),
        ("Flujo neto", format_money_es(cash_actual["metricas"]["flujo_neto"])),
        ("Saldo acumulado", format_money_es(cash_actual["metricas"]["efectivo_actual"])),
    ]
    _render_kpi_row(kpis_actual, cols=4)

    serie_actual_raw = cash_actual["serie"]
    serie_actual = _aggregate_cash_series(serie_actual_raw, period_cash_actual)

    if not serie_actual.empty:
        fmt = "%Y-%m-%d" if period_cash_actual in {"Diario", "Semanal"} else "%Y-%m"
        c1, c2 = st.columns([2, 1])
        with c1:
            _line_chart(serie_actual, COL_FECHA, "saldo", "Saldo acumulado", color="#22c55e")
        with c2:
            _bar_chart(
                serie_actual.tail(24).assign(periodo=lambda d: d[COL_FECHA].dt.strftime(fmt)),
                "periodo",
                "flujo",
                f"Flujo {period_cash_actual.lower()}",
                color="#0ea5e9",
            )
    else:
        st.info("No hay movimientos reales en el periodo filtrado.")

    if vista_modo == "Por empresa" and not cash_actual["movimientos"].empty:
        st.markdown("#### Flujo por empresa")
        por_empresa = (
            cash_actual["movimientos"].groupby("Empresa", as_index=False)["flujo"].sum().sort_values("flujo", ascending=False)
        )
        _bar_chart(
            por_empresa.rename(columns={"Empresa": "empresa"}),
            "empresa",
            "flujo",
            "Flujo neto por empresa",
            color="#22c55e",
        )

with tab_c:
    st.markdown("### Flujo de caja proyectado")
    st.caption(
        "Incluye cobros pendientes y pagos pendientes futuros. Si no existe fecha estimada de pago, "
        "se usa la Fecha del registro como fallback y se reporta en calidad de datos."
    )
    st.caption(f"Horizonte activo: {int(horizonte_proy_meses)} meses desde el mes actual.")
    proj_inicio, proj_fin = _projection_window(int(horizonte_proy_meses))
    st.caption(f"Ventana proyectada: {proj_inicio.isoformat()} -> {proj_fin.isoformat()}")

    if custom_periods:
        period_cash_proj = st.selectbox(
            "Periodo (flujo proyectado)",
            options=PERIOD_OPTIONS_CASH,
            index=PERIOD_OPTIONS_CASH.index("Mensual"),
            key="f2_period_cash_proj",
        )
    else:
        st.caption("Periodicidad por defecto aplicada: Mensual.")

    kpis_proj = [
        ("Saldo inicial", format_money_es(proyectado["metricas"]["saldo_inicial"])),
        ("Cobros futuros", format_money_es(proyectado["metricas"]["cobros_futuros"])),
        ("Pagos futuros", format_money_es(proyectado["metricas"]["pagos_futuros"])),
        ("Flujo neto proyectado", format_money_es(proyectado["metricas"]["flujo_neto_proyectado"])),
        ("Saldo final proyectado", format_money_es(proyectado["metricas"]["saldo_proyectado_final"])),
    ]
    _render_kpi_row(kpis_proj, cols=5)

    for note in proyectado.get("notas", []):
        st.caption(f"- {note}")

    serie_proj_base = (
        proyectado["serie"]
        .rename(columns={"fecha_evento": COL_FECHA, "flujo_proyectado": "flujo", "saldo_proyectado": "saldo"})
        .copy()
    )
    serie_proj = _aggregate_cash_series(serie_proj_base, period_cash_proj)
    if not serie_proj.empty:
        fmt = "%Y-%m-%d" if period_cash_proj in {"Diario", "Semanal"} else "%Y-%m"
        c1, c2 = st.columns([2, 1])
        with c1:
            _line_chart(serie_proj, COL_FECHA, "saldo", "Saldo proyectado", color="#f59e0b")
        with c2:
            _bar_chart(
                serie_proj.tail(24).assign(periodo=lambda d: d[COL_FECHA].dt.strftime(fmt)),
                "periodo",
                "flujo",
                f"Flujo proyectado {period_cash_proj.lower()}",
                color="#f59e0b",
            )
        st.markdown("#### Eventos futuros")
        st.dataframe(proyectado["eventos"], use_container_width=True, hide_index=True)
    else:
        st.info("No hay eventos de cobro/pago pendientes para proyectar en el rango filtrado.")

with tab_d:
    st.markdown("### Estado de resultados (gerencial)")
    st.caption(f"Periodo analizado: {resultados_desde.isoformat()} -> {resultados_hasta.isoformat()}")
    st.caption(f"Tipo de periodo: {results_period_default}")
    if custom_periods:
        period_results = st.selectbox(
            "Periodo (estado de resultados)",
            options=PERIOD_OPTIONS_RESULTS,
            index=PERIOD_OPTIONS_RESULTS.index(results_period_default),
            key="f2_period_results",
        )
    else:
        st.caption(f"Periodicidad por defecto aplicada: {results_period_default}.")

    for note in estado.get("notas", []):
        st.caption(f"- {note}")

    st.dataframe(
        estado["estado"],
        use_container_width=True,
        hide_index=True,
        column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
    )

    resultados_periodo = _aggregate_resultados_periodo(estado["mensual"], period_results)
    if not resultados_periodo.empty:
        st.markdown(f"#### Evolucion {period_results.lower()}")
        df_period = resultados_periodo.copy().melt(
            id_vars=["Periodo"],
            value_vars=["Ingresos", "Gastos", "Utilidad"],
            var_name="Rubro",
            value_name="Monto",
        )
        chart = (
            alt.Chart(df_period)
            .mark_line(point=True)
            .encode(
                x=alt.X("Periodo:T", title=period_results),
                y=alt.Y("Monto:Q", title="Monto"),
                color=alt.Color("Rubro:N", title="Rubro"),
                tooltip=["Periodo:T", "Rubro:N", alt.Tooltip("Monto:Q", format=",.2f")],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

        st.dataframe(
            resultados_periodo,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
                "Gastos": st.column_config.NumberColumn("Gastos", format="$%0.2f"),
                "Utilidad": st.column_config.NumberColumn("Utilidad", format="$%0.2f"),
            },
        )
    else:
        st.info("Sin datos para construir el estado de resultados en el periodo seleccionado.")

    if not estado["por_empresa"].empty:
        st.markdown("#### Desglose por empresa")
        st.dataframe(
            estado["por_empresa"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
                "Gastos": st.column_config.NumberColumn("Gastos", format="$%0.2f"),
                "Utilidad": st.column_config.NumberColumn("Utilidad", format="$%0.2f"),
            },
        )

with tab_e:
    st.markdown("### Balance general (simplificado)")
    st.caption(f"Periodo analizado: {balance_desde.isoformat()} -> {balance_hasta.isoformat()}")
    st.caption(f"Tipo de corte: {balance_period_default}")
    if custom_periods:
        period_balance = st.selectbox(
            "Periodo (balance)",
            options=PERIOD_OPTIONS_BALANCE,
            index=PERIOD_OPTIONS_BALANCE.index(balance_period_default),
            key="f2_period_balance",
        )
    else:
        st.caption(f"Periodicidad por defecto aplicada: {balance_period_default}.")

    snapshots = _build_balance_snapshots(
        cash_movimientos=cash_balance["movimientos"],
        split=split_balance,
        period_label=period_balance,
        fecha_desde=balance_desde,
        fecha_hasta=balance_hasta,
    )
    snapshots = snapshots.sort_values("corte") if not snapshots.empty else snapshots

    for note in balance.get("notas", []):
        st.caption(f"- {note}")

    if not snapshots.empty:
        st.markdown(f"#### Serie de cortes ({period_balance.lower()})")
        _line_chart(
            snapshots.rename(columns={"corte": COL_FECHA, "patrimonio_neto": "saldo"}),
            COL_FECHA,
            "saldo",
            "Patrimonio neto estimado",
            color="#38bdf8",
        )

        latest = snapshots.iloc[-1]
        activos_df = pd.DataFrame(
            [
                {"Cuenta": "Efectivo y equivalentes", "Monto": float(latest["efectivo"])},
                {"Cuenta": "Cuentas por cobrar", "Monto": float(latest["cuentas_por_cobrar"])},
            ]
        )
        pasivos_df = pd.DataFrame(
            [
                {"Cuenta": "Cuentas por pagar", "Monto": float(latest["cuentas_por_pagar"])},
            ]
        )
        patrimonio_df = pd.DataFrame(
            [
                {"Cuenta": "Patrimonio neto estimado", "Monto": float(latest["patrimonio_neto"])},
            ]
        )
        total_activos = float(latest["activos_totales"])
        total_pasivos = float(latest["pasivos_totales"])
        patrimonio_neto = float(latest["patrimonio_neto"])
        capital_trabajo = float(latest["capital_trabajo"])
    else:
        activos_df = balance["activos"]
        pasivos_df = balance["pasivos"]
        patrimonio_df = balance["patrimonio"]
        total_activos = float(balance["metricas"]["total_activos"])
        total_pasivos = float(balance["metricas"]["total_pasivos"])
        patrimonio_neto = float(balance["metricas"]["patrimonio_neto"])
        capital_trabajo = float(balance["metricas"]["capital_trabajo"])

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("#### Activos")
        st.dataframe(
            activos_df,
            use_container_width=True,
            hide_index=True,
            column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
        )
    with c2:
        st.markdown("#### Pasivos")
        st.dataframe(
            pasivos_df,
            use_container_width=True,
            hide_index=True,
            column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
        )
    with c3:
        st.markdown("#### Patrimonio")
        st.dataframe(
            patrimonio_df,
            use_container_width=True,
            hide_index=True,
            column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
        )

    st.markdown("#### Totales")
    totals = pd.DataFrame(
        [
            {"Indicador": "Total activos", "Monto": total_activos},
            {"Indicador": "Total pasivos", "Monto": total_pasivos},
            {"Indicador": "Patrimonio neto estimado", "Monto": patrimonio_neto},
            {"Indicador": "Capital de trabajo", "Monto": capital_trabajo},
        ]
    )
    st.dataframe(
        totals,
        use_container_width=True,
        hide_index=True,
        column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
    )

with tab_f:
    st.markdown("### Cuentas por cobrar y por pagar")

    cxc_cols, cxp_cols = st.columns(2)
    with cxc_cols:
        st.markdown("#### Cuentas por cobrar")
        st.metric("Total CxC", format_money_es(cxc_total))
        st.dataframe(
            cxc_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "monto": st.column_config.NumberColumn("Monto", format="$%0.2f"),
                "fecha_esperada_cobro": st.column_config.DateColumn("Fecha esperada"),
            },
        )
    with cxp_cols:
        st.markdown("#### Cuentas por pagar")
        st.metric("Total CxP", format_money_es(cxp_total))
        st.dataframe(
            cxp_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "monto": st.column_config.NumberColumn("Monto", format="$%0.2f"),
                "fecha_esperada_pago": st.column_config.DateColumn("Fecha esperada"),
            },
        )

    st.caption(
        f"Calidad CxP: fallback de fecha en {cxp_quality['con_fallback_fecha']} registros; "
        f"sin fecha util en {cxp_quality['sin_fecha']} registros."
    )

with tab_g:
    st.markdown("### Analisis gerencial")

    if not analisis["por_empresa"].empty:
        st.markdown("#### Ingresos, gastos y utilidad por empresa")
        st.dataframe(
            analisis["por_empresa"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
                "gastos": st.column_config.NumberColumn("Gastos", format="$%0.2f"),
                "utilidad": st.column_config.NumberColumn("Utilidad", format="$%0.2f"),
            },
        )

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Top categorias de gasto")
        _bar_chart(analisis["top_gastos_categoria"], "categoria", "gasto", "Gasto")

    with c2:
        st.markdown("#### Concentracion de cuentas por cobrar")
        _bar_chart(analisis["concentracion_cxc"].head(10), "cliente", "monto", "Monto CxC", color="#f59e0b")

    st.markdown("#### Evolucion mensual")
    evo = analisis["evolucion_mensual"]
    if not evo.empty:
        evo_long = evo.melt(id_vars=["mes"], value_vars=["ingresos", "gastos", "utilidad"], var_name="serie", value_name="monto")
        chart_evo = (
            alt.Chart(evo_long)
            .mark_line(point=True)
            .encode(
                x=alt.X("mes:T", title="Mes"),
                y=alt.Y("monto:Q", title="Monto"),
                color="serie:N",
                tooltip=["mes:T", "serie:N", alt.Tooltip("monto:Q", format=",.2f")],
            )
            .properties(height=320)
        )
        st.altair_chart(chart_evo, use_container_width=True)
    else:
        st.info("Sin datos para evolucion mensual.")

    st.markdown("#### Concentracion de ingresos por cliente")
    st.dataframe(
        analisis["concentracion_cliente"].head(15),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
            "participacion_pct": st.column_config.NumberColumn("Participacion %", format="%0.2f"),
        },
    )

    st.markdown("#### Concentracion de ingresos por proyecto")
    st.dataframe(
        analisis["concentracion_proyecto"].head(15),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
        },
    )

with tab_h:
    st.markdown("### Verificar")
    st.caption("Chequeos automaticos para validar calidad operativa de datos en Finanzas 1.")
    if checks_df.empty:
        st.info("No se generaron chequeos con los filtros actuales.")
    else:
        ok_count = int((checks_df["status"] == "OK").sum())
        revisar_count = int((checks_df["status"] == "REVISAR").sum())
        c1, c2, c3 = st.columns(3)
        c1.metric("Total chequeos", str(len(checks_df)))
        c2.metric("OK", str(ok_count))
        c3.metric("Revisar", str(revisar_count))
        st.dataframe(checks_df, use_container_width=True, hide_index=True)

with tab_i:
    st.markdown("### Por corregir")
    st.caption("Registros y alertas detectadas que conviene completar o ajustar para mejorar el panel gerencial.")
    if issues_df.empty:
        st.success("No se detectaron registros por corregir con los filtros actuales.")
    else:
        src_opts = ["Todos"] + sorted([x for x in issues_df["fuente"].dropna().astype(str).unique().tolist() if x])
        problem_opts = ["Todos"] + sorted([x for x in issues_df["problema"].dropna().astype(str).unique().tolist() if x])
        f1, f2 = st.columns(2)
        with f1:
            src_sel = st.selectbox("Filtrar por fuente", src_opts, index=0, key="f2_issue_fuente")
        with f2:
            prob_sel = st.selectbox("Filtrar por problema", problem_opts, index=0, key="f2_issue_problema")

        issues_view = issues_df.copy()
        if src_sel != "Todos":
            issues_view = issues_view[issues_view["fuente"] == src_sel]
        if prob_sel != "Todos":
            issues_view = issues_view[issues_view["problema"] == prob_sel]

        st.dataframe(
            issues_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "monto": st.column_config.NumberColumn("Monto", format="$%0.2f"),
                "fecha": st.column_config.DateColumn("Fecha"),
            },
        )
        _series_to_csv_download(issues_view, "finanzas2_por_corregir.csv", "Exportar por corregir")

st.markdown("---")
st.caption(
    "Panel Financiero Gerencial prioriza lectura gerencial. Finanzas 1 se mantiene intacta para captura/operacion. "
    "Para un modelo contable mas robusto faltaria: calendario de pagos formal, catalogo contable y clasificacion de costos directos estandarizada."
)
