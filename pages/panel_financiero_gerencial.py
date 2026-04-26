from __future__ import annotations

import inspect
import json
import uuid
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st
from services.finance_opening import get_finance_opening_config, opening_amount_for_filter

from core.finance_v2 import constants as f2c
from core.finance_v2.analysis import (
    build_analisis_gerencial,
    build_cuentas_por_cobrar,
    build_cuentas_por_pagar,
)
from core.finance_v2.cashflow import build_cashflow_actual, build_cashflow_proyectado
from core.finance_v2.helpers import format_money_es, format_number_es, format_percent_es
from core.finance_v2.loaders import get_finance_sheet_config, load_finance_inputs
from core.finance_v2.statements import (
    build_balance_general_simplificado,
    build_estado_resultados,
    compute_balance_components,
)
from core.finance_v2.transforms import (
    GlobalFilters,
    apply_global_filters,
    get_filter_options,
    normalize_gastos,
    normalize_ingresos,
    split_real_vs_pending,
)
from ui.theme import apply_global_theme


COL_FECHA = getattr(f2c, "COL_FECHA", "Fecha")
COL_CATEGORIA = getattr(f2c, "COL_CATEGORIA", "Categoria")
COL_CLIENTE_NOMBRE = getattr(f2c, "COL_CLIENTE_NOMBRE", "ClienteNombre")
COL_CONCEPTO = getattr(f2c, "COL_CONCEPTO", "Concepto")
COL_CONTRAPARTE = getattr(f2c, "COL_CONTRAPARTE", "Contraparte")
COL_DESC = getattr(f2c, "COL_DESC", "Descripcion")
COL_EMPRESA = getattr(f2c, "COL_EMPRESA", "Empresa")
COL_FACTORING_DET = getattr(f2c, "COL_FACTORING_DETALLE", "Detalle factoring")
COL_FECHA_COBRO = getattr(f2c, "COL_FECHA_COBRO", "Fecha de cobro")
COL_FECHA_REAL_COBRO = getattr(f2c, "COL_FECHA_REAL_COBRO", "Fecha real de cobro")
COL_FECHA_REAL_PAGO = getattr(f2c, "COL_FECHA_REAL_PAGO", "Fecha real de pago")
COL_FINANCIAMIENTO_CRONOGRAMA = getattr(f2c, "COL_FINANCIAMIENTO_CRONOGRAMA", "Cronograma financiamiento")
COL_FINANCIAMIENTO_FECHA_INICIO = getattr(f2c, "COL_FINANCIAMIENTO_FECHA_INICIO", "Fecha inicio financiamiento")
COL_FINANCIAMIENTO_INSTRUMENTO = getattr(f2c, "COL_FINANCIAMIENTO_INSTRUMENTO", "Instrumento financiero")
COL_FINANCIAMIENTO_MONTO = getattr(f2c, "COL_FINANCIAMIENTO_MONTO", "Monto principal financiamiento")
COL_FINANCIAMIENTO_REG_TIPO = getattr(f2c, "COL_FINANCIAMIENTO_REG_TIPO", "Registro financiamiento")
COL_FINANCIAMIENTO_TIPO = getattr(f2c, "COL_FINANCIAMIENTO_TIPO", "Tipo financiamiento")
COL_INVENTARIO_ITEM = getattr(f2c, "COL_INVENTARIO_ITEM", "Item inventario")
COL_INVENTARIO_FECHA_LLEGADA = getattr(f2c, "COL_INVENTARIO_FECHA_LLEGADA", "Fecha llegada inventario")
COL_INVENTARIO_MOVIMIENTO = getattr(f2c, "COL_INVENTARIO_MOVIMIENTO", "Movimiento inventario")
COL_MONTO = getattr(f2c, "COL_MONTO", "Monto")
COL_MONTO_REAL_COBRADO = getattr(f2c, "COL_MONTO_REAL_COBRADO", "Monto real cobrado")
COL_MONTO_REAL_PAGADO = getattr(f2c, "COL_MONTO_REAL_PAGADO", "Monto real pagado")
COL_POR_COBRAR = getattr(f2c, "COL_POR_COBRAR", "Por_cobrar")
COL_POR_PAGAR = getattr(f2c, "COL_POR_PAGAR", "Por_pagar")
COL_PREPAGO_FECHA_INICIO = getattr(f2c, "COL_PREPAGO_FECHA_INICIO", "Fecha inicio prepago")
COL_PREPAGO_MESES = getattr(f2c, "COL_PREPAGO_MESES", "Plazo prepago meses")
COL_PROVEEDOR = getattr(f2c, "COL_PROVEEDOR", "Proveedor")
COL_PROYECTO = getattr(f2c, "COL_PROYECTO", "Proyecto")
COL_TIPO_CONTRAPARTE = getattr(f2c, "COL_TIPO_CONTRAPARTE", "Tipo contraparte")
COL_TRATAMIENTO_BALANCE_ING = getattr(f2c, "COL_TRATAMIENTO_BALANCE_ING", "Tratamiento balance ingreso")
COL_TRATAMIENTO_BALANCE_GAS = getattr(f2c, "COL_TRATAMIENTO_BALANCE_GAS", "Tratamiento balance gasto")


st.set_page_config(page_title="Panel Financiero Gerencial", page_icon="\U0001f4c8", layout="wide")
apply_global_theme()

if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")


F2_DEFAULTS_VERSION = "2026-04-26-defaults-v6"


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
    st.session_state["f2_window_cash_actual"] = "Desde apertura"
    st.session_state["f2_window_results"] = "Desde apertura"
    st.session_state["f2_window_balance"] = "Desde apertura hasta hoy"
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


def _build_cashflow_actual_with_opening(
    df_ing_real: pd.DataFrame,
    df_gas_real: pd.DataFrame,
    saldo_inicial: float,
) -> dict:
    try:
        return build_cashflow_actual(df_ing_real, df_gas_real, saldo_inicial=saldo_inicial)
    except TypeError:
        out = build_cashflow_actual(df_ing_real, df_gas_real)
        if not isinstance(out, dict):
            return {
                "movimientos": pd.DataFrame(columns=[COL_FECHA, COL_EMPRESA, "tipo", "flujo"]),
                "serie": pd.DataFrame(columns=[COL_FECHA, "flujo", "saldo"]),
                "metricas": {
                    "entradas_reales": 0.0,
                    "salidas_reales": 0.0,
                    "flujo_neto": 0.0,
                    "efectivo_actual": float(saldo_inicial),
                },
            }
        serie = out.get("serie")
        if isinstance(serie, pd.DataFrame) and not serie.empty and "saldo" in serie.columns:
            serie = serie.copy()
            serie["saldo"] = pd.to_numeric(serie["saldo"], errors="coerce").fillna(0.0) + float(saldo_inicial)
            out["serie"] = serie
        metricas = dict(out.get("metricas", {}) or {})
        metricas["efectivo_actual"] = float(metricas.get("efectivo_actual", 0.0)) + float(saldo_inicial)
        out["metricas"] = metricas
        return out


PERIOD_OPTIONS_CASH = ["Mensual", "Semanal", "Diario"]
PERIOD_OPTIONS_RESULTS = ["Semestral", "Cuatrimestral", "Mensual", "Trimestral", "Anual"]
PERIOD_OPTIONS_BALANCE = ["Mensual", "Cuatrimestral", "Anual"]


def _freq_from_period_label(label: str) -> str:
    key = str(label or "").strip().lower()
    mapping = {
        "diario": "D",
        "semanal": "W",
        "mensual": "ME",
        "semestral": "6MS",
        "trimestral": "QE",
        "cuatrimestral": "4MS",
        "anual": "YE",
    }
    freq = mapping.get(key, "ME")
    try:
        pd.tseries.frequencies.to_offset(freq)
    except Exception:
        return "ME"
    return freq


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


def _resolve_cash_window(
    label: str,
    today_ref: date,
    min_date: date,
    max_date: date,
    opening_date: date | None = None,
) -> tuple[date, date]:
    if label == "Desde apertura" and opening_date is not None:
        return _clip_window(opening_date, today_ref, min_date, max_date)
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


def _resolve_results_window(
    label: str,
    today_ref: date,
    min_date: date,
    max_date: date,
    opening_date: date | None = None,
) -> tuple[date, date]:
    if label == "Desde apertura" and opening_date is not None:
        return _clip_window(opening_date, today_ref, min_date, max_date)
    if label == "Ultimo cuatrimestre cerrado":
        return _clip_window(*_last_closed_cuatrimester_window(today_ref), min_date, max_date)
    if label == "Ultimo año cerrado":
        return _clip_window(*_last_closed_year_window(today_ref), min_date, max_date)
    return _clip_window(*_last_closed_semester_window(today_ref), min_date, max_date)


def _resolve_balance_window(
    label: str,
    today_ref: date,
    min_date: date,
    max_date: date,
    opening_date: date | None = None,
) -> tuple[date, date]:
    if label == "Desde apertura hasta hoy" and opening_date is not None:
        return _clip_window(opening_date, today_ref, min_date, max_date)
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


def _opening_balance_components_for_filter(opening_cfg, empresa_filter: str) -> dict[str, float]:
    return {
        "prestamos_otorgados": opening_amount_for_filter(opening_cfg.loans_granted_by_company, empresa_filter),
        "prestamos_recibidos": opening_amount_for_filter(opening_cfg.loans_received_by_company, empresa_filter),
        "inventario": opening_amount_for_filter(opening_cfg.inventory_by_company, empresa_filter),
        "inventario_en_transito": opening_amount_for_filter(opening_cfg.inventory_transit_by_company, empresa_filter),
        "anticipos_prepagos": opening_amount_for_filter(opening_cfg.prepayments_by_company, empresa_filter),
        "activos_fijos_netos": opening_amount_for_filter(opening_cfg.fixed_assets_by_company, empresa_filter),
        "inversiones_participaciones": opening_amount_for_filter(opening_cfg.investments_by_company, empresa_filter),
        "factoring_retenido": opening_amount_for_filter(opening_cfg.factoring_retained_by_company, empresa_filter),
        "aportes_capital": opening_amount_for_filter(opening_cfg.capital_by_company, empresa_filter),
        "otras_deudas": opening_amount_for_filter(opening_cfg.other_debts_by_company, empresa_filter),
    }


def _prepare_expected_dates_for_balance(split: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    ing_p = split["ing_pend"].copy()
    if COL_TRATAMIENTO_BALANCE_ING in ing_p.columns:
        trat_ing = ing_p[COL_TRATAMIENTO_BALANCE_ING].astype(str)
        ing_p = ing_p[(trat_ing == "") | (trat_ing == "Cuenta por cobrar")].copy()
    ing_p["fecha_origen"] = pd.to_datetime(ing_p.get(COL_FECHA), errors="coerce")
    ing_p["fecha_esperada"] = pd.to_datetime(ing_p.get(COL_FECHA_COBRO), errors="coerce")
    ing_p["monto"] = pd.to_numeric(ing_p.get("__monto_pendiente", ing_p.get(COL_MONTO)), errors="coerce").fillna(0.0)

    gas_p = split["gas_pend"].copy()
    if COL_TRATAMIENTO_BALANCE_GAS in gas_p.columns:
        trat_gas = gas_p[COL_TRATAMIENTO_BALANCE_GAS].astype(str)
        gas_p = gas_p[trat_gas != "Cancelacion de pasivo / deuda"].copy()
    gas_p["fecha_origen"] = pd.to_datetime(gas_p.get(COL_FECHA), errors="coerce")
    gas_p["fecha_esperada"] = pd.to_datetime(gas_p.get("__fecha_pago_estimada"), errors="coerce")
    gas_p.loc[gas_p["fecha_esperada"].isna(), "fecha_esperada"] = pd.to_datetime(
        gas_p.loc[gas_p["fecha_esperada"].isna(), COL_FECHA],
        errors="coerce",
    )
    gas_p["monto"] = pd.to_numeric(gas_p.get("__monto_pendiente", gas_p.get(COL_MONTO)), errors="coerce").fillna(0.0)
    return ing_p, gas_p


def _build_balance_snapshots(
    cash_movimientos: pd.DataFrame,
    split: dict[str, pd.DataFrame],
    df_ing_scope: pd.DataFrame,
    df_gas_scope: pd.DataFrame,
    period_label: str,
    fecha_desde: date,
    fecha_hasta: date,
    efectivo_inicial: float = 0.0,
    opening_balance_extras: dict[str, float] | None = None,
) -> pd.DataFrame:
    freq = _freq_from_period_label(period_label)
    start = pd.Timestamp(fecha_desde)
    end = pd.Timestamp(fecha_hasta)
    if start > end:
        return pd.DataFrame()

    # Corte por periodo. Ej.: anual / cuatrimestral / mensual.
    try:
        cutoffs = pd.date_range(start=start, end=end, freq=freq)
    except Exception:
        cutoffs = pd.date_range(start=start, end=end, freq="ME")
    if len(cutoffs) == 0 or cutoffs[-1] != end:
        cutoffs = cutoffs.append(pd.DatetimeIndex([end]))

    mov = cash_movimientos.copy()
    mov[COL_FECHA] = pd.to_datetime(mov.get(COL_FECHA), errors="coerce")
    mov["flujo"] = pd.to_numeric(mov.get("flujo"), errors="coerce").fillna(0.0)
    mov = mov.dropna(subset=[COL_FECHA])

    ing_p, gas_p = _prepare_expected_dates_for_balance(split)

    opening_balance_extras = dict(opening_balance_extras or {})
    rows: list[dict[str, object]] = []
    for cutoff in cutoffs:
        efectivo = float(efectivo_inicial + (mov.loc[mov[COL_FECHA] <= cutoff, "flujo"].sum() if not mov.empty else 0.0))

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
        extras = compute_balance_components(df_ing_scope, df_gas_scope, cutoff_date=cutoff)
        for key, amount in opening_balance_extras.items():
            extras[key] = float(extras.get(key, 0.0)) + float(amount or 0.0)
        activos = float(
            efectivo
            + cxc
            + float(extras.get("prestamos_otorgados", 0.0))
            + float(extras.get("inventario", 0.0))
            + float(extras.get("inventario_en_transito", 0.0))
            + float(extras.get("anticipos_prepagos", 0.0))
            + float(extras.get("inversiones_participaciones", 0.0))
            + float(extras.get("factoring_retenido", 0.0))
            + float(extras.get("activos_fijos_netos", 0.0))
        )
        pasivos = float(cxp + float(extras.get("prestamos_recibidos", 0.0)) + float(extras.get("otras_deudas", 0.0)))
        patrimonio = float(activos - pasivos)
        capital_trabajo = float(
            efectivo
            + cxc
            + float(extras.get("inventario", 0.0))
            + float(extras.get("anticipos_prepagos", 0.0))
            + float(extras.get("factoring_retenido", 0.0))
            - cxp
            - float(extras.get("prestamos_recibidos", 0.0))
            - float(extras.get("otras_deudas", 0.0))
        )

        rows.append(
            {
                "corte": cutoff,
                "efectivo": efectivo,
                "cuentas_por_cobrar": cxc,
                "cuentas_por_pagar": cxp,
                "prestamos_otorgados": float(extras.get("prestamos_otorgados", 0.0)),
                "inventario": float(extras.get("inventario", 0.0)),
                "inventario_en_transito": float(extras.get("inventario_en_transito", 0.0)),
                "anticipos_prepagos": float(extras.get("anticipos_prepagos", 0.0)),
                "inversiones_participaciones": float(extras.get("inversiones_participaciones", 0.0)),
                "factoring_retenido": float(extras.get("factoring_retenido", 0.0)),
                "activos_fijos_netos": float(extras.get("activos_fijos_netos", 0.0)),
                "prestamos_recibidos": float(extras.get("prestamos_recibidos", 0.0)),
                "aportes_capital": float(extras.get("aportes_capital", 0.0)),
                "otras_deudas": float(extras.get("otras_deudas", 0.0)),
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

    def _ensure_bool_mask(values, *, index) -> pd.Series:
        if isinstance(values, pd.Series):
            mask = values.copy()
        elif values is None:
            mask = pd.Series(False, index=index)
        else:
            mask = pd.Series(values, index=index)
        mask = mask.reindex(index)
        try:
            mask = mask.astype("boolean")
        except Exception:
            mask = mask.map(lambda v: bool(v) if pd.notna(v) else False).astype("boolean")
        return mask.fillna(False).astype(bool)

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
        if df is None or df.empty or mask is None:
            return
        mask = _ensure_bool_mask(mask, index=df.index)
        if not mask.any():
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

    ing_real_amount = pd.to_numeric(ing_f.get(COL_MONTO_REAL_COBRADO), errors="coerce").fillna(0.0)
    gas_real_amount = pd.to_numeric(gas_f.get(COL_MONTO_REAL_PAGADO), errors="coerce").fillna(0.0)
    ing_real_date = pd.to_datetime(ing_f.get(COL_FECHA_REAL_COBRO), errors="coerce")
    gas_real_date = pd.to_datetime(gas_f.get(COL_FECHA_REAL_PAGO), errors="coerce")
    ing_counterparty = ing_f.get(COL_CONTRAPARTE, pd.Series("", index=ing_f.index)).astype(str).str.strip()
    gas_counterparty = gas_f.get(COL_CONTRAPARTE, pd.Series("", index=gas_f.index)).astype(str).str.strip()
    ing_balance = ing_f.get(COL_TRATAMIENTO_BALANCE_ING, pd.Series("", index=ing_f.index)).astype(str).str.strip()
    gas_balance = gas_f.get(COL_TRATAMIENTO_BALANCE_GAS, pd.Series("", index=gas_f.index)).astype(str).str.strip()

    ing_partial_missing_date = (
        _ensure_bool_mask(ing_f[COL_POR_COBRAR].map(lambda x: str(x).strip().lower()).ne("no"), index=ing_f.index)
        & _ensure_bool_mask(ing_real_amount.gt(0), index=ing_f.index)
        & _ensure_bool_mask(ing_real_date.isna(), index=ing_f.index)
    ) if COL_POR_COBRAR in ing_f.columns else pd.Series(False, index=ing_f.index)
    gas_partial_missing_date = (
        _ensure_bool_mask(gas_f[COL_POR_PAGAR].map(lambda x: str(x).strip().lower()).ne("no"), index=gas_f.index)
        & _ensure_bool_mask(gas_real_amount.gt(0), index=gas_f.index)
        & _ensure_bool_mask(gas_real_date.isna(), index=gas_f.index)
    ) if COL_POR_PAGAR in gas_f.columns else pd.Series(False, index=gas_f.index)
    prepago_missing_cfg = _ensure_bool_mask(gas_balance.eq("Anticipo / prepago"), index=gas_f.index) & (
        _ensure_bool_mask(pd.to_numeric(gas_f.get(COL_PREPAGO_MESES), errors="coerce").fillna(0).le(0), index=gas_f.index)
        | _ensure_bool_mask(pd.to_datetime(gas_f.get(COL_PREPAGO_FECHA_INICIO), errors="coerce").isna(), index=gas_f.index)
    )
    inventory_missing_cfg = _ensure_bool_mask(gas_balance.eq("Inventario"), index=gas_f.index) & (
        _ensure_bool_mask(gas_f.get(COL_INVENTARIO_MOVIMIENTO, pd.Series("", index=gas_f.index)).astype(str).str.strip().eq(""), index=gas_f.index)
        | _ensure_bool_mask(gas_f.get(COL_INVENTARIO_ITEM, pd.Series("", index=gas_f.index)).astype(str).str.strip().eq(""), index=gas_f.index)
    )
    factoring_raw = ing_f.get(COL_FACTORING_DET, pd.Series("", index=ing_f.index))
    factoring_detail = factoring_raw.map(_safe_factoring_detail)
    factoring_on = _ensure_bool_mask(factoring_detail.map(bool), index=ing_f.index)
    factoring_counterparty_missing = factoring_on & _ensure_bool_mask(ing_counterparty.eq(""), index=ing_f.index)
    factoring_bad_initial = _ensure_bool_mask(factoring_detail.map(
        lambda d: False if not d else abs(
            float(d.get("initial_cash_received", 0.0) or 0.0)
            + float(d.get("initial_retained", 0.0) or 0.0)
            + float(d.get("initial_fee", 0.0) or 0.0)
            - float(d.get("factored_amount", 0.0) or 0.0)
        ) > 0.01
    ), index=ing_f.index)
    factoring_bad_final = _ensure_bool_mask(factoring_detail.map(
        lambda d: False if not d else (
            abs(
                float(d.get("final_cash_received", 0.0) or 0.0)
                + float(d.get("final_fee", 0.0) or 0.0)
                - float(d.get("initial_retained", 0.0) or 0.0)
            ) > 0.01
            if pd.notna(pd.to_datetime(d.get("fecha_liquidacion_final"), errors="coerce"))
            else False
        )
    ), index=ing_f.index)
    ing_counterparty_missing = _ensure_bool_mask(ing_balance.isin(["Patrimonio", "Pasivo financiero"]), index=ing_f.index) & _ensure_bool_mask(ing_counterparty.eq(""), index=ing_f.index)
    gas_counterparty_missing = _ensure_bool_mask(gas_balance.isin(["Inversion / participacion en otra empresa", "Cuenta por cobrar / prestamo otorgado"]), index=gas_f.index) & _ensure_bool_mask(gas_counterparty.eq(""), index=gas_f.index)

    checks.append(
        {
            "check": "Cobros/pagos parciales consistentes",
            "status": "OK" if int(ing_partial_missing_date.sum() + gas_partial_missing_date.sum()) == 0 else "REVISAR",
            "detalle": (
                f"Ingresos parciales sin fecha real: {int(ing_partial_missing_date.sum())} | "
                f"Gastos parciales sin fecha real: {int(gas_partial_missing_date.sum())}"
            ),
        }
    )
    checks.append(
        {
            "check": "Prepagos configurados",
            "status": "OK" if int(prepago_missing_cfg.sum()) == 0 else "REVISAR",
            "detalle": f"Registros de prepago incompletos: {int(prepago_missing_cfg.sum())}",
        }
    )
    checks.append(
        {
            "check": "Inventario con datos minimos",
            "status": "OK" if int(inventory_missing_cfg.sum()) == 0 else "REVISAR",
            "detalle": f"Registros de inventario incompletos: {int(inventory_missing_cfg.sum())}",
        }
    )
    checks.append(
        {
            "check": "Contrapartes en movimientos especiales",
            "status": "OK" if int(ing_counterparty_missing.sum() + gas_counterparty_missing.sum() + factoring_counterparty_missing.sum()) == 0 else "REVISAR",
            "detalle": (
                f"Ingresos especiales sin contraparte: {int(ing_counterparty_missing.sum())} | "
                f"Gastos especiales sin contraparte: {int(gas_counterparty_missing.sum())} | "
                f"Factoring sin contraparte: {int(factoring_counterparty_missing.sum())}"
            ),
        }
    )
    checks.append(
        {
            "check": "Operaciones con factoring consistentes",
            "status": "OK" if int(factoring_bad_initial.sum() + factoring_bad_final.sum()) == 0 else "REVISAR",
            "detalle": (
                f"Factoring inicial inconsistente: {int(factoring_bad_initial.sum())} | "
                f"Liquidacion final inconsistente: {int(factoring_bad_final.sum())}"
            ),
        }
    )

    _append_issue(
        ing_f,
        "Ingresos",
        ing_partial_missing_date,
        "Cobro parcial sin fecha real",
        "Completar Fecha real de cobro o dejar el monto real en cero.",
    )
    _append_issue(
        gas_f,
        "Gastos",
        gas_partial_missing_date,
        "Pago parcial sin fecha real",
        "Completar Fecha real de pago o dejar el monto real en cero.",
    )
    _append_issue(
        gas_f,
        "Gastos",
        prepago_missing_cfg,
        "Prepago incompleto",
        "Completar plazo y fecha inicio del prepago.",
    )
    _append_issue(
        gas_f,
        "Gastos",
        inventory_missing_cfg,
        "Inventario incompleto",
        "Completar movimiento inventario e item / referencia.",
    )
    _append_issue(
        ing_f,
        "Ingresos",
        ing_counterparty_missing,
        "Movimiento especial sin contraparte",
        "Completar la contraparte del aporte o financiamiento.",
    )
    _append_issue(
        gas_f,
        "Gastos",
        gas_counterparty_missing,
        "Movimiento especial sin contraparte",
        "Completar la contraparte de la inversion o prestamo.",
    )
    _append_issue(
        ing_f,
        "Ingresos",
        factoring_counterparty_missing,
        "Operacion con factoring sin contraparte",
        "Completar la empresa de factoring / contraparte.",
    )
    _append_issue(
        ing_f,
        "Ingresos",
        factoring_bad_initial,
        "Factoring inicial inconsistente",
        "Verificar que recibido inicial + retenido + comision inicial sea igual al monto con factoring.",
    )
    _append_issue(
        ing_f,
        "Ingresos",
        factoring_bad_final,
        "Liquidacion final con factoring inconsistente",
        "Verificar que valor final recibido + comision final no exceda el retenido inicial.",
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


def _safe_schedule_view(raw_value) -> list[dict]:
    try:
        data = json.loads(str(raw_value or "[]"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _safe_factoring_detail(raw_value) -> dict[str, object]:
    try:
        data = json.loads(str(raw_value or "{}"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _factoring_retenido_pendiente(raw_value) -> float:
    detail = raw_value if isinstance(raw_value, dict) else _safe_factoring_detail(raw_value)
    if not detail:
        return 0.0
    return max(
        0.0,
        float(detail.get("initial_retained", 0.0) or 0.0)
        - float(detail.get("final_cash_received", 0.0) or 0.0)
        - float(detail.get("final_fee", 0.0) or 0.0),
    )


def _schedule_position(row: pd.Series, as_of: pd.Timestamp) -> dict[str, object]:
    principal = float(pd.to_numeric(pd.Series([row.get(COL_FINANCIAMIENTO_MONTO, row.get(COL_MONTO, 0.0))]), errors="coerce").fillna(0.0).iloc[0])
    saldo = principal
    next_date = pd.NaT
    next_capital = 0.0
    next_interes = 0.0
    next_cuota = 0.0
    for item in _safe_schedule_view(row.get(COL_FINANCIAMIENTO_CRONOGRAMA, "[]")):
        due = pd.to_datetime(item.get("fecha"), errors="coerce")
        if pd.isna(due):
            continue
        if due <= as_of:
            saldo = float(item.get("saldo_pendiente", saldo) or saldo)
            continue
        next_date = due
        next_capital = float(item.get("capital", 0.0) or 0.0)
        next_interes = float(item.get("interes", 0.0) or 0.0)
        next_cuota = float(item.get("cuota_total", 0.0) or 0.0)
        break
    return {
        "saldo_estimado": max(0.0, float(saldo)),
        "proxima_fecha": next_date,
        "proximo_capital": float(next_capital),
        "proximo_interes": float(next_interes),
        "proxima_cuota": float(next_cuota),
    }


def _build_deuda_inversion_views(ing_scope: pd.DataFrame, gas_scope: pd.DataFrame) -> dict[str, pd.DataFrame]:
    def _num(df: pd.DataFrame, col: str, fallback: str | None = None) -> pd.Series:
        if df is None or df.empty:
            return pd.Series(dtype="float64")
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        if fallback and fallback in df.columns:
            return pd.to_numeric(df[fallback], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    def _date(df: pd.DataFrame, col: str, fallback: str | None = None) -> pd.Series:
        if df is None or df.empty:
            return pd.Series(dtype="datetime64[ns]")
        if col in df.columns:
            return pd.to_datetime(df[col], errors="coerce")
        if fallback and fallback in df.columns:
            return pd.to_datetime(df[fallback], errors="coerce")
        return pd.Series(pd.NaT, index=df.index)

    def _make_view(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        schedule_df = pd.DataFrame([_schedule_position(row, pd.Timestamp(date.today())) for _, row in df.iterrows()])
        out = pd.DataFrame(
            {
                "tipo_registro": label,
                "empresa": df.get(COL_EMPRESA, pd.Series("", index=df.index)).astype(str),
                "tipo_contraparte": df.get(COL_TIPO_CONTRAPARTE, pd.Series("", index=df.index)).astype(str),
                "contraparte": df.get(COL_CONTRAPARTE, pd.Series("", index=df.index)).astype(str),
                "fecha_movimiento": _date(df, COL_FECHA),
                "fecha_inicio": _date(df, COL_FINANCIAMIENTO_FECHA_INICIO, COL_FECHA),
                "tipo_financiamiento": df.get(COL_FINANCIAMIENTO_TIPO, pd.Series("", index=df.index)).astype(str),
                "monto_principal_registrado": _num(df, COL_FINANCIAMIENTO_MONTO, COL_MONTO),
                "monto_operativo_registrado": _num(df, COL_MONTO),
            }
        )
        if not schedule_df.empty:
            out = pd.concat([out.reset_index(drop=True), schedule_df.reset_index(drop=True)], axis=1)
        return out.reset_index(drop=True)

    ing_balance = ing_scope.get(COL_TRATAMIENTO_BALANCE_ING, pd.Series("", index=ing_scope.index)).astype(str).str.strip()
    gas_balance = gas_scope.get(COL_TRATAMIENTO_BALANCE_GAS, pd.Series("", index=gas_scope.index)).astype(str).str.strip()

    aportes = _make_view(ing_scope[ing_balance.eq("Patrimonio")].copy(), "Aporte de socio / capital")
    deuda = _make_view(ing_scope[ing_balance.eq("Pasivo financiero")].copy(), "Financiamiento recibido")
    prestamos_otorgados = _make_view(
        gas_scope[gas_balance.eq("Cuenta por cobrar / prestamo otorgado")].copy(),
        "Prestamo otorgado",
    )
    inversiones = _make_view(
        gas_scope[gas_balance.eq("Inversion / participacion en otra empresa")].copy(),
        "Inversion / participacion",
    )

    disb_mask = ing_scope.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=ing_scope.index)).astype(str).str.strip().eq("Desembolso")
    capital_mask = gas_scope.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().eq("Pago capital")
    interes_mask = gas_scope.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().eq("Pago interes")
    cargo_mask = gas_scope.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().eq("Cargo")
    card_consumo_mask = gas_scope.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().eq("Consumo tarjeta")
    card_cargo_mask = gas_scope.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().eq("Cargo tarjeta")

    line_disb = ing_scope[
        disb_mask
        & ing_scope.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=ing_scope.index)).astype(str).str.strip().ne("")
    ].copy()
    line_pay = gas_scope[
        (capital_mask | interes_mask | cargo_mask)
        & gas_scope.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().ne("")
    ].copy()
    if line_disb.empty and line_pay.empty:
        lineas_credito = pd.DataFrame()
    else:
        rows = []
        instrumentos = sorted({
            *line_disb.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=line_disb.index)).astype(str).str.strip().tolist(),
            *line_pay.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=line_pay.index)).astype(str).str.strip().tolist(),
        })
        instrumentos = [x for x in instrumentos if x]
        for instrument in instrumentos:
            ing_i = line_disb[line_disb.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=line_disb.index)).astype(str).str.strip() == instrument].copy()
            gas_i = line_pay[line_pay.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=line_pay.index)).astype(str).str.strip() == instrument].copy()
            total_desembolsado = float(pd.to_numeric(ing_i.get(COL_MONTO_REAL_COBRADO, ing_i.get(COL_MONTO, 0.0)), errors="coerce").fillna(0.0).sum()) if not ing_i.empty else 0.0
            capital_pagado = float(pd.to_numeric(gas_i.loc[gas_i.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_i.index)).astype(str).str.strip().eq("Pago capital"), COL_MONTO_REAL_PAGADO], errors="coerce").fillna(0.0).sum()) if not gas_i.empty else 0.0
            interes_pagado = float(pd.to_numeric(gas_i.loc[gas_i.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_i.index)).astype(str).str.strip().eq("Pago interes"), COL_MONTO_REAL_PAGADO], errors="coerce").fillna(0.0).sum()) if not gas_i.empty else 0.0
            cargos_pagados = float(pd.to_numeric(gas_i.loc[gas_i.get(COL_FINANCIAMIENTO_REG_TIPO, pd.Series("", index=gas_i.index)).astype(str).str.strip().eq("Cargo"), COL_MONTO_REAL_PAGADO], errors="coerce").fillna(0.0).sum()) if not gas_i.empty else 0.0
            empresa = str(ing_i.get(COL_EMPRESA, pd.Series("", index=ing_i.index)).astype(str).iloc[0] if not ing_i.empty else gas_i.get(COL_EMPRESA, pd.Series("", index=gas_i.index)).astype(str).iloc[0]).strip()
            banco = str(ing_i.get(COL_CONTRAPARTE, pd.Series("", index=ing_i.index)).astype(str).iloc[0] if not ing_i.empty else gas_i.get(COL_CONTRAPARTE, pd.Series("", index=gas_i.index)).astype(str).iloc[0]).strip()
            fecha_primera = pd.to_datetime(ing_i.get(COL_FECHA_REAL_COBRO, ing_i.get(COL_FECHA)), errors="coerce").min() if not ing_i.empty else pd.NaT
            fecha_ultimo_pago = pd.to_datetime(gas_i.get(COL_FECHA_REAL_PAGO, gas_i.get(COL_FECHA)), errors="coerce").max() if not gas_i.empty else pd.NaT
            rows.append(
                {
                    "empresa": empresa,
                    "linea_credito": instrument,
                    "banco": banco,
                    "fecha_primer_desembolso": fecha_primera,
                    "fecha_ultimo_pago": fecha_ultimo_pago,
                    "total_desembolsado": total_desembolsado,
                    "capital_pagado": capital_pagado,
                    "interes_pagado": interes_pagado,
                    "cargos_pagados": cargos_pagados,
                    "saldo_estimado": max(0.0, total_desembolsado - capital_pagado),
                }
            )
        lineas_credito = pd.DataFrame(rows).sort_values(["empresa", "linea_credito"], na_position="last").reset_index(drop=True)

    card_cons = gas_scope[
        card_consumo_mask
        & gas_scope.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().ne("")
    ].copy()
    card_charges = gas_scope[
        card_cargo_mask
        & gas_scope.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=gas_scope.index)).astype(str).str.strip().ne("")
    ].copy()
    if card_cons.empty and card_charges.empty:
        tarjetas_credito = pd.DataFrame()
    else:
        rows = []
        instrumentos = sorted({
            *card_cons.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=card_cons.index)).astype(str).str.strip().tolist(),
            *card_charges.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=card_charges.index)).astype(str).str.strip().tolist(),
        })
        instrumentos = [x for x in instrumentos if x]
        for instrument in instrumentos:
            gas_i = card_cons[card_cons.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=card_cons.index)).astype(str).str.strip() == instrument].copy()
            cargos_i = card_charges[card_charges.get(COL_FINANCIAMIENTO_INSTRUMENTO, pd.Series("", index=card_charges.index)).astype(str).str.strip() == instrument].copy()
            total_consumos = float(pd.to_numeric(gas_i.get(COL_MONTO), errors="coerce").fillna(0.0).sum()) if not gas_i.empty else 0.0
            pagado_consumos = float(pd.to_numeric(gas_i.get(COL_MONTO_REAL_PAGADO), errors="coerce").fillna(0.0).sum()) if not gas_i.empty else 0.0
            saldo_pendiente = max(0.0, total_consumos - pagado_consumos)
            intereses_cargos = float(pd.to_numeric(cargos_i.get(COL_MONTO_REAL_PAGADO, cargos_i.get(COL_MONTO, 0.0)), errors="coerce").fillna(0.0).sum()) if not cargos_i.empty else 0.0
            empresa = str(gas_i.get(COL_EMPRESA, pd.Series("", index=gas_i.index)).astype(str).iloc[0] if not gas_i.empty else cargos_i.get(COL_EMPRESA, pd.Series("", index=cargos_i.index)).astype(str).iloc[0]).strip()
            banco = str(gas_i.get(COL_CONTRAPARTE, pd.Series("", index=gas_i.index)).astype(str).iloc[0] if not gas_i.empty else cargos_i.get(COL_CONTRAPARTE, pd.Series("", index=cargos_i.index)).astype(str).iloc[0]).strip()
            if not banco:
                banco = str(gas_i.get(COL_PROVEEDOR, pd.Series("", index=gas_i.index)).astype(str).iloc[0] if not gas_i.empty else cargos_i.get(COL_PROVEEDOR, pd.Series("", index=cargos_i.index)).astype(str).iloc[0]).strip()
            pendiente_mask = (
                pd.to_numeric(gas_i.get(COL_MONTO), errors="coerce").fillna(0.0)
                - pd.to_numeric(gas_i.get(COL_MONTO_REAL_PAGADO), errors="coerce").fillna(0.0)
            ) > 0.01 if not gas_i.empty else pd.Series(dtype=bool)
            proxima_fecha = pd.to_datetime(gas_i.loc[pendiente_mask, COL_FECHA_PAGO], errors="coerce").min() if not gas_i.empty and pendiente_mask.any() else pd.NaT
            rows.append(
                {
                    "empresa": empresa,
                    "tarjeta_credito": instrument,
                    "banco": banco,
                    "proxima_fecha": proxima_fecha,
                    "total_consumos": total_consumos,
                    "pagado_a_consumos": pagado_consumos,
                    "saldo_pendiente": saldo_pendiente,
                    "intereses_y_cargos": intereses_cargos,
                }
            )
        tarjetas_credito = pd.DataFrame(rows).sort_values(["empresa", "tarjeta_credito"], na_position="last").reset_index(drop=True)

    return {
        "aportes": aportes,
        "deuda": deuda,
        "prestamos_otorgados": prestamos_otorgados,
        "inversiones": inversiones,
        "lineas_credito": lineas_credito,
        "tarjetas_credito": tarjetas_credito,
    }


def _build_inventory_operativo_view(gas_scope: pd.DataFrame, cutoff_date: date | pd.Timestamp) -> pd.DataFrame:
    if gas_scope is None or gas_scope.empty:
        return pd.DataFrame()
    trat = gas_scope.get(COL_TRATAMIENTO_BALANCE_GAS, pd.Series("", index=gas_scope.index)).astype(str).str.strip()
    inv = gas_scope[trat.eq("Inventario")].copy()
    if inv.empty:
        return pd.DataFrame()
    cutoff_ts = pd.Timestamp(cutoff_date)
    inv["fecha"] = pd.to_datetime(inv.get(COL_FECHA), errors="coerce")
    inv["fecha_llegada_inventario"] = pd.to_datetime(inv.get(COL_INVENTARIO_FECHA_LLEGADA), errors="coerce")
    inv["monto_registrado"] = pd.to_numeric(inv.get(COL_MONTO), errors="coerce").fillna(0.0).abs()
    inv["movimiento_inventario"] = inv.get(COL_INVENTARIO_MOVIMIENTO, pd.Series("", index=inv.index)).astype(str).str.strip()
    inv["item_inventario"] = inv.get(COL_INVENTARIO_ITEM, pd.Series("", index=inv.index)).astype(str).str.strip()
    arrival = inv["fecha_llegada_inventario"].where(inv["fecha_llegada_inventario"].notna(), inv["fecha"])
    inv["estado_inventario"] = "Disponible"
    inv.loc[
        inv["movimiento_inventario"].isin(["Entrada", "Ajuste positivo"]) & (arrival > cutoff_ts),
        "estado_inventario",
    ] = "En transito"
    inv.loc[
        inv["movimiento_inventario"].isin(["Salida / consumo", "Ajuste negativo"]),
        "estado_inventario",
    ] = inv.loc[
        inv["movimiento_inventario"].isin(["Salida / consumo", "Ajuste negativo"]),
        "movimiento_inventario",
    ]
    inv["impacto_inventario"] = inv["monto_registrado"]
    inv.loc[inv["movimiento_inventario"].isin(["Salida / consumo", "Ajuste negativo"]), "impacto_inventario"] *= -1.0
    view = pd.DataFrame(
        {
            "fecha": inv["fecha"],
            "fecha_llegada_inventario": inv["fecha_llegada_inventario"],
            "empresa": inv.get(COL_EMPRESA, pd.Series("", index=inv.index)).astype(str),
            "item_inventario": inv["item_inventario"],
            "movimiento_inventario": inv["movimiento_inventario"],
            "estado_inventario": inv["estado_inventario"],
            "categoria": inv.get(COL_CATEGORIA, pd.Series("", index=inv.index)).astype(str),
            "monto_registrado": inv["monto_registrado"],
            "impacto_inventario": inv["impacto_inventario"],
        }
    )
    return view.sort_values(["fecha", "empresa"], na_position="last").reset_index(drop=True)


def _build_prepagos_view(gas_scope: pd.DataFrame, cutoff_date: date) -> pd.DataFrame:
    if gas_scope is None or gas_scope.empty:
        return pd.DataFrame()
    trat = gas_scope.get(COL_TRATAMIENTO_BALANCE_GAS, pd.Series("", index=gas_scope.index)).astype(str).str.strip()
    prep = gas_scope[trat.eq("Anticipo / prepago")].copy()
    if prep.empty:
        return pd.DataFrame()

    cutoff_ts = pd.Timestamp(cutoff_date)
    rows = []
    for _, row in prep.iterrows():
        start = pd.to_datetime(row.get(COL_PREPAGO_FECHA_INICIO), errors="coerce")
        if pd.isna(start):
            start = pd.to_datetime(row.get(COL_FECHA), errors="coerce")
        meses = int(pd.to_numeric(pd.Series([row.get(COL_PREPAGO_MESES, 0)]), errors="coerce").fillna(0).iloc[0])
        monto = float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        devengado = 0.0
        saldo = monto
        if pd.notna(start) and meses > 0 and monto > 0:
            start_period = start.to_period("M")
            cutoff_period = cutoff_ts.to_period("M")
            if cutoff_period >= start_period:
                months_elapsed = len(pd.period_range(start=start_period, end=cutoff_period, freq="M"))
                devengado = min(monto, (monto / float(meses)) * max(0, months_elapsed))
                saldo = max(0.0, monto - devengado)
        rows.append(
            {
                "empresa": str(row.get(COL_EMPRESA, "") or ""),
                "contraparte": str(row.get(COL_CONTRAPARTE, "") or ""),
                "fecha_hecho": pd.to_datetime(row.get(COL_FECHA), errors="coerce"),
                "fecha_inicio_devengo": start,
                "meses_prepago": meses,
                "monto_total": monto,
                "devengado_estimado": float(devengado),
                "saldo_prepago": float(saldo),
                "categoria": str(row.get(COL_CATEGORIA, "") or ""),
            }
        )
    return pd.DataFrame(rows).sort_values(["fecha_inicio_devengo", "empresa"], na_position="last").reset_index(drop=True)


def _build_factoring_view(ing_scope: pd.DataFrame) -> pd.DataFrame:
    if ing_scope is None or ing_scope.empty:
        return pd.DataFrame()
    work = ing_scope.copy()
    if "__factoring_detalle" in work.columns:
        detail_series = work["__factoring_detalle"]
    else:
        detail_series = work.get(COL_FACTORING_DET, pd.Series("", index=work.index)).map(_safe_factoring_detail)
    work["__factoring_detalle_view"] = detail_series
    if "__factoring_retenido_pendiente" in work.columns:
        work["__factoring_retenido_pendiente_view"] = pd.to_numeric(work["__factoring_retenido_pendiente"], errors="coerce").fillna(0.0)
    else:
        work["__factoring_retenido_pendiente_view"] = work["__factoring_detalle_view"].map(_factoring_retenido_pendiente)
    work = work[work["__factoring_detalle_view"].map(bool)].copy()
    if work.empty:
        return pd.DataFrame()

    rows = []
    for _, row in work.iterrows():
        detail = row.get("__factoring_detalle_view", {}) or {}
        rows.append(
            {
                "empresa": str(row.get(COL_EMPRESA, "") or ""),
                "cliente": str(row.get(COL_CLIENTE_NOMBRE, "") or ""),
                "proyecto": str(row.get(COL_PROYECTO, "") or ""),
                "contraparte": str(detail.get("contraparte", row.get(COL_CONTRAPARTE, "")) or ""),
                "fecha_inicio_factoring": pd.to_datetime(detail.get("fecha_inicio"), errors="coerce"),
                "monto_con_factoring": float(detail.get("factored_amount", 0.0) or 0.0),
                "valor_recibido_inicial": float(detail.get("initial_cash_received", 0.0) or 0.0),
                "comision_inicial": float(detail.get("initial_fee", 0.0) or 0.0),
                "retenido_inicial": float(detail.get("initial_retained", 0.0) or 0.0),
                "fecha_liquidacion_final": pd.to_datetime(detail.get("fecha_liquidacion_final"), errors="coerce"),
                "valor_recibido_final": float(detail.get("final_cash_received", 0.0) or 0.0),
                "comision_final": float(detail.get("final_fee", 0.0) or 0.0),
                "retenido_pendiente": float(row.get("__factoring_retenido_pendiente_view", 0.0) or 0.0),
                "estado_factoring": "Retenido pendiente" if float(row.get("__factoring_retenido_pendiente_view", 0.0) or 0.0) > 0.01 else "Liquidado",
            }
        )
    return pd.DataFrame(rows).sort_values(["fecha_inicio_factoring", "empresa"], na_position="last").reset_index(drop=True)


st.title("Panel Financiero Gerencial")
st.caption(
    "Vista gerencial y analitica construida sobre los mismos datos de Finanzas 1. "
    "No reemplaza ni modifica el flujo operativo actual."
)

with st.expander("Informacion de interes", expanded=False):
    st.markdown("#### Como se estructura cada reporte")
    st.markdown(
        "- `Flujo de caja actual`: usa fecha real de cobro y fecha real de pago.\n"
        "- `Flujo de caja proyectado`: usa fechas esperadas, recurrencias y cronogramas futuros.\n"
        "- `Estado de resultados`: usa fecha del hecho economico; capital no va al resultado, solo intereses si entran al resultado.\n"
        "- `Balance general`: integra caja, cuentas abiertas, prestamos, inventario disponible, inventario en transito, prepagos, retenidos con factoring, saldo de tarjeta dentro de cuentas por pagar y activos fijos netos cuando existen."
    )
    st.markdown("#### Pendiente para robustecer")
    st.markdown(
        "- Control operativo de inventario por cantidades y costo unitario.\n"
        "- Cierre mensual persistente.\n"
        "- Conciliacion bancaria.\n"
        "- Historial de cambios de tasa diaria dentro del mismo periodo de uso de la linea.\n"
        "- Tarjetas con multiples ciclos/cortes historicos y conciliacion contra estado de cuenta.\n"
        "- Factoring con recurso y proyeccion estimada del retenido cuando aun no existe liquidacion final.\n"
        "- Ajustes avanzados de valuacion para inversiones / participaciones."
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
opening_cfg = get_finance_opening_config()

with st.sidebar:
    st.markdown("### Filtros globales")
    today = date.today()
    mes_inicio = date(today.year, today.month, 1)
    default_desde = max(min_date, opening_cfg.effective_date)
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
            "Activa los periodos recomendados por defecto: Flujo actual desde apertura, "
            "Estado de resultados desde apertura y Balance desde apertura hasta hoy."
        ),
        key="f2_use_recommended_periods",
    )
    custom_periods = not bool(use_recommended_periods)

    if use_recommended_periods:
        st.session_state["f2_period_cash_actual"] = "Mensual"
        st.session_state["f2_period_cash_proj"] = "Mensual"
        st.session_state["f2_period_results"] = "Semestral"
        st.session_state["f2_period_balance"] = "Mensual"
        st.session_state["f2_window_cash_actual"] = "Desde apertura"
        st.session_state["f2_window_results"] = "Desde apertura"
        st.session_state["f2_window_balance"] = "Desde apertura hasta hoy"

    if modo_tiempo != "Rango personalizado":
        st.markdown("#### Ventanas de analisis")
        cash_window_label = st.selectbox(
            "Flujo de caja actual",
            options=["Desde apertura", "Mes corriente", "Ultimos 30 dias", "Mes anterior", "Trimestre corriente", "Año corriente"],
            index=0,
            key="f2_window_cash_actual",
        )
        results_window_label = st.selectbox(
            "Estado de resultados",
            options=["Desde apertura", "Ultimo semestre cerrado", "Ultimo cuatrimestre cerrado", "Ultimo año cerrado"],
            index=0,
            key="f2_window_results",
        )
        balance_window_label = st.selectbox(
            "Balance general",
            options=["Desde apertura hasta hoy", "Ultimo cierre mensual", "Ultimo cierre cuatrimestral", "Ultimo cierre anual"],
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
gas_scope_payables = (
    _filter_df_window(gas_scope, opening_cfg.effective_date, max_date)
    if (not opening_cfg.preserve_existing_cxp and pd.Timestamp(max_date) >= pd.Timestamp(opening_cfg.effective_date))
    else gas_scope.copy()
)

if modo_tiempo == "Rango personalizado":
    cash_desde, cash_hasta = fecha_desde, fecha_hasta
    resultados_desde, resultados_hasta = fecha_desde, fecha_hasta
    balance_desde, balance_hasta = fecha_desde, fecha_hasta
    results_period_default = st.session_state.get("f2_period_results", "Semestral")
    balance_period_default = st.session_state.get("f2_period_balance", "Mensual")
else:
    today_ref = date.today()
    cash_desde, cash_hasta = _resolve_cash_window(cash_window_label, today_ref, min_date, max_date, opening_cfg.effective_date)
    resultados_desde, resultados_hasta = _resolve_results_window(results_window_label, today_ref, min_date, max_date, opening_cfg.effective_date)
    balance_desde, balance_hasta = _resolve_balance_window(balance_window_label, today_ref, min_date, max_date, opening_cfg.effective_date)
    results_period_default = {
        "Desde apertura": "Mensual",
        "Ultimo semestre cerrado": "Semestral",
        "Ultimo cuatrimestre cerrado": "Cuatrimestral",
        "Ultimo año cerrado": "Anual",
    }.get(results_window_label, "Mensual")
    balance_period_default = {
        "Desde apertura hasta hoy": "Mensual",
        "Ultimo cierre mensual": "Mensual",
        "Ultimo cierre cuatrimestral": "Cuatrimestral",
        "Ultimo cierre anual": "Anual",
    }.get(balance_window_label, "Mensual")

# Flujos/KPIs del periodo operativo visible bajo la apertura financiera vigente.
opening_active_cash = pd.Timestamp(cash_hasta) >= pd.Timestamp(opening_cfg.effective_date)
opening_active_results = pd.Timestamp(resultados_hasta) >= pd.Timestamp(opening_cfg.effective_date)
opening_active_balance = pd.Timestamp(balance_hasta) >= pd.Timestamp(opening_cfg.effective_date)
opening_cash = opening_amount_for_filter(opening_cfg.cash_by_company, empresa)
opening_cash_balance = opening_cash if opening_active_balance else 0.0
opening_cash_period = opening_cash if opening_active_cash else 0.0
opening_cxc = opening_amount_for_filter(opening_cfg.cxc_by_company, empresa)
opening_balance_extras = _opening_balance_components_for_filter(opening_cfg, empresa) if opening_active_balance else {}

cash_scope_desde = max(cash_desde, opening_cfg.effective_date) if opening_active_cash else cash_desde
ing_f = _filter_df_window(ing_scope, cash_scope_desde, cash_hasta)
gas_f = _filter_df_window(gas_scope, cash_scope_desde, cash_hasta)
split = split_real_vs_pending(ing_f, gas_f)

# Saldos actuales: usa apertura + movimientos reales posteriores a la fecha de arranque.
if opening_active_cash and cash_scope_desde > opening_cfg.effective_date:
    cash_prev_end = cash_scope_desde - timedelta(days=1)
    ing_cash_prev = _filter_df_window(ing_scope, opening_cfg.effective_date, cash_prev_end)
    gas_cash_prev = _filter_df_window(gas_scope, opening_cfg.effective_date, cash_prev_end)
    split_cash_prev = split_real_vs_pending(ing_cash_prev, gas_cash_prev)
    cash_prev = _build_cashflow_actual_with_opening(
        split_cash_prev["ing_real"],
        split_cash_prev["gas_real"],
        opening_cash_period,
    )
    saldo_inicial_periodo = float(cash_prev["metricas"]["efectivo_actual"])
else:
    saldo_inicial_periodo = float(opening_cash_period)

# Proyeccion y CxC: parte desde apertura para no arrastrar cartera historica anterior.
ing_scope_proj = (
    _filter_df_window(ing_scope, opening_cfg.effective_date, max_date)
    if opening_active_cash
    else ing_scope.copy()
)
gas_scope_proj = (
    _filter_df_window(gas_scope_payables, opening_cfg.effective_date, max_date)
    if opening_active_cash
    else gas_scope_payables.copy()
)
split_proj = split_real_vs_pending(ing_scope_proj, gas_scope_proj)

# Estado de resultados: usar periodo posterior a la apertura para que sea util en la transicion.
result_scope_desde = max(resultados_desde, opening_cfg.effective_date) if opening_active_results else resultados_desde
ing_res = _filter_df_window(ing_scope, result_scope_desde, resultados_hasta)
gas_res = _filter_df_window(gas_scope, result_scope_desde, resultados_hasta)

# Balance: caja y componentes de balance desde la apertura para evitar arrastrar historia previa.
balance_scope_desde = max(balance_desde, opening_cfg.effective_date) if opening_active_balance else balance_desde
ing_balance = (
    _filter_df_window(ing_scope, balance_scope_desde, balance_hasta)
    if opening_active_balance
    else ing_scope.copy()
)
gas_balance_cash = (
    _filter_df_window(gas_scope, balance_scope_desde, balance_hasta)
    if opening_active_balance
    else gas_scope.copy()
)
gas_balance_payables = (
    _filter_df_window(gas_scope, balance_scope_desde, balance_hasta)
    if (not opening_cfg.preserve_existing_cxp and opening_active_balance)
    else gas_scope.copy()
)
split_balance = split_real_vs_pending(ing_balance, gas_balance_payables)
split_balance_cash = split_real_vs_pending(ing_balance, gas_balance_cash)

cash_actual = _build_cashflow_actual_with_opening(split["ing_real"], split["gas_real"], saldo_inicial_periodo)
cash_balance = _build_cashflow_actual_with_opening(
    split_balance_cash["ing_real"],
    split_balance_cash["gas_real"],
    opening_cash_balance,
)
cxc_df = build_cuentas_por_cobrar(split_proj["ing_pend"])
cxp_df, cxp_quality = build_cuentas_por_pagar(split_proj["gas_pend"])
cxc_total = (float(cxc_df["monto"].sum()) if not cxc_df.empty else 0.0) + float(opening_cxc if opening_active_cash else 0.0)
cxp_total = float(cxp_df["monto"].sum()) if not cxp_df.empty else 0.0

proyectado = build_cashflow_proyectado(
    ing_scope_proj,
    gas_scope_proj,
    saldo_inicial=cash_actual["metricas"]["efectivo_actual"],
    granularidad="D",
    horizon_months=int(horizonte_proy_meses),
)

estado = build_estado_resultados(
    ing_res,
    gas_res,
    include_miscelaneos=include_misc,
    fecha_desde=result_scope_desde,
    fecha_hasta=resultados_hasta,
)
balance_snapshots_summary = _build_balance_snapshots(
    cash_movimientos=cash_balance["movimientos"],
    split=split_balance,
    df_ing_scope=ing_balance,
    df_gas_scope=gas_balance_cash,
    period_label=balance_period_default,
    fecha_desde=balance_scope_desde,
    fecha_hasta=balance_hasta,
    efectivo_inicial=opening_cash_balance,
    opening_balance_extras=opening_balance_extras,
)
latest_balance_summary = (
    balance_snapshots_summary.sort_values("corte").iloc[-1].to_dict()
    if not balance_snapshots_summary.empty
    else {}
)
balance_components = compute_balance_components(ing_balance, gas_balance_cash, cutoff_date=balance_hasta)
for _key, _amount in opening_balance_extras.items():
    balance_components[_key] = float(balance_components.get(_key, 0.0)) + float(_amount or 0.0)
balance_kwargs = {
    "efectivo_actual": float(latest_balance_summary.get("efectivo", 0.0)),
    "cuentas_por_cobrar": float(latest_balance_summary.get("cuentas_por_cobrar", 0.0)),
    "cuentas_por_pagar": float(latest_balance_summary.get("cuentas_por_pagar", 0.0)),
    "prestamos_otorgados": float(balance_components.get("prestamos_otorgados", 0.0)),
    "inventario": float(balance_components.get("inventario", 0.0)),
    "inventario_en_transito": float(balance_components.get("inventario_en_transito", 0.0)),
    "anticipos_prepagos": float(balance_components.get("anticipos_prepagos", 0.0)),
    "inversiones_participaciones": float(balance_components.get("inversiones_participaciones", 0.0)),
    "factoring_retenido": float(balance_components.get("factoring_retenido", 0.0)),
    "activos_fijos_netos": float(balance_components.get("activos_fijos_netos", 0.0)),
    "prestamos_recibidos": float(balance_components.get("prestamos_recibidos", 0.0)),
    "aportes_capital": float(balance_components.get("aportes_capital", 0.0)),
    "otras_deudas": float(balance_components.get("otras_deudas", 0.0)),
}
supported_balance_params = set(inspect.signature(build_balance_general_simplificado).parameters)
balance = build_balance_general_simplificado(
    **{key: value for key, value in balance_kwargs.items() if key in supported_balance_params}
)
debt_views = _build_deuda_inversion_views(ing_scope, gas_scope)
inventory_view = _build_inventory_operativo_view(gas_scope, balance_hasta)
prepagos_view = _build_prepagos_view(gas_scope, balance_hasta)
factoring_view = _build_factoring_view(ing_scope)

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
st.caption(
    f"Apertura financiera vigente desde {opening_cfg.effective_date.isoformat()}. "
    "Caja parte de saldo inicial por empresa; CxC arranca en 0; CxP solo cuenta pendientes desde la apertura; "
    "resultado y balance recomendados se leen desde la apertura para evitar arrastrar historia desordenada."
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
    st.caption("`Flujo neto` = entradas menos salidas del periodo. `Saldo acumulado` = caja acumulada despues de sumar los movimientos reales del periodo en orden cronologico.")

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
    st.caption("`Flujo neto proyectado` = cobros futuros menos pagos futuros. `Saldo final proyectado` = saldo inicial mas ese flujo neto acumulado en el horizonte.")

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
    st.caption(f"Periodo analizado: {result_scope_desde.isoformat()} -> {resultados_hasta.isoformat()}")
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
    st.caption(f"Periodo analizado: {balance_scope_desde.isoformat()} -> {balance_hasta.isoformat()}")
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
        df_ing_scope=ing_balance,
        df_gas_scope=gas_balance_cash,
        period_label=period_balance,
        fecha_desde=balance_scope_desde,
        fecha_hasta=balance_hasta,
        efectivo_inicial=opening_cash_balance,
        opening_balance_extras=opening_balance_extras,
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
                {"Cuenta": "Prestamos otorgados", "Monto": float(latest.get("prestamos_otorgados", 0.0))},
                {"Cuenta": "Inventario", "Monto": float(latest.get("inventario", 0.0))},
                {"Cuenta": "Inventario en transito", "Monto": float(latest.get("inventario_en_transito", 0.0))},
                {"Cuenta": "Anticipos / prepagos", "Monto": float(latest.get("anticipos_prepagos", 0.0))},
                {"Cuenta": "Inversiones / participaciones", "Monto": float(latest.get("inversiones_participaciones", 0.0))},
                {"Cuenta": "Retenido con factoring", "Monto": float(latest.get("factoring_retenido", 0.0))},
                {"Cuenta": "Activos fijos netos", "Monto": float(latest.get("activos_fijos_netos", 0.0))},
            ]
        )
        activos_df = activos_df[activos_df["Monto"] != 0].reset_index(drop=True)
        pasivos_df = pd.DataFrame(
            [
                {"Cuenta": "Cuentas por pagar", "Monto": float(latest["cuentas_por_pagar"])},
                {"Cuenta": "Prestamos recibidos", "Monto": float(latest.get("prestamos_recibidos", 0.0))},
                {"Cuenta": "Otras deudas", "Monto": float(latest.get("otras_deudas", 0.0))},
            ]
        )
        pasivos_df = pasivos_df[pasivos_df["Monto"] != 0].reset_index(drop=True)
        patrimonio_df = pd.DataFrame(
            [
                {"Cuenta": "Aportes de socios / capital", "Monto": float(latest.get("aportes_capital", 0.0))},
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

with st.expander("Inventario operativo", expanded=False):
    st.caption(
        "Vista administrativa de apoyo para entradas, salidas, ajustes e inventario en transito. "
        "El saldo del inventario en balance se separa entre disponible y en transito segun la fecha de llegada / disponibilidad."
    )
    inv_entradas = float(inventory_view.loc[inventory_view["impacto_inventario"] > 0, "impacto_inventario"].sum()) if not inventory_view.empty else 0.0
    inv_salidas = float(abs(inventory_view.loc[inventory_view["impacto_inventario"] < 0, "impacto_inventario"].sum())) if not inventory_view.empty else 0.0
    i1, i2, i3, i4 = st.columns(4)
    i1.metric("Entradas inventario", format_money_es(inv_entradas))
    i2.metric("Salidas / consumos", format_money_es(inv_salidas))
    i3.metric("Inventario disponible", format_money_es(float(balance_components.get("inventario", 0.0))))
    i4.metric("Inventario en transito", format_money_es(float(balance_components.get("inventario_en_transito", 0.0))))
    if inventory_view.empty:
        st.info("Sin movimientos de inventario en el alcance filtrado.")
    else:
        st.dataframe(
            inventory_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "fecha": st.column_config.DateColumn("Fecha"),
                "fecha_llegada_inventario": st.column_config.DateColumn("Fecha llegada"),
                "monto_registrado": st.column_config.NumberColumn("Monto registrado", format="$%0.2f"),
                "impacto_inventario": st.column_config.NumberColumn("Impacto inventario", format="$%0.2f"),
            },
        )

with st.expander("Prepagos activos", expanded=False):
    st.caption(
        "Vista administrativa de apoyo para anticipos / prepagos. "
        "Muestra el monto total registrado, lo ya devengado estimado y el saldo que sigue activo en balance."
    )
    p1, p2, p3 = st.columns(3)
    p1.metric("Prepagos activos", str(len(prepagos_view)))
    p2.metric("Devengado estimado", format_money_es(float(prepagos_view.get("devengado_estimado", pd.Series(dtype=float)).sum()) if not prepagos_view.empty else 0.0))
    p3.metric("Saldo prepago", format_money_es(float(balance_components.get("anticipos_prepagos", 0.0))))
    if prepagos_view.empty:
        st.info("Sin prepagos activos en el alcance filtrado.")
    else:
        st.dataframe(
            prepagos_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "fecha_hecho": st.column_config.DateColumn("Fecha hecho"),
                "fecha_inicio_devengo": st.column_config.DateColumn("Inicio devengo"),
                "monto_total": st.column_config.NumberColumn("Monto total", format="$%0.2f"),
                "devengado_estimado": st.column_config.NumberColumn("Devengado estimado", format="$%0.2f"),
                "saldo_prepago": st.column_config.NumberColumn("Saldo prepago", format="$%0.2f"),
            },
        )

with st.expander("Deudas e inversiones", expanded=False):
    st.caption(
        "Vista administrativa de apoyo para financiamientos, lineas de credito, tarjetas de credito, prestamos otorgados, inversiones, aportes de capital y operaciones con factoring. "
        "Aqui se muestra principal registrado, saldo estimado, saldo pendiente, retenidos pendientes y la proxima cuota cuando existe cronograma."
    )
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Prestamos recibidos", format_money_es(float(balance_components.get("prestamos_recibidos", 0.0))))
    d2.metric("Prestamos otorgados", format_money_es(float(balance_components.get("prestamos_otorgados", 0.0))))
    d3.metric("Inversiones / participaciones", format_money_es(float(balance_components.get("inversiones_participaciones", 0.0))))
    d4.metric("Aportes de capital", format_money_es(float(balance_components.get("aportes_capital", 0.0))))
    d5, d6, d7 = st.columns(3)
    d5.metric("Retenido con factoring", format_money_es(float(balance_components.get("factoring_retenido", 0.0))))
    d6.metric("Lineas de credito", format_money_es(float(debt_views.get("lineas_credito", pd.DataFrame()).get("saldo_estimado", pd.Series(dtype=float)).sum()) if not debt_views.get("lineas_credito", pd.DataFrame()).empty else 0.0))
    d7.metric("Tarjetas de credito", format_money_es(float(debt_views.get("tarjetas_credito", pd.DataFrame()).get("saldo_pendiente", pd.Series(dtype=float)).sum()) if not debt_views.get("tarjetas_credito", pd.DataFrame()).empty else 0.0))

    for title, key in [
        ("Lineas de credito", "lineas_credito"),
        ("Tarjetas de credito", "tarjetas_credito"),
        ("Financiamientos recibidos", "deuda"),
        ("Prestamos otorgados", "prestamos_otorgados"),
        ("Inversiones / participaciones", "inversiones"),
        ("Aportes de socio / capital", "aportes"),
    ]:
        st.markdown(f"#### {title}")
        df_view = debt_views.get(key, pd.DataFrame())
        if df_view.empty:
            st.info("Sin registros en el alcance filtrado.")
            continue
        st.dataframe(
            df_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "fecha_primer_desembolso": st.column_config.DateColumn("Primer desembolso"),
                "fecha_ultimo_pago": st.column_config.DateColumn("Ultimo pago"),
                "fecha_movimiento": st.column_config.DateColumn("Fecha movimiento"),
                "fecha_inicio": st.column_config.DateColumn("Fecha inicio"),
                "proxima_fecha": st.column_config.DateColumn("Proxima fecha"),
                "monto_principal_registrado": st.column_config.NumberColumn("Principal registrado", format="$%0.2f"),
                "monto_operativo_registrado": st.column_config.NumberColumn("Monto registrado", format="$%0.2f"),
                "total_desembolsado": st.column_config.NumberColumn("Total desembolsado", format="$%0.2f"),
                "capital_pagado": st.column_config.NumberColumn("Capital pagado", format="$%0.2f"),
                "interes_pagado": st.column_config.NumberColumn("Interes pagado", format="$%0.2f"),
                "cargos_pagados": st.column_config.NumberColumn("Cargos pagados", format="$%0.2f"),
                "total_consumos": st.column_config.NumberColumn("Total consumos", format="$%0.2f"),
                "pagado_a_consumos": st.column_config.NumberColumn("Pagado a consumos", format="$%0.2f"),
                "saldo_pendiente": st.column_config.NumberColumn("Saldo pendiente", format="$%0.2f"),
                "intereses_y_cargos": st.column_config.NumberColumn("Intereses y cargos", format="$%0.2f"),
                "saldo_estimado": st.column_config.NumberColumn("Saldo estimado", format="$%0.2f"),
                "proximo_capital": st.column_config.NumberColumn("Proximo capital", format="$%0.2f"),
                "proximo_interes": st.column_config.NumberColumn("Proximo interes", format="$%0.2f"),
                "proxima_cuota": st.column_config.NumberColumn("Proxima cuota", format="$%0.2f"),
            },
        )

    st.markdown("#### Operaciones con factoring")
    if factoring_view.empty:
        st.info("Sin operaciones con factoring en el alcance filtrado.")
    else:
        st.dataframe(
            factoring_view,
            use_container_width=True,
            hide_index=True,
            column_config={
                "fecha_inicio_factoring": st.column_config.DateColumn("Fecha factoring"),
                "fecha_liquidacion_final": st.column_config.DateColumn("Fecha liquidacion final"),
                "monto_con_factoring": st.column_config.NumberColumn("Monto con factoring", format="$%0.2f"),
                "valor_recibido_inicial": st.column_config.NumberColumn("Valor recibido inicial", format="$%0.2f"),
                "comision_inicial": st.column_config.NumberColumn("Comision inicial", format="$%0.2f"),
                "retenido_inicial": st.column_config.NumberColumn("Retenido inicial", format="$%0.2f"),
                "valor_recibido_final": st.column_config.NumberColumn("Valor recibido final", format="$%0.2f"),
                "comision_final": st.column_config.NumberColumn("Comision final", format="$%0.2f"),
                "retenido_pendiente": st.column_config.NumberColumn("Retenido pendiente", format="$%0.2f"),
            },
        )

st.markdown("---")
st.caption(
    "Panel Financiero Gerencial prioriza lectura gerencial. Finanzas 1 se mantiene intacta para captura/operacion. "
    "Pendientes mayores: cierre mensual persistente, conciliacion bancaria, inventario con cantidades/costo unitario, historial de cambios de tasa diaria para lineas revolventes y valuacion avanzada de inversiones / participaciones."
)

