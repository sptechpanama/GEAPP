from __future__ import annotations

import json
from datetime import date

import pandas as pd
from pandas.api.types import is_period_dtype

from .constants import (
    COL_ACTIVO_FIJO_DEP_MENSUAL,
    COL_ACTIVO_FIJO_DEP_TOGGLE,
    COL_ACTIVO_FIJO_FECHA_INICIO,
    COL_ACTIVO_FIJO_VALOR_RESIDUAL,
    COL_CATEGORIA,
    COL_EMPRESA,
    COL_FECHA,
    COL_FECHA_REAL_COBRO,
    COL_FECHA_REAL_PAGO,
    COL_FINANCIAMIENTO_CRONOGRAMA,
    COL_FINANCIAMIENTO_INSTRUMENTO,
    COL_FINANCIAMIENTO_MONTO,
    COL_FINANCIAMIENTO_REG_TIPO,
    COL_FINANCIAMIENTO_TIPO,
    COL_FACTORING_DETALLE,
    COL_GASTO_DETALLE,
    COL_INVENTARIO_FECHA_LLEGADA,
    COL_MONTO,
    COL_MONTO_REAL_COBRADO,
    COL_MONTO_REAL_PAGADO,
    COL_NATURALEZA_INGRESO,
    COL_INVENTARIO_ITEM,
    COL_INVENTARIO_MOVIMIENTO,
    COL_POR_COBRAR,
    COL_POR_PAGAR,
    COL_PREPAGO_FECHA_INICIO,
    COL_PREPAGO_MESES,
    COL_SUBCLASIFICACION_GERENCIAL,
    COL_TRATAMIENTO_BALANCE_ING,
    COL_TRATAMIENTO_BALANCE_GAS,
)
from .helpers import include_by_category, safe_div


def _safe_series(df: pd.DataFrame, col: str, default_value: object = "") -> pd.Series:
    if not isinstance(df, pd.DataFrame):
        return pd.Series(dtype="object")
    if col in df.columns:
        raw = df[col]
        if isinstance(raw, pd.DataFrame):
            series = raw.iloc[:, 0]
        elif isinstance(raw, pd.Series):
            series = raw
        else:
            series = pd.Series([raw] * len(df), index=df.index)
    else:
        series = pd.Series([default_value] * len(df), index=df.index)
    return series.reindex(df.index)


def _safe_datetime_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(_safe_series(df, col, pd.NaT), errors="coerce")


def _ensure_estado_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if isinstance(out, pd.DataFrame) and out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated(keep="first")].copy()
    for col, default in [
        (COL_CATEGORIA, ""),
        (COL_MONTO, 0.0),
        (COL_FECHA, pd.NaT),
        (COL_EMPRESA, ""),
        (COL_NATURALEZA_INGRESO, ""),
        (COL_SUBCLASIFICACION_GERENCIAL, ""),
        (COL_TRATAMIENTO_BALANCE_GAS, ""),
        (COL_FINANCIAMIENTO_TIPO, ""),
        (COL_FINANCIAMIENTO_CRONOGRAMA, ""),
        (COL_FINANCIAMIENTO_INSTRUMENTO, ""),
        (COL_FINANCIAMIENTO_REG_TIPO, ""),
        (COL_GASTO_DETALLE, ""),
        (COL_MONTO_REAL_COBRADO, 0.0),
        (COL_MONTO_REAL_PAGADO, 0.0),
        (COL_INVENTARIO_MOVIMIENTO, ""),
        (COL_INVENTARIO_ITEM, ""),
        (COL_INVENTARIO_FECHA_LLEGADA, pd.NaT),
        (COL_PREPAGO_MESES, 0),
        (COL_PREPAGO_FECHA_INICIO, pd.NaT),
        (COL_ACTIVO_FIJO_DEP_TOGGLE, "No"),
        (COL_ACTIVO_FIJO_DEP_MENSUAL, 0.0),
        (COL_ACTIVO_FIJO_FECHA_INICIO, pd.NaT),
        (COL_ACTIVO_FIJO_VALOR_RESIDUAL, 0.0),
        (COL_FINANCIAMIENTO_MONTO, 0.0),
    ]:
        if col not in out.columns:
            out[col] = default
    out[COL_CATEGORIA] = _safe_series(out, COL_CATEGORIA, "").astype(str).fillna("")
    out[COL_MONTO] = pd.to_numeric(_safe_series(out, COL_MONTO, 0.0), errors="coerce").fillna(0.0)
    out[COL_FECHA] = _safe_datetime_series(out, COL_FECHA)
    out[COL_EMPRESA] = _safe_series(out, COL_EMPRESA, "").astype(str).fillna("")
    out[COL_NATURALEZA_INGRESO] = _safe_series(out, COL_NATURALEZA_INGRESO, "").astype(str).fillna("")
    out[COL_SUBCLASIFICACION_GERENCIAL] = _safe_series(out, COL_SUBCLASIFICACION_GERENCIAL, "").astype(str).fillna("")
    out[COL_TRATAMIENTO_BALANCE_GAS] = _safe_series(out, COL_TRATAMIENTO_BALANCE_GAS, "").astype(str).fillna("")
    out[COL_FINANCIAMIENTO_TIPO] = _safe_series(out, COL_FINANCIAMIENTO_TIPO, "").astype(str).fillna("")
    out[COL_FINANCIAMIENTO_CRONOGRAMA] = _safe_series(out, COL_FINANCIAMIENTO_CRONOGRAMA, "").astype(str).fillna("")
    out[COL_FINANCIAMIENTO_INSTRUMENTO] = _safe_series(out, COL_FINANCIAMIENTO_INSTRUMENTO, "").astype(str).fillna("")
    out[COL_FINANCIAMIENTO_REG_TIPO] = _safe_series(out, COL_FINANCIAMIENTO_REG_TIPO, "").astype(str).fillna("")
    out[COL_ACTIVO_FIJO_DEP_TOGGLE] = _safe_series(out, COL_ACTIVO_FIJO_DEP_TOGGLE, "No").astype(str).fillna("No")
    out[COL_INVENTARIO_MOVIMIENTO] = _safe_series(out, COL_INVENTARIO_MOVIMIENTO, "").astype(str).fillna("")
    out[COL_INVENTARIO_ITEM] = _safe_series(out, COL_INVENTARIO_ITEM, "").astype(str).fillna("")
    out[COL_INVENTARIO_FECHA_LLEGADA] = _safe_datetime_series(out, COL_INVENTARIO_FECHA_LLEGADA)
    out[COL_MONTO_REAL_COBRADO] = pd.to_numeric(_safe_series(out, COL_MONTO_REAL_COBRADO, 0.0), errors="coerce").fillna(0.0)
    out[COL_MONTO_REAL_PAGADO] = pd.to_numeric(_safe_series(out, COL_MONTO_REAL_PAGADO, 0.0), errors="coerce").fillna(0.0)
    out[COL_PREPAGO_MESES] = pd.to_numeric(_safe_series(out, COL_PREPAGO_MESES, 0), errors="coerce").fillna(0).astype(int)
    out[COL_PREPAGO_FECHA_INICIO] = _safe_datetime_series(out, COL_PREPAGO_FECHA_INICIO)
    out[COL_ACTIVO_FIJO_DEP_MENSUAL] = pd.to_numeric(_safe_series(out, COL_ACTIVO_FIJO_DEP_MENSUAL, 0.0), errors="coerce").fillna(0.0)
    out[COL_ACTIVO_FIJO_FECHA_INICIO] = _safe_datetime_series(out, COL_ACTIVO_FIJO_FECHA_INICIO)
    out[COL_ACTIVO_FIJO_VALOR_RESIDUAL] = pd.to_numeric(_safe_series(out, COL_ACTIVO_FIJO_VALOR_RESIDUAL, 0.0), errors="coerce").fillna(0.0)
    out[COL_FINANCIAMIENTO_MONTO] = pd.to_numeric(_safe_series(out, COL_FINANCIAMIENTO_MONTO, 0.0), errors="coerce").fillna(0.0)
    return out


def _parse_schedule_df(df: pd.DataFrame) -> pd.DataFrame:
    empty_columns = [
        "fecha",
        "interes",
        "capital",
        "cuota_total",
        "saldo_pendiente",
        "fin_type",
        "empresa",
    ]
    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        raw = str(row.get(COL_FINANCIAMIENTO_CRONOGRAMA, "") or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for item in data:
            due_date = pd.to_datetime(item.get("fecha"), errors="coerce")
            if pd.isna(due_date):
                continue
            rows.append(
                {
                    "fecha": due_date,
                    "interes": float(item.get("interes", 0.0) or 0.0),
                    "capital": float(item.get("capital", 0.0) or 0.0),
                    "cuota_total": float(item.get("cuota_total", 0.0) or 0.0),
                    "saldo_pendiente": float(item.get("saldo_pendiente", 0.0) or 0.0),
                    "fin_type": str(row.get(COL_FINANCIAMIENTO_TIPO, "") or ""),
                    "empresa": str(row.get(COL_EMPRESA, "") or ""),
                }
            )
    if not rows:
        return pd.DataFrame(columns=empty_columns)
    return pd.DataFrame(rows, columns=empty_columns)


def _calc_depreciacion_periodo(gas: pd.DataFrame, fecha_desde: pd.Timestamp, fecha_hasta: pd.Timestamp) -> float:
    total = 0.0
    for _, row in gas.iterrows():
        if str(row.get(COL_ACTIVO_FIJO_DEP_TOGGLE, "No")).strip().lower() not in {"si", "s?"}:
            continue
        dep_mensual = float(row.get(COL_ACTIVO_FIJO_DEP_MENSUAL, 0.0) or 0.0)
        start_date = pd.to_datetime(row.get(COL_ACTIVO_FIJO_FECHA_INICIO), errors="coerce")
        if dep_mensual <= 0 or pd.isna(start_date):
            continue
        start_period = max(start_date.to_period("M"), fecha_desde.to_period("M"))
        end_period = fecha_hasta.to_period("M")
        if end_period < start_period:
            continue
        months = len(pd.period_range(start=start_period, end=end_period, freq="M"))
        total += dep_mensual * months
    return float(total)


def _prepago_schedule(row: pd.Series) -> tuple[pd.Timestamp | pd.NaT, int, float]:
    start_date = pd.to_datetime(row.get(COL_PREPAGO_FECHA_INICIO), errors="coerce")
    if pd.isna(start_date):
        start_date = pd.to_datetime(row.get(COL_FECHA), errors="coerce")
    meses = int(pd.to_numeric(pd.Series([row.get(COL_PREPAGO_MESES, 0)]), errors="coerce").fillna(0).iloc[0])
    total = float(row.get(COL_MONTO, 0.0) or 0.0)
    return start_date, meses, total


def _expand_prepagos_devengados(gas: pd.DataFrame, fecha_desde: pd.Timestamp, fecha_hasta: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    prepagos = gas[_safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").eq("Anticipo / prepago")].copy()
    if prepagos.empty:
        return pd.DataFrame(columns=gas.columns)

    start_period = fecha_desde.to_period("M")
    end_period = fecha_hasta.to_period("M")
    for _, row in prepagos.iterrows():
        start_date, meses, total = _prepago_schedule(row)
        if pd.isna(start_date) or meses <= 0 or total <= 0:
            continue
        monthly = total / float(meses)
        period_start = start_date.to_period("M")
        for idx in range(meses):
            period = period_start + idx
            if period < start_period or period > end_period:
                continue
            row_copy = row.to_dict()
            row_copy[COL_FECHA] = period.to_timestamp()
            row_copy[COL_MONTO] = monthly
            row_copy[COL_TRATAMIENTO_BALANCE_GAS] = "Gasto del periodo"
            rows.append(row_copy)
    return pd.DataFrame(rows, columns=gas.columns) if rows else pd.DataFrame(columns=gas.columns)


def _remaining_prepago_at_cutoff(gas: pd.DataFrame, cutoff: pd.Timestamp) -> float:
    total_restante = 0.0
    prepagos = gas[_safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").eq("Anticipo / prepago")].copy()
    for _, row in prepagos.iterrows():
        start_date, meses, total = _prepago_schedule(row)
        if pd.isna(start_date) or meses <= 0 or total <= 0:
            total_restante += total
            continue
        start_period = start_date.to_period("M")
        cutoff_period = cutoff.to_period("M")
        if cutoff_period < start_period:
            total_restante += total
            continue
        months_elapsed = len(pd.period_range(start=start_period, end=cutoff_period, freq="M"))
        consumed = min(total, (total / float(meses)) * max(0, months_elapsed))
        total_restante += max(0.0, total - consumed)
    return float(total_restante)


def _parse_factoring_detail(raw_value) -> dict[str, object]:
    try:
        data = json.loads(str(raw_value or "{}"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}

    def _num(key: str) -> float:
        return float(pd.to_numeric(pd.Series([data.get(key, 0.0)]), errors="coerce").fillna(0.0).iloc[0])

    detail = {
        "modo": str(data.get("modo", "") or "").strip(),
        "fecha_inicio": pd.to_datetime(data.get("fecha_inicio"), errors="coerce"),
        "fecha_liquidacion_final": pd.to_datetime(data.get("fecha_liquidacion_final"), errors="coerce"),
        "initial_retained": _num("initial_retained"),
        "final_cash_received": _num("final_cash_received"),
        "final_fee": _num("final_fee"),
    }
    return detail if detail["modo"] else {}


def _factoring_retained_at_cutoff(df_ing: pd.DataFrame, cutoff: pd.Timestamp) -> float:
    if df_ing is None or df_ing.empty or COL_FACTORING_DETALLE not in df_ing.columns:
        return 0.0
    total = 0.0
    for _, row in df_ing.iterrows():
        detail = _parse_factoring_detail(row.get(COL_FACTORING_DETALLE, ""))
        if not detail:
            continue
        start = pd.to_datetime(detail.get("fecha_inicio"), errors="coerce")
        if pd.isna(start) or start > cutoff:
            continue
        pendiente = max(
            0.0,
            float(detail.get("initial_retained", 0.0) or 0.0)
            - float(detail.get("final_cash_received", 0.0) or 0.0)
            - float(detail.get("final_fee", 0.0) or 0.0),
        )
        if pendiente <= 0:
            continue
        total += pendiente
    return float(total)


def _inventory_signed_amounts(gas: pd.DataFrame) -> pd.Series:
    if gas is None or gas.empty:
        return pd.Series(dtype="float64")
    movimiento = _safe_series(gas, COL_INVENTARIO_MOVIMIENTO, "").astype(str).str.strip()
    monto = pd.to_numeric(_safe_series(gas, COL_MONTO, 0.0), errors="coerce").fillna(0.0).abs()
    signed = monto.copy()
    signed.loc[movimiento.isin(["Salida / consumo", "Ajuste negativo"])] *= -1.0
    return signed


def _inventory_split_at_cutoff(gas: pd.DataFrame, cutoff: pd.Timestamp) -> tuple[float, float]:
    if gas is None or gas.empty:
        return 0.0, 0.0
    work = gas.copy()
    work[COL_FECHA] = pd.to_datetime(_safe_series(work, COL_FECHA, pd.NaT), errors="coerce")
    work[COL_INVENTARIO_FECHA_LLEGADA] = pd.to_datetime(
        _safe_series(work, COL_INVENTARIO_FECHA_LLEGADA, pd.NaT),
        errors="coerce",
    )
    work[COL_INVENTARIO_MOVIMIENTO] = _safe_series(work, COL_INVENTARIO_MOVIMIENTO, "").astype(str).str.strip()
    work[COL_MONTO] = pd.to_numeric(_safe_series(work, COL_MONTO, 0.0), errors="coerce").fillna(0.0).abs()
    work = work[work[COL_FECHA].notna() & (work[COL_FECHA] <= cutoff)].copy()
    if work.empty:
        return 0.0, 0.0

    positive_mask = work[COL_INVENTARIO_MOVIMIENTO].isin(["Entrada", "Ajuste positivo"])
    negative_mask = work[COL_INVENTARIO_MOVIMIENTO].isin(["Salida / consumo", "Ajuste negativo"])
    arrival_dates = work[COL_INVENTARIO_FECHA_LLEGADA].where(work[COL_INVENTARIO_FECHA_LLEGADA].notna(), work[COL_FECHA])

    inventario_disponible = float(
        work.loc[positive_mask & (arrival_dates <= cutoff), COL_MONTO].sum()
        - work.loc[negative_mask, COL_MONTO].sum()
    )
    inventario_transito = float(work.loc[positive_mask & (arrival_dates > cutoff), COL_MONTO].sum())
    return max(0.0, inventario_disponible), max(0.0, inventario_transito)


def _expand_inventory_consumption(gas: pd.DataFrame) -> pd.DataFrame:
    if gas is None or gas.empty:
        return pd.DataFrame(columns=(gas.columns if isinstance(gas, pd.DataFrame) else []))
    trat = _safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").astype(str)
    mov = _safe_series(gas, COL_INVENTARIO_MOVIMIENTO, "").astype(str)
    consumo = gas[trat.eq("Inventario") & mov.isin(["Salida / consumo", "Ajuste negativo"])].copy()
    if consumo.empty:
        return pd.DataFrame(columns=gas.columns)
    consumo[COL_MONTO] = pd.to_numeric(consumo.get(COL_MONTO), errors="coerce").fillna(0.0).abs()
    consumo[COL_TRATAMIENTO_BALANCE_GAS] = "Gasto del periodo"
    consumo.loc[_safe_series(consumo, COL_SUBCLASIFICACION_GERENCIAL, "").eq(""), COL_SUBCLASIFICACION_GERENCIAL] = "Costo directo"
    return consumo


def build_estado_resultados(
    df_ing: pd.DataFrame,
    df_gas: pd.DataFrame,
    *,
    include_miscelaneos: bool,
    fecha_desde: date | pd.Timestamp | None = None,
    fecha_hasta: date | pd.Timestamp | None = None,
) -> dict:
    ing_all = _ensure_estado_schema(df_ing)
    gas_all = _ensure_estado_schema(df_gas)

    if fecha_desde is None:
        fecha_desde_ts = pd.to_datetime(_safe_series(ing_all, COL_FECHA, pd.NaT), errors="coerce").min()
    else:
        fecha_desde_ts = pd.Timestamp(fecha_desde)
    if fecha_hasta is None:
        fecha_hasta_ts = pd.to_datetime(_safe_series(gas_all, COL_FECHA, pd.NaT), errors="coerce").max()
    else:
        fecha_hasta_ts = pd.Timestamp(fecha_hasta)
    if pd.isna(fecha_desde_ts):
        fecha_desde_ts = pd.Timestamp(date.today().replace(day=1))
    if pd.isna(fecha_hasta_ts):
        fecha_hasta_ts = pd.Timestamp(date.today())

    ing = ing_all[(ing_all[COL_FECHA] >= fecha_desde_ts) & (ing_all[COL_FECHA] <= fecha_hasta_ts)].copy()
    gas = gas_all[(gas_all[COL_FECHA] >= fecha_desde_ts) & (gas_all[COL_FECHA] <= fecha_hasta_ts)].copy()

    ing = ing[ing[COL_CATEGORIA].map(lambda x: include_by_category(x, include_miscelaneos))].copy()
    gas = gas[gas[COL_CATEGORIA].map(lambda x: include_by_category(x, include_miscelaneos))].copy()

    nature = _safe_series(ing, COL_NATURALEZA_INGRESO, "").astype(str)
    ingresos_resultado = ing[nature.isin(["Operativo", "Financiero", "No operativo"])].copy()
    nature_res = _safe_series(ingresos_resultado, COL_NATURALEZA_INGRESO, "").astype(str)
    ingresos_operativos = float(pd.to_numeric(ingresos_resultado.loc[nature_res.eq("Operativo"), COL_MONTO], errors="coerce").fillna(0.0).sum())
    ingresos_financieros_base = float(pd.to_numeric(ingresos_resultado.loc[nature_res.eq("Financiero"), COL_MONTO], errors="coerce").fillna(0.0).sum())
    ingresos_no_operativos = float(pd.to_numeric(ingresos_resultado.loc[nature_res.eq("No operativo"), COL_MONTO], errors="coerce").fillna(0.0).sum())

    subclas = _safe_series(gas, COL_SUBCLASIFICACION_GERENCIAL, "").astype(str)
    trat = _safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").astype(str)
    period_mask = trat.eq("Gasto del periodo")
    prepago_devengado = _expand_prepagos_devengados(gas_all, fecha_desde_ts, fecha_hasta_ts)
    inventario_consumido = _expand_inventory_consumption(gas)
    gas_resultado = pd.concat([gas.loc[period_mask].copy(), prepago_devengado, inventario_consumido], ignore_index=True, sort=False)
    subclas_result = _safe_series(gas_resultado, COL_SUBCLASIFICACION_GERENCIAL, "").astype(str)
    costos_directos = float(pd.to_numeric(gas_resultado.loc[subclas_result.eq("Costo directo"), COL_MONTO], errors="coerce").fillna(0.0).sum())
    gastos_operativos = float(pd.to_numeric(gas_resultado.loc[subclas_result.isin(["Administrativo fijo", "Operativo variable", "Comercial / ventas", "No operativo"]), COL_MONTO], errors="coerce").fillna(0.0).sum())
    gastos_financieros_base = float(pd.to_numeric(gas_resultado.loc[subclas_result.eq("Financiero"), COL_MONTO], errors="coerce").fillna(0.0).sum())
    impuestos_mask = subclas_result.eq("Impuestos") | _safe_series(gas_resultado, COL_CATEGORIA, "").eq("Impuestos")
    impuestos = float(
        pd.to_numeric(gas_resultado.loc[impuestos_mask, COL_MONTO], errors="coerce")
        .fillna(0.0)
        .sum()
    )

    sched_ing = _parse_schedule_df(ing_all)
    sched_gas = _parse_schedule_df(gas_all)
    if not sched_ing.empty:
        sched_ing = sched_ing[(sched_ing["fecha"] >= fecha_desde_ts) & (sched_ing["fecha"] <= fecha_hasta_ts)]
    if not sched_gas.empty:
        sched_gas = sched_gas[(sched_gas["fecha"] >= fecha_desde_ts) & (sched_gas["fecha"] <= fecha_hasta_ts)]

    ingresos_financieros = ingresos_financieros_base + float(sched_gas.loc[sched_gas["fin_type"].eq("Financiamiento otorgado"), "interes"].sum())
    gastos_financieros = gastos_financieros_base + float(sched_ing.loc[sched_ing["fin_type"].eq("Financiamiento recibido"), "interes"].sum()) + float(sched_gas.loc[sched_gas["fin_type"].eq("Activo fijo financiado"), "interes"].sum())
    depreciacion = _calc_depreciacion_periodo(gas_all, fecha_desde_ts, fecha_hasta_ts)

    ingresos_totales = ingresos_operativos + ingresos_financieros + ingresos_no_operativos
    utilidad_bruta = ingresos_totales - costos_directos
    utilidad_operativa = utilidad_bruta - gastos_operativos - gastos_financieros - impuestos - depreciacion
    margen_operativo = safe_div(utilidad_operativa, ingresos_totales) * 100.0

    estado = pd.DataFrame(
        [
            {"Rubro": "Ingresos operativos", "Monto": ingresos_operativos},
            {"Rubro": "Ingresos financieros", "Monto": ingresos_financieros},
            {"Rubro": "Ingresos no operativos", "Monto": ingresos_no_operativos},
            {"Rubro": "Costos directos", "Monto": -costos_directos},
            {"Rubro": "Gastos operativos", "Monto": -gastos_operativos},
            {"Rubro": "Gastos financieros", "Monto": -gastos_financieros},
            {"Rubro": "Impuestos", "Monto": -impuestos},
            {"Rubro": "Depreciacion / amortizacion", "Monto": -depreciacion},
            {"Rubro": "Utilidad operativa", "Monto": utilidad_operativa},
            {"Rubro": "Margen operativo (%)", "Monto": margen_operativo},
        ]
    )

    ing_month = ingresos_resultado.copy()
    gas_month = gas_resultado.copy()
    ing_month["Mes"] = _safe_datetime_series(ing_month, COL_FECHA).dt.to_period("M")
    gas_month["Mes"] = _safe_datetime_series(gas_month, COL_FECHA).dt.to_period("M")

    m_ing = (
        ing_month.groupby("Mes", as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_MONTO: "Ingresos"})
    )
    m_gas = (
        gas_month.groupby("Mes", as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_MONTO: "Gastos"})
    )
    mensual = m_ing.merge(m_gas, on="Mes", how="outer")
    for col in ("Ingresos", "Gastos"):
        if col not in mensual.columns:
            mensual[col] = 0.0
        mensual[col] = pd.to_numeric(mensual[col], errors="coerce").fillna(0.0)
    if not mensual.empty and "Mes" in mensual.columns:
        mes_series = mensual["Mes"]
        if is_period_dtype(mes_series):
            mensual["Mes"] = mes_series.dt.to_timestamp()
        else:
            mensual["Mes"] = pd.to_datetime(mes_series, errors="coerce")
            mensual = mensual.dropna(subset=["Mes"])
    mensual = mensual.sort_values("Mes", na_position="last")
    mensual["Utilidad"] = mensual["Ingresos"] - mensual["Gastos"]

    empresa_ing = ingresos_resultado.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Ingresos"})
    empresa_gas = gas_resultado.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Gastos"})
    por_empresa = empresa_ing.merge(empresa_gas, on=COL_EMPRESA, how="outer").fillna(0.0)
    por_empresa["Utilidad"] = por_empresa["Ingresos"] - por_empresa["Gastos"]
    por_empresa = por_empresa.sort_values("Utilidad", ascending=False)

    gasto_categoria = (
        gas_resultado.groupby(COL_CATEGORIA, as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_CATEGORIA: "Categoria", COL_MONTO: "Gasto"})
        .sort_values("Gasto", ascending=False)
    )

    notes = [
        "Estado de resultados gerencial: usa fecha del hecho economico, no fecha de cobro/pago.",
        "Capital de prestamos no entra al resultado; solo intereses si entran al resultado.",
        "Activo fijo no pega completo al gasto; se reconoce via depreciacion/amortizacion cuando aplica.",
        "Aportes de socio / capital e inversiones / participaciones en otras empresas no entran al resultado; se reflejan en caja y balance.",
    ]
    if not prepago_devengado.empty:
        notes.append("Los anticipos / prepagos se devengan mensualmente segun su plazo configurado.")
    if not inventario_consumido.empty:
        notes.append("Las salidas / consumos de inventario impactan resultados como costo directo u operativo segun su clasificacion.")
    if not include_miscelaneos:
        notes.append("Politica aplicada: categoria Miscelaneos excluida de rentabilidad y estado de resultados.")

    return {
        "estado": estado,
        "mensual": mensual,
        "por_empresa": por_empresa,
        "gasto_categoria": gasto_categoria,
        "metricas": {
            "ingresos": ingresos_totales,
            "costos_directos": costos_directos,
            "gastos_operativos": gastos_operativos + gastos_financieros + impuestos + depreciacion,
            "utilidad_operativa": utilidad_operativa,
            "margen_operativo": margen_operativo,
            "ingresos_operativos": ingresos_operativos,
            "ingresos_financieros": ingresos_financieros,
            "ingresos_no_operativos": ingresos_no_operativos,
            "gastos_financieros": gastos_financieros,
            "impuestos": impuestos,
            "depreciacion": depreciacion,
        },
        "notas": notes,
    }


def compute_balance_components(df_ing: pd.DataFrame, df_gas: pd.DataFrame, *, cutoff_date: date | pd.Timestamp) -> dict:
    cutoff = pd.Timestamp(cutoff_date)
    ing = _ensure_estado_schema(df_ing)
    gas = _ensure_estado_schema(df_gas)

    prestamos_otorgados = 0.0
    prestamos_recibidos = 0.0
    activos_fijos_netos = 0.0
    inversiones_participaciones = 0.0
    factoring_retenido = 0.0
    aportes_capital = 0.0
    gas_dates = _safe_datetime_series(gas, COL_FECHA)
    vigente_mask = gas_dates.notna() & (gas_dates <= cutoff)
    inventario_mask = vigente_mask & _safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").eq("Inventario")
    inventario, inventario_en_transito = _inventory_split_at_cutoff(
        gas.loc[inventario_mask].copy(),
        cutoff,
    ) if inventario_mask.any() else (0.0, 0.0)
    anticipos = _remaining_prepago_at_cutoff(gas.loc[vigente_mask].copy(), cutoff)

    ing_trat = _safe_series(ing, COL_TRATAMIENTO_BALANCE_ING, "").astype(str)
    ing_estado = _safe_series(ing, COL_POR_COBRAR, "No").astype(str)
    ing_real_date = _safe_datetime_series(ing, COL_FECHA_REAL_COBRO)
    ing_event_date = ing_real_date.fillna(_safe_datetime_series(ing, COL_FECHA))
    aportes_mask = (
        ing_trat.eq("Patrimonio")
        & ~ing_estado.eq("Si")
        & ing_event_date.notna()
        & (ing_event_date <= cutoff)
    )
    aportes_capital = float(
        pd.to_numeric(
            _safe_series(ing.loc[aportes_mask], "__monto_realizado", _safe_series(ing.loc[aportes_mask], COL_MONTO, 0.0)),
            errors="coerce",
        ).fillna(0.0).sum()
    )

    for frame, sign in ((ing, "recibido"), (gas, "gasto")):
        for _, row in frame.iterrows():
            fin_type = str(row.get(COL_FINANCIAMIENTO_TIPO, "") or "").strip()
            raw = str(row.get(COL_FINANCIAMIENTO_CRONOGRAMA, "") or "").strip()
            principal = float(row.get(COL_FINANCIAMIENTO_MONTO, 0.0) or 0.0)
            if raw and fin_type:
                try:
                    sched = json.loads(raw)
                except Exception:
                    sched = []
                sched = sched if isinstance(sched, list) else []
                saldo = principal
                for item in sched:
                    due = pd.to_datetime(item.get("fecha"), errors="coerce")
                    if pd.isna(due):
                        continue
                    if due <= cutoff:
                        saldo = float(item.get("saldo_pendiente", saldo) or saldo)
                if fin_type == "Financiamiento recibido":
                    prestamos_recibidos += float(max(0.0, saldo))
                elif fin_type == "Financiamiento otorgado":
                    prestamos_otorgados += float(max(0.0, saldo))
                elif fin_type == "Activo fijo financiado":
                    prestamos_recibidos += float(max(0.0, saldo))

    ing_fin_type = _safe_series(ing, COL_FINANCIAMIENTO_TIPO, "").astype(str).str.strip()
    ing_reg_type = _safe_series(ing, COL_FINANCIAMIENTO_REG_TIPO, "").astype(str).str.strip()
    ing_fin_sched = _safe_series(ing, COL_FINANCIAMIENTO_CRONOGRAMA, "").astype(str).str.strip()
    ing_fin_event_date = _safe_datetime_series(ing, COL_FECHA_REAL_COBRO).fillna(_safe_datetime_series(ing, COL_FECHA))
    manual_disbursements_mask = (
        ing_trat.eq("Pasivo financiero")
        & ing_fin_type.eq("Financiamiento recibido")
        & ing_fin_sched.eq("")
        & ing_fin_event_date.notna()
        & (ing_fin_event_date <= cutoff)
        & ing_reg_type.isin(["", "Desembolso"])
    )
    manual_disbursement_amount = pd.to_numeric(
        _safe_series(ing.loc[manual_disbursements_mask], COL_MONTO_REAL_COBRADO, 0.0),
        errors="coerce",
    ).fillna(0.0)
    manual_disbursement_amount = manual_disbursement_amount.where(
        manual_disbursement_amount > 0,
        pd.to_numeric(
            _safe_series(ing.loc[manual_disbursements_mask], COL_FINANCIAMIENTO_MONTO, 0.0),
            errors="coerce",
        ).fillna(0.0),
    )
    manual_disbursement_amount = manual_disbursement_amount.where(
        manual_disbursement_amount > 0,
        pd.to_numeric(
            _safe_series(ing.loc[manual_disbursements_mask], COL_MONTO, 0.0),
            errors="coerce",
        ).fillna(0.0),
    )
    manual_disbursements = float(
        manual_disbursement_amount.sum()
    )

    gas_reg_type = _safe_series(gas, COL_FINANCIAMIENTO_REG_TIPO, "").astype(str).str.strip()
    gas_fin_instrument = _safe_series(gas, COL_FINANCIAMIENTO_INSTRUMENTO, "").astype(str).str.strip()
    gas_fin_sched = _safe_series(gas, COL_FINANCIAMIENTO_CRONOGRAMA, "").astype(str).str.strip()
    gas_fin_event_date = _safe_datetime_series(gas, COL_FECHA_REAL_PAGO).fillna(_safe_datetime_series(gas, COL_FECHA))
    manual_repayments_mask = (
        gas_trat.eq("Cancelacion de pasivo / deuda")
        & gas_fin_sched.eq("")
        & gas_fin_event_date.notna()
        & (gas_fin_event_date <= cutoff)
        & gas_reg_type.eq("Pago capital")
        & gas_fin_instrument.ne("")
    )
    manual_capital_repayments = float(
        pd.to_numeric(
            _safe_series(gas.loc[manual_repayments_mask], COL_MONTO_REAL_PAGADO, 0.0),
            errors="coerce",
        ).fillna(0.0).where(
            pd.to_numeric(
                _safe_series(gas.loc[manual_repayments_mask], COL_MONTO_REAL_PAGADO, 0.0),
                errors="coerce",
            ).fillna(0.0) > 0,
            pd.to_numeric(
                _safe_series(gas.loc[manual_repayments_mask], COL_MONTO, 0.0),
                errors="coerce",
            ).fillna(0.0),
        ).sum()
    )
    prestamos_recibidos += max(0.0, manual_disbursements - manual_capital_repayments)

    gas_trat = _safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").astype(str)
    gas_estado = _safe_series(gas, COL_POR_PAGAR, "No").astype(str)
    gas_real_date = _safe_datetime_series(gas, COL_FECHA_REAL_PAGO)
    gas_event_date = gas_real_date.fillna(_safe_datetime_series(gas, COL_FECHA))
    inversiones_mask = (
        gas_trat.eq("Inversion / participacion en otra empresa")
        & ~gas_estado.eq("Si")
        & gas_event_date.notna()
        & (gas_event_date <= cutoff)
    )
    inversiones_participaciones = float(
        pd.to_numeric(
            _safe_series(gas.loc[inversiones_mask], "__monto_realizado", _safe_series(gas.loc[inversiones_mask], COL_MONTO, 0.0)),
            errors="coerce",
        ).fillna(0.0).sum()
    )
    factoring_retenido = _factoring_retained_at_cutoff(ing, cutoff)

    activos_rows = gas[
        vigente_mask & _safe_series(gas, COL_TRATAMIENTO_BALANCE_GAS, "").eq("Activo fijo")
    ].copy()
    for _, row in activos_rows.iterrows():
        costo = float(row.get(COL_MONTO, 0.0) or 0.0)
        residual = float(row.get(COL_ACTIVO_FIJO_VALOR_RESIDUAL, 0.0) or 0.0)
        dep_mensual = float(row.get(COL_ACTIVO_FIJO_DEP_MENSUAL, 0.0) or 0.0)
        start = pd.to_datetime(row.get(COL_ACTIVO_FIJO_FECHA_INICIO), errors="coerce")
        if pd.isna(start):
            start = pd.to_datetime(row.get(COL_FECHA), errors="coerce")
        acumulada = 0.0
        if dep_mensual > 0 and pd.notna(start) and start <= cutoff:
            months = len(pd.period_range(start=start.to_period("M"), end=cutoff.to_period("M"), freq="M"))
            acumulada = min(max(0.0, costo - residual), dep_mensual * months)
        neto = max(residual, costo - acumulada)
        activos_fijos_netos += float(neto)

    return {
        "prestamos_otorgados": float(prestamos_otorgados),
        "prestamos_recibidos": float(prestamos_recibidos),
        "inventario": float(inventario),
        "inventario_en_transito": float(inventario_en_transito),
        "anticipos_prepagos": float(anticipos),
        "activos_fijos_netos": float(activos_fijos_netos),
        "inversiones_participaciones": float(inversiones_participaciones),
        "factoring_retenido": float(factoring_retenido),
        "aportes_capital": float(aportes_capital),
    }


def build_balance_general_simplificado(
    *,
    efectivo_actual: float,
    cuentas_por_cobrar: float,
    cuentas_por_pagar: float,
    prestamos_otorgados: float = 0.0,
    inventario: float = 0.0,
    inventario_en_transito: float = 0.0,
    anticipos_prepagos: float = 0.0,
    activos_fijos_netos: float = 0.0,
    inversiones_participaciones: float = 0.0,
    factoring_retenido: float = 0.0,
    prestamos_recibidos: float = 0.0,
    aportes_capital: float = 0.0,
    otras_deudas: float = 0.0,
) -> dict:
    activos_df = pd.DataFrame(
        [
            {"Cuenta": "Efectivo y equivalentes", "Monto": float(efectivo_actual)},
            {"Cuenta": "Cuentas por cobrar", "Monto": float(cuentas_por_cobrar)},
            {"Cuenta": "Prestamos otorgados", "Monto": float(prestamos_otorgados)},
            {"Cuenta": "Inventario", "Monto": float(inventario)},
            {"Cuenta": "Inventario en transito", "Monto": float(inventario_en_transito)},
            {"Cuenta": "Anticipos / prepagos", "Monto": float(anticipos_prepagos)},
            {"Cuenta": "Activos fijos netos", "Monto": float(activos_fijos_netos)},
            {"Cuenta": "Inversiones / participaciones", "Monto": float(inversiones_participaciones)},
            {"Cuenta": "Retenido con factoring", "Monto": float(factoring_retenido)},
        ]
    )
    activos_df = activos_df[activos_df["Monto"] != 0].reset_index(drop=True)
    total_activos = float(activos_df["Monto"].sum()) if not activos_df.empty else 0.0

    pasivos_df = pd.DataFrame(
        [
            {"Cuenta": "Cuentas por pagar", "Monto": float(cuentas_por_pagar)},
            {"Cuenta": "Prestamos recibidos", "Monto": float(prestamos_recibidos)},
            {"Cuenta": "Otras deudas", "Monto": float(otras_deudas)},
        ]
    )
    pasivos_df = pasivos_df[pasivos_df["Monto"] != 0].reset_index(drop=True)
    total_pasivos = float(pasivos_df["Monto"].sum()) if not pasivos_df.empty else 0.0

    patrimonio_neto = total_activos - total_pasivos
    capital_trabajo = (efectivo_actual + cuentas_por_cobrar + inventario + anticipos_prepagos + factoring_retenido) - (cuentas_por_pagar + prestamos_recibidos + otras_deudas)

    patrimonio_df = pd.DataFrame(
        [
            {"Cuenta": "Aportes de socios / capital", "Monto": float(aportes_capital)},
            {"Cuenta": "Patrimonio neto estimado", "Monto": float(patrimonio_neto)},
        ]
    )
    patrimonio_df = patrimonio_df[patrimonio_df["Monto"] != 0].reset_index(drop=True)

    notes = [
        "Balance general gerencial y simplificado: incorpora caja, cuentas abiertas, prestamos, inventario, prepagos, retenidos con factoring, inversiones y activos fijos netos cuando existen.",
        "El inventario en transito se muestra separado del inventario disponible y no se incorpora al capital de trabajo hasta su llegada / disponibilidad.",
        "Los prepagos se consumen de forma lineal segun el plazo configurado; sin cierre persistente ni conciliacion bancaria, algunos saldos siguen siendo aproximados.",
        "El inventario usa movimientos de entrada/salida por monto; aun falta una valorizacion mas fina por cantidades y costo unitario.",
    ]

    return {
        "activos": activos_df,
        "pasivos": pasivos_df,
        "patrimonio": patrimonio_df,
        "metricas": {
            "total_activos": total_activos,
            "total_pasivos": total_pasivos,
            "patrimonio_neto": patrimonio_neto,
            "capital_trabajo": capital_trabajo,
            "posicion_financiera_neta": patrimonio_neto,
        },
        "notas": notes,
    }
