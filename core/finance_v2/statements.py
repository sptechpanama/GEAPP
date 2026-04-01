from __future__ import annotations

import pandas as pd
from pandas.api.types import is_period_dtype

from .constants import COL_CATEGORIA, COL_EMPRESA, COL_FECHA, COL_MONTO
from .helpers import include_by_category, safe_div


def _is_direct_cost(category: str) -> bool:
    key = str(category or "").strip().lower()
    return "proyecto" in key


def _safe_series(df: pd.DataFrame, col: str, default_value: object = "") -> pd.Series:
    """
    Devuelve siempre una Series alineada al indice de `df` para evitar
    errores cuando la columna falta, viene duplicada o llega con tipo inesperado.
    """
    if not isinstance(df, pd.DataFrame):
        return pd.Series(dtype="object")

    if col in df.columns:
        raw = df[col]
        if isinstance(raw, pd.DataFrame):
            # Si hay columnas duplicadas, toma la primera de forma determinista.
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


def _ensure_estado_resultados_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Blindaje defensivo: evita KeyError por columnas faltantes.
    """
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if isinstance(out, pd.DataFrame) and out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated(keep="first")].copy()
    if COL_CATEGORIA not in out.columns:
        out[COL_CATEGORIA] = ""
    if COL_MONTO not in out.columns:
        out[COL_MONTO] = 0.0
    if COL_FECHA not in out.columns:
        out[COL_FECHA] = pd.NaT
    if COL_EMPRESA not in out.columns:
        out[COL_EMPRESA] = ""

    out[COL_CATEGORIA] = _safe_series(out, COL_CATEGORIA, "").astype(str).fillna("")
    out[COL_MONTO] = pd.to_numeric(_safe_series(out, COL_MONTO, 0.0), errors="coerce").fillna(0.0)
    out[COL_FECHA] = _safe_datetime_series(out, COL_FECHA)
    out[COL_EMPRESA] = _safe_series(out, COL_EMPRESA, "").astype(str).fillna("")
    return out


def build_estado_resultados(
    df_ing: pd.DataFrame,
    df_gas: pd.DataFrame,
    *,
    include_miscelaneos: bool,
) -> dict:
    ing = _ensure_estado_resultados_schema(df_ing)
    gas = _ensure_estado_resultados_schema(df_gas)

    cat_ing = _safe_series(ing, COL_CATEGORIA, "").astype(str)
    cat_gas = _safe_series(gas, COL_CATEGORIA, "").astype(str)

    ing = ing[cat_ing.map(lambda x: include_by_category(x, include_miscelaneos))].copy()
    gas = gas[cat_gas.map(lambda x: include_by_category(x, include_miscelaneos))].copy()
    cat_gas = _safe_series(gas, COL_CATEGORIA, "").astype(str)

    ingresos = float(pd.to_numeric(_safe_series(ing, COL_MONTO, 0.0), errors="coerce").fillna(0.0).sum())
    direct_mask = cat_gas.map(_is_direct_cost)
    gas_amounts = pd.to_numeric(_safe_series(gas, COL_MONTO, 0.0), errors="coerce").fillna(0.0)
    costos_directos = float(gas_amounts[direct_mask].sum()) if not gas_amounts.empty else 0.0
    gastos_operativos = float(gas_amounts[~direct_mask].sum()) if not gas_amounts.empty else 0.0

    utilidad_bruta = ingresos - costos_directos
    utilidad_operativa = utilidad_bruta - gastos_operativos
    margen_operativo = safe_div(utilidad_operativa, ingresos) * 100.0

    estado = pd.DataFrame(
        [
            {"Rubro": "Ingresos", "Monto": ingresos},
            {"Rubro": "Costos directos", "Monto": -costos_directos},
            {"Rubro": "Utilidad bruta", "Monto": utilidad_bruta},
            {"Rubro": "Gastos operativos", "Monto": -gastos_operativos},
            {"Rubro": "Utilidad operativa", "Monto": utilidad_operativa},
            {"Rubro": "Margen operativo (%)", "Monto": margen_operativo},
        ]
    )

    ing_month = ing.copy()
    gas_month = gas.copy()
    ing_month["Mes"] = _safe_datetime_series(ing_month, COL_FECHA).dt.to_period("M")
    gas_month["Mes"] = _safe_datetime_series(gas_month, COL_FECHA).dt.to_period("M")

    if COL_MONTO not in ing_month.columns:
        ing_month[COL_MONTO] = 0.0
    if COL_MONTO not in gas_month.columns:
        gas_month[COL_MONTO] = 0.0

    ing_m = (
        ing_month.groupby("Mes", as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_MONTO: "Ingresos"})
    )
    gas_m = (
        gas_month.groupby("Mes", as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_MONTO: "Gastos"})
    )
    mensual = ing_m.merge(gas_m, on="Mes", how="outer")
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

    if COL_EMPRESA not in ing.columns:
        ing[COL_EMPRESA] = ""
    if COL_EMPRESA not in gas.columns:
        gas[COL_EMPRESA] = ""
    if COL_MONTO not in ing.columns:
        ing[COL_MONTO] = 0.0
    if COL_MONTO not in gas.columns:
        gas[COL_MONTO] = 0.0

    empresa_ing = ing.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Ingresos"})
    empresa_gas = gas.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Gastos"})
    por_empresa = empresa_ing.merge(empresa_gas, on=COL_EMPRESA, how="outer").fillna(0.0)
    por_empresa["Utilidad"] = por_empresa["Ingresos"] - por_empresa["Gastos"]
    por_empresa = por_empresa.sort_values("Utilidad", ascending=False)

    if COL_CATEGORIA not in gas.columns:
        gas[COL_CATEGORIA] = ""
    if COL_MONTO not in gas.columns:
        gas[COL_MONTO] = 0.0
    gasto_categoria = (
        gas.groupby(COL_CATEGORIA, as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_CATEGORIA: "Categoria", COL_MONTO: "Gasto"})
        .sort_values("Gasto", ascending=False)
    )

    notes = [
        "Estado de resultados gerencial: aproximacion operativa con base en ingresos y gastos registrados.",
        "No representa devengo contable completo; depende de la calidad de categorias actuales.",
        "Clasificacion usada: 'Proyectos' como costo directo; resto como gasto operativo.",
    ]
    if not include_miscelaneos:
        notes.append("Politica aplicada: categoria Miscelaneos excluida de rentabilidad y estado de resultados.")

    return {
        "estado": estado,
        "mensual": mensual,
        "por_empresa": por_empresa,
        "gasto_categoria": gasto_categoria,
        "metricas": {
            "ingresos": ingresos,
            "costos_directos": costos_directos,
            "gastos_operativos": gastos_operativos,
            "utilidad_operativa": utilidad_operativa,
            "margen_operativo": margen_operativo,
        },
        "notas": notes,
    }


def build_balance_general_simplificado(
    *,
    efectivo_actual: float,
    cuentas_por_cobrar: float,
    cuentas_por_pagar: float,
) -> dict:
    activos_df = pd.DataFrame(
        [
            {"Cuenta": "Efectivo y equivalentes", "Monto": float(efectivo_actual)},
            {"Cuenta": "Cuentas por cobrar", "Monto": float(cuentas_por_cobrar)},
        ]
    )
    total_activos = float(activos_df["Monto"].sum())

    pasivos_df = pd.DataFrame(
        [
            {"Cuenta": "Cuentas por pagar", "Monto": float(cuentas_por_pagar)},
        ]
    )
    total_pasivos = float(pasivos_df["Monto"].sum())

    patrimonio_neto = total_activos - total_pasivos
    capital_trabajo = (efectivo_actual + cuentas_por_cobrar) - cuentas_por_pagar

    patrimonio_df = pd.DataFrame(
        [{"Cuenta": "Patrimonio neto estimado", "Monto": float(patrimonio_neto)}]
    )

    notes = [
        "Balance general gerencial y simplificado (no incluye inventarios, deuda bancaria u otros rubros no registrados).",
        "Los importes se derivan unicamente de caja actual, cuentas por cobrar y cuentas por pagar disponibles en el sistema.",
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
