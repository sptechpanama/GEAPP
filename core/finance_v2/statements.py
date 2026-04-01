from __future__ import annotations

import pandas as pd

from .constants import COL_CATEGORIA, COL_EMPRESA, COL_FECHA, COL_MONTO
from .helpers import include_by_category, safe_div


def _is_direct_cost(category: str) -> bool:
    key = str(category or "").strip().lower()
    return "proyecto" in key


def _ensure_estado_resultados_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Blindaje defensivo: evita KeyError por columnas faltantes.
    """
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if COL_CATEGORIA not in out.columns:
        out[COL_CATEGORIA] = ""
    if COL_MONTO not in out.columns:
        out[COL_MONTO] = 0.0
    if COL_FECHA not in out.columns:
        out[COL_FECHA] = pd.NaT
    if COL_EMPRESA not in out.columns:
        out[COL_EMPRESA] = ""

    out[COL_CATEGORIA] = out[COL_CATEGORIA].astype(str).fillna("")
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0)
    out[COL_FECHA] = pd.to_datetime(out[COL_FECHA], errors="coerce")
    out[COL_EMPRESA] = out[COL_EMPRESA].astype(str).fillna("")
    return out


def build_estado_resultados(
    df_ing: pd.DataFrame,
    df_gas: pd.DataFrame,
    *,
    include_miscelaneos: bool,
) -> dict:
    ing = _ensure_estado_resultados_schema(df_ing)
    gas = _ensure_estado_resultados_schema(df_gas)

    ing = ing[ing[COL_CATEGORIA].map(lambda x: include_by_category(x, include_miscelaneos))].copy()
    gas = gas[gas[COL_CATEGORIA].map(lambda x: include_by_category(x, include_miscelaneos))].copy()

    ingresos = float(ing[COL_MONTO].sum())
    costos_directos = float(gas[gas[COL_CATEGORIA].map(_is_direct_cost)][COL_MONTO].sum())
    gastos_operativos = float(gas[~gas[COL_CATEGORIA].map(_is_direct_cost)][COL_MONTO].sum())

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
    ing_month["Mes"] = pd.to_datetime(ing_month[COL_FECHA], errors="coerce").dt.to_period("M")
    gas_month["Mes"] = pd.to_datetime(gas_month[COL_FECHA], errors="coerce").dt.to_period("M")

    ing_m = ing_month.groupby("Mes", as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Ingresos"})
    gas_m = gas_month.groupby("Mes", as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Gastos"})
    mensual = ing_m.merge(gas_m, on="Mes", how="outer").fillna(0.0)
    if not mensual.empty:
        mensual["Mes"] = mensual["Mes"].dt.to_timestamp()
    mensual["Utilidad"] = mensual["Ingresos"] - mensual["Gastos"]

    empresa_ing = ing.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Ingresos"})
    empresa_gas = gas.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "Gastos"})
    por_empresa = empresa_ing.merge(empresa_gas, on=COL_EMPRESA, how="outer").fillna(0.0)
    por_empresa["Utilidad"] = por_empresa["Ingresos"] - por_empresa["Gastos"]
    por_empresa = por_empresa.sort_values("Utilidad", ascending=False)

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
