from __future__ import annotations

from datetime import date

import pandas as pd

from .constants import (
    COL_CATEGORIA,
    COL_CLIENTE_NOMBRE,
    COL_EMPRESA,
    COL_FECHA,
    COL_FECHA_COBRO,
    COL_MONTO,
    COL_PROVEEDOR,
    COL_PROYECTO,
)
from .helpers import include_by_category, safe_div


def _safe_series(df: pd.DataFrame, col: str, default_value: object = "") -> pd.Series:
    """
    Devuelve una Series segura alineada al indice de df.
    Evita errores por columnas faltantes o duplicadas.
    """
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


def _ensure_analysis_schema(df: pd.DataFrame, *, is_gasto: bool) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if isinstance(out, pd.DataFrame) and out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated(keep="first")].copy()

    required_common = [COL_FECHA, COL_CATEGORIA, COL_MONTO, COL_EMPRESA, COL_PROYECTO]
    required_extra = [COL_PROVEEDOR] if is_gasto else [COL_CLIENTE_NOMBRE]
    for col in required_common + required_extra:
        if col not in out.columns:
            out[col] = ""

    out[COL_FECHA] = pd.to_datetime(_safe_series(out, COL_FECHA, pd.NaT), errors="coerce")
    out[COL_CATEGORIA] = _safe_series(out, COL_CATEGORIA, "").astype(str)
    out[COL_MONTO] = pd.to_numeric(_safe_series(out, COL_MONTO, 0.0), errors="coerce").fillna(0.0)
    out[COL_EMPRESA] = _safe_series(out, COL_EMPRESA, "").astype(str)
    out[COL_PROYECTO] = _safe_series(out, COL_PROYECTO, "").astype(str)
    if is_gasto:
        out[COL_PROVEEDOR] = _safe_series(out, COL_PROVEEDOR, "").astype(str)
    else:
        out[COL_CLIENTE_NOMBRE] = _safe_series(out, COL_CLIENTE_NOMBRE, "").astype(str)
    return out


def _due_status(days_delta: float | int | None) -> str:
    if days_delta is None or pd.isna(days_delta):
        return "Sin fecha"
    d = int(days_delta)
    if d < 0:
        return "Vencido"
    if d <= 7:
        return "Proximo (<=7 dias)"
    return "Programado"


def build_cuentas_por_cobrar(df_ing_pend: pd.DataFrame, *, fecha_hoy: date | None = None) -> pd.DataFrame:
    today = pd.Timestamp(fecha_hoy or date.today())
    out = df_ing_pend.copy()
    out["fecha_esperada"] = pd.to_datetime(out.get(COL_FECHA_COBRO), errors="coerce")
    out["dias_para_cobro"] = (out["fecha_esperada"] - today).dt.days
    out["estado"] = out["dias_para_cobro"].map(_due_status)

    result = pd.DataFrame(
        {
            "cliente": out.get(COL_CLIENTE_NOMBRE, ""),
            "proyecto": out.get(COL_PROYECTO, ""),
            "empresa": out.get(COL_EMPRESA, ""),
            "monto": pd.to_numeric(out.get(COL_MONTO), errors="coerce").fillna(0.0),
            "fecha_esperada_cobro": out["fecha_esperada"],
            "dias_para_cobro": out["dias_para_cobro"],
            "estado": out["estado"],
        }
    )
    result = result.sort_values(["fecha_esperada_cobro", "monto"], ascending=[True, False], na_position="last")
    return result.reset_index(drop=True)


def build_cuentas_por_pagar(df_gas_pend: pd.DataFrame, *, fecha_hoy: date | None = None) -> tuple[pd.DataFrame, dict]:
    today = pd.Timestamp(fecha_hoy or date.today())
    out = df_gas_pend.copy()

    out["fecha_esperada"] = pd.to_datetime(out.get("__fecha_pago_estimada"), errors="coerce")
    fallback_mask = out["fecha_esperada"].isna()
    out.loc[fallback_mask, "fecha_esperada"] = pd.to_datetime(out.loc[fallback_mask, COL_FECHA], errors="coerce")
    out["fuente_fecha"] = out.get("__fecha_pago_fuente", "sin_fecha")
    out.loc[fallback_mask, "fuente_fecha"] = "fallback_fecha_registro"

    out["dias_para_pago"] = (out["fecha_esperada"] - today).dt.days
    out["estado"] = out["dias_para_pago"].map(_due_status)

    result = pd.DataFrame(
        {
            "proveedor": out.get(COL_PROVEEDOR, ""),
            "proyecto": out.get(COL_PROYECTO, ""),
            "empresa": out.get(COL_EMPRESA, ""),
            "monto": pd.to_numeric(out.get(COL_MONTO), errors="coerce").fillna(0.0),
            "fecha_esperada_pago": out["fecha_esperada"],
            "dias_para_pago": out["dias_para_pago"],
            "estado": out["estado"],
            "fuente_fecha": out["fuente_fecha"],
        }
    )
    result = result.sort_values(["fecha_esperada_pago", "monto"], ascending=[True, False], na_position="last")

    quality = {
        "total_pendientes": int(len(result)),
        "con_fallback_fecha": int((result["fuente_fecha"] == "fallback_fecha_registro").sum()),
        "sin_fecha": int(result["fecha_esperada_pago"].isna().sum()),
    }
    return result.reset_index(drop=True), quality


def build_analisis_gerencial(
    df_ing: pd.DataFrame,
    df_gas: pd.DataFrame,
    cxc_df: pd.DataFrame,
    *,
    include_miscelaneos: bool,
) -> dict:
    ing = _ensure_analysis_schema(df_ing, is_gasto=False)
    gas = _ensure_analysis_schema(df_gas, is_gasto=True)

    ing_cat = _safe_series(ing, COL_CATEGORIA, "").astype(str)
    gas_cat = _safe_series(gas, COL_CATEGORIA, "").astype(str)
    ing = ing[ing_cat.map(lambda x: include_by_category(x, include_miscelaneos))].copy()
    gas = gas[gas_cat.map(lambda x: include_by_category(x, include_miscelaneos))].copy()

    ing_empresa = ing.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "ingresos"})
    gas_empresa = gas.groupby(COL_EMPRESA, as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "gastos"})
    empresa = ing_empresa.merge(gas_empresa, on=COL_EMPRESA, how="outer").fillna(0.0)
    empresa["utilidad"] = empresa["ingresos"] - empresa["gastos"]
    empresa = empresa.sort_values("utilidad", ascending=False)

    top_gastos = (
        gas.groupby(COL_CATEGORIA, as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_CATEGORIA: "categoria", COL_MONTO: "gasto"})
        .sort_values("gasto", ascending=False)
    )

    ing_m = ing.copy()
    gas_m = gas.copy()
    ing_m["mes"] = pd.to_datetime(_safe_series(ing_m, COL_FECHA, pd.NaT), errors="coerce").dt.to_period("M")
    gas_m["mes"] = pd.to_datetime(_safe_series(gas_m, COL_FECHA, pd.NaT), errors="coerce").dt.to_period("M")
    m_ing = ing_m.groupby("mes", as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "ingresos"})
    m_gas = gas_m.groupby("mes", as_index=False)[COL_MONTO].sum().rename(columns={COL_MONTO: "gastos"})
    evolucion = m_ing.merge(m_gas, on="mes", how="outer").fillna(0.0)
    if not evolucion.empty:
        evolucion["mes"] = evolucion["mes"].dt.to_timestamp()
    evolucion["utilidad"] = evolucion["ingresos"] - evolucion["gastos"]

    conc_cliente = (
        ing.groupby(COL_CLIENTE_NOMBRE, as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_CLIENTE_NOMBRE: "cliente", COL_MONTO: "ingresos"})
        .sort_values("ingresos", ascending=False)
    )
    total_ing = float(conc_cliente["ingresos"].sum()) if not conc_cliente.empty else 0.0
    if total_ing > 0:
        conc_cliente["participacion_pct"] = conc_cliente["ingresos"].map(lambda x: safe_div(x, total_ing) * 100.0)
    else:
        conc_cliente["participacion_pct"] = 0.0

    conc_proyecto = (
        ing.groupby(COL_PROYECTO, as_index=False)[COL_MONTO]
        .sum()
        .rename(columns={COL_PROYECTO: "proyecto", COL_MONTO: "ingresos"})
        .sort_values("ingresos", ascending=False)
    )

    cxc_work = cxc_df.copy() if isinstance(cxc_df, pd.DataFrame) else pd.DataFrame()
    if "cliente" not in cxc_work.columns:
        cxc_work["cliente"] = ""
    if "monto" not in cxc_work.columns:
        cxc_work["monto"] = 0.0
    cxc_work["monto"] = pd.to_numeric(cxc_work["monto"], errors="coerce").fillna(0.0)
    cxc_concentracion = cxc_work.groupby("cliente", as_index=False)["monto"].sum().sort_values("monto", ascending=False)
    cxc_total = float(cxc_concentracion["monto"].sum()) if not cxc_concentracion.empty else 0.0
    if cxc_total > 0:
        cxc_concentracion["participacion_pct"] = cxc_concentracion["monto"].map(lambda x: safe_div(x, cxc_total) * 100.0)
    else:
        cxc_concentracion["participacion_pct"] = 0.0

    return {
        "por_empresa": empresa,
        "top_gastos_categoria": top_gastos,
        "evolucion_mensual": evolucion,
        "concentracion_cliente": conc_cliente,
        "concentracion_proyecto": conc_proyecto,
        "concentracion_cxc": cxc_concentracion,
    }
