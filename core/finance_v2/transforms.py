from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .constants import (
    COL_CATEGORIA,
    COL_CLIENTE_ID,
    COL_CLIENTE_NOMBRE,
    COL_COBRADO,
    COL_CONCEPTO,
    COL_DESC,
    COL_EMPRESA,
    COL_ESCENARIO,
    COL_FECHA,
    COL_FECHA_COBRO,
    COL_FECHA_PAGO,
    COL_MONTO,
    COL_POR_COBRAR,
    COL_POR_PAGAR,
    COL_PROVEEDOR,
    COL_PROYECTO,
    COL_ROW_ID,
    COL_USUARIO,
    GASTOS_BASE_COLUMNS,
    INGRESOS_BASE_COLUMNS,
)
from .helpers import (
    include_by_category,
    normalize_category,
    normalize_text,
    parse_number_maybe_es,
    yes_no_flag,
)


@dataclass
class GlobalFilters:
    fecha_desde: date
    fecha_hasta: date
    empresa: str = "Todas"
    busqueda: str = ""
    escenarios: list[str] | None = None



def _ensure_columns(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    for col in required_columns:
        if col not in out.columns:
            out[col] = ""
    return out


def normalize_ingresos(df_ing: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_columns(df_ing, INGRESOS_BASE_COLUMNS)
    out[COL_FECHA] = pd.to_datetime(out[COL_FECHA], errors="coerce")
    out[COL_FECHA_COBRO] = pd.to_datetime(out[COL_FECHA_COBRO], errors="coerce")
    out[COL_MONTO] = out[COL_MONTO].map(parse_number_maybe_es)

    for col in [COL_DESC, COL_CONCEPTO, COL_CATEGORIA, COL_ESCENARIO, COL_PROYECTO, COL_CLIENTE_ID, COL_CLIENTE_NOMBRE, COL_EMPRESA, COL_ROW_ID, COL_USUARIO]:
        out[col] = out[col].map(normalize_text)

    out[COL_CATEGORIA] = out[COL_CATEGORIA].map(normalize_category)
    out[COL_POR_COBRAR] = out[COL_POR_COBRAR].map(yes_no_flag)
    out[COL_COBRADO] = out[COL_COBRADO].map(yes_no_flag)

    out["__source"] = "ingreso"
    return out


def normalize_gastos(df_gas: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_columns(df_gas, GASTOS_BASE_COLUMNS)
    out[COL_FECHA] = pd.to_datetime(out[COL_FECHA], errors="coerce")
    out[COL_MONTO] = out[COL_MONTO].map(parse_number_maybe_es)

    for col in [COL_DESC, COL_CONCEPTO, COL_CATEGORIA, COL_ESCENARIO, COL_PROYECTO, COL_CLIENTE_ID, COL_CLIENTE_NOMBRE, COL_EMPRESA, COL_PROVEEDOR, COL_ROW_ID, COL_USUARIO]:
        out[col] = out[col].map(normalize_text)

    out[COL_CATEGORIA] = out[COL_CATEGORIA].map(normalize_category)
    out[COL_POR_PAGAR] = out[COL_POR_PAGAR].map(yes_no_flag)

    # Fecha esperada de pago: puede no existir en el esquema actual.
    fallback_candidates = [
        COL_FECHA_PAGO,
        "Fecha de pago",
        "Fecha pago",
        "Fecha de vencimiento",
        "Fecha_pago",
    ]
    fecha_pago_col = next((c for c in fallback_candidates if c in out.columns), None)
    if fecha_pago_col:
        out["__fecha_pago_estimada"] = pd.to_datetime(out[fecha_pago_col], errors="coerce")
        out["__fecha_pago_fuente"] = "columna_fecha_pago"
    else:
        out["__fecha_pago_estimada"] = pd.NaT
        out["__fecha_pago_fuente"] = "sin_fecha_pago"

    out["__source"] = "gasto"
    return out


def get_filter_options(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> dict:
    empresas = sorted({
        *[x for x in df_ing[COL_EMPRESA].dropna().astype(str).str.strip().tolist() if x],
        *[x for x in df_gas[COL_EMPRESA].dropna().astype(str).str.strip().tolist() if x],
    })
    escenarios = sorted({
        *[x for x in df_ing[COL_ESCENARIO].dropna().astype(str).str.strip().tolist() if x],
        *[x for x in df_gas[COL_ESCENARIO].dropna().astype(str).str.strip().tolist() if x],
    })
    return {
        "empresas": empresas,
        "escenarios": escenarios,
    }


def apply_global_filters(
    df_ing: pd.DataFrame,
    df_gas: pd.DataFrame,
    filters: GlobalFilters,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _date_filter(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out = out[(out[COL_FECHA] >= pd.Timestamp(filters.fecha_desde)) & (out[COL_FECHA] <= pd.Timestamp(filters.fecha_hasta))]
        return out

    ing = _date_filter(df_ing)
    gas = _date_filter(df_gas)

    if filters.empresa and filters.empresa.lower() != "todas":
        ing = ing[ing[COL_EMPRESA].str.upper() == filters.empresa.upper()]
        gas = gas[gas[COL_EMPRESA].str.upper() == filters.empresa.upper()]

    if filters.escenarios:
        selected = {str(x).strip().lower() for x in filters.escenarios if str(x).strip()}
        if selected:
            ing = ing[ing[COL_ESCENARIO].str.lower().isin(selected)]
            gas = gas[gas[COL_ESCENARIO].str.lower().isin(selected)]

    q = (filters.busqueda or "").strip().lower()
    if q:
        def _search(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
            tmp = df.copy()
            mask = pd.Series(False, index=tmp.index)
            for col in cols:
                if col not in tmp.columns:
                    continue
                mask = mask | tmp[col].astype(str).str.lower().str.contains(q, na=False)
            return tmp[mask]

        ing = _search(ing, [COL_DESC, COL_CONCEPTO, COL_CATEGORIA, COL_PROYECTO, COL_CLIENTE_NOMBRE, COL_CLIENTE_ID, COL_EMPRESA])
        gas = _search(gas, [COL_DESC, COL_CONCEPTO, COL_CATEGORIA, COL_PROYECTO, COL_CLIENTE_NOMBRE, COL_CLIENTE_ID, COL_PROVEEDOR, COL_EMPRESA])

    return ing.copy(), gas.copy()


def split_real_vs_pending(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ing_real = df_ing[df_ing[COL_POR_COBRAR] == "No"].copy()
    ing_pend = df_ing[df_ing[COL_POR_COBRAR] == "Si"].copy()
    gas_real = df_gas[df_gas[COL_POR_PAGAR] == "No"].copy()
    gas_pend = df_gas[df_gas[COL_POR_PAGAR] == "Si"].copy()
    return {
        "ing_real": ing_real,
        "ing_pend": ing_pend,
        "gas_real": gas_real,
        "gas_pend": gas_pend,
    }


def apply_miscelaneos_policy(df: pd.DataFrame, include_miscelaneos: bool) -> pd.DataFrame:
    if include_miscelaneos:
        return df.copy()
    out = df[df[COL_CATEGORIA].map(lambda x: include_by_category(x, include_miscelaneos=False))].copy()
    return out
