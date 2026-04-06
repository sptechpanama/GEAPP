from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json

import pandas as pd

from .constants import (
    COL_ACTIVO_FIJO_DEP_MENSUAL,
    COL_ACTIVO_FIJO_DEP_TOGGLE,
    COL_ACTIVO_FIJO_FECHA_INICIO,
    COL_ACTIVO_FIJO_TOGGLE,
    COL_ACTIVO_FIJO_TIPO,
    COL_ACTIVO_FIJO_VALOR_RESIDUAL,
    COL_ACTIVO_FIJO_VIDA,
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
    COL_FECHA_REAL_COBRO,
    COL_FECHA_REAL_PAGO,
    COL_FECHA_PAGO,
    COL_FINANCIAMIENTO_CRONOGRAMA,
    COL_FINANCIAMIENTO_FECHA_INICIO,
    COL_FINANCIAMIENTO_MODALIDAD,
    COL_FINANCIAMIENTO_MONTO,
    COL_FINANCIAMIENTO_PERIODICIDAD,
    COL_FINANCIAMIENTO_PLAZO,
    COL_FINANCIAMIENTO_TASA,
    COL_FINANCIAMIENTO_TASA_TIPO,
    COL_FINANCIAMIENTO_TIPO,
    COL_FINANCIAMIENTO_TOGGLE,
    COL_CONTRAPARTE,
    COL_TIPO_CONTRAPARTE,
    COL_GASTO_DETALLE,
    COL_INGRESO_DETALLE,
    COL_MONTO,
    COL_MONTO_REAL_COBRADO,
    COL_MONTO_REAL_PAGADO,
    COL_EVENTOS_PARCIALES_ING,
    COL_EVENTOS_PARCIALES_GAS,
    COL_FACTORING_DETALLE,
    COL_INVENTARIO_MOVIMIENTO,
    COL_INVENTARIO_ITEM,
    COL_NATURALEZA_INGRESO,
    COL_POR_COBRAR,
    COL_POR_PAGAR,
    COL_PREPAGO_FECHA_INICIO,
    COL_PREPAGO_MESES,
    COL_REC_PERIOD,
    COL_REC_RULE,
    COL_RECURRENTE,
    COL_REC_COUNT,
    COL_REC_DURATION,
    COL_REC_UNTIL,
    COL_PROVEEDOR,
    COL_PROYECTO,
    COL_ROW_ID,
    COL_SUBCLASIFICACION_GERENCIAL,
    COL_TRATAMIENTO_BALANCE_GAS,
    COL_TRATAMIENTO_BALANCE_ING,
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


def _derive_ing_balance(category: str, por_cobrar: str) -> str:
    if normalize_text(category) == "Aporte de socio / capital":
        return "Patrimonio"
    if normalize_text(category) == "Financiamiento recibido":
        return "Pasivo financiero"
    return "Cuenta por cobrar" if yes_no_flag(por_cobrar) == "Si" else "Caja / banco"


def _derive_gas_sub(category: str) -> str:
    key = normalize_text(category)
    mapping = {
        "Proyectos": "Costo directo",
        "Gastos fijos": "Administrativo fijo",
        "Gastos operativos": "Operativo variable",
        "Oficina": "Administrativo fijo",
        "Inversiones": "No operativo",
        "Miscelaneos": "No operativo",
        "Comisiones": "Comercial / ventas",
        "Gasto financiero": "Financiero",
        "Impuestos": "Impuestos",
    }
    return mapping.get(key, "Operativo variable")


def _derive_gas_balance(category: str) -> str:
    if normalize_text(category) == "Inversiones":
        return "Inversion / participacion en otra empresa"
    return "Gasto del periodo"


def _coerce_non_negative_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).clip(lower=0.0)


def _parse_partial_events(raw_value) -> list[dict[str, object]]:
    try:
        data = json.loads(str(raw_value or "[]"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        fecha = pd.to_datetime(item.get("fecha"), errors="coerce")
        monto = float(pd.to_numeric(pd.Series([item.get("monto", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        nota = str(item.get("nota", "") or "").strip()
        if pd.isna(fecha) or monto <= 0:
            continue
        rows.append({"fecha": fecha, "monto": monto, "nota": nota})
    return rows


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
        "contraparte_tipo": str(data.get("contraparte_tipo", "") or "").strip(),
        "contraparte": str(data.get("contraparte", "") or "").strip(),
        "fecha_inicio": pd.to_datetime(data.get("fecha_inicio"), errors="coerce"),
        "fecha_liquidacion_final": pd.to_datetime(data.get("fecha_liquidacion_final"), errors="coerce"),
        "factored_amount": _num("factored_amount"),
        "initial_cash_received": _num("initial_cash_received"),
        "initial_retained": _num("initial_retained"),
        "initial_fee": _num("initial_fee"),
        "final_cash_received": _num("final_cash_received"),
        "final_fee": _num("final_fee"),
        "nota": str(data.get("nota", "") or "").strip(),
    }
    if not detail["modo"]:
        return {}
    return detail


def _factoring_retained_pending(raw_value) -> float:
    detail = raw_value if isinstance(raw_value, dict) else _parse_factoring_detail(raw_value)
    if not detail:
        return 0.0
    return max(
        0.0,
        float(detail.get("initial_retained", 0.0) or 0.0)
        - float(detail.get("final_cash_received", 0.0) or 0.0)
        - float(detail.get("final_fee", 0.0) or 0.0),
    )


def normalize_ingresos(df_ing: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_columns(df_ing, INGRESOS_BASE_COLUMNS)
    out[COL_FECHA] = pd.to_datetime(out[COL_FECHA], errors="coerce")
    out[COL_FECHA_COBRO] = pd.to_datetime(out[COL_FECHA_COBRO], errors="coerce")
    out[COL_FECHA_REAL_COBRO] = pd.to_datetime(out[COL_FECHA_REAL_COBRO], errors="coerce")
    out[COL_REC_UNTIL] = pd.to_datetime(out[COL_REC_UNTIL], errors="coerce")
    out[COL_FINANCIAMIENTO_FECHA_INICIO] = pd.to_datetime(out[COL_FINANCIAMIENTO_FECHA_INICIO], errors="coerce")
    out[COL_MONTO] = out[COL_MONTO].map(parse_number_maybe_es)
    out[COL_FINANCIAMIENTO_MONTO] = out[COL_FINANCIAMIENTO_MONTO].map(parse_number_maybe_es)
    out[COL_FINANCIAMIENTO_TASA] = out[COL_FINANCIAMIENTO_TASA].map(parse_number_maybe_es)
    out[COL_MONTO_REAL_COBRADO] = out[COL_MONTO_REAL_COBRADO].map(parse_number_maybe_es)
    out[COL_FINANCIAMIENTO_PLAZO] = pd.to_numeric(out[COL_FINANCIAMIENTO_PLAZO], errors="coerce").fillna(0).astype(int)
    out[COL_REC_COUNT] = pd.to_numeric(out[COL_REC_COUNT], errors="coerce").fillna(0).astype(int)

    for col in [
        COL_DESC,
        COL_CONCEPTO,
        COL_CATEGORIA,
        COL_ESCENARIO,
        COL_PROYECTO,
        COL_CLIENTE_ID,
        COL_CLIENTE_NOMBRE,
        COL_EMPRESA,
        COL_REC_PERIOD,
        COL_REC_RULE,
        COL_REC_DURATION,
        COL_ROW_ID,
        COL_USUARIO,
        COL_INGRESO_DETALLE,
        COL_NATURALEZA_INGRESO,
        COL_TRATAMIENTO_BALANCE_ING,
        COL_FINANCIAMIENTO_TOGGLE,
        COL_FINANCIAMIENTO_TIPO,
        COL_FINANCIAMIENTO_TASA_TIPO,
        COL_FINANCIAMIENTO_MODALIDAD,
        COL_FINANCIAMIENTO_PERIODICIDAD,
        COL_FINANCIAMIENTO_CRONOGRAMA,
        COL_TIPO_CONTRAPARTE,
        COL_CONTRAPARTE,
        COL_EVENTOS_PARCIALES_ING,
    ]:
        out[col] = out[col].map(normalize_text)

    out[COL_CATEGORIA] = out[COL_CATEGORIA].map(normalize_category)
    out[COL_POR_COBRAR] = out[COL_POR_COBRAR].map(yes_no_flag)
    out[COL_COBRADO] = out[COL_COBRADO].map(yes_no_flag)
    out[COL_RECURRENTE] = out[COL_RECURRENTE].map(yes_no_flag)
    out[COL_FINANCIAMIENTO_TOGGLE] = out[COL_FINANCIAMIENTO_TOGGLE].map(yes_no_flag)
    out.loc[out[COL_RECURRENTE] != "Si", [COL_REC_PERIOD, COL_REC_RULE, COL_REC_DURATION]] = ""
    out.loc[out[COL_RECURRENTE] != "Si", COL_REC_COUNT] = 0
    out.loc[out[COL_RECURRENTE] != "Si", COL_REC_UNTIL] = pd.NaT
    out.loc[(out[COL_RECURRENTE] == "Si") & (out[COL_REC_PERIOD] == ""), COL_REC_PERIOD] = "Mensual"
    out.loc[(out[COL_RECURRENTE] == "Si") & (out[COL_REC_RULE] == ""), COL_REC_RULE] = "Inicio de cada mes"
    out.loc[(out[COL_RECURRENTE] == "Si") & (out[COL_REC_DURATION] == ""), COL_REC_DURATION] = "Indefinida"
    out.loc[out[COL_REC_PERIOD] == "Quincenal", COL_REC_PERIOD] = "15nal"
    out.loc[out[COL_REC_PERIOD].isin(["Bimestral", "Trimestral"]), COL_REC_PERIOD] = "Mensual"
    out.loc[out[COL_REC_RULE] == "Fin de mes", COL_REC_RULE] = "Inicio de cada mes"
    out[COL_NATURALEZA_INGRESO] = out[COL_CATEGORIA].map(
        lambda x: "Capital" if normalize_text(x) == "Aporte de socio / capital" else (
            "Financiamiento" if normalize_text(x) == "Financiamiento recibido" else (
            "Financiero" if normalize_text(x) == "Ingreso financiero" else (
                "No operativo" if normalize_text(x) == "Ingreso no operativo" else "Operativo"
            )
            )
        )
    )
    treat_mask = out[COL_TRATAMIENTO_BALANCE_ING].isin(["", "Pasivo financiero", "Patrimonio", "Cuenta por cobrar", "Caja / banco"])
    out.loc[treat_mask, COL_TRATAMIENTO_BALANCE_ING] = [
        _derive_ing_balance(cat, por_cobrar)
        for cat, por_cobrar in zip(out.loc[treat_mask, COL_CATEGORIA], out.loc[treat_mask, COL_POR_COBRAR])
    ]
    total_ing = _coerce_non_negative_numeric(out[COL_MONTO])
    factoring_details = out[COL_FACTORING_DETALLE].map(_parse_factoring_detail)
    out["__is_factored"] = factoring_details.map(bool)
    out["__factoring_retenido_pendiente"] = factoring_details.map(_factoring_retained_pending)
    out["__factoring_detalle"] = factoring_details
    realized_ing = _coerce_non_negative_numeric(out[COL_MONTO_REAL_COBRADO]).clip(upper=total_ing)
    event_realized = out[COL_EVENTOS_PARCIALES_ING].map(lambda raw: sum(evt["monto"] for evt in _parse_partial_events(raw)))
    event_realized = pd.to_numeric(event_realized, errors="coerce").fillna(0.0).clip(lower=0.0)
    realized_ing = event_realized.where(event_realized > 0, realized_ing).clip(upper=total_ing)
    full_realized_mask = out[COL_POR_COBRAR].eq("No")
    full_cash_mask = full_realized_mask & ~out["__is_factored"]
    realized_ing = realized_ing.where(~full_cash_mask, total_ing)
    out[COL_MONTO_REAL_COBRADO] = realized_ing
    out.loc[event_realized > 0, COL_FECHA_REAL_COBRO] = out.loc[event_realized > 0, COL_EVENTOS_PARCIALES_ING].map(
        lambda raw: max((evt["fecha"] for evt in _parse_partial_events(raw)), default=pd.NaT)
    )
    out["__monto_realizado"] = realized_ing
    out["__monto_pendiente"] = (total_ing - realized_ing).clip(lower=0.0)
    out.loc[full_realized_mask, "__monto_pendiente"] = 0.0
    out["__is_financiamiento"] = (
        out[COL_FINANCIAMIENTO_TOGGLE].map(yes_no_flag).eq("Si")
        | out[COL_CATEGORIA].eq("Financiamiento recibido")
    )
    out["__source"] = "ingreso"
    return out


def normalize_gastos(df_gas: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_columns(df_gas, GASTOS_BASE_COLUMNS)
    out[COL_FECHA] = pd.to_datetime(out[COL_FECHA], errors="coerce")
    out[COL_FECHA_PAGO] = pd.to_datetime(out[COL_FECHA_PAGO], errors="coerce")
    out[COL_FECHA_REAL_PAGO] = pd.to_datetime(out[COL_FECHA_REAL_PAGO], errors="coerce")
    out[COL_REC_UNTIL] = pd.to_datetime(out[COL_REC_UNTIL], errors="coerce")
    out[COL_ACTIVO_FIJO_FECHA_INICIO] = pd.to_datetime(out[COL_ACTIVO_FIJO_FECHA_INICIO], errors="coerce")
    out[COL_FINANCIAMIENTO_FECHA_INICIO] = pd.to_datetime(out[COL_FINANCIAMIENTO_FECHA_INICIO], errors="coerce")
    out[COL_MONTO] = out[COL_MONTO].map(parse_number_maybe_es)
    out[COL_ACTIVO_FIJO_VALOR_RESIDUAL] = out[COL_ACTIVO_FIJO_VALOR_RESIDUAL].map(parse_number_maybe_es)
    out[COL_ACTIVO_FIJO_DEP_MENSUAL] = out[COL_ACTIVO_FIJO_DEP_MENSUAL].map(parse_number_maybe_es)
    out[COL_FINANCIAMIENTO_MONTO] = out[COL_FINANCIAMIENTO_MONTO].map(parse_number_maybe_es)
    out[COL_FINANCIAMIENTO_TASA] = out[COL_FINANCIAMIENTO_TASA].map(parse_number_maybe_es)
    out[COL_MONTO_REAL_PAGADO] = out[COL_MONTO_REAL_PAGADO].map(parse_number_maybe_es)
    out[COL_PREPAGO_FECHA_INICIO] = pd.to_datetime(out[COL_PREPAGO_FECHA_INICIO], errors="coerce")
    out[COL_PREPAGO_MESES] = pd.to_numeric(out[COL_PREPAGO_MESES], errors="coerce").fillna(0).astype(int)
    out[COL_FINANCIAMIENTO_PLAZO] = pd.to_numeric(out[COL_FINANCIAMIENTO_PLAZO], errors="coerce").fillna(0).astype(int)
    out[COL_REC_COUNT] = pd.to_numeric(out[COL_REC_COUNT], errors="coerce").fillna(0).astype(int)
    out[COL_ACTIVO_FIJO_VIDA] = pd.to_numeric(out[COL_ACTIVO_FIJO_VIDA], errors="coerce").fillna(0).astype(int)

    for col in [
        COL_DESC,
        COL_CONCEPTO,
        COL_CATEGORIA,
        COL_ESCENARIO,
        COL_PROYECTO,
        COL_CLIENTE_ID,
        COL_CLIENTE_NOMBRE,
        COL_EMPRESA,
        COL_PROVEEDOR,
        COL_REC_PERIOD,
        COL_REC_RULE,
        COL_REC_DURATION,
        COL_ROW_ID,
        COL_USUARIO,
        COL_SUBCLASIFICACION_GERENCIAL,
        COL_GASTO_DETALLE,
        COL_TRATAMIENTO_BALANCE_GAS,
        COL_ACTIVO_FIJO_TOGGLE,
        COL_ACTIVO_FIJO_TIPO,
        COL_ACTIVO_FIJO_DEP_TOGGLE,
        COL_FINANCIAMIENTO_TOGGLE,
        COL_FINANCIAMIENTO_TIPO,
        COL_FINANCIAMIENTO_TASA_TIPO,
        COL_FINANCIAMIENTO_MODALIDAD,
        COL_FINANCIAMIENTO_PERIODICIDAD,
        COL_FINANCIAMIENTO_CRONOGRAMA,
        COL_TIPO_CONTRAPARTE,
        COL_CONTRAPARTE,
        COL_EVENTOS_PARCIALES_GAS,
        COL_INVENTARIO_MOVIMIENTO,
        COL_INVENTARIO_ITEM,
    ]:
        out[col] = out[col].map(normalize_text)

    out[COL_CATEGORIA] = out[COL_CATEGORIA].map(normalize_category)
    out[COL_POR_PAGAR] = out[COL_POR_PAGAR].map(yes_no_flag)
    out[COL_RECURRENTE] = out[COL_RECURRENTE].map(yes_no_flag)
    out[COL_ACTIVO_FIJO_TOGGLE] = out[COL_ACTIVO_FIJO_TOGGLE].map(yes_no_flag)
    out[COL_ACTIVO_FIJO_DEP_TOGGLE] = out[COL_ACTIVO_FIJO_DEP_TOGGLE].map(yes_no_flag)
    out[COL_FINANCIAMIENTO_TOGGLE] = out[COL_FINANCIAMIENTO_TOGGLE].map(yes_no_flag)
    out.loc[out[COL_RECURRENTE] != "Si", [COL_REC_PERIOD, COL_REC_RULE, COL_REC_DURATION]] = ""
    out.loc[out[COL_RECURRENTE] != "Si", COL_REC_COUNT] = 0
    out.loc[out[COL_RECURRENTE] != "Si", COL_REC_UNTIL] = pd.NaT
    out.loc[(out[COL_RECURRENTE] == "Si") & (out[COL_REC_PERIOD] == ""), COL_REC_PERIOD] = "Mensual"
    out.loc[(out[COL_RECURRENTE] == "Si") & (out[COL_REC_RULE] == ""), COL_REC_RULE] = "Inicio de cada mes"
    out.loc[(out[COL_RECURRENTE] == "Si") & (out[COL_REC_DURATION] == ""), COL_REC_DURATION] = "Indefinida"
    out.loc[out[COL_REC_PERIOD] == "Quincenal", COL_REC_PERIOD] = "15nal"
    out.loc[out[COL_REC_PERIOD].isin(["Bimestral", "Trimestral"]), COL_REC_PERIOD] = "Mensual"
    out.loc[out[COL_REC_RULE] == "Fin de mes", COL_REC_RULE] = "Inicio de cada mes"
    out[COL_SUBCLASIFICACION_GERENCIAL] = out[COL_CATEGORIA].map(_derive_gas_sub)
    treat_mask = out[COL_TRATAMIENTO_BALANCE_GAS].isin(["", "Inversion / participacion en otra empresa", "Gasto del periodo"])
    out.loc[treat_mask, COL_TRATAMIENTO_BALANCE_GAS] = out.loc[treat_mask, COL_CATEGORIA].map(_derive_gas_balance)
    total_gas = _coerce_non_negative_numeric(out[COL_MONTO])
    realized_gas = _coerce_non_negative_numeric(out[COL_MONTO_REAL_PAGADO]).clip(upper=total_gas)
    event_realized = out[COL_EVENTOS_PARCIALES_GAS].map(lambda raw: sum(evt["monto"] for evt in _parse_partial_events(raw)))
    event_realized = pd.to_numeric(event_realized, errors="coerce").fillna(0.0).clip(lower=0.0)
    realized_gas = event_realized.where(event_realized > 0, realized_gas).clip(upper=total_gas)
    full_paid_mask = out[COL_POR_PAGAR].eq("No")
    realized_gas = realized_gas.where(~full_paid_mask, total_gas)
    out[COL_MONTO_REAL_PAGADO] = realized_gas
    out.loc[event_realized > 0, COL_FECHA_REAL_PAGO] = out.loc[event_realized > 0, COL_EVENTOS_PARCIALES_GAS].map(
        lambda raw: max((evt["fecha"] for evt in _parse_partial_events(raw)), default=pd.NaT)
    )
    out["__monto_realizado"] = realized_gas
    out["__monto_pendiente"] = (total_gas - realized_gas).clip(lower=0.0)
    out.loc[full_paid_mask, "__monto_pendiente"] = 0.0
    prepago_mask = out[COL_TRATAMIENTO_BALANCE_GAS].eq("Anticipo / prepago")
    out.loc[~prepago_mask, [COL_PREPAGO_MESES, COL_PREPAGO_FECHA_INICIO]] = [0, pd.NaT]
    out.loc[prepago_mask & out[COL_PREPAGO_FECHA_INICIO].isna(), COL_PREPAGO_FECHA_INICIO] = out.loc[prepago_mask & out[COL_PREPAGO_FECHA_INICIO].isna(), COL_FECHA]
    inventory_mask = out[COL_TRATAMIENTO_BALANCE_GAS].eq("Inventario")
    out.loc[~inventory_mask, [COL_INVENTARIO_MOVIMIENTO, COL_INVENTARIO_ITEM]] = ["", ""]
    out.loc[inventory_mask & out[COL_INVENTARIO_MOVIMIENTO].eq(""), COL_INVENTARIO_MOVIMIENTO] = "Entrada"

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

    out["__is_financiamiento"] = out[COL_FINANCIAMIENTO_TOGGLE].map(yes_no_flag).eq("Si")
    out["__is_activo_fijo"] = (
        out[COL_ACTIVO_FIJO_TOGGLE].map(yes_no_flag).eq("Si")
        | out[COL_TRATAMIENTO_BALANCE_GAS].eq("Activo fijo")
    )
    out["__source"] = "gasto"
    return out


def get_filter_options(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> dict:
    def _clean_option(x) -> str:
        try:
            if pd.isna(x):
                return ""
        except Exception:
            pass
        val = str(x or "").strip()
        if val.lower() in {"nan", "none", "null"}:
            return ""
        return val

    empresas = sorted({
        *[_clean_option(x) for x in df_ing[COL_EMPRESA].tolist()],
        *[_clean_option(x) for x in df_gas[COL_EMPRESA].tolist()],
    })
    escenarios = sorted({
        *[_clean_option(x) for x in df_ing[COL_ESCENARIO].tolist()],
        *[_clean_option(x) for x in df_gas[COL_ESCENARIO].tolist()],
    })
    empresas = [x for x in empresas if x]
    escenarios = [x for x in escenarios if x]
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
    ing_real_mask = pd.to_numeric(df_ing.get("__monto_realizado", pd.Series(0.0, index=df_ing.index)), errors="coerce").fillna(0.0).astype(float) > 0
    ing_pend_mask = pd.to_numeric(df_ing.get("__monto_pendiente", pd.Series(0.0, index=df_ing.index)), errors="coerce").fillna(0.0).astype(float) > 0
    gas_real_mask = pd.to_numeric(df_gas.get("__monto_realizado", pd.Series(0.0, index=df_gas.index)), errors="coerce").fillna(0.0).astype(float) > 0
    gas_pend_mask = pd.to_numeric(df_gas.get("__monto_pendiente", pd.Series(0.0, index=df_gas.index)), errors="coerce").fillna(0.0).astype(float) > 0
    ing_real = df_ing[ing_real_mask].copy()
    ing_pend = df_ing[ing_pend_mask].copy()
    gas_real = df_gas[gas_real_mask].copy()
    gas_pend = df_gas[gas_pend_mask].copy()
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
