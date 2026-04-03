from __future__ import annotations

from datetime import date
from calendar import monthrange

import pandas as pd

from .constants import (
    COL_CLIENTE_NOMBRE,
    COL_EMPRESA,
    COL_FECHA,
    COL_FECHA_COBRO,
    COL_MONTO,
    COL_REC_PERIOD,
    COL_REC_RULE,
    COL_RECURRENTE,
    COL_PROVEEDOR,
    COL_PROYECTO,
)
from .helpers import normalize_text_key, yes_no_flag


def _build_cash_movements(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> pd.DataFrame:
    ing = df_ing[[COL_FECHA, COL_MONTO, COL_EMPRESA]].copy()
    ing["tipo"] = "entrada"
    ing["flujo"] = pd.to_numeric(ing[COL_MONTO], errors="coerce").fillna(0.0)

    gas = df_gas[[COL_FECHA, COL_MONTO, COL_EMPRESA]].copy()
    gas["tipo"] = "salida"
    gas["flujo"] = -pd.to_numeric(gas[COL_MONTO], errors="coerce").fillna(0.0)

    out = pd.concat([ing, gas], ignore_index=True)
    out = out.dropna(subset=[COL_FECHA]).copy()
    out[COL_FECHA] = pd.to_datetime(out[COL_FECHA], errors="coerce")
    out = out.dropna(subset=[COL_FECHA]).sort_values(COL_FECHA)
    return out


def build_cashflow_actual(df_ing_real: pd.DataFrame, df_gas_real: pd.DataFrame) -> dict:
    mov = _build_cash_movements(df_ing_real, df_gas_real)
    if mov.empty:
        empty = pd.DataFrame(columns=[COL_FECHA, "flujo", "saldo"])
        return {
            "movimientos": mov,
            "serie": empty,
            "metricas": {
                "entradas_reales": 0.0,
                "salidas_reales": 0.0,
                "flujo_neto": 0.0,
                "efectivo_actual": 0.0,
            },
        }

    diario = mov.groupby(pd.Grouper(key=COL_FECHA, freq="D"))["flujo"].sum().reset_index()
    diario["saldo"] = diario["flujo"].cumsum()

    entradas = float(mov.loc[mov["flujo"] > 0, "flujo"].sum())
    salidas = float(abs(mov.loc[mov["flujo"] < 0, "flujo"].sum()))
    neto = float(diario["flujo"].sum())
    saldo = float(diario["saldo"].iloc[-1])

    return {
        "movimientos": mov,
        "serie": diario,
        "metricas": {
            "entradas_reales": entradas,
            "salidas_reales": salidas,
            "flujo_neto": neto,
            "efectivo_actual": saldo,
        },
    }


def build_cashflow_proyectado(
    df_ing_pend: pd.DataFrame,
    df_gas_pend: pd.DataFrame,
    saldo_inicial: float,
    *,
    granularidad: str = "D",
    fecha_hoy: date | None = None,
    horizon_months: int = 4,
) -> dict:
    today = pd.Timestamp(fecha_hoy or date.today())
    today_norm = today.normalize()

    horizon_months = int(horizon_months or 4)
    if horizon_months < 1:
        horizon_months = 1
    end_period = today_norm.to_period("M") + (horizon_months - 1)
    horizon_end = end_period.to_timestamp(how="end").normalize()

    def _recurrence_interval_months(raw_value) -> int:
        key = normalize_text_key(raw_value)
        if "trimestr" in key:
            return 3
        if "bimestr" in key:
            return 2
        return 1

    def _due_date_for_period(period: pd.Period, base_date: pd.Timestamp, rule_raw) -> pd.Timestamp:
        rule = normalize_text_key(rule_raw)
        max_day = monthrange(int(period.year), int(period.month))[1]
        if "inicio" in rule:
            day = 1
        elif "15" in rule:
            day = 15
        elif "fin" in rule:
            day = max_day
        else:
            day = min(int(base_date.day) if int(base_date.day) > 0 else 1, max_day)
        return pd.Timestamp(year=int(period.year), month=int(period.month), day=day)

    def _expand_recurrent_events(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return frame
        recurrent_mask = frame.get("__is_recurrente", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
        one_off = frame.loc[~recurrent_mask].copy()
        recurrent = frame.loc[recurrent_mask].copy()
        if recurrent.empty:
            return one_off

        month_range = pd.period_range(
            start=today_norm.to_period("M"),
            end=horizon_end.to_period("M"),
            freq="M",
        )

        expanded_rows: list[dict] = []
        for _, row in recurrent.iterrows():
            base_date = pd.to_datetime(row.get("fecha_evento"), errors="coerce")
            if pd.isna(base_date):
                continue
            anchor_period = base_date.to_period("M")
            interval_months = _recurrence_interval_months(row.get(COL_REC_PERIOD, "Mensual"))
            recurrence_rule = row.get(COL_REC_RULE, "Mismo dia de fecha esperada")
            row_base = row.to_dict()
            for period in month_range:
                month_delta = (int(period.year) - int(anchor_period.year)) * 12 + (int(period.month) - int(anchor_period.month))
                if month_delta < 0 or (month_delta % interval_months) != 0:
                    continue
                due_date = _due_date_for_period(period, base_date, recurrence_rule)
                row_copy = dict(row_base)
                row_copy["fecha_evento"] = due_date
                row_copy["fecha_fuente"] = f"{row_copy.get('fecha_fuente', 'fecha')}_recurrente"
                expanded_rows.append(row_copy)

        recurrent_expanded = pd.DataFrame(expanded_rows, columns=frame.columns) if expanded_rows else pd.DataFrame(columns=frame.columns)
        return pd.concat([one_off, recurrent_expanded], ignore_index=True, sort=False)

    # Cobros esperados: usamos Fecha de cobro; fallback a Fecha si falta.
    cxc = df_ing_pend.copy()
    cxc["fecha_evento"] = pd.to_datetime(cxc.get(COL_FECHA_COBRO), errors="coerce")
    missing_cxc = cxc["fecha_evento"].isna().sum()
    cxc.loc[cxc["fecha_evento"].isna(), "fecha_evento"] = pd.to_datetime(cxc.loc[cxc["fecha_evento"].isna(), COL_FECHA], errors="coerce")
    cxc["fecha_fuente"] = "fecha_cobro"
    cxc.loc[cxc["fecha_evento"].isna(), "fecha_fuente"] = "sin_fecha"
    cxc["monto_evento"] = pd.to_numeric(cxc[COL_MONTO], errors="coerce").fillna(0.0)
    cxc["tipo_evento"] = "cobro"
    cxc_rec = cxc[COL_RECURRENTE] if COL_RECURRENTE in cxc.columns else pd.Series("No", index=cxc.index)
    cxc["__is_recurrente"] = cxc_rec.map(yes_no_flag).eq("Si")

    # Pagos esperados: preferimos fecha estimada de pago; fallback a Fecha del registro.
    cxp = df_gas_pend.copy()
    cxp["fecha_evento"] = pd.to_datetime(cxp.get("__fecha_pago_estimada"), errors="coerce")
    missing_cxp = cxp["fecha_evento"].isna().sum()
    cxp.loc[cxp["fecha_evento"].isna(), "fecha_evento"] = pd.to_datetime(cxp.loc[cxp["fecha_evento"].isna(), COL_FECHA], errors="coerce")
    cxp["fecha_fuente"] = "fecha_pago_estimada"
    cxp.loc[cxp["fecha_evento"].isna(), "fecha_fuente"] = "sin_fecha"
    cxp.loc[cxp["fecha_fuente"] == "fecha_pago_estimada", "fecha_fuente"] = cxp.get("__fecha_pago_fuente", "fecha_pago_estimada")
    cxp["monto_evento"] = -pd.to_numeric(cxp[COL_MONTO], errors="coerce").fillna(0.0)
    cxp["tipo_evento"] = "pago"
    cxp_rec = cxp[COL_RECURRENTE] if COL_RECURRENTE in cxp.columns else pd.Series("No", index=cxp.index)
    cxp["__is_recurrente"] = cxp_rec.map(yes_no_flag).eq("Si")

    recurring_cxc = int(cxc["__is_recurrente"].sum()) if "__is_recurrente" in cxc.columns else 0
    recurring_cxp = int(cxp["__is_recurrente"].sum()) if "__is_recurrente" in cxp.columns else 0

    cxc_events = _expand_recurrent_events(cxc)
    cxp_events = _expand_recurrent_events(cxp)

    events = pd.concat([
        cxc_events[["fecha_evento", "monto_evento", "tipo_evento", "fecha_fuente", COL_EMPRESA, COL_CLIENTE_NOMBRE, COL_PROYECTO]],
        cxp_events[["fecha_evento", "monto_evento", "tipo_evento", "fecha_fuente", COL_EMPRESA, COL_PROVEEDOR, COL_PROYECTO]],
    ], ignore_index=True, sort=False)

    events["fecha_evento"] = pd.to_datetime(events["fecha_evento"], errors="coerce").dt.normalize()
    events = events.dropna(subset=["fecha_evento"]).copy()
    events = events[(events["fecha_evento"] >= today_norm) & (events["fecha_evento"] <= horizon_end)].sort_values("fecha_evento")

    if events.empty:
        return {
            "eventos": events,
            "serie": pd.DataFrame(columns=["fecha_evento", "flujo_proyectado", "saldo_proyectado"]),
            "metricas": {
                "saldo_inicial": float(saldo_inicial),
                "cobros_futuros": 0.0,
                "pagos_futuros": 0.0,
                "flujo_neto_proyectado": 0.0,
                "saldo_proyectado_final": float(saldo_inicial),
            },
            "notas": [
                f"No hay eventos futuros dentro del horizonte de {horizon_months} mes(es).",
                f"Cobros pendientes sin Fecha de cobro (fallback): {int(missing_cxc)}",
                f"Pagos pendientes sin fecha estimada (fallback a Fecha registro): {int(missing_cxp)}",
                f"Registros recurrentes detectados: cobros={recurring_cxc}, pagos={recurring_cxp}",
            ],
        }

    freq_map = {"D": "D", "W": "W", "M": "M"}
    freq = freq_map.get(str(granularidad).upper(), "D")
    serie = (
        events.groupby(pd.Grouper(key="fecha_evento", freq=freq))["monto_evento"]
        .sum()
        .reset_index()
        .rename(columns={"monto_evento": "flujo_proyectado"})
    )
    serie["saldo_proyectado"] = float(saldo_inicial) + serie["flujo_proyectado"].cumsum()

    cobros = float(events.loc[events["tipo_evento"] == "cobro", "monto_evento"].sum())
    pagos = float(abs(events.loc[events["tipo_evento"] == "pago", "monto_evento"].sum()))
    flujo_neto = float(serie["flujo_proyectado"].sum())
    saldo_final = float(serie["saldo_proyectado"].iloc[-1])

    notes = [
        "Supuesto de proyeccion: caja (cobros y pagos), no devengo contable.",
        f"Horizonte de proyeccion: {horizon_months} mes(es) desde el mes actual.",
        f"Cobros pendientes sin Fecha de cobro (fallback a Fecha): {int(missing_cxc)}",
        f"Pagos pendientes sin fecha estimada (fallback a Fecha registro): {int(missing_cxp)}",
        f"Registros recurrentes detectados: cobros={recurring_cxc}, pagos={recurring_cxp}",
    ]

    return {
        "eventos": events,
        "serie": serie,
        "metricas": {
            "saldo_inicial": float(saldo_inicial),
            "cobros_futuros": cobros,
            "pagos_futuros": pagos,
            "flujo_neto_proyectado": flujo_neto,
            "saldo_proyectado_final": saldo_final,
        },
        "notas": notes,
    }
