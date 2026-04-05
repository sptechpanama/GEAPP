from __future__ import annotations

import json
from calendar import monthrange
from datetime import date

import pandas as pd

from .constants import (
    COL_CLIENTE_NOMBRE,
    COL_EMPRESA,
    COL_FECHA,
    COL_FECHA_COBRO,
    COL_FECHA_PAGO,
    COL_FECHA_REAL_COBRO,
    COL_FECHA_REAL_PAGO,
    COL_FINANCIAMIENTO_CRONOGRAMA,
    COL_FINANCIAMIENTO_TIPO,
    COL_MONTO,
    COL_POR_COBRAR,
    COL_POR_PAGAR,
    COL_REC_COUNT,
    COL_REC_DURATION,
    COL_REC_PERIOD,
    COL_REC_RULE,
    COL_REC_UNTIL,
    COL_RECURRENTE,
    COL_PROVEEDOR,
    COL_PROYECTO,
)
from .helpers import normalize_text_key, yes_no_flag


def _safe_schedule(raw_value) -> list[dict]:
    try:
        data = json.loads(str(raw_value or "[]"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _build_cash_movements(df_ing_real: pd.DataFrame, df_gas_real: pd.DataFrame) -> pd.DataFrame:
    ing = df_ing_real.copy()
    ing_fecha = pd.to_datetime(ing.get(COL_FECHA_REAL_COBRO), errors="coerce")
    ing_fecha = ing_fecha.fillna(pd.to_datetime(ing.get(COL_FECHA), errors="coerce"))
    ing_monto = pd.to_numeric(ing.get("__monto_realizado"), errors="coerce")
    if ing_monto.isna().all():
        ing_monto = pd.to_numeric(ing.get(COL_MONTO), errors="coerce")
    ing = pd.DataFrame(
        {
            COL_FECHA: ing_fecha,
            COL_EMPRESA: ing.get(COL_EMPRESA, ""),
            "tipo": "entrada",
            "flujo": ing_monto.fillna(0.0),
        }
    )

    gas = df_gas_real.copy()
    gas_fecha = pd.to_datetime(gas.get(COL_FECHA_REAL_PAGO), errors="coerce")
    gas_fecha = gas_fecha.fillna(pd.to_datetime(gas.get(COL_FECHA), errors="coerce"))
    gas_monto = pd.to_numeric(gas.get("__monto_realizado"), errors="coerce")
    if gas_monto.isna().all():
        gas_monto = pd.to_numeric(gas.get(COL_MONTO), errors="coerce")
    gas = pd.DataFrame(
        {
            COL_FECHA: gas_fecha,
            COL_EMPRESA: gas.get(COL_EMPRESA, ""),
            "tipo": "salida",
            "flujo": -gas_monto.fillna(0.0),
        }
    )

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


def _recurrence_interval_months(raw_value) -> int:
    key = normalize_text_key(raw_value)
    if "semestr" in key:
        return 6
    return 1


def _due_date_for_period(period: pd.Period, base_date: pd.Timestamp, rule_raw) -> pd.Timestamp:
    rule = normalize_text_key(rule_raw)
    max_day = monthrange(int(period.year), int(period.month))[1]
    if "inicio" in rule:
        day = 1
    elif "15" in rule:
        day = 15
    else:
        day = min(max(1, int(base_date.day)), max_day)
    return pd.Timestamp(year=int(period.year), month=int(period.month), day=day)


def _within_recurrence_limit(row: pd.Series, event_index: int, event_date: pd.Timestamp) -> bool:
    duration = normalize_text_key(row.get(COL_REC_DURATION, ""))
    until_date = pd.to_datetime(row.get(COL_REC_UNTIL), errors="coerce")
    count_limit = int(pd.to_numeric(pd.Series([row.get(COL_REC_COUNT, 0)]), errors="coerce").fillna(0).iloc[0])
    if "hasta fecha" in duration and pd.notna(until_date):
        return event_date.normalize() <= until_date.normalize()
    if "cantidad" in duration and count_limit > 0:
        return event_index <= count_limit
    return True


def _expand_recurrent_events(frame: pd.DataFrame, today_norm: pd.Timestamp, horizon_end: pd.Timestamp) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=frame.columns if isinstance(frame, pd.DataFrame) else [])
    recurrent_mask = frame.get("__is_recurrente", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
    one_off = frame.loc[~recurrent_mask].copy()
    recurrent = frame.loc[recurrent_mask].copy()
    if recurrent.empty:
        return one_off

    expanded_rows: list[dict] = []
    for _, row in recurrent.iterrows():
        base_date = pd.to_datetime(row.get("fecha_evento"), errors="coerce")
        if pd.isna(base_date):
            continue
        period_key = normalize_text_key(row.get(COL_REC_PERIOD, "Mensual"))
        recurrence_rule = row.get(COL_REC_RULE, "Inicio de cada mes")
        row_base = row.to_dict()
        event_index = 0
        if "quinc" in period_key or "15nal" in period_key:
            current = today_norm.normalize().replace(day=1)
            while current <= horizon_end:
                for day in (1, 15):
                    max_day = monthrange(current.year, current.month)[1]
                    due_date = pd.Timestamp(year=current.year, month=current.month, day=min(day, max_day))
                    if due_date < today_norm or due_date > horizon_end:
                        continue
                    event_index += 1
                    if not _within_recurrence_limit(row, event_index, due_date):
                        continue
                    row_copy = dict(row_base)
                    row_copy["fecha_evento"] = due_date
                    row_copy["fecha_fuente"] = f"{row_copy.get('fecha_fuente', 'fecha')}_recurrente"
                    expanded_rows.append(row_copy)
                current = current + pd.DateOffset(months=1)
            continue

        anchor_period = base_date.to_period("M")
        month_range = pd.period_range(start=today_norm.to_period("M"), end=horizon_end.to_period("M"), freq="M")
        interval_months = _recurrence_interval_months(row.get(COL_REC_PERIOD, "Mensual"))
        for period in month_range:
            month_delta = (int(period.year) - int(anchor_period.year)) * 12 + (int(period.month) - int(anchor_period.month))
            if month_delta < 0 or (month_delta % interval_months) != 0:
                continue
            due_date = _due_date_for_period(period, base_date, recurrence_rule)
            if due_date < today_norm or due_date > horizon_end:
                continue
            event_index += 1
            if not _within_recurrence_limit(row, event_index, due_date):
                continue
            row_copy = dict(row_base)
            row_copy["fecha_evento"] = due_date
            row_copy["fecha_fuente"] = f"{row_copy.get('fecha_fuente', 'fecha')}_recurrente"
            expanded_rows.append(row_copy)

    recurrent_expanded = pd.DataFrame(expanded_rows, columns=frame.columns) if expanded_rows else pd.DataFrame(columns=frame.columns)
    return pd.concat([one_off, recurrent_expanded], ignore_index=True, sort=False)


def _build_financing_events(df_ing: pd.DataFrame, df_gas: pd.DataFrame, today_norm: pd.Timestamp, horizon_end: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for frame, source in ((df_ing, "ingreso"), (df_gas, "gasto")):
        if frame is None or frame.empty:
            continue
        for _, row in frame.iterrows():
            schedule = _safe_schedule(row.get(COL_FINANCIAMIENTO_CRONOGRAMA, "[]"))
            fin_type = str(row.get(COL_FINANCIAMIENTO_TIPO, "")).strip()
            if not schedule or not fin_type:
                continue
            if fin_type == "Financiamiento recibido":
                sign = -1.0
            elif fin_type == "Financiamiento otorgado":
                sign = 1.0
            elif fin_type == "Activo fijo financiado":
                sign = -1.0
            else:
                continue
            for item in schedule:
                due_date = pd.to_datetime(item.get("fecha"), errors="coerce")
                if pd.isna(due_date):
                    continue
                due_date = due_date.normalize()
                if due_date < today_norm or due_date > horizon_end:
                    continue
                rows.append(
                    {
                        "fecha_evento": due_date,
                        "monto_evento": sign * float(item.get("cuota_total", 0.0) or 0.0),
                        "tipo_evento": "cobro" if sign > 0 else "pago",
                        "fecha_fuente": f"cronograma_{source}",
                        COL_EMPRESA: row.get(COL_EMPRESA, ""),
                        COL_CLIENTE_NOMBRE: row.get(COL_CLIENTE_NOMBRE, "") if source == "ingreso" else "",
                        COL_PROVEEDOR: row.get(COL_PROVEEDOR, "") if source == "gasto" else "",
                        COL_PROYECTO: row.get(COL_PROYECTO, ""),
                        "interes": float(item.get("interes", 0.0) or 0.0),
                        "capital": float(item.get("capital", 0.0) or 0.0),
                        "fin_type": fin_type,
                    }
                )

    return pd.DataFrame(rows)


def build_cashflow_proyectado(
    df_ing: pd.DataFrame,
    df_gas: pd.DataFrame,
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

    cxc = df_ing[df_ing.get(COL_POR_COBRAR, "No").astype(str).eq("Si")].copy() if isinstance(df_ing, pd.DataFrame) else pd.DataFrame()
    cxc["fecha_evento"] = pd.to_datetime(cxc.get(COL_FECHA_COBRO), errors="coerce")
    missing_cxc = cxc["fecha_evento"].isna().sum() if not cxc.empty else 0
    if not cxc.empty:
        cxc.loc[cxc["fecha_evento"].isna(), "fecha_evento"] = pd.to_datetime(cxc.loc[cxc["fecha_evento"].isna(), COL_FECHA], errors="coerce")
    cxc["fecha_fuente"] = "fecha_cobro"
    cxc["monto_evento"] = pd.to_numeric(cxc.get("__monto_pendiente"), errors="coerce").fillna(
        pd.to_numeric(cxc.get(COL_MONTO), errors="coerce").fillna(0.0)
    )
    cxc["tipo_evento"] = "cobro"
    cxc["__is_recurrente"] = cxc.get(COL_RECURRENTE, pd.Series("No", index=cxc.index)).map(yes_no_flag).eq("Si") if not cxc.empty else pd.Series(dtype=bool)

    cxp = df_gas[df_gas.get(COL_POR_PAGAR, "No").astype(str).eq("Si")].copy() if isinstance(df_gas, pd.DataFrame) else pd.DataFrame()
    cxp["fecha_evento"] = pd.to_datetime(cxp.get("__fecha_pago_estimada"), errors="coerce")
    missing_cxp = cxp["fecha_evento"].isna().sum() if not cxp.empty else 0
    if not cxp.empty:
        cxp.loc[cxp["fecha_evento"].isna(), "fecha_evento"] = pd.to_datetime(cxp.loc[cxp["fecha_evento"].isna(), COL_FECHA], errors="coerce")
    cxp["fecha_fuente"] = "fecha_pago_estimada"
    cxp["monto_evento"] = -pd.to_numeric(cxp.get("__monto_pendiente"), errors="coerce").fillna(
        pd.to_numeric(cxp.get(COL_MONTO), errors="coerce").fillna(0.0)
    )
    cxp["tipo_evento"] = "pago"
    cxp["__is_recurrente"] = cxp.get(COL_RECURRENTE, pd.Series("No", index=cxp.index)).map(yes_no_flag).eq("Si") if not cxp.empty else pd.Series(dtype=bool)

    recurring_cxc = int(cxc["__is_recurrente"].sum()) if not cxc.empty and "__is_recurrente" in cxc.columns else 0
    recurring_cxp = int(cxp["__is_recurrente"].sum()) if not cxp.empty and "__is_recurrente" in cxp.columns else 0

    cxc = cxc[cxc["monto_evento"] > 0].copy()
    cxp = cxp[cxp["monto_evento"] < 0].copy()
    cxc_events = _expand_recurrent_events(cxc, today_norm, horizon_end)
    cxp_events = _expand_recurrent_events(cxp, today_norm, horizon_end)
    financing_events = _build_financing_events(df_ing, df_gas, today_norm, horizon_end)

    event_frames: list[pd.DataFrame] = []
    if not cxc_events.empty:
        event_frames.append(
            cxc_events[
                ["fecha_evento", "monto_evento", "tipo_evento", "fecha_fuente", COL_EMPRESA, COL_CLIENTE_NOMBRE, COL_PROYECTO]
            ]
        )
    if not cxp_events.empty:
        event_frames.append(
            cxp_events[
                ["fecha_evento", "monto_evento", "tipo_evento", "fecha_fuente", COL_EMPRESA, COL_PROVEEDOR, COL_PROYECTO]
            ]
        )
    if not financing_events.empty:
        event_frames.append(financing_events)
    events = pd.concat(event_frames, ignore_index=True, sort=False) if event_frames else pd.DataFrame()

    if not events.empty:
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
                f"Cronogramas de financiamiento detectados: {int(len(financing_events))}",
            ],
        }

    freq_map = {"D": "D", "W": "W", "M": "ME"}
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
    neto = float(events["monto_evento"].sum())
    saldo_final = float(saldo_inicial + neto)

    return {
        "eventos": events.reset_index(drop=True),
        "serie": serie,
        "metricas": {
            "saldo_inicial": float(saldo_inicial),
            "cobros_futuros": cobros,
            "pagos_futuros": pagos,
            "flujo_neto_proyectado": neto,
            "saldo_proyectado_final": saldo_final,
        },
        "notas": [
            f"Cobros pendientes sin Fecha de cobro (fallback): {int(missing_cxc)}",
            f"Pagos pendientes sin fecha estimada (fallback a Fecha registro): {int(missing_cxp)}",
            f"Registros recurrentes detectados: cobros={recurring_cxc}, pagos={recurring_cxp}",
            f"Cronogramas de financiamiento detectados: {int(len(financing_events))}",
        ],
    }
