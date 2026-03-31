from __future__ import annotations

from datetime import date

import pandas as pd

from .constants import (
    COL_CLIENTE_NOMBRE,
    COL_EMPRESA,
    COL_FECHA,
    COL_FECHA_COBRO,
    COL_MONTO,
    COL_PROVEEDOR,
    COL_PROYECTO,
)


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
) -> dict:
    today = pd.Timestamp(fecha_hoy or date.today())

    # Cobros esperados: usamos Fecha de cobro; fallback a Fecha si falta.
    cxc = df_ing_pend.copy()
    cxc["fecha_evento"] = pd.to_datetime(cxc.get(COL_FECHA_COBRO), errors="coerce")
    missing_cxc = cxc["fecha_evento"].isna().sum()
    cxc.loc[cxc["fecha_evento"].isna(), "fecha_evento"] = pd.to_datetime(cxc.loc[cxc["fecha_evento"].isna(), COL_FECHA], errors="coerce")
    cxc["fecha_fuente"] = "fecha_cobro"
    cxc.loc[cxc["fecha_evento"].isna(), "fecha_fuente"] = "sin_fecha"
    cxc["monto_evento"] = pd.to_numeric(cxc[COL_MONTO], errors="coerce").fillna(0.0)
    cxc["tipo_evento"] = "cobro"

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

    events = pd.concat([
        cxc[["fecha_evento", "monto_evento", "tipo_evento", "fecha_fuente", COL_EMPRESA, COL_CLIENTE_NOMBRE, COL_PROYECTO]],
        cxp[["fecha_evento", "monto_evento", "tipo_evento", "fecha_fuente", COL_EMPRESA, COL_PROVEEDOR, COL_PROYECTO]],
    ], ignore_index=True, sort=False)

    events["fecha_evento"] = pd.to_datetime(events["fecha_evento"], errors="coerce")
    events = events.dropna(subset=["fecha_evento"]).copy()
    events = events[events["fecha_evento"] >= today].sort_values("fecha_evento")

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
                "No hay eventos futuros dentro del rango filtrado.",
                f"Cobros pendientes sin Fecha de cobro (fallback): {int(missing_cxc)}",
                f"Pagos pendientes sin fecha estimada (fallback a Fecha registro): {int(missing_cxp)}",
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
        f"Cobros pendientes sin Fecha de cobro (fallback a Fecha): {int(missing_cxc)}",
        f"Pagos pendientes sin fecha estimada (fallback a Fecha registro): {int(missing_cxp)}",
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
