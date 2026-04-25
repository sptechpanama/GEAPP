import json

import pandas as pd


def _yes_no_norm(value) -> str:
    s = str(value).strip().lower()
    return "Sí" if s in {"si", "sí", "sí", "yes", "y", "true", "1"} else "No"


def _extract_income_movements(df_ing: pd.DataFrame) -> pd.DataFrame:
    if df_ing is None or df_ing.empty or "Monto" not in df_ing or "Fecha" not in df_ing:
        return pd.DataFrame(columns=["Fecha", "Monto"])

    def _parse_events(raw):
        try:
            data = json.loads(str(raw or "[]"))
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        rows = []
        for item in data:
            if not isinstance(item, dict):
                continue
            fecha = pd.to_datetime(item.get("fecha"), errors="coerce")
            monto = float(pd.to_numeric(pd.Series([item.get("monto", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            if pd.isna(fecha) or monto <= 0:
                continue
            rows.append({"Fecha": fecha, "Monto": monto})
        return rows

    out = df_ing.copy()
    partial_rows: list[dict] = []
    for _, row in out.iterrows():
        events = _parse_events(row.get("Detalle cobros parciales"))
        for evt in events:
            partial_rows.append(evt)
    if partial_rows:
        return pd.DataFrame(partial_rows).sort_values("Fecha").reset_index(drop=True)

    total = pd.to_numeric(out.get("Monto"), errors="coerce").fillna(0.0)
    real_amount = pd.to_numeric(out.get("Monto real cobrado"), errors="coerce").fillna(0.0)
    estado = out.get("Por_cobrar", pd.Series("No", index=out.index)).map(_yes_no_norm)
    factoring = out.get("Detalle factoring", pd.Series("", index=out.index)).astype(str).str.strip()
    fecha_real = pd.to_datetime(out.get("Fecha real de cobro"), errors="coerce")
    fecha_base = pd.to_datetime(out.get("Fecha"), errors="coerce")

    movement_date = fecha_real.where(fecha_real.notna(), fecha_base)
    movement_amount = real_amount.where(real_amount > 0, 0.0)

    full_realized = estado.eq("No") & factoring.eq("")
    movement_amount = movement_amount.where(~full_realized, total)
    movement_date = movement_date.where(~full_realized, fecha_real.where(fecha_real.notna(), fecha_base))

    mov = pd.DataFrame({"Fecha": movement_date, "Monto": movement_amount})
    mov = mov.dropna(subset=["Fecha"])
    mov["Monto"] = pd.to_numeric(mov["Monto"], errors="coerce").fillna(0.0)
    return mov[mov["Monto"] > 0].reset_index(drop=True)


def _extract_expense_movements(df_gas: pd.DataFrame) -> pd.DataFrame:
    if df_gas is None or df_gas.empty or "Monto" not in df_gas or "Fecha" not in df_gas:
        return pd.DataFrame(columns=["Fecha", "Monto"])

    def _parse_events(raw):
        try:
            data = json.loads(str(raw or "[]"))
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        rows = []
        for item in data:
            if not isinstance(item, dict):
                continue
            fecha = pd.to_datetime(item.get("fecha"), errors="coerce")
            monto = float(pd.to_numeric(pd.Series([item.get("monto", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            if pd.isna(fecha) or monto <= 0:
                continue
            rows.append({"Fecha": fecha, "Monto": -monto})
        return rows

    out = df_gas.copy()
    partial_rows: list[dict] = []
    for _, row in out.iterrows():
        events = _parse_events(row.get("Detalle pagos parciales"))
        for evt in events:
            partial_rows.append(evt)
    if partial_rows:
        return pd.DataFrame(partial_rows).sort_values("Fecha").reset_index(drop=True)

    total = pd.to_numeric(out.get("Monto"), errors="coerce").fillna(0.0)
    real_amount = pd.to_numeric(out.get("Monto real pagado"), errors="coerce").fillna(0.0)
    estado = out.get("Por_pagar", pd.Series("No", index=out.index)).map(_yes_no_norm)
    fecha_real = pd.to_datetime(out.get("Fecha real de pago"), errors="coerce")
    fecha_base = pd.to_datetime(out.get("Fecha"), errors="coerce")

    movement_date = fecha_real.where(fecha_real.notna(), fecha_base)
    movement_amount = real_amount.where(real_amount > 0, 0.0)

    full_paid = estado.eq("No")
    movement_amount = movement_amount.where(~full_paid, total)
    movement_date = movement_date.where(~full_paid, fecha_real.where(fecha_real.notna(), fecha_base))

    mov = pd.DataFrame({"Fecha": movement_date, "Monto": movement_amount * -1.0})
    mov = mov.dropna(subset=["Fecha"])
    mov["Monto"] = pd.to_numeric(mov["Monto"], errors="coerce").fillna(0.0)
    return mov[mov["Monto"] != 0].reset_index(drop=True)


def preparar_cashflow(df_ing: pd.DataFrame, df_gas: pd.DataFrame, *, saldo_inicial: float = 0.0) -> pd.DataFrame:
    """
    Une ingresos (+) y gastos (-), agrupa por día y calcula saldo acumulado.

    Compatibilidad:
    - si existen `Monto real cobrado/pagado` y `Fecha real de cobro/pago`, usa esos importes
      para soportar cobros/pagos parciales
    - si el registro está totalmente realizado (`Por_cobrar/Por_pagar = No`), usa el monto total
    - si no existen columnas nuevas, cae al comportamiento histórico
    """

    a = _extract_income_movements(df_ing)
    b = _extract_expense_movements(df_gas)

    mov = pd.concat([a, b], ignore_index=True)
    if mov.empty:
        return pd.DataFrame(columns=["Fecha", "Flujo", "Saldo"])

    mov["Fecha"] = pd.to_datetime(mov["Fecha"], errors="coerce")
    mov = mov.dropna(subset=["Fecha"])
    if mov.empty:
        return pd.DataFrame(columns=["Fecha", "Flujo", "Saldo"])

    diario = mov.groupby(mov["Fecha"].dt.date)["Monto"].sum().sort_index()
    saldo = diario.cumsum() + float(saldo_inicial)

    return pd.DataFrame(
        {
            "Fecha": pd.to_datetime(diario.index),
            "Flujo": diario.values,
            "Saldo": saldo.values,
        }
    )
