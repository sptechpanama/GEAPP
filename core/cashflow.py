# core/cashflow.py

# Importamos pandas para manipular DataFrames
import pandas as pd  # Análisis y series de tiempo

def preparar_cashflow(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> pd.DataFrame:
    """
    Une ingresos (+) y gastos (-), agrupa por día y calcula saldo acumulado.
    Devuelve DataFrame con columnas ['Fecha', 'Flujo', 'Saldo'].
    """
    # Preparamos ingresos: nos quedamos con Fecha y Monto
    if not df_ing.empty and "Monto" in df_ing and "Fecha" in df_ing:
        a = df_ing[["Fecha", "Monto"]].dropna().copy()  # Copia defensiva
        a["Monto"] = pd.to_numeric(a["Monto"], errors="coerce").fillna(0)  # Forzamos numérico
    else:
        a = pd.DataFrame(columns=["Fecha", "Monto"])  # DF vacío con columnas correctas

    # Preparamos gastos: iguales columnas, pero el signo será negativo
    if not df_gas.empty and "Monto" in df_gas and "Fecha" in df_gas:
        b = df_gas[["Fecha", "Monto"]].dropna().copy()
        b["Monto"] = pd.to_numeric(b["Monto"], errors="coerce").fillna(0) * -1  # Convertimos a número y negamos
    else:
        b = pd.DataFrame(columns=["Fecha", "Monto"])  # DF vacío con columnas correctas

    # Unimos movimientos (ingresos y gastos) en un solo DF
    mov = pd.concat([a, b], ignore_index=True)  # Concatena filas, reinicia índices

    # Si no hay movimientos, devolvemos estructura vacía
    if mov.empty:
        return pd.DataFrame(columns=["Fecha", "Flujo", "Saldo"])

    # Normalizamos las fechas a tipo datetime y quitamos filas sin fecha válida
    mov["Fecha"] = pd.to_datetime(mov["Fecha"], errors="coerce")  # Convertimos a datetime
    mov = mov.dropna(subset=["Fecha"])                             # Filas sin fecha válida se eliminan

    # Agrupamos por día (solo la parte de fecha, sin hora) y sumamos montos del día
    diario = mov.groupby(mov["Fecha"].dt.date)["Monto"].sum().sort_index()  # Serie por día

    # Calculamos el saldo acumulado sobre el flujo diario
    saldo = diario.cumsum()  # Acumulado del flujo en el tiempo

    # Construimos el DataFrame de salida con Fecha, Flujo del día y Saldo acumulado
    return pd.DataFrame({
        "Fecha": pd.to_datetime(diario.index),  # Volvemos a datetime la fecha (venía como date)
        "Flujo": diario.values,                 # Valores del flujo neto por día
        "Saldo": saldo.values                   # Valores del acumulado
    })
