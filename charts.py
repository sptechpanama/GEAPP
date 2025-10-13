# =========================
# charts.py
# Agregaciones y gráficos (Altair)
# =========================

import pandas as pd  # DataFrames
import altair as alt # Gráficas declarativas


def chart_bars_saldo_mensual(cash_df: pd.DataFrame,
                             color_pos: str = "#10b981",
                             color_neg: str = "#ef4444") -> alt.Chart:
    """
    Barra mensual del SALDO FINAL (cierre de mes) con color:
      - verde si saldo >= 0
      - rojo si saldo < 0
    Requiere columnas: ['Fecha','Saldo'] en cash_df.
    """
    if cash_df.empty or not {"Fecha","Saldo"}.issubset(cash_df.columns):
        return alt.Chart(pd.DataFrame({"Mes": [], "Saldo": []})).mark_bar()

    cash_m = (
        cash_df[["Fecha", "Saldo"]].copy()
        .assign(Mes=lambda d: pd.to_datetime(d["Fecha"]).dt.to_period("M"))
        .sort_values(["Mes", "Fecha"])
        .groupby("Mes", as_index=False)
        .tail(1)  # saldo de cierre de cada mes
    )
    cash_m["Mes"] = cash_m["Mes"].dt.to_timestamp()

    return (
        alt.Chart(cash_m)
        .mark_bar()
        .encode(
            x=alt.X("Mes:T", title="Mes"),
            y=alt.Y("Saldo:Q", title="Saldo final del mes"),
            color=alt.condition(
                "datum.Saldo >= 0",
                alt.value(color_pos),
                alt.value(color_neg),
            ),
            tooltip=[
                alt.Tooltip("Mes:T", title="Mes"),
                alt.Tooltip("Saldo:Q", title="Saldo", format=",.2f"),
            ],
        )
        .properties(height=280)
    )

def chart_line_ing_gas_util_mensual(pnl_df: pd.DataFrame) -> alt.Chart:
    """
    Línea de Ingresos, Gastos y Utilidad por mes (estilo P&L).
    Espera columnas: ['Mes','Ingresos','Gastos','Utilidad'].
    - Si el DF está vacío o faltan columnas, retorna un chart vacío seguro.
    """
    # Validación defensiva: evita crashear si llegan columnas incompletas
    needed = {"Mes","Ingresos","Gastos","Utilidad"}           # columnas requeridas
    if pnl_df is None or pnl_df.empty or not needed.issubset(pnl_df.columns):
        # Chart vacío “seguro”
        return alt.Chart(pd.DataFrame({"Mes": [], "Serie": [], "Valor": []})).mark_line()

    # Pasamos a formato largo para graficar 3 series con color distinto
    df_long = pnl_df.melt(
        id_vars=["Mes"],                                       # deja 'Mes' como columna identificadora
        value_vars=["Ingresos", "Gastos", "Utilidad"],         # columnas que queremos apilar
        var_name="Serie",                                      # nombre de la nueva columna de series
        value_name="Valor"                                     # nombre para los valores
    )

    # Gráfico de línea con puntos + tooltips
    chart = (
        alt.Chart(df_long)
        .mark_line(point=True)                                 # línea + punto en cada dato
        .encode(
            x=alt.X("Mes:T", title="Mes"),                     # eje X temporal (T)
            y=alt.Y("Valor:Q", title="Monto"),                 # eje Y cuantitativo (Q)
            color=alt.Color("Serie:N", title="Serie"),         # color por serie (N = nominal)
            tooltip=[
                alt.Tooltip("Mes:T", title="Mes"),
                alt.Tooltip("Serie:N", title="Serie"),
                alt.Tooltip("Valor:Q", title="Monto", format=",.2f"),
            ],
        )
        .properties(height=280)                                # alto razonable
    )
    return chart


def chart_bar_top_gastos(df_top: pd.DataFrame) -> alt.Chart:
    """
    Barras horizontales del TOP-N de categorías de gasto.
    Espera columnas: ['Categoria','Gasto'].
    - Si el DF está vacío o faltan columnas, retorna un chart vacío seguro.
    """
    # Validación defensiva
    needed = {"Categoria","Gasto"}                             # columnas requeridas
    if df_top is None or df_top.empty or not needed.issubset(df_top.columns):
        # Chart vacío “seguro”
        return alt.Chart(pd.DataFrame({"Categoria": [], "Gasto": []})).mark_bar()

    # Barras horizontales ordenadas por valor (desc)
    chart = (
        alt.Chart(df_top)
        .mark_bar()
        .encode(
            x=alt.X("Gasto:Q", title="Gasto total"),           # cuantitativo en X
            y=alt.Y("Categoria:N", sort="-x", title="Categoría"),  # nominal en Y, ordenado por x desc
            tooltip=[
                alt.Tooltip("Categoria:N", title="Categoría"),
                alt.Tooltip("Gasto:Q", title="Gasto", format=",.2f"),
            ],
        )
        .properties(height=280)
    )
    return chart
