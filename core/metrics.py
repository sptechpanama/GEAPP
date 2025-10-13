# core/metrics.py

import pandas as pd  # Cálculos tabulares

def monthly_pnl(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> pd.DataFrame:
    """
    Construye un P&L mensual: Ingresos, Gastos, Utilidad por mes.
    Devuelve columnas: ['Mes','Ingresos','Gastos','Utilidad']
    """
    # ---- agregamos meses (periodo M) y sumamos ----
    a = df_ing.copy()                                        # copia defensiva
    a["Mes"] = pd.to_datetime(a["Fecha"]).dt.to_period("M")  # periodo mensual en ingresos
    ing_m = a.groupby("Mes")["Monto"].sum().rename("Ingresos")

    b = df_gas.copy()
    b["Mes"] = pd.to_datetime(b["Fecha"]).dt.to_period("M")  # periodo mensual en gastos
    gas_m = b.groupby("Mes")["Monto"].sum().rename("Gastos")

    # ---- combinamos y calculamos utilidad ----
    pnl = pd.concat([ ing_m, gas_m ], axis=1).fillna(0.0)    # unimos por Mes
    pnl["Utilidad"] = pnl["Ingresos"] - pnl["Gastos"]        # utilidad mensual

    # ---- devolvemos como DataFrame ordenado por Mes ----
    out = pnl.reset_index()
    out["Mes"] = out["Mes"].dt.to_timestamp()                # convertimos Period -> Timestamp
    return out

def kpis_finanzas(df_ing: pd.DataFrame, df_gas: pd.DataFrame) -> dict:
    """
    KPIs estilo QuickBooks:
      - total_ingresos, total_gastos, utilidad, margen_pct
      - burn_rate_mensual (promedio de gastos mensuales)
      - neto_mensual_promedio (promedio de utilidad mensual)
    """
    # ---- totales simples ----
    total_ing = float(df_ing["Monto"].sum()) if "Monto" in df_ing else 0.0
    total_gas = float(df_gas["Monto"].sum()) if "Monto" in df_gas else 0.0
    utilidad  = total_ing - total_gas
    margen_pct = (utilidad / total_ing * 100.0) if total_ing > 0 else 0.0

    # ---- P&L mensual para promedios ----
    pnl_m = monthly_pnl(df_ing, df_gas)                      # usamos la función anterior
    neto_mensual_promedio = float(pnl_m["Utilidad"].mean()) if not pnl_m.empty else 0.0
    burn_rate_mensual = float(pnl_m["Gastos"].mean()) if not pnl_m.empty else 0.0

    return {
        "total_ingresos": total_ing,
        "total_gastos": total_gas,
        "utilidad": utilidad,
        "margen_pct": margen_pct,
        "neto_mensual_promedio": neto_mensual_promedio,
        "burn_rate_mensual": burn_rate_mensual,
    }

def top_gastos_por_categoria(df_gas: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Top-N categorías de gasto por monto absoluto (para gráfico de barras).
    Devuelve DataFrame con columnas ['Categoria','Gasto'].
    """
    if df_gas.empty or "Categoria" not in df_gas or "Monto" not in df_gas:
        return pd.DataFrame(columns=["Categoria","Gasto"])

    # sumamos por categoría y ordenamos (usamos valor absoluto por si hay signos invertidos)
    s = df_gas.groupby("Categoria")["Monto"].sum().abs().sort_values(ascending=False)
    s = s.head(top_n)
    return s.reset_index().rename(columns={"Monto":"Gasto"})
