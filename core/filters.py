# core/filters.py

# Importamos pandas para manipular DataFrames
import pandas as pd  # Biblioteca para manipulación de datos (tablas)

def filtrar_por_fecha(df: pd.DataFrame, desde, hasta) -> pd.DataFrame:
    """
    Filtra un DataFrame por rango de fechas incluyente.
    - df:     DataFrame con una columna 'Fecha'
    - desde:  fecha inicial (incluida)
    - hasta:  fecha final (incluida)
    Devuelve: copia del DataFrame solo con filas dentro del rango.
    """
    # Si el DF está vacío o no tiene la columna 'Fecha', no podemos filtrar por fechas
    if "Fecha" not in df.columns or df.empty:
        return df.copy()  # Devolvemos una copia tal cual para no modificar el original

    # Creamos una máscara booleana (True en todas las filas al inicio)
    m = pd.Series(True, index=df.index)  # Esto nos permitirá ir filtrando paso a paso

    # Si 'desde' tiene valor (no es NaT/None), filtramos fechas >= desde
    if pd.notna(desde):
        m &= df["Fecha"] >= pd.to_datetime(desde)  # Comparamos cada fecha con 'desde'

    # Si 'hasta' tiene valor, filtramos fechas <= hasta
    if pd.notna(hasta):
        m &= df["Fecha"] <= pd.to_datetime(hasta)  # Comparamos cada fecha con 'hasta'

    # Aplicamos la máscara y devolvemos una COPIA (para no tocar el original)
    return df.loc[m].copy()
