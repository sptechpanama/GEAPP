# commissions.py
# ============================================================
# Lógica mínima para comisión única de cierre por proyecto.
# Reglas:
#  - Disparo automático cuando se marca un Ingreso con:
#       Proyecto != "", Cobrado == "Sí", Cierre_Proyecto == "Sí"
#    y el proyecto YA tiene costos listos (o, en su defecto, tiene gastos asociados).
#  - Comisión = margin_pct * (Ingresos cobrados del proyecto - Gastos reales del proyecto)
#  - Se registra como un GASTO "Comisión Comercial", SIN proyecto (para no afectar el margen ya calculado).
#  - Si ya existe una línea con ese ProyectoID y monto (±1 USD), NO se duplica.
# ============================================================

from __future__ import annotations
import pandas as pd
from typing import Tuple, Optional

# ---------- Constantes por defecto (ajústalas a tu entorno) ----------
WS_INGRESOS   = "Ingresos"     # Nombre de pestaña de Ingresos en tu Sheet
WS_GASTOS     = "Gastos"       # Nombre de pestaña de Gastos en tu Sheet
WS_PROYECTOS  = "Proyectos"    # Nombre de pestaña de Proyectos (maestro simple)
DEFAULT_MARGIN_PCT = 0.10      # 10% si no se define por proyecto

# ---------- Columnas esperadas (para robustez) ----------
COL_PROY   = "Proyecto"
COL_COB    = "Cobrado"
COL_CIERRE = "Cierre_Proyecto"
COL_CAT    = "Categoria"
COL_MONTO  = "Monto"
COL_FECHA  = "Fecha"
COL_DESC   = "Descripcion"
COL_TIPO   = "Tipo"

# ---------- Funciones utilitarias "blandas" (no rompen si faltan columnas) ----------
def ensure_min_columns(df: pd.DataFrame, columns_with_defaults: dict) -> pd.DataFrame:
    """
    Asegura que el DataFrame tenga las columnas clave. Si no existen, las crea con default.
    Esto se hace en memoria (no altera tu Sheet todavía).
    """
    df = df.copy()
    for col, default in columns_with_defaults.items():
        if col not in df.columns:
            df[col] = default
    return df

def coerce_yesno(series: pd.Series) -> pd.Series:
    """
    Normaliza 'Sí/No' de manera tolerante: admite "Sí", "Si", "si", "SI", True -> "Sí"; todo lo demás -> "No".
    """
    def norm(x):
        if isinstance(x, str) and x.strip().lower() in ("si", "sí", "yes", "y", "true"):
            return "Sí"
        if x is True:
            return "Sí"
        return "No"
    return series.apply(norm)

# ---------- Carga parámetros de comisión por proyecto ----------
def get_margin_pct_for_project(df_proyectos: pd.DataFrame, proyecto_id: str) -> float:
    """
    Lee margin_pct desde la hoja Proyectos para el ProyectoID. Si no lo encuentra, usa DEFAULT_MARGIN_PCT.
    Espera columnas: ProyectoID, margin_pct (0.10 = 10%)
    """
    if df_proyectos is None or df_proyectos.empty:
        return DEFAULT_MARGIN_PCT
    if "ProyectoID" not in df_proyectos.columns:
        return DEFAULT_MARGIN_PCT
    row = df_proyectos.loc[df_proyectos["ProyectoID"] == proyecto_id]
    if row.empty:
        return DEFAULT_MARGIN_PCT
    if "margin_pct" not in df_proyectos.columns:
        return DEFAULT_MARGIN_PCT
    try:
        val = float(row["margin_pct"].iloc[0])
        if val <= 0 or val > 1:
            # Permite que lo ingreses como 10 o 0.10; normaliza si parece 10
            if val > 1 and val <= 100:
                val = val / 100.0
            else:
                val = DEFAULT_MARGIN_PCT
        return val
    except Exception:
        return DEFAULT_MARGIN_PCT

def costos_listos(df_proyectos: pd.DataFrame, proyecto_id: str) -> bool:
    """
    Devuelve True si en Proyectos.Costos_listos = 'Sí' para ese ProyectoID; tolerante con mayúsculas/minúsculas.
    Si no existe la columna o el registro, devuelve False (o podrás dejar pasar porque tienes gastos reales).
    """
    if df_proyectos is None or df_proyectos.empty:
        return False
    if "ProyectoID" not in df_proyectos.columns or "Costos_listos" not in df_proyectos.columns:
        return False
    row = df_proyectos.loc[df_proyectos["ProyectoID"] == proyecto_id]
    if row.empty:
        return False
    val = str(row["Costos_listos"].iloc[0]).strip().lower()
    return val in ("si", "sí", "yes", "y", "true")

# ---------- Cálculos base por proyecto ----------
def calc_ingresos_cobrados(df_ing: pd.DataFrame, proyecto_id: str) -> float:
    """
    Suma Monto de Ingresos con Proyecto=proyecto_id y Cobrado='Sí'.
    Funciona aunque falten columnas (devuelve 0).
    """
    if df_ing is None or df_ing.empty:
        return 0.0
    if (COL_PROY not in df_ing.columns) or (COL_MONTO not in df_ing.columns):
        return 0.0
    df = df_ing.copy()
    df[COL_COB] = coerce_yesno(df.get(COL_COB, pd.Series(["No"] * len(df))))
    mask = (df[COL_PROY] == proyecto_id) & (df[COL_COB] == "Sí")
    return float(df.loc[mask, COL_MONTO].fillna(0).sum())

def calc_gastos_reales(df_gas: pd.DataFrame, proyecto_id: str) -> float:
    """
    Suma Monto de Gastos con Proyecto=proyecto_id.
    """
    if df_gas is None or df_gas.empty:
        return 0.0
    if (COL_PROY not in df_gas.columns) or (COL_MONTO not in df_gas.columns):
        return 0.0
    mask = (df_gas[COL_PROY] == proyecto_id)
    return float(df_gas.loc[mask, COL_MONTO].fillna(0).sum())

def calc_margen_real(df_ing: pd.DataFrame, df_gas: pd.DataFrame, proyecto_id: str) -> float:
    """
    Margen real = Ingresos cobrados - Gastos reales.
    Nunca negativo (máx con 0).
    """
    ingresos = calc_ingresos_cobrados(df_ing, proyecto_id)
    gastos   = calc_gastos_reales(df_gas, proyecto_id)
    margen   = max(0.0, ingresos - gastos)
    return margen

# ---------- Anti-duplicado ----------
def already_posted_commission(df_gas: pd.DataFrame, proyecto_id: str, monto: float) -> bool:
    """
    Revisa si ya existe un gasto "Comisión Comercial" asociado a ese ProyectoID en la Descripcion,
    con un monto igual (±1 USD). Se busca sin proyecto (o con Proyecto vacío/NaN).
    """
    if df_gas is None or df_gas.empty:
        return False
    if (COL_MONTO not in df_gas.columns) or (COL_DESC not in df_gas.columns) or (COL_CAT not in df_gas.columns):
        return False
    subset = df_gas[df_gas[COL_CAT].fillna("").str.strip().str.lower() == "comisión comercial"]
    if subset.empty:
        return False
    texto = f"Comisión  cierre – {proyecto_id}".lower()
    # Permitimos dos variantes de descripción para robustez
    posibles = {texto, f"comisión 10% cierre – {proyecto_id}".lower(), f"comision 10% cierre – {proyecto_id}".lower()}
    subset = subset[subset[COL_DESC].fillna("").str.lower().isin(posibles)]
    if subset.empty:
        return False
    # Diferencia máxima $1 para evitar problemas de redondeo
    dif_ok = subset[COL_MONTO].sub(monto).abs() <= 1.0
    return bool(dif_ok.any())

# ---------- Construcción de fila de gasto para la comisión ----------
def build_commission_expense_row(fecha, monto: float, proyecto_id: str) -> dict:
    """
    Arma la fila de Gasto para insertar en la hoja 'Gastos'.
    Recomendación: SIN proyecto (o 'GLOBAL') para no afectar el margen ya calculado.
    """
    return {
        COL_FECHA: fecha,
        COL_TIPO: "Variable",                 # o SG&A según tu taxonomía
        COL_CAT: "Comisión Comercial",
        COL_MONTO: round(float(monto), 2),
        COL_PROY: "",                         # vacío (no imputar al proyecto)
        COL_DESC: f"Comisión 10% cierre – {proyecto_id}",
    }

# ---------- Orquestador para disparo automático ----------
def maybe_generate_closing_commission(
    df_ingresos: pd.DataFrame,
    df_gastos: pd.DataFrame,
    df_proyectos: Optional[pd.DataFrame],
    proyecto_id: str,
    fecha_referencia,           # normalmente la fecha del ingreso de cierre, o 'pd.Timestamp.today()'
    write_append_fn,            # función para APPEND una fila a la hoja 'Gastos'
) -> Tuple[bool, Optional[dict]]:
    """
    Si se cumplen condiciones, calcula comisión y APPEND a 'Gastos'.
    - df_ingresos: DataFrame de Ingresos (con Proyecto, Cobrado, Cierre_Proyecto, Monto)
    - df_gastos:   DataFrame de Gastos   (con Proyecto, Monto)
    - df_proyectos: hoja Proyectos (para leer margin_pct y costos_listos). Puede ser None si no usas maestro.
    - proyecto_id: string exacto del proyecto (igual a 'Proyecto' en Ingresos/Gastos)
    - fecha_referencia: fecha para registrar el gasto de comisión
    - write_append_fn: callable(dict_row) -> None  que inserta la fila a 'Gastos' en Google Sheets

    Devuelve (created: bool, row_dict: Optional[dict])
    """
    # Asegurar columnas clave en memoria (tolerante)
    df_ingresos = ensure_min_columns(df_ingresos, {COL_PROY: "", COL_COB: "No", COL_CIERRE: "No", COL_MONTO: 0})
    df_gastos   = ensure_min_columns(df_gastos,   {COL_PROY: "", COL_MONTO: 0, COL_CAT: "", COL_DESC: ""})

    # Si usas maestro de proyectos, verifica "costos listos"
    listo = costos_listos(df_proyectos, proyecto_id) if df_proyectos is not None else False

    # Si no hay 'costos listos', permite avanzar SOLO si existen gastos para ese proyecto (costeo real).
    if not listo:
        if calc_gastos_reales(df_gastos, proyecto_id) <= 0:
            # No hay costos listos ni gastos reales → NO se puede calcular aún
            return (False, None)

    # Obtén margen %
    margin_pct = get_margin_pct_for_project(df_proyectos, proyecto_id)
    # Calcula margen real
    margen = calc_margen_real(df_ingresos, df_gastos, proyecto_id)
    if margen <= 0:
        # Sin margen no hay comisión (queda 0)
        return (False, None)

    # Comisión
    comision = margin_pct * margen

    # Evita duplicar si ya existe un asiento idéntico
    if already_posted_commission(df_gastos, proyecto_id, comision):
        return (False, None)

    # Construye la fila de gasto
    row = build_commission_expense_row(fecha=fecha_referencia, monto=comision, proyecto_id=proyecto_id)

    # Inserta en Google Sheets (APPEND)
    write_append_fn(row)

    return (True, row)
