# core/normalize.py

# Importamos pandas para manipular DataFrames
import pandas as pd  # Tablas y transformación de datos
import uuid

# Definimos el orden/las columnas que esperamos siempre
EXPECTED = ["Fecha", "Concepto", "Monto", "Categoria"]  # Esquema canon para ingresos/gastos

def normalizar_esquema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Asegura columnas base
    for col in EXPECTED:
        if col not in out.columns:
            out[col] = 0 if col == "Monto" else ""

    # === NUEVO: asegurar 'Escenario' ===
    if "Escenario" not in out.columns:
        out["Escenario"] = "Real"          # valor por defecto si la hoja no la trae
    out["Escenario"] = out["Escenario"].astype(str)

    # Tipos
    out["Fecha"] = pd.to_datetime(out["Fecha"], errors="coerce")
    out["Monto"] = pd.to_numeric(out["Monto"], errors="coerce").fillna(0)
    out["Concepto"] = out["Concepto"].astype(str)
    out["Categoria"] = out["Categoria"].astype(str)

    # === NUEVO: devolver también 'Escenario' ===
    return out[EXPECTED + ["Escenario"]]


def normalizar_tasks(df: pd.DataFrame, hoy: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Asegura el esquema y tipos del cuadro de Tasks (Pendientes) y calcula "Tiempo sin completar (días)".

    Columnas garantizadas (en este orden de salida):
      - 'ID'                          : str (uuid4 si falta)
      - 'Tarea'                       : str
      - 'Estado'                      : str  (Pendiente / Completada / Descartar)
      - 'Fecha de ingreso'            : datetime64[ns] (YYYY-MM-DD)
      - 'Fecha de completado'         : datetime64[ns] (YYYY-MM-DD) o NaT
      - 'Tiempo sin completar (días)' : int (calculado)
    """
    import uuid
    out = (df.copy() if df is not None else pd.DataFrame())

    expected = [
        "ID",
        "Tarea",
        "Estado",
        "Fecha de ingreso",
        "Fecha de completado",
        "Tiempo sin completar (días)",
    ]
    for col in expected:
        if col not in out.columns:
            out[col] = 0 if col == "Tiempo sin completar (días)" else ""

    # Tipos base
    out["Tarea"] = out["Tarea"].astype(str)
    out["Estado"] = out["Estado"].astype(str)
    out["Fecha de ingreso"] = pd.to_datetime(out["Fecha de ingreso"], errors="coerce")
    out["Fecha de completado"] = pd.to_datetime(out["Fecha de completado"], errors="coerce")

    # ID estable
    def _ensure_id(x: str) -> str:
        x = str(x).strip()
        return x if x not in ("", "nan", "NaN", "None") else str(uuid.uuid4())
    out["ID"] = out["ID"].apply(_ensure_id)

    # Estado por defecto
    out.loc[out["Estado"].isin(["", "nan", "NaN", "None"]), "Estado"] = "Pendiente"

    # 'Hoy' normalizado
    if hoy is None:
        hoy = pd.Timestamp.today().normalize()

    # === Cálculo robusto SIN to_numeric ni fillna ===
    dias_int: list[int] = []
    for _, row in out.iterrows():
        fi = row["Fecha de ingreso"]
        fc = row["Fecha de completado"]
        est = (row["Estado"] or "").strip()

        if pd.isna(fi):
            dias_int.append(0)  # <-- aquí estaba el bug: antes decía 'dias.append(0)'
            continue

        # Normalizamos fechas a Timestamp/solo fecha
        fi = pd.Timestamp(fi).normalize()
        if not pd.isna(fc):
            fc = pd.Timestamp(fc).normalize()

        if est == "Completada" and not pd.isna(fc):
            delta = (fc - fi).days
            dias_int.append(max(int(delta), 0))
        else:
            delta = (hoy - fi).days
            dias_int.append(max(int(delta), 0))

    out["Tiempo sin completar (días)"] = dias_int

    return out[expected]
