# ============================
# backup_utils.py
# Respaldo automático en Google Sheets
# ============================

import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

def _now_str():
    return datetime.now().strftime("%Y-%m-%d_%H-%M")

def generar_nombre_respaldo(nombre_hoja: str) -> str:
    # “Carpeta” lógica usando prefijo en el nombre de la worksheet
    return f"Respaldo/{nombre_hoja}_backup_{_now_str()}"

def listar_hojas_respaldo(sheet, prefijo: str):
    # Todas las worksheets cuyo título empiece con Respaldo/prefijo_backup_
    return [
        h for h in sheet.worksheets()
        if h.title.startswith(f"Respaldo/{prefijo}_backup_")
    ]

def borrar_hojas_antiguas(sheet, prefijo: str, max_dias: int = 30, max_hojas: int = 10):
    """
    - Borra respaldos con fecha > max_dias.
    - Mantiene como máximo max_hojas (borra las más antiguas).
    """
    hojas = listar_hojas_respaldo(sheet, prefijo)
    # Ordenamos por nombre descendente (por llevar fecha en el nombre funciona razonable)
    hojas_ordenadas = sorted(hojas, key=lambda h: h.title, reverse=True)

    # Regex para extraer fecha YYYY-MM-DD del título
    pattern = re.compile(rf"{re.escape('Respaldo/' + prefijo)}_backup_(\d{{4}}-\d{{2}}-\d{{2}})_")
    hoy = datetime.now()
    preserva = []

    # 1) Por fecha
    for ws in hojas_ordenadas:
        m = pattern.search(ws.title)
        if not m:
            # Si no matchea formato, lo tratamos como antiguo y se borra
            sheet.del_worksheet(ws)
            continue
        fecha = datetime.strptime(m.group(1), "%Y-%m-%d")
        if (hoy - fecha).days > max_dias:
            sheet.del_worksheet(ws)
        else:
            preserva.append(ws)

    # 2) Por cantidad
    if len(preserva) > max_hojas:
        # Borra desde el final (los más viejos de los preservados)
        for ws in reversed(preserva[max_hojas:]):
            sheet.del_worksheet(ws)

def firma_tabla(df) -> str:
    """Firma textual simple para detectar cambios de contenido."""
    if df is None or df.empty:
        return ""
    snap = df.copy()
    # Homogeneiza fechas si existe 'Fecha'
    if "Fecha" in snap.columns:
        snap["Fecha"] = snap["Fecha"].astype(str)
    return snap.to_csv(index=False)

def requiere_backup_por_tiempo(ultima_fecha: Optional[datetime], dias: int = 3) -> bool:
    if not ultima_fecha:
        return True
    return (datetime.now() - ultima_fecha) >= timedelta(days=dias)

def hacer_respaldo(sheet, hoja_nombre: str):
    """
    Duplica la worksheet `hoja_nombre` con prefijo 'Respaldo/...'.
    Retorna timestamp del backup realizado.
    """
    original = sheet.worksheet(hoja_nombre)
    sheet.duplicate_sheet(original.id, new_sheet_name=generar_nombre_respaldo(hoja_nombre))
    borrar_hojas_antiguas(sheet, hoja_nombre)
    return datetime.now()

def hacer_respaldo_si_corresponde(
    sheet,
    hoja_nombre: str,
    df_actual,
    ultima_firma_guardada: str,
    ultima_fecha_backup: Optional[datetime],
    forzar_cada_3_dias: bool = True
) -> Tuple[Optional[datetime], str]:
    """
    Dispara backup si:
      - hubo cambios (firma distinta), o
      - han pasado >=3 días desde el último backup (si forzar_cada_3_dias=True).

    Retorna (timestamp_backup_o_None, firma_actual).
    """
    actual = firma_tabla(df_actual)
    hay_cambios = (actual != (ultima_firma_guardada or ""))

    if hay_cambios or (forzar_cada_3_dias and requiere_backup_por_tiempo(ultima_fecha_backup, dias=3)):
        ts = hacer_respaldo(sheet, hoja_nombre)
        return ts, actual

    return None, actual
