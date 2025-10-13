# core/sync.py

# Importamos pandas para trabajar con DataFrames
import pandas as pd  # Uniones, concat, etc.

# Reutilizamos normalizar_esquema para asegurar formato
from core.normalize import normalizar_esquema  # Garantiza columnas y tipos
from core.normalize import normalizar_tasks
from typing import Optional, Callable, Any


def _as_df(x: Any) -> pd.DataFrame:
    if x is None:
        return pd.DataFrame()
    if isinstance(x, pd.DataFrame):
        return x.copy()
    try:
        return pd.DataFrame(x).copy()
    except Exception:
        return pd.DataFrame()


def _ensure_id(df: pd.DataFrame, id_column: Optional[str]) -> pd.DataFrame:
    out = _as_df(df)
    if id_column and id_column not in out.columns:
        out[id_column] = ""
    return out


def sync_cambios(
    edited_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    *,
    base_df_key: str,
    worksheet_name: str,
    session_state,
    write_worksheet,
    client,
    sheet_id: str,
    ensure_columns_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    id_column: Optional[str] = "RowID",
):
    """
    Toma la tabla editada (edited_df), la aplica sobre el subconjunto filtrado (filtered_df),
    actualiza la base completa en session_state[base_df_key] y persiste en Google Sheets.
    """
    base = _as_df(session_state.get(base_df_key, pd.DataFrame()))
    before = base.copy(deep=True)

    edited_df = _as_df(edited_df)
    filtered_df = _as_df(filtered_df)

    base = _ensure_id(base, id_column)
    edited_df = _ensure_id(edited_df, id_column)
    filtered_df = _ensure_id(filtered_df, id_column)

    if id_column and id_column in base.columns:
        # ----- Sincronía por ID -----
        base_ids = base[id_column].astype(str).tolist()

        # 1) Actualizar existentes
        for _, erow in edited_df.iterrows():
            rid = str(erow.get(id_column, "")).strip()
            if not rid:
                continue
            if rid in base_ids:
                bidx = base.index[base[id_column].astype(str) == rid][0]
                # copiar columnas presentes en edited_df
                for c in edited_df.columns:
                    base.at[bidx, c] = erow.get(c)

        # 2) Altas: filas sin ID (no recomendable, pero soportado)
        new_rows = edited_df[edited_df[id_column].astype(str).str.strip() == ""]
        if not new_rows.empty:
            base = pd.concat([base, new_rows], ignore_index=True)
    else:
        # ----- Sincronía posicional (fallback) -----
        common = [c for c in base.columns if c in filtered_df.columns and c in edited_df.columns]
        if not edited_df.empty and not filtered_df.empty:
            # asumimos que el filtro no re-ordena; reemplazo por longitud mínima
            n = min(len(base), len(edited_df))
            for i in range(n):
                for c in common:
                    base.at[i, c] = edited_df.iloc[i][c]

    # Normalizar/asegurar columnas antes de escribir
    if ensure_columns_fn:
        base = ensure_columns_fn(base)

    # Persistir si hubo cambios
    if not base.equals(before):
        session_state[base_df_key] = base
        write_worksheet(client, sheet_id, worksheet_name, base)


def sync_tasks(
    edited_view: pd.DataFrame,
    prev_view: pd.DataFrame,
    session_state,
    base_df_key: str,
    write_worksheet,
    client,
    sheet_id: str,
    worksheet_name: str,
):
    """
    Proyecta cambios del editor (vista filtrada) al DF base en sesión (por ID) y
    persiste el DF completo en Google Sheets.
    - edited_view : DataFrame que viene de st.data_editor (posibles filas nuevas)
    - prev_view   : DataFrame como estaba antes de editar (para detectar nuevas filas)
    - base_df_key : e.g., 'df_tasks'
    """
    # Normalizamos ambas vistas y el DF base para esquema/tipos consistentes
    new_v = normalizar_tasks(edited_view)     # Después de editar
    old_v = normalizar_tasks(prev_view)       # Antes de editar

    # Si no hay cambios efectivos, no hacemos nada
    if new_v.equals(old_v):
        return

    base = normalizar_tasks(session_state[base_df_key])

    # -------------------------------
    # 1) ACTUALIZAR filas existentes por ID
    # -------------------------------
    # Unimos por ID para alinear índices de base con new_v
    base_idx_by_id = {rid: i for i, rid in enumerate(base["ID"].tolist())}

    for _, row in new_v.iterrows():
        rid = row["ID"]
        if rid in base_idx_by_id:
            i = base_idx_by_id[rid]
            # Actualizamos columnas principales
            base.loc[i, "Tarea"] = row["Tarea"]
            base.loc[i, "Estado"] = row["Estado"]
            base.loc[i, "Fecha de ingreso"] = row["Fecha de ingreso"]
            base.loc[i, "Fecha de completado"] = row["Fecha de completado"]
            # Tiempo sin completar se recalculará globalmente más abajo

    # -------------------------------
    # 2) INSERTAR filas nuevas (ID que están en new_v pero no en old_v/base)
    # -------------------------------
    new_ids = set(new_v["ID"]) - set(old_v["ID"])
    if new_ids:
        filas_nuevas = new_v[new_v["ID"].isin(new_ids)].copy()
        # Anexamos al final del base
        base = pd.concat([base, filas_nuevas], ignore_index=True)

    # -------------------------------
    # 3) Recalcular tiempos y persistir
    # -------------------------------
    base = normalizar_tasks(base)  # Recalcula "Tiempo sin completar (días)"
    session_state[base_df_key] = base  # Guarda base actualizado en memoria

    # Escribimos TODO el DF a Sheets (sobrescribe la hoja)
    write_worksheet(client, sheet_id, worksheet_name, base)
