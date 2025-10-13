# ============================================
# pages/7_✅_Tasks.py
# Tasks con:
# - Form “Nueva tarea” (ID/Estado/Fecha listos)
# - Lectura desde Sheets saneando filas nuevas (respeta Estado manual en Sheets)
# - Editor para ver/editar/descartar (Descartar = borra en Sheets)
# - Autoguardado robusto (reescribe la pestaña completa con firma)
# - Colores por estado y orden por urgencia
# - (NUEVO) Resumen global discreto + filtro (Todos / Pendientes / Completadas)
# ============================================

# ── IMPORTS ──────────────────────────────────────────────────────────────────────
import streamlit as st                      # Librería UI
import pandas as pd                         # DataFrames y fechas
import uuid                                 # uuid4 para crear IDs únicos
from datetime import datetime               # Para "hoy"

# I/O Google Sheets (helpers existentes en tu proyecto)
from sheets import get_client, read_worksheet, write_worksheet


# ── CONFIG / SECRETOS ────────────────────────────────────────────────────────────
st.set_page_config(page_title="✅ Tasks", page_icon="✅", layout="wide")  # Config básica
WS_TASKS = st.secrets.get("app", {}).get("WS_TASKS", "pendientes")       # Pestaña destino en Sheets
ESTADOS_VALIDOS = ["Pendiente", "Completada", "Descartar"]               # Estados permitidos

# Mapa SOLO-UI para “Estado (visual)” (esto NO se escribe a Sheets)
MAPA_ESTADO_VISUAL = {
    "Pendiente": "🟥 Pendiente",   # rojo
    "Completada": "🟩 Completada", # verde
    "Descartar": "—",              # guion
}

# Orden lógico de estados para mostrar en el grid (pendiente primero)
ESTADO_ORDEN = ["Pendiente", "Completada", "Descartar"]

# Guardas en sesión: evitan loops de guardado y escrituras repetidas
if "tasks_saving" not in st.session_state:
    st.session_state["tasks_saving"] = False
if "tasks_last_sig" not in st.session_state:
    st.session_state["tasks_last_sig"] = ""


# ── HELPERS INTERNOS ────────────────────────────────────────────────────────────
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza encabezados que vengan de Sheets a nuestros canónicos."""
    canon = {
        "id": "ID",
        "tarea": "Tarea",
        "estado": "Estado",
        "fecha de ingreso": "Fecha de ingreso",
        "fecha de completado": "Fecha de completado",
        "tiempo sin completar (días)": "Tiempo sin completar (días)",
        "tiempo sin completar (dias)": "Tiempo sin completar (días)",
        "tiempo_sin_completar": "Tiempo sin completar (días)",
    }
    mapping = {}
    for c in df.columns:
        key = str(c).strip().lower()
        mapping[c] = canon.get(key, c)
    return df.rename(columns=mapping).copy()


def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura columnas y tipos correctos para UI + Sheets."""
    out = df.copy()
    for col in ["ID", "Tarea", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]:
        if col not in out.columns:
            if col in ["Fecha de ingreso", "Fecha de completado"]:
                out[col] = pd.NaT
            elif col == "Tiempo sin completar (días)":
                out[col] = 0
            else:
                out[col] = ""
    out["Fecha de ingreso"]    = pd.to_datetime(out["Fecha de ingreso"], errors="coerce")
    out["Fecha de completado"] = pd.to_datetime(out["Fecha de completado"], errors="coerce")
    out["Tiempo sin completar (días)"] = pd.to_numeric(out["Tiempo sin completar (días)"], errors="coerce").fillna(0).astype(int)
    out["ID"]     = out["ID"].astype(str)
    out["Tarea"]  = out["Tarea"].astype(str)
    out["Estado"] = out["Estado"].astype(str)
    return out


def apply_sheet_defaults_on_new_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    SANEA filas nuevas escritas DIRECTAMENTE en Sheets, completando SOLO lo que falte.
    Respeta 'Estado' si el usuario lo escribió manualmente.
    """
    out = df.copy()
    m_nueva = out["Tarea"].astype(str).str.strip() != ""                          # Tarea con texto
    m_estado_vacio = out["Estado"].astype(str).str.strip().isin(["", "nan", "NaN", "None"])
    out.loc[m_nueva & m_estado_vacio, "Estado"] = "Pendiente"                     # Estado por defecto si faltó
    hoy = pd.Timestamp(datetime.today().date())
    m_fi_vacia = out["Fecha de ingreso"].isna()
    out.loc[m_nueva & m_fi_vacia, "Fecha de ingreso"] = hoy                       # Fecha de ingreso si faltó
    m_id_vacio = out["ID"].astype(str).str.strip().isin(["", "nan", "NaN", "None"])
    count = int((m_nueva & m_id_vacio).sum())
    if count > 0:
        out.loc[m_nueva & m_id_vacio, "ID"] = [str(uuid.uuid4()) for _ in range(count)]  # ID si faltó
    return out


def compute_days(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcula 'Tiempo sin completar (días)' en caliente."""
    out = df.copy()
    hoy = pd.Timestamp(datetime.today().date())
    out["Tiempo sin completar (días)"] = 0
    m_ing = out["Fecha de ingreso"].notna()
    m_comp = (out["Estado"] == "Completada") & out["Fecha de completado"].notna() & m_ing
    out.loc[m_comp, "Tiempo sin completar (días)"] = (
        (out.loc[m_comp, "Fecha de completado"].dt.normalize() - out.loc[m_comp, "Fecha de ingreso"].dt.normalize()).dt.days
    ).clip(lower=0).astype(int)
    m_nocomp = (~m_comp) & m_ing
    out.loc[m_nocomp, "Tiempo sin completar (días)"] = (
        (hoy - out.loc[m_nocomp, "Fecha de ingreso"].dt.normalize()).dt.days
    ).clip(lower=0).astype(int)
    return out


def simple_signature(df: pd.DataFrame) -> str:
    """Firma textual estable para decidir si escribimos a Sheets."""
    cols = ["ID", "Tarea", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]
    cols = [c for c in cols if c in df.columns]
    snap = df[cols].copy()
    for c in ["Fecha de ingreso", "Fecha de completado"]:
        if c in snap.columns:
            snap[c] = pd.to_datetime(snap[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    return snap.to_csv(index=False)


def write_all(df_final: pd.DataFrame):
    """
    Reescribe TODO el DataFrame en la pestaña de Sheets:
    - Fuerza orden de columnas
    - Actualiza firma y copia en sesión
    """
    cols_order = ["ID", "Tarea", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]
    cols_present = [c for c in cols_order if c in df_final.columns]
    df_to_write = df_final[cols_present].copy()
    write_worksheet(get_client()[0], st.secrets["app"]["SHEET_ID"], WS_TASKS, df_to_write)
    st.session_state["tasks_last_sig"] = simple_signature(df_to_write)
    st.session_state["df_tasks"] = df_to_write.copy()


# ── UI: TÍTULO + BOTÓN REFRESCAR ───────────────────────────────────────────────
st.markdown("## ✅ Tasks (Pendientes)")
left, right = st.columns([0.85, 0.15])                                           # layout para resumen/botón
##with right:
##    if st.button("↻", width='stretch', help="Volver a leer desde Google Sheets"):
##        st.session_state.pop("df_tasks", None)                                   # fuerza recarga
##        st.session_state["tasks_last_sig"] = ""                                  # resetea firma
##        st.rerun()                                                               # re-ejecuta la app


# ── LECTURA INICIAL DESDE SHEETS (con saneo de filas nuevas) ───────────────────
if "df_tasks" not in st.session_state:
    base = read_worksheet(get_client()[0], st.secrets["app"]["SHEET_ID"], WS_TASKS)  # leemos crudo
    base = standardize_columns(base)                                                 # normalizamos encabezados
    base = ensure_schema(base)                                                       # aseguramos columnas/tipos
    base = apply_sheet_defaults_on_new_rows(base)                                    # completamos SOLO lo que falte
    m_id_vacio = base["ID"].astype(str).str.strip().isin(["", "nan", "NaN", "None"]) # por seguridad
    if m_id_vacio.any():
        base.loc[m_id_vacio, "ID"] = [str(uuid.uuid4()) for _ in range(int(m_id_vacio.sum()))]
    base = compute_days(base)                                                        # recálculo de días
    write_all(base)                                                                  # persistimos y fijamos firma


# ── FORMULARIO: NUEVA TAREA ────────────────────────────────────────────────────
st.markdown("### ➕ Nueva tarea")
with st.form("new_task_form", clear_on_submit=True):
    nueva_tarea = st.text_input("Descripción", placeholder="Escribe la tarea...", label_visibility="collapsed")
    submitted = st.form_submit_button("Agregar", use_container_width=True)
    if submitted:
        if nueva_tarea.strip() == "":
            st.warning("Escribe una descripción para la tarea.")
        else:
            hoy = pd.Timestamp(datetime.today().date())
            nueva_fila = pd.DataFrame([{
                "ID": str(uuid.uuid4()),
                "Tarea": nueva_tarea.strip(),
                "Estado": "Pendiente",
                "Fecha de ingreso": hoy,
                "Fecha de completado": pd.NaT,
                "Tiempo sin completar (días)": 0,
            }])
            dfb = ensure_schema(st.session_state["df_tasks"])
            dfb = pd.concat([nueva_fila, dfb], ignore_index=True)
            dfb = compute_days(dfb)
            write_all(dfb)
            st.success("Tarea agregada.")
            st.rerun()


# ── GRID: VER / EDITAR / DESCARTAR ─────────────────────────────────────────────
# 1) Tomamos la base guardada
df_view_base = ensure_schema(st.session_state["df_tasks"]).copy()
df_view_base = compute_days(df_view_base)

# 2) (NUEVO) Resumen global + Filtro en expander “discreto”
with st.expander("📊 Resumen global y filtro de vista", expanded=False):
    # Resumen GLOBAL (no filtrado): cuenta todo lo que hay en la hoja
    total_pend_global = int((df_view_base["Estado"] == "Pendiente").sum())
    total_comp_global = int((df_view_base["Estado"] == "Completada").sum())
    st.markdown(
        f"- **Pendientes:** 🟥 `{total_pend_global}`  "
        f"- **Completadas:** 🟩 `{total_comp_global}`"
    )

    # Selector (tipo “pestaña desplegable”) para FILTRAR la vista del grid
    filtro_vista = st.selectbox(
        "Mostrar en el listado",
        options=["Todos", "Solo pendientes", "Solo completadas"],
        index=0,
        help="Este filtro solo afecta el listado de abajo; el resumen es global."
    )

# 3) Aplicamos el filtro elegido a una copia de la vista
df_view = df_view_base.copy()
if filtro_vista == "Solo pendientes":                            # si pidió ver solo pendientes…
    df_view = df_view[df_view["Estado"] == "Pendiente"].copy()
elif filtro_vista == "Solo completadas":                         # si pidió ver solo completadas…
    df_view = df_view[df_view["Estado"] == "Completada"].copy()
# (Si es "Todos", no filtramos)

# 4) Añadimos columna SOLO-UI para colorear, y ordenamos la vista
df_view["Estado (visual)"] = df_view["Estado"].map(MAPA_ESTADO_VISUAL).fillna("—")
df_view["Estado"] = pd.Categorical(df_view["Estado"], categories=ESTADO_ORDEN, ordered=True)
df_view = df_view.sort_values(by=["Estado", "Tiempo sin completar (días)"], ascending=[True, False], kind="mergesort")

# 5) Config del editor
column_config = {
    "Estado (visual)": st.column_config.TextColumn("Estado (visual)", disabled=True, help="Indicador visual (no editable)."),
    "ID": st.column_config.TextColumn("ID", disabled=True, help="Identificador único de la tarea."),
    "Tarea": st.column_config.TextColumn("Tarea", help="Descripción de la tarea."),
    "Estado": st.column_config.SelectboxColumn("Estado", options=ESTADOS_VALIDOS, help="Pendiente / Completada / Descartar"),
    "Fecha de ingreso": st.column_config.DateColumn("Fecha de ingreso", format="YYYY-MM-DD"),
    "Fecha de completado": st.column_config.DateColumn("Fecha de completado", format="YYYY-MM-DD"),
    "Tiempo sin completar (días)": st.column_config.NumberColumn("Tiempo sin completar (días)", disabled=True),
}
column_order = [
    "Estado (visual)", "Tarea", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)", "ID"
]

# 6) Editor (sin fila dinámica; las altas se hacen con el formulario)
edited = st.data_editor(
    df_view,
    hide_index=True,
    column_config=column_config,
    column_order=column_order,
    width='stretch',
    key="tasks_editor",
)

# ── AUTOGUARDADO DE EDICIONES (MERGE POR ID CONTRA DF COMPLETO) ───────────────
# En lugar de escribir lo que se ve (filtrado), fusionamos cambios del grid
# sobre el DF COMPLETO en sesión usando el ID, y recién allí escribimos TODO.
if not st.session_state.get("tasks_saving", False):
    try:
        st.session_state["tasks_saving"] = True

        # 0) Tomamos el DF COMPLETO actual de sesión (no filtrado)
        base_full = ensure_schema(st.session_state["df_tasks"]).copy()

        # 1) Quitamos columna solo-UI del EDITADO (lo que se ve en el grid)
        edited_clean = edited.drop(columns=["Estado (visual)"], errors="ignore").copy()
        edited_clean = ensure_schema(edited_clean)  # Aseguramos tipos/columnas

        # 2) Capturamos IDs marcados como "Descartar" en el grid (ANTES de filtrarlos)
        discard_ids = set(
            edited_clean.loc[edited_clean["Estado"] == "Descartar", "ID"]
            .dropna().astype(str).tolist()
        )

        # 3) Si marcan "Completada" sin fecha => ponemos HOY (sólo si falta)
        hoy = pd.Timestamp(datetime.today().date())
        m_fc_falta_y_comp = (edited_clean["Estado"] == "Completada") & (edited_clean["Fecha de completado"].isna())
        edited_clean.loc[m_fc_falta_y_comp, "Fecha de completado"] = hoy

        # 4) Quitamos de edited_clean las filas marcadas "Descartar" (ya registramos sus IDs)
        edited_clean = edited_clean[edited_clean["Estado"] != "Descartar"].copy()

        # 5) MERGE por ID: actualizamos el DF COMPLETO con los cambios del grid
        #    - Sólo afectamos las filas cuyos IDs aparecen en el grid editado (filtrado o no).
        #    - Las filas que NO aparecen en el grid (por el filtro) QUEDAN INTACTAS.
        cols_update = ["Tarea", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]
        # Aseguramos que las columnas a actualizar existan en ambos:
        cols_update = [c for c in cols_update if c in edited_clean.columns and c in base_full.columns]

        # Indexamos por ID para actualizar rápidamente
        base_idx = base_full.set_index("ID", drop=False)
        edit_idx = edited_clean.set_index("ID", drop=False)

        # a) Actualizamos filas existentes por ID
        ids_intersect = base_idx.index.intersection(edit_idx.index)
        if len(ids_intersect) > 0 and len(cols_update) > 0:
            base_idx.loc[ids_intersect, cols_update] = edit_idx.loc[ids_intersect, cols_update].values

        # b) Si el grid tiene alguna fila con ID que NO existe en base (raro, pero por si acaso), la agregamos
        ids_new = edit_idx.index.difference(base_idx.index)
        if len(ids_new) > 0:
            base_idx = pd.concat([base_idx, edit_idx.loc[ids_new]], axis=0)

        # c) Eliminamos de base_full las filas marcadas como "Descartar"
        if discard_ids:
            base_idx = base_idx.loc[~base_idx.index.astype(str).isin(discard_ids)].copy()

        # Volvemos a DF plano
        merged_full = base_idx.reset_index(drop=True)

        # 6) Recalcular días en el DF COMPLETO fusionado
        merged_full = compute_days(merged_full)

        # 7) Escribir sólo si cambió respecto a lo último persistido
        sig = simple_signature(merged_full)
        if sig != st.session_state["tasks_last_sig"]:
            write_all(merged_full)                 # reescribe TODO (completo, no filtrado)
            st.toast("✅ Cambios guardados.", icon="✅")
            st.rerun()

    except Exception as e:
        st.error("No se pudieron guardar los cambios.")
        with st.expander("Detalles técnicos"):
            st.exception(e)
    finally:
        st.session_state["tasks_saving"] = False

st.page_link("inicio.py", label="⬅️ Volver al Panel Principal", icon="🏠")
