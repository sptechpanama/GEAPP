# ============================================
# pages/7_✅_Tasks.py
# ============================================

import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from sheets import get_client, read_worksheet, write_worksheet

# --------- Guard: require inicio de sesión -----------
if not st.session_state.get("auth_ok", False):
    st.warning("Debes iniciar sesión para entrar.")
    try:
        # Streamlit >= 1.31
        st.switch_page("Inicio.py")
    except Exception:
        st.write("Ir al Inicio desde el menú lateral.")
    st.stop()
# ------
st.set_page_config(page_title="✅ Tasks", page_icon="✅", layout="wide")
WS_TASKS = st.secrets.get("app", {}).get("WS_TASKS", "pendientes")
ESTADOS_VALIDOS = ["Pendiente", "Completada", "Descartar"]

MAPA_ESTADO_VISUAL = {
    "Pendiente": "🟥 Pendiente",
    "Completada": "🟩 Completada",
    "Descartar": "—",
}
ESTADO_ORDEN = ["Pendiente", "Completada", "Descartar"]

if "tasks_saving" not in st.session_state:
    st.session_state["tasks_saving"] = False
if "tasks_last_sig" not in st.session_state:
    st.session_state["tasks_last_sig"] = ""

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    canon = {
        "id": "ID",
        "tarea": "Tarea",
        "estado": "Estado",
        "categoria": "Categoria",  # <- NUEVO
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
    out = df.copy()
    for col in ["ID", "Tarea", "Estado", "Categoria", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]:
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
    out["ID"]        = out["ID"].astype(str)
    out["Tarea"]     = out["Tarea"].astype(str)
    out["Estado"]    = out["Estado"].astype(str)
    out["Categoria"] = out["Categoria"].astype(str)

    m_nueva = out["Tarea"].astype(str).str.strip() != ""
    m_estado_vacio = out["Estado"].astype(str).str.strip().isin(["", "nan", "NaN", "None"])
    out.loc[m_nueva & m_estado_vacio, "Estado"] = "Pendiente"
    hoy = pd.Timestamp(datetime.today().date())
    m_fi_vacia = out["Fecha de ingreso"].isna()
    out.loc[m_nueva & m_fi_vacia, "Fecha de ingreso"] = hoy
    m_id_vacio = out["ID"].astype(str).str.strip().isin(["", "nan", "NaN", "None"])
    count = int((m_nueva & m_id_vacio).sum())
    if count > 0:
        out.loc[m_nueva & m_id_vacio, "ID"] = [str(uuid.uuid4()) for _ in range(count)]
    return out

def compute_days(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    hoy = pd.Timestamp(datetime.today().date())
    fi = pd.to_datetime(out["Fecha de ingreso"], errors="coerce")
    fc = pd.to_datetime(out["Fecha de completado"], errors="coerce")
    dias = (fc.fillna(hoy) - fi).dt.days
    dias = pd.to_numeric(dias, errors="coerce").fillna(0).astype(int)
    out["Tiempo sin completar (días)"] = dias.clip(lower=0)
    return out

def load_tasks() -> pd.DataFrame:
    client, _ = get_client()
    df = read_worksheet(client, st.secrets["app"]["SHEET_ID"], WS_TASKS)
    if df is None or df.empty:
        df = pd.DataFrame(columns=["ID", "Tarea", "Categoria", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"])
    df = standardize_columns(df)
    df = ensure_schema(df)
    df = compute_days(df)
    return df

def simple_signature(df: pd.DataFrame) -> str:
    snap = df.copy()
    for c in snap.columns:
        if str(snap[c].dtype).startswith("datetime64"):
            snap[c] = pd.to_datetime(snap[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        elif str(snap[c].dtype) in ("int64", "float64"):
            snap[c] = snap[c].fillna(0).astype(str)
        else:
            snap[c] = snap[c].fillna("")
    return snap.to_csv(index=False)

def write_all(df_final: pd.DataFrame):
    cols_order = ["ID", "Tarea", "Categoria", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]
    cols_present = [c for c in cols_order if c in df_final.columns]
    df_to_write = df_final[cols_present].copy()
    write_worksheet(get_client()[0], st.secrets["app"]["SHEET_ID"], WS_TASKS, df_to_write)
    st.session_state["tasks_last_sig"] = simple_signature(df_to_write)
    st.session_state["df_tasks"] = df_to_write.copy()

if "df_tasks" not in st.session_state:
    st.session_state["df_tasks"] = load_tasks()
else:
    st.session_state["df_tasks"] = compute_days(ensure_schema(st.session_state["df_tasks"]))

df_all = ensure_schema(st.session_state["df_tasks"]).copy()
total = len(df_all)
pend = int((df_all["Estado"] == "Pendiente").sum())
comp = int((df_all["Estado"] == "Completada").sum())

col_a, col_b, col_c = st.columns(3)
col_a.metric("Total", total)
col_b.metric("Pendientes", pend)
col_c.metric("Completadas", comp)

# Selector SIN título (etiqueta colapsada)
filtro_estado = st.segmented_control(
    "",  # <- sin palabra "Filtro"
    options=["Todos", "Pendientes", "Completadas"],
    default="Todos",
    # algunos builds de Streamlit soportan esto; si no, se ignora
    label_visibility="collapsed",
)

st.markdown("### ➕ Nueva tarea")
with st.form("new_task_form", clear_on_submit=True):
    c1, c2 = st.columns([2, 1])
    with c1:
        nueva_tarea = st.text_input("Descripción", placeholder="Escribe la tarea...", label_visibility="collapsed")
    with c2:
        # SIN título arriba y placeholder "Categoría"
        nueva_categoria = st.text_input("Categoría", placeholder="Categoría", label_visibility="collapsed")
    submitted = st.form_submit_button("Agregar", use_container_width=True)
    if submitted:
        if nueva_tarea.strip() == "":
            st.warning("Escribe una descripción para la tarea.")
        else:
            hoy = pd.Timestamp(datetime.today().date())
            nueva_fila = pd.DataFrame([{
                "ID": str(uuid.uuid4()),
                "Tarea": nueva_tarea.strip(),
                "Categoria": (nueva_categoria or "").strip(),
                "Estado": "Pendiente",
                "Fecha de ingreso": hoy,
                "Fecha de completado": pd.NaT,
                "Tiempo sin completar (días)": 0,
            }])
            dfb = ensure_schema(st.session_state["df_tasks"])
            dfb = pd.concat([nueva_fila, dfb], ignore_index=True)
            dfb = compute_days(dfb)
            write_all(dfb)        # escribe todo
            st.success("Tarea agregada.")
            st.rerun()            # <- rerun inmediato para reflejar en UI

df_view_base = ensure_schema(st.session_state["df_tasks"]).copy()

if filtro_estado == "Pendientes":
    df_view = df_view_base[df_view_base["Estado"] == "Pendiente"].copy()
elif filtro_estado == "Completadas":
    df_view = df_view_base[df_view_base["Estado"] == "Completada"].copy()
else:
    df_view = df_view_base.copy()

def estado_visual(row):
    return MAPA_ESTADO_VISUAL.get(row.get("Estado", "Pendiente"), "🟥 Pendiente")

df_view["Estado (visual)"] = df_view.apply(estado_visual, axis=1)

df_view["_orden_estado"] = pd.Categorical(df_view["Estado"], categories=ESTADO_ORDEN, ordered=True)
df_view = df_view.sort_values(by=["_orden_estado", "Fecha de ingreso"], ascending=[True, True]).drop(columns=["_orden_estado"])

column_config = {
    "Estado (visual)": st.column_config.TextColumn("Estado (visual)", disabled=True, help="Indicador visual (no editable)."),
    "ID": st.column_config.TextColumn("ID", disabled=True, help="Identificador único de la tarea."),
    "Tarea": st.column_config.TextColumn("Tarea", help="Descripción de la tarea."),
    "Categoria": st.column_config.TextColumn("Categoría"),
    "Estado": st.column_config.SelectboxColumn("Estado", options=ESTADOS_VALIDOS, help="Pendiente / Completada / Descartar"),
    "Fecha de ingreso": st.column_config.DateColumn("Fecha de ingreso", format="YYYY-MM-DD"),
    "Fecha de completado": st.column_config.DateColumn("Fecha de completado", format="YYYY-MM-DD"),
    "Tiempo sin completar (días)": st.column_config.NumberColumn("Tiempo sin completar (días)", disabled=True),
}
column_order = [
    "Estado (visual)", "Tarea", "Categoria", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)", "ID"
]

edited = st.data_editor(
    df_view,
    hide_index=True,
    column_config=column_config,
    column_order=column_order,
    width='stretch',
    key="tasks_editor",
)

if not st.session_state.get("tasks_saving", False):
    try:
        st.session_state["tasks_saving"] = True

        base_full = ensure_schema(st.session_state["df_tasks"]).copy()
        edited_clean = edited.drop(columns=["Estado (visual)"], errors="ignore").copy()
        edited_clean = ensure_schema(edited_clean)

        discard_ids = set(
            edited_clean.loc[edited_clean["Estado"] == "Descartar", "ID"]
            .dropna().astype(str).tolist()
        )
        edited_keep = edited_clean[~edited_clean["ID"].astype(str).isin(discard_ids)].copy()

        cols_update = ["Tarea", "Categoria", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)"]
        cols_update = [c for c in cols_update if c in edited_clean.columns and c in base_full.columns]

        base_idx = base_full.set_index("ID", drop=False)
        edit_idx = edited_keep.set_index("ID", drop=False)

        common_ids = base_idx.index.intersection(edit_idx.index)
        for cid in common_ids:
            for c in cols_update:
                base_idx.at[cid, c] = edit_idx.at[cid, c]

        merged_full = base_idx.reset_index(drop=True)

        if discard_ids:
            merged_full = merged_full[~merged_full["ID"].astype(str).isin(discard_ids)].copy()

        merged_full = compute_days(ensure_schema(merged_full))
        sig = simple_signature(merged_full)
        if sig != st.session_state["tasks_last_sig"]:
            write_all(merged_full)
            st.toast("✅ Cambios guardados.", icon="✅")
            st.rerun()  # <- rerun inmediato tras guardar

    except Exception as e:
        st.error("No se pudieron guardar los cambios.")
        with st.expander("Detalles técnicos"):
            st.exception(e)
    finally:
        st.session_state["tasks_saving"] = False

st.page_link("Inicio.py", label="⬅️ Volver al Panel Principal", icon="🏠")
