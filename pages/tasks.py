# ============================================
# pages/7_✅_Tasks.py
# ============================================

import streamlit as st
import pandas as pd
import uuid
import re
from datetime import datetime
from gspread.exceptions import WorksheetNotFound, APIError
from sheets import get_client, read_worksheet, write_worksheet

# --------- Guard: require inicio de sesión -----------
# Usar la misma clave que `Inicio.py` (streamlit-authenticator pone
# `authentication_status` en `st.session_state`). Antes se revisaba
# `auth_ok`, que no existe y causaba redirección a Inicio incluso si
# el usuario estaba autenticado.
status = st.session_state.get("authentication_status", None)
if status is not True:
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
DEFAULT_TASK_COLUMNS = [
    "ID",
    "Tarea",
    "Categoria",
    "Usuario",
    "Asignado a",
    "Estado",
    "Fecha de ingreso",
    "Fecha de completado",
    "Tiempo sin completar (días)",
]

ASSIGNABLE_USERS = [
    "Rodrigo Sánchez",
    "Irvin Sánchez",
    "Iris Grisel Sánchez",
]

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


_ASSIGNEE_SPLIT_RE = re.compile(r"[;\n\r]+")


def _normalize_assignee_token(token: str) -> str | None:
    cleaned = str(token).strip()
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if lowered in {"nan", "none", "null"}:
        return None
    if cleaned in {"--", "..", "...", "—"}:
        return None
    if lowered.startswith("dtype:"):
        return None
    if lowered.startswith("name:"):
        return None
    match = re.match(r"^\d+\s*[:.-]?\s*(.+)$", cleaned)
    if match:
        candidate = match.group(1).strip()
        if not candidate:
            return None
        cleaned = candidate
        lowered = cleaned.lower()
        if lowered in {"nan", "none", "null"} or lowered.startswith("dtype:") or lowered.startswith("name:"):
            return None
    if cleaned.isdigit():
        return None
    return cleaned


def serialize_asignado(value) -> str:
    if isinstance(value, pd.DataFrame):
        value = value.stack().tolist()
    if isinstance(value, pd.Series):
        value = value.dropna().tolist()
    if isinstance(value, list):
        parts = [str(v).strip() for v in value if str(v).strip()]
    elif value is None or (isinstance(value, float) and pd.isna(value)):
        parts = []
    else:
        text = str(value)
        if "\\n" in text:
            text = text.replace("\\r", "\\n")
            text = text.replace("\\n", "\n")
        if "\r" in text and "\n" not in text:
            text = text.replace("\r", "\n")
        if text.strip() in ("nan", "NaN", "None"):
            parts = []
        else:
            parts = [p.strip() for p in text.split(";") if p.strip()]
    candidates: list[str] = []
    for raw in parts:
        if isinstance(raw, str):
            candidates.extend(_ASSIGNEE_SPLIT_RE.split(raw))
        else:
            candidates.append(str(raw))
    clean_parts: list[str] = []
    for raw in candidates:
        normalized = _normalize_assignee_token(raw)
        if normalized:
            clean_parts.append(normalized)
    return "; ".join(dict.fromkeys(clean_parts))


def deserialize_asignado(value) -> list[str]:
    if isinstance(value, pd.Series):
        value = value.dropna().tolist()
        temp = serialize_asignado(value)
        return temp.split("; ") if temp else []
    serialized = serialize_asignado(value)
    if not serialized:
        return []
    return [p for p in serialized.split("; ") if p]


def _collect_assignees(series: pd.Series) -> set[str]:
    results: set[str] = set()
    if series is None:
        return results
    for value in series.fillna(""):
        for name in deserialize_asignado(value):
            clean = name.strip()
            lower = clean.lower()
            if clean and clean not in {"--", "..", "...", "—"} and not clean.isdigit() and lower not in {"nan", "none", "null"} and not lower.startswith("dtype:"):
                results.add(clean)
    return results

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    canon = {
        "id": "ID",
        "tarea": "Tarea",
    "estado": "Estado",
        "categoria": "Categoria",  # <- NUEVO
    "usuario": "Usuario",
    "asignado a": "Asignado a",
    "asignado_a": "Asignado a",
    "asignado": "Asignado a",
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
    renamed = df.rename(columns=mapping).copy()
    renamed = renamed.loc[:, ~renamed.columns.duplicated()]
    return renamed

def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in DEFAULT_TASK_COLUMNS:
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
    out["Usuario"]   = out["Usuario"].astype(str)
    out["Asignado a"] = out["Asignado a"].apply(serialize_asignado)

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
    sheet_id = st.secrets["app"]["SHEET_ID"]
    ws_title = WS_TASKS
    try:
        df = read_worksheet(client, sheet_id, ws_title)
    except WorksheetNotFound:
        sh = client.open_by_key(sheet_id)
        normalized_target = ws_title.strip().lower()
        alt_title = None
        for ws in sh.worksheets():
            if ws.title.strip().lower() == normalized_target:
                alt_title = ws.title
                break
        if alt_title:
            ws_title = alt_title
            df = read_worksheet(client, sheet_id, ws_title)
        else:
            try:
                ws = sh.add_worksheet(title=ws_title, rows=200, cols=len(DEFAULT_TASK_COLUMNS))
                ws.update("A1", [DEFAULT_TASK_COLUMNS])
            except APIError as api_err:
                if "already exists" not in str(api_err):
                    raise
                df = read_worksheet(client, sheet_id, ws_title)
            else:
                df = pd.DataFrame(columns=DEFAULT_TASK_COLUMNS)
    st.session_state["tasks_ws_title"] = ws_title
    if df is None or df.empty:
        df = pd.DataFrame(columns=DEFAULT_TASK_COLUMNS)
    df = standardize_columns(df)
    df = ensure_schema(df)
    df = compute_days(df)
    return df

def simple_signature(df: pd.DataFrame) -> str:
    snap = df.copy()
    for c in snap.columns:
        col = snap[c]
        if isinstance(col, pd.DataFrame):
            col = col.iloc[:, 0]
        if col.dtype == object and col.apply(lambda x: isinstance(x, list)).any():
            col = col.apply(lambda v: "; ".join(v) if isinstance(v, list) else (v or ""))
        if str(col.dtype).startswith("datetime64"):
            col = pd.to_datetime(col, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        elif str(col.dtype) in ("int64", "float64"):
            col = col.fillna(0).astype(str)
        else:
            col = col.fillna("")
        snap[c] = col
    return snap.to_csv(index=False)

def write_all(df_final: pd.DataFrame):
    cols_order = [
        "ID",
        "Tarea",
        "Categoria",
        "Usuario",
        "Asignado a",
        "Estado",
        "Fecha de ingreso",
        "Fecha de completado",
        "Tiempo sin completar (días)",
    ]
    cols_present = [c for c in cols_order if c in df_final.columns]
    df_to_write = df_final[cols_present].copy()
    if "Asignado a" in df_to_write.columns:
        df_to_write["Asignado a"] = df_to_write["Asignado a"].apply(serialize_asignado)
    ws_name = st.session_state.get("tasks_ws_title", WS_TASKS)
    write_worksheet(get_client()[0], st.secrets["app"]["SHEET_ID"], ws_name, df_to_write)
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

if "Asignado a" in df_all.columns:
    asignado_series_all = df_all["Asignado a"]
    if isinstance(asignado_series_all, pd.DataFrame):
        asignado_series_all = asignado_series_all.iloc[:, 0]
else:
    asignado_series_all = pd.Series(dtype=str)

existing_assignees_all = _collect_assignees(asignado_series_all.fillna(""))
def _filter_valid_names(names):
    filtered = []
    for name in names:
        normalized = _normalize_assignee_token(name)
        if normalized:
            filtered.append(normalized)
    return filtered

current_assignable_options = sorted(set(_filter_valid_names(ASSIGNABLE_USERS)) | set(existing_assignees_all))

col_a, col_b, col_c = st.columns(3)
col_a.metric("Total", total)
col_b.metric("Pendientes", pend)
col_c.metric("Completadas", comp)

# Selector SIN título (etiqueta colapsada)
filtro_estado = st.segmented_control(
    "Estado",
    options=["Todos", "Pendientes", "Completadas"],
    default="Todos",
    label_visibility="collapsed",
)

st.markdown("### ➕ Nueva tarea")
with st.form("new_task_form", clear_on_submit=True):
    col_desc, col_cat = st.columns([2, 1])
    with col_desc:
        nueva_tarea = st.text_input("Descripción", placeholder="Escribe la tarea...", label_visibility="collapsed")
    with col_cat:
        nueva_categoria = st.text_input("Categoría", placeholder="Categoría", label_visibility="collapsed")

    usuario_default = st.session_state.get("name") or st.session_state.get("username") or ""

    asignado_multi = st.multiselect(
        "Asignado a",
        options=current_assignable_options,
        default=[],
        placeholder="Selecciona responsable(s)",
    )
    submitted = st.form_submit_button("Agregar", width="stretch")
    if submitted:
        if nueva_tarea.strip() == "":
            st.warning("Escribe una descripción para la tarea.")
        else:
            hoy = pd.Timestamp(datetime.today().date())
            nueva_fila = pd.DataFrame([{
                "ID": str(uuid.uuid4()),
                "Tarea": nueva_tarea.strip(),
                "Categoria": (nueva_categoria or "").strip(),
                "Usuario": usuario_default.strip(),
                "Asignado a": serialize_asignado(asignado_multi),
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

df_display = df_view.copy()

if "Asignado a" in df_display.columns:
    asignado_series_view = df_display["Asignado a"]
    if isinstance(asignado_series_view, pd.DataFrame):
        asignado_series_view = asignado_series_view.iloc[:, 0]
    df_display["Asignado a"] = asignado_series_view.apply(deserialize_asignado)
else:
    df_display["Asignado a"] = [[] for _ in range(len(df_display))]

existing_assignees = sorted({p for lista in df_display["Asignado a"] for p in (lista or []) if p})
assignable_options = sorted(set(current_assignable_options) | set(existing_assignees))

column_config = {
    "Estado (visual)": st.column_config.TextColumn("Estado (visual)", disabled=True, help="Indicador visual (no editable)."),
    "ID": st.column_config.TextColumn("ID", disabled=True, help="Identificador único de la tarea."),
    "Tarea": st.column_config.TextColumn("Tarea", help="Descripción de la tarea."),
    "Categoria": st.column_config.TextColumn("Categoría"),
    "Usuario": st.column_config.TextColumn("Usuario", help="Quien registró el pendiente."),
    "Asignado a": st.column_config.MultiselectColumn(
        "Asignado a",
        help="Selecciona uno o más responsables (escribe y presiona Enter).",
        default=[],
        options=assignable_options,
        accept_new_options=True,
    ),
    "Estado": st.column_config.SelectboxColumn("Estado", options=ESTADOS_VALIDOS, help="Pendiente / Completada / Descartar"),
    "Fecha de ingreso": st.column_config.DateColumn("Fecha de ingreso", format="YYYY-MM-DD"),
    "Fecha de completado": st.column_config.DateColumn("Fecha de completado", format="YYYY-MM-DD"),
    "Tiempo sin completar (días)": st.column_config.NumberColumn("Tiempo sin completar (días)", disabled=True),
}
column_order = [
    "Estado (visual)", "Tarea", "Categoria", "Usuario", "Asignado a", "Estado", "Fecha de ingreso", "Fecha de completado", "Tiempo sin completar (días)", "ID"
]

edited = st.data_editor(
    df_display,
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
        if "Asignado a" in edited_clean.columns:
            edited_clean["Asignado a"] = edited_clean["Asignado a"].apply(
                lambda v: list(v) if isinstance(v, (list, tuple, set)) else v
            )
            edited_clean["Asignado a"] = edited_clean["Asignado a"].apply(serialize_asignado)
        edited_clean = ensure_schema(edited_clean)

        discard_ids = set(
            edited_clean.loc[edited_clean["Estado"] == "Descartar", "ID"]
            .dropna().astype(str).tolist()
        )
        edited_keep = edited_clean[~edited_clean["ID"].astype(str).isin(discard_ids)].copy()

        cols_update = [
            "Tarea",
            "Categoria",
            "Usuario",
            "Asignado a",
            "Estado",
            "Fecha de ingreso",
            "Fecha de completado",
            "Tiempo sin completar (días)",
        ]
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
