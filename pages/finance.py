# ================================================
# finance.py
# Finanzas operativas (Ingresos / Gastos)
# - Borrado real en Sheets
# - Comisiones 8% automáticas al cobrarse ingresos
# - Gastos con Cliente/Proyecto (cuando Categoría=Proyectos)
# - Ingresos: ocultar "Concepto" en la tabla (queda solo "Descripcion")
# - Catálogo: Un único expander para crear Clientes y Proyectos (ID auto)
# ================================================

from __future__ import annotations
import uuid, time
import streamlit as st
st.set_page_config(page_title="Finanzas Operativas", page_icon="📊", layout="wide")
import pandas as pd
from datetime import date

from sheets import get_client, read_worksheet, write_worksheet
from charts import (
    chart_bars_saldo_mensual,
    chart_line_ing_gas_util_mensual,
    chart_bar_top_gastos,
)
from services.backups import debug_sa_quota
from core.metrics import kpis_finanzas, monthly_pnl, top_gastos_por_categoria
from core.cashflow import preparar_cashflow
try:
    from core.sync import sync_cambios
except Exception:
    from sync import sync_cambios

from services.backups import (
    start_backup_scheduler_once,
    get_last_backup_info,
    create_backup_now,  
)

from gspread.exceptions import APIError

from entities import (
    client_selector,
    project_selector,
    WS_PROYECTOS,
    WS_CLIENTES,
    _load_clients,
    _load_projects,
)


# ---------- Guard: require inicio de sesión ------------
import bcrypt, streamlit_authenticator as stauth

USERS = {
    "rsanchez": ("Rodrigo Sánchez", "Sptech-71"),
    "isanchez": ("Irvin Sánchez",   "Sptech-71"),
    "igsanchez": ("Iris Grisel Sánchez", "Sptech-71"),
}
def _hash(pw: str) -> str:
    import bcrypt
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

credentials = {"usernames": {u: {"name": n, "password": _hash(p)} for u,(n,p) in USERS.items()}}

COOKIE_NAME = "finapp_auth"
COOKIE_KEY  = "finapp_key_123"
authenticator = stauth.Authenticate(credentials, COOKIE_NAME, COOKIE_KEY, 30)

# 🔁 Rehidrata autenticación desde cookie (no mostramos formulario aquí)
# Nota: en versiones actuales, llamar login() rellena session_state si la cookie es válida
try:
    authenticator.login(" ", location="sidebar", key="auth_finanzas_silent")
    # inmediatamente limpiamos el contenedor del sidebar (evita parpadeo si no hay cookie)
    st.sidebar.empty()
except Exception:
    pass

# ✅ Si NO está autenticado, redirige a Inicio en vez de mostrar error
if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")

# 🔧 Normaliza claves para _current_user()
st.session_state.setdefault("auth_user_name", st.session_state.get("name", ""))
st.session_state.setdefault("auth_username",  st.session_state.get("username", ""))

# Botón de logout visible en esta página
authenticator.logout("Cerrar sesión", location="sidebar")



# … contenido real de la página …

# -------------------- Constantes --------------------
COL_FECHA   = "Fecha"
COL_DESC    = "Descripcion"       # tu app usa "Descripcion" (sin tilde)
COL_CONC    = "Concepto"
COL_MONTO   = "Monto"
COL_CAT     = "Categoria"
COL_ESC     = "Escenario"
COL_PROY    = "Proyecto"
COL_CLI_ID  = "ClienteID"
COL_CLI_NOM = "ClienteNombre"
COL_EMP     = "Empresa"
COL_COB     = "Cobrado"
COL_FCOBRO  = "Fecha de cobro"
COL_ROWID   = "RowID"
COL_REF_RID = "Ref RowID Ingreso"
COL_POR_COB = "Por_cobrar"        # Ingresos: "No"/"Sí"
COL_POR_PAG = "Por_pagar"         # Gastos:   "No"/"Sí"
COL_PROV    = "Proveedor"         # Gastos: proveedor del gasto
COL_USER  = "Usuario"


EMPRESAS_OPCIONES = ["RS-SP", "RIR"]
EMPRESA_DEFAULT   = "RS-SP"


# -------------------- Helpers generales --------------------


def _today() -> date: return date.today()

def _ts(x):
    try: return pd.to_datetime(x, errors="coerce")
    except Exception: return pd.NaT

def _si_no_norm(x) -> str:
    s = str(x).strip().lower()
    return "Sí" if s in {"si","sí","sí","yes","y","true","1"} else "No"

def _current_user() -> str:
    """
    Devuelve el nombre de usuario desde session_state.
    Ajusta las claves si tu app guarda el usuario con otro nombre.
    """
    for k in ("auth_user_name", "auth_username", "user_name", "user", "usuario", "auth_user"):
        v = st.session_state.get(k)
        if v:
            return str(v).strip()
    return ""


def _ensure_text(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype("string").fillna("")
    return out

def _canon_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres alternos hacia las columnas que usa tu app."""
    df = df.copy()
    ren = {}
    if "Descripción" in df.columns and COL_DESC not in df.columns: ren["Descripción"] = COL_DESC
    if "Categoría" in df.columns and COL_CAT not in df.columns:   ren["Categoría"] = COL_CAT
    for alt in ["EmpresaID","EmpresaNombre","Company","Razón Social","Razon Social"]:
        if alt in df.columns and COL_EMP not in df.columns: ren[alt] = COL_EMP; break
    for alt in ["Por cobrar","PorCobrar","por_cobrar"]:
        if alt in df.columns and COL_POR_COB not in df.columns: ren[alt] = COL_POR_COB; break
    for alt in ["Por pagar","PorPagar","por_pagar"]:
        if alt in df.columns and COL_POR_PAG not in df.columns: ren[alt] = COL_POR_PAG; break
    return df.rename(columns=ren) if ren else df

def _make_rowid(row: pd.Series) -> str:
    rid = str(row.get(COL_ROWID, "")).strip()
    return rid or uuid.uuid4().hex


# -------------------- Normalizadores (Ingresos/Gastos) --------------------
def ensure_ingresos_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = _canon_cols(df.copy())
    for col in [
        COL_FECHA, COL_DESC, COL_CONC, COL_MONTO, COL_CAT, COL_ESC,
        COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_COB,
        COL_COB, COL_FCOBRO, COL_ROWID, COL_USER
    ]:
        if col not in out.columns:
            if col in {COL_MONTO}:
                out[col] = 0.0
            elif col in {COL_FECHA, COL_FCOBRO}:
                out[col] = pd.NaT
            elif col in {COL_EMP}:
                out[col] = EMPRESA_DEFAULT
            elif col in {COL_POR_COB, COL_COB}:
                out[col] = "No"
            else:
                out[col] = ""
    out[COL_FECHA] = _ts(out[COL_FECHA]); out[COL_FCOBRO] = _ts(out[COL_FCOBRO])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_EMP]   = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT
    )
    out[COL_POR_COB] = out[COL_POR_COB].map(_si_no_norm)
    out[COL_COB]     = out[COL_COB].map(_si_no_norm)
    out = _ensure_text(out, [COL_DESC, COL_CONC, COL_CAT, COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_COB, COL_COB, COL_ROWID, COL_USER])
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out

def ensure_gastos_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = _canon_cols(df.copy())
    for col in [COL_FECHA, COL_CONC, COL_MONTO, COL_CAT, COL_ESC, COL_REF_RID,
                COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_PAG, COL_PROV, COL_ROWID, COL_USER]:
        if col not in out.columns:
            if col == COL_MONTO: out[col] = 0.0
            elif col == COL_FECHA: out[col] = pd.NaT
            elif col == COL_EMP: out[col] = EMPRESA_DEFAULT
            elif col == COL_POR_PAG: out[col] = "No"
            else: out[col] = ""
    out[COL_FECHA] = _ts(out[COL_FECHA])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_EMP]   = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT
    )
    out[COL_POR_PAG] = out[COL_POR_PAG].map(_si_no_norm)
    out = _ensure_text(out, [COL_CONC, COL_CAT, COL_REF_RID, COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_PAG, COL_PROV, COL_ROWID, COL_USER])
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out


# -------------------- Normalizadores (Catálogo) --------------------
def ensure_clientes_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if COL_CLI_ID not in out.columns:  out[COL_CLI_ID] = ""
    if COL_CLI_NOM not in out.columns: out[COL_CLI_NOM] = ""
    if COL_EMP not in out.columns:     out[COL_EMP] = EMPRESA_DEFAULT
    if COL_ROWID not in out.columns:   out[COL_ROWID] = ""
    out = _ensure_text(out, [COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_ROWID])
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out

def ensure_proyectos_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if COL_PROY not in out.columns:    out[COL_PROY] = ""
    if COL_CLI_ID not in out.columns:  out[COL_CLI_ID] = ""
    if COL_CLI_NOM not in out.columns: out[COL_CLI_NOM] = ""
    if COL_EMP not in out.columns:     out[COL_EMP] = EMPRESA_DEFAULT
    if COL_ROWID not in out.columns:   out[COL_ROWID] = ""
    out = _ensure_text(out, [COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_ROWID])
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out


# -------------------- Catálogo en memoria --------------------
CATALOG_TTL_SECONDS = 180  # recarga cada 3 minutos si no se fuerza


def _format_catalog_label(name: str, identifier: str) -> str:
    name = (name or "").strip()
    identifier = (identifier or "").strip()
    if identifier and name:
        return f"{name} ▸ {identifier}"
    return identifier or name or ""


def _ensure_catalog_data(
    client,
    sheet_id: str,
    *,
    force: bool = False,
    clients_df: pd.DataFrame | None = None,
    projects_df: pd.DataFrame | None = None,
) -> None:
    now_ts = time.time()
    last_loaded = st.session_state.get("catalog_loaded_at", 0.0)
    should_refresh = force or (now_ts - last_loaded > CATALOG_TTL_SECONDS) or (
        "catalog_clients_df" not in st.session_state or "catalog_projects_df" not in st.session_state
    )
    if not should_refresh:
        return

    if clients_df is not None:
        raw_cli = clients_df.copy()
    else:
        try:
            raw_cli = read_worksheet(client, sheet_id, WS_CLIENTES)
        except APIError as api_err:
            _handle_gspread_error(api_err, "cargar el catálogo de clientes")
            return
        except Exception as exc:
            st.error(f"No se pudo cargar el catálogo de clientes. {exc}")
            return

    df_cli = ensure_clientes_columns(raw_cli)
    df_cli[COL_CLI_ID] = df_cli[COL_CLI_ID].astype(str).str.strip()
    df_cli[COL_CLI_NOM] = df_cli[COL_CLI_NOM].astype(str).str.strip()
    df_cli = df_cli[df_cli[COL_CLI_ID] != ""].drop_duplicates(subset=[COL_CLI_ID]).reset_index(drop=True)
    cli_labels: list[str] = []
    cli_label_map: dict[str, dict[str, str]] = {}
    cli_id_to_label: dict[str, str] = {}
    cli_by_emp: dict[str, list[str]] = {}
    for _, row in df_cli.iterrows():
        label = _format_catalog_label(row[COL_CLI_NOM], row[COL_CLI_ID]) or row[COL_CLI_ID]
        cli_labels.append(label)
        cli_label_map[label] = {"ClienteID": row[COL_CLI_ID], "ClienteNombre": row[COL_CLI_NOM]}
        cli_id_to_label[row[COL_CLI_ID]] = label
        emp_key = (row.get(COL_EMP) or EMPRESA_DEFAULT).strip().upper()
        bucket = cli_by_emp.setdefault(emp_key, [])
        if label not in bucket:
            bucket.append(label)
    st.session_state["catalog_clients_df"] = df_cli
    st.session_state["catalog_clients_opts"] = [""] + cli_labels
    st.session_state["catalog_clients_label_map"] = cli_label_map
    st.session_state["catalog_clients_id_to_label"] = cli_id_to_label
    st.session_state["catalog_clients_by_emp"] = {
        emp: [""] + labels if labels and labels[0] != "" else labels or [""]
        for emp, labels in cli_by_emp.items()
    }

    if projects_df is not None:
        raw_proj = projects_df.copy()
    else:
        try:
            raw_proj = read_worksheet(client, sheet_id, WS_PROYECTOS)
        except APIError as api_err:
            _handle_gspread_error(api_err, "cargar el catálogo de proyectos")
            return
        except Exception as exc:
            st.error(f"No se pudo cargar el catálogo de proyectos. {exc}")
            return

    df_proj = ensure_proyectos_columns(raw_proj)
    df_proj[COL_PROY] = df_proj[COL_PROY].astype(str).str.strip()
    df_proj[COL_CLI_ID] = df_proj[COL_CLI_ID].astype(str).str.strip()
    df_proj[COL_CLI_NOM] = df_proj[COL_CLI_NOM].astype(str).str.strip()
    df_proj = df_proj[df_proj[COL_PROY] != ""].drop_duplicates(subset=[COL_PROY]).reset_index(drop=True)
    proj_labels = []
    proj_label_map = {}
    for _, row in df_proj.iterrows():
        proj_id = str(row.get("ProyectoID", row[COL_PROY])).strip()
        proj_name = str(row.get("ProyectoNombre", row[COL_PROY])).strip()
        label = _format_catalog_label(proj_name or proj_id, proj_id) or proj_id
        proj_labels.append(label)
        proj_label_map[label] = {
            "ProyectoID": proj_id,
            "ProyectoNombre": proj_name or proj_id,
            "ClienteID": row[COL_CLI_ID],
            "ClienteNombre": row[COL_CLI_NOM],
        }
    df_proj["__label__"] = proj_labels
    st.session_state["catalog_projects_df"] = df_proj
    st.session_state["catalog_projects_label_map"] = proj_label_map
    st.session_state["catalog_loaded_at"] = now_ts


def _handle_gspread_error(exc: Exception, action: str) -> None:
    """Show a friendly error message after a Sheets API failure."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    detail = ""
    try:
        payload = getattr(exc, "response", None)
        if payload is not None:
            data = payload.json()
            detail = data.get("error", {}).get("message", "")
    except Exception:
        detail = ""

    if status == 429:
        detail = detail or "Google limitó temporalmente las lecturas/escrituras. Espera unos segundos e inténtalo nuevamente."
    elif status == 403:
        detail = detail or "La cuenta actual no tiene permisos suficientes para modificar la hoja."
    message = detail or str(exc)
    st.error(f"No se pudo {action}. {message}")


def _on_client_change(prefix: str) -> None:
    label = st.session_state.get(f"{prefix}_cliente_raw", "")
    info = st.session_state.get("catalog_clients_label_map", {}).get(label)
    if info:
        st.session_state[f"{prefix}_cliente_id"] = info["ClienteID"]
        st.session_state[f"{prefix}_cliente_nombre"] = info["ClienteNombre"]
    else:
        st.session_state[f"{prefix}_cliente_id"] = ""
        st.session_state[f"{prefix}_cliente_nombre"] = ""
    if not st.session_state.pop(f"{prefix}_skip_project_sync", False):
        _sync_project_selection(prefix)


def _on_project_change(prefix: str) -> None:
    label = st.session_state.get(f"{prefix}_proyecto_raw", "")
    info = st.session_state.get("catalog_projects_label_map", {}).get(label)
    if info:
        st.session_state[f"{prefix}_proyecto_id"] = info["ProyectoID"]
        st.session_state[f"{prefix}_proyecto_nombre"] = info["ProyectoNombre"]
        st.session_state[f"{prefix}_proyecto_cliente_id"] = info.get("ClienteID", "")
        st.session_state[f"{prefix}_proyecto_cliente_nombre"] = info.get("ClienteNombre", "")
        client_label = st.session_state.get("catalog_clients_id_to_label", {}).get(info.get("ClienteID"), "")
        if client_label:
            current_label = st.session_state.get(f"{prefix}_cliente_raw")
            if current_label != client_label:
                st.session_state[f"{prefix}_skip_project_sync"] = True
                st.session_state[f"{prefix}_cliente_raw"] = client_label
                _on_client_change(prefix)
    else:
        st.session_state[f"{prefix}_proyecto_id"] = ""
        st.session_state[f"{prefix}_proyecto_nombre"] = ""
        st.session_state[f"{prefix}_proyecto_cliente_id"] = ""
        st.session_state[f"{prefix}_proyecto_cliente_nombre"] = ""


def _build_project_options(prefix: str, client_id: str | None = None) -> list[str]:
    df_proj = st.session_state.get("catalog_projects_df")
    if df_proj is None or df_proj.empty:
        return [""]
    if client_id is None:
        client_id = st.session_state.get(f"{prefix}_cliente_id", "")
    if client_id:
        df_view = df_proj[df_proj[COL_CLI_ID].astype(str) == str(client_id)].copy()
    else:
        df_view = df_proj.copy()
    labels = df_view["__label__"].tolist()
    return [""] + labels if labels else [""]


def _sync_project_selection(prefix: str) -> None:
    options = _build_project_options(prefix)
    key = f"{prefix}_proyecto_raw"
    if key not in st.session_state or st.session_state[key] not in options:
        st.session_state[key] = options[0] if options else ""
    if st.session_state.get(key):
        _on_project_change(prefix)


def _client_options_for_company(company: str | None) -> list[str]:
    """
    Devuelve las opciones de cliente filtradas por empresa.
    Si no hay coincidencias o la empresa es None, se usan todas las opciones.
    """
    by_emp = st.session_state.get("catalog_clients_by_emp")
    if company and isinstance(by_emp, dict):
        opts = by_emp.get(str(company).strip().upper())
        if opts:
            return opts
    return st.session_state.get("catalog_clients_opts", [""])


def _ensure_client_selection(prefix: str, options: list[str]) -> None:
    """
    Asegura que el valor en session_state para el cliente pertenezca a `options`.
    Si no pertenece, se reajusta al primer valor y se sincroniza la info derivada.
    """
    key = f"{prefix}_cliente_raw"
    if key not in st.session_state or st.session_state[key] not in options:
        st.session_state[key] = options[0] if options else ""
    _on_client_change(prefix)


def _prepare_entry_defaults(prefix: str) -> list[str]:
    client_opts = st.session_state.get("catalog_clients_opts", [""])
    _ensure_client_selection(prefix, client_opts)
    options = _build_project_options(prefix)
    proj_key = f"{prefix}_proyecto_raw"
    if proj_key not in st.session_state or st.session_state[proj_key] not in options:
        st.session_state[proj_key] = options[0] if options else ""
    if st.session_state.get(proj_key):
        _on_project_change(prefix)
    return options


def _reset_entry_state(prefix: str) -> None:
    for suffix in [
        "cliente_raw",
        "cliente_id",
        "cliente_nombre",
        "proyecto_raw",
        "proyecto_id",
        "proyecto_nombre",
        "proyecto_cliente_id",
        "proyecto_cliente_nombre",
        "empresa_quick",
        "fecha_quick",
        "monto_quick",
        "porcob_quick",
        "porpag_quick",
        "categoria_quick",
        "desc_quick",
        "proveedor_quick",
        "skip_project_sync",
    ]:
        st.session_state.pop(f"{prefix}_{suffix}", None)


# -------------------- Página --------------------
st.markdown("<h1>📊 Finanzas</h1>", unsafe_allow_html=True)


# ======================
# 🔧 CONEXIÓN GOOGLE SHEETS (OPTIMIZADA)
# ======================

if "google_client" not in st.session_state or "google_creds" not in st.session_state:
    with st.spinner("Conectando con Google Sheets..."):
        gclient, gcreds = get_client()
        st.session_state.google_client = gclient
        st.session_state.google_creds = gcreds
        st.session_state.google_cache_token = uuid.uuid4().hex
        st.success("✅ Conexión establecida")

client = st.session_state.google_client
creds = st.session_state.google_creds
if "google_cache_token" not in st.session_state:
    st.session_state.google_cache_token = uuid.uuid4().hex

SHEET_ID = st.secrets["app"]["SHEET_ID"]
WS_ING   = st.secrets["app"]["WS_ING"]
WS_GAS   = st.secrets["app"]["WS_GAS"]

# Guardar las credenciales en session_state
st.session_state.google_creds = creds
st.session_state.google_client = client

force_catalog_reload = st.session_state.pop("catalog_force_reload", False)
st.session_state.setdefault("catalog_clients_opts", [""])
st.session_state.setdefault("catalog_clients_label_map", {})
st.session_state.setdefault("catalog_clients_id_to_label", {})
st.session_state.setdefault("catalog_projects_df", pd.DataFrame())
st.session_state.setdefault("catalog_projects_label_map", {})
try:
    _ensure_catalog_data(client, SHEET_ID, force=force_catalog_reload)
except APIError as api_err:
    _handle_gspread_error(api_err, "cargar el catálogo")
except Exception as exc:
    st.error(f"No se pudo cargar el catálogo. {exc}")


# ---- Scheduler de backups: iniciar una sola vez
if not st.session_state.get("backup_started"):
    try:
        start_backup_scheduler_once(creds, st.secrets["app"]["SHEET_ID"])
        st.session_state["backup_started"] = True
        print("[INIT] Backup scheduler iniciado.")
    except Exception as e:
        print(f"[WARN] No se pudo iniciar backup: {e}")

# ======================
# 📦 CACHÉ DE LECTURA
# ======================

@st.cache_data(ttl=120)
def get_sheet_df_cached(sid: str, ws: str, cache_token: str):
    # usa el client guardado en session_state para evitar pasarlo como arg
    # cache_token asegura invalidación si el client/credencial cambia
    client_obj = st.session_state.get("google_client")
    if client_obj is None:
        client_obj, client_creds = get_client()
        st.session_state.google_client = client_obj
        st.session_state.google_creds = client_creds
        st.session_state.google_cache_token = uuid.uuid4().hex
    _ = cache_token  # solo para clave de caché
    return read_worksheet(client_obj, sid, ws)


@st.cache_data(ttl=300)
def load_norm_cached(sid: str, ws: str, is_ingresos: bool, cache_token: str):
    df = get_sheet_df_cached(sid, ws, cache_token)
    return ensure_ingresos_columns(df) if is_ingresos else ensure_gastos_columns(df)


def _norm_for_compare(df: pd.DataFrame, id_col: str | None = None) -> pd.DataFrame:
    out = df.copy()

    # Orden estable por id si existe
    if id_col and id_col in out.columns:
        out = out.sort_values(id_col).reset_index(drop=True)

    # Normalizar datetimes a YYYY-MM-DD (o vacío si NaT)
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")

    # Redondear floats a 2 decimales para comparación estable
    for c in out.select_dtypes(include=["float", "float64", "float32"]).columns:
        out[c] = out[c].round(2)

    # Texto: sin NaN
    out = out.fillna("")
    # Columnas en orden determinista
    out = out.reindex(sorted(out.columns), axis=1)
    return out


def safe_write_worksheet(client, sheet_id, worksheet, new_df, old_df=None, id_col: str | None = "RowID") -> bool:
    """
    Escribe solo si cambió. Devuelve True si escribió.
    """
    try:
        nd = _norm_for_compare(new_df, id_col)
        if old_df is not None:
            od = _norm_for_compare(old_df, id_col)
            if nd.equals(od):
                return False
        write_worksheet(client, sheet_id, worksheet, new_df)
        return True
    except Exception as e:
        print(f"[WARN] Error al escribir en {worksheet}: {e}")
        return False

# Carga base
cache_token = st.session_state.google_cache_token
st.session_state.df_ing = load_norm_cached(SHEET_ID, WS_ING, True, cache_token)
st.session_state.df_gas = load_norm_cached(SHEET_ID, WS_GAS, False, cache_token)


# === Copias "antes" para comparar cambios ===
df_ing_before = st.session_state.df_ing.copy()
df_gas_before = st.session_state.df_gas.copy()



# -------------------- Filtros + Buscador + Empresa --------------------
default_desde = date(date.today().year, 1, 1)
default_hasta = _today()

with st.sidebar:
    st.markdown("### 🎛️ Filtros")
    with st.expander("Rango y criterios", expanded=True):
        f_desde = st.date_input("Desde", value=default_desde, key="filtro_desde")
        f_hasta = st.date_input("Hasta", value=default_hasta, key="filtro_hasta")
        filtro_empresa = st.selectbox("Empresa", options=["Todas"] + EMPRESAS_OPCIONES, index=0, key="filtro_empresa")
        search_q = st.text_input(
            "🔎 Buscar (cliente, proyecto, descripción, concepto, categoría, empresa)",
            key="global_search",
        )

    active_tags = []
    if isinstance(f_desde, date) and f_desde != default_desde:
        active_tags.append(f"Desde {f_desde.strftime('%Y-%m-%d')}")
    if isinstance(f_hasta, date) and f_hasta != default_hasta:
        active_tags.append(f"Hasta {f_hasta.strftime('%Y-%m-%d')}")
    if filtro_empresa != "Todas":
        active_tags.append(f"Empresa: {filtro_empresa}")
    if search_q.strip():
        active_tags.append(f"Busca: {search_q.strip()[:30]}" + ("…" if len(search_q.strip()) > 30 else ""))

    if active_tags:
        chips = " ".join(
            f"<span style='background-color:#1f2630;padding:4px 8px;border-radius:12px;font-size:12px;display:inline-block;margin-right:4px;margin-bottom:4px;'>{tag}</span>"
            for tag in active_tags
        )
        st.sidebar.markdown("**Filtros activos:**<br>" + chips, unsafe_allow_html=True)

def _filtrar_periodo(df: pd.DataFrame, d1: date, d2: date) -> pd.DataFrame:
    if COL_FECHA not in df.columns: return df.copy()
    out = df.copy(); out[COL_FECHA] = _ts(out[COL_FECHA])
    m = (out[COL_FECHA] >= pd.Timestamp(d1)) & (out[COL_FECHA] <= pd.Timestamp(d2))
    return out[m]

df_ing_f = _filtrar_periodo(st.session_state.df_ing, f_desde, f_hasta)
df_gas_f = _filtrar_periodo(st.session_state.df_gas, f_desde, f_hasta)

# Filtro por Empresa
if filtro_empresa != "Todas":
    df_ing_f = df_ing_f[df_ing_f[COL_EMP].astype(str).str.upper() == filtro_empresa.upper()]
    df_gas_f = df_gas_f[df_gas_f[COL_EMP].astype(str).str.upper() == filtro_empresa.upper()]

# Buscador global
if search_q.strip():
    q = search_q.strip().lower()
    def _match_df(df: pd.DataFrame) -> pd.DataFrame:
        cols = [COL_CLI_NOM, COL_CLI_ID, COL_PROY, COL_DESC, COL_CONC, COL_CAT, COL_EMP]
        tmp = df.copy()
        for c in cols:
            if c not in tmp.columns:
                tmp[c] = ""
            tmp[c] = tmp[c].astype(str).str.lower()
        mask = pd.Series(False, index=tmp.index)
        for c in cols:
            mask = mask | tmp[c].str.contains(q, na=False)
        return df[mask]
    df_ing_f = _match_df(df_ing_f)
    df_gas_f = _match_df(df_gas_f)


# Reales (excluyen por cobrar / por pagar)
df_ing_reales  = df_ing_f[df_ing_f[COL_POR_COB].map(_si_no_norm) == "No"].copy()
df_gas_reales  = df_gas_f[df_gas_f[COL_POR_PAG].map(_si_no_norm) == "No"].copy()

# -------------------- KPIs principales --------------------
ing_total = float(df_ing_reales[COL_MONTO].sum()) if COL_MONTO in df_ing_reales.columns else 0.0
gas_total = float(df_gas_reales[COL_MONTO].sum()) if COL_MONTO in df_gas_reales.columns else 0.0
utilidad  = ing_total - gas_total
margen    = (utilidad / ing_total * 100.0) if ing_total > 0 else 0.0

from ui.kpis import render_kpis
render_kpis(ing_total, gas_total, utilidad, margen)

# ---- Flujo y saldo actual ----
cash = preparar_cashflow(df_ing_reales, df_gas_reales)
saldo_actual = float(cash["Saldo"].iloc[-1]) if not cash.empty else 0.0

# KPI: Capital disponible + Capital actual + CxC futuras (al lado)
colchon_fijo = st.number_input("Colchón fijo (USD)", min_value=0.0, value=15000.0, step=500.0)
cxp_activas = float(df_gas_f[df_gas_f[COL_POR_PAG].map(_si_no_norm) == "Sí"][COL_MONTO].sum()) if not df_gas_f.empty else 0.0
capital_disponible = saldo_actual - colchon_fijo - cxp_activas
cxc_futuras = float(df_ing_f[df_ing_f[COL_POR_COB].map(_si_no_norm) == "Sí"][COL_MONTO].sum()) if not df_ing_f.empty else 0.0

k1, k2, k3 = st.columns(3)
with k1: st.metric("Capital disponible para inversión", f"${capital_disponible:,.2f}")
with k2: st.metric("Capital actual", f"${saldo_actual:,.2f}")
with k3: st.metric("Cuentas por cobrar (futuras)", f"${cxc_futuras:,.2f}")

# -------------------- Gráficas y análisis --------------------
with st.expander("📈 Ver análisis y gráficas", expanded=False):
    st.markdown("### Tendencia mensual (Ingresos vs Gastos vs Utilidad)")
    pnl_m = monthly_pnl(df_ing_reales, df_gas_reales)
    st.altair_chart(chart_line_ing_gas_util_mensual(pnl_m), width="stretch")

    st.markdown("### Top categorías de gasto")
    try:
        df_top = top_gastos_por_categoria(df_gas_reales, top_n=5)
        st.altair_chart(chart_bar_top_gastos(df_top), width="stretch")
    except Exception:
        st.caption("Sin categorías de gasto disponibles.")

    st.markdown("### Flujo de caja acumulado (filtrado)")
    if cash.empty:
        st.info("No hay datos en el período seleccionado.")
    else:
        gc1, gc2 = st.columns([2, 1])
        with gc1:
            st.line_chart(cash.set_index(COL_FECHA)[["Saldo"]], height=280, width="stretch")
        with gc2:
            st.altair_chart(chart_bars_saldo_mensual(cash), width="stretch")


# ============================================================
# CATÁLOGO — Un único expander: crear Clientes y Proyectos
# ============================================================
if st.session_state.get("btn_crear_cliente") or st.session_state.get("btn_crear_proyecto"):
    st.session_state["catalog_force_open"] = True
    st.session_state.setdefault("catalog_scroll_to", True)

current_proj_client = st.session_state.get("cat_proj_cliente")
prev_proj_client = st.session_state.get("catalog_prev_proj_cliente", None)
if current_proj_client != prev_proj_client:
    st.session_state["catalog_prev_proj_cliente"] = current_proj_client
    if current_proj_client:
        st.session_state["catalog_force_open"] = True
        st.session_state.setdefault("catalog_scroll_to", True)

current_proj_emp = st.session_state.get("cat_emp_proj")
if current_proj_emp:
    st.session_state.setdefault("catalog_force_open", True)
    st.session_state.setdefault("catalog_scroll_to", True)

catalog_should_expand = st.session_state.pop("catalog_force_open", False)
scroll_to_catalog = st.session_state.pop("catalog_scroll_to", False)
if st.session_state.pop("catalog_reset_cliente_inputs", False):
    st.session_state.pop("cat_cli_nom", None)
    st.session_state.pop("cat_emp_cliente", None)
if st.session_state.pop("catalog_reset_proyecto_inputs", False):
    st.session_state.pop("cat_proj_nom", None)
    st.session_state.pop("cat_emp_proj", None)
st.markdown('<div id="catalog-anchor"></div>', unsafe_allow_html=True)
if scroll_to_catalog:
    st.markdown(
        """
        <script>
        const anchor = document.getElementById('catalog-anchor');
        if (anchor) {
            anchor.scrollIntoView({behavior: 'smooth', block: 'start'});
        }
        </script>
        """,
        unsafe_allow_html=True,
    )

st.markdown("### Catálogo")
with st.expander("➕ Clientes y Proyectos", expanded=catalog_should_expand):
    # --- Crear Cliente (ID automático) ---
    st.subheader("Crear nuevo cliente")
    colc1, colc2 = st.columns([1, 2])
    with colc1:
        emp_cliente = st.selectbox(
            "Empresa (cliente)",
            EMPRESAS_OPCIONES,
            index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT),
            key="cat_emp_cliente"
        )
    with colc2:
        cli_nom_in = st.text_input("Nombre del cliente", key="cat_cli_nom")

    if st.button("Crear cliente", key="btn_crear_cliente"):
        st.session_state["catalog_force_open"] = True
        st.session_state["catalog_scroll_to"] = True
        if not cli_nom_in.strip():
            st.warning("Debes indicar el nombre del cliente.")
        else:
            with st.spinner("Creando cliente..."):
                dfc_state = st.session_state.get("catalog_clients_df")
                if dfc_state is not None and isinstance(dfc_state, pd.DataFrame) and not dfc_state.empty:
                    dfc = dfc_state.copy()
                else:
                    try:
                        dfc = read_worksheet(client, SHEET_ID, WS_CLIENTES)
                    except APIError as api_err:
                        _handle_gspread_error(api_err, "cargar los clientes existentes")
                        st.stop()
                    except Exception as exc:
                        st.error(f"No se pudo leer la hoja de clientes. {exc}")
                        st.stop()
                dfc = ensure_clientes_columns(dfc)

                new_id = f"C-{uuid.uuid4().hex[:8].upper()}"
                dup = False
                if not dfc.empty:
                    dup = (
                        (dfc[COL_CLI_NOM].astype(str).str.lower() == cli_nom_in.strip().lower())
                        & (dfc[COL_EMP].astype(str).str.upper() == emp_cliente.upper())
                    ).any()
                if dup:
                    st.warning("Ya existe un cliente con ese nombre en la misma empresa.")
                else:
                    new_row = {
                        COL_ROWID: uuid.uuid4().hex,
                        COL_CLI_ID: new_id,
                        COL_CLI_NOM: cli_nom_in.strip(),
                        COL_EMP: emp_cliente,
                    }
                    dfc = pd.concat([dfc, pd.DataFrame([new_row])], ignore_index=True)
                    try:
                        write_worksheet(client, SHEET_ID, WS_CLIENTES, dfc)
                    except APIError as api_err:
                        _handle_gspread_error(api_err, "crear el cliente")
                    except Exception as exc:
                        st.error(f"No se pudo crear el cliente. {exc}")
                    else:
                        _load_clients.clear()
                        projects_df_state = st.session_state.get("catalog_projects_df")
                        _ensure_catalog_data(
                            client,
                            SHEET_ID,
                            force=True,
                            clients_df=dfc,
                            projects_df=projects_df_state if isinstance(projects_df_state, pd.DataFrame) else None,
                        )
                        st.session_state["catalog_reset_cliente_inputs"] = True
                        st.session_state["catalog_force_open"] = True
                        st.session_state["catalog_scroll_to"] = True
                        st.toast(f"Cliente creado: {new_id} — {cli_nom_in.strip()}")
                        st.rerun()

    st.divider()

    # --- Crear Proyecto (asociado a cliente) ---
    st.subheader("Crear nuevo proyecto")
    colp1, colp2 = st.columns([2, 1])
    with colp1:
        cli_sel_id, cli_sel_nom = client_selector(client, SHEET_ID, key="cat_proj")
    with colp2:
        emp_proy = st.selectbox(
            "Empresa (proyecto)",
            EMPRESAS_OPCIONES,
            index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT),
            key="cat_emp_proj"
        )
    proy_nom_in = st.text_input("Nombre del proyecto", key="cat_proj_nom")

    if st.button("Crear proyecto", key="btn_crear_proyecto"):
        st.session_state["catalog_force_open"] = True
        st.session_state["catalog_scroll_to"] = True
        if not proy_nom_in.strip():
            st.warning("Debes indicar el nombre del proyecto.")
        elif not cli_sel_id.strip():
            st.warning("Debes seleccionar un cliente.")
        else:
            with st.spinner("Creando proyecto..."):
                dfp_state = st.session_state.get("catalog_projects_df")
                if dfp_state is not None and isinstance(dfp_state, pd.DataFrame) and not dfp_state.empty:
                    dfp = dfp_state.copy()
                else:
                    try:
                        dfp = read_worksheet(client, SHEET_ID, WS_PROYECTOS)
                    except APIError as api_err:
                        _handle_gspread_error(api_err, "cargar los proyectos existentes")
                        st.stop()
                    except Exception as exc:
                        st.error(f"No se pudo leer la hoja de proyectos. {exc}")
                        st.stop()
                dfp = ensure_proyectos_columns(dfp)

                dup = False
                if not dfp.empty:
                    dup = (
                        (dfp[COL_PROY].astype(str).str.lower() == proy_nom_in.strip().lower())
                        & (dfp[COL_CLI_ID].astype(str) == cli_sel_id)
                        & (dfp[COL_EMP].astype(str).str.upper() == emp_proy.upper())
                    ).any()
                if dup:
                    st.warning("Ya existe un proyecto con ese nombre para ese cliente y empresa.")
                else:
                    new_row = {
                        COL_ROWID: uuid.uuid4().hex,
                        COL_PROY: proy_nom_in.strip(),
                        COL_CLI_ID: cli_sel_id.strip(),
                        COL_CLI_NOM: cli_sel_nom.strip(),
                        COL_EMP: emp_proy,
                    }
                    dfp = pd.concat([dfp, pd.DataFrame([new_row])], ignore_index=True)
                    try:
                        write_worksheet(client, SHEET_ID, WS_PROYECTOS, dfp)
                    except APIError as api_err:
                        _handle_gspread_error(api_err, "crear el proyecto")
                    except Exception as exc:
                        st.error(f"No se pudo crear el proyecto. {exc}")
                    else:
                        _load_projects.clear()
                        clients_df_state = st.session_state.get("catalog_clients_df")
                        _ensure_catalog_data(
                            client,
                            SHEET_ID,
                            force=True,
                            clients_df=clients_df_state if isinstance(clients_df_state, pd.DataFrame) else None,
                            projects_df=dfp,
                        )
                        st.session_state["catalog_reset_proyecto_inputs"] = True
                        st.session_state["catalog_force_open"] = True
                        st.session_state["catalog_scroll_to"] = True
                        st.toast(f"Proyecto creado: {proy_nom_in.strip()} (Cliente: {cli_sel_nom})")
                        st.rerun()


# ============================================================
# INGRESOS — Añadir ingreso (rápido)
# ============================================================
st.markdown("## Ingresos")
st.session_state.setdefault("ing_form_open", False)
with st.expander(
    "Añadir ingreso (rápido)",
    expanded=st.session_state.get("ing_form_open", False),
):
    _prepare_entry_defaults("ing")
    st.session_state["ing_form_open"] = True

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1.1])
    with c1:
        empresa_ing = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT), key="ing_empresa_quick")
    with c2:
        fecha_nueva = st.date_input("Fecha", value=_today(), key="ing_fecha_quick")
    with c3:
        monto_nuevo = st.number_input("Monto", min_value=0.0, step=1.0, key="ing_monto_quick")
    with c4:
        por_cobrar_nuevo = st.selectbox("Por_cobrar", ["No", "Sí"], index=0, key="ing_porcob_quick")

    ing_company_code = (empresa_ing or EMPRESA_DEFAULT).strip().upper()
    client_options = _client_options_for_company(ing_company_code)
    _ensure_client_selection("ing", client_options)

    st.selectbox(
        "Cliente",
        client_options,
        key="ing_cliente_raw",
        on_change=lambda prefix="ing": _on_client_change(prefix),
    )
    project_options = _build_project_options("ing")
    if st.session_state.get("ing_proyecto_raw") not in project_options:
        st.session_state["ing_proyecto_raw"] = project_options[0] if project_options else ""
    st.selectbox(
        "Proyecto",
        project_options,
        key="ing_proyecto_raw",
        on_change=lambda prefix="ing": _on_project_change(prefix),
    )
    desc_nueva = st.text_input("Descripción", key="ing_desc_quick")

    submitted_ing = st.button("Guardar ingreso", type="primary", key="btn_guardar_ing_quick")

    if submitted_ing:
        cliente_id = st.session_state.get("ing_cliente_id", "")
        cliente_nombre = st.session_state.get("ing_cliente_nombre", "")
        proyecto_id = st.session_state.get("ing_proyecto_id", "")
        linked_client_id = st.session_state.get("ing_proyecto_cliente_id")
        linked_client_name = st.session_state.get("ing_proyecto_cliente_nombre")
        if linked_client_id:
            cliente_id = linked_client_id
            cliente_nombre = linked_client_name or cliente_nombre

        hoy_ts = pd.Timestamp(_today())
        rid = uuid.uuid4().hex
        cobrado = "No" if por_cobrar_nuevo == "Sí" else "Sí"
        fecha_cobro = hoy_ts if cobrado == "Sí" else pd.NaT
        nueva = {
            COL_ROWID: rid,
            COL_FECHA: _ts(fecha_nueva),
            COL_MONTO: float(monto_nuevo),
            COL_PROY: (proyecto_id or "").strip(),
            COL_CLI_ID: (cliente_id or "").strip(),
            COL_CLI_NOM: (cliente_nombre or "").strip(),
            COL_EMP: (empresa_ing or EMPRESA_DEFAULT).strip(),
            COL_DESC: (desc_nueva or "").strip(),
            COL_CONC: (desc_nueva or "").strip(),
            COL_POR_COB: por_cobrar_nuevo,
            COL_COB: cobrado,
            COL_FCOBRO: fecha_cobro,
            COL_CAT: "",
            COL_ESC: "Real",
            COL_USER: _current_user(),
        }
        st.session_state.df_ing = pd.concat([st.session_state.df_ing, pd.DataFrame([nueva])], ignore_index=True)
        st.session_state.df_ing = ensure_ingresos_columns(st.session_state.df_ing)
        wrote = safe_write_worksheet(client, SHEET_ID, WS_ING, st.session_state.df_ing, old_df=df_ing_before)
        if wrote:
            st.cache_data.clear()
    _reset_entry_state("ing")
    st.session_state["ing_form_open"] = False
    st.rerun()


# Tabla Ingresos (OCULTANDO "Concepto" en la vista)
st.markdown("### Ingresos (tabla)")
ing_cols_view = [c for c in df_ing_f.columns if c not in (COL_ROWID, COL_ESC, COL_CONC)] + [COL_ROWID]
ing_colcfg = {
    COL_POR_COB: st.column_config.SelectboxColumn(COL_POR_COB, options=["No","Sí"]),
    COL_CAT:     st.column_config.TextColumn(COL_CAT),
    # COL_CONC oculto en la vista
    COL_DESC:    st.column_config.TextColumn(COL_DESC),
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
    COL_USER:   st.column_config.TextColumn(COL_USER, disabled=True),
}
edited_ing = st.data_editor(
    df_ing_f[ing_cols_view], num_rows="dynamic", hide_index=True, width="stretch",
    column_config=ing_colcfg, key="tabla_ingresos"
)

# === BORRADO REAL PRIMERO (INGRESOS) ===
if COL_ROWID not in edited_ing.columns:
    st.warning("No se encontró columna RowID en la tabla de Ingresos; no se pueden borrar filas en Sheets.")
else:
    ids_original = set(df_ing_f[COL_ROWID].astype(str)) if not df_ing_f.empty else set()
    ids_editados = set(edited_ing[COL_ROWID].astype(str)) if not edited_ing.empty else set()
    ids_a_borrar = ids_original - ids_editados
    if ids_a_borrar:
        base_ing = st.session_state.df_ing.copy()
        base_ing = base_ing[~base_ing[COL_ROWID].astype(str).isin(ids_a_borrar)].reset_index(drop=True)
        write_worksheet(client, SHEET_ID, WS_ING, ensure_ingresos_columns(base_ing))
        st.session_state.df_ing = base_ing.copy()
        st.cache_data.clear()
        # refrescar vista filtrada para que sync no "reviva" las filas borradas
        df_ing_f = _filtrar_periodo(st.session_state.df_ing, f_desde, f_hasta)
        if filtro_empresa != "Todas":
            df_ing_f = df_ing_f[df_ing_f[COL_EMP].astype(str).str.upper() == filtro_empresa.upper()]
        if search_q.strip():
            q = search_q.strip().lower()
            def _match_df(df):
                cols = [COL_CLI_NOM, COL_CLI_ID, COL_PROY, COL_DESC, COL_CONC, COL_CAT, COL_EMP]
                tmp = df.copy()
                for c in cols:
                    if c not in tmp.columns: tmp[c] = ""
                    tmp[c] = tmp[c].astype(str).str.lower()
                mask = False
                for c in cols: mask = mask | tmp[c].str.contains(q, na=False)
                return df[mask]
            df_ing_f = _match_df(df_ing_f)

# === ALTAS/EDICIONES (sync normal) ===
sync_cambios(
    edited_df=edited_ing, filtered_df=df_ing_f,
    base_df_key="df_ing", worksheet_name=WS_ING,
    session_state=st.session_state, write_worksheet=write_worksheet,
    client=client, sheet_id=SHEET_ID, id_column=COL_ROWID,
    ensure_columns_fn=ensure_ingresos_columns,
)


# ============================================================
# GASTOS — Añadir gasto (rápido)
# ============================================================

st.markdown("## Gastos")
st.session_state.setdefault("gas_form_open", False)
with st.expander(
    "Añadir gasto (rápido)",
    expanded=st.session_state.get("gas_form_open", False),
):
    _prepare_entry_defaults("gas")
    st.session_state["gas_form_open"] = True

    g1, g2, g3, g4, g5 = st.columns([1, 1, 1, 2, 1])
    with g1:
        empresa_g = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT), key="gas_empresa_quick")
    with g2:
        fecha_g = st.date_input("Fecha", value=_today(), key="gas_fecha_quick")
    with g3:
        categoria_g = st.selectbox(
            "Categoría",
            ["Proyectos", "Gastos fijos", "Gastos operativos", "Oficina"],
            index=0,
            key="gas_categoria_quick",
        )
    with g4:
        monto_g = st.number_input("Monto", min_value=0.0, step=1.0, key="gas_monto_quick")
    with g5:
        por_pagar_nuevo = st.selectbox("Por_pagar", ["No", "Sí"], index=0, key="gas_porpag_quick")

    cliente_id_g = ""
    cliente_nombre_g = ""
    proyecto_id_g = ""
    if categoria_g == "Proyectos":
        gas_company_code = (empresa_g or EMPRESA_DEFAULT).strip().upper()
        client_options = _client_options_for_company(gas_company_code)
        _ensure_client_selection("gas", client_options)
        st.selectbox(
            "Cliente",
            client_options,
            key="gas_cliente_raw",
            on_change=lambda prefix="gas": _on_client_change(prefix),
        )
        project_options_g = _build_project_options("gas")
        if st.session_state.get("gas_proyecto_raw") not in project_options_g:
            st.session_state["gas_proyecto_raw"] = project_options_g[0] if project_options_g else ""
        st.selectbox(
            "Proyecto",
            project_options_g,
            key="gas_proyecto_raw",
            on_change=lambda prefix="gas": _on_project_change(prefix),
        )
        cliente_id_g = st.session_state.get("gas_cliente_id", "")
        cliente_nombre_g = st.session_state.get("gas_cliente_nombre", "")
        proyecto_id_g = st.session_state.get("gas_proyecto_id", "")
        linked_client_id_g = st.session_state.get("gas_proyecto_cliente_id")
        linked_client_name_g = st.session_state.get("gas_proyecto_cliente_nombre")
        if linked_client_id_g:
            cliente_id_g = linked_client_id_g
            cliente_nombre_g = linked_client_name_g or cliente_nombre_g

    desc_g = st.text_input("Descripción", key="gas_desc_quick")
    prov_g = st.text_input("Proveedor", key="gas_proveedor_quick")

    submitted_gas = st.button("Guardar gasto", type="primary", key="btn_guardar_gas_quick")

    if submitted_gas:
        if categoria_g != "Proyectos":
            cliente_id_g = ""
            cliente_nombre_g = ""
            proyecto_id_g = ""
        nueva_g = {
            COL_ROWID: uuid.uuid4().hex,
            COL_FECHA: _ts(fecha_g),
            COL_MONTO: float(monto_g),
            COL_DESC: (desc_g or "").strip(),
            COL_CONC: (desc_g or "").strip(),
            COL_CAT: categoria_g,
            COL_EMP: (empresa_g or EMPRESA_DEFAULT).strip(),
            COL_POR_PAG: por_pagar_nuevo,
            COL_PROY: (proyecto_id_g or "").strip(),
            COL_CLI_ID: (cliente_id_g or "").strip(),
            COL_CLI_NOM: (cliente_nombre_g or "").strip(),
            COL_PROV: (prov_g or "").strip(),
            COL_USER: _current_user(),
        }
        st.session_state.df_gas = pd.concat([st.session_state.df_gas, pd.DataFrame([nueva_g])], ignore_index=True)
        st.session_state.df_gas = ensure_gastos_columns(st.session_state.df_gas)
        wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, st.session_state.df_gas, old_df=df_gas_before)
        if wrote:
            st.cache_data.clear()
    _reset_entry_state("gas")
    st.session_state["gas_form_open"] = False
    st.rerun()



# Tabla Gastos (etiqueta "Descripción" para Concepto)
st.markdown("### Gastos (tabla)")
gas_cols_view = [c for c in df_gas_f.columns if c not in (COL_ROWID, COL_ESC)] + [COL_ROWID]
gas_colcfg = {
    COL_POR_PAG: st.column_config.SelectboxColumn(COL_POR_PAG, options=["No","Sí"]),
    COL_CAT:     st.column_config.SelectboxColumn(
        COL_CAT,
        options=["Proyectos", "Gastos fijos", "Gastos operativos", "Oficina", "Comisiones"],
    ),
    COL_CONC:    st.column_config.TextColumn("Descripción"),
    COL_PROV:    st.column_config.TextColumn("Proveedor"),  # ← NUEVO
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_REF_RID: st.column_config.TextColumn(COL_REF_RID, disabled=True),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
    COL_USER:   st.column_config.TextColumn(COL_USER, disabled=True),
}
# Fuerza un orden amigable: ... Descripción, Proveedor, ...
gas_order = [x for x in [
    COL_FECHA, COL_CONC, COL_PROV, COL_MONTO, COL_CAT, COL_EMP, COL_POR_PAG,
    COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_USER, COL_REF_RID, COL_ROWID
] if x in gas_cols_view]

edited_gas = st.data_editor(
    df_gas_f[gas_cols_view], num_rows="dynamic", hide_index=True, width="stretch",
    column_config=gas_colcfg, key="tabla_gastos",
    column_order=gas_order  # ← NUEVO: asegura que Proveedor quede debajo de Descripción
)
# === BORRADO REAL PRIMERO (GASTOS) ===
if COL_ROWID not in edited_gas.columns:
    st.warning("No se encontró columna RowID en la tabla de Gastos; no se pueden borrar filas en Sheets.")
else:
    ids_original_g = set(df_gas_f[COL_ROWID].astype(str)) if not df_gas_f.empty else set()
    ids_editados_g = set(edited_gas[COL_ROWID].astype(str)) if not edited_gas.empty else set()
    ids_a_borrar_g = ids_original_g - ids_editados_g
    if ids_a_borrar_g:
        base_g = st.session_state.df_gas.copy()
        base_g = base_g[~base_g[COL_ROWID].astype(str).isin(ids_a_borrar_g)].reset_index(drop=True)
        write_worksheet(client, SHEET_ID, WS_GAS, ensure_gastos_columns(base_g))
        st.session_state.df_gas = base_g.copy()
        st.cache_data.clear() 
        # refrescar la vista filtrada después del borrado
        df_gas_f = _filtrar_periodo(st.session_state.df_gas, f_desde, f_hasta)
        if filtro_empresa != "Todas":
            df_gas_f = df_gas_f[df_gas_f[COL_EMP].astype(str).str.upper() == filtro_empresa.upper()]
        if search_q.strip():
            q = search_q.strip().lower()
            def _match_df(df):
                cols = [COL_CLI_NOM, COL_CLI_ID, COL_PROY, COL_DESC, COL_CONC, COL_PROV, COL_CAT, COL_EMP]
                tmp = df.copy()
                for c in cols:
                    if c not in tmp.columns: tmp[c] = ""
                    tmp[c] = tmp[c].astype(str).str.lower()
                mask = False
                for c in cols: mask = mask | tmp[c].str.contains(q, na=False)
                return df[mask]
            df_gas_f = _match_df(df_gas_f)

# === ALTAS/EDICIONES (sync normal) ===
sync_cambios(
    edited_df=edited_gas, filtered_df=df_gas_f,
    base_df_key="df_gas", worksheet_name=WS_GAS,
    session_state=st.session_state, write_worksheet=write_worksheet,
    client=client, sheet_id=SHEET_ID, id_column=COL_ROWID,
    ensure_columns_fn=ensure_gastos_columns,
)


# ============================================================
# COMISIONES 8% AUTOMÁTICAS (tras sincronizar cambios)
# - Para cada ingreso con Por_cobrar == "No", si no existe un gasto
#   con Ref RowID Ingreso = RowID del ingreso, se crea:
#   Monto = 8% del ingreso, Categoria = "Comisiones", Por_pagar = "No".
# ============================================================
def _generar_comisiones_8(client, sheet_id):
    base_ing = st.session_state.df_ing.copy()
    base_gas = st.session_state.df_gas.copy()

    if base_ing.empty:
        return

    # refs de comisiones ya creadas
    existing_refs = set()
    if (not base_gas.empty) and (COL_REF_RID in base_gas.columns):
        existing_refs = set(base_gas[COL_REF_RID].fillna("").astype(str))

    cobrados = base_ing[base_ing[COL_POR_COB].map(_si_no_norm) == "No"].copy()
    if cobrados.empty:
        return

    nuevos = []
    for _, r in cobrados.iterrows():
        rid_ing = str(r.get(COL_ROWID, "")).strip()
        if not rid_ing or rid_ing in existing_refs:
            continue  # ya existe comisión o sin id

        # 🔴 NUEVO: filtrar por empresa (solo RS-SP genera comisión)
        if str(r.get(COL_EMP, "")).strip().upper() != "RS-SP":
            continue

        monto = float(r.get(COL_MONTO, 0.0))
        if monto <= 0:
            continue

        fecha = r.get(COL_FCOBRO) if pd.notna(r.get(COL_FCOBRO)) else r.get(COL_FECHA)
        nuevos.append({
            COL_ROWID: uuid.uuid4().hex,
            COL_FECHA: _ts(fecha),
            COL_MONTO: round(monto * 0.08, 2),
            COL_DESC: f"Comisión 8% de ingreso: {r.get(COL_DESC, '')}",
            COL_CONC: f"Comisión 8% de {str(r.get(COL_DESC, '')).strip()}",
            COL_CAT:  "Comisiones",
            COL_EMP:  r.get(COL_EMP, EMPRESA_DEFAULT),
            COL_POR_PAG: "No",  # gasto real al cobrarse
            COL_PROY: r.get(COL_PROY, ""),
            COL_CLI_ID: r.get(COL_CLI_ID, ""),
            COL_CLI_NOM: r.get(COL_CLI_NOM, ""),
            COL_REF_RID: rid_ing,
            COL_USER: r.get(COL_USER, _current_user()),  # ← NUEVO

        })

    if nuevos:
        base_gas = pd.concat([base_gas, pd.DataFrame(nuevos)], ignore_index=True)
        base_gas = ensure_gastos_columns(base_gas)
        write_worksheet(client, sheet_id, WS_GAS, base_gas)
        st.session_state.df_gas = base_gas

        # 👇 refrescar inmediatamente
        st.cache_data.clear()
        st.toast(f"Se generaron {len(nuevos)} comisiones (8%).")
        st.rerun()


# Ejecutar creación de comisiones (si aplica)
_generar_comisiones_8(client, SHEET_ID)


# ============================================================
# (No hay backup: sección eliminada a petición)
# ============================================================

st.divider()
col_bk1, col_bk2 = st.columns([1, 3])

with col_bk1:
    if st.button("📦 Respaldar ahora", width="stretch"):
        try:
            bk = create_backup_now(creds, SHEET_ID)
            if bk:
                st.success(f"Respaldo creado: {bk.name}")
                # refresca cache y vuelve a renderizar para mostrar el último respaldo actualizado
                st.cache_data.clear()
                st.rerun()
            else:
                st.warning("No se pudo crear respaldo (revisa DRIVE_BACKUP_FOLDER_ID en secrets).")
        except Exception as e:
            st.error(f"No se pudo crear el respaldo: {e}")

with col_bk2:
    name, ts_local = get_last_backup_info(creds)
    if name and ts_local is not None:
        st.caption(f"📦 Último respaldo: **{ts_local.strftime('%Y-%m-%d %H:%M')}** — *{name}*")
    else:
        st.caption("📦 Aún no hay respaldos en la carpeta configurada.")


##import os
##if st.sidebar.checkbox("🔍 Diagnóstico de recursos"):
##    try:
##        import psutil
##        p = psutil.Process(os.getpid())
##        st.sidebar.write("Archivos abiertos:", len(p.open_files()))
##        st.sidebar.write("Conexiones de red:", len(p.connections()))
##        st.sidebar.write("Threads activos:", p.num_threads())
##    except Exception as e:
##        st.sidebar.warning(f"No se pudo leer recursos del sistema ({e})")



# Footer
try:
    st.page_link("Inicio.py", label="⬅️ Volver al Home", icon="🏠")
except Exception:
    try: st.page_link("inicio.py", label="⬅️ Volver al Home", icon="🏠")
    except Exception: st.write("Abre la página principal desde el menú lateral.")
