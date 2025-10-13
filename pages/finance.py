# ================================================
# finance.py
# Finanzas operativas (Ingresos / Gastos)
# - EMPRESA: selector (RS-SP / RIR) en Ingresos y Gastos.
# - Ingresos: Por_cobrar (No/Sí). Solo los NO suman a KPIs/Gráficas.
# - Gastos: Por_pagar (No/Sí). Solo los NO suman a KPIs/Gráficas.
# - Comisiones automáticas: RS-SP genera gasto "Comisiones" al cobrarse.
# - Crear Cliente / Proyecto (rápido) en la misma página (proyecto asociado).
# - Filtro por Empresa + Buscador global + Backup diario.
# - Escenario: **quitado de la UI** (ni filtro ni columnas visibles).
# - KPIs: se quitan Burn rate / Saldo inicial / Runway;
#         se muestran arriba "Capital disponible" y "Capital actual".
# - Actualización inmediata tras guardar/editar (cache clear + rerun).
# ================================================

from __future__ import annotations
import uuid
import streamlit as st
import pandas as pd
from datetime import date, datetime

from sheets import get_client, read_worksheet, write_worksheet  # type: ignore
from charts import (
    chart_bars_saldo_mensual,
    chart_line_ing_gas_util_mensual,
    chart_bar_top_gastos,
)
from core.metrics import kpis_finanzas, monthly_pnl, top_gastos_por_categoria
from core.cashflow import preparar_cashflow

try:
    from core.sync import sync_cambios  # type: ignore
except Exception:
    from sync import sync_cambios  # type: ignore

from entities import client_selector, project_selector, WS_PROYECTOS, WS_CLIENTES  # type: ignore

# -------------------- Constantes --------------------
COL_FECHA   = "Fecha"
COL_DESC    = "Descripcion"
COL_CONC    = "Concepto"
COL_MONTO   = "Monto"
COL_CAT     = "Categoria"
COL_ESC     = "Escenario"        # mantenido en datos, no visible en UI
COL_PROY    = "Proyecto"         # guardamos ProyectoID
COL_CLI_ID  = "ClienteID"
COL_CLI_NOM = "ClienteNombre"
COL_EMP     = "Empresa"          # RS-SP | RIR
COL_COB     = "Cobrado"          # (legacy)
COL_FCOBRO  = "Fecha de cobro"   # (legacy)
COL_ROWID   = "RowID"
COL_REF_RID = "Ref RowID Ingreso"
COL_POR_COB = "Por_cobrar"       # Ingresos: "No"/"Sí"
COL_POR_PAG = "Por_pagar"        # Gastos:   "No"/"Sí"
COL_COM_PCT = "Comision (%)"
COL_COM_FIX = "Comision fija"
COL_COM_GEN = "Comision generada"  # (legacy)

DEFAULT_COMMISSION_PCT = 10.0
EMPRESAS_OPCIONES = ["RS-SP", "RIR"]
EMPRESA_DEFAULT   = "RS-SP"

# -------------------- Helpers --------------------
def _today() -> date: return date.today()
def _ts(x):
    try: return pd.to_datetime(x, errors="coerce")
    except Exception: return pd.NaT
def _is_yes(x) -> bool:
    if isinstance(x, bool): return x
    return str(x).strip().lower() in {"si","sí","sí","yes","y","true","1"}
def _si_no_norm(x) -> str:
    s = str(x).strip().lower()
    return "Sí" if s in {"si","sí","sí","yes","y","true","1"} else "No"
def _to_float(x) -> float:
    try:
        v = float(x); return 0.0 if pd.isna(v) else v
    except Exception: return 0.0
def _ensure_text(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype("string").fillna("")
    return out
def _canon_cols(df: pd.DataFrame) -> pd.DataFrame:
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

def ensure_ingresos_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = _canon_cols(df.copy())
    for col in [
        COL_FECHA, COL_DESC, COL_CONC, COL_MONTO, COL_CAT, COL_ESC,
        COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_COB,
        COL_COB, COL_FCOBRO, COL_COM_PCT, COL_COM_FIX, COL_COM_GEN, COL_ROWID
    ]:
        if col not in out.columns:
            if col in {COL_MONTO, COL_COM_PCT, COL_COM_FIX}: out[col] = 0.0
            elif col in {COL_FECHA, COL_FCOBRO}: out[col] = pd.NaT
            elif col in {COL_EMP}: out[col] = EMPRESA_DEFAULT
            elif col in {COL_POR_COB}: out[col] = "No"
            elif col in {COL_COB, COL_COM_GEN}: out[col] = "No"
            else: out[col] = ""
    out[COL_FECHA] = _ts(out[COL_FECHA]); out[COL_FCOBRO] = _ts(out[COL_FCOBRO])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_COM_PCT] = pd.to_numeric(out[COL_COM_PCT], errors="coerce").fillna(0.0).astype(float)
    out[COL_COM_FIX] = pd.to_numeric(out[COL_COM_FIX], errors="coerce").fillna(0.0).astype(float)
    out[COL_EMP] = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT
    )
    out[COL_POR_COB] = out[COL_POR_COB].map(_si_no_norm)
    out[COL_COB] = out[COL_COB].map(_si_no_norm)
    out = _ensure_text(out, [COL_DESC, COL_CONC, COL_CAT, COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_COB, COL_COB, COL_ROWID])
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out

def ensure_gastos_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = _canon_cols(df.copy())
    for col in [COL_FECHA, COL_CONC, COL_MONTO, COL_CAT, COL_ESC, COL_REF_RID,
                COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_PAG, COL_ROWID]:
        if col not in out.columns:
            if col == COL_MONTO: out[col] = 0.0
            elif col == COL_FECHA: out[col] = pd.NaT
            elif col == COL_EMP: out[col] = EMPRESA_DEFAULT
            elif col == COL_POR_PAG: out[col] = "No"
            else: out[col] = ""
    out[COL_FECHA] = _ts(out[COL_FECHA])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_EMP] = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT
    )
    out[COL_POR_PAG] = out[COL_POR_PAG].map(_si_no_norm)
    out = _ensure_text(out, [COL_CONC, COL_CAT, COL_REF_RID, COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_PAG, COL_ROWID])
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out

def mirror_description_to_concept(df_ing: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    if COL_DESC not in df_ing.columns or COL_CONC not in df_ing.columns: return df_ing, False
    df = df_ing.copy()
    mask = df[COL_DESC].astype("string").fillna("") != df[COL_CONC].astype("string").fillna("")
    if mask.any():
        df.loc[mask, COL_CONC] = df.loc[mask, COL_DESC].astype("string").fillna("")
        return df, True
    return df, False

def calc_comision(row: pd.Series) -> float:
    base = _to_float(row.get(COL_MONTO, 0.0))
    p = _to_float(row.get(COL_COM_PCT, 0.0)) if COL_COM_PCT in row.index else 0.0
    f = _to_float(row.get(COL_COM_FIX, 0.0)) if COL_COM_FIX in row.index else 0.0
    if p == 0 and f == 0: return round(base * (DEFAULT_COMMISSION_PCT / 100.0), 2)
    return round(base * (p / 100.0) + f, 2)

def generate_commission_on_cobro(prev_ing: pd.DataFrame, curr_ing: pd.DataFrame, curr_gas: pd.DataFrame):
    """Comisión automática al cobrarse (Por_cobrar='No') si Empresa=RS-SP."""
    changed_gas = False
    curr_i = curr_ing.set_index(COL_ROWID, drop=False)
    existing_refs = set(curr_gas.get(COL_REF_RID, pd.Series([], dtype=str)).astype(str))
    for rid, row in curr_i.iterrows():
        empresa_val = str(row.get(COL_EMP, EMPRESA_DEFAULT)).upper().strip() or EMPRESA_DEFAULT
        por_cobrar = _si_no_norm(row.get(COL_POR_COB, "No"))
        if empresa_val != "RS-SP" or por_cobrar != "No" or rid in existing_refs:
            continue
        com = calc_comision(row)
        if com <= 0: continue
        gasto = {
            COL_ROWID: uuid.uuid4().hex,
            COL_FECHA: pd.Timestamp(_today()),
            COL_CONC: f"Comisión de {str(row.get(COL_DESC) or row.get(COL_CONC) or '').strip()}",
            COL_MONTO: com,
            COL_CAT: "Comisiones",
            COL_ESC: str(row.get(COL_ESC, "Real")).strip(),
            COL_REF_RID: rid,
            COL_PROY: str(row.get(COL_PROY, "")),
            COL_CLI_ID: str(row.get(COL_CLI_ID, "")),
            COL_CLI_NOM: str(row.get(COL_CLI_NOM, "")),
            COL_EMP: empresa_val,
            COL_POR_PAG: "No",
        }
        curr_gas = pd.concat([curr_gas, pd.DataFrame([gasto])], ignore_index=True)
        changed_gas = True
    return curr_i.reset_index(drop=True), curr_gas.reset_index(drop=True), changed_gas

# ------- Compat selector de proyectos (por cliente) -------
@st.cache_data(ttl=120, show_spinner=False)
def _load_projects_df(_client, sheet_id: str) -> pd.DataFrame:
    df = read_worksheet(_client, sheet_id, WS_PROYECTOS).copy()
    ren = {}
    if "ProyectoID" not in df.columns:
        for c in ["ID","Id","project_id","Proyecto"]:
            if c in df.columns: ren[c] = "ProyectoID"; break
    if "ProyectoNombre" not in df.columns:
        for c in ["Nombre","Name"]:
            if c in df.columns: ren[c] = "ProyectoNombre"; break
    if "ClienteID" not in df.columns:
        for c in ["client_id","IdCliente","ID Cliente","ID_Cliente","Cliente"]:
            if c in df.columns: ren[c] = "ClienteID"; break
    if "ClienteNombre" not in df.columns:
        for c in ["NombreCliente","ClientName"]:
            if c in df.columns: ren[c] = "ClienteNombre"; break
    if ren: df = df.rename(columns=ren)
    for c in ["ProyectoID","ProyectoNombre","ClienteID","ClienteNombre"]:
        if c not in df.columns: df[c] = ""
    return df.drop_duplicates(subset=["ProyectoID"]).reset_index(drop=True)

def project_selector_compat(client, sheet_id: str, *, key: str, allow_client_link: bool=True, selected_client_id: str|None=None):
    from entities import project_selector as _proj_sel  # real
    try:
        return _proj_sel(client, sheet_id, key=key, allow_client_link=allow_client_link, selected_client_id=selected_client_id)
    except TypeError:
        dfp = _load_projects_df(client, sheet_id)
        view = dfp if not selected_client_id else dfp[dfp["ClienteID"].astype(str).str.strip() == str(selected_client_id).strip()]
        opciones = [""] + [f"{row.ProyectoNombre or row.ProyectoID} ▸ {row.ProyectoID}" for _, row in view.iterrows()]
        sel = st.selectbox("Proyecto", opciones, index=0, key=f"{key}_proyecto")
        if not sel: return "", "", None, None
        nombre, _, pid = sel.partition(" ▸ ")
        fila = dfp[dfp["ProyectoID"].astype(str) == pid.strip()].head(1)
        if fila.empty: return pid.strip(), nombre.strip(), None, None
        r = fila.iloc[0]
        return pid.strip(), nombre.strip(), (r.get("ClienteID") or None), (r.get("ClienteNombre") or None)

# -------------------- Página --------------------
st.set_page_config(page_title="Finanzas Operativas", page_icon="📊", layout="wide")
st.markdown("<h1>Finanzas operativas y proyecciones</h1>", unsafe_allow_html=True)

client, creds = get_client()
SHEET_ID = st.secrets["app"]["SHEET_ID"]
WS_ING   = st.secrets["app"]["WS_ING"]
WS_GAS   = st.secrets["app"]["WS_GAS"]

@st.cache_data(ttl=30)
def load_norm(_client, sid: str, ws: str, is_ingresos: bool) -> pd.DataFrame:
    df = read_worksheet(_client, sid, ws)
    return ensure_ingresos_columns(df) if is_ingresos else ensure_gastos_columns(df)

# Carga base
st.session_state.df_ing = load_norm(client, SHEET_ID, WS_ING, True)
st.session_state.df_gas = load_norm(client, SHEET_ID, WS_GAS, False)

# -------------------- Filtros + Buscador + Empresa --------------------
st.markdown("### Filtros")
fc1, fc2, fc3 = st.columns([1, 1, 1.4])
with fc1: f_desde = st.date_input("Desde", value=date(date.today().year, 1, 1))
with fc2: f_hasta = st.date_input("Hasta", value=_today())
with fc3:
    filtro_empresa = st.selectbox("Empresa", options=["Todas"] + EMPRESAS_OPCIONES, index=0)

search_q = st.text_input("🔎 Buscar (cliente, proyecto, descripción, concepto, categoría, empresa)", key="global_search")

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
            if c not in tmp.columns: tmp[c] = ""
            tmp[c] = tmp[c].astype(str).str.lower()
        mask = False
        for c in cols: mask = mask | tmp[c].str.contains(q, na=False)
        return df[mask]
    df_ing_f = _match_df(df_ing_f)
    df_gas_f = _match_df(df_gas_f)

# Reales (excluyen por cobrar / por pagar)
df_ing_reales = df_ing_f[df_ing_f[COL_POR_COB].map(_si_no_norm) == "No"].copy()
df_gas_reales = df_gas_f[df_gas_f[COL_POR_PAG].map(_si_no_norm) == "No"].copy()

# -------------------- KPIs principales --------------------
ing_total = float(df_ing_reales[COL_MONTO].sum()) if COL_MONTO in df_ing_reales.columns else 0.0
gas_total = float(df_gas_reales[COL_MONTO].sum()) if COL_MONTO in df_gas_reales.columns else 0.0
utilidad  = ing_total - gas_total
margen    = (utilidad / ing_total * 100.0) if ing_total > 0 else 0.0

from ui.kpis import render_kpis  # (ya existente en tu app)
render_kpis(ing_total, gas_total, utilidad, margen)

# ---- Flujo y saldo actual (para los KPIs de capital que van aquí) ----
cash = preparar_cashflow(df_ing_reales, df_gas_reales)
if cash.empty:
    saldo_actual = 0.0
else:
    try: saldo_actual = float(cash["Saldo"].iloc[-1])
    except Exception: saldo_actual = 0.0

# KPI: Capital disponible (arriba) + Capital actual (arriba)
colchon_fijo = st.number_input("Colchón fijo (USD)", min_value=0.0, value=15000.0, step=500.0)
cxp_activas = float(df_gas_f[df_gas_f[COL_POR_PAG].map(_si_no_norm) == "Sí"][COL_MONTO].sum()) if not df_gas_f.empty else 0.0
capital_disponible = saldo_actual - colchon_fijo - cxp_activas

k1, k2 = st.columns(2)
with k1: st.metric("Capital disponible para inversión", f"${capital_disponible:,.2f}")
with k2: st.metric("Capital actual", f"${saldo_actual:,.2f}")

# -------------------- Gráficas y análisis --------------------
st.markdown("### Tendencia mensual (Ingresos vs Gastos vs Utilidad)")
pnl_m = monthly_pnl(df_ing_reales, df_gas_reales)
st.altair_chart(chart_line_ing_gas_util_mensual(pnl_m), use_container_width=True)

st.markdown("### Top categorías de gasto")
try:
    df_top = top_gastos_por_categoria(df_gas_reales, top_n=5)
    st.altair_chart(chart_bar_top_gastos(df_top), use_container_width=True)
except Exception:
    st.caption("Sin categorías de gasto disponibles.")

st.markdown("### Flujo de caja acumulado (filtrado)")
if cash.empty:
    st.info("No hay datos en el período seleccionado.")
else:
    gc1, gc2 = st.columns([2, 1])
    with gc1: st.line_chart(cash.set_index(COL_FECHA)[["Saldo"]], height=280, use_container_width=True)
    with gc2: st.altair_chart(chart_bars_saldo_mensual(cash), use_container_width=True)

# Indicador complementario (lo mantenemos)
cxc_futuras = float(df_ing_f[df_ing_f[COL_POR_COB].map(_si_no_norm) == "Sí"][COL_MONTO].sum()) if not df_ing_f.empty else 0.0
st.metric("Cuentas por cobrar (futuras)", f"${cxc_futuras:,.2f}")

# ============================================================
# Maestro: Crear Cliente/Proyecto (arriba de Ingresos)
# ============================================================
with st.expander("➕ Crear cliente / proyecto (rápido)"):
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Nuevo Cliente", divider=True)
        nuevo_cli_nombre = st.text_input("Nombre del cliente", key="new_cli_nom")
        nuevo_cli_id_opt = st.text_input("ID del cliente (opcional)", help="Si lo dejas vacío, se genera automáticamente.", key="new_cli_id")
        if st.button("Crear cliente", key="btn_crear_cliente"):
            if not nuevo_cli_nombre.strip():
                st.warning("Ingresa el nombre del cliente.")
            else:
                df_cli = read_worksheet(client, SHEET_ID, WS_CLIENTES).copy()
                if "ClienteID" not in df_cli.columns:
                    if "ID" in df_cli.columns: df_cli = df_cli.rename(columns={"ID":"ClienteID"})
                    else: df_cli["ClienteID"] = ""
                if "ClienteNombre" not in df_cli.columns:
                    if "Nombre" in df_cli.columns: df_cli = df_cli.rename(columns={"Nombre":"ClienteNombre"})
                    else: df_cli["ClienteNombre"] = ""
                nuevo_id = (nuevo_cli_id_opt or uuid.uuid4().hex[:8]).strip()
                fila = {"ClienteID": nuevo_id, "ClienteNombre": nuevo_cli_nombre.strip()}
                df_cli = pd.concat([df_cli, pd.DataFrame([fila])], ignore_index=True)
                write_worksheet(client, SHEET_ID, WS_CLIENTES, df_cli)
                st.success(f"Cliente creado: {fila['ClienteNombre']} ({fila['ClienteID']})")
                st.cache_data.clear(); st.rerun()

    with c2:
        st.subheader("Nuevo Proyecto", divider=True)
        cli_id_for_proj, cli_nom_for_proj = client_selector(client, SHEET_ID, key="newproj")
        nuevo_proj_nombre = st.text_input("Nombre del proyecto", key="new_proj_nom")
        nuevo_proj_id_opt = st.text_input("ID del proyecto (opcional)", help="Si lo dejas vacío, se genera automáticamente.", key="new_proj_id")
        if st.button("Crear proyecto", key="btn_crear_proyecto"):
            if not cli_id_for_proj:
                st.warning("Selecciona el cliente al que se asociará el proyecto.")
            elif not nuevo_proj_nombre.strip():
                st.warning("Ingresa el nombre del proyecto.")
            else:
                df_proj = read_worksheet(client, SHEET_ID, WS_PROYECTOS).copy()
                if "ProyectoID" not in df_proj.columns:
                    if "ID" in df_proj.columns: df_proj = df_proj.rename(columns={"ID":"ProyectoID"})
                    elif "Proyecto" in df_proj.columns: df_proj = df_proj.rename(columns={"Proyecto":"ProyectoID"})
                    else: df_proj["ProyectoID"] = ""
                if "ProyectoNombre" not in df_proj.columns:
                    if "Nombre" in df_proj.columns: df_proj = df_proj.rename(columns={"Nombre":"ProyectoNombre"})
                    else: df_proj["ProyectoNombre"] = ""
                if "ClienteID" not in df_proj.columns: df_proj["ClienteID"] = ""
                if "ClienteNombre" not in df_proj.columns: df_proj["ClienteNombre"] = ""
                nuevo_pid = (nuevo_proj_id_opt or uuid.uuid4().hex[:8]).strip()
                fila = {"ProyectoID": nuevo_pid, "ProyectoNombre": nuevo_proj_nombre.strip(),
                        "ClienteID": cli_id_for_proj.strip(), "ClienteNombre": (cli_nom_for_proj or "").strip()}
                df_proj = pd.concat([df_proj, pd.DataFrame([fila])], ignore_index=True)
                write_worksheet(client, SHEET_ID, WS_PROYECTOS, df_proj)
                st.success(f"Proyecto creado: {fila['ProyectoNombre']} ({fila['ProyectoID']})")
                st.cache_data.clear(); st.rerun()

# ============================================================
# INGRESOS — Añadir ingreso (rápido)
# ============================================================
st.markdown("## Ingresos")
st.markdown("### Añadir ingreso (rápido)")

cliente_id, cliente_nombre = client_selector(client, SHEET_ID, key="ing")
def _project_selector_ing():
    try:
        from entities import project_selector as _proj_sel
        return _proj_sel(client, SHEET_ID, key="ing", allow_client_link=True, selected_client_id=cliente_id or None)
    except TypeError:
        return project_selector_compat(client, SHEET_ID, key="ing", allow_client_link=True, selected_client_id=cliente_id or None)

proyecto_id, proyecto_nom, cli_id_from_proj, cli_nom_from_proj = _project_selector_ing()
if cli_id_from_proj:
    cliente_id = cli_id_from_proj; cliente_nombre = cli_nom_from_proj or cliente_nombre

c1, c2, c3, c4 = st.columns([1, 1, 1, 1.1])
with c1: empresa_ing = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT), key="ing_empresa_quick")
with c2: fecha_nueva = st.date_input("Fecha", value=_today(), key="ing_fecha_quick")
with c3: monto_nuevo = st.number_input("Monto", min_value=0.0, step=1.0, key="ing_monto_quick")
with c4: por_cobrar_nuevo = st.selectbox("Por_cobrar", ["No","Sí"], index=0, key="ing_porcob_quick")
desc_nueva = st.text_input("Descripción", key="ing_desc_quick")

if st.button("Guardar ingreso", type="primary", key="btn_guardar_ing_quick"):
    hoy_ts = pd.Timestamp(_today()); rid = uuid.uuid4().hex
    nueva = {
        COL_ROWID: rid, COL_FECHA: _ts(fecha_nueva), COL_MONTO: float(monto_nuevo),
        COL_PROY: (proyecto_id or "").strip(), COL_CLI_ID: (cliente_id or "").strip(),
        COL_CLI_NOM: (cliente_nombre or "").strip(), COL_EMP: (empresa_ing or EMPRESA_DEFAULT).strip(),
        COL_DESC: (desc_nueva or "").strip(), COL_CONC: (desc_nueva or "").strip(),
        COL_POR_COB: por_cobrar_nuevo, COL_COB: "Sí", COL_FCOBRO: hoy_ts, COL_CAT: "", COL_ESC: "Real",
    }
    st.session_state.df_ing = pd.concat([st.session_state.df_ing, pd.DataFrame([nueva])], ignore_index=True)
    if por_cobrar_nuevo == "No" and (nueva[COL_EMP].upper() == "RS-SP"):
        com = calc_comision(pd.Series(nueva))
        if com > 0:
            gasto = {
                COL_ROWID: uuid.uuid4().hex, COL_FECHA: hoy_ts,
                COL_CONC: f"Comisión de {nueva[COL_DESC]}", COL_MONTO: com, COL_CAT: "Comisiones", COL_ESC: "Real",
                COL_REF_RID: rid, COL_PROY: nueva[COL_PROY], COL_CLI_ID: nueva[COL_CLI_ID], COL_CLI_NOM: nueva[COL_CLI_NOM],
                COL_EMP: nueva[COL_EMP], COL_POR_PAG: "No",
            }
            st.session_state.df_gas = pd.concat([st.session_state.df_gas, pd.DataFrame([gasto])], ignore_index=True)
    st.session_state.df_ing = ensure_ingresos_columns(st.session_state.df_ing)
    st.session_state.df_gas = ensure_gastos_columns(st.session_state.df_gas)
    write_worksheet(client, SHEET_ID, WS_ING, st.session_state.df_ing)
    write_worksheet(client, SHEET_ID, WS_GAS, st.session_state.df_gas)
    st.cache_data.clear(); st.rerun()

# Tabla Ingresos (sin Estado, sin Escenario)
st.markdown("### Ingresos (tabla)")
ing_cols_view = [c for c in df_ing_f.columns if c not in (COL_ROWID, COL_ESC)] + [COL_ROWID]
ing_colcfg = {
    COL_POR_COB: st.column_config.SelectboxColumn(COL_POR_COB, options=["No","Sí"]),
    COL_CAT:     st.column_config.TextColumn(COL_CAT),
    COL_CONC:    st.column_config.TextColumn(COL_CONC),
    COL_DESC:    st.column_config.TextColumn(COL_DESC),
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
}
edited_ing = st.data_editor(
    df_ing_f[ing_cols_view], num_rows="dynamic", hide_index=True, use_container_width=True,
    column_config=ing_colcfg, key="tabla_ingresos"
)

_ing_snapshot_before = st.session_state.df_ing.copy(deep=True)
sync_cambios(
    edited_df=edited_ing, filtered_df=df_ing_f,
    base_df_key="df_ing", worksheet_name=WS_ING,
    session_state=st.session_state, write_worksheet=write_worksheet,
    client=client, sheet_id=SHEET_ID, id_column=COL_ROWID,
    ensure_columns_fn=ensure_ingresos_columns,
)
prev_ing = st.session_state.df_ing.copy(deep=True)
prev_gas = st.session_state.df_gas.copy(deep=True)
new_ing, _ = mirror_description_to_concept(prev_ing)
new_ing2, new_gas2, ch_g = generate_commission_on_cobro(prev_ing=ensure_ingresos_columns(prev_ing),
                                                        curr_ing=ensure_ingresos_columns(new_ing),
                                                        curr_gas=ensure_gastos_columns(prev_gas))
applied_any = False
if not new_ing2.equals(prev_ing):
    write_worksheet(client, SHEET_ID, WS_ING, new_ing2); st.session_state.df_ing = new_ing2; applied_any = True
if ch_g:
    write_worksheet(client, SHEET_ID, WS_GAS, new_gas2); st.session_state.df_gas = new_gas2; applied_any = True
if not _ing_snapshot_before.equals(st.session_state.df_ing): applied_any = True
if applied_any: st.cache_data.clear(); st.rerun()

# ============================================================
# GASTOS — Añadir gasto (rápido)
# ============================================================
st.markdown("## Gastos")
st.markdown("### Añadir gasto (rápido)")

g1, g2, g3, g4, g5 = st.columns([1, 1, 1, 2, 1])
with g1: empresa_g = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT), key="gas_empresa_quick")
with g2: fecha_g = st.date_input("Fecha", value=_today(), key="gas_fecha_quick")
with g3: categoria_g = st.selectbox("Categoría", ["Proyectos", "Gastos fijos"], index=0, key="gas_categoria_quick")
with g4: monto_g = st.number_input("Monto", min_value=0.0, step=1.0, key="gas_monto_quick")
with g5: por_pagar_nuevo = st.selectbox("Por_pagar", ["No","Sí"], index=0, key="gas_porpag_quick")

cliente_id_g = ""; cliente_nombre_g = ""; proyecto_id_g = ""; proyecto_nom_g = ""
if categoria_g == "Proyectos":
    cliente_id_g, cliente_nombre_g = client_selector(client, SHEET_ID, key="gas")
    try:
        from entities import project_selector as _proj_sel
        proyecto_id_g, proyecto_nom_g, cli_id_from_proj_g, cli_nom_from_proj_g = _proj_sel(client, SHEET_ID, key="gas", allow_client_link=True, selected_client_id=cliente_id_g or None)
    except TypeError:
        proyecto_id_g, proyecto_nom_g, cli_id_from_proj_g, cli_nom_from_proj_g = project_selector_compat(client, SHEET_ID, key="gas", allow_client_link=True, selected_client_id=cliente_id_g or None)
    if cli_id_from_proj_g:
        cliente_id_g = cli_id_from_proj_g; cliente_nombre_g = cli_nom_from_proj_g or cliente_nombre_g

desc_g = st.text_input("Descripción", key="gas_desc_quick")

if st.button("Guardar gasto", type="primary", key="btn_guardar_gas_quick"):
    nueva_g = {
        COL_ROWID: uuid.uuid4().hex, COL_FECHA: _ts(fecha_g), COL_MONTO: float(monto_g),
        COL_DESC: (desc_g or "").strip(), COL_CONC: (desc_g or "").strip(),
        COL_CAT: categoria_g, COL_EMP: (empresa_g or EMPRESA_DEFAULT).strip(),
        COL_POR_PAG: por_pagar_nuevo,
    }
    if categoria_g == "Proyectos":
        nueva_g[COL_PROY] = (proyecto_id_g or "").strip()
        nueva_g[COL_CLI_ID] = (cliente_id_g or "").strip()
        nueva_g[COL_CLI_NOM] = (cliente_nombre_g or "").strip()
    else:
        nueva_g[COL_PROY] = ""; nueva_g[COL_CLI_ID] = ""; nueva_g[COL_CLI_NOM] = ""
    st.session_state.df_gas = pd.concat([st.session_state.df_gas, pd.DataFrame([nueva_g])], ignore_index=True)
    st.session_state.df_gas = ensure_gastos_columns(st.session_state.df_gas)
    write_worksheet(client, SHEET_ID, WS_GAS, st.session_state.df_gas)
    st.cache_data.clear(); st.rerun()

# Tabla Gastos (sin Estado, sin Escenario)
st.markdown("### Gastos (tabla)")
gas_cols_view = [c for c in df_gas_f.columns if c not in (COL_ROWID, COL_ESC)] + [COL_ROWID]
gas_colcfg = {
    COL_POR_PAG: st.column_config.SelectboxColumn(COL_POR_PAG, options=["No","Sí"]),
    COL_CAT:     st.column_config.TextColumn(COL_CAT),
    COL_CONC:    st.column_config.TextColumn(COL_CONC),
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_REF_RID: st.column_config.TextColumn(COL_REF_RID, disabled=True),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
}
edited_gas = st.data_editor(
    df_gas_f[gas_cols_view], num_rows="dynamic", hide_index=True, use_container_width=True,
    column_config=gas_colcfg, key="tabla_gastos"
)
_gas_snapshot_before = st.session_state.df_gas.copy(deep=True)
sync_cambios(
    edited_df=edited_gas, filtered_df=df_gas_f,
    base_df_key="df_gas", worksheet_name=WS_GAS,
    session_state=st.session_state, write_worksheet=write_worksheet,
    client=client, sheet_id=SHEET_ID, id_column=COL_ROWID,
    ensure_columns_fn=ensure_gastos_columns,
)
if not _gas_snapshot_before.equals(st.session_state.df_gas):
    st.cache_data.clear(); st.rerun()

# ============================================================
# Backup diario (tabs nuevas en el mismo Sheet)
# ============================================================
st.divider()
st.markdown("### Respaldo")
st.caption("Crea o actualiza una copia de seguridad diaria en nuevas pestañas del mismo Google Sheet.")
if st.button("💾 Backup a Drive (diario)", type="secondary", help="Genera INGRESOS_backup_YYYYMMDD y GASTOS_backup_YYYYMMDD"):
    fecha_tag = datetime.now().strftime("%Y%m%d")
    ing_bak_name = f"{WS_ING}_backup_{fecha_tag}"
    gas_bak_name = f"{WS_GAS}_backup_{fecha_tag}"
    try:
        write_worksheet(client, SHEET_ID, ing_bak_name, st.session_state.df_ing)
        write_worksheet(client, SHEET_ID, gas_bak_name, st.session_state.df_gas)
        st.success(f"Backup actualizado: {ing_bak_name} y {gas_bak_name}")
    except Exception as e:
        st.error(f"No se pudo crear el backup: {e}")

st.divider()
try:
    st.page_link("Inicio.py", label="⬅️ Volver al Home", icon="🏠")
except Exception:
    try: st.page_link("inicio.py", label="⬅️ Volver al Home", icon="🏠")
    except Exception: st.write("Abre la página principal desde el menú lateral.")
