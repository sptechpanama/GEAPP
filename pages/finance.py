# ================================================
# finance.py
# Finanzas operativas (Ingresos / Gastos)
# - Borrado real en Sheets
# - Backup automático en archivo aparte (Drive/Respaldo)
# - Gastos con Cliente/Proyecto (cuando Categoría=Proyectos)
# - Ingresos: ocultar "Concepto" en la tabla (queda solo "Descripcion")
# - Catálogo: Un único expander para crear Clientes y Proyectos (ID auto)
# ================================================

from __future__ import annotations
import uuid, time
import streamlit as st
import pandas as pd
from datetime import date

from sheets import get_client, read_worksheet, write_worksheet
from charts import (
    chart_bars_saldo_mensual,
    chart_line_ing_gas_util_mensual,
    chart_bar_top_gastos,
)
from core.metrics import kpis_finanzas, monthly_pnl, top_gastos_por_categoria
from core.cashflow import preparar_cashflow
try:
    from core.sync import sync_cambios
except Exception:
    from sync import sync_cambios

from entities import client_selector, project_selector, WS_PROYECTOS, WS_CLIENTES

# === Drive API para backup (archivo aparte) ===
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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
        COL_COB, COL_FCOBRO, COL_ROWID
    ]:
        if col not in out.columns:
            if col in {COL_MONTO}: out[col] = 0.0
            elif col in {COL_FECHA, COL_FCOBRO}: out[col] = pd.NaT
            elif col in {COL_EMP}: out[col] = EMPRESA_DEFAULT
            elif col in {COL_POR_COB, COL_COB}: out[col] = "No"
            else: out[col] = ""
    out[COL_FECHA] = _ts(out[COL_FECHA]); out[COL_FCOBRO] = _ts(out[COL_FCOBRO])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_EMP]   = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT
    )
    out[COL_POR_COB] = out[COL_POR_COB].map(_si_no_norm)
    out[COL_COB]     = out[COL_COB].map(_si_no_norm)
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
    out[COL_EMP]   = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT
    )
    out[COL_POR_PAG] = out[COL_POR_PAG].map(_si_no_norm)
    out = _ensure_text(out, [COL_CONC, COL_CAT, COL_REF_RID, COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_PAG, COL_ROWID])
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


# -------------------- Página --------------------
st.set_page_config(page_title="Finanzas Operativas", page_icon="📊", layout="wide")
st.markdown("<h1>Finanzas operativas y proyecciones</h1>", unsafe_allow_html=True)

client, creds = get_client()  # tu get_client debe devolver (gspread_client, creds)
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

# === Firmas para detectar cambios (para backup) ===
_sig_ing_before = st.session_state.df_ing.to_csv(index=False) if not st.session_state.df_ing.empty else ""
_sig_gas_before = st.session_state.df_gas.to_csv(index=False) if not st.session_state.df_gas.empty else ""


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


# ============================================================
# CATÁLOGO — Un único expander: crear Clientes y Proyectos
# ============================================================
st.markdown("### Catálogo")
with st.expander("➕ Clientes y Proyectos"):
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
        if not cli_nom_in.strip():
            st.warning("Debes indicar el nombre del cliente.")
        else:
            try:
                dfc = read_worksheet(client, SHEET_ID, WS_CLIENTES)
            except Exception:
                dfc = pd.DataFrame()
            dfc = ensure_clientes_columns(dfc)

            new_id = f"C-{uuid.uuid4().hex[:8].upper()}"  # <-- ID generado siempre
            # Evitar duplicados por Nombre+Empresa
            dup = False
            if not dfc.empty:
                dup = ((dfc[COL_CLI_NOM].astype(str).str.lower()==cli_nom_in.strip().lower()) &
                       (dfc[COL_EMP].astype(str).str.upper()==emp_cliente.upper())).any()
            if dup:
                st.warning("Ya existe un cliente con ese nombre en la misma empresa.")
            else:
                new_row = {
                    COL_ROWID: uuid.uuid4().hex,
                    COL_CLI_ID: new_id,
                    COL_CLI_NOM: cli_nom_in.strip(),
                    COL_EMP: emp_cliente
                }
                dfc = pd.concat([dfc, pd.DataFrame([new_row])], ignore_index=True)
                write_worksheet(client, SHEET_ID, WS_CLIENTES, dfc)
                st.toast(f"Cliente creado: {new_id} — {cli_nom_in.strip()}")
                st.rerun()  # refrescar inmediatamente los selectores

    st.divider()

    # --- Crear Proyecto (asociado a cliente) ---
    st.subheader("Crear nuevo proyecto")
    colp1, colp2 = st.columns([2, 1])
    with colp1:
        # Seleccionar cliente existente (reusa tu selector)
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
        if not proy_nom_in.strip():
            st.warning("Debes indicar el nombre del proyecto.")
        elif not cli_sel_id.strip():
            st.warning("Debes seleccionar un cliente.")
        else:
            try:
                dfp = read_worksheet(client, SHEET_ID, WS_PROYECTOS)
            except Exception:
                dfp = pd.DataFrame()
            dfp = ensure_proyectos_columns(dfp)

            # Evitar duplicados por (Proyecto + Cliente + Empresa)
            dup = False
            if not dfp.empty:
                dup = ((dfp[COL_PROY].astype(str).str.lower()==proy_nom_in.strip().lower()) &
                       (dfp[COL_CLI_ID].astype(str)==cli_sel_id) &
                       (dfp[COL_EMP].astype(str).str.upper()==emp_proy.upper())).any()
            if dup:
                st.warning("Ya existe un proyecto con ese nombre para ese cliente y empresa.")
            else:
                new_row = {
                    COL_ROWID: uuid.uuid4().hex,
                    COL_PROY: proy_nom_in.strip(),
                    COL_CLI_ID: cli_sel_id.strip(),
                    COL_CLI_NOM: cli_sel_nom.strip(),
                    COL_EMP: emp_proy
                }
                dfp = pd.concat([dfp, pd.DataFrame([new_row])], ignore_index=True)
                write_worksheet(client, SHEET_ID, WS_PROYECTOS, dfp)
                st.toast(f"Proyecto creado: {proy_nom_in.strip()} (Cliente: {cli_sel_nom})")
                st.rerun()  # refrescar inmediatamente los selectores


# ============================================================
# INGRESOS — Añadir ingreso (rápido)
# ============================================================
st.markdown("## Ingresos")
st.markdown("### Añadir ingreso (rápido)")

cliente_id, cliente_nombre = client_selector(client, SHEET_ID, key="ing")
proyecto_id, proyecto_nom, cli_id_from_proj, cli_nom_from_proj = project_selector(
    client, SHEET_ID, key="ing", allow_client_link=True, selected_client_id=cliente_id or None
)
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
    st.session_state.df_ing = ensure_ingresos_columns(st.session_state.df_ing)
    write_worksheet(client, SHEET_ID, WS_ING, st.session_state.df_ing)
    st.cache_data.clear(); st.rerun()

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
}
edited_ing = st.data_editor(
    df_ing_f[ing_cols_view], num_rows="dynamic", hide_index=True, use_container_width=True,
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
st.markdown("### Añadir gasto (rápido)")

g1, g2, g3, g4, g5 = st.columns([1, 1, 1, 2, 1])
with g1: empresa_g = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT), key="gas_empresa_quick")
with g2: fecha_g = st.date_input("Fecha", value=_today(), key="gas_fecha_quick")
with g3: categoria_g = st.selectbox("Categoría", ["Proyectos", "Gastos fijos"], index=0, key="gas_categoria_quick")
with g4: monto_g = st.number_input("Monto", min_value=0.0, step=1.0, key="gas_monto_quick")
with g5: por_pagar_nuevo = st.selectbox("Por_pagar", ["No","Sí"], index=0, key="gas_porpag_quick")

# Cliente/Proyecto SOLO si es categoría Proyectos
cliente_id_g = ""; cliente_nombre_g = ""; proyecto_id_g = ""; proyecto_nom_g = ""
if categoria_g == "Proyectos":
    cliente_id_g, cliente_nombre_g = client_selector(client, SHEET_ID, key="gas")
    proyecto_id_g, proyecto_nom_g, cli_id_from_proj_g, cli_nom_from_proj_g = project_selector(
        client, SHEET_ID, key="gas", allow_client_link=True, selected_client_id=cliente_id_g or None
    )
    if cli_id_from_proj_g:
        cliente_id_g = cli_id_from_proj_g; cliente_nombre_g = cli_nom_from_proj_g or cliente_nombre_g

desc_g = st.text_input("Descripción", key="gas_desc_quick")

if st.button("Guardar gasto", type="primary", key="btn_guardar_gas_quick"):
    nueva_g = {
        COL_ROWID: uuid.uuid4().hex, COL_FECHA: _ts(fecha_g), COL_MONTO: float(monto_g),
        COL_DESC: (desc_g or "").strip(), COL_CONC: (desc_g or "").strip(),
        COL_CAT: categoria_g, COL_EMP: (empresa_g or EMPRESA_DEFAULT).strip(),
        COL_POR_PAG: por_pagar_nuevo,
        COL_PROY: (proyecto_id_g or "").strip(),
        COL_CLI_ID: (cliente_id_g or "").strip(),
        COL_CLI_NOM: (cliente_nombre_g or "").strip(),
    }
    st.session_state.df_gas = pd.concat([st.session_state.df_gas, pd.DataFrame([nueva_g])], ignore_index=True)
    st.session_state.df_gas = ensure_gastos_columns(st.session_state.df_gas)
    write_worksheet(client, SHEET_ID, WS_GAS, st.session_state.df_gas)
    st.cache_data.clear(); st.rerun()

# Tabla Gastos (etiqueta "Descripción" para Concepto)
st.markdown("### Gastos (tabla)")
gas_cols_view = [c for c in df_gas_f.columns if c not in (COL_ROWID, COL_ESC)] + [COL_ROWID]
gas_colcfg = {
    COL_POR_PAG: st.column_config.SelectboxColumn(COL_POR_PAG, options=["No","Sí"]),
    COL_CAT:     st.column_config.TextColumn(COL_CAT),
    COL_CONC:    st.column_config.TextColumn("Descripción"),  # ← solo etiqueta visible
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_REF_RID: st.column_config.TextColumn(COL_REF_RID, disabled=True),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
}
edited_gas = st.data_editor(
    df_gas_f[gas_cols_view], num_rows="dynamic", hide_index=True, use_container_width=True,
    column_config=gas_colcfg, key="tabla_gastos"
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
        # refrescar la vista filtrada después del borrado
        df_gas_f = _filtrar_periodo(st.session_state.df_gas, f_desde, f_hasta)
        if filtro_empresa != "Todas":
            df_gas_f = df_gas_f[df_gas_f[COL_EMP].astype(str).str.upper() == filtro_empresa.upper()]
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
# BACKUP AUTOMÁTICO A ARCHIVO APARTE (Drive/Respaldo)
# - solo si hubo cambios reales (firmas before/after)
# - solo si pasaron >= 3 días desde el último backup
# - limpia >30 días y máx. 10 archivos
# ============================================================
def _drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _get_parent_id(drive, file_id: str) -> str:
    meta = drive.files().get(fileId=file_id, fields="parents").execute()
    parents = meta.get("parents", [])
    if parents:
        return parents[0]
    root = drive.files().get(fileId="root", fields="id").execute()
    return root["id"]

def _get_or_create_folder(drive, parent_id: str, name: str) -> str:
    q = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name)", pageSize=50).execute()
    arr = res.get("files", [])
    if arr:
        return arr[0]["id"]
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    f = drive.files().create(body=body, fields="id").execute()
    return f["id"]

def _list_backups(drive, folder_id: str) -> list:
    q = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    res = drive.files().list(q=q, fields="files(id,name,createdTime)", orderBy="createdTime", pageSize=1000).execute()
    return res.get("files", [])

def _copy_spreadsheet(drive, source_sheet_id: str, dst_name: str, folder_id: str) -> str:
    body = {"name": dst_name, "parents": [folder_id], "mimeType": "application/vnd.google-apps.spreadsheet"}
    out = drive.files().copy(fileId=source_sheet_id, fields="id,name,createdTime", body=body).execute()
    return out["id"]

def _delete_file(drive, file_id: str):
    drive.files().delete(fileId=file_id).execute()

def _latest_backup_info(backups: list):
    if not backups:
        return None, "—"
    last = backups[-1]
    dt = pd.to_datetime(last["createdTime"], errors="coerce")
    if pd.isna(dt):
        return None, "—"
    try:
        human = dt.tz_convert("America/Panama").strftime("%Y-%m-%d %H:%M")
    except Exception:
        human = dt.strftime("%Y-%m-%d %H:%M")
    return dt.timestamp(), human

def _should_backup(last_epoch: float|None, min_days=3) -> bool:
    if last_epoch is None:
        return True
    return (time.time() - last_epoch) >= (min_days * 86400)

def _cleanup_backups(drive, folder_id: str, keep_days=30, max_files=10):
    files = _list_backups(drive, folder_id)
    now = time.time()
    # antigüedad
    for f in files:
        dt = pd.to_datetime(f["createdTime"], errors="coerce")
        if pd.isna(dt):
            continue
        if (now - dt.timestamp()) > keep_days * 86400:
            _delete_file(drive, f["id"])
    # limitar a máx. N
    files2 = _list_backups(drive, folder_id)
    if len(files2) > max_files:
        to_del = files2[: len(files2) - max_files]
        for f in to_del:
            _delete_file(drive, f["id"])

# Comparar firmas
_sig_ing_after = st.session_state.df_ing.to_csv(index=False) if not st.session_state.df_ing.empty else ""
_sig_gas_after = st.session_state.df_gas.to_csv(index=False) if not st.session_state.df_gas.empty else ""
hubo_cambios = (_sig_ing_after != _sig_ing_before) or (_sig_gas_after != _sig_gas_before)

# Ejecutar backup si corresponde
try:
    drive = _drive_service(creds)
    parent_id = _get_parent_id(drive, SHEET_ID)
    respaldo_folder_id = _get_or_create_folder(drive, parent_id, "Respaldo")

    backups = _list_backups(drive, respaldo_folder_id)
    last_epoch, last_human = _latest_backup_info(backups)

    if hubo_cambios and _should_backup(last_epoch, min_days=3):
        ts = pd.Timestamp.now(tz="UTC").tz_convert("America/Panama").strftime("%Y-%m-%d_%H-%M")
        _copy_spreadsheet(drive, SHEET_ID, f"GEAPP_backup_{ts}", respaldo_folder_id)
        _cleanup_backups(drive, respaldo_folder_id, keep_days=30, max_files=10)
        backups = _list_backups(drive, respaldo_folder_id)
        last_epoch, last_human = _latest_backup_info(backups)

    st.markdown("---")
    st.caption(f"🗂 Último respaldo: {last_human if last_human else '—'}")

except HttpError as e:
    st.markdown("---")
    st.warning(f"No se pudo crear/consultar respaldo en Drive: {e}")
except Exception as e:
    st.markdown("---")
    st.warning(f"No se pudo ejecutar el backup: {e}")

# Footer
try:
    st.page_link("Inicio.py", label="⬅️ Volver al Home", icon="🏠")
except Exception:
    try: st.page_link("inicio.py", label="⬅️ Volver al Home", icon="🏠")
    except Exception: st.write("Abre la página principal desde el menú lateral.")
