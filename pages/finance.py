# ================================================
# finance.py
# Finanzas operativas (Ingresos / Gastos)
# - Borrado real en Sheets
# - Gastos con Cliente/Proyecto (cuando Categoría=Proyectos)
# - Ingresos: ocultar "Concepto" en la tabla (queda solo "Descripcion")
# - Catálogo: Un único expander para crear Clientes y Proyectos (ID auto)
# ================================================

from __future__ import annotations
import json, uuid, time
import streamlit as st
import streamlit.components.v1 as components
from ui.theme import apply_global_theme
st.set_page_config(page_title="Finanzas Operativas", page_icon="📊", layout="wide")
apply_global_theme()
import pandas as pd
from datetime import date


def _safe_rerun() -> None:
    rerun = getattr(st, "rerun", None)
    if callable(rerun):
        rerun()
        return
    legacy = getattr(st, "experimental_rerun", None)
    if callable(legacy):
        legacy()

from sheets import get_client, read_worksheet, write_worksheet
from services.backups import debug_sa_quota
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
COL_FPAGO   = "Fecha esperada de pago"
COL_FCOBRO_REAL = "Fecha real de cobro"
COL_FPAGO_REAL = "Fecha real de pago"
COL_REC     = "Recurrente"
COL_REC_PER = "Periodo recurrencia"
COL_REC_REG = "Regla fecha recurrencia"
COL_REC_DUR = "Duracion recurrencia"
COL_REC_HASTA = "Recurrencia hasta fecha"
COL_REC_CANT = "Recurrencia cantidad periodos"
COL_ROWID   = "RowID"
COL_REF_RID = "Ref RowID Ingreso"
COL_POR_COB = "Por_cobrar"        # Ingresos: "No"/"Sí"
COL_POR_PAG = "Por_pagar"         # Gastos:   "No"/"Sí"
COL_PROV    = "Proveedor"         # Gastos: proveedor del gasto
COL_USER  = "Usuario"
COL_ING_DET = "Detalle ingreso"
COL_ING_NAT = "Naturaleza ingreso"
COL_TRAT_BAL_ING = "Tratamiento balance ingreso"
COL_CTP_TIPO = "Tipo contraparte"
COL_CTP_NOMBRE = "Contraparte"
COL_COBRO_REAL_MONTO = "Monto real cobrado"
COL_ING_PARTIALS = "Detalle cobros parciales"
COL_GAS_SUB = "Subclasificacion gerencial"
COL_GAS_DET = "Detalle gasto"
COL_TRAT_BAL_GAS = "Tratamiento balance gasto"
COL_PAGO_REAL_MONTO = "Monto real pagado"
COL_GAS_PARTIALS = "Detalle pagos parciales"
COL_PREPAGO_MESES = "Plazo prepago meses"
COL_PREPAGO_FEC_INI = "Fecha inicio prepago"
COL_INV_MOV = "Movimiento inventario"
COL_INV_ITEM = "Item inventario"
COL_AF_TOGGLE = "Activo fijo"
COL_AF_TIPO = "Tipo activo fijo"
COL_AF_VIDA = "Vida util activo anios"
COL_AF_FEC_INI = "Fecha inicio activo"
COL_AF_VAL_RES = "Valor residual activo"
COL_AF_DEP_TOGGLE = "Depreciar amortizar"
COL_AF_DEP_MENSUAL = "Depreciacion mensual"
COL_FIN_TOGGLE = "Financiamiento"
COL_FIN_TIPO = "Tipo financiamiento"
COL_FIN_MONTO = "Monto principal financiamiento"
COL_FIN_FEC_INI = "Fecha inicio financiamiento"
COL_FIN_PLAZO = "Plazo financiamiento meses"
COL_FIN_TASA = "Tasa financiamiento"
COL_FIN_TASA_TIPO = "Tipo tasa financiamiento"
COL_FIN_MODALIDAD = "Modalidad financiamiento"
COL_FIN_PERIOD = "Periodicidad financiamiento"
COL_FIN_CRONO = "Cronograma financiamiento"
COL_FACT_DET = "Detalle factoring"


EMPRESAS_OPCIONES = ["RS-SP", "RIR"]
EMPRESA_DEFAULT   = "RS-SP"
YES_NO_OPTIONS    = ["No", "Sí"]
REC_PERIOD_OPTIONS = ["15nal", "Mensual", "Semestral"]
REC_RULE_OPTIONS = [
    "Inicio de cada mes",
    "Dia 15 de cada mes",
    "Dia 1 y 15 de cada mes",
    "Mismo dia de fecha esperada",
]

STATE_OPTIONS = ["Pendiente", "Parcial", "Realizado"]
REC_DURATION_OPTIONS = ["Indefinida", "Hasta fecha", "Por cantidad de periodos"]
CONTRAPARTE_TYPE_OPTIONS = ["", "Socio", "Banco", "Empresa relacionada", "Empresa invertida", "Cliente", "Proveedor", "Tercero", "Otro"]
ING_CATEGORY_OPTIONS = [
    "Proyectos",
    "Oficina",
    "Otros ingresos operativos",
    "Ingreso financiero",
    "Ingreso no operativo",
    "Aporte de socio / capital",
    "Financiamiento recibido",
    "Miscelaneos",
]
ING_DETAIL_OPTIONS = [
    "Cobro de proyecto",
    "Cobro de servicio",
    "Interes ganado",
    "Ingreso extraordinario",
    "Aporte de capital",
    "Prestamo recibido",
    "Otro",
]
ING_NATURE_OPTIONS = ["Operativo", "Financiero", "No operativo", "Capital", "Financiamiento"]
ING_BALANCE_OPTIONS = ["Cuenta por cobrar", "Caja / banco", "Patrimonio", "Pasivo financiero"]
GAS_CATEGORY_OPTIONS = [
    "Proyectos",
    "Gastos fijos",
    "Gastos operativos",
    "Oficina",
    "Inversiones",
    "Miscelaneos",
    "Comisiones",
    "Gasto financiero",
    "Impuestos",
]
GAS_SUB_OPTIONS = [
    "Costo directo",
    "Administrativo fijo",
    "Operativo variable",
    "Comercial / ventas",
    "Financiero",
    "Impuestos",
    "No operativo",
]
GAS_DETAIL_OPTIONS = [
    "Alquiler",
    "Internet",
    "Planilla",
    "Gasolina",
    "Viaticos",
    "Comisiones",
    "Mercadeo",
    "Intereses",
    "Materiales",
    "Subcontratos",
    "Aporte a otra empresa",
    "Timbres / tasas",
    "Otros",
]
GAS_BALANCE_OPTIONS = [
    "Gasto del periodo",
    "Activo fijo",
    "Inventario",
    "Anticipo / prepago",
    "Inversion / participacion en otra empresa",
    "Cuenta por cobrar / prestamo otorgado",
    "Cancelacion de pasivo / deuda",
]
FIN_TYPE_OPTIONS = ["Financiamiento recibido", "Financiamiento otorgado", "Activo fijo financiado"]
FIN_RATE_TYPE_OPTIONS = ["Mensual", "Anual"]
FIN_MODALITY_OPTIONS = ["Cuotas periodicas", "Pago unico al vencimiento"]
AF_TYPE_OPTIONS = ["Tangible", "Intangible"]
AF_LIFE_OPTIONS = [1, 3, 5, 7, 10]

ING_CATEGORY_HELP = {
    "Proyectos": "Que entra: cobros principales del negocio. Ejemplos: suministro hospitalario; mantenimiento de equipos.",
    "Oficina": "Que entra: reintegros o ingresos administrativos menores. Ejemplos: reintegro de caja chica; reembolso administrativo.",
    "Otros ingresos operativos": "Que entra: ingresos operativos secundarios. Ejemplos: visita tecnica cobrada; servicio menor.",
    "Ingreso financiero": "Que entra: intereses o rendimientos financieros. Ejemplos: interes de prestamo otorgado; rendimiento financiero.",
    "Ingreso no operativo": "Que entra: ingresos extraordinarios no habituales. Ejemplos: venta ocasional de un bien; recuperacion extraordinaria.",
    "Aporte de socio / capital": "Que entra: aportes de capital de socios. Ejemplos: aporte inicial del socio; capitalizacion extraordinaria.",
    "Financiamiento recibido": "Que entra: dinero prestado recibido por la empresa. Ejemplos: prestamo bancario; prestamo del socio tratado como deuda.",
    "Miscelaneos": "Que entra: ingresos pendientes de reclasificar. Ejemplos: ingreso aislado; recuperacion no definida aun.",
}
GAS_CATEGORY_HELP = {
    "Proyectos": "Que entra: costos directos de proyecto. Ejemplos: materiales; subcontratos.",
    "Gastos fijos": "Que entra: estructura fija del negocio. Ejemplos: alquiler; planilla administrativa.",
    "Gastos operativos": "Que entra: operacion diaria no directa. Ejemplos: gasolina general; viaticos.",
    "Oficina": "Que entra: administracion y consumibles menores. Ejemplos: impresiones; utiles de oficina.",
    "Inversiones": "Que entra: aportes o participaciones en otras empresas. Ejemplos: aporte a RIR; participacion en una afiliada.",
    "Miscelaneos": "Que entra: gasto pendiente de reclasificar. Ejemplos: gasto aislado; gasto no definido aun.",
    "Comisiones": "Que entra: gastos comerciales por venta. Ejemplos: comision de cierre; incentivo comercial.",
    "Gasto financiero": "Que entra: intereses y cargos financieros. Ejemplos: interes de prestamo; cargo bancario.",
    "Impuestos": "Que entra: tasas e impuestos no recuperables. Ejemplos: timbres; tasas municipales.",
}
GAS_SUB_HELP = {
    "Costo directo": "Que entra: costos ligados al proyecto o venta. Ejemplos: material de obra; instalacion directa.",
    "Administrativo fijo": "Que entra: estructura fija y administrativa. Ejemplos: alquiler; internet fijo.",
    "Operativo variable": "Que entra: operacion diaria variable. Ejemplos: combustible; viaticos.",
    "Comercial / ventas": "Que entra: gastos de venta y mercadeo. Ejemplos: comisiones; publicidad.",
    "Financiero": "Que entra: costos por deuda o servicios financieros. Ejemplos: intereses; cargos bancarios.",
    "Impuestos": "Que entra: tributos y tasas. Ejemplos: timbres; tasas municipales.",
    "No operativo": "Que entra: salidas extraordinarias no habituales. Ejemplos: perdida extraordinaria; gasto aislado no recurrente.",
}
BALANCE_GAS_HELP = {
    "Gasto del periodo": "Que entra: consumo del mismo periodo. Ejemplos: alquiler del mes; gasolina del mes.",
    "Activo fijo": "Que entra: bien duradero que seguira dando valor. Ejemplos: laptop; vehiculo.",
    "Inventario": "Que entra: bienes para vender o usar despues. Ejemplos: equipos para reventa; insumos en stock.",
    "Anticipo / prepago": "Que entra: pago adelantado no consumido aun. Ejemplos: seguro anual; alquiler adelantado.",
    "Inversion / participacion en otra empresa": "Que entra: aportes e inversiones en otras empresas. Ejemplos: aporte de RS a RIR; compra de participacion societaria.",
    "Cuenta por cobrar / prestamo otorgado": "Que entra: dinero entregado que debe recuperarse. Ejemplos: prestamo a tercero; prestamo a empresa relacionada.",
    "Cancelacion de pasivo / deuda": "Que entra: pago de capital de deuda. Ejemplos: abono a prestamo; pago de capital de financiamiento.",
}
INV_MOV_OPTIONS = ["Entrada", "Salida / consumo", "Ajuste positivo", "Ajuste negativo"]


# -------------------- Helpers generales --------------------


def _today() -> date: return date.today()

def _ts(x):
    try: return pd.to_datetime(x, errors="coerce")
    except Exception: return pd.NaT

def _si_no_norm(x) -> str:
    s = str(x).strip().lower()
    return "Sí" if s in {"si","sí","sí","yes","y","true","1"} else "No"

def _estado_to_yes_no(estado: str) -> str:
    return YES_NO_OPTIONS[1] if str(estado or "").strip() in {"Pendiente", "Parcial"} else YES_NO_OPTIONS[0]


def _bool_from_toggle(value) -> bool:
    return _si_no_norm(value) != "No"


def _derive_ing_nature(category: str) -> str:
    mapping = {
        "Proyectos": "Operativo",
        "Oficina": "Operativo",
        "Otros ingresos operativos": "Operativo",
        "Ingreso financiero": "Financiero",
        "Ingreso no operativo": "No operativo",
        "Aporte de socio / capital": "Capital",
        "Financiamiento recibido": "Financiamiento",
    }
    return mapping.get(str(category or "").strip(), "Operativo")


def _derive_ing_balance(category: str, estado: str) -> str:
    if str(category or "").strip() == "Aporte de socio / capital":
        return "Patrimonio"
    if str(category or "").strip() == "Financiamiento recibido":
        return "Pasivo financiero"
    return "Cuenta por cobrar" if str(estado or "").strip() == "Pendiente" else "Caja / banco"


def _derive_gas_sub(category: str) -> str:
    mapping = {
        "Proyectos": "Costo directo",
        "Gastos fijos": "Administrativo fijo",
        "Gastos operativos": "Operativo variable",
        "Oficina": "Administrativo fijo",
        "Inversiones": "No operativo",
        "Miscelaneos": "No operativo",
        "Comisiones": "Comercial / ventas",
        "Gasto financiero": "Financiero",
        "Impuestos": "Impuestos",
    }
    return mapping.get(str(category or "").strip(), "Operativo variable")


def _derive_gas_balance(category: str) -> str:
    if str(category or "").strip() == "Inversiones":
        return "Inversion / participacion en otra empresa"
    return "Gasto del periodo"


def _help_for_option(mapping: dict[str, str], selected: str, fallback: str = "") -> str:
    return mapping.get(str(selected or "").strip(), fallback)


def _counterparty_required_for_ing(category: str, treatment: str, fin_on: bool) -> bool:
    return fin_on or category == "Aporte de socio / capital" or treatment in {"Patrimonio", "Pasivo financiero"}


def _counterparty_required_for_gas(category: str, treatment: str, fin_on: bool) -> bool:
    return fin_on or category == "Inversiones" or treatment in {
        "Inversion / participacion en otra empresa",
        "Cuenta por cobrar / prestamo otorgado",
    }


def _autoderive_ing_df(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_ingresos_columns(df)
    out[COL_ING_NAT] = out[COL_CAT].map(_derive_ing_nature)
    factoring_mask = out[COL_FACT_DET].map(_has_factoring) if COL_FACT_DET in out.columns else pd.Series(False, index=out.index)
    special_mask = out[COL_CAT].astype(str).isin(["Aporte de socio / capital", "Financiamiento recibido"])
    estado_series = out[COL_POR_COB].map(lambda x: "Pendiente" if _si_no_norm(x) != "No" else "Realizado")
    out.loc[special_mask, COL_TRAT_BAL_ING] = [
        _derive_ing_balance(cat, estado)
        for cat, estado in zip(out.loc[special_mask, COL_CAT], estado_series.loc[special_mask])
    ]
    full_real_mask = out[COL_POR_COB].map(_si_no_norm).eq("No")
    complete_partial_mask = (
        out[COL_POR_COB].map(_si_no_norm).ne("No")
        & pd.to_numeric(out[COL_COBRO_REAL_MONTO], errors="coerce").fillna(0.0).ge(pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0))
        & _ts(out[COL_FCOBRO_REAL]).notna()
    )
    out.loc[complete_partial_mask, COL_POR_COB] = "No"
    full_cash_mask = (full_real_mask | complete_partial_mask) & ~factoring_mask
    out.loc[full_cash_mask, COL_COBRO_REAL_MONTO] = pd.to_numeric(out.loc[full_cash_mask, COL_MONTO], errors="coerce").fillna(0.0)
    return ensure_ingresos_columns(out)


def _autoderive_gas_df(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_gastos_columns(df)
    out[COL_GAS_SUB] = out[COL_CAT].map(_derive_gas_sub)
    inv_mask = out[COL_CAT].astype(str).eq("Inversiones")
    out.loc[inv_mask, COL_TRAT_BAL_GAS] = "Inversion / participacion en otra empresa"
    complete_partial_mask = (
        out[COL_POR_PAG].map(_si_no_norm).ne("No")
        & pd.to_numeric(out[COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0).ge(pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0))
        & _ts(out[COL_FPAGO_REAL]).notna()
    )
    out.loc[complete_partial_mask, COL_POR_PAG] = "No"
    full_paid_mask = out[COL_POR_PAG].map(_si_no_norm).eq("No")
    out.loc[full_paid_mask | complete_partial_mask, COL_PAGO_REAL_MONTO] = pd.to_numeric(out.loc[full_paid_mask | complete_partial_mask, COL_MONTO], errors="coerce").fillna(0.0)
    return ensure_gastos_columns(out)


def _validate_ing_df(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    for idx, row in df.iterrows():
        monto = float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        monto_real = float(pd.to_numeric(pd.Series([row.get(COL_COBRO_REAL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        por_cobrar = _si_no_norm(row.get(COL_POR_COB, "No"))
        fecha_esp = _ts(row.get(COL_FCOBRO))
        fecha_real = _ts(row.get(COL_FCOBRO_REAL))
        categoria = str(row.get(COL_CAT, "") or "").strip()
        tratamiento = str(row.get(COL_TRAT_BAL_ING, "") or "").strip()
        contraparte = str(row.get(COL_CTP_NOMBRE, "") or "").strip()
        factoring_detail = _parse_factoring_detail(row.get(COL_FACT_DET, ""))
        factoring_on = bool(factoring_detail)
        factoring_retenido = _factoring_retained_pending(factoring_detail)
        fin_on = _bool_from_toggle(row.get(COL_FIN_TOGGLE, "No")) or categoria == "Financiamiento recibido"
        label = str(row.get(COL_DESC, "") or row.get(COL_ROWID, f"fila {idx+1}")).strip() or f"fila {idx+1}"
        if monto <= 0:
            errors.append(f"Ingresos: `{label}` tiene monto no valido.")
        if por_cobrar != "No" and pd.isna(fecha_esp):
            errors.append(f"Ingresos: `{label}` requiere Fecha esperada de cobro.")
        if por_cobrar == "No" and pd.isna(fecha_real):
            errors.append(f"Ingresos: `{label}` requiere Fecha real de cobro.")
        if por_cobrar != "No" and monto_real > 0 and pd.isna(fecha_real):
            errors.append(f"Ingresos: `{label}` tiene monto cobrado parcial pero sin Fecha real de cobro.")
        if por_cobrar != "No" and monto_real >= monto and monto > 0:
            errors.append(f"Ingresos: `{label}` tiene monto cobrado parcial igual o mayor al total; marque realizado si ya se cobro todo.")
        if por_cobrar == "No" and not factoring_on and abs(monto_real - monto) > 0.01:
            errors.append(f"Ingresos: `{label}` marcado realizado debe tener monto real cobrado igual al monto total.")
        if factoring_on and factoring_retenido < 0:
            errors.append(f"Ingresos: `{label}` tiene factoring inconsistente; revisa el retenido.")
        if _counterparty_required_for_ing(categoria, tratamiento, fin_on) and not contraparte:
            errors.append(f"Ingresos: `{label}` requiere Entidad relacionada / contraparte.")
    return errors


def _validate_gas_df(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    for idx, row in df.iterrows():
        monto = float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        monto_real = float(pd.to_numeric(pd.Series([row.get(COL_PAGO_REAL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        por_pagar = _si_no_norm(row.get(COL_POR_PAG, "No"))
        fecha_esp = _ts(row.get(COL_FPAGO))
        fecha_real = _ts(row.get(COL_FPAGO_REAL))
        categoria = str(row.get(COL_CAT, "") or "").strip()
        tratamiento = str(row.get(COL_TRAT_BAL_GAS, "") or "").strip()
        contraparte = str(row.get(COL_CTP_NOMBRE, "") or "").strip()
        fin_on = _bool_from_toggle(row.get(COL_FIN_TOGGLE, "No"))
        label = str(row.get(COL_CONC, "") or row.get(COL_ROWID, f"fila {idx+1}")).strip() or f"fila {idx+1}"
        if monto <= 0:
            errors.append(f"Gastos: `{label}` tiene monto no valido.")
        if por_pagar != "No" and pd.isna(fecha_esp):
            errors.append(f"Gastos: `{label}` requiere Fecha esperada de pago.")
        if por_pagar == "No" and pd.isna(fecha_real):
            errors.append(f"Gastos: `{label}` requiere Fecha real de pago.")
        if por_pagar != "No" and monto_real > 0 and pd.isna(fecha_real):
            errors.append(f"Gastos: `{label}` tiene monto pagado parcial pero sin Fecha real de pago.")
        if por_pagar != "No" and monto_real >= monto and monto > 0:
            errors.append(f"Gastos: `{label}` tiene monto pagado parcial igual o mayor al total; marque realizado si ya se pago todo.")
        if por_pagar == "No" and abs(monto_real - monto) > 0.01:
            errors.append(f"Gastos: `{label}` marcado realizado debe tener monto real pagado igual al monto total.")
        if tratamiento == "Anticipo / prepago":
            plazo = int(pd.to_numeric(pd.Series([row.get(COL_PREPAGO_MESES, 0)]), errors="coerce").fillna(0).iloc[0])
            fecha_inicio = _ts(row.get(COL_PREPAGO_FEC_INI))
            if plazo <= 0:
                errors.append(f"Gastos: `{label}` con Anticipo / prepago requiere Plazo prepago meses.")
            if pd.isna(fecha_inicio):
                errors.append(f"Gastos: `{label}` con Anticipo / prepago requiere Fecha inicio prepago.")
        if tratamiento == "Inventario":
            inv_mov = str(row.get(COL_INV_MOV, "") or "").strip()
            inv_item = str(row.get(COL_INV_ITEM, "") or "").strip()
            if not inv_mov:
                errors.append(f"Gastos: `{label}` con Inventario requiere Movimiento inventario.")
            if not inv_item:
                errors.append(f"Gastos: `{label}` con Inventario requiere Item inventario / referencia.")
        if _counterparty_required_for_gas(categoria, tratamiento, fin_on) and not contraparte:
            errors.append(f"Gastos: `{label}` requiere Entidad relacionada / contraparte.")
    return errors


def _date_or_nat(value):
    ts = _ts(value)
    return ts if not pd.isna(ts) else pd.NaT


def _serialize_schedule(entries: list[dict]) -> str:
    try:
        return json.dumps(entries, ensure_ascii=False)
    except Exception:
        return "[]"


def _parse_partial_events(raw_value) -> list[dict]:
    try:
        data = json.loads(str(raw_value or "[]"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    rows: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        fecha = _ts(item.get("fecha"))
        monto = float(pd.to_numeric(pd.Series([item.get("monto", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        nota = str(item.get("nota", "") or "").strip()
        if pd.isna(fecha) or monto <= 0:
            continue
        rows.append({"fecha": fecha, "monto": monto, "nota": nota})
    rows.sort(key=lambda x: x["fecha"])
    return rows


def _serialize_partial_events(entries: list[dict]) -> str:
    payload = []
    for entry in entries:
        fecha = _ts(entry.get("fecha"))
        monto = float(pd.to_numeric(pd.Series([entry.get("monto", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        nota = str(entry.get("nota", "") or "").strip()
        if pd.isna(fecha) or monto <= 0:
            continue
        payload.append({"fecha": fecha.date().isoformat(), "monto": monto, "nota": nota})
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return "[]"


def _parse_factoring_detail(raw_value) -> dict:
    try:
        data = json.loads(str(raw_value or "{}"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}

    def _num(key: str) -> float:
        return float(pd.to_numeric(pd.Series([data.get(key, 0.0)]), errors="coerce").fillna(0.0).iloc[0])

    detail = {
        "modo": str(data.get("modo", "") or "").strip(),
        "contraparte_tipo": str(data.get("contraparte_tipo", "") or "").strip(),
        "contraparte": str(data.get("contraparte", "") or "").strip(),
        "fecha_inicio": _date_or_nat(data.get("fecha_inicio")),
        "fecha_liquidacion_final": _date_or_nat(data.get("fecha_liquidacion_final")),
        "factored_amount": _num("factored_amount"),
        "initial_cash_received": _num("initial_cash_received"),
        "initial_retained": _num("initial_retained"),
        "initial_fee": _num("initial_fee"),
        "final_cash_received": _num("final_cash_received"),
        "final_fee": _num("final_fee"),
        "nota": str(data.get("nota", "") or "").strip(),
    }
    if not detail["modo"]:
        return {}
    return detail


def _serialize_factoring_detail(detail: dict) -> str:
    if not isinstance(detail, dict) or not detail:
        return ""
    payload = {
        "modo": str(detail.get("modo", "") or "").strip(),
        "contraparte_tipo": str(detail.get("contraparte_tipo", "") or "").strip(),
        "contraparte": str(detail.get("contraparte", "") or "").strip(),
        "fecha_inicio": (_ts(detail.get("fecha_inicio")).date().isoformat() if not pd.isna(_ts(detail.get("fecha_inicio"))) else ""),
        "fecha_liquidacion_final": (_ts(detail.get("fecha_liquidacion_final")).date().isoformat() if not pd.isna(_ts(detail.get("fecha_liquidacion_final"))) else ""),
        "factored_amount": float(pd.to_numeric(pd.Series([detail.get("factored_amount", 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
        "initial_cash_received": float(pd.to_numeric(pd.Series([detail.get("initial_cash_received", 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
        "initial_retained": float(pd.to_numeric(pd.Series([detail.get("initial_retained", 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
        "initial_fee": float(pd.to_numeric(pd.Series([detail.get("initial_fee", 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
        "final_cash_received": float(pd.to_numeric(pd.Series([detail.get("final_cash_received", 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
        "final_fee": float(pd.to_numeric(pd.Series([detail.get("final_fee", 0.0)]), errors="coerce").fillna(0.0).iloc[0]),
        "nota": str(detail.get("nota", "") or "").strip(),
    }
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return ""


def _factoring_retained_pending(raw_value) -> float:
    detail = raw_value if isinstance(raw_value, dict) else _parse_factoring_detail(raw_value)
    if not detail:
        return 0.0
    pendiente = (
        float(detail.get("initial_retained", 0.0) or 0.0)
        - float(detail.get("final_cash_received", 0.0) or 0.0)
        - float(detail.get("final_fee", 0.0) or 0.0)
    )
    return max(0.0, float(pendiente))


def _has_factoring(raw_value) -> bool:
    return bool(_parse_factoring_detail(raw_value))


def _seed_partial_events_from_row(row: pd.Series, amount_col: str, date_col: str) -> list[dict]:
    events = _parse_partial_events(row.get(COL_ING_PARTIALS if amount_col == COL_COBRO_REAL_MONTO else COL_GAS_PARTIALS))
    if events:
        return events
    monto = float(pd.to_numeric(pd.Series([row.get(amount_col, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
    fecha = _ts(row.get(date_col))
    if monto > 0 and not pd.isna(fecha):
        return [{"fecha": fecha, "monto": monto, "nota": "Saldo real existente"}]
    return []


def _partial_events_summary(entries: list[dict]) -> tuple[float, pd.Timestamp | pd.NaT]:
    if not entries:
        return 0.0, pd.NaT
    total = float(sum(float(x.get("monto", 0.0) or 0.0) for x in entries))
    last_date = max((_ts(x.get("fecha")) for x in entries), default=pd.NaT)
    return total, last_date


def _build_factoring_fee_row(
    *,
    base_row: pd.Series,
    factor_nombre: str,
    factor_tipo_ctp: str,
    fecha_evento,
    monto_fee: float,
    fee_label: str,
) -> dict:
    desc_base = str(base_row.get(COL_DESC, "") or "").strip()
    concepto = f"{fee_label} - {desc_base}" if desc_base else fee_label
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: _ts(fecha_evento),
        COL_DESC: concepto,
        COL_CONC: concepto,
        COL_MONTO: float(monto_fee),
        COL_CAT: "Gasto financiero",
        COL_ESC: "Real",
        COL_REF_RID: str(base_row.get(COL_ROWID, "") or ""),
        COL_PROY: str(base_row.get(COL_PROY, "") or ""),
        COL_CLI_ID: str(base_row.get(COL_CLI_ID, "") or ""),
        COL_CLI_NOM: str(base_row.get(COL_CLI_NOM, "") or ""),
        COL_EMP: str(base_row.get(COL_EMP, EMPRESA_DEFAULT) or EMPRESA_DEFAULT),
        COL_POR_PAG: "No",
        COL_PROV: str(factor_nombre or "").strip(),
        COL_REC: "No",
        COL_FPAGO: pd.NaT,
        COL_FPAGO_REAL: _ts(fecha_evento),
        COL_CTP_TIPO: str(factor_tipo_ctp or "").strip(),
        COL_CTP_NOMBRE: str(factor_nombre or "").strip(),
        COL_PAGO_REAL_MONTO: float(monto_fee),
        COL_GAS_PARTIALS: _serialize_partial_events(
            [{"fecha": _ts(fecha_evento), "monto": float(monto_fee), "nota": fee_label}]
        ),
        COL_GAS_SUB: "Financiero",
        COL_GAS_DET: "Otros",
        COL_TRAT_BAL_GAS: "Gasto del periodo",
        COL_INV_MOV: "",
        COL_INV_ITEM: "",
        COL_USER: _current_user(),
    }


def _render_table_error_block(title: str, errors: list[str]) -> None:
    if not errors:
        return
    st.error(f"No se guardaron cambios en {title} hasta corregir la tabla editable.")
    st.dataframe(pd.DataFrame({"detalle": errors}), use_container_width=True, hide_index=True)


def _build_financing_schedule(
    *,
    principal: float,
    fecha_inicio,
    plazo_meses: int,
    tasa: float,
    tasa_tipo: str,
    modalidad: str,
    periodicidad: str,
) -> str:
    principal = float(principal or 0.0)
    plazo_meses = int(plazo_meses or 0)
    tasa = float(tasa or 0.0)
    start_ts = _date_or_nat(fecha_inicio)
    if principal <= 0 or plazo_meses <= 0 or pd.isna(start_ts):
        return "[]"

    periodicidad = str(periodicidad or "Mensual").strip() or "Mensual"
    modalidad = str(modalidad or "Cuotas periodicas").strip() or "Cuotas periodicas"
    tasa_tipo = str(tasa_tipo or "Anual").strip() or "Anual"

    step_months = 6 if periodicidad == "Semestral" else 1
    periods = max(1, plazo_meses // step_months)
    period_rate = tasa / 100.0
    if tasa_tipo == "Anual":
        period_rate = period_rate / 12.0
    if periodicidad == "15nal":
        period_rate = period_rate / 2.0
        periods = max(1, plazo_meses * 2)

    entries: list[dict] = []
    saldo = principal
    capital_const = principal / periods if modalidad == "Cuotas periodicas" else 0.0

    for idx in range(1, periods + 1):
        if periodicidad == "15nal":
            fecha = start_ts + pd.Timedelta(days=15 * idx)
        else:
            fecha = start_ts + pd.DateOffset(months=step_months * idx)

        interes = saldo * period_rate
        if modalidad == "Pago unico al vencimiento":
            capital = saldo if idx == periods else 0.0
        else:
            capital = capital_const if idx < periods else saldo
        cuota = capital + interes
        saldo = max(0.0, saldo - capital)
        entries.append(
            {
                "n": idx,
                "fecha": pd.Timestamp(fecha).date().isoformat(),
                "interes": round(interes, 2),
                "capital": round(capital, 2),
                "cuota_total": round(cuota, 2),
                "saldo_pendiente": round(saldo, 2),
            }
        )
    return _serialize_schedule(entries)


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


def _format_number_es(value, decimals: int = 2) -> str:
    """Formatea con miles '.' y decimales ',' (ej: 1.500,00)."""
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        n = float(value)
    except Exception:
        return ""
    us = f"{n:,.{decimals}f}"
    return us.replace(",", "__tmp__").replace(".", ",").replace("__tmp__", ".")


def _format_money_es(value) -> str:
    return f"${_format_number_es(value, 2)}"


def _parse_number_maybe_es(value) -> float:
    """Acepta 1500.00, 1,500.00, 1500,00, 1.500,00 y devuelve float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return 0.0
        except Exception:
            pass
        return float(value)

    s = str(value).strip()
    if not s:
        return 0.0
    s = s.replace("$", "").replace(" ", "")

    comma_pos = s.rfind(",")
    dot_pos = s.rfind(".")
    if comma_pos >= 0 and dot_pos >= 0:
        if comma_pos > dot_pos:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif comma_pos >= 0:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")

    try:
        return float(s)
    except Exception:
        return 0.0


def _editor_state_to_dataframe(
    original_df: pd.DataFrame,
    editor_key: str,
    *,
    numeric_cols: set[str] | None = None,
) -> pd.DataFrame:
    work = original_df.copy().reset_index(drop=True)
    editor_state = st.session_state.get(editor_key)
    if not isinstance(editor_state, dict):
        if numeric_cols:
            for col in numeric_cols:
                if col in work.columns:
                    work[col] = work[col].map(_parse_number_maybe_es)
        return work

    numeric_cols = numeric_cols or set()

    edited_rows = editor_state.get("edited_rows") or {}
    for row_idx, changes in edited_rows.items():
        try:
            i = int(row_idx)
        except Exception:
            continue
        if i < 0 or i >= len(work) or not isinstance(changes, dict):
            continue
        for col, value in changes.items():
            if col not in work.columns:
                continue
            if col in numeric_cols:
                work.at[i, col] = _parse_number_maybe_es(value)
            else:
                work.at[i, col] = value

    deleted_rows = sorted(editor_state.get("deleted_rows") or [], reverse=True)
    for row_idx in deleted_rows:
        try:
            i = int(row_idx)
        except Exception:
            continue
        if 0 <= i < len(work):
            work = work.drop(index=i)

    added_rows = editor_state.get("added_rows") or []
    if added_rows:
        base_defaults = {c: "" for c in work.columns}
        for col in numeric_cols:
            if col in base_defaults:
                base_defaults[col] = 0.0
        new_records: list[dict] = []
        for row in added_rows:
            if not isinstance(row, dict):
                continue
            rec = base_defaults.copy()
            for col, value in row.items():
                if col not in rec:
                    continue
                rec[col] = _parse_number_maybe_es(value) if col in numeric_cols else value
            new_records.append(rec)
        if new_records:
            work = pd.concat([work, pd.DataFrame(new_records)], ignore_index=True)

    work = work.reset_index(drop=True)
    for col in numeric_cols:
        if col in work.columns:
            work[col] = work[col].map(_parse_number_maybe_es)
    return work


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
    for alt in ["Recurrente", "recurrente", "Recurrencia", "Recurrente mensual", "Es recurrente"]:
        if alt in df.columns and COL_REC not in df.columns:
            ren[alt] = COL_REC
            break
    for alt in ["Periodo recurrencia", "Recurrencia periodo", "Periodo de recurrencia", "Frecuencia recurrencia"]:
        if alt in df.columns and COL_REC_PER not in df.columns:
            ren[alt] = COL_REC_PER
            break
    for alt in ["Regla fecha recurrencia", "Fecha recurrencia", "Dia de recurrencia", "Regla de fecha"]:
        if alt in df.columns and COL_REC_REG not in df.columns:
            ren[alt] = COL_REC_REG
            break
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
        COL_COB, COL_FCOBRO, COL_FCOBRO_REAL, COL_CTP_TIPO, COL_CTP_NOMBRE, COL_COBRO_REAL_MONTO,
        COL_ING_PARTIALS, COL_FACT_DET,
        COL_REC, COL_REC_PER, COL_REC_REG,
        COL_REC_DUR, COL_REC_HASTA, COL_REC_CANT, COL_ING_DET, COL_ING_NAT,
        COL_TRAT_BAL_ING, COL_FIN_TOGGLE, COL_FIN_TIPO, COL_FIN_MONTO,
        COL_FIN_FEC_INI, COL_FIN_PLAZO, COL_FIN_TASA, COL_FIN_TASA_TIPO,
        COL_FIN_MODALIDAD, COL_FIN_PERIOD, COL_FIN_CRONO, COL_ROWID, COL_USER,
    ]:
        if col not in out.columns:
            if col == COL_MONTO:
                out[col] = 0.0
            elif col in {COL_FECHA, COL_FCOBRO, COL_FCOBRO_REAL, COL_REC_HASTA, COL_FIN_FEC_INI}:
                out[col] = pd.NaT
            elif col == COL_EMP:
                out[col] = EMPRESA_DEFAULT
            elif col in {COL_POR_COB, COL_COB, COL_REC, COL_FIN_TOGGLE}:
                out[col] = "No"
            elif col == COL_REC_PER:
                out[col] = "Mensual"
            elif col == COL_REC_REG:
                out[col] = "Inicio de cada mes"
            elif col == COL_REC_DUR:
                out[col] = "Indefinida"
            elif col == COL_ING_NAT:
                out[col] = "Operativo"
            elif col == COL_TRAT_BAL_ING:
                out[col] = "Cuenta por cobrar"
            elif col == COL_FIN_TIPO:
                out[col] = "Financiamiento recibido"
            elif col == COL_FIN_TASA_TIPO:
                out[col] = "Anual"
            elif col == COL_FIN_MODALIDAD:
                out[col] = "Cuotas periodicas"
            elif col == COL_FIN_PERIOD:
                out[col] = "Mensual"
            elif col in {COL_FIN_MONTO, COL_FIN_TASA, COL_COBRO_REAL_MONTO}:
                out[col] = 0.0
            elif col in {COL_FIN_PLAZO, COL_REC_CANT}:
                out[col] = 0
            else:
                out[col] = ""

    out[COL_FECHA] = _ts(out[COL_FECHA])
    out[COL_FCOBRO] = _ts(out[COL_FCOBRO])
    out[COL_FCOBRO_REAL] = _ts(out[COL_FCOBRO_REAL])
    out[COL_REC_HASTA] = _ts(out[COL_REC_HASTA])
    out[COL_FIN_FEC_INI] = _ts(out[COL_FIN_FEC_INI])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_COBRO_REAL_MONTO] = pd.to_numeric(out[COL_COBRO_REAL_MONTO], errors="coerce").fillna(0.0).clip(lower=0.0).astype(float)
    out[COL_FIN_MONTO] = pd.to_numeric(out[COL_FIN_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_FIN_TASA] = pd.to_numeric(out[COL_FIN_TASA], errors="coerce").fillna(0.0).astype(float)
    out[COL_FIN_PLAZO] = pd.to_numeric(out[COL_FIN_PLAZO], errors="coerce").fillna(0).astype(int)
    out[COL_REC_CANT] = pd.to_numeric(out[COL_REC_CANT], errors="coerce").fillna(0).astype(int)
    out[COL_EMP] = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT,
    )
    out[COL_POR_COB] = out[COL_POR_COB].map(_si_no_norm)
    out[COL_COB] = out[COL_COB].map(_si_no_norm)
    out[COL_REC] = out[COL_REC].map(_si_no_norm)
    out[COL_FIN_TOGGLE] = out[COL_FIN_TOGGLE].map(_si_no_norm)
    out = _ensure_text(
        out,
        [
            COL_DESC, COL_CONC, COL_CAT, COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP,
            COL_POR_COB, COL_COB, COL_REC, COL_REC_PER, COL_REC_REG, COL_REC_DUR,
            COL_ROWID, COL_USER, COL_ING_DET, COL_ING_NAT, COL_TRAT_BAL_ING, COL_CTP_TIPO, COL_CTP_NOMBRE,
            COL_FIN_TOGGLE, COL_FIN_TIPO, COL_FIN_TASA_TIPO, COL_FIN_MODALIDAD,
            COL_FIN_PERIOD, COL_FIN_CRONO,
            COL_ING_PARTIALS,
        ],
    )
    out.loc[out[COL_REC_PER] == "Quincenal", COL_REC_PER] = "15nal"
    out.loc[out[COL_REC_PER].isin(["Bimestral", "Trimestral"]), COL_REC_PER] = "Mensual"
    out.loc[out[COL_REC_REG] == "Fin de mes", COL_REC_REG] = "Inicio de cada mes"
    rec_mask = out[COL_REC].map(_bool_from_toggle)
    out.loc[~rec_mask, [COL_REC_PER, COL_REC_REG, COL_REC_DUR, COL_REC_HASTA, COL_REC_CANT]] = ["", "", "", pd.NaT, 0]
    out.loc[rec_mask & (out[COL_REC_PER].astype(str).str.strip() == ""), COL_REC_PER] = "Mensual"
    out.loc[rec_mask & (out[COL_REC_REG].astype(str).str.strip() == ""), COL_REC_REG] = "Inicio de cada mes"
    out.loc[rec_mask & (out[COL_REC_DUR].astype(str).str.strip() == ""), COL_REC_DUR] = "Indefinida"
    out[COL_ING_NAT] = out[COL_CAT].map(_derive_ing_nature)
    estado_series = out[COL_POR_COB].map(lambda x: "Pendiente" if _si_no_norm(x) != "No" else "Realizado")
    balance_mask = out[COL_TRAT_BAL_ING].astype(str).str.strip() == ""
    out.loc[balance_mask, COL_TRAT_BAL_ING] = [
        _derive_ing_balance(cat, estado)
        for cat, estado in zip(out.loc[balance_mask, COL_CAT], estado_series.loc[balance_mask])
    ]
    total_ing = out[COL_MONTO].clip(lower=0.0)
    factoring_mask = out[COL_FACT_DET].map(_has_factoring)
    partial_events_total = out[COL_ING_PARTIALS].map(lambda raw: sum(evt["monto"] for evt in _parse_partial_events(raw)))
    partial_events_total = pd.to_numeric(partial_events_total, errors="coerce").fillna(0.0).clip(lower=0.0)
    out[COL_COBRO_REAL_MONTO] = partial_events_total.where(partial_events_total > 0, out[COL_COBRO_REAL_MONTO])
    out.loc[partial_events_total > 0, COL_FCOBRO_REAL] = out.loc[partial_events_total > 0, COL_ING_PARTIALS].map(
        lambda raw: max((evt["fecha"] for evt in _parse_partial_events(raw)), default=pd.NaT)
    )
    full_cash_mask = out[COL_POR_COB].map(_si_no_norm).eq("No") & ~factoring_mask
    out.loc[full_cash_mask, COL_COBRO_REAL_MONTO] = total_ing.loc[full_cash_mask]
    out[COL_COBRO_REAL_MONTO] = out[COL_COBRO_REAL_MONTO].clip(upper=total_ing)
    fin_mask = out[COL_FIN_TOGGLE].map(_bool_from_toggle) | out[COL_CAT].astype(str).eq("Financiamiento recibido")
    out.loc[~fin_mask, [COL_FIN_TIPO, COL_FIN_MONTO, COL_FIN_FEC_INI, COL_FIN_PLAZO, COL_FIN_TASA, COL_FIN_TASA_TIPO, COL_FIN_MODALIDAD, COL_FIN_PERIOD, COL_FIN_CRONO]] = ["", 0.0, pd.NaT, 0, 0.0, "", "", "", ""]
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out


def ensure_gastos_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = _canon_cols(df.copy())
    for col in [
        COL_FECHA, COL_CONC, COL_MONTO, COL_CAT, COL_ESC, COL_REF_RID,
        COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_EMP, COL_POR_PAG, COL_REC,
        COL_REC_PER, COL_REC_REG, COL_REC_DUR, COL_REC_HASTA, COL_REC_CANT,
        COL_PROV, COL_FPAGO, COL_FPAGO_REAL, COL_CTP_TIPO, COL_CTP_NOMBRE, COL_PAGO_REAL_MONTO,
        COL_GAS_PARTIALS,
        COL_GAS_SUB, COL_GAS_DET, COL_TRAT_BAL_GAS, COL_PREPAGO_MESES, COL_PREPAGO_FEC_INI, COL_AF_TOGGLE, COL_AF_TIPO, COL_AF_VIDA,
        COL_INV_MOV, COL_INV_ITEM,
        COL_AF_FEC_INI, COL_AF_VAL_RES, COL_AF_DEP_TOGGLE, COL_AF_DEP_MENSUAL,
        COL_FIN_TOGGLE, COL_FIN_TIPO, COL_FIN_MONTO, COL_FIN_FEC_INI,
        COL_FIN_PLAZO, COL_FIN_TASA, COL_FIN_TASA_TIPO, COL_FIN_MODALIDAD,
        COL_FIN_PERIOD, COL_FIN_CRONO, COL_ROWID, COL_USER,
    ]:
        if col not in out.columns:
            if col == COL_MONTO:
                out[col] = 0.0
            elif col in {COL_FECHA, COL_FPAGO, COL_FPAGO_REAL, COL_REC_HASTA, COL_AF_FEC_INI, COL_FIN_FEC_INI, COL_PREPAGO_FEC_INI}:
                out[col] = pd.NaT
            elif col == COL_EMP:
                out[col] = EMPRESA_DEFAULT
            elif col in {COL_POR_PAG, COL_REC, COL_AF_TOGGLE, COL_AF_DEP_TOGGLE, COL_FIN_TOGGLE}:
                out[col] = "No"
            elif col == COL_REC_PER:
                out[col] = "Mensual"
            elif col == COL_REC_REG:
                out[col] = "Inicio de cada mes"
            elif col == COL_REC_DUR:
                out[col] = "Indefinida"
            elif col == COL_GAS_SUB:
                out[col] = "Operativo variable"
            elif col == COL_TRAT_BAL_GAS:
                out[col] = "Gasto del periodo"
            elif col == COL_AF_TIPO:
                out[col] = "Tangible"
            elif col == COL_AF_VIDA:
                out[col] = 5
            elif col in {COL_AF_VAL_RES, COL_AF_DEP_MENSUAL, COL_FIN_MONTO, COL_FIN_TASA, COL_PAGO_REAL_MONTO}:
                out[col] = 0.0
            elif col in {COL_FIN_PLAZO, COL_REC_CANT, COL_PREPAGO_MESES}:
                out[col] = 0
            elif col == COL_FIN_TIPO:
                out[col] = "Financiamiento otorgado"
            elif col == COL_FIN_TASA_TIPO:
                out[col] = "Anual"
            elif col == COL_FIN_MODALIDAD:
                out[col] = "Cuotas periodicas"
            elif col == COL_FIN_PERIOD:
                out[col] = "Mensual"
            else:
                out[col] = ""

    out[COL_FECHA] = _ts(out[COL_FECHA])
    out[COL_FPAGO] = _ts(out[COL_FPAGO])
    out[COL_FPAGO_REAL] = _ts(out[COL_FPAGO_REAL])
    out[COL_REC_HASTA] = _ts(out[COL_REC_HASTA])
    out[COL_PREPAGO_FEC_INI] = _ts(out[COL_PREPAGO_FEC_INI])
    out[COL_AF_FEC_INI] = _ts(out[COL_AF_FEC_INI])
    out[COL_FIN_FEC_INI] = _ts(out[COL_FIN_FEC_INI])
    out[COL_MONTO] = pd.to_numeric(out[COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_PAGO_REAL_MONTO] = pd.to_numeric(out[COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0).clip(lower=0.0).astype(float)
    out[COL_AF_VAL_RES] = pd.to_numeric(out[COL_AF_VAL_RES], errors="coerce").fillna(0.0).astype(float)
    out[COL_AF_DEP_MENSUAL] = pd.to_numeric(out[COL_AF_DEP_MENSUAL], errors="coerce").fillna(0.0).astype(float)
    out[COL_FIN_MONTO] = pd.to_numeric(out[COL_FIN_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[COL_FIN_TASA] = pd.to_numeric(out[COL_FIN_TASA], errors="coerce").fillna(0.0).astype(float)
    out[COL_FIN_PLAZO] = pd.to_numeric(out[COL_FIN_PLAZO], errors="coerce").fillna(0).astype(int)
    out[COL_REC_CANT] = pd.to_numeric(out[COL_REC_CANT], errors="coerce").fillna(0).astype(int)
    out[COL_AF_VIDA] = pd.to_numeric(out[COL_AF_VIDA], errors="coerce").fillna(5).astype(int)
    out[COL_EMP] = out[COL_EMP].astype("string").str.upper().str.strip().where(
        out[COL_EMP].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT,
    )
    out[COL_POR_PAG] = out[COL_POR_PAG].map(_si_no_norm)
    out[COL_REC] = out[COL_REC].map(_si_no_norm)
    out[COL_AF_TOGGLE] = out[COL_AF_TOGGLE].map(_si_no_norm)
    out[COL_AF_DEP_TOGGLE] = out[COL_AF_DEP_TOGGLE].map(_si_no_norm)
    out[COL_FIN_TOGGLE] = out[COL_FIN_TOGGLE].map(_si_no_norm)
    out = _ensure_text(
        out,
        [
            COL_CONC, COL_CAT, COL_REF_RID, COL_PROY, COL_CLI_ID, COL_CLI_NOM,
            COL_EMP, COL_POR_PAG, COL_REC, COL_REC_PER, COL_REC_REG, COL_REC_DUR,
            COL_PROV, COL_ROWID, COL_USER, COL_GAS_SUB, COL_GAS_DET, COL_CTP_TIPO, COL_CTP_NOMBRE,
            COL_TRAT_BAL_GAS, COL_AF_TOGGLE, COL_AF_TIPO, COL_AF_DEP_TOGGLE,
            COL_FIN_TOGGLE, COL_FIN_TIPO, COL_FIN_TASA_TIPO, COL_FIN_MODALIDAD,
            COL_FIN_PERIOD, COL_FIN_CRONO, COL_GAS_PARTIALS, COL_INV_MOV, COL_INV_ITEM,
        ],
    )
    out.loc[out[COL_REC_PER] == "Quincenal", COL_REC_PER] = "15nal"
    out.loc[out[COL_REC_PER].isin(["Bimestral", "Trimestral"]), COL_REC_PER] = "Mensual"
    out.loc[out[COL_REC_REG] == "Fin de mes", COL_REC_REG] = "Inicio de cada mes"
    rec_mask = out[COL_REC].map(_bool_from_toggle)
    out.loc[~rec_mask, [COL_REC_PER, COL_REC_REG, COL_REC_DUR, COL_REC_HASTA, COL_REC_CANT]] = ["", "", "", pd.NaT, 0]
    out.loc[rec_mask & (out[COL_REC_PER].astype(str).str.strip() == ""), COL_REC_PER] = "Mensual"
    out.loc[rec_mask & (out[COL_REC_REG].astype(str).str.strip() == ""), COL_REC_REG] = "Inicio de cada mes"
    out.loc[rec_mask & (out[COL_REC_DUR].astype(str).str.strip() == ""), COL_REC_DUR] = "Indefinida"
    out[COL_GAS_SUB] = out[COL_CAT].map(_derive_gas_sub)
    out.loc[out[COL_TRAT_BAL_GAS].astype(str).str.strip() == "", COL_TRAT_BAL_GAS] = out[COL_CAT].map(_derive_gas_balance)
    total_gas = out[COL_MONTO].clip(lower=0.0)
    partial_events_total = out[COL_GAS_PARTIALS].map(lambda raw: sum(evt["monto"] for evt in _parse_partial_events(raw)))
    partial_events_total = pd.to_numeric(partial_events_total, errors="coerce").fillna(0.0).clip(lower=0.0)
    out[COL_PAGO_REAL_MONTO] = partial_events_total.where(partial_events_total > 0, out[COL_PAGO_REAL_MONTO])
    out.loc[partial_events_total > 0, COL_FPAGO_REAL] = out.loc[partial_events_total > 0, COL_GAS_PARTIALS].map(
        lambda raw: max((evt["fecha"] for evt in _parse_partial_events(raw)), default=pd.NaT)
    )
    out.loc[out[COL_POR_PAG].map(_si_no_norm) == "No", COL_PAGO_REAL_MONTO] = total_gas.loc[out[COL_POR_PAG].map(_si_no_norm) == "No"]
    out[COL_PAGO_REAL_MONTO] = out[COL_PAGO_REAL_MONTO].clip(upper=total_gas)
    prepago_mask = out[COL_TRAT_BAL_GAS].astype(str).eq("Anticipo / prepago")
    out.loc[~prepago_mask, [COL_PREPAGO_MESES, COL_PREPAGO_FEC_INI]] = [0, pd.NaT]
    out.loc[prepago_mask & out[COL_PREPAGO_FEC_INI].isna(), COL_PREPAGO_FEC_INI] = out.loc[prepago_mask & out[COL_PREPAGO_FEC_INI].isna(), COL_FECHA]
    inventory_mask = out[COL_TRAT_BAL_GAS].astype(str).eq("Inventario")
    out.loc[~inventory_mask, [COL_INV_MOV, COL_INV_ITEM]] = ["", ""]
    out.loc[inventory_mask & out[COL_INV_MOV].astype(str).str.strip().eq(""), COL_INV_MOV] = "Entrada"
    af_mask = out[COL_TRAT_BAL_GAS].astype(str).eq("Activo fijo")
    out.loc[~af_mask, [COL_AF_TOGGLE, COL_AF_TIPO, COL_AF_VIDA, COL_AF_FEC_INI, COL_AF_VAL_RES, COL_AF_DEP_TOGGLE, COL_AF_DEP_MENSUAL]] = ["No", "", 0, pd.NaT, 0.0, "No", 0.0]
    fin_mask = out[COL_FIN_TOGGLE].map(_bool_from_toggle)
    out.loc[~fin_mask, [COL_FIN_TIPO, COL_FIN_MONTO, COL_FIN_FEC_INI, COL_FIN_PLAZO, COL_FIN_TASA, COL_FIN_TASA_TIPO, COL_FIN_MODALIDAD, COL_FIN_PERIOD, COL_FIN_CRONO]] = ["", 0.0, pd.NaT, 0, 0.0, "", "", "", ""]
    out[COL_ROWID] = out.apply(_make_rowid, axis=1)
    return out


# -------------------- Normalizadores (Cat??logo) --------------------
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


def _on_client_change(prefix: str, *, mark_open: bool = True) -> None:
    label = st.session_state.get(f"{prefix}_cliente_raw", "")
    info = st.session_state.get("catalog_clients_label_map", {}).get(label)
    if info:
        st.session_state[f"{prefix}_cliente_id"] = info["ClienteID"]
        st.session_state[f"{prefix}_cliente_nombre"] = info["ClienteNombre"]
    else:
        st.session_state[f"{prefix}_cliente_id"] = ""
        st.session_state[f"{prefix}_cliente_nombre"] = ""
    if not st.session_state.pop(f"{prefix}_skip_project_sync", False):
        _sync_project_selection(prefix, mark_open=mark_open)
    if mark_open:
        _mark_form_force_open(prefix)


def _on_project_change(prefix: str, *, mark_open: bool = True) -> None:
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
                _on_client_change(prefix, mark_open=mark_open)
    else:
        st.session_state[f"{prefix}_proyecto_id"] = ""
        st.session_state[f"{prefix}_proyecto_nombre"] = ""
        st.session_state[f"{prefix}_proyecto_cliente_id"] = ""
        st.session_state[f"{prefix}_proyecto_cliente_nombre"] = ""
    if mark_open:
        _mark_form_force_open(prefix)


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


def _sync_project_selection(prefix: str, mark_open: bool = True) -> None:
    options = _build_project_options(prefix)
    key = f"{prefix}_proyecto_raw"
    if key not in st.session_state or st.session_state[key] not in options:
        st.session_state[key] = options[0] if options else ""
    if st.session_state.get(key):
        _on_project_change(prefix, mark_open=mark_open)


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


def _mark_form_force_open(prefix: str) -> None:
    st.session_state[f"{prefix}_force_open"] = True
    st.session_state[f"{prefix}_scroll_to"] = True


def _clear_form_force_open(prefix: str) -> None:
    st.session_state[f"{prefix}_force_open"] = False
    st.session_state[f"{prefix}_scroll_to"] = False


def _render_form_scroll_restore(anchor_id: str, should_scroll: bool) -> None:
    st.markdown(f'<div id="{anchor_id}"></div>', unsafe_allow_html=True)
    if not should_scroll:
        return
    components.html(
        f"""
        <script>
        const anchor = window.parent.document.getElementById("{anchor_id}");
        if (anchor) {{
          anchor.scrollIntoView({{behavior: "auto", block: "start"}});
        }}
        </script>
        """,
        height=0,
    )


def _ensure_client_selection(prefix: str, options: list[str]) -> None:
    """
    Asegura que el valor en session_state para el cliente pertenezca a `options`.
    Si no pertenece, se reajusta al primer valor y se sincroniza la info derivada.
    """
    key = f"{prefix}_cliente_raw"
    if key not in st.session_state or st.session_state[key] not in options:
        st.session_state[key] = options[0] if options else ""
    _on_client_change(prefix, mark_open=False)


def _prepare_entry_defaults(prefix: str) -> list[str]:
    client_opts = st.session_state.get("catalog_clients_opts", [""])
    _ensure_client_selection(prefix, client_opts)
    options = _build_project_options(prefix)
    proj_key = f"{prefix}_proyecto_raw"
    if proj_key not in st.session_state or st.session_state[proj_key] not in options:
        st.session_state[proj_key] = options[0] if options else ""
    if st.session_state.get(proj_key):
        _on_project_change(prefix, mark_open=False)
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
        "fecha_cobro_quick",
        "fecha_cobro_real_quick",
        "fecha_pago_quick",
        "fecha_pago_real_quick",
        "recurrente_quick",
        "rec_period_quick",
        "rec_rule_quick",
        "rec_duracion_quick",
        "rec_hasta_quick",
        "rec_cantidad_quick",
        "categoria_quick",
        "estado_quick",
        "detalle_ing_quick",
        "naturaleza_ing_quick",
        "trat_balance_ing_quick",
        "subclas_gas_quick",
        "detalle_gas_quick",
        "trat_balance_gas_quick",
        "activo_fijo_quick",
        "activo_tipo_quick",
        "activo_vida_quick",
        "activo_inicio_quick",
        "activo_residual_quick",
        "activo_dep_quick",
        "fin_toggle_quick",
        "fin_tipo_quick",
        "fin_monto_quick",
        "fin_fecha_inicio_quick",
        "fin_plazo_quick",
        "fin_tasa_quick",
        "fin_tasa_tipo_quick",
        "fin_modalidad_quick",
        "fin_periodicidad_quick",
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


def _ensure_operational_schema_persisted_once(client, sheet_id: str) -> None:
    """
    Garantiza que las hojas operativas tengan las columnas requeridas
    para Finanzas 2 (incluyendo fechas esperadas).
    """
    if st.session_state.get("finance_schema_persisted", False):
        return
    try:
        raw_ing = get_sheet_df_cached(sheet_id, WS_ING, st.session_state.google_cache_token)
        raw_gas = get_sheet_df_cached(sheet_id, WS_GAS, st.session_state.google_cache_token)

        norm_ing = ensure_ingresos_columns(raw_ing)
        norm_gas = ensure_gastos_columns(raw_gas)

        if set(norm_ing.columns) != set(raw_ing.columns):
            safe_write_worksheet(client, sheet_id, WS_ING, norm_ing, old_df=raw_ing, id_col=COL_ROWID)
        if set(norm_gas.columns) != set(raw_gas.columns):
            safe_write_worksheet(client, sheet_id, WS_GAS, norm_gas, old_df=raw_gas, id_col=COL_ROWID)
    except Exception:
        # Si falla, no bloquea la operativa.
        pass
    st.session_state["finance_schema_persisted"] = True

# Carga base
cache_token = st.session_state.google_cache_token
st.session_state.df_ing = load_norm_cached(SHEET_ID, WS_ING, True, cache_token)
st.session_state.df_gas = load_norm_cached(SHEET_ID, WS_GAS, False, cache_token)
_ensure_operational_schema_persisted_once(client, SHEET_ID)


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


# -------------------- KPIs principales --------------------
ing_total = (
    float(pd.to_numeric(df_ing_f.get(COL_COBRO_REAL_MONTO), errors="coerce").fillna(0.0).sum())
    if COL_COBRO_REAL_MONTO in df_ing_f.columns
    else 0.0
)
gas_total = (
    float(pd.to_numeric(df_gas_f.get(COL_PAGO_REAL_MONTO), errors="coerce").fillna(0.0).sum())
    if COL_PAGO_REAL_MONTO in df_gas_f.columns
    else 0.0
)

k1, k2 = st.columns(2)
with k1:
    st.markdown(
        '<div class="kpi-card"><p class="kpi-label">Ingresos (filtrados)</p>'
        f'<p class="kpi-value">{_format_money_es(ing_total)}</p></div>',
        unsafe_allow_html=True,
    )
with k2:
    st.markdown(
        '<div class="kpi-card"><p class="kpi-label">Gastos (filtrados)</p>'
        f'<p class="kpi-value">{_format_money_es(gas_total)}</p></div>',
        unsafe_allow_html=True,
    )

# ---- Flujo y saldo actual ----
cash = preparar_cashflow(df_ing_f, df_gas_f)
saldo_actual = float(cash["Saldo"].iloc[-1]) if not cash.empty else 0.0

# KPI: Capital actual + CxC futuras + CxP activas
cxp_activas = (
    float(
        (
            pd.to_numeric(df_gas_f.get(COL_MONTO), errors="coerce").fillna(0.0)
            - pd.to_numeric(df_gas_f.get(COL_PAGO_REAL_MONTO), errors="coerce").fillna(0.0)
        ).clip(lower=0.0).sum()
    )
    if not df_gas_f.empty
    else 0.0
)
cxc_futuras = (
    float(
        (
            pd.to_numeric(df_ing_f.get(COL_MONTO), errors="coerce").fillna(0.0)
            - pd.to_numeric(df_ing_f.get(COL_COBRO_REAL_MONTO), errors="coerce").fillna(0.0)
        ).clip(lower=0.0).sum()
    )
    if not df_ing_f.empty
    else 0.0
)

k1, k2, k3 = st.columns(3)
with k1: st.metric("Capital actual", _format_money_es(saldo_actual))
with k2: st.metric("Cuentas por cobrar", _format_money_es(cxc_futuras))
with k3: st.metric("Cuentas por pagar", _format_money_es(cxp_activas))

with st.expander("Informacion de interes", expanded=False):
    st.markdown("#### Reportes")
    st.markdown(
        "- `Flujo de caja actual`: usa fecha real de cobro/pago y cualquier movimiento real de dinero.\n"
        "- `Flujo de caja proyectado`: usa fechas esperadas, recurrencias y cronogramas futuros.\n"
        "- `Estado de resultados`: usa la fecha del hecho economico y la clasificacion gerencial.\n"
        "- `Balance general`: usa caja, cuentas abiertas, activos, pasivos y patrimonio estimado."
    )
    st.markdown("#### Informacion de interes")
    st.markdown(
        "- `Fecha del hecho economico`: sirve para resultados/devengo.\n"
        "- `Fecha esperada`: sirve para proyecciones.\n"
        "- `Fecha real`: sirve para flujo de caja real.\n"
        "- `Estado pendiente`: el cobro o pago aun no ocurre.\n"
        "- `Estado realizado`: el dinero ya entro o salio.\n"
        "- `Activo fijo`: compra que seguira dando valor en el tiempo.\n"
        "- `Financiamiento recibido`: entra caja y nace pasivo.\n"
        "- `Financiamiento otorgado`: sale caja y nace cuenta por cobrar.\n"
        "- `Entidad relacionada / contraparte`: quien esta del otro lado del movimiento. Ejemplos: banco, socio, empresa relacionada o empresa invertida.\n"
        "- `Con factoring`: el valor recibido inicial entra a caja; el retenido queda como activo hasta la liquidacion final.\n"
        "- `Gasto del periodo`: consumo del mismo periodo.\n"
        "- `Saldo acumulado`: caja acumulada despues de sumar todos los movimientos reales hasta la fecha.\n"
        "- `Flujo neto`: diferencia entre entradas y salidas del periodo analizado.\n"
        "- `Anticipo / prepago`: pago adelantado aun no consumido.\n"
        "- `Valor residual`: valor estimado del activo al final de su vida util."
    )
    st.markdown("#### Reglas automaticas clave")
    st.markdown(
        "- Ingresos normales: pendiente pide fecha esperada; realizado pide fecha real.\n"
        "- Gastos normales: pendiente pide fecha esperada; realizado pide fecha real.\n"
        "- Recurrencia: solo abre frecuencia y duracion si se marca `Si`.\n"
        "- Financiamiento: capital no va al resultado; solo intereses si van al resultado.\n"
        "- Activo fijo: no pega completo al gasto; pasa por depreciacion/amortizacion si aplica."
    )
    st.markdown("#### Pendiente para robustecer")
    st.markdown(
        "- Cierre mensual persistente.\n"
        "- Conciliacion bancaria.\n"
        "- Cantidades, costo unitario y valorizacion mas fina de inventario.\n"
        "- Factoring con recurso.\n"
        "- Proyeccion estimada del retenido cuando aun no existe liquidacion final.\n"
        "- Ajustes avanzados de valuacion para inversiones / participaciones."
    )

# La visualización analítica fue trasladada al "Panel Financiero Gerencial".


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
# INGRESOS - Anadir ingreso (rapido)
# ============================================================
st.markdown("## Ingresos")
ing_should_expand = st.session_state.pop("ing_force_open", False)
ing_should_scroll = st.session_state.pop("ing_scroll_to", False)
if ing_should_expand:
    st.session_state["ing_force_open"] = True
_render_form_scroll_restore("finance-ing-form-anchor", ing_should_scroll)
with st.expander("Anadir ingreso (rapido)", expanded=ing_should_expand):
    _prepare_entry_defaults("ing")
    if _bool_from_toggle(st.session_state.get("ing_recurrente_quick", "No")):
        st.session_state["ing_estado_quick"] = "Pendiente"
        st.session_state["ing_porcob_quick"] = YES_NO_OPTIONS[1]
    ing_categoria_state = st.session_state.get("ing_categoria_quick", ING_CATEGORY_OPTIONS[0])
    ing_fin_state = _bool_from_toggle(st.session_state.get("ing_fin_toggle_quick", "No")) or ing_categoria_state == "Financiamiento recibido"
    ing_monto_label = "Monto desembolsado / principal" if ing_fin_state else "Monto"

    st.markdown("#### Datos base")
    c1, c2, c3, c4, c5 = st.columns([1.0, 1.0, 1.0, 1.1, 1.0])
    with c1:
        empresa_ing = st.selectbox(
            "Empresa",
            EMPRESAS_OPCIONES,
            index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT),
            key="ing_empresa_quick",
            on_change=lambda: _mark_form_force_open("ing"),
        )
    with c2:
        fecha_nueva = st.date_input(
            "Fecha del hecho economico",
            value=_today(),
            key="ing_fecha_quick",
            on_change=lambda: _mark_form_force_open("ing"),
        )
    with c3:
        monto_nuevo = st.number_input(
            ing_monto_label,
            min_value=0.0,
            step=1.0,
            key="ing_monto_quick",
            on_change=lambda: _mark_form_force_open("ing"),
        )
    ing_recurrente_state = _bool_from_toggle(st.session_state.get("ing_recurrente_quick", "No"))
    if ing_recurrente_state:
        st.session_state["ing_estado_quick"] = "Pendiente"
    with c4:
        estado_ing = st.radio(
            "Estado",
            STATE_OPTIONS,
            horizontal=True,
            key="ing_estado_quick",
            disabled=ing_recurrente_state,
        )
    with c5:
        recurrente_ing = st.selectbox(
            "Recurrente",
            YES_NO_OPTIONS,
            index=0,
            key="ing_recurrente_quick",
            on_change=lambda: _mark_form_force_open("ing"),
        )

    if _bool_from_toggle(recurrente_ing):
        estado_ing = "Pendiente"

    c6, c7, c8 = st.columns([1, 1, 1])
    fecha_cobro_esperada = pd.NaT
    fecha_cobro_real = pd.NaT
    monto_cobrado_real = 0.0
    if estado_ing == "Pendiente":
        with c6:
            fecha_cobro_esperada = st.date_input(
                "Fecha esperada de cobro",
                value=_today(),
                key="ing_fecha_cobro_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
    elif estado_ing == "Parcial":
        with c6:
            fecha_cobro_esperada = st.date_input(
                "Fecha esperada de cobro",
                value=_today(),
                key="ing_fecha_cobro_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
        with c7:
            fecha_cobro_real = st.date_input(
                "Fecha real de cobro",
                value=_today(),
                key="ing_fecha_cobro_real_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
        with c8:
            monto_cobrado_real = st.number_input(
                "Monto real cobrado",
                min_value=0.0,
                max_value=float(monto_nuevo),
                step=1.0,
                key="ing_monto_cobrado_real_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
    else:
        with c6:
            fecha_cobro_real = st.date_input(
                "Fecha real de cobro",
                value=_today(),
                key="ing_fecha_cobro_real_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
        monto_cobrado_real = float(monto_nuevo)

    ing_company_code = (empresa_ing or EMPRESA_DEFAULT).strip().upper()
    client_options = _client_options_for_company(ing_company_code)
    _ensure_client_selection("ing", client_options)
    st.selectbox(
        "Cliente",
        client_options,
        key="ing_cliente_raw",
        on_change=lambda prefix="ing": _on_client_change(prefix, mark_open=True),
    )
    project_options = _build_project_options("ing")
    if st.session_state.get("ing_proyecto_raw") not in project_options:
        st.session_state["ing_proyecto_raw"] = project_options[0] if project_options else ""
    st.selectbox(
        "Proyecto",
        project_options,
        key="ing_proyecto_raw",
        on_change=lambda prefix="ing": _on_project_change(prefix, mark_open=True),
    )
    desc_nueva = st.text_input(
        "Descripcion",
        key="ing_desc_quick",
        on_change=lambda: _mark_form_force_open("ing"),
    )

    st.markdown("#### Recurrencia")
    rec_period_ing = ""
    rec_rule_ing = ""
    rec_dur_ing = ""
    rec_hasta_ing = pd.NaT
    rec_cant_ing = 0
    if _bool_from_toggle(recurrente_ing):
        r1, r2, r3 = st.columns([1, 1.2, 1])
        with r1:
            rec_period_ing = st.selectbox(
                "Frecuencia",
                REC_PERIOD_OPTIONS,
                index=0,
                key="ing_rec_period_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
        with r2:
            if rec_period_ing == "15nal":
                rec_rule_ing = "Dia 1 y 15 de cada mes"
                st.text_input("Regla fecha recurrencia", value=rec_rule_ing, disabled=True, key="ing_rec_rule_quick_locked")
            else:
                rec_rule_ing = st.selectbox(
                    "Regla fecha recurrencia",
                    [x for x in REC_RULE_OPTIONS if x != "Dia 1 y 15 de cada mes"],
                    index=0,
                    key="ing_rec_rule_quick",
                    on_change=lambda: _mark_form_force_open("ing"),
                )
        with r3:
            rec_dur_ing = st.selectbox(
                "Duracion",
                REC_DURATION_OPTIONS,
                index=0,
                key="ing_rec_duracion_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
        if rec_dur_ing == "Hasta fecha":
            rec_hasta_ing = st.date_input(
                "Recurrencia hasta fecha",
                value=_today(),
                key="ing_rec_hasta_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )
        elif rec_dur_ing == "Por cantidad de periodos":
            rec_cant_ing = st.number_input(
                "Cantidad de periodos",
                min_value=1,
                step=1,
                key="ing_rec_cantidad_quick",
                on_change=lambda: _mark_form_force_open("ing"),
            )

    st.markdown("#### Clasificacion del ingreso")
    categoria_ing = st.selectbox(
        "Categoria operativa",
        ING_CATEGORY_OPTIONS,
        index=0,
        key="ing_categoria_quick",
        help=_help_for_option(ING_CATEGORY_HELP, st.session_state.get("ing_categoria_quick", ING_CATEGORY_OPTIONS[0])),
        on_change=lambda: _mark_form_force_open("ing"),
    )
    detalle_ing = st.selectbox(
        "Detalle",
        ING_DETAIL_OPTIONS,
        index=0,
        key="ing_detalle_ing_quick",
        on_change=lambda: _mark_form_force_open("ing"),
    )
    cp1, cp2 = st.columns([1, 2])
    with cp1:
        tipo_contraparte_ing = st.selectbox(
            "Tipo de entidad relacionada / contraparte",
            CONTRAPARTE_TYPE_OPTIONS,
            index=0,
            key="ing_ctp_tipo_quick",
            help="Contraparte = la entidad del otro lado del movimiento. Ejemplos: socio, banco, empresa relacionada o empresa invertida.",
            on_change=lambda: _mark_form_force_open("ing"),
        )
    with cp2:
        contraparte_ing = st.text_input(
            "Entidad relacionada / contraparte",
            key="ing_ctp_nombre_quick",
            help="Nombre de la entidad del otro lado del movimiento. Ejemplos: Banco General, Socio A, RIR Medical.",
            on_change=lambda: _mark_form_force_open("ing"),
        )
    naturaleza_default = _derive_ing_nature(categoria_ing)
    naturaleza_ing = naturaleza_default
    st.text_input(
        "Naturaleza ingreso (automatica)",
        value=naturaleza_ing,
        disabled=True,
        help="Se deduce automaticamente desde Categoria operativa.",
    )

    st.markdown("#### Tratamiento en balance")
    balance_ing_default = _derive_ing_balance(categoria_ing, estado_ing)
    if categoria_ing in {"Aporte de socio / capital", "Financiamiento recibido"}:
        tratamiento_ing = balance_ing_default
        st.text_input(
            "Tratamiento balance ingreso (automatico)",
            value=tratamiento_ing,
            disabled=True,
            help="Se deduce automaticamente para categorias patrimoniales y de financiamiento.",
        )
    else:
        tratamiento_ing = st.selectbox(
            "Tratamiento balance ingreso",
            ING_BALANCE_OPTIONS,
            index=ING_BALANCE_OPTIONS.index(balance_ing_default if balance_ing_default in ING_BALANCE_OPTIONS else "Cuenta por cobrar"),
            key="ing_trat_balance_ing_quick",
            on_change=lambda: _mark_form_force_open("ing"),
        )

    st.markdown("#### Financiamiento")
    fin_ing_default = YES_NO_OPTIONS[1] if categoria_ing == "Financiamiento recibido" else YES_NO_OPTIONS[0]
    fin_ing_toggle = st.selectbox(
        "?Corresponde a financiamiento recibido?",
        YES_NO_OPTIONS,
        index=YES_NO_OPTIONS.index(fin_ing_default),
        key="ing_fin_toggle_quick",
        on_change=lambda: _mark_form_force_open("ing"),
    )
    fin_ing_on = _bool_from_toggle(fin_ing_toggle) or categoria_ing == "Financiamiento recibido"
    fin_tipo_ing = ""
    fin_monto_ing = 0.0
    fin_fecha_inicio_ing = pd.NaT
    fin_plazo_ing = 0
    fin_tasa_ing = 0.0
    fin_tasa_tipo_ing = "Anual"
    fin_modalidad_ing = "Cuotas periodicas"
    fin_periodicidad_ing = "Mensual"
    if fin_ing_on:
        f1, f2, f3 = st.columns(3)
        fin_monto_ing = float(monto_nuevo)
        with f1:
            fin_tipo_ing = st.selectbox("Tipo", ["Financiamiento recibido"], key="ing_fin_tipo_quick")
            st.caption(f"Se usa el monto base como principal/desembolso: {_format_money_es(fin_monto_ing)}")
            fin_fecha_inicio_ing = st.date_input("Fecha inicio", value=fecha_nueva, key="ing_fin_fecha_inicio_quick")
        with f2:
            fin_plazo_ing = st.number_input("Plazo en meses", min_value=1, step=1, value=1, key="ing_fin_plazo_quick")
            fin_tasa_ing = st.number_input("Tasa", min_value=0.0, step=0.1, key="ing_fin_tasa_quick")
            fin_tasa_tipo_ing = st.selectbox("Tipo de tasa", FIN_RATE_TYPE_OPTIONS, index=1, key="ing_fin_tasa_tipo_quick")
        with f3:
            fin_modalidad_ing = st.selectbox("Modalidad", FIN_MODALITY_OPTIONS, index=0, key="ing_fin_modalidad_quick")
            fin_periodicidad_ing = st.selectbox("Periodicidad", REC_PERIOD_OPTIONS, index=1, key="ing_fin_periodicidad_quick")
            st.caption("Capital no va al resultado; solo intereses si van al resultado.")

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

        rid = uuid.uuid4().hex
        estado_ing_final = "Pendiente" if _bool_from_toggle(recurrente_ing) else estado_ing
        por_cobrar_final = _estado_to_yes_no(estado_ing_final)
        cobrado = "No" if _si_no_norm(por_cobrar_final) != "No" else "Si"
        fecha_cobro = _ts(fecha_cobro_esperada) if estado_ing_final in {"Pendiente", "Parcial"} else pd.NaT
        fecha_real_cobro = _ts(fecha_cobro_real) if estado_ing_final in {"Parcial", "Realizado"} else pd.NaT
        categoria_final = "Financiamiento recibido" if fin_ing_on else categoria_ing
        naturaleza_final = "Financiamiento" if fin_ing_on else (naturaleza_ing or naturaleza_default)
        tratamiento_balance_final = "Pasivo financiero" if fin_ing_on else (tratamiento_ing or balance_ing_default)
        monto_cobrado_real_final = float(monto_nuevo) if estado_ing_final == "Realizado" else float(monto_cobrado_real or 0.0)
        if monto_cobrado_real_final > float(monto_nuevo):
            monto_cobrado_real_final = float(monto_nuevo)
        partial_events_ing = []
        if monto_cobrado_real_final > 0 and not pd.isna(fecha_real_cobro):
            partial_events_ing = [{
                "fecha": _ts(fecha_real_cobro),
                "monto": float(monto_cobrado_real_final),
                "nota": "Registro inicial",
            }]
        if _counterparty_required_for_ing(categoria_final, tratamiento_balance_final, fin_ing_on) and not str(contraparte_ing or "").strip():
            st.error("Debes indicar la contraparte para este tipo de ingreso.")
            st.stop()
        if estado_ing_final == "Parcial" and (monto_cobrado_real_final <= 0 or monto_cobrado_real_final >= float(monto_nuevo)):
            st.error("Para un ingreso parcial, el monto real cobrado debe ser mayor que 0 y menor que el monto total.")
            st.stop()
        cronograma_fin = _build_financing_schedule(
            principal=fin_monto_ing,
            fecha_inicio=fin_fecha_inicio_ing,
            plazo_meses=fin_plazo_ing,
            tasa=fin_tasa_ing,
            tasa_tipo=fin_tasa_tipo_ing,
            modalidad=fin_modalidad_ing,
            periodicidad=fin_periodicidad_ing,
        ) if fin_ing_on else ""

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
            COL_POR_COB: por_cobrar_final,
            COL_COB: cobrado,
            COL_FCOBRO: fecha_cobro,
            COL_FCOBRO_REAL: fecha_real_cobro,
            COL_CTP_TIPO: (tipo_contraparte_ing or "").strip(),
            COL_CTP_NOMBRE: (contraparte_ing or "").strip(),
            COL_COBRO_REAL_MONTO: float(monto_cobrado_real_final),
            COL_ING_PARTIALS: _serialize_partial_events(partial_events_ing),
            COL_REC: recurrente_ing,
            COL_REC_PER: rec_period_ing if _bool_from_toggle(recurrente_ing) else "",
            COL_REC_REG: rec_rule_ing if _bool_from_toggle(recurrente_ing) else "",
            COL_REC_DUR: rec_dur_ing if _bool_from_toggle(recurrente_ing) else "",
            COL_REC_HASTA: _ts(rec_hasta_ing) if _bool_from_toggle(recurrente_ing) and rec_dur_ing == "Hasta fecha" else pd.NaT,
            COL_REC_CANT: int(rec_cant_ing) if _bool_from_toggle(recurrente_ing) and rec_dur_ing == "Por cantidad de periodos" else 0,
            COL_CAT: categoria_final,
            COL_ING_DET: detalle_ing,
            COL_ING_NAT: naturaleza_final,
            COL_TRAT_BAL_ING: tratamiento_balance_final,
            COL_FIN_TOGGLE: YES_NO_OPTIONS[1] if fin_ing_on else YES_NO_OPTIONS[0],
            COL_FIN_TIPO: fin_tipo_ing if fin_ing_on else "",
            COL_FIN_MONTO: float(fin_monto_ing) if fin_ing_on else 0.0,
            COL_FIN_FEC_INI: _ts(fin_fecha_inicio_ing) if fin_ing_on else pd.NaT,
            COL_FIN_PLAZO: int(fin_plazo_ing) if fin_ing_on else 0,
            COL_FIN_TASA: float(fin_tasa_ing) if fin_ing_on else 0.0,
            COL_FIN_TASA_TIPO: fin_tasa_tipo_ing if fin_ing_on else "",
            COL_FIN_MODALIDAD: fin_modalidad_ing if fin_ing_on else "",
            COL_FIN_PERIOD: fin_periodicidad_ing if fin_ing_on else "",
            COL_FIN_CRONO: cronograma_fin,
            COL_ESC: "Real",
            COL_USER: _current_user(),
        }
        st.session_state.df_ing = pd.concat([st.session_state.df_ing, pd.DataFrame([nueva])], ignore_index=True)
        st.session_state.df_ing = ensure_ingresos_columns(st.session_state.df_ing)
        wrote = safe_write_worksheet(client, SHEET_ID, WS_ING, st.session_state.df_ing, old_df=df_ing_before)
        if wrote:
            st.cache_data.clear()
        _clear_form_force_open("ing")
        _reset_entry_state("ing")
        _safe_rerun()

# Tabla Ingresos (OCULTANDO "Concepto" en la vista)
st.markdown("### Ingresos (tabla)")
ing_cols_view = [c for c in df_ing_f.columns if c not in (COL_ROWID, COL_ESC, COL_CONC)] + [COL_ROWID]
ing_cat_options = [""] + ING_CATEGORY_OPTIONS.copy()
if COL_CAT in df_ing_f.columns:
    existing_cats = [
        c for c in df_ing_f[COL_CAT].fillna("").astype(str).str.strip().unique() if c
    ]
    for cat in existing_cats:
        if cat not in ing_cat_options:
            ing_cat_options.append(cat)
ing_colcfg = {
    COL_POR_COB: st.column_config.SelectboxColumn(COL_POR_COB, options=YES_NO_OPTIONS),
    COL_REC:     st.column_config.SelectboxColumn(COL_REC, options=YES_NO_OPTIONS),
    COL_REC_PER: st.column_config.SelectboxColumn(COL_REC_PER, options=REC_PERIOD_OPTIONS),
    COL_REC_REG: st.column_config.SelectboxColumn(COL_REC_REG, options=REC_RULE_OPTIONS),
    COL_REC_DUR: st.column_config.SelectboxColumn(COL_REC_DUR, options=REC_DURATION_OPTIONS),
    COL_FCOBRO:  st.column_config.DateColumn("Fecha esperada de cobro"),
    COL_FCOBRO_REAL: st.column_config.DateColumn("Fecha real de cobro"),
    COL_COBRO_REAL_MONTO: st.column_config.NumberColumn("Monto real cobrado", format="$%0.2f"),
    COL_CAT:     st.column_config.SelectboxColumn("Categoria operativa", options=ing_cat_options),
    COL_ING_DET: st.column_config.SelectboxColumn(COL_ING_DET, options=ING_DETAIL_OPTIONS),
    COL_ING_NAT: st.column_config.TextColumn("Naturaleza ingreso", disabled=True),
    COL_TRAT_BAL_ING: st.column_config.SelectboxColumn(COL_TRAT_BAL_ING, options=ING_BALANCE_OPTIONS),
    COL_CTP_TIPO: st.column_config.SelectboxColumn("Tipo entidad rel. / contraparte", options=CONTRAPARTE_TYPE_OPTIONS),
    COL_CTP_NOMBRE: st.column_config.TextColumn("Entidad rel. / contraparte"),
    COL_FIN_TOGGLE: st.column_config.SelectboxColumn(COL_FIN_TOGGLE, options=YES_NO_OPTIONS),
    COL_FIN_TIPO: st.column_config.SelectboxColumn(COL_FIN_TIPO, options=["", "Financiamiento recibido"]),
    COL_MONTO:   st.column_config.TextColumn(COL_MONTO, help="Formato: 1.500,00"),
    COL_DESC:    st.column_config.TextColumn(COL_DESC),
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
    COL_USER:    st.column_config.TextColumn(COL_USER, disabled=True),
}
df_ing_editor = df_ing_f[ing_cols_view].copy()
if COL_MONTO in df_ing_editor.columns:
    df_ing_editor[COL_MONTO] = df_ing_editor[COL_MONTO].map(_format_number_es)
ing_order = [x for x in [
    COL_FECHA, COL_MONTO, COL_CAT, COL_ING_DET, COL_ING_NAT, COL_TRAT_BAL_ING, COL_EMP,
    COL_POR_COB, COL_FCOBRO, COL_FCOBRO_REAL, COL_COBRO_REAL_MONTO, COL_CTP_TIPO, COL_CTP_NOMBRE,
    COL_REC, COL_REC_PER, COL_REC_REG, COL_REC_DUR,
    COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_DESC, COL_FIN_TOGGLE, COL_FIN_TIPO, COL_USER, COL_ROWID
] if x in ing_cols_view]
edited_ing = st.data_editor(
    df_ing_editor, num_rows="dynamic", hide_index=True, width="stretch",
    column_config=ing_colcfg, key="tabla_ingresos", column_order=ing_order
)
edited_ing = _editor_state_to_dataframe(
    df_ing_editor,
    "tabla_ingresos",
    numeric_cols={COL_MONTO, COL_COBRO_REAL_MONTO},
)
edited_ing = _autoderive_ing_df(edited_ing)
ing_table_errors = _validate_ing_df(edited_ing)

if COL_POR_COB in st.session_state.df_ing.columns and COL_FCOBRO in st.session_state.df_ing.columns:
    miss_cobro_mask = (
        st.session_state.df_ing[COL_POR_COB].map(_si_no_norm).ne("No")
        & st.session_state.df_ing[COL_FCOBRO].isna()
    )
    miss_cobro_count = int(miss_cobro_mask.sum())
    if miss_cobro_count > 0:
        st.warning(f"Hay {miss_cobro_count} ingreso(s) por cobrar sin Fecha de cobro.")
        if st.button("Completar Fecha de cobro faltante con Fecha del registro", key="btn_fill_ing_fechacobro"):
            st.session_state.df_ing.loc[miss_cobro_mask, COL_FCOBRO] = st.session_state.df_ing.loc[miss_cobro_mask, COL_FECHA]
            st.session_state.df_ing = ensure_ingresos_columns(st.session_state.df_ing)
            wrote = safe_write_worksheet(client, SHEET_ID, WS_ING, st.session_state.df_ing, old_df=df_ing_before)
            if wrote:
                st.cache_data.clear()
            st.success("Fechas de cobro completadas para ingresos pendientes.")
            _safe_rerun()

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
if ing_table_errors:
    _render_table_error_block("Ingresos", ing_table_errors[:50])
else:
    sync_cambios(
        edited_df=edited_ing, filtered_df=df_ing_f,
        base_df_key="df_ing", worksheet_name=WS_ING,
        session_state=st.session_state, write_worksheet=write_worksheet,
        client=client, sheet_id=SHEET_ID, id_column=COL_ROWID,
        ensure_columns_fn=_autoderive_ing_df,
    )

with st.expander("Registrar cobro parcial", expanded=False):
    ing_base_partial = ensure_ingresos_columns(st.session_state.df_ing.copy())
    ing_pending_view = ing_base_partial[
        (
            pd.to_numeric(ing_base_partial.get(COL_MONTO), errors="coerce").fillna(0.0)
            - pd.to_numeric(ing_base_partial.get(COL_COBRO_REAL_MONTO), errors="coerce").fillna(0.0)
        ).clip(lower=0.0) > 0
    ].copy()
    if ing_pending_view.empty:
        st.info("No hay ingresos con saldo pendiente para registrar cobros parciales.")
    else:
        ing_pending_view["__saldo_pendiente"] = (
            pd.to_numeric(ing_pending_view[COL_MONTO], errors="coerce").fillna(0.0)
            - pd.to_numeric(ing_pending_view[COL_COBRO_REAL_MONTO], errors="coerce").fillna(0.0)
        ).clip(lower=0.0)
        ing_options = {
            f"{str(row.get(COL_DESC, '')).strip() or str(row.get(COL_ROWID, ''))} | pendiente {_format_money_es(row['__saldo_pendiente'])}": str(row.get(COL_ROWID, ""))
            for _, row in ing_pending_view.iterrows()
        }
        ing_sel_label = st.selectbox("Ingreso con saldo pendiente", list(ing_options.keys()), key="ing_partial_row_select")
        ing_sel_id = ing_options.get(ing_sel_label, "")
        ing_row = ing_pending_view[ing_pending_view[COL_ROWID].astype(str) == ing_sel_id].iloc[0]
        current_events = _seed_partial_events_from_row(ing_row, COL_COBRO_REAL_MONTO, COL_FCOBRO_REAL)
        saldo_pendiente = float(ing_row["__saldo_pendiente"])
        p1, p2, p3 = st.columns([1, 1, 2])
        with p1:
            monto_parcial = st.number_input("Monto a registrar", min_value=0.01, max_value=max(0.01, saldo_pendiente), step=1.0, key="ing_partial_amount")
        with p2:
            fecha_parcial = st.date_input("Fecha real del cobro", value=_today(), key="ing_partial_date")
        with p3:
            nota_parcial = st.text_input("Nota del cobro", key="ing_partial_note")
        st.caption(f"Total ingreso: {_format_money_es(ing_row.get(COL_MONTO, 0.0))} | Cobrado acumulado: {_format_money_es(ing_row.get(COL_COBRO_REAL_MONTO, 0.0))} | Pendiente: {_format_money_es(saldo_pendiente)}")
        if current_events:
            st.dataframe(
                pd.DataFrame(
                    [{"Fecha": _ts(evt["fecha"]), "Monto": float(evt["monto"]), "Nota": evt.get("nota", "")} for evt in current_events]
                ),
                use_container_width=True,
                hide_index=True,
                column_config={"Fecha": st.column_config.DateColumn("Fecha"), "Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
            )
        if st.button("Guardar cobro parcial", key="btn_guardar_ing_partial"):
            updated = st.session_state.df_ing.copy()
            mask = updated[COL_ROWID].astype(str) == ing_sel_id
            if mask.any():
                row = updated.loc[mask].iloc[0]
                events = _seed_partial_events_from_row(row, COL_COBRO_REAL_MONTO, COL_FCOBRO_REAL)
                events.append({"fecha": _ts(fecha_parcial), "monto": float(monto_parcial), "nota": str(nota_parcial or "").strip()})
                total_real, last_real = _partial_events_summary(events)
                total_monto = float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
                total_real = min(total_real, total_monto)
                updated.loc[mask, COL_ING_PARTIALS] = _serialize_partial_events(events)
                updated.loc[mask, COL_COBRO_REAL_MONTO] = total_real
                updated.loc[mask, COL_FCOBRO_REAL] = last_real
                updated.loc[mask, COL_POR_COB] = "No" if total_real >= total_monto - 0.01 else "Sí"
                updated = ensure_ingresos_columns(updated)
                wrote = safe_write_worksheet(client, SHEET_ID, WS_ING, updated, old_df=df_ing_before)
                if wrote:
                    st.session_state.df_ing = updated
                    st.cache_data.clear()
                st.success("Cobro parcial registrado.")
                _safe_rerun()

with st.expander("Registrar con factoring", expanded=False):
    st.caption(
        "Usa esta seccion cuando una cuenta por cobrar propia entra con factoring. "
        "El sistema registra el dinero recibido inicial, el retenido y la comision inicial."
    )
    st.caption(
        "Regla actual: saldo pendiente con factoring = dinero recibido inicial + retenido + comision inicial. "
        "Luego el retenido se liquida en una segunda seccion."
    )
    ing_base_fact = ensure_ingresos_columns(st.session_state.df_ing.copy())
    if COL_FACT_DET not in ing_base_fact.columns:
        ing_base_fact[COL_FACT_DET] = ""
    ing_fact_view = ing_base_fact[
        (
            pd.to_numeric(ing_base_fact.get(COL_MONTO), errors="coerce").fillna(0.0)
            - pd.to_numeric(ing_base_fact.get(COL_COBRO_REAL_MONTO), errors="coerce").fillna(0.0)
        ).clip(lower=0.0) > 0
    ].copy()
    ing_fact_view = ing_fact_view[~ing_fact_view[COL_FACT_DET].map(_has_factoring)].copy()
    if ing_fact_view.empty:
        st.info("No hay cuentas por cobrar pendientes disponibles para registrar con factoring.")
    else:
        ing_fact_view["__saldo_pendiente"] = (
            pd.to_numeric(ing_fact_view[COL_MONTO], errors="coerce").fillna(0.0)
            - pd.to_numeric(ing_fact_view[COL_COBRO_REAL_MONTO], errors="coerce").fillna(0.0)
        ).clip(lower=0.0)
        fact_options = {
            f"{str(row.get(COL_DESC, '')).strip() or str(row.get(COL_ROWID, ''))} | saldo {_format_money_es(row['__saldo_pendiente'])}": str(row.get(COL_ROWID, ""))
            for _, row in ing_fact_view.iterrows()
        }
        fact_sel_label = st.selectbox("Cuenta por cobrar a registrar con factoring", list(fact_options.keys()), key="ing_factoring_row_select")
        fact_sel_id = fact_options.get(fact_sel_label, "")
        fact_row = ing_fact_view[ing_fact_view[COL_ROWID].astype(str) == fact_sel_id].iloc[0]
        saldo_fact = float(fact_row["__saldo_pendiente"])
        existing_real = float(pd.to_numeric(pd.Series([fact_row.get(COL_COBRO_REAL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        current_events = _seed_partial_events_from_row(fact_row, COL_COBRO_REAL_MONTO, COL_FCOBRO_REAL)

        f1, f2 = st.columns([1, 1])
        with f1:
            st.selectbox(
                "Tipo de operacion",
                ["Con factoring sin recurso"],
                index=0,
                key="ing_factoring_tipo",
            )
            factor_tipo_ctp_default = CONTRAPARTE_TYPE_OPTIONS.index("Tercero") if "Tercero" in CONTRAPARTE_TYPE_OPTIONS else 0
            factor_tipo_ctp = st.selectbox(
                "Tipo de entidad relacionada / contraparte",
                CONTRAPARTE_TYPE_OPTIONS,
                index=factor_tipo_ctp_default,
                key="ing_factoring_ctp_tipo",
            )
            factor_nombre = st.text_input(
                "Empresa de factoring / contraparte",
                key="ing_factoring_ctp_nombre",
                help="Nombre de la empresa de factoring que desembolsa el anticipo.",
            )
        with f2:
            fecha_factoring = st.date_input("Fecha de desembolso inicial con factoring", value=_today(), key="ing_factoring_fecha")
            neto_factoring = st.number_input(
                "Valor recibido inicial",
                min_value=0.0,
                max_value=max(0.0, saldo_fact),
                step=1.0,
                key="ing_factoring_neto",
            )
            retenido_factoring = st.number_input(
                "Monto retenido",
                min_value=0.0,
                max_value=max(0.0, saldo_fact),
                step=1.0,
                key="ing_factoring_retenido",
            )
            comision_factoring = st.number_input(
                "Comision inicial",
                min_value=0.0,
                max_value=max(0.0, saldo_fact),
                step=1.0,
                key="ing_factoring_comision",
            )
        nota_factoring = st.text_area(
            "Observacion / nota",
            key="ing_factoring_nota",
            placeholder="Ej: con factoring factura hospital X; retencion temporal hasta pago del deudor.",
        )
        st.caption(
            f"Monto total cuenta: {_format_money_es(fact_row.get(COL_MONTO, 0.0))} | "
            f"Cobrado previo: {_format_money_es(existing_real)} | "
            f"Saldo que entra con factoring: {_format_money_es(saldo_fact)}"
        )
        st.caption(
            f"Validacion: recibido inicial {_format_money_es(neto_factoring)} + "
            f"retenido {_format_money_es(retenido_factoring)} + "
            f"comision inicial {_format_money_es(comision_factoring)} = "
            f"{_format_money_es(float(neto_factoring) + float(retenido_factoring) + float(comision_factoring))}"
        )
        if current_events:
            st.dataframe(
                pd.DataFrame(
                    [{"Fecha": _ts(evt["fecha"]), "Monto": float(evt["monto"]), "Nota": evt.get("nota", "")} for evt in current_events]
                ),
                use_container_width=True,
                hide_index=True,
                column_config={"Fecha": st.column_config.DateColumn("Fecha"), "Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
            )
        if st.button("Guardar con factoring", key="btn_guardar_factoring"):
            if not str(factor_nombre or "").strip():
                st.error("Debes indicar la empresa de factoring / contraparte.")
                st.stop()
            if abs((float(neto_factoring) + float(retenido_factoring) + float(comision_factoring)) - float(saldo_fact)) > 0.01:
                st.error("Valor recibido inicial + retenido + comision inicial debe ser igual al saldo que entra con factoring.")
                st.stop()
            if float(neto_factoring) <= 0:
                st.error("El valor recibido inicial debe ser mayor que cero.")
                st.stop()

            old_ing_df = st.session_state.df_ing.copy()
            old_gas_df = st.session_state.df_gas.copy()
            new_ing_df = old_ing_df.copy()
            new_gas_df = old_gas_df.copy()
            mask_ing = new_ing_df[COL_ROWID].astype(str) == fact_sel_id
            if not mask_ing.any():
                st.error("No se encontro la cuenta por cobrar seleccionada.")
                st.stop()

            base_row = new_ing_df.loc[mask_ing].iloc[0]
            fact_events = _seed_partial_events_from_row(base_row, COL_COBRO_REAL_MONTO, COL_FCOBRO_REAL)
            note_parts = [
                f"Con factoring - valor recibido inicial {_format_money_es(neto_factoring)}",
                f"retenido {_format_money_es(retenido_factoring)}",
                f"comision inicial {_format_money_es(comision_factoring)}",
                f"contraparte {str(factor_nombre).strip()}",
            ]
            if str(nota_factoring or "").strip():
                note_parts.append(str(nota_factoring or "").strip())
            fact_events.append(
                {
                    "fecha": _ts(fecha_factoring),
                    "monto": float(neto_factoring),
                    "nota": " | ".join(note_parts),
                }
            )
            total_real, last_real = _partial_events_summary(fact_events)
            total_monto = float(pd.to_numeric(pd.Series([base_row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            total_real = min(total_real, total_monto)
            factoring_detail = {
                "modo": "con_factoring_sin_recurso",
                "contraparte_tipo": str(factor_tipo_ctp or "").strip(),
                "contraparte": str(factor_nombre or "").strip(),
                "fecha_inicio": _ts(fecha_factoring),
                "fecha_liquidacion_final": pd.NaT,
                "factored_amount": float(saldo_fact),
                "initial_cash_received": float(neto_factoring),
                "initial_retained": float(retenido_factoring),
                "initial_fee": float(comision_factoring),
                "final_cash_received": 0.0,
                "final_fee": 0.0,
                "nota": str(nota_factoring or "").strip(),
            }
            new_ing_df.loc[mask_ing, COL_ING_PARTIALS] = _serialize_partial_events(fact_events)
            new_ing_df.loc[mask_ing, COL_COBRO_REAL_MONTO] = float(total_real)
            new_ing_df.loc[mask_ing, COL_FCOBRO_REAL] = last_real
            new_ing_df.loc[mask_ing, COL_POR_COB] = "No"
            new_ing_df.loc[mask_ing, COL_COB] = "Si" if float(retenido_factoring) <= 0.01 else "No"
            new_ing_df.loc[mask_ing, COL_CTP_TIPO] = str(factor_tipo_ctp or "").strip()
            new_ing_df.loc[mask_ing, COL_CTP_NOMBRE] = str(factor_nombre or "").strip()
            new_ing_df.loc[mask_ing, COL_FACT_DET] = _serialize_factoring_detail(factoring_detail)
            new_ing_df = ensure_ingresos_columns(new_ing_df)

            if float(comision_factoring) > 0:
                factoring_fee_row = _build_factoring_fee_row(
                    base_row=base_row,
                    factor_nombre=str(factor_nombre or "").strip(),
                    factor_tipo_ctp=str(factor_tipo_ctp or "").strip(),
                    fecha_evento=fecha_factoring,
                    monto_fee=float(comision_factoring),
                    fee_label="Comision factoring inicial",
                )
                new_gas_df = pd.concat([new_gas_df, pd.DataFrame([factoring_fee_row])], ignore_index=True)
                new_gas_df = ensure_gastos_columns(new_gas_df)

            wrote_ing = safe_write_worksheet(client, SHEET_ID, WS_ING, new_ing_df, old_df=old_ing_df)
            if not wrote_ing:
                st.error("No se pudo actualizar la cuenta por cobrar para registrar la operacion con factoring.")
                st.stop()

            wrote_gas = True
            if float(comision_factoring) > 0:
                wrote_gas = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                if not wrote_gas:
                    rollback_ok = safe_write_worksheet(client, SHEET_ID, WS_ING, old_ing_df, old_df=new_ing_df)
                    if rollback_ok:
                        st.error("No se pudo guardar la comision inicial. Se revirtio la operacion con factoring.")
                    else:
                        st.error("No se pudo guardar la comision inicial y tampoco se pudo revertir automaticamente el ingreso. Revisa ambas hojas.")
                    st.stop()

            st.session_state.df_ing = new_ing_df
            st.session_state.df_gas = new_gas_df
            st.cache_data.clear()
            if float(retenido_factoring) > 0:
                st.success("Operacion con factoring registrada. El retenido queda pendiente para liquidacion final.")
            else:
                st.success("Operacion con factoring registrada y liquidada sin retenido.")
            _safe_rerun()

with st.expander("Liquidar retenido con factoring", expanded=False):
    st.caption(
        "Usa esta seccion cuando la empresa de factoring libera el retenido. "
        "Aqui registras el valor final recibido y la comision final generada por el tiempo transcurrido."
    )
    ing_base_ret = ensure_ingresos_columns(st.session_state.df_ing.copy())
    if COL_FACT_DET not in ing_base_ret.columns:
        ing_base_ret[COL_FACT_DET] = ""
    ing_base_ret["__factoring_det"] = ing_base_ret[COL_FACT_DET].map(_parse_factoring_detail)
    ing_base_ret["__retenido_pendiente"] = ing_base_ret["__factoring_det"].map(_factoring_retained_pending)
    ing_ret_view = ing_base_ret[ing_base_ret["__retenido_pendiente"] > 0.01].copy()
    if ing_ret_view.empty:
        st.info("No hay retenidos con factoring pendientes de liquidar.")
    else:
        ret_options = {
            f"{str(row.get(COL_DESC, '')).strip() or str(row.get(COL_ROWID, ''))} | retenido pendiente {_format_money_es(row['__retenido_pendiente'])}": str(row.get(COL_ROWID, ""))
            for _, row in ing_ret_view.iterrows()
        }
        ret_sel_label = st.selectbox("Operacion con factoring", list(ret_options.keys()), key="ing_factoring_ret_row_select")
        ret_sel_id = ret_options.get(ret_sel_label, "")
        ret_row = ing_ret_view[ing_ret_view[COL_ROWID].astype(str) == ret_sel_id].iloc[0]
        ret_detail = ret_row["__factoring_det"]
        retenido_pendiente = float(ret_row["__retenido_pendiente"])
        factor_nombre = str(ret_detail.get("contraparte", "") or ret_row.get(COL_CTP_NOMBRE, "")).strip()
        factor_tipo_ctp = str(ret_detail.get("contraparte_tipo", "") or ret_row.get(COL_CTP_TIPO, "")).strip()

        r1, r2 = st.columns([1, 1])
        with r1:
            fecha_liq_fact = st.date_input("Fecha de liquidacion final", value=_today(), key="ing_factoring_liq_fecha")
            neto_liq_fact = st.number_input(
                "Valor final recibido",
                min_value=0.0,
                max_value=max(0.0, retenido_pendiente),
                step=1.0,
                key="ing_factoring_liq_neto",
            )
        with r2:
            comision_liq_fact = st.number_input(
                "Comision final",
                min_value=0.0,
                max_value=max(0.0, retenido_pendiente),
                step=1.0,
                key="ing_factoring_liq_comision",
            )
            nota_liq_fact = st.text_area(
                "Observacion / nota liquidacion final",
                key="ing_factoring_liq_nota",
                placeholder="Ej: retenido liberado luego del pago del deudor.",
            )

        st.caption(
            f"Factor: {factor_nombre or 'Sin contraparte'} | "
            f"Factoring inicial: recibido {_format_money_es(ret_detail.get('initial_cash_received', 0.0))} | "
            f"retenido inicial {_format_money_es(ret_detail.get('initial_retained', 0.0))} | "
            f"retenido pendiente {_format_money_es(retenido_pendiente)}"
        )
        st.caption(
            f"Validacion: valor final recibido {_format_money_es(neto_liq_fact)} + "
            f"comision final {_format_money_es(comision_liq_fact)} = "
            f"{_format_money_es(float(neto_liq_fact) + float(comision_liq_fact))}"
        )

        if st.button("Guardar liquidacion final con factoring", key="btn_guardar_factoring_liq"):
            if abs((float(neto_liq_fact) + float(comision_liq_fact)) - float(retenido_pendiente)) > 0.01:
                st.error("Valor final recibido + comision final debe ser igual al retenido pendiente.")
                st.stop()
            if float(neto_liq_fact) <= 0 and float(comision_liq_fact) <= 0:
                st.error("Debes registrar al menos valor final recibido o comision final.")
                st.stop()

            old_ing_df = st.session_state.df_ing.copy()
            old_gas_df = st.session_state.df_gas.copy()
            new_ing_df = old_ing_df.copy()
            new_gas_df = old_gas_df.copy()
            mask_ing = new_ing_df[COL_ROWID].astype(str) == ret_sel_id
            if not mask_ing.any():
                st.error("No se encontro la operacion seleccionada.")
                st.stop()

            base_row = new_ing_df.loc[mask_ing].iloc[0]
            current_detail = _parse_factoring_detail(base_row.get(COL_FACT_DET, ""))
            if not current_detail:
                st.error("La fila seleccionada no contiene una operacion con factoring valida.")
                st.stop()
            fact_events = _seed_partial_events_from_row(base_row, COL_COBRO_REAL_MONTO, COL_FCOBRO_REAL)
            if float(neto_liq_fact) > 0:
                note_liq_parts = [
                    f"Liquidacion final factoring - valor recibido {_format_money_es(neto_liq_fact)}",
                    f"comision final {_format_money_es(comision_liq_fact)}",
                    f"contraparte {factor_nombre}",
                ]
                if str(nota_liq_fact or "").strip():
                    note_liq_parts.append(str(nota_liq_fact or "").strip())
                fact_events.append(
                    {
                        "fecha": _ts(fecha_liq_fact),
                        "monto": float(neto_liq_fact),
                        "nota": " | ".join(note_liq_parts),
                    }
                )
            total_real, last_real = _partial_events_summary(fact_events)
            total_monto = float(pd.to_numeric(pd.Series([base_row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            total_real = min(total_real, total_monto)
            current_detail["fecha_liquidacion_final"] = _ts(fecha_liq_fact)
            current_detail["final_cash_received"] = float(neto_liq_fact)
            current_detail["final_fee"] = float(comision_liq_fact)
            nota_actual = str(current_detail.get("nota", "") or "").strip()
            nota_liq_text = str(nota_liq_fact or "").strip()
            if nota_liq_text:
                current_detail["nota"] = f"{nota_actual} | Liquidacion final: {nota_liq_text}" if nota_actual else f"Liquidacion final: {nota_liq_text}"

            new_ing_df.loc[mask_ing, COL_ING_PARTIALS] = _serialize_partial_events(fact_events)
            new_ing_df.loc[mask_ing, COL_COBRO_REAL_MONTO] = float(total_real)
            new_ing_df.loc[mask_ing, COL_FCOBRO_REAL] = last_real
            new_ing_df.loc[mask_ing, COL_POR_COB] = "No"
            new_ing_df.loc[mask_ing, COL_COB] = "Si"
            new_ing_df.loc[mask_ing, COL_CTP_TIPO] = factor_tipo_ctp
            new_ing_df.loc[mask_ing, COL_CTP_NOMBRE] = factor_nombre
            new_ing_df.loc[mask_ing, COL_FACT_DET] = _serialize_factoring_detail(current_detail)
            new_ing_df = ensure_ingresos_columns(new_ing_df)

            if float(comision_liq_fact) > 0:
                factoring_fee_row = _build_factoring_fee_row(
                    base_row=base_row,
                    factor_nombre=factor_nombre,
                    factor_tipo_ctp=factor_tipo_ctp,
                    fecha_evento=fecha_liq_fact,
                    monto_fee=float(comision_liq_fact),
                    fee_label="Comision factoring final",
                )
                new_gas_df = pd.concat([new_gas_df, pd.DataFrame([factoring_fee_row])], ignore_index=True)
                new_gas_df = ensure_gastos_columns(new_gas_df)

            wrote_ing = safe_write_worksheet(client, SHEET_ID, WS_ING, new_ing_df, old_df=old_ing_df)
            if not wrote_ing:
                st.error("No se pudo actualizar la operacion con factoring.")
                st.stop()

            wrote_gas = True
            if float(comision_liq_fact) > 0:
                wrote_gas = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                if not wrote_gas:
                    rollback_ok = safe_write_worksheet(client, SHEET_ID, WS_ING, old_ing_df, old_df=new_ing_df)
                    if rollback_ok:
                        st.error("No se pudo guardar la comision final. Se revirtio la liquidacion final del retenido.")
                    else:
                        st.error("No se pudo guardar la comision final y tampoco se pudo revertir automaticamente el ingreso. Revisa ambas hojas.")
                    st.stop()

            st.session_state.df_ing = new_ing_df
            st.session_state.df_gas = new_gas_df
            st.cache_data.clear()
            st.success("Liquidacion final con factoring registrada.")
            _safe_rerun()


# ============================================================
# GASTOS - Anadir gasto (rapido)
# ============================================================
st.markdown("## Gastos")
gas_should_expand = st.session_state.pop("gas_force_open", False)
gas_should_scroll = st.session_state.pop("gas_scroll_to", False)
if gas_should_expand:
    st.session_state["gas_force_open"] = True
_render_form_scroll_restore("finance-gas-form-anchor", gas_should_scroll)
with st.expander("Anadir gasto (rapido)", expanded=gas_should_expand):
    _prepare_entry_defaults("gas")
    if _bool_from_toggle(st.session_state.get("gas_recurrente_quick", "No")):
        st.session_state["gas_estado_quick"] = "Pendiente"
        st.session_state["gas_porpag_quick"] = YES_NO_OPTIONS[1]
    gas_trat_state = st.session_state.get("gas_trat_balance_gas_quick", "Gasto del periodo")
    gas_fin_state = _bool_from_toggle(st.session_state.get("gas_fin_toggle_quick", "No"))
    gas_monto_label = "Monto desembolsado / principal" if (gas_fin_state and gas_trat_state == "Cuenta por cobrar / prestamo otorgado") else "Monto"

    st.markdown("#### Datos base")
    c1, c2, c3, c4, c5 = st.columns([1.0, 1.0, 1.0, 1.1, 1.0])
    with c1:
        empresa_g = st.selectbox(
            "Empresa",
            EMPRESAS_OPCIONES,
            index=EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT),
            key="gas_empresa_quick",
            on_change=lambda: _mark_form_force_open("gas"),
        )
    with c2:
        fecha_g = st.date_input(
            "Fecha del hecho economico",
            value=_today(),
            key="gas_fecha_quick",
            on_change=lambda: _mark_form_force_open("gas"),
        )
    with c3:
        monto_g = st.number_input(
            gas_monto_label,
            min_value=0.0,
            step=1.0,
            key="gas_monto_quick",
            on_change=lambda: _mark_form_force_open("gas"),
        )
    gas_recurrente_state = _bool_from_toggle(st.session_state.get("gas_recurrente_quick", "No"))
    if gas_recurrente_state:
        st.session_state["gas_estado_quick"] = "Pendiente"
    with c4:
        estado_g = st.radio(
            "Estado",
            STATE_OPTIONS,
            horizontal=True,
            key="gas_estado_quick",
            disabled=gas_recurrente_state,
        )
    with c5:
        recurrente_gas = st.selectbox(
            "Recurrente",
            YES_NO_OPTIONS,
            index=0,
            key="gas_recurrente_quick",
            on_change=lambda: _mark_form_force_open("gas"),
        )

    if _bool_from_toggle(recurrente_gas):
        estado_g = "Pendiente"

    c6, c7, c8 = st.columns([1, 1, 1])
    fecha_pago_esperada = pd.NaT
    fecha_pago_real = pd.NaT
    monto_pagado_real = 0.0
    if estado_g == "Pendiente":
        with c6:
            fecha_pago_esperada = st.date_input(
                "Fecha esperada de pago",
                value=_today(),
                key="gas_fecha_pago_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
    elif estado_g == "Parcial":
        with c6:
            fecha_pago_esperada = st.date_input(
                "Fecha esperada de pago",
                value=_today(),
                key="gas_fecha_pago_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        with c7:
            fecha_pago_real = st.date_input(
                "Fecha real de pago",
                value=_today(),
                key="gas_fecha_pago_real_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        with c8:
            monto_pagado_real = st.number_input(
                "Monto real pagado",
                min_value=0.0,
                max_value=float(monto_g),
                step=1.0,
                key="gas_monto_pagado_real_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
    else:
        with c6:
            fecha_pago_real = st.date_input(
                "Fecha real de pago",
                value=_today(),
                key="gas_fecha_pago_real_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        monto_pagado_real = float(monto_g)

    categoria_g = st.selectbox(
        "Categoria operativa",
        GAS_CATEGORY_OPTIONS,
        index=0,
        key="gas_categoria_quick",
        help=_help_for_option(GAS_CATEGORY_HELP, st.session_state.get("gas_categoria_quick", GAS_CATEGORY_OPTIONS[0])),
        on_change=lambda: _mark_form_force_open("gas"),
    )
    if categoria_g == "Inversiones":
        if st.session_state.get("gas_subclas_gas_quick", "") in {"", "Operativo variable"}:
            st.session_state["gas_subclas_gas_quick"] = "No operativo"
        if st.session_state.get("gas_trat_balance_gas_quick", "") in {"", "Gasto del periodo"}:
            st.session_state["gas_trat_balance_gas_quick"] = "Inversion / participacion en otra empresa"

    gas_company_code = (empresa_g or EMPRESA_DEFAULT).strip().upper()
    gas_client_options = _client_options_for_company(gas_company_code)
    _ensure_client_selection("gas", gas_client_options)

    if categoria_g == "Proyectos":
        st.selectbox(
            "Cliente",
            gas_client_options,
            key="gas_cliente_raw",
            on_change=lambda prefix="gas": _on_client_change(prefix, mark_open=True),
        )
        gas_project_options = _build_project_options("gas")
        if st.session_state.get("gas_proyecto_raw") not in gas_project_options:
            st.session_state["gas_proyecto_raw"] = gas_project_options[0] if gas_project_options else ""
        st.selectbox(
            "Proyecto",
            gas_project_options,
            key="gas_proyecto_raw",
            on_change=lambda prefix="gas": _on_project_change(prefix, mark_open=True),
        )
    else:
        cliente_id_g = ""
        cliente_nombre_g = ""
        proyecto_id_g = ""

    prov_g = st.text_input(
        "Proveedor",
        key="gas_proveedor_quick",
        on_change=lambda: _mark_form_force_open("gas"),
    )
    cp1, cp2 = st.columns([1, 2])
    with cp1:
        tipo_contraparte_g = st.selectbox(
            "Tipo de entidad relacionada / contraparte",
            CONTRAPARTE_TYPE_OPTIONS,
            index=0,
            key="gas_ctp_tipo_quick",
            help="Contraparte = la entidad del otro lado del movimiento. Ejemplos: banco, socio, empresa relacionada o empresa invertida.",
            on_change=lambda: _mark_form_force_open("gas"),
        )
    with cp2:
        contraparte_g = st.text_input(
            "Entidad relacionada / contraparte",
            key="gas_ctp_nombre_quick",
            help="Nombre de la entidad del otro lado del movimiento. Ejemplos: Banco General, RIR Medical, Socio A.",
            on_change=lambda: _mark_form_force_open("gas"),
        )
    desc_g = st.text_input(
        "Descripcion",
        key="gas_desc_quick",
        on_change=lambda: _mark_form_force_open("gas"),
    )

    st.markdown("#### Recurrencia")
    rec_period_gas = ""
    rec_rule_gas = ""
    rec_dur_gas = ""
    rec_hasta_gas = pd.NaT
    rec_cant_gas = 0
    if _bool_from_toggle(recurrente_gas):
        r1, r2, r3 = st.columns([1, 1.2, 1])
        with r1:
            rec_period_gas = st.selectbox(
                "Frecuencia",
                REC_PERIOD_OPTIONS,
                index=0,
                key="gas_rec_period_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        with r2:
            if rec_period_gas == "15nal":
                rec_rule_gas = "Dia 1 y 15 de cada mes"
                st.text_input("Regla fecha recurrencia", value=rec_rule_gas, disabled=True, key="gas_rec_rule_quick_locked")
            else:
                rec_rule_gas = st.selectbox(
                    "Regla fecha recurrencia",
                    [x for x in REC_RULE_OPTIONS if x != "Dia 1 y 15 de cada mes"],
                    index=0,
                    key="gas_rec_rule_quick",
                    on_change=lambda: _mark_form_force_open("gas"),
                )
        with r3:
            rec_dur_gas = st.selectbox(
                "Duracion",
                REC_DURATION_OPTIONS,
                index=0,
                key="gas_rec_duracion_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        if rec_dur_gas == "Hasta fecha":
            rec_hasta_gas = st.date_input(
                "Recurrencia hasta fecha",
                value=_today(),
                key="gas_rec_hasta_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        elif rec_dur_gas == "Por cantidad de periodos":
            rec_cant_gas = st.number_input(
                "Cantidad de periodos",
                min_value=1,
                step=1,
                key="gas_rec_cantidad_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )

    st.markdown("#### Clasificacion del gasto")
    subclas_gas_default = _derive_gas_sub(categoria_g)
    subclas_gas = subclas_gas_default
    st.text_input(
        "Clasificacion gerencial (automatica)",
        value=subclas_gas,
        disabled=True,
        help=_help_for_option(GAS_SUB_HELP, subclas_gas_default),
    )
    detalle_gas = st.selectbox(
        "Detalle",
        GAS_DETAIL_OPTIONS,
        index=0,
        key="gas_detalle_gas_quick",
        on_change=lambda: _mark_form_force_open("gas"),
    )

    st.markdown("#### Tratamiento en balance")
    tratamiento_gas_default = _derive_gas_balance(categoria_g)
    tratamiento_gas = st.selectbox(
        "Tratamiento balance gasto",
        GAS_BALANCE_OPTIONS,
        index=GAS_BALANCE_OPTIONS.index(tratamiento_gas_default if tratamiento_gas_default in GAS_BALANCE_OPTIONS else "Gasto del periodo"),
        key="gas_trat_balance_gas_quick",
        help=_help_for_option(BALANCE_GAS_HELP, st.session_state.get("gas_trat_balance_gas_quick", GAS_BALANCE_OPTIONS[0])),
        on_change=lambda: _mark_form_force_open("gas"),
    )

    prepago_meses = 0
    prepago_inicio = pd.NaT
    inventario_mov = ""
    inventario_item = ""

    st.markdown("#### Activo fijo")
    activo_dep_toggle = YES_NO_OPTIONS[0]
    activo_tipo = ""
    activo_vida = 5
    activo_inicio = pd.NaT
    activo_residual = 0.0
    activo_dep_mensual = 0.0
    if tratamiento_gas == "Activo fijo":
        st.caption("Usar solo para compras de activos relevantes o inversiones de largo plazo y el tiempo se basa en la vida util del activo a adquirir")
        af1, af2, af3 = st.columns(3)
        with af1:
            activo_dep_toggle = st.selectbox("?Depreciar / amortizar?", YES_NO_OPTIONS, index=0, key="gas_activo_dep_quick")
            activo_tipo = st.selectbox("Tipo", AF_TYPE_OPTIONS, index=0, key="gas_activo_tipo_quick")
        with af2:
            activo_vida = st.selectbox("Vida util (anios)", AF_LIFE_OPTIONS, index=2, key="gas_activo_vida_quick")
            activo_inicio = st.date_input("Fecha de inicio", value=fecha_g, key="gas_activo_inicio_quick")
        with af3:
            activo_residual = st.number_input("Valor residual", min_value=0.0, step=1.0, key="gas_activo_residual_quick")
        if _bool_from_toggle(activo_dep_toggle):
            activo_dep_mensual = max(0.0, float(monto_g) - float(activo_residual)) / max(1, int(activo_vida) * 12)
            st.caption(f"Depreciacion/amortizacion mensual estimada: {_format_money_es(activo_dep_mensual)}")
    elif tratamiento_gas == "Anticipo / prepago":
        st.markdown("#### Devengo de prepago")
        pg1, pg2 = st.columns(2)
        with pg1:
            prepago_meses = st.number_input(
                "Plazo prepago meses",
                min_value=1,
                step=1,
                value=12,
                key="gas_prepago_meses_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        with pg2:
            prepago_inicio = st.date_input(
                "Fecha inicio prepago",
                value=fecha_g,
                key="gas_prepago_inicio_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        st.caption("El gasto se devengara mensualmente en el panel gerencial durante el plazo indicado.")
    elif tratamiento_gas == "Inventario":
        st.markdown("#### Inventario operativo")
        iv1, iv2 = st.columns([1, 2])
        with iv1:
            inventario_mov = st.selectbox(
                "Movimiento inventario",
                INV_MOV_OPTIONS,
                index=0,
                key="gas_inv_mov_quick",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        with iv2:
            inventario_item = st.text_input(
                "Item inventario / referencia",
                key="gas_inv_item_quick",
                help="Referencia corta del inventario. Ejemplos: sensor SpO2, transductor, kit de reactivos.",
                on_change=lambda: _mark_form_force_open("gas"),
            )
        st.caption("Entrada aumenta inventario. Salida / consumo y ajuste negativo lo disminuyen; en el panel pueden afectar resultado y balance.")

    st.markdown("#### Financiamiento")
    fin_gas_toggle = st.selectbox(
        "?Tiene financiamiento asociado?",
        YES_NO_OPTIONS,
        index=0,
        key="gas_fin_toggle_quick",
        on_change=lambda: _mark_form_force_open("gas"),
    )
    fin_gas_on = _bool_from_toggle(fin_gas_toggle)
    fin_tipo_gas = ""
    fin_monto_gas = 0.0
    fin_fecha_inicio_gas = pd.NaT
    fin_plazo_gas = 0
    fin_tasa_gas = 0.0
    fin_tasa_tipo_gas = "Anual"
    fin_modalidad_gas = "Cuotas periodicas"
    fin_periodicidad_gas = "Mensual"
    if fin_gas_on:
        if tratamiento_gas == "Cuenta por cobrar / prestamo otorgado":
            fin_type_options = ["Financiamiento otorgado"]
        elif tratamiento_gas == "Activo fijo":
            fin_type_options = ["Activo fijo financiado"]
        else:
            fin_type_options = ["Financiamiento otorgado", "Activo fijo financiado"]
        f1, f2, f3 = st.columns(3)
        with f1:
            fin_tipo_gas = st.selectbox("Tipo", fin_type_options, index=0, key="gas_fin_tipo_quick")
            if fin_tipo_gas == "Financiamiento otorgado":
                fin_monto_gas = float(monto_g)
                st.caption(f"Se usa el monto base como principal/desembolso: {_format_money_es(fin_monto_gas)}")
            else:
                fin_monto_gas = st.number_input("Monto financiado", min_value=0.0, step=1.0, key="gas_fin_monto_quick")
            fin_fecha_inicio_gas = st.date_input("Fecha inicio", value=fecha_g, key="gas_fin_fecha_inicio_quick")
        with f2:
            fin_plazo_gas = st.number_input("Plazo en meses", min_value=1, step=1, value=1, key="gas_fin_plazo_quick")
            fin_tasa_gas = st.number_input("Tasa", min_value=0.0, step=0.1, key="gas_fin_tasa_quick")
            fin_tasa_tipo_gas = st.selectbox("Tipo de tasa", FIN_RATE_TYPE_OPTIONS, index=1, key="gas_fin_tasa_tipo_quick")
        with f3:
            fin_modalidad_gas = st.selectbox("Modalidad", FIN_MODALITY_OPTIONS, index=0, key="gas_fin_modalidad_quick")
            fin_periodicidad_gas = st.selectbox("Periodicidad", REC_PERIOD_OPTIONS, index=1, key="gas_fin_periodicidad_quick")
            st.caption("Capital no va al resultado; solo intereses si van al resultado.")

    submitted_gas = st.button("Guardar gasto", type="primary", key="btn_guardar_gas_quick")

    if submitted_gas:
        cliente_id_g = st.session_state.get("gas_cliente_id", "")
        cliente_nombre_g = st.session_state.get("gas_cliente_nombre", "")
        proyecto_id_g = st.session_state.get("gas_proyecto_id", "")
        if categoria_g != "Proyectos":
            cliente_id_g = ""
            cliente_nombre_g = ""
            proyecto_id_g = ""

        estado_g_final = "Pendiente" if _bool_from_toggle(recurrente_gas) else estado_g
        por_pagar_final = _estado_to_yes_no(estado_g_final)
        fecha_pago_exp = _ts(fecha_pago_esperada) if estado_g_final in {"Pendiente", "Parcial"} else pd.NaT
        fecha_pago_real_final = _ts(fecha_pago_real) if estado_g_final in {"Parcial", "Realizado"} else pd.NaT
        activo_fijo_on = tratamiento_gas == "Activo fijo"
        dep_on = activo_fijo_on and _bool_from_toggle(activo_dep_toggle)
        monto_pagado_real_final = float(monto_g) if estado_g_final == "Realizado" else float(monto_pagado_real or 0.0)
        if monto_pagado_real_final > float(monto_g):
            monto_pagado_real_final = float(monto_g)
        partial_events_gas = []
        if monto_pagado_real_final > 0 and not pd.isna(fecha_pago_real_final):
            partial_events_gas = [{
                "fecha": _ts(fecha_pago_real_final),
                "monto": float(monto_pagado_real_final),
                "nota": "Registro inicial",
            }]
        if _counterparty_required_for_gas(categoria_g, tratamiento_gas, fin_gas_on) and not str(contraparte_g or "").strip():
            st.error("Debes indicar la contraparte para este tipo de gasto.")
            st.stop()
        if estado_g_final == "Parcial" and (monto_pagado_real_final <= 0 or monto_pagado_real_final >= float(monto_g)):
            st.error("Para un gasto parcial, el monto real pagado debe ser mayor que 0 y menor que el monto total.")
            st.stop()
        if tratamiento_gas == "Anticipo / prepago" and int(prepago_meses or 0) <= 0:
            st.error("Debes indicar el plazo del prepago en meses.")
            st.stop()
        if tratamiento_gas == "Inventario" and not str(inventario_item or "").strip():
            st.error("Debes indicar el item inventario / referencia.")
            st.stop()
        cronograma_fin = _build_financing_schedule(
            principal=fin_monto_gas,
            fecha_inicio=fin_fecha_inicio_gas,
            plazo_meses=fin_plazo_gas,
            tasa=fin_tasa_gas,
            tasa_tipo=fin_tasa_tipo_gas,
            modalidad=fin_modalidad_gas,
            periodicidad=fin_periodicidad_gas,
        ) if fin_gas_on else ""

        nueva_g = {
            COL_ROWID: uuid.uuid4().hex,
            COL_FECHA: _ts(fecha_g),
            COL_MONTO: float(monto_g),
            COL_DESC: (desc_g or "").strip(),
            COL_CONC: (desc_g or "").strip(),
            COL_CAT: categoria_g,
            COL_EMP: (empresa_g or EMPRESA_DEFAULT).strip(),
            COL_POR_PAG: por_pagar_final,
            COL_CTP_TIPO: (tipo_contraparte_g or "").strip(),
            COL_CTP_NOMBRE: (contraparte_g or "").strip(),
            COL_PAGO_REAL_MONTO: float(monto_pagado_real_final),
            COL_GAS_PARTIALS: _serialize_partial_events(partial_events_gas),
            COL_REC: recurrente_gas,
            COL_REC_PER: rec_period_gas if _bool_from_toggle(recurrente_gas) else "",
            COL_REC_REG: rec_rule_gas if _bool_from_toggle(recurrente_gas) else "",
            COL_REC_DUR: rec_dur_gas if _bool_from_toggle(recurrente_gas) else "",
            COL_REC_HASTA: _ts(rec_hasta_gas) if _bool_from_toggle(recurrente_gas) and rec_dur_gas == "Hasta fecha" else pd.NaT,
            COL_REC_CANT: int(rec_cant_gas) if _bool_from_toggle(recurrente_gas) and rec_dur_gas == "Por cantidad de periodos" else 0,
            COL_FPAGO: fecha_pago_exp,
            COL_FPAGO_REAL: fecha_pago_real_final,
            COL_PROY: (proyecto_id_g or "").strip(),
            COL_CLI_ID: (cliente_id_g or "").strip(),
            COL_CLI_NOM: (cliente_nombre_g or "").strip(),
            COL_PROV: (prov_g or "").strip(),
            COL_GAS_SUB: subclas_gas,
            COL_GAS_DET: detalle_gas,
            COL_TRAT_BAL_GAS: tratamiento_gas,
            COL_PREPAGO_MESES: int(prepago_meses) if tratamiento_gas == "Anticipo / prepago" else 0,
            COL_PREPAGO_FEC_INI: _ts(prepago_inicio) if tratamiento_gas == "Anticipo / prepago" else pd.NaT,
            COL_INV_MOV: inventario_mov if tratamiento_gas == "Inventario" else "",
            COL_INV_ITEM: (inventario_item or "").strip() if tratamiento_gas == "Inventario" else "",
            COL_AF_TOGGLE: YES_NO_OPTIONS[1] if activo_fijo_on else YES_NO_OPTIONS[0],
            COL_AF_TIPO: activo_tipo if activo_fijo_on else "",
            COL_AF_VIDA: int(activo_vida) if activo_fijo_on else 0,
            COL_AF_FEC_INI: _ts(activo_inicio) if activo_fijo_on else pd.NaT,
            COL_AF_VAL_RES: float(activo_residual) if activo_fijo_on else 0.0,
            COL_AF_DEP_TOGGLE: YES_NO_OPTIONS[1] if dep_on else YES_NO_OPTIONS[0],
            COL_AF_DEP_MENSUAL: float(activo_dep_mensual) if dep_on else 0.0,
            COL_FIN_TOGGLE: YES_NO_OPTIONS[1] if fin_gas_on else YES_NO_OPTIONS[0],
            COL_FIN_TIPO: fin_tipo_gas if fin_gas_on else "",
            COL_FIN_MONTO: float(fin_monto_gas) if fin_gas_on else 0.0,
            COL_FIN_FEC_INI: _ts(fin_fecha_inicio_gas) if fin_gas_on else pd.NaT,
            COL_FIN_PLAZO: int(fin_plazo_gas) if fin_gas_on else 0,
            COL_FIN_TASA: float(fin_tasa_gas) if fin_gas_on else 0.0,
            COL_FIN_TASA_TIPO: fin_tasa_tipo_gas if fin_gas_on else "",
            COL_FIN_MODALIDAD: fin_modalidad_gas if fin_gas_on else "",
            COL_FIN_PERIOD: fin_periodicidad_gas if fin_gas_on else "",
            COL_FIN_CRONO: cronograma_fin,
            COL_USER: _current_user(),
        }
        st.session_state.df_gas = pd.concat([st.session_state.df_gas, pd.DataFrame([nueva_g])], ignore_index=True)
        st.session_state.df_gas = ensure_gastos_columns(st.session_state.df_gas)
        wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, st.session_state.df_gas, old_df=df_gas_before)
        if wrote:
            st.cache_data.clear()
        _clear_form_force_open("gas")
        _reset_entry_state("gas")
        _safe_rerun()
# Tabla Gastos (etiqueta "Descripcion" para Concepto)
st.markdown("### Gastos (tabla)")
gas_cols_view = [c for c in df_gas_f.columns if c not in (COL_ROWID, COL_ESC)] + [COL_ROWID]
gas_colcfg = {
    COL_POR_PAG: st.column_config.SelectboxColumn(COL_POR_PAG, options=YES_NO_OPTIONS),
    COL_REC:     st.column_config.SelectboxColumn(COL_REC, options=YES_NO_OPTIONS),
    COL_REC_PER: st.column_config.SelectboxColumn(COL_REC_PER, options=REC_PERIOD_OPTIONS),
    COL_REC_REG: st.column_config.SelectboxColumn(COL_REC_REG, options=REC_RULE_OPTIONS),
    COL_REC_DUR: st.column_config.SelectboxColumn(COL_REC_DUR, options=REC_DURATION_OPTIONS),
    COL_FPAGO:   st.column_config.DateColumn("Fecha esperada de pago"),
    COL_FPAGO_REAL: st.column_config.DateColumn("Fecha real de pago"),
    COL_PAGO_REAL_MONTO: st.column_config.NumberColumn("Monto real pagado", format="$%0.2f"),
    COL_MONTO:   st.column_config.TextColumn(COL_MONTO, help="Formato: 1.500,00"),
    COL_CAT:     st.column_config.SelectboxColumn(
        "Categoria operativa",
        options=GAS_CATEGORY_OPTIONS,
    ),
    COL_GAS_SUB: st.column_config.TextColumn("Clasificacion gerencial", disabled=True),
    COL_GAS_DET: st.column_config.SelectboxColumn(COL_GAS_DET, options=GAS_DETAIL_OPTIONS),
    COL_TRAT_BAL_GAS: st.column_config.SelectboxColumn(COL_TRAT_BAL_GAS, options=GAS_BALANCE_OPTIONS),
    COL_INV_MOV: st.column_config.SelectboxColumn("Movimiento inventario", options=[""] + INV_MOV_OPTIONS),
    COL_INV_ITEM: st.column_config.TextColumn("Item inventario / referencia"),
    COL_PREPAGO_MESES: st.column_config.NumberColumn("Plazo prepago meses", format="%d"),
    COL_PREPAGO_FEC_INI: st.column_config.DateColumn("Fecha inicio prepago"),
    COL_CTP_TIPO: st.column_config.SelectboxColumn("Tipo entidad rel. / contraparte", options=CONTRAPARTE_TYPE_OPTIONS),
    COL_CTP_NOMBRE: st.column_config.TextColumn("Entidad rel. / contraparte"),
    COL_AF_TOGGLE: st.column_config.SelectboxColumn(COL_AF_TOGGLE, options=YES_NO_OPTIONS),
    COL_AF_DEP_TOGGLE: st.column_config.SelectboxColumn(COL_AF_DEP_TOGGLE, options=YES_NO_OPTIONS),
    COL_FIN_TOGGLE: st.column_config.SelectboxColumn(COL_FIN_TOGGLE, options=YES_NO_OPTIONS),
    COL_FIN_TIPO: st.column_config.SelectboxColumn(COL_FIN_TIPO, options=["", "Financiamiento otorgado", "Activo fijo financiado"]),
    COL_CONC:    st.column_config.TextColumn("Descripcion"),
    COL_PROV:    st.column_config.TextColumn("Proveedor"),
    COL_EMP:     st.column_config.TextColumn(COL_EMP),
    COL_REF_RID: st.column_config.TextColumn(COL_REF_RID, disabled=True),
    COL_ROWID:   st.column_config.TextColumn(COL_ROWID, disabled=True),
    COL_USER:    st.column_config.TextColumn(COL_USER, disabled=True),
}
gas_order = [x for x in [
    COL_FECHA, COL_CONC, COL_PROV, COL_MONTO, COL_CAT, COL_GAS_SUB, COL_GAS_DET, COL_TRAT_BAL_GAS, COL_EMP,
    COL_POR_PAG, COL_FPAGO, COL_FPAGO_REAL, COL_PAGO_REAL_MONTO, COL_CTP_TIPO, COL_CTP_NOMBRE,
    COL_REC, COL_REC_PER, COL_REC_REG, COL_REC_DUR, COL_INV_MOV, COL_INV_ITEM, COL_PREPAGO_MESES, COL_PREPAGO_FEC_INI,
    COL_AF_TOGGLE, COL_AF_DEP_TOGGLE, COL_FIN_TOGGLE, COL_FIN_TIPO,
    COL_PROY, COL_CLI_ID, COL_CLI_NOM, COL_USER, COL_REF_RID, COL_ROWID
] if x in gas_cols_view]

edited_gas = st.data_editor(
    (
        lambda _df: (
            _df.assign(**{COL_MONTO: _df[COL_MONTO].map(_format_number_es)})
            if COL_MONTO in _df.columns
            else _df
        )
    )(df_gas_f[gas_cols_view].copy()),
    num_rows="dynamic", hide_index=True, width="stretch",
    column_config=gas_colcfg, key="tabla_gastos",
    column_order=gas_order
)
edited_gas = _editor_state_to_dataframe(
    (
        lambda _df: (
            _df.assign(**{COL_MONTO: _df[COL_MONTO].map(_format_number_es)})
            if COL_MONTO in _df.columns
            else _df
        )
    )(df_gas_f[gas_cols_view].copy()),
    "tabla_gastos",
    numeric_cols={COL_MONTO, COL_PAGO_REAL_MONTO, COL_PREPAGO_MESES},
)
edited_gas = _autoderive_gas_df(edited_gas)
gas_table_errors = _validate_gas_df(edited_gas)

if COL_POR_PAG in st.session_state.df_gas.columns and COL_FPAGO in st.session_state.df_gas.columns:
    miss_pago_mask = (
        st.session_state.df_gas[COL_POR_PAG].map(_si_no_norm).ne("No")
        & st.session_state.df_gas[COL_FPAGO].isna()
    )
    miss_pago_count = int(miss_pago_mask.sum())
    if miss_pago_count > 0:
        st.warning(f"Hay {miss_pago_count} gasto(s) por pagar sin Fecha esperada de pago.")
        if st.button("Completar Fecha esperada de pago faltante con Fecha del registro", key="btn_fill_gas_fechapago"):
            st.session_state.df_gas.loc[miss_pago_mask, COL_FPAGO] = st.session_state.df_gas.loc[miss_pago_mask, COL_FECHA]
            st.session_state.df_gas = ensure_gastos_columns(st.session_state.df_gas)
            wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, st.session_state.df_gas, old_df=df_gas_before)
            if wrote:
                st.cache_data.clear()
            st.success("Fechas esperadas de pago completadas para gastos pendientes.")
            _safe_rerun()

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
if gas_table_errors:
    _render_table_error_block("Gastos", gas_table_errors[:50])
else:
    sync_cambios(
        edited_df=edited_gas, filtered_df=df_gas_f,
        base_df_key="df_gas", worksheet_name=WS_GAS,
        session_state=st.session_state, write_worksheet=write_worksheet,
        client=client, sheet_id=SHEET_ID, id_column=COL_ROWID,
        ensure_columns_fn=_autoderive_gas_df,
    )

with st.expander("Registrar pago parcial", expanded=False):
    gas_base_partial = ensure_gastos_columns(st.session_state.df_gas.copy())
    gas_pending_view = gas_base_partial[
        (
            pd.to_numeric(gas_base_partial.get(COL_MONTO), errors="coerce").fillna(0.0)
            - pd.to_numeric(gas_base_partial.get(COL_PAGO_REAL_MONTO), errors="coerce").fillna(0.0)
        ).clip(lower=0.0) > 0
    ].copy()
    if gas_pending_view.empty:
        st.info("No hay gastos con saldo pendiente para registrar pagos parciales.")
    else:
        gas_pending_view["__saldo_pendiente"] = (
            pd.to_numeric(gas_pending_view[COL_MONTO], errors="coerce").fillna(0.0)
            - pd.to_numeric(gas_pending_view[COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0)
        ).clip(lower=0.0)
        gas_options = {
            f"{str(row.get(COL_CONC, '')).strip() or str(row.get(COL_ROWID, ''))} | pendiente {_format_money_es(row['__saldo_pendiente'])}": str(row.get(COL_ROWID, ""))
            for _, row in gas_pending_view.iterrows()
        }
        gas_sel_label = st.selectbox("Gasto con saldo pendiente", list(gas_options.keys()), key="gas_partial_row_select")
        gas_sel_id = gas_options.get(gas_sel_label, "")
        gas_row = gas_pending_view[gas_pending_view[COL_ROWID].astype(str) == gas_sel_id].iloc[0]
        current_events = _seed_partial_events_from_row(gas_row, COL_PAGO_REAL_MONTO, COL_FPAGO_REAL)
        saldo_pendiente = float(gas_row["__saldo_pendiente"])
        p1, p2, p3 = st.columns([1, 1, 2])
        with p1:
            monto_parcial = st.number_input("Monto a registrar", min_value=0.01, max_value=max(0.01, saldo_pendiente), step=1.0, key="gas_partial_amount")
        with p2:
            fecha_parcial = st.date_input("Fecha real del pago", value=_today(), key="gas_partial_date")
        with p3:
            nota_parcial = st.text_input("Nota del pago", key="gas_partial_note")
        st.caption(f"Total gasto: {_format_money_es(gas_row.get(COL_MONTO, 0.0))} | Pagado acumulado: {_format_money_es(gas_row.get(COL_PAGO_REAL_MONTO, 0.0))} | Pendiente: {_format_money_es(saldo_pendiente)}")
        if current_events:
            st.dataframe(
                pd.DataFrame(
                    [{"Fecha": _ts(evt["fecha"]), "Monto": float(evt["monto"]), "Nota": evt.get("nota", "")} for evt in current_events]
                ),
                use_container_width=True,
                hide_index=True,
                column_config={"Fecha": st.column_config.DateColumn("Fecha"), "Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
            )
        if st.button("Guardar pago parcial", key="btn_guardar_gas_partial"):
            updated = st.session_state.df_gas.copy()
            mask = updated[COL_ROWID].astype(str) == gas_sel_id
            if mask.any():
                row = updated.loc[mask].iloc[0]
                events = _seed_partial_events_from_row(row, COL_PAGO_REAL_MONTO, COL_FPAGO_REAL)
                events.append({"fecha": _ts(fecha_parcial), "monto": float(monto_parcial), "nota": str(nota_parcial or "").strip()})
                total_real, last_real = _partial_events_summary(events)
                total_monto = float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
                total_real = min(total_real, total_monto)
                updated.loc[mask, COL_GAS_PARTIALS] = _serialize_partial_events(events)
                updated.loc[mask, COL_PAGO_REAL_MONTO] = total_real
                updated.loc[mask, COL_FPAGO_REAL] = last_real
                updated.loc[mask, COL_POR_PAG] = "No" if total_real >= total_monto - 0.01 else "Sí"
                updated = ensure_gastos_columns(updated)
                wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, updated, old_df=df_gas_before)
                if wrote:
                    st.session_state.df_gas = updated
                    st.cache_data.clear()
                st.success("Pago parcial registrado.")
                _safe_rerun()




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
