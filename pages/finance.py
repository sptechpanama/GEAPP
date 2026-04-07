# ================================================
# finance.py
# Finanzas operativas (Ingresos / Gastos)
# - Borrado real en Sheets
# - Gastos con Cliente/Proyecto (cuando Categoría=Proyectos)
# - Ingresos: ocultar "Concepto" en la tabla (queda solo "Descripcion")
# - Catálogo: Un único expander para crear Clientes y Proyectos (ID auto)
# ================================================

from __future__ import annotations
import base64, hashlib, io, json, mimetypes, os, re, uuid, time
import streamlit as st
import streamlit.components.v1 as components
from ui.theme import apply_global_theme
st.set_page_config(page_title="Finanzas Operativas", page_icon="📊", layout="wide")
apply_global_theme()
import pandas as pd
import requests
import calendar
from datetime import date
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


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
COL_INV_FEC_LLEGADA = "Fecha llegada inventario"
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
COL_FIN_INSTRUMENTO = "Instrumento financiero"
COL_FIN_REG_TIPO = "Registro financiamiento"
COL_FACT_DET = "Detalle factoring"

WS_LINEAS_CREDITO = "LineasCredito"
LC_COL_ROWID = "RowID"
LC_COL_EMPRESA = "Empresa"
LC_COL_NOMBRE = "Nombre linea"
LC_COL_BANCO = "Banco"
LC_COL_LIMITE = "Limite vigente"
LC_COL_TASA_DIARIA = "Tasa diaria pct"
LC_COL_TASA_DESDE = "Fecha vigencia tasa"
LC_COL_CARGO_ANUAL_PCT = "Cargo anual pct"
LC_COL_CARGO_DESEMBOLSO = "Cargo desembolso fijo"
LC_COL_CARGO_BANCA_MENSUAL = "Cargo banca en linea mensual"
LC_COL_SEGURO_INCENDIO_1 = "Seguro incendio 1 anual"
LC_COL_SEGURO_INCENDIO_2 = "Seguro incendio 2 anual"
LC_COL_POLIZA_VIDA_MENSUAL = "Poliza vida mensual"
LC_COL_ACTIVA = "Activa"
LC_COL_NOTAS = "Notas"
LC_COL_UPDATED_AT = "Actualizado"
LC_BASE_COLUMNS = [
    LC_COL_ROWID,
    LC_COL_EMPRESA,
    LC_COL_NOMBRE,
    LC_COL_BANCO,
    LC_COL_LIMITE,
    LC_COL_TASA_DIARIA,
    LC_COL_TASA_DESDE,
    LC_COL_CARGO_ANUAL_PCT,
    LC_COL_CARGO_DESEMBOLSO,
    LC_COL_CARGO_BANCA_MENSUAL,
    LC_COL_SEGURO_INCENDIO_1,
    LC_COL_SEGURO_INCENDIO_2,
    LC_COL_POLIZA_VIDA_MENSUAL,
    LC_COL_ACTIVA,
    LC_COL_NOTAS,
    COL_USER,
    LC_COL_UPDATED_AT,
]

WS_TARJETAS_CREDITO = "TarjetasCredito"
TC_COL_ROWID = "RowID"
TC_COL_EMPRESA = "Empresa"
TC_COL_NOMBRE = "Nombre tarjeta"
TC_COL_BANCO = "Banco"
TC_COL_LIMITE = "Limite vigente"
TC_COL_DIA_CORTE = "Dia corte"
TC_COL_DIA_VENC = "Dia vencimiento"
TC_COL_ACTIVA = "Activa"
TC_COL_NOTAS = "Notas"
TC_COL_UPDATED_AT = "Actualizado"
TC_BASE_COLUMNS = [
    TC_COL_ROWID,
    TC_COL_EMPRESA,
    TC_COL_NOMBRE,
    TC_COL_BANCO,
    TC_COL_LIMITE,
    TC_COL_DIA_CORTE,
    TC_COL_DIA_VENC,
    TC_COL_ACTIVA,
    TC_COL_NOTAS,
    COL_USER,
    TC_COL_UPDATED_AT,
]

WS_DOCS_FINANCIEROS = "FinanzasDocs"
DOC_COL_ROWID = "RowID"
DOC_COL_FECHA_CARGA = "Fecha carga"
DOC_COL_USUARIO = "Usuario"
DOC_COL_EMPRESA = "Empresa"
DOC_COL_ARCHIVO = "Archivo"
DOC_COL_HASH = "Hash archivo"
DOC_COL_MIME = "Mime type"
DOC_COL_DRIVE_ID = "Drive file id"
DOC_COL_DRIVE_URL = "Drive url"
DOC_COL_ORIGEN = "Origen"
DOC_COL_NOTA = "Nota usuario"
DOC_COL_TEXTO_USUARIO = "Texto usuario"
DOC_COL_TIPO = "Tipo documento"
DOC_COL_ESTADO = "Estado borrador"
DOC_COL_MENSAJE = "Mensaje"
DOC_COL_API_USADA = "API usada"
DOC_COL_MODELO = "Modelo IA"
DOC_COL_JSON = "Extraccion JSON"
DOC_COL_DESTINO = "Destino sugerido"
DOC_COL_ACCION = "Accion sugerida"
DOC_COL_CATEGORIA = "Categoria sugerida"
DOC_COL_TRATAMIENTO = "Tratamiento sugerido"
DOC_COL_ESTADO_MOV = "Estado movimiento"
DOC_COL_FECHA_HECHO = "Fecha hecho sugerida"
DOC_COL_FECHA_ESPERADA = "Fecha esperada sugerida"
DOC_COL_FECHA_REAL = "Fecha real sugerida"
DOC_COL_MONTO = "Monto sugerido"
DOC_COL_CONTRAPARTE = "Contraparte sugerida"
DOC_COL_DETALLE = "Detalle sugerido"
DOC_COL_DESCRIPCION = "Descripcion sugerida"
DOC_COL_CONFIANZA = "Confianza"
DOC_COL_DUPLICADO = "Posible duplicado"
DOC_COL_WS_REG = "Worksheet registrado"
DOC_COL_ROWID_REG = "RowID registrado"
DOC_COL_APROBADO_POR = "Aprobado por"
DOC_COL_FECHA_APROBADO = "Fecha aprobado"
DOC_BASE_COLUMNS = [
    DOC_COL_ROWID,
    DOC_COL_FECHA_CARGA,
    DOC_COL_USUARIO,
    DOC_COL_EMPRESA,
    DOC_COL_ARCHIVO,
    DOC_COL_HASH,
    DOC_COL_MIME,
    DOC_COL_DRIVE_ID,
    DOC_COL_DRIVE_URL,
    DOC_COL_ORIGEN,
    DOC_COL_NOTA,
    DOC_COL_TEXTO_USUARIO,
    DOC_COL_TIPO,
    DOC_COL_ESTADO,
    DOC_COL_MENSAJE,
    DOC_COL_API_USADA,
    DOC_COL_MODELO,
    DOC_COL_JSON,
    DOC_COL_DESTINO,
    DOC_COL_ACCION,
    DOC_COL_CATEGORIA,
    DOC_COL_TRATAMIENTO,
    DOC_COL_ESTADO_MOV,
    DOC_COL_FECHA_HECHO,
    DOC_COL_FECHA_ESPERADA,
    DOC_COL_FECHA_REAL,
    DOC_COL_MONTO,
    DOC_COL_CONTRAPARTE,
    DOC_COL_DETALLE,
    DOC_COL_DESCRIPCION,
    DOC_COL_CONFIANZA,
    DOC_COL_DUPLICADO,
    DOC_COL_WS_REG,
    DOC_COL_ROWID_REG,
    DOC_COL_APROBADO_POR,
    DOC_COL_FECHA_APROBADO,
]


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
INVENTORY_POSITIVE_MOVEMENTS = {"Entrada", "Ajuste positivo"}
LINE_CHARGE_OPTIONS = [
    "Instalacion anual de linea",
    "Banca en linea mensual",
    "Seguro incendio 1 anual",
    "Seguro incendio 2 anual",
    "Poliza de vida mensual",
    "Cargo manual",
]


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


def _as_date_or_default(value, default_date: date) -> date:
    ts = _ts(value)
    return ts.date() if not pd.isna(ts) else default_date


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
        COL_FIN_MODALIDAD, COL_FIN_PERIOD, COL_FIN_CRONO, COL_FIN_INSTRUMENTO, COL_FIN_REG_TIPO, COL_ROWID, COL_USER,
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
            COL_FIN_PERIOD, COL_FIN_CRONO, COL_FIN_INSTRUMENTO, COL_FIN_REG_TIPO,
            COL_ING_PARTIALS,
        ],
    )
    out.loc[out[COL_REC_PER] == "Quincenal", COL_REC_PER] = "15nal"
    out.loc[out[COL_REC_PER].isin(["Bimestral", "Trimestral"]), COL_REC_PER] = "Mensual"
    out.loc[out[COL_REC_REG] == "Fin de mes", COL_REC_REG] = "Inicio de cada mes"
    rec_mask = out[COL_REC].map(_bool_from_toggle)
    out.loc[~rec_mask, COL_REC_PER] = ""
    out.loc[~rec_mask, COL_REC_REG] = ""
    out.loc[~rec_mask, COL_REC_DUR] = ""
    out.loc[~rec_mask, COL_REC_HASTA] = pd.NaT
    out.loc[~rec_mask, COL_REC_CANT] = 0
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
    fin_mask = (
        out[COL_FIN_TOGGLE].map(_bool_from_toggle)
        | out[COL_CAT].astype(str).eq("Financiamiento recibido")
        | out[COL_FIN_INSTRUMENTO].astype(str).str.strip().ne("")
        | out[COL_FIN_REG_TIPO].astype(str).str.strip().ne("")
    )
    out.loc[~fin_mask, COL_FIN_TIPO] = ""
    out.loc[~fin_mask, COL_FIN_MONTO] = 0.0
    out.loc[~fin_mask, COL_FIN_FEC_INI] = pd.NaT
    out.loc[~fin_mask, COL_FIN_PLAZO] = 0
    out.loc[~fin_mask, COL_FIN_TASA] = 0.0
    out.loc[~fin_mask, COL_FIN_TASA_TIPO] = ""
    out.loc[~fin_mask, COL_FIN_MODALIDAD] = ""
    out.loc[~fin_mask, COL_FIN_PERIOD] = ""
    out.loc[~fin_mask, COL_FIN_CRONO] = ""
    out.loc[~fin_mask, COL_FIN_INSTRUMENTO] = ""
    out.loc[~fin_mask, COL_FIN_REG_TIPO] = ""
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
        COL_INV_MOV, COL_INV_ITEM, COL_INV_FEC_LLEGADA,
        COL_AF_FEC_INI, COL_AF_VAL_RES, COL_AF_DEP_TOGGLE, COL_AF_DEP_MENSUAL,
        COL_FIN_TOGGLE, COL_FIN_TIPO, COL_FIN_MONTO, COL_FIN_FEC_INI,
        COL_FIN_PLAZO, COL_FIN_TASA, COL_FIN_TASA_TIPO, COL_FIN_MODALIDAD,
        COL_FIN_PERIOD, COL_FIN_CRONO, COL_FIN_INSTRUMENTO, COL_FIN_REG_TIPO, COL_ROWID, COL_USER,
    ]:
        if col not in out.columns:
            if col == COL_MONTO:
                out[col] = 0.0
            elif col in {COL_FECHA, COL_FPAGO, COL_FPAGO_REAL, COL_REC_HASTA, COL_AF_FEC_INI, COL_FIN_FEC_INI, COL_PREPAGO_FEC_INI, COL_INV_FEC_LLEGADA}:
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
    out[COL_INV_FEC_LLEGADA] = _ts(out[COL_INV_FEC_LLEGADA])
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
            COL_FIN_PERIOD, COL_FIN_CRONO, COL_FIN_INSTRUMENTO, COL_FIN_REG_TIPO, COL_GAS_PARTIALS, COL_INV_MOV, COL_INV_ITEM,
        ],
    )
    out.loc[out[COL_REC_PER] == "Quincenal", COL_REC_PER] = "15nal"
    out.loc[out[COL_REC_PER].isin(["Bimestral", "Trimestral"]), COL_REC_PER] = "Mensual"
    out.loc[out[COL_REC_REG] == "Fin de mes", COL_REC_REG] = "Inicio de cada mes"
    rec_mask = out[COL_REC].map(_bool_from_toggle)
    out.loc[~rec_mask, COL_REC_PER] = ""
    out.loc[~rec_mask, COL_REC_REG] = ""
    out.loc[~rec_mask, COL_REC_DUR] = ""
    out.loc[~rec_mask, COL_REC_HASTA] = pd.NaT
    out.loc[~rec_mask, COL_REC_CANT] = 0
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
    out.loc[~prepago_mask, COL_PREPAGO_MESES] = 0
    out.loc[~prepago_mask, COL_PREPAGO_FEC_INI] = pd.NaT
    out.loc[prepago_mask & out[COL_PREPAGO_FEC_INI].isna(), COL_PREPAGO_FEC_INI] = out.loc[prepago_mask & out[COL_PREPAGO_FEC_INI].isna(), COL_FECHA]
    inventory_mask = out[COL_TRAT_BAL_GAS].astype(str).eq("Inventario")
    out.loc[~inventory_mask, COL_INV_MOV] = ""
    out.loc[~inventory_mask, COL_INV_ITEM] = ""
    out.loc[~inventory_mask, COL_INV_FEC_LLEGADA] = pd.NaT
    out.loc[inventory_mask & out[COL_INV_MOV].astype(str).str.strip().eq(""), COL_INV_MOV] = "Entrada"
    inv_positive_mask = inventory_mask & out[COL_INV_MOV].astype(str).isin(INVENTORY_POSITIVE_MOVEMENTS)
    out.loc[inv_positive_mask & out[COL_INV_FEC_LLEGADA].isna(), COL_INV_FEC_LLEGADA] = out.loc[inv_positive_mask & out[COL_INV_FEC_LLEGADA].isna(), COL_FECHA]
    out.loc[inventory_mask & ~out[COL_INV_MOV].astype(str).isin(INVENTORY_POSITIVE_MOVEMENTS), COL_INV_FEC_LLEGADA] = pd.NaT
    af_mask = out[COL_TRAT_BAL_GAS].astype(str).eq("Activo fijo")
    out.loc[~af_mask, COL_AF_TOGGLE] = "No"
    out.loc[~af_mask, COL_AF_TIPO] = ""
    out.loc[~af_mask, COL_AF_VIDA] = 0
    out.loc[~af_mask, COL_AF_FEC_INI] = pd.NaT
    out.loc[~af_mask, COL_AF_VAL_RES] = 0.0
    out.loc[~af_mask, COL_AF_DEP_TOGGLE] = "No"
    out.loc[~af_mask, COL_AF_DEP_MENSUAL] = 0.0
    fin_mask = (
        out[COL_FIN_TOGGLE].map(_bool_from_toggle)
        | out[COL_FIN_INSTRUMENTO].astype(str).str.strip().ne("")
        | out[COL_FIN_REG_TIPO].astype(str).str.strip().ne("")
    )
    out.loc[~fin_mask, COL_FIN_TIPO] = ""
    out.loc[~fin_mask, COL_FIN_MONTO] = 0.0
    out.loc[~fin_mask, COL_FIN_FEC_INI] = pd.NaT
    out.loc[~fin_mask, COL_FIN_PLAZO] = 0
    out.loc[~fin_mask, COL_FIN_TASA] = 0.0
    out.loc[~fin_mask, COL_FIN_TASA_TIPO] = ""
    out.loc[~fin_mask, COL_FIN_MODALIDAD] = ""
    out.loc[~fin_mask, COL_FIN_PERIOD] = ""
    out.loc[~fin_mask, COL_FIN_CRONO] = ""
    out.loc[~fin_mask, COL_FIN_INSTRUMENTO] = ""
    out.loc[~fin_mask, COL_FIN_REG_TIPO] = ""
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


def ensure_lineas_credito_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    for col in LC_BASE_COLUMNS:
        if col not in out.columns:
            if col in {LC_COL_LIMITE, LC_COL_TASA_DIARIA, LC_COL_CARGO_ANUAL_PCT, LC_COL_CARGO_DESEMBOLSO, LC_COL_CARGO_BANCA_MENSUAL, LC_COL_SEGURO_INCENDIO_1, LC_COL_SEGURO_INCENDIO_2, LC_COL_POLIZA_VIDA_MENSUAL}:
                out[col] = 0.0
            elif col in {LC_COL_TASA_DESDE, LC_COL_UPDATED_AT}:
                out[col] = pd.NaT
            elif col == LC_COL_EMPRESA:
                out[col] = EMPRESA_DEFAULT
            elif col == LC_COL_ACTIVA:
                out[col] = "Sí"
            else:
                out[col] = ""
    out[LC_COL_LIMITE] = pd.to_numeric(out[LC_COL_LIMITE], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_TASA_DIARIA] = pd.to_numeric(out[LC_COL_TASA_DIARIA], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_CARGO_ANUAL_PCT] = pd.to_numeric(out[LC_COL_CARGO_ANUAL_PCT], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_CARGO_DESEMBOLSO] = pd.to_numeric(out[LC_COL_CARGO_DESEMBOLSO], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_CARGO_BANCA_MENSUAL] = pd.to_numeric(out[LC_COL_CARGO_BANCA_MENSUAL], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_SEGURO_INCENDIO_1] = pd.to_numeric(out[LC_COL_SEGURO_INCENDIO_1], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_SEGURO_INCENDIO_2] = pd.to_numeric(out[LC_COL_SEGURO_INCENDIO_2], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_POLIZA_VIDA_MENSUAL] = pd.to_numeric(out[LC_COL_POLIZA_VIDA_MENSUAL], errors="coerce").fillna(0.0).astype(float)
    out[LC_COL_TASA_DESDE] = _ts(out[LC_COL_TASA_DESDE])
    out[LC_COL_UPDATED_AT] = _ts(out[LC_COL_UPDATED_AT])
    out[LC_COL_EMPRESA] = out[LC_COL_EMPRESA].astype("string").str.upper().str.strip().where(
        out[LC_COL_EMPRESA].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT,
    )
    out[LC_COL_ACTIVA] = out[LC_COL_ACTIVA].map(_si_no_norm)
    out = _ensure_text(out, [LC_COL_ROWID, LC_COL_EMPRESA, LC_COL_NOMBRE, LC_COL_BANCO, LC_COL_ACTIVA, LC_COL_NOTAS, COL_USER])
    out[LC_COL_ROWID] = out.apply(lambda row: str(row.get(LC_COL_ROWID, "")).strip() or uuid.uuid4().hex, axis=1)
    return out


def _ensure_worksheet_exists(client, sheet_id: str, worksheet_name: str, headers: list[str]) -> None:
    sh = client.open_by_key(sheet_id)
    try:
        sh.worksheet(worksheet_name)
        return
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows=200, cols=max(24, len(headers) + 4))
        ws.update("A1", [headers])


def load_credit_lines_df(client, sheet_id: str) -> pd.DataFrame:
    _ensure_worksheet_exists(client, sheet_id, WS_LINEAS_CREDITO, LC_BASE_COLUMNS)
    try:
        return ensure_lineas_credito_columns(read_worksheet(client, sheet_id, WS_LINEAS_CREDITO))
    except Exception:
        return ensure_lineas_credito_columns(pd.DataFrame(columns=LC_BASE_COLUMNS))


def safe_write_credit_lines(client, sheet_id: str, new_df: pd.DataFrame, old_df: pd.DataFrame | None = None) -> bool:
    _ensure_worksheet_exists(client, sheet_id, WS_LINEAS_CREDITO, LC_BASE_COLUMNS)
    return safe_write_worksheet(client, sheet_id, WS_LINEAS_CREDITO, ensure_lineas_credito_columns(new_df), old_df=old_df, id_col=LC_COL_ROWID)


def ensure_tarjetas_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    for col in TC_BASE_COLUMNS:
        if col not in out.columns:
            if col == TC_COL_LIMITE:
                out[col] = 0.0
            elif col in {TC_COL_DIA_CORTE, TC_COL_DIA_VENC}:
                out[col] = 1
            elif col == TC_COL_EMPRESA:
                out[col] = EMPRESA_DEFAULT
            elif col == TC_COL_ACTIVA:
                out[col] = "Sí"
            elif col == TC_COL_UPDATED_AT:
                out[col] = pd.NaT
            else:
                out[col] = ""
    out[TC_COL_LIMITE] = pd.to_numeric(out[TC_COL_LIMITE], errors="coerce").fillna(0.0).astype(float)
    out[TC_COL_DIA_CORTE] = pd.to_numeric(out[TC_COL_DIA_CORTE], errors="coerce").fillna(1).clip(lower=1, upper=31).astype(int)
    out[TC_COL_DIA_VENC] = pd.to_numeric(out[TC_COL_DIA_VENC], errors="coerce").fillna(1).clip(lower=1, upper=31).astype(int)
    out[TC_COL_UPDATED_AT] = _ts(out[TC_COL_UPDATED_AT])
    out[TC_COL_EMPRESA] = out[TC_COL_EMPRESA].astype("string").str.upper().str.strip().where(
        out[TC_COL_EMPRESA].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT,
    )
    out[TC_COL_ACTIVA] = out[TC_COL_ACTIVA].map(_si_no_norm)
    out = _ensure_text(out, [TC_COL_ROWID, TC_COL_EMPRESA, TC_COL_NOMBRE, TC_COL_BANCO, TC_COL_ACTIVA, TC_COL_NOTAS, COL_USER])
    out[TC_COL_ROWID] = out.apply(lambda row: str(row.get(TC_COL_ROWID, "")).strip() or uuid.uuid4().hex, axis=1)
    return out


def load_cards_df(client, sheet_id: str) -> pd.DataFrame:
    _ensure_worksheet_exists(client, sheet_id, WS_TARJETAS_CREDITO, TC_BASE_COLUMNS)
    try:
        return ensure_tarjetas_columns(read_worksheet(client, sheet_id, WS_TARJETAS_CREDITO))
    except Exception:
        return ensure_tarjetas_columns(pd.DataFrame(columns=TC_BASE_COLUMNS))


def safe_write_cards(client, sheet_id: str, new_df: pd.DataFrame, old_df: pd.DataFrame | None = None) -> bool:
    _ensure_worksheet_exists(client, sheet_id, WS_TARJETAS_CREDITO, TC_BASE_COLUMNS)
    return safe_write_worksheet(client, sheet_id, WS_TARJETAS_CREDITO, ensure_tarjetas_columns(new_df), old_df=old_df, id_col=TC_COL_ROWID)


def ensure_finance_docs_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    for col in DOC_BASE_COLUMNS:
        if col not in out.columns:
            if col in {DOC_COL_FECHA_CARGA, DOC_COL_FECHA_HECHO, DOC_COL_FECHA_ESPERADA, DOC_COL_FECHA_REAL, DOC_COL_FECHA_APROBADO}:
                out[col] = pd.NaT
            elif col in {DOC_COL_MONTO, DOC_COL_CONFIANZA}:
                out[col] = 0.0
            else:
                out[col] = ""
    for col in [DOC_COL_FECHA_CARGA, DOC_COL_FECHA_HECHO, DOC_COL_FECHA_ESPERADA, DOC_COL_FECHA_REAL, DOC_COL_FECHA_APROBADO]:
        out[col] = _ts(out[col])
    out[DOC_COL_MONTO] = pd.to_numeric(out[DOC_COL_MONTO], errors="coerce").fillna(0.0).astype(float)
    out[DOC_COL_CONFIANZA] = pd.to_numeric(out[DOC_COL_CONFIANZA], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
    out[DOC_COL_EMPRESA] = out[DOC_COL_EMPRESA].astype("string").str.upper().str.strip().where(
        out[DOC_COL_EMPRESA].astype("string").str.upper().str.strip().isin(EMPRESAS_OPCIONES),
        other=EMPRESA_DEFAULT,
    )
    text_cols = [c for c in DOC_BASE_COLUMNS if c not in {DOC_COL_MONTO, DOC_COL_CONFIANZA, DOC_COL_FECHA_CARGA, DOC_COL_FECHA_HECHO, DOC_COL_FECHA_ESPERADA, DOC_COL_FECHA_REAL, DOC_COL_FECHA_APROBADO}]
    out = _ensure_text(out, text_cols)
    out[DOC_COL_ROWID] = out.apply(lambda row: str(row.get(DOC_COL_ROWID, "")).strip() or uuid.uuid4().hex, axis=1)
    return out[DOC_BASE_COLUMNS + [c for c in out.columns if c not in DOC_BASE_COLUMNS]]


def load_finance_docs_df(client, sheet_id: str) -> pd.DataFrame:
    _ensure_worksheet_exists(client, sheet_id, WS_DOCS_FINANCIEROS, DOC_BASE_COLUMNS)
    try:
        return ensure_finance_docs_columns(read_worksheet(client, sheet_id, WS_DOCS_FINANCIEROS))
    except Exception:
        return ensure_finance_docs_columns(pd.DataFrame(columns=DOC_BASE_COLUMNS))


def safe_write_finance_docs(client, sheet_id: str, new_df: pd.DataFrame, old_df: pd.DataFrame | None = None) -> bool:
    _ensure_worksheet_exists(client, sheet_id, WS_DOCS_FINANCIEROS, DOC_BASE_COLUMNS)
    return safe_write_worksheet(client, sheet_id, WS_DOCS_FINANCIEROS, ensure_finance_docs_columns(new_df), old_df=old_df, id_col=DOC_COL_ROWID)


def _finance_docs_folder_id() -> str:
    candidates: list[str | None] = []
    try:
        app_cfg = st.secrets.get("app", {})
        candidates.extend(
            [
                app_cfg.get("DRIVE_FINANCE_DOCS_FOLDER_ID"),
                app_cfg.get("DRIVE_DOCUMENTOS_FINANCIEROS_FOLDER_ID"),
                app_cfg.get("DRIVE_BACKUP_FOLDER_ID"),
            ]
        )
    except Exception:
        pass
    candidates.append(os.environ.get("DRIVE_FINANCE_DOCS_FOLDER_ID"))
    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return ""


def _finance_docs_folder_id_for_empresa(empresa: str) -> str:
    suffix = str(empresa or "").upper().replace("-", "_").replace(" ", "_")
    candidates: list[str | None] = []
    try:
        app_cfg = st.secrets.get("app", {})
        candidates.extend(
            [
                app_cfg.get(f"DRIVE_FINANCE_DOCS_FOLDER_ID_{suffix}"),
                app_cfg.get(f"DRIVE_DOCUMENTOS_FINANCIEROS_FOLDER_ID_{suffix}"),
            ]
        )
    except Exception:
        pass
    candidates.extend(
        [
            os.environ.get(f"DRIVE_FINANCE_DOCS_FOLDER_ID_{suffix}"),
            os.environ.get(f"DRIVE_DOCUMENTOS_FINANCIEROS_FOLDER_ID_{suffix}"),
        ]
    )
    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return ""


def _openai_finance_api_key() -> str:
    candidates: list[str | None] = []
    try:
        app_cfg = st.secrets.get("app", {})
        candidates.append(app_cfg.get("OPENAI_API_KEY"))
    except Exception:
        pass
    try:
        candidates.append(st.secrets.get("OPENAI_API_KEY"))
    except Exception:
        pass
    candidates.append(os.environ.get("OPENAI_API_KEY"))
    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return ""


def _openai_finance_model() -> str:
    candidates: list[str | None] = []
    try:
        app_cfg = st.secrets.get("app", {})
        candidates.extend([app_cfg.get("OPENAI_FINANCE_DOC_MODEL"), app_cfg.get("OPENAI_MODEL"), app_cfg.get("OPENAI_CHAT_MODEL")])
    except Exception:
        pass
    try:
        candidates.append(st.secrets.get("OPENAI_FINANCE_DOC_MODEL"))
    except Exception:
        pass
    candidates.extend([os.environ.get("OPENAI_FINANCE_DOC_MODEL"), os.environ.get("OPENAI_MODEL")])
    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return "gpt-4o-mini"


def _hash_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw or b"").hexdigest()


def _clean_filename(name: str) -> str:
    raw = str(name or "documento").strip() or "documento"
    raw = re.sub(r"[^\w.\- áéíóúÁÉÍÓÚñÑ()]+", "_", raw, flags=re.UNICODE)
    return raw[:140] or "documento"


def _drive_client_from_creds(creds_obj):
    return build("drive", "v3", credentials=creds_obj)


def _upload_finance_doc_to_drive(creds_obj, *, filename: str, content: bytes, mime_type: str, folder_id: str) -> dict[str, str]:
    if not folder_id:
        raise RuntimeError("Configura DRIVE_FINANCE_DOCS_FOLDER_ID o DRIVE_BACKUP_FOLDER_ID en secrets para guardar documentos en Drive.")
    drive = _drive_client_from_creds(creds_obj)
    safe_name = _clean_filename(filename)
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type or "application/octet-stream", resumable=False)
    body = {"name": safe_name, "parents": [folder_id]}
    created = drive.files().create(
        body=body,
        media_body=media,
        fields="id,name,webViewLink,mimeType",
        supportsAllDrives=True,
    ).execute()
    return {
        "id": str(created.get("id", "")),
        "name": str(created.get("name", safe_name)),
        "url": str(created.get("webViewLink", "")),
        "mime": str(created.get("mimeType", mime_type or "")),
    }


def _download_drive_file_bytes(creds_obj, file_id: str) -> tuple[bytes, str, str]:
    drive = _drive_client_from_creds(creds_obj)
    meta = drive.files().get(fileId=file_id, fields="id,name,mimeType", supportsAllDrives=True).execute()
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue(), str(meta.get("name", "")), str(meta.get("mimeType", ""))


def _import_finance_docs_from_drive(creds_obj, folder_id: str, docs_df: pd.DataFrame, *, limit: int = 25) -> pd.DataFrame:
    if not folder_id:
        raise RuntimeError("Configura DRIVE_FINANCE_DOCS_FOLDER_ID o DRIVE_BACKUP_FOLDER_ID para importar desde Drive.")
    drive = _drive_client_from_creds(creds_obj)
    query = f"'{folder_id}' in parents and trashed=false"
    resp = drive.files().list(
        q=query,
        fields="files(id,name,mimeType,webViewLink,modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=int(limit),
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    existing_hashes = set(ensure_finance_docs_columns(docs_df).get(DOC_COL_HASH, pd.Series(dtype=str)).astype(str).str.strip())
    rows = []
    for item in resp.get("files", []) or []:
        fid = str(item.get("id", "") or "").strip()
        if not fid:
            continue
        file_hash = f"drive:{fid}"
        if file_hash in existing_hashes:
            continue
        rows.append(
            {
                DOC_COL_ROWID: uuid.uuid4().hex,
                DOC_COL_FECHA_CARGA: _ts(date.today()),
                DOC_COL_USUARIO: _current_user(),
                DOC_COL_EMPRESA: EMPRESA_DEFAULT,
                DOC_COL_ARCHIVO: str(item.get("name", "") or ""),
                DOC_COL_HASH: file_hash,
                DOC_COL_MIME: str(item.get("mimeType", "") or ""),
                DOC_COL_DRIVE_ID: fid,
                DOC_COL_DRIVE_URL: str(item.get("webViewLink", "") or ""),
                DOC_COL_ORIGEN: "Drive",
                DOC_COL_ESTADO: "Borrador",
                DOC_COL_MENSAJE: "Importado desde Drive. Pendiente de revisar/procesar.",
            }
        )
    if not rows:
        return ensure_finance_docs_columns(docs_df)
    return ensure_finance_docs_columns(pd.concat([docs_df, pd.DataFrame(rows)], ignore_index=True))


def _extract_pdf_text_local(content: bytes, *, max_chars: int = 6000) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        return ""
    try:
        reader = PdfReader(io.BytesIO(content or b""))
        chunks: list[str] = []
        for page in reader.pages[:5]:
            chunks.append(page.extract_text() or "")
            if sum(len(x) for x in chunks) >= max_chars:
                break
        return "\n".join(chunks).strip()[:max_chars]
    except Exception:
        return ""


def _finance_doc_local_text(content: bytes | None, mime_type: str, existing_text: str = "") -> str:
    base = str(existing_text or "").strip()
    if base:
        return base[:6000]
    mime = str(mime_type or "").lower()
    if content and ("pdf" in mime):
        return _extract_pdf_text_local(content)
    return ""


def _new_finance_doc_row(
    *,
    empresa: str,
    file_name: str,
    file_hash: str,
    mime_type: str,
    drive_id: str,
    drive_url: str,
    origen: str,
    note: str,
    local_text: str,
    proposal: dict,
    message: str,
    api_used: str = "No",
    model_name: str = "",
) -> dict:
    return {
        DOC_COL_ROWID: uuid.uuid4().hex,
        DOC_COL_FECHA_CARGA: _ts(_today()),
        DOC_COL_USUARIO: _current_user(),
        DOC_COL_EMPRESA: empresa,
        DOC_COL_ARCHIVO: str(file_name or "").strip(),
        DOC_COL_HASH: str(file_hash or "").strip(),
        DOC_COL_MIME: str(mime_type or "").strip(),
        DOC_COL_DRIVE_ID: str(drive_id or "").strip(),
        DOC_COL_DRIVE_URL: str(drive_url or "").strip(),
        DOC_COL_ORIGEN: str(origen or "").strip(),
        DOC_COL_NOTA: str(note or "").strip(),
        DOC_COL_TEXTO_USUARIO: str(local_text or "").strip()[:6000],
        DOC_COL_ESTADO: "Procesado" if api_used == "Si" else "Borrador",
        DOC_COL_MENSAJE: str(message or "").strip()[:500],
        DOC_COL_API_USADA: api_used,
        DOC_COL_MODELO: model_name if api_used == "Si" else "",
        **proposal,
    }


def _analyze_finance_doc_content(
    *,
    file_name: str,
    mime_type: str,
    content: bytes | None,
    empresa: str,
    note: str = "",
    tipo_hint: str = "Auto",
    use_api: bool = False,
    api_key: str = "",
    model_name: str = "",
) -> tuple[dict, str, str, str]:
    extracted_text = _finance_doc_local_text(content, mime_type, "")
    local_text = "\n".join([x for x in [str(note or "").strip(), extracted_text] if x]).strip()[:6000]
    proposal = _guess_finance_doc_proposal(
        file_name=file_name,
        note=note,
        text=local_text,
        tipo_hint=tipo_hint,
        empresa=empresa,
        monto_manual=0.0,
        fecha_doc=_today(),
        fecha_esperada=_today(),
    )
    api_used = "No"
    message = "Borrador creado con reglas locales, sin usar API."
    if use_api and api_key:
        try:
            payload = _call_openai_finance_doc(
                api_key=api_key,
                model=model_name,
                file_name=file_name,
                mime_type=mime_type,
                content=content,
                user_text=local_text,
                user_note=note,
            )
            ai_row = _apply_ai_doc_payload(pd.Series({**proposal, DOC_COL_TEXTO_USUARIO: local_text}), payload)
            for col in [
                DOC_COL_TIPO,
                DOC_COL_DESTINO,
                DOC_COL_ACCION,
                DOC_COL_CATEGORIA,
                DOC_COL_TRATAMIENTO,
                DOC_COL_ESTADO_MOV,
                DOC_COL_FECHA_HECHO,
                DOC_COL_FECHA_ESPERADA,
                DOC_COL_FECHA_REAL,
                DOC_COL_MONTO,
                DOC_COL_CONTRAPARTE,
                DOC_COL_DETALLE,
                DOC_COL_DESCRIPCION,
                DOC_COL_CONFIANZA,
                DOC_COL_JSON,
            ]:
                proposal[col] = ai_row.get(col, proposal.get(col, ""))
            api_used = "Si"
            message = str(ai_row.get(DOC_COL_MENSAJE, "Procesado con IA. Revisa antes de aprobar.") or "").strip()
        except Exception as exc:
            message = f"Reglas locales aplicadas. IA fallo: {str(exc)[:220]}"
    return proposal, local_text, api_used, message


def _scan_finance_docs_folder_for_empresa(
    *,
    creds_obj,
    folder_id: str,
    docs_df: pd.DataFrame,
    empresa: str,
    limit: int,
    use_api: bool,
    api_key: str,
    model_name: str,
) -> tuple[pd.DataFrame, int, int, list[str]]:
    if not folder_id:
        raise RuntimeError("Falta carpeta Drive para la empresa.")
    drive = _drive_client_from_creds(creds_obj)
    resp = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,webViewLink,modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=int(limit),
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    work_docs = ensure_finance_docs_columns(docs_df.copy())
    existing_hashes = set(work_docs.get(DOC_COL_HASH, pd.Series(dtype=str)).astype(str).str.strip())
    existing_drive_ids = set(work_docs.get(DOC_COL_DRIVE_ID, pd.Series(dtype=str)).astype(str).str.strip())
    created = 0
    skipped = 0
    errors: list[str] = []
    for item in resp.get("files", []) or []:
        fid = str(item.get("id", "") or "").strip()
        if not fid:
            continue
        if fid in existing_drive_ids:
            skipped += 1
            continue
        try:
            try:
                content, file_name, mime_type = _download_drive_file_bytes(creds_obj, fid)
            except Exception:
                content = None
                file_name = str(item.get("name", "") or "")
                mime_type = str(item.get("mimeType", "") or "")
            file_hash = _hash_bytes(content) if content else f"drive:{fid}"
            if file_hash in existing_hashes:
                skipped += 1
                continue
            proposal, local_text, api_used, message = _analyze_finance_doc_content(
                file_name=file_name or str(item.get("name", "") or ""),
                mime_type=mime_type or str(item.get("mimeType", "") or ""),
                content=content,
                empresa=empresa,
                note=f"Documento importado desde Drive para {empresa}",
                use_api=use_api,
                api_key=api_key,
                model_name=model_name,
            )
            new_row = _new_finance_doc_row(
                empresa=empresa,
                file_name=file_name or str(item.get("name", "") or ""),
                file_hash=file_hash,
                mime_type=mime_type or str(item.get("mimeType", "") or ""),
                drive_id=fid,
                drive_url=str(item.get("webViewLink", "") or ""),
                origen="Drive",
                note=f"Documento importado desde Drive para {empresa}",
                local_text=local_text,
                proposal=proposal,
                message=message,
                api_used=api_used,
                model_name=model_name,
            )
            new_row[DOC_COL_DUPLICADO] = _finance_doc_possible_duplicate(pd.Series(new_row), st.session_state.df_ing, st.session_state.df_gas, work_docs)
            work_docs = ensure_finance_docs_columns(pd.concat([work_docs, pd.DataFrame([new_row])], ignore_index=True))
            existing_hashes.add(file_hash)
            existing_drive_ids.add(fid)
            created += 1
        except Exception as exc:
            errors.append(f"{str(item.get('name', '') or fid)}: {str(exc)[:180]}")
    return work_docs, created, skipped, errors


def _guess_doc_type_from_text(text: str, hint: str = "") -> str:
    if hint and hint != "Auto":
        return hint
    t = str(text or "").lower()
    if any(k in t for k in ["estado de cuenta", "mastercard", "visa", "tarjeta"]):
        return "Estado de tarjeta"
    if any(k in t for k in ["factoring", "retenido", "factor"]):
        return "Factoring"
    if any(k in t for k in ["desembolso", "linea de credito", "línea de crédito", "tasa diaria"]):
        return "Linea de credito"
    if any(k in t for k in ["transferencia", "ach", "deposito", "depósito"]):
        return "Comprobante transferencia"
    if any(k in t for k in ["factura", "invoice"]):
        return "Factura proveedor"
    if any(k in t for k in ["cobro", "gestion de cobro", "gestión de cobro"]):
        return "Gestion de cobro"
    return "Otro"


def _guess_amount_from_text(text: str) -> float:
    candidates = []
    for match in re.findall(r"(?:usd|us\$|\$|b/\.?)\s*([0-9][0-9.,]*)", str(text or ""), flags=re.I):
        num = pd.to_numeric(pd.Series([match.replace(".", "").replace(",", ".")]), errors="coerce").fillna(0.0).iloc[0]
        if float(num) > 0:
            candidates.append(float(num))
    return max(candidates) if candidates else 0.0


def _guess_finance_doc_proposal(*, file_name: str, note: str, text: str, tipo_hint: str, empresa: str, monto_manual: float, fecha_doc, fecha_esperada) -> dict[str, object]:
    corpus = " ".join([str(file_name or ""), str(note or ""), str(text or "")]).strip()
    tipo = _guess_doc_type_from_text(corpus, tipo_hint)
    monto = float(monto_manual or 0.0) or _guess_amount_from_text(corpus)
    desc = str(note or "").strip() or f"Documento financiero - {str(file_name or '').strip()}"
    lower = corpus.lower()
    if tipo in {"Factura cliente", "Comprobante cobro"}:
        destino = "Ingreso"
        categoria = "Proyectos"
        tratamiento = "Cuenta por cobrar"
        detalle = "Cobro de proyecto"
        estado = "Pendiente"
    elif tipo in {"Linea de credito"}:
        destino = "Linea de credito"
        categoria = "Financiamiento recibido"
        tratamiento = "Pasivo financiero"
        detalle = "Prestamo recibido"
        estado = "Realizado"
    elif tipo in {"Estado de tarjeta"}:
        destino = "Tarjeta de credito"
        categoria = "Gastos operativos"
        tratamiento = "Gasto del periodo"
        detalle = "Otros"
        estado = "Pendiente"
    elif tipo in {"Factoring"}:
        destino = "Factoring"
        categoria = "Gasto financiero"
        tratamiento = "Gasto del periodo"
        detalle = "Otros"
        estado = "Realizado"
    elif tipo in {"Gestion de cobro"}:
        destino = "Gestion de cobro"
        categoria = ""
        tratamiento = ""
        detalle = ""
        estado = "Pendiente"
    else:
        destino = "Gasto"
        categoria = "Gastos operativos"
        tratamiento = "Gasto del periodo"
        detalle = "Otros"
        estado = "Pendiente"
        if any(k in lower for k in ["kit", "anestesia", "stock", "inventario", "mercancia", "mercancía"]):
            categoria = "Proyectos"
            tratamiento = "Inventario"
            detalle = "Materiales"
        elif any(k in lower for k in ["interes", "interés", "banco", "comision", "comisión"]):
            categoria = "Gasto financiero"
            tratamiento = "Gasto del periodo"
            detalle = "Intereses"
        elif any(k in lower for k in ["alquiler", "internet", "poliza", "póliza", "seguro"]):
            categoria = "Gastos fijos"
            detalle = "Alquiler" if "alquiler" in lower else "Otros"
        elif any(k in lower for k in ["laptop", "vehiculo", "vehículo", "computadora", "equipo"]):
            tratamiento = "Activo fijo"
            detalle = "Otros"
    return {
        DOC_COL_TIPO: tipo,
        DOC_COL_DESTINO: destino,
        DOC_COL_ACCION: "Registrar",
        DOC_COL_CATEGORIA: categoria,
        DOC_COL_TRATAMIENTO: tratamiento,
        DOC_COL_ESTADO_MOV: estado,
        DOC_COL_FECHA_HECHO: _ts(fecha_doc),
        DOC_COL_FECHA_ESPERADA: _ts(fecha_esperada) if estado == "Pendiente" else pd.NaT,
        DOC_COL_FECHA_REAL: _ts(fecha_doc) if estado == "Realizado" else pd.NaT,
        DOC_COL_MONTO: monto,
        DOC_COL_CONTRAPARTE: "",
        DOC_COL_DETALLE: detalle,
        DOC_COL_DESCRIPCION: desc[:240],
        DOC_COL_CONFIANZA: 0.45 if monto > 0 else 0.25,
    }


def _extract_json_object(raw: str) -> dict:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.S)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _call_openai_finance_doc(*, api_key: str, model: str, file_name: str, mime_type: str, content: bytes | None, user_text: str, user_note: str) -> dict:
    allowed_income_categories = ", ".join(ING_CATEGORY_OPTIONS)
    allowed_expense_categories = ", ".join(GAS_CATEGORY_OPTIONS)
    allowed_expense_treatments = ", ".join(GAS_BALANCE_OPTIONS)
    prompt = (
        "Extrae datos de un documento financiero para crear un BORRADOR, no un registro final.\n"
        "Devuelve SOLO JSON valido con estas claves: tipo_documento, destino_sugerido, accion_sugerida, categoria_operativa, "
        "tratamiento_balance, estado_movimiento, fecha_hecho, fecha_esperada, fecha_real, monto, contraparte, detalle, descripcion, confianza, explicacion.\n"
        "Destinos permitidos: Ingreso, Gasto, Tarjeta de credito, Linea de credito, Factoring, Gestion de cobro, Revisar manualmente.\n"
        f"Categorias de ingreso permitidas: {allowed_income_categories}.\n"
        f"Categorias de gasto permitidas: {allowed_expense_categories}.\n"
        f"Tratamientos de gasto permitidos: {allowed_expense_treatments}.\n"
        "Tratamientos de ingreso permitidos: Cuenta por cobrar, Caja / banco, Patrimonio, Pasivo financiero.\n"
        "Fechas en formato YYYY-MM-DD. Monto como numero decimal.\n"
        "Reglas: financiamiento recibido no es ingreso operativo; pagos de tarjeta no son gasto nuevo de consumos; capital de deuda no va al resultado; inventario/activo fijo/prepago requieren revision humana.\n"
        "Si no estas seguro usa destino_sugerido='Revisar manualmente' y confianza baja.\n\n"
        f"Archivo: {file_name}\nTipo MIME: {mime_type}\nNota usuario: {user_note or '-'}\nTexto usuario/OCR local: {user_text or '-'}"
    )
    content_items: list[dict] = [{"type": "text", "text": prompt}]
    if content and str(mime_type or "").lower().startswith("image/"):
        b64 = base64.b64encode(content).decode("ascii")
        content_items.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{b64}", "detail": "low"},
            }
        )
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "Eres un extractor financiero conservador. Devuelve JSON valido y marca baja confianza si falta informacion."},
                {"role": "user", "content": content_items},
            ],
            "temperature": 0.0,
            "max_tokens": 700,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    raw = str((payload.get("choices") or [{}])[0].get("message", {}).get("content", "") or "")
    return _extract_json_object(raw)


def _apply_ai_doc_payload(row: pd.Series, payload: dict) -> dict:
    out = row.to_dict()
    tipo = str(payload.get("tipo_documento", "") or "").strip() or out.get(DOC_COL_TIPO, "")
    destino = str(payload.get("destino_sugerido", "") or "").strip() or out.get(DOC_COL_DESTINO, "")
    categoria = str(payload.get("categoria_operativa", "") or "").strip() or out.get(DOC_COL_CATEGORIA, "")
    tratamiento = str(payload.get("tratamiento_balance", "") or "").strip() or out.get(DOC_COL_TRATAMIENTO, "")
    estado = str(payload.get("estado_movimiento", "") or "").strip() or out.get(DOC_COL_ESTADO_MOV, "")
    detalle = str(payload.get("detalle", "") or "").strip() or out.get(DOC_COL_DETALLE, "")
    monto = float(pd.to_numeric(pd.Series([payload.get("monto", out.get(DOC_COL_MONTO, 0.0))]), errors="coerce").fillna(0.0).iloc[0])
    confianza = float(pd.to_numeric(pd.Series([payload.get("confianza", out.get(DOC_COL_CONFIANZA, 0.0))]), errors="coerce").fillna(0.0).iloc[0])
    out.update(
        {
            DOC_COL_TIPO: tipo,
            DOC_COL_DESTINO: destino,
            DOC_COL_ACCION: str(payload.get("accion_sugerida", "Registrar") or "Registrar").strip(),
            DOC_COL_CATEGORIA: categoria,
            DOC_COL_TRATAMIENTO: tratamiento,
            DOC_COL_ESTADO_MOV: estado,
            DOC_COL_FECHA_HECHO: _ts(payload.get("fecha_hecho", out.get(DOC_COL_FECHA_HECHO))),
            DOC_COL_FECHA_ESPERADA: _ts(payload.get("fecha_esperada", out.get(DOC_COL_FECHA_ESPERADA))),
            DOC_COL_FECHA_REAL: _ts(payload.get("fecha_real", out.get(DOC_COL_FECHA_REAL))),
            DOC_COL_MONTO: max(0.0, monto),
            DOC_COL_CONTRAPARTE: str(payload.get("contraparte", out.get(DOC_COL_CONTRAPARTE, "")) or "").strip(),
            DOC_COL_DETALLE: detalle,
            DOC_COL_DESCRIPCION: str(payload.get("descripcion", out.get(DOC_COL_DESCRIPCION, "")) or "").strip()[:240],
            DOC_COL_CONFIANZA: max(0.0, min(1.0, confianza)),
            DOC_COL_JSON: json.dumps(payload, ensure_ascii=False),
            DOC_COL_MENSAJE: str(payload.get("explicacion", "Procesado con IA.") or "Procesado con IA.").strip()[:500],
            DOC_COL_API_USADA: "Si",
        }
    )
    return out


def _finance_doc_possible_duplicate(row: pd.Series, ing_df: pd.DataFrame, gas_df: pd.DataFrame, docs_df: pd.DataFrame) -> str:
    file_hash = str(row.get(DOC_COL_HASH, "") or "").strip()
    rid = str(row.get(DOC_COL_ROWID, "") or "").strip()
    if file_hash:
        dup_docs = docs_df[(docs_df[DOC_COL_HASH].astype(str).str.strip() == file_hash) & (docs_df[DOC_COL_ROWID].astype(str).str.strip() != rid)]
        if not dup_docs.empty:
            return "Posible duplicado: mismo archivo ya existe en la bandeja."
    monto = float(pd.to_numeric(pd.Series([row.get(DOC_COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
    fecha = _ts(row.get(DOC_COL_FECHA_HECHO))
    if monto <= 0 or pd.isna(fecha):
        return ""
    for label, df in [("ingresos", ing_df), ("gastos", gas_df)]:
        if df is None or df.empty:
            continue
        amounts = pd.to_numeric(df.get(COL_MONTO, pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        dates = _ts(df.get(COL_FECHA, pd.Series(dtype=str)))
        mask = (amounts.sub(monto).abs() <= 0.01) & (dates.dt.date == fecha.date())
        if mask.any():
            return f"Posible duplicado: existe movimiento similar en {label} por monto y fecha."
    return ""


def _build_ingreso_row_from_doc(row: pd.Series) -> dict:
    estado = str(row.get(DOC_COL_ESTADO_MOV, "Pendiente") or "Pendiente").strip()
    categoria = str(row.get(DOC_COL_CATEGORIA, "Proyectos") or "Proyectos").strip()
    monto = float(row.get(DOC_COL_MONTO, 0.0) or 0.0)
    fecha_hecho = _ts(row.get(DOC_COL_FECHA_HECHO))
    fecha_esp = _ts(row.get(DOC_COL_FECHA_ESPERADA))
    fecha_real = _ts(row.get(DOC_COL_FECHA_REAL))
    if estado == "Realizado" and pd.isna(fecha_real):
        fecha_real = fecha_hecho
    por_cobrar = "No" if estado == "Realizado" else "Si"
    desc = str(row.get(DOC_COL_DESCRIPCION, "") or row.get(DOC_COL_ARCHIVO, "") or "Ingreso desde documento").strip()
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: fecha_hecho,
        COL_DESC: desc,
        COL_CONC: desc,
        COL_MONTO: monto,
        COL_CAT: categoria,
        COL_ESC: "Real",
        COL_PROY: "",
        COL_CLI_ID: "",
        COL_CLI_NOM: "",
        COL_EMP: str(row.get(DOC_COL_EMPRESA, EMPRESA_DEFAULT) or EMPRESA_DEFAULT),
        COL_POR_COB: por_cobrar,
        COL_COB: "Si" if estado == "Realizado" else "No",
        COL_FCOBRO: fecha_esp if por_cobrar == "Si" else pd.NaT,
        COL_FCOBRO_REAL: fecha_real if por_cobrar == "No" else pd.NaT,
        COL_REC: "No",
        COL_REC_PER: "",
        COL_REC_REG: "",
        COL_REC_DUR: "",
        COL_REC_HASTA: pd.NaT,
        COL_REC_CANT: 0,
        COL_ING_DET: str(row.get(DOC_COL_DETALLE, "Otro") or "Otro"),
        COL_ING_NAT: _derive_ing_nature(categoria),
        COL_TRAT_BAL_ING: _derive_ing_balance(categoria, estado),
        COL_CTP_TIPO: "",
        COL_CTP_NOMBRE: str(row.get(DOC_COL_CONTRAPARTE, "") or "").strip(),
        COL_COBRO_REAL_MONTO: monto if por_cobrar == "No" else 0.0,
        COL_ING_PARTIALS: _serialize_partial_events([{"fecha": fecha_real, "monto": monto, "nota": "Aprobado desde documento"}]) if por_cobrar == "No" else "",
        COL_FACT_DET: "",
        COL_FIN_TOGGLE: "No",
        COL_FIN_TIPO: "",
        COL_FIN_MONTO: 0.0,
        COL_FIN_FEC_INI: pd.NaT,
        COL_FIN_PLAZO: 0,
        COL_FIN_TASA: 0.0,
        COL_FIN_TASA_TIPO: "",
        COL_FIN_MODALIDAD: "",
        COL_FIN_PERIOD: "",
        COL_FIN_CRONO: "",
        COL_FIN_INSTRUMENTO: "",
        COL_FIN_REG_TIPO: "",
        COL_USER: _current_user(),
    }


def _build_gasto_row_from_doc(row: pd.Series) -> dict:
    estado = str(row.get(DOC_COL_ESTADO_MOV, "Pendiente") or "Pendiente").strip()
    categoria = str(row.get(DOC_COL_CATEGORIA, "Gastos operativos") or "Gastos operativos").strip()
    tratamiento = str(row.get(DOC_COL_TRATAMIENTO, "Gasto del periodo") or "Gasto del periodo").strip()
    monto = float(row.get(DOC_COL_MONTO, 0.0) or 0.0)
    fecha_hecho = _ts(row.get(DOC_COL_FECHA_HECHO))
    fecha_esp = _ts(row.get(DOC_COL_FECHA_ESPERADA))
    fecha_real = _ts(row.get(DOC_COL_FECHA_REAL))
    if estado == "Realizado" and pd.isna(fecha_real):
        fecha_real = fecha_hecho
    por_pagar = "No" if estado == "Realizado" else "Si"
    desc = str(row.get(DOC_COL_DESCRIPCION, "") or row.get(DOC_COL_ARCHIVO, "") or "Gasto desde documento").strip()
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: fecha_hecho,
        COL_DESC: desc,
        COL_CONC: desc,
        COL_MONTO: monto,
        COL_CAT: categoria,
        COL_ESC: "Real",
        COL_REF_RID: "",
        COL_PROY: "",
        COL_CLI_ID: "",
        COL_CLI_NOM: "",
        COL_EMP: str(row.get(DOC_COL_EMPRESA, EMPRESA_DEFAULT) or EMPRESA_DEFAULT),
        COL_POR_PAG: por_pagar,
        COL_PROV: str(row.get(DOC_COL_CONTRAPARTE, "") or "").strip(),
        COL_REC: "No",
        COL_REC_PER: "",
        COL_REC_REG: "",
        COL_FPAGO: fecha_esp if por_pagar == "Si" else pd.NaT,
        COL_FPAGO_REAL: fecha_real if por_pagar == "No" else pd.NaT,
        COL_REC_DUR: "",
        COL_REC_HASTA: pd.NaT,
        COL_REC_CANT: 0,
        COL_GAS_SUB: _derive_gas_sub(categoria),
        COL_GAS_DET: str(row.get(DOC_COL_DETALLE, "Otros") or "Otros"),
        COL_TRAT_BAL_GAS: tratamiento,
        COL_CTP_TIPO: "",
        COL_CTP_NOMBRE: str(row.get(DOC_COL_CONTRAPARTE, "") or "").strip(),
        COL_PAGO_REAL_MONTO: monto if por_pagar == "No" else 0.0,
        COL_GAS_PARTIALS: _serialize_partial_events([{"fecha": fecha_real, "monto": monto, "nota": "Aprobado desde documento"}]) if por_pagar == "No" else "",
        COL_PREPAGO_MESES: 0,
        COL_PREPAGO_FEC_INI: pd.NaT,
        COL_INV_MOV: "",
        COL_INV_ITEM: "",
        COL_INV_FEC_LLEGADA: pd.NaT,
        COL_AF_TOGGLE: "No",
        COL_AF_TIPO: "",
        COL_AF_VIDA: 0,
        COL_AF_FEC_INI: pd.NaT,
        COL_AF_VAL_RES: 0.0,
        COL_AF_DEP_TOGGLE: "No",
        COL_AF_DEP_MENSUAL: 0.0,
        COL_FIN_TOGGLE: "No",
        COL_FIN_TIPO: "",
        COL_FIN_MONTO: 0.0,
        COL_FIN_FEC_INI: pd.NaT,
        COL_FIN_PLAZO: 0,
        COL_FIN_TASA: 0.0,
        COL_FIN_TASA_TIPO: "",
        COL_FIN_MODALIDAD: "",
        COL_FIN_PERIOD: "",
        COL_FIN_CRONO: "",
        COL_FIN_INSTRUMENTO: "",
        COL_FIN_REG_TIPO: "",
        COL_USER: _current_user(),
    }


def _can_auto_register_doc(row: pd.Series) -> tuple[bool, str]:
    destino = str(row.get(DOC_COL_DESTINO, "") or "").strip()
    categoria = str(row.get(DOC_COL_CATEGORIA, "") or "").strip()
    tratamiento = str(row.get(DOC_COL_TRATAMIENTO, "") or "").strip()
    monto = float(pd.to_numeric(pd.Series([row.get(DOC_COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
    fecha = _ts(row.get(DOC_COL_FECHA_HECHO))
    if monto <= 0:
        return False, "El borrador no tiene monto valido."
    if pd.isna(fecha):
        return False, "El borrador no tiene fecha del hecho economico valida."
    if destino == "Ingreso":
        if categoria not in ING_CATEGORY_OPTIONS:
            return False, f"Categoria `{categoria}` no es valida para ingresos."
        if categoria in {"Financiamiento recibido", "Aporte de socio / capital"}:
            return False, f"`{categoria}` debe registrarse por flujo manual/especializado para evitar omitir condiciones."
        return True, ""
    if destino == "Gasto":
        if categoria not in GAS_CATEGORY_OPTIONS:
            return False, f"Categoria `{categoria}` no es valida para gastos."
        if categoria == "Inversiones":
            return False, "`Inversiones` debe registrarse manualmente para validar contraparte y tratamiento."
        if tratamiento != "Gasto del periodo":
            return False, f"Tratamiento `{tratamiento}` requiere el flujo especializado de Finanzas 1."
        return True, ""
    return False, f"Destino `{destino}` requiere revision y registro manual en su flujo especializado."


def _render_finance_docs_company_inbox(
    *,
    empresa: str,
    client_obj,
    sheet_id: str,
    creds_obj,
    docs_before: pd.DataFrame,
    docs_df: pd.DataFrame,
    folder_id: str,
    api_key: str,
    model_name: str,
) -> None:
    emp_key = str(empresa or "").replace("-", "_")
    docs_company = docs_df[docs_df[DOC_COL_EMPRESA].astype(str).str.upper().str.strip().eq(str(empresa).upper())].copy()
    status_s = docs_company[DOC_COL_ESTADO].astype(str).str.strip()
    pending_docs = docs_company[status_s.isin({"", "Borrador", "Procesado", "Requiere revision"})].copy()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Pendientes", int(len(pending_docs)))
    m2.metric("Aprobados", int(status_s.eq("Aprobado").sum()))
    m3.metric("Descartados", int(status_s.eq("Descartado").sum()))
    m4.metric("API", "Si" if api_key else "No")
    st.caption(f"Carpeta Drive {empresa}: `{folder_id or 'no configurada'}`")
    if not folder_id:
        st.caption(f"Secret esperado: `DRIVE_FINANCE_DOCS_FOLDER_ID_{emp_key}`")

    use_api = st.checkbox(
        "Procesar nuevos con IA si hay API configurada",
        value=bool(api_key),
        key=f"doc_auto_ai_{emp_key}",
        help="Solo se usa para documentos nuevos al subir/verificar. No registra nada automaticamente.",
    )
    limit = st.number_input(
        "Cantidad maxima a verificar",
        min_value=1,
        max_value=100,
        value=25,
        step=1,
        key=f"doc_scan_limit_{emp_key}",
    )

    scan_col, upload_col = st.columns(2)
    with scan_col:
        if st.button(f"Verificar ahora {empresa}", key=f"btn_doc_scan_{emp_key}"):
            if not folder_id:
                st.error(f"Falta configurar carpeta Drive para {empresa}.")
            else:
                try:
                    new_docs, created, skipped, errors = _scan_finance_docs_folder_for_empresa(
                        creds_obj=creds_obj,
                        folder_id=folder_id,
                        docs_df=docs_df,
                        empresa=empresa,
                        limit=int(limit),
                        use_api=bool(use_api and api_key),
                        api_key=api_key,
                        model_name=model_name,
                    )
                    if created:
                        wrote = safe_write_finance_docs(client_obj, sheet_id, new_docs, old_df=docs_before)
                        if wrote:
                            st.cache_data.clear()
                            st.success(f"{created} documento(s) nuevo(s) preparados. {skipped} ya existian.")
                            _safe_rerun()
                        else:
                            st.info("No hubo cambios para guardar.")
                    else:
                        st.info(f"No se encontraron documentos nuevos. Ya existentes: {skipped}.")
                    if errors:
                        st.warning("Algunos documentos no se pudieron preparar: " + " | ".join(errors[:5]))
                except Exception as exc:
                    st.error(f"No se pudo verificar Drive. Finanzas manual no fue afectado. Detalle: {str(exc)[:220]}")

    with upload_col:
        uploaded_docs = st.file_uploader(
            f"Subir archivos a Drive {empresa}",
            type=["pdf", "png", "jpg", "jpeg", "webp"],
            accept_multiple_files=True,
            key=f"doc_upload_files_{emp_key}",
        )
        quick_note = st.text_input("Nota opcional", key=f"doc_upload_note_{emp_key}")
        if st.button(f"Subir y preparar {empresa}", key=f"btn_doc_upload_{emp_key}"):
            if not folder_id:
                st.error(f"Falta configurar carpeta Drive para {empresa}.")
            elif not uploaded_docs:
                st.error("Sube al menos un archivo.")
            else:
                work_docs = docs_df.copy()
                existing_hashes = set(work_docs.get(DOC_COL_HASH, pd.Series(dtype=str)).astype(str).str.strip())
                created = 0
                skipped = 0
                errors: list[str] = []
                for uploaded in uploaded_docs:
                    try:
                        raw = uploaded.getvalue()
                        file_hash = _hash_bytes(raw)
                        if file_hash in existing_hashes:
                            skipped += 1
                            continue
                        mime_type = uploaded.type or mimetypes.guess_type(uploaded.name)[0] or "application/octet-stream"
                        drive_meta = _upload_finance_doc_to_drive(
                            creds_obj,
                            filename=uploaded.name,
                            content=raw,
                            mime_type=mime_type,
                            folder_id=folder_id,
                        )
                        proposal, local_text, api_used, message = _analyze_finance_doc_content(
                            file_name=str(drive_meta.get("name") or uploaded.name),
                            mime_type=str(drive_meta.get("mime") or mime_type),
                            content=raw,
                            empresa=empresa,
                            note=quick_note,
                            use_api=bool(use_api and api_key),
                            api_key=api_key,
                            model_name=model_name,
                        )
                        new_row = _new_finance_doc_row(
                            empresa=empresa,
                            file_name=str(drive_meta.get("name") or uploaded.name),
                            file_hash=file_hash,
                            mime_type=str(drive_meta.get("mime") or mime_type),
                            drive_id=str(drive_meta.get("id", "")),
                            drive_url=str(drive_meta.get("url", "")),
                            origen="Upload app",
                            note=quick_note,
                            local_text=local_text,
                            proposal=proposal,
                            message=message,
                            api_used=api_used,
                            model_name=model_name,
                        )
                        new_row[DOC_COL_DUPLICADO] = _finance_doc_possible_duplicate(pd.Series(new_row), st.session_state.df_ing, st.session_state.df_gas, work_docs)
                        work_docs = ensure_finance_docs_columns(pd.concat([work_docs, pd.DataFrame([new_row])], ignore_index=True))
                        existing_hashes.add(file_hash)
                        created += 1
                    except Exception as exc:
                        errors.append(f"{uploaded.name}: {str(exc)[:160]}")
                if created:
                    try:
                        wrote = safe_write_finance_docs(client_obj, sheet_id, work_docs, old_df=docs_before)
                        if wrote:
                            st.cache_data.clear()
                            st.success(f"{created} borrador(es) preparado(s). Duplicados omitidos: {skipped}.")
                            _safe_rerun()
                        else:
                            st.info("No hubo cambios para guardar.")
                    except Exception as exc:
                        st.error(f"No se pudo guardar la bandeja. Ingresos/Gastos no fueron modificados. Detalle: {str(exc)[:220]}")
                elif skipped:
                    st.info(f"No se crearon borradores nuevos. Duplicados omitidos: {skipped}.")
                if errors:
                    st.warning("Archivos no procesados: " + " | ".join(errors[:5]))

    st.divider()
    st.markdown("#### Borradores listos para revisar")
    if pending_docs.empty:
        st.info(f"No hay borradores pendientes para {empresa}.")
        return

    editable_cols = [
        DOC_COL_ROWID,
        DOC_COL_ARCHIVO,
        DOC_COL_TIPO,
        DOC_COL_DESTINO,
        DOC_COL_CATEGORIA,
        DOC_COL_TRATAMIENTO,
        DOC_COL_ESTADO_MOV,
        DOC_COL_FECHA_HECHO,
        DOC_COL_FECHA_ESPERADA,
        DOC_COL_FECHA_REAL,
        DOC_COL_MONTO,
        DOC_COL_CONTRAPARTE,
        DOC_COL_DETALLE,
        DOC_COL_DESCRIPCION,
        DOC_COL_CONFIANZA,
        DOC_COL_DUPLICADO,
        DOC_COL_MENSAJE,
    ]
    edited_pending = st.data_editor(
        pending_docs[editable_cols],
        use_container_width=True,
        hide_index=True,
        disabled=[DOC_COL_ROWID, DOC_COL_ARCHIVO, DOC_COL_CONFIANZA, DOC_COL_DUPLICADO, DOC_COL_MENSAJE],
        column_config={
            DOC_COL_DESTINO: st.column_config.SelectboxColumn(
                DOC_COL_DESTINO,
                options=["Ingreso", "Gasto", "Tarjeta de credito", "Linea de credito", "Factoring", "Gestion de cobro", "Revisar manualmente"],
            ),
            DOC_COL_CATEGORIA: st.column_config.SelectboxColumn(
                DOC_COL_CATEGORIA,
                options=[""] + sorted(set(ING_CATEGORY_OPTIONS + GAS_CATEGORY_OPTIONS + ["Financiamiento recibido"])),
            ),
            DOC_COL_TRATAMIENTO: st.column_config.SelectboxColumn(
                DOC_COL_TRATAMIENTO,
                options=[""] + sorted(set(ING_BALANCE_OPTIONS + GAS_BALANCE_OPTIONS + ["Pasivo financiero"])),
            ),
            DOC_COL_ESTADO_MOV: st.column_config.SelectboxColumn(DOC_COL_ESTADO_MOV, options=STATE_OPTIONS),
        },
        key=f"doc_pending_editor_{emp_key}",
    )
    if st.button(f"Guardar correcciones {empresa}", key=f"btn_doc_save_editor_{emp_key}"):
        try:
            new_docs = docs_df.copy()
            for _, edited_row in edited_pending.iterrows():
                rid = str(edited_row.get(DOC_COL_ROWID, "") or "").strip()
                idx = new_docs.index[new_docs[DOC_COL_ROWID].astype(str).eq(rid)]
                if len(idx) == 0:
                    continue
                for col in editable_cols:
                    if col in {DOC_COL_ROWID, DOC_COL_ARCHIVO, DOC_COL_CONFIANZA, DOC_COL_DUPLICADO, DOC_COL_MENSAJE}:
                        continue
                    new_docs.loc[idx[0], col] = edited_row.get(col, "")
                new_docs.loc[idx[0], DOC_COL_DUPLICADO] = _finance_doc_possible_duplicate(
                    new_docs.loc[idx[0]],
                    st.session_state.df_ing,
                    st.session_state.df_gas,
                    new_docs,
                )
            wrote = safe_write_finance_docs(client_obj, sheet_id, new_docs, old_df=docs_before)
            if wrote:
                st.cache_data.clear()
                st.success("Correcciones guardadas.")
                _safe_rerun()
            else:
                st.info("No hubo cambios para guardar.")
        except Exception as exc:
            st.error(f"No se pudo guardar la correccion. Ingresos/Gastos no fueron modificados. Detalle: {str(exc)[:220]}")

    labels = [
        f"{str(row.get(DOC_COL_ARCHIVO, '')).strip() or 'Documento'} | {str(row.get(DOC_COL_DESTINO, '')).strip() or 'Sin destino'} | {_format_money_es(float(row.get(DOC_COL_MONTO, 0.0) or 0.0))} | {str(row.get(DOC_COL_ROWID, ''))[:8]}"
        for _, row in pending_docs.iterrows()
    ]
    selected_label = st.selectbox("Borrador para aprobar/quitar", labels, key=f"doc_action_select_{emp_key}")
    selected_row = pending_docs.iloc[labels.index(selected_label)].copy()
    selected_id = str(selected_row.get(DOC_COL_ROWID, "") or "").strip()
    if str(selected_row.get(DOC_COL_DRIVE_URL, "") or "").strip():
        st.markdown(f"[Abrir documento en Drive]({str(selected_row.get(DOC_COL_DRIVE_URL)).strip()})")
    if str(selected_row.get(DOC_COL_DUPLICADO, "") or "").strip():
        st.warning(str(selected_row.get(DOC_COL_DUPLICADO)))
    confirm_dup = st.checkbox("Registrar aunque marque duplicado", key=f"doc_confirm_dup_{emp_key}_{selected_id}")
    a1, a2, a3 = st.columns(3)
    with a1:
        if st.button("Procesar seleccionado con IA", key=f"btn_doc_ai_{emp_key}_{selected_id}"):
            if not api_key:
                st.error("Falta `OPENAI_API_KEY`; no se llamo a la API.")
            else:
                try:
                    file_bytes = None
                    file_name = str(selected_row.get(DOC_COL_ARCHIVO, "") or "documento")
                    mime_type = str(selected_row.get(DOC_COL_MIME, "") or "")
                    drive_id = str(selected_row.get(DOC_COL_DRIVE_ID, "") or "").strip()
                    if drive_id:
                        file_bytes, file_name_dl, mime_dl = _download_drive_file_bytes(creds_obj, drive_id)
                        file_name = file_name_dl or file_name
                        mime_type = mime_dl or mime_type
                    local_text = _finance_doc_local_text(file_bytes, mime_type, str(selected_row.get(DOC_COL_TEXTO_USUARIO, "") or ""))
                    payload = _call_openai_finance_doc(
                        api_key=api_key,
                        model=model_name,
                        file_name=file_name,
                        mime_type=mime_type,
                        content=file_bytes,
                        user_text=local_text,
                        user_note=str(selected_row.get(DOC_COL_NOTA, "") or ""),
                    )
                    updated = _apply_ai_doc_payload(selected_row, payload)
                    updated[DOC_COL_MODELO] = model_name
                    updated[DOC_COL_ESTADO] = "Procesado"
                    updated[DOC_COL_TEXTO_USUARIO] = local_text
                    new_docs = docs_df.copy()
                    idx = new_docs.index[new_docs[DOC_COL_ROWID].astype(str).eq(selected_id)]
                    if len(idx) > 0:
                        updated[DOC_COL_DUPLICADO] = _finance_doc_possible_duplicate(pd.Series(updated), st.session_state.df_ing, st.session_state.df_gas, new_docs)
                        for col, val in updated.items():
                            new_docs.loc[idx[0], col] = val
                        wrote = safe_write_finance_docs(client_obj, sheet_id, new_docs, old_df=docs_before)
                        if wrote:
                            st.cache_data.clear()
                            st.success("Borrador procesado con IA.")
                            _safe_rerun()
                except Exception as exc:
                    st.error(f"No se pudo procesar con IA. No se registro nada. Detalle: {str(exc)[:220]}")
    with a2:
        if st.button("Aceptar y cargar a Finanzas 1", key=f"btn_doc_approve_{emp_key}_{selected_id}"):
            try:
                updated = selected_row.to_dict()
                updated[DOC_COL_EMPRESA] = empresa
                dup_msg = _finance_doc_possible_duplicate(pd.Series(updated), st.session_state.df_ing, st.session_state.df_gas, docs_df)
                updated[DOC_COL_DUPLICADO] = dup_msg
                can_register, reason = _can_auto_register_doc(pd.Series(updated))
                if dup_msg and not confirm_dup:
                    st.error("Hay posible duplicado. Marca la confirmacion si aun deseas registrar.")
                elif not can_register:
                    st.error(reason)
                else:
                    old_ing_df = st.session_state.df_ing.copy()
                    old_gas_df = st.session_state.df_gas.copy()
                    ws_reg = ""
                    rid_reg = ""
                    wrote_main = False
                    if str(updated.get(DOC_COL_DESTINO, "")).strip() == "Ingreso":
                        new_row = _build_ingreso_row_from_doc(pd.Series(updated))
                        rid_reg = str(new_row.get(COL_ROWID, ""))
                        new_ing = ensure_ingresos_columns(pd.concat([old_ing_df, pd.DataFrame([new_row])], ignore_index=True))
                        wrote_main = safe_write_worksheet(client_obj, sheet_id, WS_ING, new_ing, old_df=old_ing_df)
                        if wrote_main:
                            st.session_state.df_ing = new_ing
                            ws_reg = WS_ING
                    elif str(updated.get(DOC_COL_DESTINO, "")).strip() == "Gasto":
                        new_row = _build_gasto_row_from_doc(pd.Series(updated))
                        rid_reg = str(new_row.get(COL_ROWID, ""))
                        new_gas = ensure_gastos_columns(pd.concat([old_gas_df, pd.DataFrame([new_row])], ignore_index=True))
                        wrote_main = safe_write_worksheet(client_obj, sheet_id, WS_GAS, new_gas, old_df=old_gas_df)
                        if wrote_main:
                            st.session_state.df_gas = new_gas
                            ws_reg = WS_GAS
                    if not wrote_main:
                        st.error("No se pudo registrar en Finanzas 1.")
                    else:
                        updated[DOC_COL_ESTADO] = "Aprobado"
                        updated[DOC_COL_WS_REG] = ws_reg
                        updated[DOC_COL_ROWID_REG] = rid_reg
                        updated[DOC_COL_APROBADO_POR] = _current_user()
                        updated[DOC_COL_FECHA_APROBADO] = _ts(_today())
                        new_docs = docs_df.copy()
                        idx = new_docs.index[new_docs[DOC_COL_ROWID].astype(str).eq(selected_id)]
                        if len(idx) > 0:
                            for col, val in updated.items():
                                new_docs.loc[idx[0], col] = val
                            wrote_doc = safe_write_finance_docs(client_obj, sheet_id, new_docs, old_df=docs_before)
                            if not wrote_doc:
                                st.warning("El movimiento fue registrado, pero el borrador no pudo marcarse como aprobado.")
                        st.cache_data.clear()
                        st.success(f"Borrador registrado en `{ws_reg}`.")
                        _safe_rerun()
            except Exception as exc:
                st.error(f"No se pudo aprobar. Detalle: {str(exc)[:220]}")
    with a3:
        if st.button("Quitar borrador", key=f"btn_doc_discard_{emp_key}_{selected_id}"):
            try:
                new_docs = docs_df.copy()
                idx = new_docs.index[new_docs[DOC_COL_ROWID].astype(str).eq(selected_id)]
                if len(idx) > 0:
                    new_docs.loc[idx[0], DOC_COL_ESTADO] = "Descartado"
                    new_docs.loc[idx[0], DOC_COL_MENSAJE] = "Descartado por usuario."
                    wrote = safe_write_finance_docs(client_obj, sheet_id, new_docs, old_df=docs_before)
                    if wrote:
                        st.cache_data.clear()
                        st.success("Borrador descartado.")
                        _safe_rerun()
            except Exception as exc:
                st.error(f"No se pudo quitar. Detalle: {str(exc)[:220]}")


def _build_credit_line_ingreso_row(*, line_row: pd.Series, fecha_evento, monto: float, nota: str = "") -> dict:
    line_name = str(line_row.get(LC_COL_NOMBRE, "") or "").strip()
    bank_name = str(line_row.get(LC_COL_BANCO, "") or "").strip()
    desc = f"Desembolso linea de credito - {line_name}" if line_name else "Desembolso linea de credito"
    if str(nota or "").strip():
        desc = f"{desc} | {str(nota or '').strip()}"
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: _ts(fecha_evento),
        COL_MONTO: float(monto),
        COL_PROY: "",
        COL_CLI_ID: "",
        COL_CLI_NOM: "",
        COL_EMP: str(line_row.get(LC_COL_EMPRESA, EMPRESA_DEFAULT) or EMPRESA_DEFAULT).strip(),
        COL_DESC: desc,
        COL_CONC: desc,
        COL_POR_COB: "No",
        COL_COB: "Si",
        COL_FCOBRO: pd.NaT,
        COL_FCOBRO_REAL: _ts(fecha_evento),
        COL_CTP_TIPO: "Banco",
        COL_CTP_NOMBRE: bank_name,
        COL_COBRO_REAL_MONTO: float(monto),
        COL_ING_PARTIALS: _serialize_partial_events([{"fecha": _ts(fecha_evento), "monto": float(monto), "nota": desc}]),
        COL_REC: "No",
        COL_CAT: "Financiamiento recibido",
        COL_ING_DET: "Prestamo recibido",
        COL_ING_NAT: "Financiamiento",
        COL_TRAT_BAL_ING: "Pasivo financiero",
        COL_FIN_TOGGLE: "Sí",
        COL_FIN_TIPO: "Financiamiento recibido",
        COL_FIN_MONTO: float(monto),
        COL_FIN_FEC_INI: _ts(fecha_evento),
        COL_FIN_PLAZO: 0,
        COL_FIN_TASA: 0.0,
        COL_FIN_TASA_TIPO: "",
        COL_FIN_MODALIDAD: "",
        COL_FIN_PERIOD: "",
        COL_FIN_CRONO: "",
        COL_FIN_INSTRUMENTO: line_name,
        COL_FIN_REG_TIPO: "Desembolso",
        COL_ESC: "Real",
        COL_USER: _current_user(),
    }


def _build_credit_line_gasto_row(
    *,
    line_row: pd.Series,
    fecha_evento,
    monto: float,
    descripcion: str,
    tratamiento: str,
    detalle_gasto: str = "Otros",
    registro_financiamiento: str = "",
    prepago_meses: int = 0,
    prepago_inicio=None,
) -> dict:
    bank_name = str(line_row.get(LC_COL_BANCO, "") or "").strip()
    line_name = str(line_row.get(LC_COL_NOMBRE, "") or "").strip()
    partials = [{"fecha": _ts(fecha_evento), "monto": float(monto), "nota": descripcion}]
    is_prepago = tratamiento == "Anticipo / prepago" and int(prepago_meses or 0) > 0
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: _ts(fecha_evento),
        COL_MONTO: float(monto),
        COL_DESC: descripcion,
        COL_CONC: descripcion,
        COL_CAT: "Gasto financiero",
        COL_ESC: "Real",
        COL_REF_RID: "",
        COL_PROY: "",
        COL_CLI_ID: "",
        COL_CLI_NOM: "",
        COL_EMP: str(line_row.get(LC_COL_EMPRESA, EMPRESA_DEFAULT) or EMPRESA_DEFAULT).strip(),
        COL_POR_PAG: "No",
        COL_PROV: bank_name,
        COL_REC: "No",
        COL_FPAGO: pd.NaT,
        COL_FPAGO_REAL: _ts(fecha_evento),
        COL_CTP_TIPO: "Banco",
        COL_CTP_NOMBRE: bank_name,
        COL_PAGO_REAL_MONTO: float(monto),
        COL_GAS_PARTIALS: _serialize_partial_events(partials),
        COL_GAS_SUB: "Financiero",
        COL_GAS_DET: detalle_gasto,
        COL_TRAT_BAL_GAS: tratamiento,
        COL_PREPAGO_MESES: int(prepago_meses) if is_prepago else 0,
        COL_PREPAGO_FEC_INI: _ts(prepago_inicio or fecha_evento) if is_prepago else pd.NaT,
        COL_INV_MOV: "",
        COL_INV_ITEM: "",
        COL_INV_FEC_LLEGADA: pd.NaT,
        COL_FIN_TOGGLE: "No",
        COL_FIN_TIPO: "",
        COL_FIN_MONTO: 0.0,
        COL_FIN_FEC_INI: pd.NaT,
        COL_FIN_PLAZO: 0,
        COL_FIN_TASA: 0.0,
        COL_FIN_TASA_TIPO: "",
        COL_FIN_MODALIDAD: "",
        COL_FIN_PERIOD: "",
        COL_FIN_CRONO: "",
        COL_FIN_INSTRUMENTO: line_name,
        COL_FIN_REG_TIPO: registro_financiamiento,
        COL_USER: _current_user(),
    }


def _safe_day_of_month(year: int, month: int, day: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(max(1, int(day)), last_day))


def _estimate_card_due_date(fecha_compra, dia_corte: int, dia_venc: int) -> pd.Timestamp:
    compra = _ts(fecha_compra)
    if pd.isna(compra):
        return pd.NaT
    compra_date = compra.date()
    if compra_date.day <= int(dia_corte):
        cut_year, cut_month = compra_date.year, compra_date.month
    else:
        if compra_date.month == 12:
            cut_year, cut_month = compra_date.year + 1, 1
        else:
            cut_year, cut_month = compra_date.year, compra_date.month + 1
    if cut_month == 12:
        due_year, due_month = cut_year + 1, 1
    else:
        due_year, due_month = cut_year, cut_month + 1
    return pd.Timestamp(_safe_day_of_month(due_year, due_month, int(dia_venc)))


def _build_card_consumo_row(
    *,
    card_row: pd.Series,
    fecha_evento,
    monto: float,
    descripcion: str,
    categoria: str,
    proveedor: str,
    tratamiento: str,
    detalle_gasto: str,
    fecha_pago_esperada,
    proyecto: str = "",
    cliente_id: str = "",
    cliente_nombre: str = "",
    prepago_meses: int = 0,
    prepago_inicio=None,
    inventario_mov: str = "",
    inventario_item: str = "",
    inventario_fecha_llegada=None,
    activo_fijo_tipo: str = "",
    activo_fijo_vida: int = 0,
    activo_fijo_inicio=None,
    activo_fijo_residual: float = 0.0,
    activo_fijo_dep_toggle: str = "No",
    activo_fijo_dep_mensual: float = 0.0,
) -> dict:
    bank_name = str(card_row.get(TC_COL_BANCO, "") or "").strip()
    card_name = str(card_row.get(TC_COL_NOMBRE, "") or "").strip()
    activo_fijo_on = tratamiento == "Activo fijo"
    dep_on = activo_fijo_on and _bool_from_toggle(activo_fijo_dep_toggle)
    is_prepago = tratamiento == "Anticipo / prepago" and int(prepago_meses or 0) > 0
    is_inventory = tratamiento == "Inventario"
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: _ts(fecha_evento),
        COL_MONTO: float(monto),
        COL_DESC: descripcion,
        COL_CONC: descripcion,
        COL_CAT: categoria,
        COL_ESC: "Real",
        COL_REF_RID: "",
        COL_PROY: str(proyecto or "").strip(),
        COL_CLI_ID: str(cliente_id or "").strip(),
        COL_CLI_NOM: str(cliente_nombre or "").strip(),
        COL_EMP: str(card_row.get(TC_COL_EMPRESA, EMPRESA_DEFAULT) or EMPRESA_DEFAULT).strip(),
        COL_POR_PAG: "Sí",
        COL_PROV: str(proveedor or "").strip(),
        COL_REC: "No",
        COL_FPAGO: _ts(fecha_pago_esperada),
        COL_FPAGO_REAL: pd.NaT,
        COL_CTP_TIPO: "Banco",
        COL_CTP_NOMBRE: bank_name,
        COL_PAGO_REAL_MONTO: 0.0,
        COL_GAS_PARTIALS: "",
        COL_GAS_SUB: _derive_gas_sub(categoria),
        COL_GAS_DET: detalle_gasto,
        COL_TRAT_BAL_GAS: tratamiento,
        COL_PREPAGO_MESES: int(prepago_meses) if is_prepago else 0,
        COL_PREPAGO_FEC_INI: _ts(prepago_inicio or fecha_evento) if is_prepago else pd.NaT,
        COL_INV_MOV: inventario_mov if is_inventory else "",
        COL_INV_ITEM: str(inventario_item or "").strip() if is_inventory else "",
        COL_INV_FEC_LLEGADA: _ts(inventario_fecha_llegada) if is_inventory and inventario_mov in INVENTORY_POSITIVE_MOVEMENTS else pd.NaT,
        COL_AF_TOGGLE: "Sí" if activo_fijo_on else "No",
        COL_AF_TIPO: activo_fijo_tipo if activo_fijo_on else "",
        COL_AF_VIDA: int(activo_fijo_vida) if activo_fijo_on else 0,
        COL_AF_FEC_INI: _ts(activo_fijo_inicio or fecha_evento) if activo_fijo_on else pd.NaT,
        COL_AF_VAL_RES: float(activo_fijo_residual) if activo_fijo_on else 0.0,
        COL_AF_DEP_TOGGLE: "Sí" if dep_on else "No",
        COL_AF_DEP_MENSUAL: float(activo_fijo_dep_mensual) if dep_on else 0.0,
        COL_FIN_TOGGLE: "No",
        COL_FIN_TIPO: "",
        COL_FIN_MONTO: 0.0,
        COL_FIN_FEC_INI: pd.NaT,
        COL_FIN_PLAZO: 0,
        COL_FIN_TASA: 0.0,
        COL_FIN_TASA_TIPO: "",
        COL_FIN_MODALIDAD: "",
        COL_FIN_PERIOD: "",
        COL_FIN_CRONO: "",
        COL_FIN_INSTRUMENTO: card_name,
        COL_FIN_REG_TIPO: "Consumo tarjeta",
        COL_USER: _current_user(),
    }


def _build_card_charge_row(
    *,
    card_row: pd.Series,
    fecha_evento,
    monto: float,
    descripcion: str,
    detalle_gasto: str = "Otros",
) -> dict:
    bank_name = str(card_row.get(TC_COL_BANCO, "") or "").strip()
    card_name = str(card_row.get(TC_COL_NOMBRE, "") or "").strip()
    partials = [{"fecha": _ts(fecha_evento), "monto": float(monto), "nota": descripcion}]
    return {
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: _ts(fecha_evento),
        COL_MONTO: float(monto),
        COL_DESC: descripcion,
        COL_CONC: descripcion,
        COL_CAT: "Gasto financiero",
        COL_ESC: "Real",
        COL_REF_RID: "",
        COL_PROY: "",
        COL_CLI_ID: "",
        COL_CLI_NOM: "",
        COL_EMP: str(card_row.get(TC_COL_EMPRESA, EMPRESA_DEFAULT) or EMPRESA_DEFAULT).strip(),
        COL_POR_PAG: "No",
        COL_PROV: bank_name,
        COL_REC: "No",
        COL_FPAGO: pd.NaT,
        COL_FPAGO_REAL: _ts(fecha_evento),
        COL_CTP_TIPO: "Banco",
        COL_CTP_NOMBRE: bank_name,
        COL_PAGO_REAL_MONTO: float(monto),
        COL_GAS_PARTIALS: _serialize_partial_events(partials),
        COL_GAS_SUB: "Financiero",
        COL_GAS_DET: detalle_gasto,
        COL_TRAT_BAL_GAS: "Gasto del periodo",
        COL_PREPAGO_MESES: 0,
        COL_PREPAGO_FEC_INI: pd.NaT,
        COL_INV_MOV: "",
        COL_INV_ITEM: "",
        COL_INV_FEC_LLEGADA: pd.NaT,
        COL_AF_TOGGLE: "No",
        COL_AF_TIPO: "",
        COL_AF_VIDA: 0,
        COL_AF_FEC_INI: pd.NaT,
        COL_AF_VAL_RES: 0.0,
        COL_AF_DEP_TOGGLE: "No",
        COL_AF_DEP_MENSUAL: 0.0,
        COL_FIN_TOGGLE: "No",
        COL_FIN_TIPO: "",
        COL_FIN_MONTO: 0.0,
        COL_FIN_FEC_INI: pd.NaT,
        COL_FIN_PLAZO: 0,
        COL_FIN_TASA: 0.0,
        COL_FIN_TASA_TIPO: "",
        COL_FIN_MODALIDAD: "",
        COL_FIN_PERIOD: "",
        COL_FIN_CRONO: "",
        COL_FIN_INSTRUMENTO: card_name,
        COL_FIN_REG_TIPO: "Cargo tarjeta",
        COL_USER: _current_user(),
    }


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
        "- `Inventario en transito`: entrada de inventario cuya fecha de llegada / disponibilidad aun no ocurre.\n"
        "- `Linea de credito`: los desembolsos suben caja y pasivo; capital pagado baja deuda; el interes diario se sugiere automaticamente con la tasa vigente configurada y luego se registra como gasto financiero.\n"
        "- `Tarjeta de credito`: el consumo crea un gasto pendiente; el pago de tarjeta liquida ese saldo sin duplicar el gasto; intereses y cargos se registran aparte como gasto financiero.\n"
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
        "- Historial de cambios de tasa diaria dentro del mismo periodo de uso de la linea.\n"
        "- Tarjetas con multiples ciclos/cortes historicos y conciliacion contra estado de cuenta.\n"
        "- Factoring con recurso.\n"
        "- Proyeccion estimada del retenido cuando aun no existe liquidacion final.\n"
        "- Ajustes avanzados de valuacion para inversiones / participaciones."
    )

st.markdown("### Bandeja de documentos financieros")
st.caption(
    "Sube documentos o verifica carpetas Drive por empresa. La app prepara borradores listos para revisar; "
    "solo se cargan a Finanzas 1 cuando aceptas."
)
try:
    docs_before = load_finance_docs_df(client, SHEET_ID)
    docs_df = ensure_finance_docs_columns(docs_before.copy())
    docs_ready = True
except Exception as exc:
    docs_before = ensure_finance_docs_columns(pd.DataFrame(columns=DOC_BASE_COLUMNS))
    docs_df = docs_before.copy()
    docs_ready = False
    st.warning(f"La bandeja no pudo cargar su hoja aislada. Finanzas manual sigue operando. Detalle: {str(exc)[:220]}")

if docs_ready:
    api_key_fin_docs = _openai_finance_api_key()
    model_fin_docs = _openai_finance_model()
    for _empresa_docs in ["RIR", "RS-SP"]:
        with st.expander(f"Bandeja documentos {_empresa_docs}", expanded=False):
            _render_finance_docs_company_inbox(
                empresa=_empresa_docs,
                client_obj=client,
                sheet_id=SHEET_ID,
                creds_obj=creds,
                docs_before=docs_before,
                docs_df=docs_df,
                folder_id=_finance_docs_folder_id_for_empresa(_empresa_docs),
                api_key=api_key_fin_docs,
                model_name=model_fin_docs,
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


def _linea_credito_position(line_name: str) -> tuple[float, float, float]:
    ing_df = ensure_ingresos_columns(st.session_state.df_ing.copy())
    gas_df = ensure_gastos_columns(st.session_state.df_gas.copy())
    line_name_norm = str(line_name or "").strip()
    if not line_name_norm:
        return 0.0, 0.0, 0.0
    disb_mask = (
        ing_df[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(line_name_norm)
        & ing_df[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Desembolso")
    )
    pay_mask = (
        gas_df[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(line_name_norm)
        & gas_df[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Pago capital")
    )
    desembolsado = float(pd.to_numeric(ing_df.loc[disb_mask, COL_COBRO_REAL_MONTO], errors="coerce").fillna(0.0).sum())
    capital_pagado = float(pd.to_numeric(gas_df.loc[pay_mask, COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0).sum())
    saldo = max(0.0, desembolsado - capital_pagado)
    return desembolsado, capital_pagado, saldo


def _linea_credito_interest_preview(line_name: str, as_of_date, tasa_diaria_pct: float, tasa_desde=None) -> dict:
    ing_df = ensure_ingresos_columns(st.session_state.df_ing.copy())
    gas_df = ensure_gastos_columns(st.session_state.df_gas.copy())
    line_name_norm = str(line_name or "").strip()
    as_of_ts = _ts(as_of_date)
    if not line_name_norm or pd.isna(as_of_ts) or float(tasa_diaria_pct or 0.0) <= 0:
        return {"interest_start": pd.NaT, "days": 0, "opening_balance": 0.0, "interest_suggested": 0.0}

    disb = ing_df[
        ing_df[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(line_name_norm)
        & ing_df[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Desembolso")
    ][[COL_FECHA, COL_FCOBRO_REAL, COL_COBRO_REAL_MONTO, COL_MONTO]].copy()
    if not disb.empty:
        disb["fecha_evento"] = pd.to_datetime(disb[COL_FCOBRO_REAL], errors="coerce").fillna(pd.to_datetime(disb[COL_FECHA], errors="coerce"))
        disb["delta_capital"] = pd.to_numeric(disb[COL_COBRO_REAL_MONTO], errors="coerce").fillna(0.0)
        disb["delta_capital"] = disb["delta_capital"].where(disb["delta_capital"] > 0, pd.to_numeric(disb[COL_MONTO], errors="coerce").fillna(0.0))
        disb = disb[["fecha_evento", "delta_capital"]]

    capital_pay = gas_df[
        gas_df[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(line_name_norm)
        & gas_df[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Pago capital")
    ][[COL_FECHA, COL_FPAGO_REAL, COL_PAGO_REAL_MONTO, COL_MONTO]].copy()
    if not capital_pay.empty:
        capital_pay["fecha_evento"] = pd.to_datetime(capital_pay[COL_FPAGO_REAL], errors="coerce").fillna(pd.to_datetime(capital_pay[COL_FECHA], errors="coerce"))
        capital_pay["delta_capital"] = -pd.to_numeric(capital_pay[COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0)
        fallback_cap = -pd.to_numeric(capital_pay[COL_MONTO], errors="coerce").fillna(0.0).abs()
        capital_pay["delta_capital"] = capital_pay["delta_capital"].where(capital_pay["delta_capital"] < 0, fallback_cap)
        capital_pay = capital_pay[["fecha_evento", "delta_capital"]]

    interest_pay = gas_df[
        gas_df[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(line_name_norm)
        & gas_df[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Pago interes")
    ][[COL_FECHA, COL_FPAGO_REAL]].copy()
    if not interest_pay.empty:
        interest_pay["fecha_evento"] = pd.to_datetime(interest_pay[COL_FPAGO_REAL], errors="coerce").fillna(pd.to_datetime(interest_pay[COL_FECHA], errors="coerce"))

    principal_events = pd.concat([df for df in [disb, capital_pay] if not df.empty], ignore_index=True) if (not disb.empty or not capital_pay.empty) else pd.DataFrame(columns=["fecha_evento", "delta_capital"])
    if principal_events.empty:
        return {"interest_start": pd.NaT, "days": 0, "opening_balance": 0.0, "interest_suggested": 0.0}

    principal_events["fecha_evento"] = pd.to_datetime(principal_events["fecha_evento"], errors="coerce")
    principal_events = principal_events[principal_events["fecha_evento"].notna()].sort_values("fecha_evento").reset_index(drop=True)
    if principal_events.empty:
        return {"interest_start": pd.NaT, "days": 0, "opening_balance": 0.0, "interest_suggested": 0.0}

    last_interest_payment = pd.NaT
    if not interest_pay.empty:
        interest_pay = interest_pay[interest_pay["fecha_evento"].notna()]
        interest_pay = interest_pay[interest_pay["fecha_evento"] < as_of_ts]
        if not interest_pay.empty:
            last_interest_payment = interest_pay["fecha_evento"].max()

    interest_start = last_interest_payment if not pd.isna(last_interest_payment) else principal_events["fecha_evento"].min()
    tasa_desde_ts = _ts(tasa_desde)
    if not pd.isna(tasa_desde_ts):
        interest_start = max(interest_start, tasa_desde_ts)
    opening_balance = float(principal_events.loc[principal_events["fecha_evento"] <= interest_start, "delta_capital"].sum())
    opening_balance = max(0.0, opening_balance)
    current_balance = opening_balance
    current_date = interest_start
    interest_amount = 0.0
    daily_rate = float(tasa_diaria_pct or 0.0) / 100.0

    future_events = principal_events[(principal_events["fecha_evento"] > interest_start) & (principal_events["fecha_evento"] <= as_of_ts)].copy()
    for _, evt in future_events.iterrows():
        evt_date = pd.to_datetime(evt["fecha_evento"], errors="coerce")
        if pd.isna(evt_date):
            continue
        days = max(0, int((evt_date.normalize() - current_date.normalize()).days))
        if current_balance > 0 and days > 0:
            interest_amount += current_balance * daily_rate * days
        current_balance = max(0.0, current_balance + float(evt["delta_capital"] or 0.0))
        current_date = evt_date

    tail_days = max(0, int((as_of_ts.normalize() - current_date.normalize()).days))
    if current_balance > 0 and tail_days > 0:
        interest_amount += current_balance * daily_rate * tail_days

    total_days = max(0, int((as_of_ts.normalize() - interest_start.normalize()).days))
    return {
        "interest_start": interest_start,
        "days": total_days,
        "opening_balance": float(opening_balance),
        "interest_suggested": round(float(interest_amount), 2),
    }


def _tarjeta_position(card_name: str) -> tuple[float, float, float]:
    gas_df = ensure_gastos_columns(st.session_state.df_gas.copy())
    card_name_norm = str(card_name or "").strip()
    if not card_name_norm:
        return 0.0, 0.0, 0.0
    consumo_mask = (
        gas_df[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(card_name_norm)
        & gas_df[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Consumo tarjeta")
    )
    consumos = float(pd.to_numeric(gas_df.loc[consumo_mask, COL_MONTO], errors="coerce").fillna(0.0).sum())
    pagado = float(pd.to_numeric(gas_df.loc[consumo_mask, COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0).sum())
    saldo = max(0.0, consumos - pagado)
    return consumos, pagado, saldo


def _apply_card_payment(updated_df: pd.DataFrame, card_name: str, fecha_pago, monto_pago: float, nota: str) -> pd.DataFrame:
    if float(monto_pago or 0.0) <= 0:
        return ensure_gastos_columns(updated_df)
    work = ensure_gastos_columns(updated_df.copy())
    mask = (
        work[COL_FIN_INSTRUMENTO].astype(str).str.strip().eq(str(card_name or "").strip())
        & work[COL_FIN_REG_TIPO].astype(str).str.strip().eq("Consumo tarjeta")
    )
    pending = work.loc[mask].copy()
    if pending.empty:
        return work
    pending["__saldo"] = (
        pd.to_numeric(pending[COL_MONTO], errors="coerce").fillna(0.0)
        - pd.to_numeric(pending[COL_PAGO_REAL_MONTO], errors="coerce").fillna(0.0)
    ).clip(lower=0.0)
    pending = pending[pending["__saldo"] > 0].copy()
    if pending.empty:
        return work
    pending["__fecha_pago_orden"] = pd.to_datetime(pending[COL_FPAGO], errors="coerce").fillna(pd.to_datetime(pending[COL_FECHA], errors="coerce"))
    pending = pending.sort_values(["__fecha_pago_orden", COL_FECHA], na_position="last")
    restante = float(monto_pago)
    fecha_pago_ts = _ts(fecha_pago)
    nota_base = str(nota or "").strip()
    for idx, row in pending.iterrows():
        if restante <= 0:
            break
        saldo_row = float(row["__saldo"])
        abono = min(restante, saldo_row)
        if abono <= 0:
            continue
        events = _seed_partial_events_from_row(row, COL_PAGO_REAL_MONTO, COL_FPAGO_REAL)
        events.append(
            {
                "fecha": fecha_pago_ts,
                "monto": float(abono),
                "nota": nota_base or "Pago tarjeta",
            }
        )
        total_real, last_real = _partial_events_summary(events)
        total_monto = float(pd.to_numeric(pd.Series([row.get(COL_MONTO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        total_real = min(total_real, total_monto)
        work.loc[idx, COL_GAS_PARTIALS] = _serialize_partial_events(events)
        work.loc[idx, COL_PAGO_REAL_MONTO] = total_real
        work.loc[idx, COL_FPAGO_REAL] = last_real
        work.loc[idx, COL_POR_PAG] = "No" if total_real >= total_monto - 0.01 else "Sí"
        restante -= abono
    return ensure_gastos_columns(work)


st.markdown("## Linea de credito")
with st.expander("Gestionar linea de credito", expanded=False):
    st.caption(
        "Esta seccion gestiona lineas revolventes sin forzar un cronograma mensual falso. "
        "La tasa diaria, limite y cargos se guardan como referencia; el interes diario se sugiere automaticamente con la tasa vigente y se registra al momento del pago."
    )
    lineas_before = load_credit_lines_df(client, SHEET_ID)
    lineas_df = ensure_lineas_credito_columns(lineas_before.copy())
    lineas_activas = lineas_df[lineas_df[LC_COL_ACTIVA].map(_si_no_norm).eq("Sí")].copy()
    tab_lc2, tab_lc3, tab_lc4, tab_lc1 = st.tabs(
        [
            "Registrar desembolso",
            "Registrar pago",
            "Registrar cargo asociado",
            "Configurar / actualizar",
        ]
    )

    with tab_lc1:
        line_options = ["Nueva linea"] + [
            f"{str(row.get(LC_COL_EMPRESA, '')).strip()} | {str(row.get(LC_COL_NOMBRE, '')).strip()} | {str(row.get(LC_COL_BANCO, '')).strip()}"
            for _, row in lineas_df.iterrows()
        ]
        selected_line_label = st.selectbox("Linea a editar", line_options, key="lc_config_sel")
        selected_line = None
        if selected_line_label != "Nueva linea" and not lineas_df.empty:
            selected_idx = line_options.index(selected_line_label) - 1
            if 0 <= selected_idx < len(lineas_df):
                selected_line = lineas_df.iloc[selected_idx]
        suffix = str(selected_line.get(LC_COL_ROWID, "new")) if selected_line is not None else "new"
        lc_emp_default = str(selected_line.get(LC_COL_EMPRESA, EMPRESA_DEFAULT)).strip() if selected_line is not None else EMPRESA_DEFAULT
        lc_emp_index = EMPRESAS_OPCIONES.index(lc_emp_default) if lc_emp_default in EMPRESAS_OPCIONES else EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT)
        c1, c2, c3 = st.columns(3)
        with c1:
            lc_empresa = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=lc_emp_index, key=f"lc_cfg_emp_{suffix}")
            lc_nombre = st.text_input("Nombre linea", value=str(selected_line.get(LC_COL_NOMBRE, "")) if selected_line is not None else "", key=f"lc_cfg_nombre_{suffix}")
            lc_banco = st.text_input("Banco", value=str(selected_line.get(LC_COL_BANCO, "")) if selected_line is not None else "", key=f"lc_cfg_banco_{suffix}")
        with c2:
            lc_limite = st.number_input("Limite vigente", min_value=0.0, step=100.0, value=float(selected_line.get(LC_COL_LIMITE, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_limite_{suffix}")
            lc_tasa_diaria = st.number_input("Tasa diaria pct", min_value=0.0, step=0.0001, format="%.6f", value=float(selected_line.get(LC_COL_TASA_DIARIA, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_tasa_{suffix}")
            lc_tasa_desde = st.date_input("Fecha vigencia tasa", value=_as_date_or_default(selected_line.get(LC_COL_TASA_DESDE), _today()) if selected_line is not None else _today(), key=f"lc_cfg_tasa_desde_{suffix}")
            lc_cargo_anual = st.number_input("Cargo anual pct sobre limite", min_value=0.0, step=0.1, value=float(selected_line.get(LC_COL_CARGO_ANUAL_PCT, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_anual_{suffix}")
            lc_cargo_desembolso = st.number_input("Cargo desembolso fijo", min_value=0.0, step=1.0, value=float(selected_line.get(LC_COL_CARGO_DESEMBOLSO, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_desembolso_{suffix}")
        with c3:
            lc_banca = st.number_input("Cargo banca en linea mensual", min_value=0.0, step=0.01, value=float(selected_line.get(LC_COL_CARGO_BANCA_MENSUAL, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_banca_{suffix}")
            lc_seg1 = st.number_input("Seguro incendio 1 anual", min_value=0.0, step=0.01, value=float(selected_line.get(LC_COL_SEGURO_INCENDIO_1, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_seg1_{suffix}")
            lc_seg2 = st.number_input("Seguro incendio 2 anual", min_value=0.0, step=0.01, value=float(selected_line.get(LC_COL_SEGURO_INCENDIO_2, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_seg2_{suffix}")
            lc_poliza = st.number_input("Poliza vida mensual", min_value=0.0, step=0.01, value=float(selected_line.get(LC_COL_POLIZA_VIDA_MENSUAL, 0.0)) if selected_line is not None else 0.0, key=f"lc_cfg_poliza_{suffix}")
            lc_activa = st.selectbox("Activa", YES_NO_OPTIONS, index=YES_NO_OPTIONS.index(_si_no_norm(selected_line.get(LC_COL_ACTIVA, "Sí"))) if selected_line is not None and _si_no_norm(selected_line.get(LC_COL_ACTIVA, "Sí")) in YES_NO_OPTIONS else 1, key=f"lc_cfg_activa_{suffix}")
        lc_notas = st.text_area("Notas", value=str(selected_line.get(LC_COL_NOTAS, "")) if selected_line is not None else "", key=f"lc_cfg_notas_{suffix}")
        if st.button("Guardar configuracion linea", key=f"btn_guardar_lc_cfg_{suffix}"):
            if not str(lc_nombre or "").strip():
                st.error("Debes indicar el nombre de la linea.")
                st.stop()
            if not str(lc_banco or "").strip():
                st.error("Debes indicar el banco.")
                st.stop()
            line_id = str(selected_line.get(LC_COL_ROWID, "")).strip() if selected_line is not None else uuid.uuid4().hex
            new_row = {
                LC_COL_ROWID: line_id,
                LC_COL_EMPRESA: lc_empresa,
                LC_COL_NOMBRE: str(lc_nombre or "").strip(),
                LC_COL_BANCO: str(lc_banco or "").strip(),
                LC_COL_LIMITE: float(lc_limite),
                LC_COL_TASA_DIARIA: float(lc_tasa_diaria),
                LC_COL_TASA_DESDE: _ts(lc_tasa_desde),
                LC_COL_CARGO_ANUAL_PCT: float(lc_cargo_anual),
                LC_COL_CARGO_DESEMBOLSO: float(lc_cargo_desembolso),
                LC_COL_CARGO_BANCA_MENSUAL: float(lc_banca),
                LC_COL_SEGURO_INCENDIO_1: float(lc_seg1),
                LC_COL_SEGURO_INCENDIO_2: float(lc_seg2),
                LC_COL_POLIZA_VIDA_MENSUAL: float(lc_poliza),
                LC_COL_ACTIVA: lc_activa,
                LC_COL_NOTAS: str(lc_notas or "").strip(),
                LC_COL_UPDATED_AT: _ts(_today()),
                COL_USER: _current_user(),
            }
            new_lineas_df = lineas_df.copy()
            if selected_line is not None and line_id in new_lineas_df[LC_COL_ROWID].astype(str).tolist():
                for col, val in new_row.items():
                    new_lineas_df.loc[new_lineas_df[LC_COL_ROWID].astype(str) == line_id, col] = val
            else:
                new_lineas_df = pd.concat([new_lineas_df, pd.DataFrame([new_row])], ignore_index=True)
            wrote = safe_write_credit_lines(client, SHEET_ID, new_lineas_df, old_df=lineas_before)
            if wrote:
                st.cache_data.clear()
                st.success("Configuracion de linea guardada.")
                _safe_rerun()
            else:
                st.info("No hubo cambios para guardar en la linea seleccionada.")

    with tab_lc2:
        if lineas_activas.empty:
            st.info("Primero configura y activa al menos una linea de credito.")
        else:
            disb_labels = {
                f"{str(row.get(LC_COL_EMPRESA, '')).strip()} | {str(row.get(LC_COL_NOMBRE, '')).strip()} | {str(row.get(LC_COL_BANCO, '')).strip()}": str(row.get(LC_COL_ROWID, "")).strip()
                for _, row in lineas_activas.iterrows()
            }
            disb_sel = st.selectbox("Linea activa", list(disb_labels.keys()), key="lc_disb_sel")
            disb_row = lineas_activas[lineas_activas[LC_COL_ROWID].astype(str) == disb_labels.get(disb_sel, "")].iloc[0]
            disb_name = str(disb_row.get(LC_COL_NOMBRE, "") or "").strip()
            total_desembolsado, capital_pagado, saldo_actual = _linea_credito_position(disb_name)
            limite_linea = float(pd.to_numeric(pd.Series([disb_row.get(LC_COL_LIMITE, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            disponible_linea = max(0.0, limite_linea - saldo_actual) if limite_linea > 0 else 0.0
            d1, d2, d3 = st.columns(3)
            d1.metric("Desembolsado acumulado", _format_money_es(total_desembolsado))
            d2.metric("Capital pagado", _format_money_es(capital_pagado))
            d3.metric("Saldo estimado linea", _format_money_es(saldo_actual))
            st.caption(
                f"Limite vigente: {_format_money_es(limite_linea)} | Disponible estimado: {_format_money_es(disponible_linea) if limite_linea > 0 else 'Sin limite configurado'}"
            )
            fecha_desembolso = st.date_input("Fecha desembolso", value=_today(), key="lc_disb_fecha")
            monto_desembolso = st.number_input("Monto desembolso", min_value=0.0, step=100.0, key="lc_disb_monto")
            auto_cargo_desembolso = st.selectbox("Generar cargo fijo por desembolso", YES_NO_OPTIONS, index=1 if float(disb_row.get(LC_COL_CARGO_DESEMBOLSO, 0.0) or 0.0) > 0 else 0, key="lc_disb_auto_fee")
            nota_desembolso = st.text_input("Nota desembolso", key="lc_disb_nota")
            if st.button("Registrar desembolso linea", key="btn_registrar_lc_desembolso"):
                if float(monto_desembolso) <= 0:
                    st.error("Debes indicar un monto de desembolso mayor que cero.")
                    st.stop()
                if limite_linea > 0 and float(monto_desembolso) > disponible_linea + 0.01:
                    st.error("El desembolso excede el limite disponible estimado de la linea.")
                    st.stop()
                old_ing_df = st.session_state.df_ing.copy()
                old_gas_df = st.session_state.df_gas.copy()
                new_ing_df = pd.concat(
                    [old_ing_df, pd.DataFrame([_build_credit_line_ingreso_row(line_row=disb_row, fecha_evento=fecha_desembolso, monto=float(monto_desembolso), nota=nota_desembolso)])],
                    ignore_index=True,
                )
                new_ing_df = ensure_ingresos_columns(new_ing_df)
                new_gas_df = old_gas_df.copy()
                cargo_desembolso = float(pd.to_numeric(pd.Series([disb_row.get(LC_COL_CARGO_DESEMBOLSO, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
                if _bool_from_toggle(auto_cargo_desembolso) and cargo_desembolso > 0:
                    desc_cargo = f"Cargo desembolso linea de credito - {disb_name}"
                    if str(nota_desembolso or "").strip():
                        desc_cargo = f"{desc_cargo} | {str(nota_desembolso or '').strip()}"
                    new_gas_df = pd.concat(
                        [
                            new_gas_df,
                            pd.DataFrame(
                                [
                                    _build_credit_line_gasto_row(
                                        line_row=disb_row,
                                        fecha_evento=fecha_desembolso,
                                        monto=cargo_desembolso,
                                        descripcion=desc_cargo,
                                        tratamiento="Gasto del periodo",
                                        detalle_gasto="Otros",
                                        registro_financiamiento="Cargo",
                                    )
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                    new_gas_df = ensure_gastos_columns(new_gas_df)
                wrote_ing = safe_write_worksheet(client, SHEET_ID, WS_ING, new_ing_df, old_df=old_ing_df)
                if not wrote_ing:
                    st.error("No se pudo guardar el desembolso de la linea.")
                    st.stop()
                if len(new_gas_df) != len(old_gas_df):
                    wrote_gas = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                    if not wrote_gas:
                        rollback_ok = safe_write_worksheet(client, SHEET_ID, WS_ING, old_ing_df, old_df=new_ing_df)
                        if rollback_ok:
                            st.error("No se pudo guardar el cargo de desembolso. Se revirtio el desembolso para evitar inconsistencias.")
                        else:
                            st.error("No se pudo guardar el cargo de desembolso y tampoco revertir automaticamente el ingreso. Revisa ambas hojas.")
                        st.stop()
                st.session_state.df_ing = new_ing_df
                st.session_state.df_gas = new_gas_df
                st.cache_data.clear()
                st.success("Desembolso de linea registrado.")
                _safe_rerun()

    with tab_lc3:
        if lineas_activas.empty:
            st.info("Primero configura y activa al menos una linea de credito.")
        else:
            pay_labels = {
                f"{str(row.get(LC_COL_EMPRESA, '')).strip()} | {str(row.get(LC_COL_NOMBRE, '')).strip()} | {str(row.get(LC_COL_BANCO, '')).strip()}": str(row.get(LC_COL_ROWID, "")).strip()
                for _, row in lineas_activas.iterrows()
            }
            pay_sel = st.selectbox("Linea activa", list(pay_labels.keys()), key="lc_pay_sel")
            pay_row = lineas_activas[lineas_activas[LC_COL_ROWID].astype(str) == pay_labels.get(pay_sel, "")].iloc[0]
            pay_name = str(pay_row.get(LC_COL_NOMBRE, "") or "").strip()
            total_desembolsado, capital_pagado, saldo_actual = _linea_credito_position(pay_name)
            tasa_diaria_linea = float(pd.to_numeric(pd.Series([pay_row.get(LC_COL_TASA_DIARIA, 0.0)]), errors="coerce").fillna(0.0).iloc[0])
            tasa_desde_linea = _ts(pay_row.get(LC_COL_TASA_DESDE))
            p1, p2, p3 = st.columns(3)
            p1.metric("Desembolsado acumulado", _format_money_es(total_desembolsado))
            p2.metric("Capital pagado", _format_money_es(capital_pagado))
            p3.metric("Saldo estimado linea", _format_money_es(saldo_actual))
            fecha_pago_linea = st.date_input("Fecha pago linea", value=_today(), key="lc_pay_fecha")
            interest_preview = _linea_credito_interest_preview(
                pay_name,
                fecha_pago_linea,
                tasa_diaria_linea,
                tasa_desde=tasa_desde_linea,
            )
            interest_ctx = f"{str(pay_row.get(LC_COL_ROWID, ''))}|{fecha_pago_linea.isoformat()}|{tasa_diaria_linea:.6f}|{tasa_desde_linea.date().isoformat() if not pd.isna(tasa_desde_linea) else ''}"
            if st.session_state.get("lc_pay_interest_ctx") != interest_ctx:
                st.session_state["lc_pay_interest_ctx"] = interest_ctx
                st.session_state["lc_pay_interes"] = float(interest_preview.get("interest_suggested", 0.0))
            st.caption(
                f"Tasa diaria vigente: {_format_number_es(tasa_diaria_linea, 6)}% | "
                f"Base de calculo desde: {interest_preview['interest_start'].date().isoformat() if not pd.isna(interest_preview.get('interest_start')) else 'sin base'} | "
                f"Dias estimados: {int(interest_preview.get('days', 0) or 0)} | "
                f"Interes sugerido: {_format_money_es(float(interest_preview.get('interest_suggested', 0.0) or 0.0))}"
            )
            q1, q2, q3 = st.columns(3)
            with q1:
                capital_linea = st.number_input("Capital pagado", min_value=0.0, step=100.0, key="lc_pay_capital")
            with q2:
                interes_linea = st.number_input("Interes pagado", min_value=0.0, step=1.0, key="lc_pay_interes")
            with q3:
                otros_linea = st.number_input("Otros cargos pagados", min_value=0.0, step=1.0, key="lc_pay_otros")
            nota_pago_linea = st.text_input("Nota pago linea", key="lc_pay_nota")
            if st.button("Registrar pago linea", key="btn_registrar_lc_pago"):
                if float(capital_linea) <= 0 and float(interes_linea) <= 0 and float(otros_linea) <= 0:
                    st.error("Debes registrar al menos capital, interes u otros cargos.")
                    st.stop()
                if float(capital_linea) > saldo_actual + 0.01:
                    st.error("El capital pagado excede el saldo estimado actual de la linea.")
                    st.stop()
                old_gas_df = st.session_state.df_gas.copy()
                rows_new = []
                nota_base = str(nota_pago_linea or "").strip()
                if float(capital_linea) > 0:
                    desc = f"Pago capital linea de credito - {pay_name}"
                    if nota_base:
                        desc = f"{desc} | {nota_base}"
                    rows_new.append(
                        _build_credit_line_gasto_row(
                            line_row=pay_row,
                            fecha_evento=fecha_pago_linea,
                            monto=float(capital_linea),
                            descripcion=desc,
                            tratamiento="Cancelacion de pasivo / deuda",
                            detalle_gasto="Otros",
                            registro_financiamiento="Pago capital",
                        )
                    )
                if float(interes_linea) > 0:
                    desc = f"Interes linea de credito - {pay_name}"
                    if nota_base:
                        desc = f"{desc} | {nota_base}"
                    rows_new.append(
                        _build_credit_line_gasto_row(
                            line_row=pay_row,
                            fecha_evento=fecha_pago_linea,
                            monto=float(interes_linea),
                            descripcion=desc,
                            tratamiento="Gasto del periodo",
                            detalle_gasto="Intereses",
                            registro_financiamiento="Pago interes",
                        )
                    )
                if float(otros_linea) > 0:
                    desc = f"Cargos linea de credito - {pay_name}"
                    if nota_base:
                        desc = f"{desc} | {nota_base}"
                    rows_new.append(
                        _build_credit_line_gasto_row(
                            line_row=pay_row,
                            fecha_evento=fecha_pago_linea,
                            monto=float(otros_linea),
                            descripcion=desc,
                            tratamiento="Gasto del periodo",
                            detalle_gasto="Otros",
                            registro_financiamiento="Cargo",
                        )
                    )
                new_gas_df = pd.concat([old_gas_df, pd.DataFrame(rows_new)], ignore_index=True)
                new_gas_df = ensure_gastos_columns(new_gas_df)
                wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                if not wrote:
                    st.error("No se pudo guardar el pago de la linea.")
                    st.stop()
                st.session_state.df_gas = new_gas_df
                st.cache_data.clear()
                st.success("Pago de linea registrado.")
                _safe_rerun()

    with tab_lc4:
        if lineas_activas.empty:
            st.info("Primero configura y activa al menos una linea de credito.")
        else:
            cargo_labels = {
                f"{str(row.get(LC_COL_EMPRESA, '')).strip()} | {str(row.get(LC_COL_NOMBRE, '')).strip()} | {str(row.get(LC_COL_BANCO, '')).strip()}": str(row.get(LC_COL_ROWID, "")).strip()
                for _, row in lineas_activas.iterrows()
            }
            cargo_sel = st.selectbox("Linea activa", list(cargo_labels.keys()), key="lc_charge_sel")
            cargo_row = lineas_activas[lineas_activas[LC_COL_ROWID].astype(str) == cargo_labels.get(cargo_sel, "")].iloc[0]
            cargo_tipo = st.selectbox("Tipo de cargo", LINE_CHARGE_OPTIONS, key="lc_charge_type")
            cargo_fecha = st.date_input("Fecha cargo", value=_today(), key="lc_charge_fecha")
            default_charge_amount = 0.0
            charge_treatment = "Gasto del periodo"
            charge_prepago_meses = 0
            if cargo_tipo == "Instalacion anual de linea":
                default_charge_amount = float(cargo_row.get(LC_COL_LIMITE, 0.0) or 0.0) * float(cargo_row.get(LC_COL_CARGO_ANUAL_PCT, 0.0) or 0.0) / 100.0
                charge_treatment = "Anticipo / prepago"
                charge_prepago_meses = 12
            elif cargo_tipo == "Banca en linea mensual":
                default_charge_amount = float(cargo_row.get(LC_COL_CARGO_BANCA_MENSUAL, 0.0) or 0.0)
            elif cargo_tipo == "Seguro incendio 1 anual":
                default_charge_amount = float(cargo_row.get(LC_COL_SEGURO_INCENDIO_1, 0.0) or 0.0)
                charge_treatment = "Anticipo / prepago"
                charge_prepago_meses = 12
            elif cargo_tipo == "Seguro incendio 2 anual":
                default_charge_amount = float(cargo_row.get(LC_COL_SEGURO_INCENDIO_2, 0.0) or 0.0)
                charge_treatment = "Anticipo / prepago"
                charge_prepago_meses = 12
            elif cargo_tipo == "Poliza vida mensual":
                default_charge_amount = float(cargo_row.get(LC_COL_POLIZA_VIDA_MENSUAL, 0.0) or 0.0)
            cargo_monto = st.number_input("Monto cargo", min_value=0.0, step=0.01, value=float(default_charge_amount), key="lc_charge_amount")
            cargo_nota = st.text_input("Nota cargo", key="lc_charge_note")
            st.caption(
                "Instalacion anual de linea y seguros anuales se registran como prepago a 12 meses. "
                "Los cargos mensuales se registran como gasto del periodo."
            )
            if st.button("Registrar cargo asociado", key="btn_registrar_lc_charge"):
                if float(cargo_monto) <= 0:
                    st.error("Debes indicar un monto mayor que cero para el cargo.")
                    st.stop()
                old_gas_df = st.session_state.df_gas.copy()
                charge_desc = f"{cargo_tipo} - {str(cargo_row.get(LC_COL_NOMBRE, '')).strip()}"
                if str(cargo_nota or "").strip():
                    charge_desc = f"{charge_desc} | {str(cargo_nota or '').strip()}"
                charge_row = _build_credit_line_gasto_row(
                    line_row=cargo_row,
                    fecha_evento=cargo_fecha,
                    monto=float(cargo_monto),
                    descripcion=charge_desc,
                    tratamiento=charge_treatment,
                    detalle_gasto="Otros",
                    registro_financiamiento="Cargo",
                    prepago_meses=charge_prepago_meses,
                    prepago_inicio=cargo_fecha,
                )
                new_gas_df = pd.concat([old_gas_df, pd.DataFrame([charge_row])], ignore_index=True)
                new_gas_df = ensure_gastos_columns(new_gas_df)
                wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                if not wrote:
                    st.error("No se pudo guardar el cargo asociado de la linea.")
                    st.stop()
                st.session_state.df_gas = new_gas_df
                st.cache_data.clear()
            st.success("Cargo asociado registrado.")
            _safe_rerun()


st.markdown("## Tarjeta de credito")
with st.expander("Gestionar tarjeta de credito", expanded=False):
    st.caption(
        "Los consumos con tarjeta crean gastos pendientes y el pago de tarjeta liquida esos consumos sin duplicar el gasto. "
        "Los intereses y cargos se registran aparte como gasto financiero."
    )
    cards_before = load_cards_df(client, SHEET_ID)
    cards_df = ensure_tarjetas_columns(cards_before.copy())
    cards_activas = cards_df[cards_df[TC_COL_ACTIVA].map(_si_no_norm).eq("Sí")].copy()
    tab_tc2, tab_tc3, tab_tc1 = st.tabs(
        [
            "Registrar consumo",
            "Registrar pago / cargo",
            "Configurar / actualizar",
        ]
    )

    with tab_tc1:
        card_options = ["Nueva tarjeta"] + [
            f"{str(row.get(TC_COL_EMPRESA, '')).strip()} | {str(row.get(TC_COL_NOMBRE, '')).strip()} | {str(row.get(TC_COL_BANCO, '')).strip()}"
            for _, row in cards_df.iterrows()
        ]
        selected_card_label = st.selectbox("Tarjeta a editar", card_options, key="tc_config_sel")
        selected_card = None
        if selected_card_label != "Nueva tarjeta" and not cards_df.empty:
            selected_idx = card_options.index(selected_card_label) - 1
            if 0 <= selected_idx < len(cards_df):
                selected_card = cards_df.iloc[selected_idx]
        suffix = str(selected_card.get(TC_COL_ROWID, "new")) if selected_card is not None else "new"
        tc_emp_default = str(selected_card.get(TC_COL_EMPRESA, EMPRESA_DEFAULT)).strip() if selected_card is not None else EMPRESA_DEFAULT
        tc_emp_index = EMPRESAS_OPCIONES.index(tc_emp_default) if tc_emp_default in EMPRESAS_OPCIONES else EMPRESAS_OPCIONES.index(EMPRESA_DEFAULT)
        c1, c2, c3 = st.columns(3)
        with c1:
            tc_empresa = st.selectbox("Empresa", EMPRESAS_OPCIONES, index=tc_emp_index, key=f"tc_cfg_emp_{suffix}")
            tc_nombre = st.text_input("Nombre tarjeta", value=str(selected_card.get(TC_COL_NOMBRE, "")) if selected_card is not None else "", key=f"tc_cfg_nombre_{suffix}")
            tc_banco = st.text_input("Banco", value=str(selected_card.get(TC_COL_BANCO, "")) if selected_card is not None else "", key=f"tc_cfg_banco_{suffix}")
        with c2:
            tc_limite = st.number_input("Limite vigente", min_value=0.0, step=100.0, value=float(selected_card.get(TC_COL_LIMITE, 0.0)) if selected_card is not None else 0.0, key=f"tc_cfg_limite_{suffix}")
            tc_dia_corte = st.number_input("Dia de corte", min_value=1, max_value=31, step=1, value=int(selected_card.get(TC_COL_DIA_CORTE, 10)) if selected_card is not None else 10, key=f"tc_cfg_corte_{suffix}")
            tc_dia_venc = st.number_input("Dia de vencimiento", min_value=1, max_value=31, step=1, value=int(selected_card.get(TC_COL_DIA_VENC, 5)) if selected_card is not None else 5, key=f"tc_cfg_venc_{suffix}")
        with c3:
            tc_activa = st.selectbox("Activa", YES_NO_OPTIONS, index=YES_NO_OPTIONS.index(_si_no_norm(selected_card.get(TC_COL_ACTIVA, "Sí"))) if selected_card is not None and _si_no_norm(selected_card.get(TC_COL_ACTIVA, "Sí")) in YES_NO_OPTIONS else 1, key=f"tc_cfg_activa_{suffix}")
            tc_notas = st.text_area("Notas", value=str(selected_card.get(TC_COL_NOTAS, "")) if selected_card is not None else "", key=f"tc_cfg_notas_{suffix}")
        if st.button("Guardar configuracion tarjeta", key=f"btn_guardar_tc_cfg_{suffix}"):
            if not str(tc_nombre or "").strip():
                st.error("Debes indicar el nombre de la tarjeta.")
                st.stop()
            if not str(tc_banco or "").strip():
                st.error("Debes indicar el banco.")
                st.stop()
            card_id = str(selected_card.get(TC_COL_ROWID, "")).strip() if selected_card is not None else uuid.uuid4().hex
            new_row = {
                TC_COL_ROWID: card_id,
                TC_COL_EMPRESA: tc_empresa,
                TC_COL_NOMBRE: str(tc_nombre or "").strip(),
                TC_COL_BANCO: str(tc_banco or "").strip(),
                TC_COL_LIMITE: float(tc_limite),
                TC_COL_DIA_CORTE: int(tc_dia_corte),
                TC_COL_DIA_VENC: int(tc_dia_venc),
                TC_COL_ACTIVA: tc_activa,
                TC_COL_NOTAS: str(tc_notas or "").strip(),
                TC_COL_UPDATED_AT: _ts(_today()),
                COL_USER: _current_user(),
            }
            new_cards_df = cards_df.copy()
            if selected_card is not None and card_id in new_cards_df[TC_COL_ROWID].astype(str).tolist():
                for col, val in new_row.items():
                    new_cards_df.loc[new_cards_df[TC_COL_ROWID].astype(str) == card_id, col] = val
            else:
                new_cards_df = pd.concat([new_cards_df, pd.DataFrame([new_row])], ignore_index=True)
            wrote = safe_write_cards(client, SHEET_ID, new_cards_df, old_df=cards_before)
            if wrote:
                st.cache_data.clear()
                st.success("Configuracion de tarjeta guardada.")
                _safe_rerun()
            else:
                st.info("No hubo cambios para guardar en la tarjeta seleccionada.")

    with tab_tc2:
        if cards_activas.empty:
            st.info("Primero configura y activa al menos una tarjeta.")
        else:
            card_labels = {
                f"{str(row.get(TC_COL_EMPRESA, '')).strip()} | {str(row.get(TC_COL_NOMBRE, '')).strip()} | {str(row.get(TC_COL_BANCO, '')).strip()}": str(row.get(TC_COL_ROWID, "")).strip()
                for _, row in cards_activas.iterrows()
            }
            tc_sel = st.selectbox("Tarjeta activa", list(card_labels.keys()), key="tc_cons_sel")
            tc_row = cards_activas[cards_activas[TC_COL_ROWID].astype(str) == card_labels.get(tc_sel, "")].iloc[0]
            tc_name = str(tc_row.get(TC_COL_NOMBRE, "") or "").strip()
            total_consumos_tc, pagado_tc, saldo_tc = _tarjeta_position(tc_name)
            t1, t2, t3 = st.columns(3)
            t1.metric("Consumos acumulados", _format_money_es(total_consumos_tc))
            t2.metric("Pagado a consumos", _format_money_es(pagado_tc))
            t3.metric("Saldo tarjeta", _format_money_es(saldo_tc))

            fecha_consumo_tc = st.date_input("Fecha del hecho economico", value=_today(), key="tc_cons_fecha")
            fecha_pago_sugerida = _estimate_card_due_date(
                fecha_consumo_tc,
                int(tc_row.get(TC_COL_DIA_CORTE, 10) or 10),
                int(tc_row.get(TC_COL_DIA_VENC, 5) or 5),
            )
            c1, c2, c3 = st.columns(3)
            with c1:
                monto_consumo_tc = st.number_input("Monto consumo", min_value=0.0, step=1.0, key="tc_cons_monto")
                categoria_consumo_tc = st.selectbox("Categoria operativa", GAS_CATEGORY_OPTIONS, index=GAS_CATEGORY_OPTIONS.index("Gastos operativos") if "Gastos operativos" in GAS_CATEGORY_OPTIONS else 0, key="tc_cons_cat", help=_help_for_option(GAS_CATEGORY_HELP, st.session_state.get("tc_cons_cat", GAS_CATEGORY_OPTIONS[0])))
            with c2:
                proveedor_consumo_tc = st.text_input("Proveedor / comercio", key="tc_cons_proveedor")
                detalle_consumo_tc = st.selectbox("Detalle gasto", GAS_DETAIL_OPTIONS, index=GAS_DETAIL_OPTIONS.index("Otros") if "Otros" in GAS_DETAIL_OPTIONS else 0, key="tc_cons_det")
            with c3:
                tratamiento_consumo_tc = st.selectbox("Tratamiento balance gasto", GAS_BALANCE_OPTIONS, index=GAS_BALANCE_OPTIONS.index("Gasto del periodo") if "Gasto del periodo" in GAS_BALANCE_OPTIONS else 0, key="tc_cons_trat", help=_help_for_option(BALANCE_GAS_HELP, st.session_state.get("tc_cons_trat", GAS_BALANCE_OPTIONS[0])))
                fecha_pago_tc = st.date_input("Fecha esperada de pago tarjeta", value=_as_date_or_default(fecha_pago_sugerida, _today()), key="tc_cons_fpago")
            desc_consumo_tc = st.text_area("Descripcion", key="tc_cons_desc", placeholder="Ej: compra de materiales con tarjeta / gasolina flota.")
            st.caption(f"Corte dia {int(tc_row.get(TC_COL_DIA_CORTE, 10) or 10)} | Vencimiento dia {int(tc_row.get(TC_COL_DIA_VENC, 5) or 5)} | Pago sugerido: {fecha_pago_sugerida.date().isoformat() if not pd.isna(fecha_pago_sugerida) else 'sin fecha'}")

            tc_prepago_meses = 0
            tc_prepago_inicio = pd.NaT
            tc_inv_mov = ""
            tc_inv_item = ""
            tc_inv_fecha_llegada = pd.NaT
            tc_af_tipo = ""
            tc_af_vida = 5
            tc_af_inicio = pd.NaT
            tc_af_residual = 0.0
            tc_af_dep_toggle = "No"
            tc_af_dep_mensual = 0.0

            if tratamiento_consumo_tc == "Anticipo / prepago":
                pg1, pg2 = st.columns(2)
                with pg1:
                    tc_prepago_meses = st.number_input("Plazo prepago meses", min_value=1, step=1, value=12, key="tc_cons_prep_meses")
                with pg2:
                    tc_prepago_inicio = st.date_input("Fecha inicio prepago", value=fecha_consumo_tc, key="tc_cons_prep_ini")
            elif tratamiento_consumo_tc == "Inventario":
                iv1, iv2, iv3 = st.columns([1, 2, 1])
                with iv1:
                    tc_inv_mov = st.selectbox("Movimiento inventario", INV_MOV_OPTIONS, index=0, key="tc_cons_inv_mov")
                with iv2:
                    tc_inv_item = st.text_input("Item inventario / referencia", key="tc_cons_inv_item")
                with iv3:
                    tc_inv_fecha_llegada = st.date_input("Fecha llegada / disponibilidad", value=fecha_consumo_tc, key="tc_cons_inv_llegada", disabled=tc_inv_mov not in INVENTORY_POSITIVE_MOVEMENTS)
            elif tratamiento_consumo_tc == "Activo fijo":
                af1, af2, af3 = st.columns(3)
                with af1:
                    tc_af_dep_toggle = st.selectbox("¿Depreciar / amortizar?", YES_NO_OPTIONS, index=0, key="tc_cons_af_dep")
                    tc_af_tipo = st.selectbox("Tipo activo fijo", AF_TYPE_OPTIONS, index=0, key="tc_cons_af_tipo")
                with af2:
                    tc_af_vida = st.selectbox("Vida util (anios)", AF_LIFE_OPTIONS, index=2, key="tc_cons_af_vida")
                    tc_af_inicio = st.date_input("Fecha inicio activo", value=fecha_consumo_tc, key="tc_cons_af_inicio")
                with af3:
                    tc_af_residual = st.number_input("Valor residual", min_value=0.0, step=1.0, key="tc_cons_af_resid")
                if _bool_from_toggle(tc_af_dep_toggle):
                    tc_af_dep_mensual = max(0.0, float(monto_consumo_tc) - float(tc_af_residual)) / max(1, int(tc_af_vida) * 12)
                    st.caption(f"Depreciacion/amortizacion mensual estimada: {_format_money_es(tc_af_dep_mensual)}")

            if st.button("Registrar consumo tarjeta", key="btn_registrar_tc_consumo"):
                if float(monto_consumo_tc) <= 0:
                    st.error("Debes indicar un monto de consumo mayor que cero.")
                    st.stop()
                if not str(desc_consumo_tc or "").strip():
                    st.error("Debes indicar una descripcion.")
                    st.stop()
                if tratamiento_consumo_tc == "Inventario" and not str(tc_inv_item or "").strip():
                    st.error("Debes indicar el item inventario / referencia.")
                    st.stop()
                if tratamiento_consumo_tc == "Anticipo / prepago" and int(tc_prepago_meses or 0) <= 0:
                    st.error("Debes indicar el plazo del prepago.")
                    st.stop()
                new_row = _build_card_consumo_row(
                    card_row=tc_row,
                    fecha_evento=fecha_consumo_tc,
                    monto=float(monto_consumo_tc),
                    descripcion=str(desc_consumo_tc or "").strip(),
                    categoria=categoria_consumo_tc,
                    proveedor=str(proveedor_consumo_tc or "").strip(),
                    tratamiento=tratamiento_consumo_tc,
                    detalle_gasto=detalle_consumo_tc,
                    fecha_pago_esperada=fecha_pago_tc,
                    prepago_meses=int(tc_prepago_meses),
                    prepago_inicio=tc_prepago_inicio,
                    inventario_mov=tc_inv_mov,
                    inventario_item=tc_inv_item,
                    inventario_fecha_llegada=tc_inv_fecha_llegada,
                    activo_fijo_tipo=tc_af_tipo,
                    activo_fijo_vida=int(tc_af_vida or 0),
                    activo_fijo_inicio=tc_af_inicio,
                    activo_fijo_residual=float(tc_af_residual or 0.0),
                    activo_fijo_dep_toggle=tc_af_dep_toggle,
                    activo_fijo_dep_mensual=float(tc_af_dep_mensual or 0.0),
                )
                old_gas_df = st.session_state.df_gas.copy()
                new_gas_df = pd.concat([old_gas_df, pd.DataFrame([new_row])], ignore_index=True)
                new_gas_df = ensure_gastos_columns(new_gas_df)
                wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                if not wrote:
                    st.error("No se pudo guardar el consumo de tarjeta.")
                    st.stop()
                st.session_state.df_gas = new_gas_df
                st.cache_data.clear()
                st.success("Consumo de tarjeta registrado.")
                _safe_rerun()

    with tab_tc3:
        if cards_activas.empty:
            st.info("Primero configura y activa al menos una tarjeta.")
        else:
            pay_labels = {
                f"{str(row.get(TC_COL_EMPRESA, '')).strip()} | {str(row.get(TC_COL_NOMBRE, '')).strip()} | {str(row.get(TC_COL_BANCO, '')).strip()}": str(row.get(TC_COL_ROWID, "")).strip()
                for _, row in cards_activas.iterrows()
            }
            tc_pay_sel = st.selectbox("Tarjeta activa", list(pay_labels.keys()), key="tc_pay_sel")
            tc_pay_row = cards_activas[cards_activas[TC_COL_ROWID].astype(str) == pay_labels.get(tc_pay_sel, "")].iloc[0]
            tc_pay_name = str(tc_pay_row.get(TC_COL_NOMBRE, "") or "").strip()
            consumos_tc, pagado_tc, saldo_tc = _tarjeta_position(tc_pay_name)
            p1, p2, p3 = st.columns(3)
            p1.metric("Consumos acumulados", _format_money_es(consumos_tc))
            p2.metric("Pagado a consumos", _format_money_es(pagado_tc))
            p3.metric("Saldo pendiente tarjeta", _format_money_es(saldo_tc))
            fecha_pago_tc = st.date_input("Fecha pago / cargo", value=_today(), key="tc_pay_fecha")
            q1, q2, q3 = st.columns(3)
            with q1:
                pago_consumos_tc = st.number_input("Pago a consumos", min_value=0.0, step=1.0, key="tc_pay_capital")
            with q2:
                interes_tc = st.number_input("Interes pagado", min_value=0.0, step=1.0, key="tc_pay_interes")
            with q3:
                cargos_tc = st.number_input("Otros cargos", min_value=0.0, step=1.0, key="tc_pay_otros")
            nota_tc = st.text_input("Nota pago / cargo", key="tc_pay_nota")
            if st.button("Registrar pago / cargo tarjeta", key="btn_registrar_tc_pago"):
                if float(pago_consumos_tc) <= 0 and float(interes_tc) <= 0 and float(cargos_tc) <= 0:
                    st.error("Debes registrar al menos pago a consumos, interes u otros cargos.")
                    st.stop()
                if float(pago_consumos_tc) > saldo_tc + 0.01:
                    st.error("El pago a consumos excede el saldo pendiente de la tarjeta.")
                    st.stop()
                old_gas_df = st.session_state.df_gas.copy()
                new_gas_df = _apply_card_payment(old_gas_df, tc_pay_name, fecha_pago_tc, float(pago_consumos_tc), str(nota_tc or "").strip())
                if float(interes_tc) > 0:
                    desc = f"Interes tarjeta de credito - {tc_pay_name}"
                    if str(nota_tc or "").strip():
                        desc = f"{desc} | {str(nota_tc or '').strip()}"
                    new_gas_df = pd.concat(
                        [new_gas_df, pd.DataFrame([_build_card_charge_row(card_row=tc_pay_row, fecha_evento=fecha_pago_tc, monto=float(interes_tc), descripcion=desc, detalle_gasto='Intereses')])],
                        ignore_index=True,
                    )
                if float(cargos_tc) > 0:
                    desc = f"Cargos tarjeta de credito - {tc_pay_name}"
                    if str(nota_tc or "").strip():
                        desc = f"{desc} | {str(nota_tc or '').strip()}"
                    new_gas_df = pd.concat(
                        [new_gas_df, pd.DataFrame([_build_card_charge_row(card_row=tc_pay_row, fecha_evento=fecha_pago_tc, monto=float(cargos_tc), descripcion=desc, detalle_gasto='Otros')])],
                        ignore_index=True,
                    )
                new_gas_df = ensure_gastos_columns(new_gas_df)
                wrote = safe_write_worksheet(client, SHEET_ID, WS_GAS, new_gas_df, old_df=old_gas_df)
                if not wrote:
                    st.error("No se pudo guardar el pago / cargo de la tarjeta.")
                    st.stop()
                st.session_state.df_gas = new_gas_df
                st.cache_data.clear()
                st.success("Pago / cargo de tarjeta registrado.")
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
    inventario_fecha_llegada = pd.NaT

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
        iv1, iv2, iv3 = st.columns([1, 2, 1])
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
        with iv3:
            inventario_fecha_llegada = st.date_input(
                "Fecha llegada / disponibilidad",
                value=fecha_g,
                key="gas_inv_fecha_llegada_quick",
                help="Solo aplica para entradas o ajustes positivos. Antes de esa fecha se considera inventario en transito.",
                disabled=inventario_mov not in INVENTORY_POSITIVE_MOVEMENTS,
                on_change=lambda: _mark_form_force_open("gas"),
            )
        st.caption("Entrada aumenta inventario. Si la fecha de llegada es futura, el panel lo mostrara como inventario en transito hasta esa fecha.")

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
            COL_INV_FEC_LLEGADA: _ts(inventario_fecha_llegada) if tratamiento_gas == "Inventario" and inventario_mov in INVENTORY_POSITIVE_MOVEMENTS else pd.NaT,
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
    COL_INV_FEC_LLEGADA: st.column_config.DateColumn("Fecha llegada inventario"),
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
    COL_REC, COL_REC_PER, COL_REC_REG, COL_REC_DUR, COL_INV_MOV, COL_INV_ITEM, COL_INV_FEC_LLEGADA, COL_PREPAGO_MESES, COL_PREPAGO_FEC_INI,
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
