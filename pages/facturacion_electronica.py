from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st
from gspread.exceptions import WorksheetNotFound

from entities import WS_CLIENTES, WS_PROYECTOS, _load_clients, _load_projects
from services.access_control import build_authenticator, require_page_access
from services.firmatech_adapter import (
    FirmatechClient,
    FirmatechConfigurationError,
    FirmatechError,
    build_invoice_payload_from_draft,
    load_firmatech_settings,
    provider_health,
    validate_invoice_payload,
)
from sheets import get_client, read_worksheet, write_worksheet
from ui.theme import apply_global_theme


st.set_page_config(page_title="Facturacion electronica", page_icon="🧾", layout="wide")
apply_global_theme()


def _safe_rerun() -> None:
    rerun = getattr(st, "rerun", None)
    if callable(rerun):
        rerun()
        return
    legacy = getattr(st, "experimental_rerun", None)
    if callable(legacy):
        legacy()


authenticator = build_authenticator()
try:
    authenticator.login(" ", location="sidebar", key="auth_facturacion_silent")
    st.sidebar.empty()
except Exception:
    pass

require_page_access("pages/facturacion_electronica.py")
st.session_state.setdefault("auth_user_name", st.session_state.get("name", ""))
st.session_state.setdefault("auth_username", st.session_state.get("username", ""))
authenticator.logout("Cerrar sesion", location="sidebar")


SHEET_ID = st.secrets["app"]["SHEET_ID"]
WS_COT = "cotizaciones"
WS_FACT = "Facturacion"
DEFAULT_COMPANIES = ["RS Engineering", "RIR Medical", "SP Tech Solutions S.A."]
FACT_COLUMNS = [
    "FacturaID",
    "EstadoLocal",
    "EstadoAPI",
    "Empresa",
    "ClienteID",
    "ClienteNombre",
    "ClienteRUC",
    "ClienteDV",
    "ClienteEmail",
    "ClienteDireccion",
    "ProyectoID",
    "ProyectoNombre",
    "Origen",
    "OrigenRef",
    "Serie",
    "FechaEmision",
    "FechaVencimiento",
    "CondicionPago",
    "Moneda",
    "Subtotal",
    "Impuesto",
    "Total",
    "LineasJSON",
    "PayloadJSON",
    "FirmatechExternalID",
    "FirmatechCUFE",
    "FirmatechRawResponse",
    "Notas",
    "CreatedAt",
    "UpdatedAt",
    "Usuario",
]
ITEM_COLUMNS = ["sku", "description", "quantity", "unit_price", "tax_rate"]
PAYMENT_TERMS = ["Contado", "Credito 7 dias", "Credito 15 dias", "Credito 30 dias", "Credito 60 dias", "Otro"]
LOCAL_STATUS_OPTIONS = ["borrador", "listo_api", "emitida", "error_api", "anulada"]


def _default_items_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sku": "",
                "description": "",
                "quantity": 1.0,
                "unit_price": 0.0,
                "tax_rate": 7.0,
            }
        ],
        columns=ITEM_COLUMNS,
    )


def _new_invoice_id() -> str:
    return f"FAC-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _company_aliases(company: str) -> set[str]:
    base = _norm_text(company).lower()
    aliases = {base}
    if "rir" in base:
        aliases.update({"rir", "rir medical"})
    if any(token in base for token in ("rs", "sp", "engineering", "solutions")):
        aliases.update(
            {
                "rs engineering",
                "sp tech solutions s.a.",
                "sp engineering",
                "rs-sp",
                "rs / sp",
                "rs/sp",
            }
        )
    return aliases


def _client_company_matches(row_company: str, selected_company: str) -> bool:
    raw = _norm_text(row_company).lower()
    if not raw or not selected_company:
        return True
    return raw in _company_aliases(selected_company)


def _ensure_facturacion_sheet(client, sheet_id: str) -> None:
    sh = client.open_by_key(sheet_id)
    try:
        sh.worksheet(WS_FACT)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=WS_FACT, rows=1000, cols=max(len(FACT_COLUMNS), 24))
        ws.update("A1", [FACT_COLUMNS])


def _normalize_facturas_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy() if df is not None else pd.DataFrame()
    for col in FACT_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[FACT_COLUMNS].copy()
    for col in ["Subtotal", "Impuesto", "Total"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    for col in ["FechaEmision", "FechaVencimiento", "CreatedAt", "UpdatedAt"]:
        out[col] = out[col].fillna("").astype(str)
    out["FacturaID"] = out["FacturaID"].fillna("").astype(str)
    out["EstadoLocal"] = out["EstadoLocal"].replace("", "borrador").fillna("borrador")
    out["EstadoAPI"] = out["EstadoAPI"].fillna("").astype(str)
    return out


def _normalize_cotizaciones_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy() if df is not None else pd.DataFrame()
    needed = [
        "id",
        "numero_cotizacion",
        "empresa",
        "cliente_nombre",
        "cliente_ruc",
        "cliente_dv",
        "fecha_cotizacion",
        "items_json",
        "forma_pago",
        "moneda",
        "detalles_extra",
        "total",
    ]
    for col in needed:
        if col not in out.columns:
            out[col] = ""
    out["fecha_cotizacion"] = out["fecha_cotizacion"].fillna("").astype(str)
    out["total"] = pd.to_numeric(out["total"], errors="coerce").fillna(0.0)
    return out[needed].copy()


@st.cache_data(ttl=120, show_spinner=False)
def _load_facturas_cached(sheet_id: str, cache_token: str) -> pd.DataFrame:
    client, _ = get_client()
    _ensure_facturacion_sheet(client, sheet_id)
    return _normalize_facturas_df(read_worksheet(client, sheet_id, WS_FACT))


@st.cache_data(ttl=120, show_spinner=False)
def _load_cotizaciones_cached(sheet_id: str, cache_token: str) -> pd.DataFrame:
    client, _ = get_client()
    try:
        return _normalize_cotizaciones_df(read_worksheet(client, sheet_id, WS_COT))
    except WorksheetNotFound:
        return _normalize_cotizaciones_df(pd.DataFrame())


def _safe_load_clients_df(client, sheet_id: str) -> pd.DataFrame:
    try:
        return _load_clients(client, sheet_id)
    except WorksheetNotFound:
        return pd.DataFrame(columns=["ClienteID", "ClienteNombre", "Empresa", "ClienteRUC", "ClienteDV"])
    except Exception:
        return pd.DataFrame(columns=["ClienteID", "ClienteNombre", "Empresa", "ClienteRUC", "ClienteDV"])


def _safe_load_projects_df(client, sheet_id: str) -> pd.DataFrame:
    try:
        return _load_projects(client, sheet_id)
    except WorksheetNotFound:
        return pd.DataFrame(columns=["ProyectoID", "ProyectoNombre", "ClienteID", "ClienteNombre"])
    except Exception:
        return pd.DataFrame(columns=["ProyectoID", "ProyectoNombre", "ClienteID", "ClienteNombre"])


def _coerce_items_df(df: pd.DataFrame | None) -> pd.DataFrame:
    work = df.copy() if df is not None else _default_items_df()
    for col in ITEM_COLUMNS:
        if col not in work.columns:
            work[col] = ""
    work = work[ITEM_COLUMNS].copy()
    work["description"] = work["description"].fillna("").astype(str)
    work["sku"] = work["sku"].fillna("").astype(str)
    work["quantity"] = pd.to_numeric(work["quantity"], errors="coerce").fillna(0.0)
    work["unit_price"] = pd.to_numeric(work["unit_price"], errors="coerce").fillna(0.0)
    work["tax_rate"] = pd.to_numeric(work["tax_rate"], errors="coerce").fillna(0.0)
    return work


def _parse_lines_json(raw_value: Any) -> pd.DataFrame:
    try:
        items = json.loads(raw_value or "[]")
    except Exception:
        items = []
    if not isinstance(items, list):
        items = []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "sku": _norm_text(item.get("sku") or item.get("codigo")),
                "description": _norm_text(
                    item.get("description") or item.get("descripcion") or item.get("producto_servicio")
                ),
                "quantity": float(item.get("quantity") or item.get("cantidad") or 0) if item else 0.0,
                "unit_price": float(item.get("unit_price") or item.get("precio_unitario") or item.get("precio") or 0)
                if item
                else 0.0,
                "tax_rate": float(item.get("tax_rate") or item.get("itbms_pct") or item.get("tax") or 7) if item else 7.0,
            }
        )
    return _coerce_items_df(pd.DataFrame(rows)) if rows else _default_items_df()


def _items_with_totals(df: pd.DataFrame) -> pd.DataFrame:
    work = _coerce_items_df(df)
    work["line_subtotal"] = (work["quantity"] * work["unit_price"]).round(2)
    work["line_tax"] = (work["line_subtotal"] * (work["tax_rate"] / 100.0)).round(2)
    work["line_total"] = (work["line_subtotal"] + work["line_tax"]).round(2)
    return work


def _totals_from_items(df: pd.DataFrame) -> tuple[float, float, float]:
    work = _items_with_totals(df)
    subtotal = round(float(work["line_subtotal"].sum()), 2)
    tax = round(float(work["line_tax"].sum()), 2)
    total = round(float(work["line_total"].sum()), 2)
    return subtotal, tax, total


def _client_options(df_cli: pd.DataFrame, company: str) -> list[dict[str, str]]:
    work = df_cli.copy()
    if "Empresa" in work.columns:
        mask = work["Empresa"].astype(str).apply(lambda value: _client_company_matches(value, company))
        work = work[mask].copy()
    for col in ["ClienteID", "ClienteNombre", "ClienteRUC", "ClienteDV"]:
        if col not in work.columns:
            work[col] = ""
    work = work.sort_values(["ClienteNombre", "ClienteID"], na_position="last")
    options: list[dict[str, str]] = []
    for _, row in work.iterrows():
        options.append(
            {
                "label": f"{_norm_text(row.get('ClienteNombre')) or _norm_text(row.get('ClienteID'))} · {_norm_text(row.get('ClienteID'))}",
                "id": _norm_text(row.get("ClienteID")),
                "name": _norm_text(row.get("ClienteNombre")),
                "ruc": _norm_text(row.get("ClienteRUC")),
                "dv": _norm_text(row.get("ClienteDV")),
            }
        )
    return options


def _project_options(df_proj: pd.DataFrame, client_id: str) -> list[dict[str, str]]:
    work = df_proj.copy()
    for col in ["ProyectoID", "ProyectoNombre", "ClienteID"]:
        if col not in work.columns:
            work[col] = ""
    if client_id:
        work = work[work["ClienteID"].astype(str).str.strip() == client_id].copy()
    work = work.sort_values(["ProyectoNombre", "ProyectoID"], na_position="last")
    options: list[dict[str, str]] = []
    for _, row in work.iterrows():
        options.append(
            {
                "label": f"{_norm_text(row.get('ProyectoNombre')) or _norm_text(row.get('ProyectoID'))} · {_norm_text(row.get('ProyectoID'))}",
                "id": _norm_text(row.get("ProyectoID")),
                "name": _norm_text(row.get("ProyectoNombre")),
            }
        )
    return options


def _quote_options(df_cot: pd.DataFrame, company: str, client_name: str) -> list[dict[str, Any]]:
    work = df_cot.copy()
    if company:
        work = work[work["empresa"].astype(str).str.strip().str.lower().isin(_company_aliases(company))].copy()
    if client_name:
        work = work[work["cliente_nombre"].astype(str).str.strip().str.lower() == client_name.strip().lower()].copy()
    work = work.sort_values("fecha_cotizacion", ascending=False, na_position="last")
    options: list[dict[str, Any]] = []
    for _, row in work.head(200).iterrows():
        options.append(
            {
                "label": f"{_norm_text(row.get('numero_cotizacion'))} · {_norm_text(row.get('cliente_nombre'))} · ${float(row.get('total') or 0):,.2f}",
                "numero": _norm_text(row.get("numero_cotizacion")),
                "row": row.to_dict(),
            }
        )
    return options


def _set_current_from_quote(row: dict[str, Any]) -> None:
    st.session_state["fe_source"] = "Cotizacion"
    st.session_state["fe_source_ref"] = _norm_text(row.get("numero_cotizacion"))
    st.session_state["fe_company"] = _norm_text(row.get("empresa")) or st.session_state.get("fe_company", "")
    st.session_state["fe_client_name"] = _norm_text(row.get("cliente_nombre"))
    st.session_state["fe_client_ruc"] = _norm_text(row.get("cliente_ruc"))
    st.session_state["fe_client_dv"] = _norm_text(row.get("cliente_dv"))
    st.session_state["fe_currency"] = _norm_text(row.get("moneda")) or "USD"
    st.session_state["fe_notes"] = _norm_text(row.get("detalles_extra"))
    st.session_state["fe_payment_terms"] = _norm_text(row.get("forma_pago")) or "Credito 30 dias"
    st.session_state["fe_items_df"] = _parse_lines_json(row.get("items_json"))


def _set_current_from_draft(row: dict[str, Any]) -> None:
    st.session_state["fe_invoice_id"] = _norm_text(row.get("FacturaID")) or _new_invoice_id()
    st.session_state["fe_status_local"] = _norm_text(row.get("EstadoLocal")) or "borrador"
    st.session_state["fe_company"] = _norm_text(row.get("Empresa"))
    st.session_state["fe_client_id"] = _norm_text(row.get("ClienteID"))
    st.session_state["fe_client_name"] = _norm_text(row.get("ClienteNombre"))
    st.session_state["fe_client_ruc"] = _norm_text(row.get("ClienteRUC"))
    st.session_state["fe_client_dv"] = _norm_text(row.get("ClienteDV"))
    st.session_state["fe_client_email"] = _norm_text(row.get("ClienteEmail"))
    st.session_state["fe_client_address"] = _norm_text(row.get("ClienteDireccion"))
    st.session_state["fe_project_id"] = _norm_text(row.get("ProyectoID"))
    st.session_state["fe_project_name"] = _norm_text(row.get("ProyectoNombre"))
    st.session_state["fe_source"] = _norm_text(row.get("Origen")) or "Manual"
    st.session_state["fe_source_ref"] = _norm_text(row.get("OrigenRef"))
    st.session_state["fe_series"] = _norm_text(row.get("Serie"))
    st.session_state["fe_issue_date"] = pd.to_datetime(row.get("FechaEmision"), errors="coerce").date() if _norm_text(row.get("FechaEmision")) else date.today()
    st.session_state["fe_due_date"] = pd.to_datetime(row.get("FechaVencimiento"), errors="coerce").date() if _norm_text(row.get("FechaVencimiento")) else date.today()
    st.session_state["fe_payment_terms"] = _norm_text(row.get("CondicionPago")) or PAYMENT_TERMS[0]
    st.session_state["fe_currency"] = _norm_text(row.get("Moneda")) or "USD"
    st.session_state["fe_notes"] = _norm_text(row.get("Notas"))
    st.session_state["fe_items_df"] = _parse_lines_json(row.get("LineasJSON"))


def _reset_current_draft() -> None:
    st.session_state["fe_invoice_id"] = _new_invoice_id()
    st.session_state["fe_status_local"] = "borrador"
    st.session_state["fe_company"] = DEFAULT_COMPANIES[0]
    st.session_state["fe_client_id"] = ""
    st.session_state["fe_client_name"] = ""
    st.session_state["fe_client_ruc"] = ""
    st.session_state["fe_client_dv"] = ""
    st.session_state["fe_client_email"] = ""
    st.session_state["fe_client_address"] = ""
    st.session_state["fe_project_id"] = ""
    st.session_state["fe_project_name"] = ""
    st.session_state["fe_source"] = "Manual"
    st.session_state["fe_source_ref"] = ""
    st.session_state["fe_series"] = ""
    st.session_state["fe_issue_date"] = date.today()
    st.session_state["fe_due_date"] = date.today()
    st.session_state["fe_payment_terms"] = PAYMENT_TERMS[0]
    st.session_state["fe_currency"] = "USD"
    st.session_state["fe_notes"] = ""
    st.session_state["fe_items_df"] = _default_items_df()


def _payment_term_default_due(payment_terms: str, issue_date: date) -> date:
    norm = _norm_text(payment_terms).lower()
    days = 0
    if "7" in norm:
        days = 7
    elif "15" in norm:
        days = 15
    elif "30" in norm:
        days = 30
    elif "60" in norm:
        days = 60
    return issue_date + timedelta(days=days)


def _current_draft_payload(items_df: pd.DataFrame) -> dict[str, Any]:
    project_name = st.session_state.get("fe_project_name", "")
    client_name = st.session_state.get("fe_client_name", "")
    draft = {
        "invoice_id": st.session_state.get("fe_invoice_id", ""),
        "status_local": st.session_state.get("fe_status_local", "borrador"),
        "company": st.session_state.get("fe_company", ""),
        "client_id": st.session_state.get("fe_client_id", ""),
        "client_name": client_name,
        "client_ruc": st.session_state.get("fe_client_ruc", ""),
        "client_dv": st.session_state.get("fe_client_dv", ""),
        "client_email": st.session_state.get("fe_client_email", ""),
        "client_address": st.session_state.get("fe_client_address", ""),
        "project_id": st.session_state.get("fe_project_id", ""),
        "project_name": project_name,
        "source": st.session_state.get("fe_source", "Manual"),
        "source_ref": st.session_state.get("fe_source_ref", ""),
        "series": st.session_state.get("fe_series", ""),
        "issue_date": str(st.session_state.get("fe_issue_date", date.today())),
        "due_date": str(st.session_state.get("fe_due_date", date.today())),
        "payment_terms": st.session_state.get("fe_payment_terms", PAYMENT_TERMS[0]),
        "currency": st.session_state.get("fe_currency", "USD"),
        "notes": st.session_state.get("fe_notes", ""),
        "created_by": str(st.session_state.get("auth_username", "") or st.session_state.get("auth_user_name", "") or "").strip(),
        "lines": _coerce_items_df(items_df).to_dict(orient="records"),
    }
    return build_invoice_payload_from_draft(draft)


def _upsert_factura_row(drafts_df: pd.DataFrame, payload: dict[str, Any], *, response_data: dict[str, Any] | None = None) -> pd.DataFrame:
    now = _now_text()
    subtotal = float(payload.get("totals", {}).get("subtotal") or 0)
    tax = float(payload.get("totals", {}).get("tax") or 0)
    total = float(payload.get("totals", {}).get("total") or 0)
    response_data = response_data or {}

    row = {
        "FacturaID": _norm_text(payload.get("document", {}).get("internal_id")),
        "EstadoLocal": _norm_text(payload.get("metadata", {}).get("status_local")) or "borrador",
        "EstadoAPI": _norm_text(response_data.get("status") or response_data.get("estado")),
        "Empresa": _norm_text(payload.get("issuer", {}).get("company")),
        "ClienteID": _norm_text(payload.get("customer", {}).get("client_id")),
        "ClienteNombre": _norm_text(payload.get("customer", {}).get("name")),
        "ClienteRUC": _norm_text(payload.get("customer", {}).get("ruc")),
        "ClienteDV": _norm_text(payload.get("customer", {}).get("dv")),
        "ClienteEmail": _norm_text(payload.get("customer", {}).get("email")),
        "ClienteDireccion": _norm_text(payload.get("customer", {}).get("address")),
        "ProyectoID": _norm_text(payload.get("project", {}).get("project_id")),
        "ProyectoNombre": _norm_text(payload.get("project", {}).get("project_name")),
        "Origen": _norm_text(payload.get("origin", {}).get("source")),
        "OrigenRef": _norm_text(payload.get("origin", {}).get("source_ref")),
        "Serie": _norm_text(payload.get("document", {}).get("series")),
        "FechaEmision": _norm_text(payload.get("document", {}).get("issue_date")),
        "FechaVencimiento": _norm_text(payload.get("document", {}).get("due_date")),
        "CondicionPago": _norm_text(payload.get("document", {}).get("payment_terms")),
        "Moneda": _norm_text(payload.get("document", {}).get("currency")) or "USD",
        "Subtotal": round(subtotal, 2),
        "Impuesto": round(tax, 2),
        "Total": round(total, 2),
        "LineasJSON": json.dumps(payload.get("lines", []), ensure_ascii=False),
        "PayloadJSON": json.dumps(payload, ensure_ascii=False),
        "FirmatechExternalID": _norm_text(response_data.get("external_id") or response_data.get("id")),
        "FirmatechCUFE": _norm_text(response_data.get("cufe") or response_data.get("codigo_fiscal")),
        "FirmatechRawResponse": json.dumps(response_data, ensure_ascii=False) if response_data else "",
        "Notas": _norm_text(payload.get("document", {}).get("notes")),
        "CreatedAt": now,
        "UpdatedAt": now,
        "Usuario": str(st.session_state.get("auth_username", "") or st.session_state.get("auth_user_name", "") or "").strip(),
    }
    updated = _normalize_facturas_df(drafts_df)
    mask = updated["FacturaID"].astype(str).str.strip() == row["FacturaID"]
    if mask.any():
        first_idx = updated.index[mask][0]
        created_at = _norm_text(updated.at[first_idx, "CreatedAt"])
        if created_at:
            row["CreatedAt"] = created_at
        updated.loc[first_idx, FACT_COLUMNS] = [row.get(col, "") for col in FACT_COLUMNS]
    else:
        updated = pd.concat([updated, pd.DataFrame([row])], ignore_index=True)
    return _normalize_facturas_df(updated)


if "facturacion_cache_token" not in st.session_state:
    st.session_state["facturacion_cache_token"] = uuid.uuid4().hex
if "fe_invoice_id" not in st.session_state:
    _reset_current_draft()

client, _ = get_client()
_ensure_facturacion_sheet(client, SHEET_ID)

cache_token = st.session_state["facturacion_cache_token"]
drafts_df = _load_facturas_cached(SHEET_ID, cache_token)
quotes_df = _load_cotizaciones_cached(SHEET_ID, cache_token)
clients_df = _safe_load_clients_df(client, SHEET_ID)
projects_df = _safe_load_projects_df(client, SHEET_ID)

settings = load_firmatech_settings()
health = provider_health(settings)

st.title("Facturacion electronica")
st.caption("Borradores listos para emitir por API, trazables y preparados para integrarse despues con Finanzas.")

metric_1, metric_2, metric_3, metric_4 = st.columns(4)
metric_1.metric("Borradores", int((drafts_df["EstadoLocal"] == "borrador").sum()) if not drafts_df.empty else 0)
metric_2.metric("Listos API", int((drafts_df["EstadoLocal"] == "listo_api").sum()) if not drafts_df.empty else 0)
metric_3.metric("Emitidas", int((drafts_df["EstadoLocal"] == "emitida").sum()) if not drafts_df.empty else 0)
metric_4.metric("Provider", "Configurado" if health["can_emit"] else "Pendiente")

left, right = st.columns([1.45, 1.0], gap="large")

with right:
    with st.expander("Estado de integracion", expanded=True):
        st.write(f"Proveedor: `{health['provider']}`")
        st.write(f"Modo: `{health['mode']}`")
        st.write(f"Base URL: `{health['base_url'] or 'sin configurar'}`")
        st.write(f"Endpoint emitir: `{health['issue_path'] or 'sin configurar'}`")
        if health["missing"]:
            st.warning("Falta configurar: " + ", ".join(health["missing"]))
        else:
            st.success("La configuracion base ya existe. Solo faltaria validar el mapeo final del API si aplica.")

    with st.expander("Borradores guardados", expanded=True):
        if drafts_df.empty:
            st.info("Aun no hay borradores guardados.")
        else:
            drafts_view = drafts_df[
                ["FacturaID", "Empresa", "ClienteNombre", "Total", "EstadoLocal", "EstadoAPI", "UpdatedAt"]
            ].sort_values("UpdatedAt", ascending=False, na_position="last")
            st.dataframe(drafts_view, use_container_width=True, hide_index=True)
            draft_options = [
                {
                    "label": f"{row.FacturaID} · {row.ClienteNombre} · {row.EstadoLocal} · ${float(row.Total):,.2f}",
                    "id": row.FacturaID,
                }
                for _, row in drafts_view.iterrows()
            ]
            selected_label = st.selectbox(
                "Cargar borrador existente",
                options=[""] + [opt["label"] for opt in draft_options],
                index=0,
                key="fe_load_draft_label",
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Cargar borrador", use_container_width=True, type="secondary"):
                    match = next((opt for opt in draft_options if opt["label"] == selected_label), None)
                    if not match:
                        st.warning("Selecciona un borrador.")
                    else:
                        row = drafts_df[drafts_df["FacturaID"].astype(str) == match["id"]].head(1)
                        if row.empty:
                            st.error("No se encontro el borrador seleccionado.")
                        else:
                            _set_current_from_draft(row.iloc[0].to_dict())
                            _safe_rerun()
            with c2:
                if st.button("Nuevo borrador", use_container_width=True):
                    _reset_current_draft()
                    _safe_rerun()

with left:
    with st.expander("Origen y cabecera", expanded=True):
        company_options = list(dict.fromkeys(DEFAULT_COMPANIES + [_norm_text(v) for v in quotes_df.get("empresa", pd.Series(dtype=str)).tolist()] + [_norm_text(v) for v in drafts_df.get("Empresa", pd.Series(dtype=str)).tolist() if _norm_text(v)]))
        current_company = st.session_state.get("fe_company", DEFAULT_COMPANIES[0])
        if current_company not in company_options:
            company_options.append(current_company)
        st.selectbox("Empresa emisora", options=company_options, key="fe_company")

        source_col1, source_col2 = st.columns([1.0, 1.0])
        with source_col1:
            st.selectbox("Origen", options=["Manual", "Cotizacion"], key="fe_source")
        with source_col2:
            st.text_input("Referencia origen", key="fe_source_ref", placeholder="Numero de cotizacion, pedido o referencia interna")

        client_options = _client_options(clients_df, st.session_state.get("fe_company", ""))
        client_labels = [""] + [opt["label"] for opt in client_options]
        current_client_id = st.session_state.get("fe_client_id", "")
        current_client_label = ""
        for opt in client_options:
            if opt["id"] == current_client_id and current_client_id:
                current_client_label = opt["label"]
                break
        client_index = client_labels.index(current_client_label) if current_client_label in client_labels else 0
        selected_client_label = st.selectbox("Cliente", options=client_labels, index=client_index, key="fe_client_label")
        selected_client = next((opt for opt in client_options if opt["label"] == selected_client_label), None)
        if selected_client:
            st.session_state["fe_client_id"] = selected_client["id"]
            st.session_state["fe_client_name"] = selected_client["name"]
            st.session_state["fe_client_ruc"] = selected_client["ruc"]
            st.session_state["fe_client_dv"] = selected_client["dv"]
        elif not selected_client_label:
            st.session_state["fe_client_id"] = ""

        project_options = _project_options(projects_df, st.session_state.get("fe_client_id", ""))
        project_labels = [""] + [opt["label"] for opt in project_options]
        current_project_id = st.session_state.get("fe_project_id", "")
        current_project_label = ""
        for opt in project_options:
            if opt["id"] == current_project_id and current_project_id:
                current_project_label = opt["label"]
                break
        project_index = project_labels.index(current_project_label) if current_project_label in project_labels else 0
        selected_project_label = st.selectbox("Proyecto", options=project_labels, index=project_index, key="fe_project_label")
        selected_project = next((opt for opt in project_options if opt["label"] == selected_project_label), None)
        if selected_project:
            st.session_state["fe_project_id"] = selected_project["id"]
            st.session_state["fe_project_name"] = selected_project["name"]
        elif not selected_project_label:
            st.session_state["fe_project_id"] = ""
            st.session_state["fe_project_name"] = ""

        if st.session_state.get("fe_source") == "Cotizacion":
            quote_options = _quote_options(
                quotes_df,
                st.session_state.get("fe_company", ""),
                st.session_state.get("fe_client_name", ""),
            )
            quote_label = st.selectbox(
                "Cotizacion para precargar",
                options=[""] + [opt["label"] for opt in quote_options],
                index=0,
                key="fe_quote_label",
            )
            if st.button("Cargar cotizacion en el borrador", use_container_width=True, type="secondary"):
                match = next((opt for opt in quote_options if opt["label"] == quote_label), None)
                if not match:
                    st.warning("Selecciona una cotizacion.")
                else:
                    _set_current_from_quote(match["row"])
                    _safe_rerun()

        cab_1, cab_2, cab_3 = st.columns(3)
        with cab_1:
            st.text_input("Factura ID interno", key="fe_invoice_id", disabled=True)
        with cab_2:
            st.text_input("Serie / prefijo", key="fe_series", placeholder="Ej: RS, RIR, A")
        with cab_3:
            status_current = st.session_state.get("fe_status_local", "borrador")
            if status_current not in LOCAL_STATUS_OPTIONS:
                status_current = "borrador"
            st.selectbox("Estado local", options=LOCAL_STATUS_OPTIONS, index=LOCAL_STATUS_OPTIONS.index(status_current), key="fe_status_local")

        date_1, date_2, date_3 = st.columns(3)
        with date_1:
            st.date_input("Fecha emision", key="fe_issue_date")
        with date_2:
            st.selectbox("Condicion de pago", options=PAYMENT_TERMS, key="fe_payment_terms")
        with date_3:
            st.date_input("Fecha vencimiento", key="fe_due_date")
        if st.button("Recalcular vencimiento segun condicion", type="secondary"):
            st.session_state["fe_due_date"] = _payment_term_default_due(
                st.session_state.get("fe_payment_terms", PAYMENT_TERMS[0]),
                st.session_state.get("fe_issue_date", date.today()),
            )
            _safe_rerun()

        cli_1, cli_2, cli_3 = st.columns(3)
        st.text_input("Nombre cliente", key="fe_client_name", placeholder="Cliente factura electronica")
        with cli_1:
            st.text_input("RUC cliente", key="fe_client_ruc", placeholder="RUC o identificacion fiscal")
        with cli_2:
            st.text_input("DV", key="fe_client_dv")
        with cli_3:
            st.text_input("Moneda", key="fe_currency")
        st.text_input("Nombre proyecto", key="fe_project_name", placeholder="Proyecto asociado")
        st.text_input("Correo cliente", key="fe_client_email")
        st.text_input("Direccion cliente", key="fe_client_address")
        st.text_area("Notas de factura", key="fe_notes", height=90, placeholder="Observaciones comerciales o fiscales")

    with st.expander("Lineas de factura", expanded=True):
        items_value = _coerce_items_df(st.session_state.get("fe_items_df", _default_items_df()))
        edited_items = st.data_editor(
            items_value,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="fe_items_editor",
            column_config={
                "sku": st.column_config.TextColumn("SKU", help="Codigo interno o del item."),
                "description": st.column_config.TextColumn("Descripcion", required=True),
                "quantity": st.column_config.NumberColumn("Cantidad", min_value=0.0, step=1.0, format="%.2f"),
                "unit_price": st.column_config.NumberColumn("Precio unitario", min_value=0.0, step=1.0, format="%.2f"),
                "tax_rate": st.column_config.NumberColumn("ITBMS %", min_value=0.0, step=1.0, format="%.2f"),
            },
        )
        edited_items = _coerce_items_df(edited_items)
        st.session_state["fe_items_df"] = edited_items
        items_totals_df = _items_with_totals(edited_items)
        subtotal, tax, total = _totals_from_items(edited_items)
        st.dataframe(
            items_totals_df[["description", "quantity", "unit_price", "tax_rate", "line_subtotal", "line_tax", "line_total"]],
            use_container_width=True,
            hide_index=True,
        )
        tot_1, tot_2, tot_3 = st.columns(3)
        tot_1.metric("Subtotal", f"${subtotal:,.2f}")
        tot_2.metric("ITBMS", f"${tax:,.2f}")
        tot_3.metric("Total", f"${total:,.2f}")

        action_1, action_2, action_3 = st.columns(3)
        with action_1:
            if st.button("Guardar borrador", use_container_width=True):
                payload = _current_draft_payload(edited_items)
                updated_df = _upsert_factura_row(drafts_df, payload)
                write_worksheet(client, SHEET_ID, WS_FACT, updated_df)
                st.session_state["facturacion_cache_token"] = uuid.uuid4().hex
                st.success("Borrador guardado.")
                _safe_rerun()
        with action_2:
            if st.button("Guardar como listo API", use_container_width=True):
                st.session_state["fe_status_local"] = "listo_api"
                payload = _current_draft_payload(edited_items)
                errors = validate_invoice_payload(payload)
                if errors:
                    st.error("No se puede marcar listo: " + " | ".join(errors))
                    st.session_state["fe_status_local"] = "borrador"
                else:
                    updated_df = _upsert_factura_row(drafts_df, payload)
                    write_worksheet(client, SHEET_ID, WS_FACT, updated_df)
                    st.session_state["facturacion_cache_token"] = uuid.uuid4().hex
                    st.success("Borrador marcado como listo para API.")
                    _safe_rerun()
        with action_3:
            if st.button("Intentar emitir por API", use_container_width=True, type="primary"):
                payload = _current_draft_payload(edited_items)
                errors = validate_invoice_payload(payload)
                if errors:
                    st.error("Corrige antes de emitir: " + " | ".join(errors))
                else:
                    try:
                        response_data = FirmatechClient(settings).issue_invoice(payload)
                        st.session_state["fe_status_local"] = "emitida"
                        payload["metadata"]["status_local"] = "emitida"
                        updated_df = _upsert_factura_row(drafts_df, payload, response_data=response_data)
                        write_worksheet(client, SHEET_ID, WS_FACT, updated_df)
                        st.session_state["facturacion_cache_token"] = uuid.uuid4().hex
                        st.success("Factura enviada al API.")
                        _safe_rerun()
                    except FirmatechConfigurationError as exc:
                        st.warning(str(exc))
                    except FirmatechError as exc:
                        st.session_state["fe_status_local"] = "error_api"
                        payload["metadata"]["status_local"] = "error_api"
                        updated_df = _upsert_factura_row(drafts_df, payload, response_data={"status": "error", "message": str(exc)})
                        write_worksheet(client, SHEET_ID, WS_FACT, updated_df)
                        st.session_state["facturacion_cache_token"] = uuid.uuid4().hex
                        st.error(str(exc))
                    except Exception as exc:
                        st.session_state["fe_status_local"] = "error_api"
                        payload["metadata"]["status_local"] = "error_api"
                        updated_df = _upsert_factura_row(drafts_df, payload, response_data={"status": "error", "message": str(exc)})
                        write_worksheet(client, SHEET_ID, WS_FACT, updated_df)
                        st.session_state["facturacion_cache_token"] = uuid.uuid4().hex
                        st.error(f"No se pudo completar el intento de emision: {exc}")

live_payload_preview = _current_draft_payload(st.session_state.get("fe_items_df", _default_items_df()))
live_payload_errors = validate_invoice_payload(live_payload_preview)
with st.expander("Payload listo para API", expanded=False):
    if live_payload_errors:
        st.warning("Pendientes: " + " | ".join(live_payload_errors))
    else:
        st.success("Payload base validado.")
    st.code(json.dumps(live_payload_preview, indent=2, ensure_ascii=False), language="json")
    st.download_button(
        "Descargar payload JSON",
        data=json.dumps(live_payload_preview, indent=2, ensure_ascii=False),
        file_name=f"{st.session_state.get('fe_invoice_id', 'factura')}.json",
        mime="application/json",
        use_container_width=True,
    )
