from __future__ import annotations

import base64
import html
import json
import uuid
import os
from datetime import date, datetime
from io import BytesIO
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from gspread.exceptions import WorksheetNotFound
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from sheets import get_client, read_worksheet, write_worksheet
from ui.theme import apply_global_theme

st.set_page_config(page_title="Generador de cotizaciones", page_icon="🧾", layout="wide")
apply_global_theme()

# ---- Guard simple ----
if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")


# ---- Helpers ----
def _load_logo_b64(*paths: str) -> str:
    for path in paths:
        if not path:
            continue
        try:
            with open(path, "rb") as fh:
                return base64.b64encode(fh.read()).decode()
        except Exception:
            continue
    return ""


def _format_money(value: float) -> str:
    return f"${value:,.2f}"


SHEET_NAME_COT = "cotizaciones"
COT_COLUMNS = [
    "id",
    "numero_cotizacion",
    "prefijo",
    "secuencia",
    "empresa",
    "cliente_nombre",
    "cliente_direccion",
    "fecha_cotizacion",
    "created_at",
    "updated_at",
    "moneda",
    "subtotal",
    "impuesto_pct",
    "impuesto_monto",
    "total",
    "items_json",
    "items_resumen",
    "condiciones_json",
    "vigencia",
    "forma_pago",
    "entrega",
    "estado",
    "notas",
    "drive_file_id",
    "drive_file_name",
    "drive_file_url",
    "drive_folder",
]
COT_PREFIX = {
    "RS Engineering": "RS",
    "RIR Medical": "RIR",
}


def _ensure_cotizaciones_sheet(client, sheet_id: str) -> None:
    sh = client.open_by_key(sheet_id)
    try:
        sh.worksheet(SHEET_NAME_COT)
        return
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME_COT, rows=1000, cols=len(COT_COLUMNS))
        ws.update("A1", [COT_COLUMNS])


@st.cache_data(show_spinner=False)
def _load_cotizaciones_cached(sheet_id: str, cache_token: str) -> pd.DataFrame:
    client, _ = get_client()
    _ensure_cotizaciones_sheet(client, sheet_id)
    df = read_worksheet(client, sheet_id, SHEET_NAME_COT)
    return df


def _normalize_cotizaciones_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in COT_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[COT_COLUMNS]
    for col in ("subtotal", "impuesto_pct", "impuesto_monto", "total"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _next_sequence(df: pd.DataFrame, prefijo: str) -> int:
    if df.empty:
        return 1
    seq = pd.to_numeric(df.loc[df["prefijo"] == prefijo, "secuencia"], errors="coerce")
    if seq.dropna().empty:
        return 1
    return int(seq.max()) + 1


def _build_numero_cot(prefijo: str, secuencia: int) -> str:
    return f"COT-{prefijo}-{secuencia:04d}"


def _get_drive_client(creds):
    return build("drive", "v3", credentials=creds)


def _find_or_create_folder(drive, name: str, parent_id: Optional[str] = None) -> str:
    query = ["mimeType='application/vnd.google-apps.folder'", "trashed=false", f"name='{name}'"]
    if parent_id:
        query.append(f"'{parent_id}' in parents")
    resp = drive.files().list(q=" and ".join(query), fields="files(id,name)").execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        metadata["parents"] = [parent_id]
    created = drive.files().create(body=metadata, fields="id").execute()
    return created["id"]


def _get_drive_folders(drive) -> tuple[str, Dict[str, str]]:
    base_id = st.secrets.get("app", {}).get("DRIVE_COTIZACIONES_FOLDER_ID")
    if not base_id:
        base_id = _find_or_create_folder(drive, "GEAPP Cotizaciones")
    subfolders = {
        "RS Engineering": _find_or_create_folder(drive, "RS", base_id),
        "RIR Medical": _find_or_create_folder(drive, "RIR", base_id),
    }
    return base_id, subfolders


def _upload_quote_html(
    drive,
    folder_id: str,
    filename: str,
    html_body: str,
    existing_file_id: str | None = None,
) -> dict:
    media = MediaIoBaseUpload(BytesIO(html_body.encode("utf-8")), mimetype="text/html", resumable=False)
    if existing_file_id:
        return drive.files().update(fileId=existing_file_id, media_body=media, fields="id,name").execute()
    metadata = {"name": filename, "parents": [folder_id]}
    return drive.files().create(body=metadata, media_body=media, fields="id,name").execute()


def _download_drive_file(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def _items_resumen(items_df: pd.DataFrame) -> str:
    if items_df.empty:
        return ""
    first = str(items_df.iloc[0].get("producto_servicio", "") or "").strip()
    restantes = max(len(items_df) - 1, 0)
    if restantes:
        return f"{first} (+{restantes} más)"
    return first
def _build_items_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    if "cantidad" in df.columns:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0.0)
    if "precio_unitario" in df.columns:
        df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce").fillna(0.0)
    df["importe"] = df["cantidad"] * df["precio_unitario"]
    return df


def _build_invoice_html(
    empresa: str,
    branding: Dict[str, str],
    numero: str,
    fecha_cot: date,
    cliente: str,
    direccion: str,
    items: pd.DataFrame,
    impuesto_pct: float,
    condiciones: Dict[str, str],
) -> str:
    logo_b64 = branding.get("logo_b64", "")
    background_b64 = branding.get("background_b64", "")
    contacto_html = branding.get("contacto_html", "")
    logo_scale = float(branding.get("logo_scale", 1.0))
    logo_box_width = int(branding.get("logo_box_width", branding.get("logo_box", 190)) * logo_scale)
    logo_box_height = int(branding.get("logo_box_height", branding.get("logo_box", 190)) * logo_scale)
    logo_width = int(branding.get("logo_width", branding.get("logo_size", 180)) * logo_scale)
    logo_height = int(branding.get("logo_height", branding.get("logo_size", 180)) * logo_scale)
    logo_left = int(branding.get("logo_left", 120))
    logo_top = int(branding.get("logo_top", 120))
    header_left = int(branding.get("header_left", logo_left + logo_box_width + 30))
    header_top = int(branding.get("header_top", 140))
    header_width = int(branding.get("header_width", 520))
    header_height = int(branding.get("header_height", logo_box_height))
    content_offset_x = int(branding.get("content_offset_x", 0))
    content_offset_y = int(branding.get("content_offset_y", 0))

    title_top = 380 + content_offset_y
    title_left = 120 + content_offset_x
    title_meta_top = 440 + content_offset_y
    title_meta_left = 120 + content_offset_x
    columns_top = 520 + content_offset_y
    columns_left = 120 + content_offset_x
    table_top = 720 + content_offset_y
    table_left = 120 + content_offset_x
    totals_top = 1180 + content_offset_y
    totals_right = 160 - content_offset_x
    conditions_top = 1340 + content_offset_y
    conditions_left = 120 + content_offset_x

    subtotal = float(items['importe'].sum())
    impuesto = subtotal * (impuesto_pct / 100.0)
    total = subtotal + impuesto

    rows: List[str] = []
    for _, row in items.iterrows():
        rows.append(
            f"""
            <tr>
              <td>{html.escape(str(row.get('producto_servicio', '') or ''))}</td>
              <td class="num">{row.get('cantidad', 0):,.0f}</td>
              <td class="num">{_format_money(row.get('importe', 0))}</td>
            </tr>
            """
        )

    sample_rows = ''.join(rows) or """
        <tr>
            <td colspan="3" style="text-align:center;color:#64748b;">Agrega ítems para ver el desglose.</td>
        </tr>
    """

    condiciones_html = ''.join(
        f"<li><strong>{html.escape(label)}:</strong> {html.escape(text)}</li>"
        for label, text in condiciones.items()
    )

    return f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&family=Manrope:wght@400;600;700;800&display=swap');
  .quote-page {{
    position: relative;
    width: 1414px;
    height: 2000px;
    margin: 0 auto 24px auto;
    background: url('data:image/png;base64,{background_b64}') center top / cover no-repeat;
    font-family: 'Manrope', 'Inter', 'Segoe UI', sans-serif;
    color: #0c2349;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }}
  .logo {{
    position: absolute;
    top: 120px;
    left: 120px;
    width: 190px;
    height: 190px;
    display: flex;
    align-items: center;
    justify-content: center;
  }}
  .logo img {{
    width: 180px;
    height: 180px;
    object-fit: contain;
  }}
  .header-info {{
    position: absolute;
    top: 140px;
    left: 340px;
    width: 520px;
    color: #6b7280;
    line-height: 1.35;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }}
  .header-info .empresa {{
    font-size: 28px;
    font-weight: 800;
    color: #4b5563;
    margin: 0 0 8px 0;
  }}
  .header-info .datos {{
    font-size: 16px;
    color: #6b7280;
  }}
  .title {{
    position: absolute;
    top: 380px;
    left: 120px;
    font-size: 40px;
    font-weight: 800;
  }}
  .title-meta {{
    position: absolute;
    top: 440px;
    left: 120px;
    font-size: 16px;
    color: #6b7280;
    line-height: 1.4;
  }}
  .columns {{
    position: absolute;
    top: 520px;
    left: 120px;
    right: 120px;
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 70px;
    font-size: 16px;
    line-height: 1.45;
  }}
  .columns h4 {{
    margin: 0 0 10px 0;
    font-size: 17px;
    color: #0c2349;
  }}
  .columns .block div {{
    margin: 0 0 6px 0;
    color: #1f2f46;
  }}
  .columns .emisor-name {{
    margin-bottom: 6px;
  }}
  .columns .contacto {{
    color: #1f2f46;
  }}
  .table-wrap {{
    position: absolute;
    top: 720px;
    left: 120px;
    width: 1174px;
  }}
  table.items {{
    width: 100%;
    border-collapse: collapse;
    font-size: 15px;
    color: #0c2349;
  }}
  table.items th {{
    background: #1c336a;
    color: #ffffff;
    padding: 12px 10px;
    text-align: left;
    border: 2px solid #1c336a;
    font-weight: 700;
  }}
  table.items td {{
    border: 1px solid #d9e2f1;
    padding: 12px 10px;
    vertical-align: top;
  }}
  table.items td.num {{
    text-align: center;
    white-space: nowrap;
  }}
  .totals {{
    position: absolute;
    top: 1180px;
    right: 160px;
    width: 320px;
    font-size: 16px;
    line-height: 1.6;
  }}
  .totals div {{
    display: flex;
    justify-content: space-between;
  }}
  .totals .total {{
    font-weight: 800;
    font-size: 20px;
  }}
  .conditions {{
    position: absolute;
    top: 1340px;
    left: 120px;
    width: 1174px;
    font-size: 15px;
    line-height: 1.45;
    color: #0c2349;
  }}
  .conditions h4 {{
    margin: 0 0 12px 0;
    font-size: 16px;
    font-weight: 800;
  }}
  .conditions ul {{
    margin: 0;
    padding-left: 18px;
    list-style: none;
  }}
  .conditions li::before {{
    content: "• ";
    color: #0c2349;
  }}
</style>
<div class="quote-page" id="quote-root">
  <div class="logo" style="left:{logo_left}px;top:{logo_top}px;width:{logo_box_width}px;height:{logo_box_height}px;">
    {"<img src='data:image/png;base64," + logo_b64 + "' alt='logo' style='width:" + str(logo_width) + "px;height:" + str(logo_height) + "px;' />" if logo_b64 else ""}
  </div>
  <div class="header-info" style="left:{header_left}px;top:{header_top}px;width:{header_width}px;height:{header_height}px;">
    <div class="empresa">{html.escape(empresa)}</div>
    <div class="datos">{contacto_html}</div>
  </div>
  <div class="title" style="top:{title_top}px;left:{title_left}px;">Cotización</div>
  <div class="title-meta" style="top:{title_meta_top}px;left:{title_meta_left}px;">N.º cotización: <strong>{html.escape(numero)}</strong><br>Fecha: {fecha_cot.strftime('%Y-%m-%d')}</div>
  <div class="columns" style="top:{columns_top}px;left:{columns_left}px;">
    <div class="block">
      <h4>Datos del Cliente</h4>
      <div>{html.escape(cliente or '-')}</div>
      <div>{html.escape(direccion or '-')}</div>
    </div>
    <div class="block">
      <h4>Datos del Emisor</h4>
      <div class="emisor-name">{html.escape(empresa)}</div>
      {"<div class=\"contacto\">" + contacto_html + "</div>" if contacto_html else ""}
    </div>
  </div>
  <div class="table-wrap" style="top:{table_top}px;left:{table_left}px;">
    <table class="items">
      <thead>
        <tr>
          <th>Producto</th>
          <th style="width:140px;">Cantidad</th>
          <th style="width:200px;">Precio</th>
        </tr>
      </thead>
      <tbody>
        {sample_rows}
      </tbody>
    </table>
  </div>
  <div class="totals" style="top:{totals_top}px;right:{totals_right}px;">
    <div><span>Subtotal</span><span>{_format_money(subtotal)}</span></div>
    <div><span>Impuestos ({impuesto_pct:.2f}%)</span><span>{_format_money(impuesto)}</span></div>
    <div class="total"><span>TOTAL</span><span>{_format_money(total)}</span></div>
  </div>
  <div class="conditions" style="top:{conditions_top}px;left:{conditions_left}px;">
    <h4>CONDICIONES</h4>
    <ul>
      {condiciones_html}
    </ul>
  </div>
</div>
    """

def _render_pdf_component(html_body: str, filename: str, preview_scale: float = 0.75) -> None:
    """Renderiza la vista previa y un botón JS para exportar a PDF usando html2canvas + jsPDF."""
    preview_height = min(int(2000 * preview_scale + 220), 2400)
    component_html = f"""
    <style>
      .preview-shell {{
        width: 100%;
        display: flex;
        justify-content: center;
        overflow: auto;
      }}
      .preview-scale {{
        display: inline-block;
        transform: scale({preview_scale});
        transform-origin: top center;
      }}
    </style>
    <div class="preview-shell">
      <div class="preview-scale">{html_body}</div>
    </div>
    <div style="margin: 10px 0 16px 0;">
      <button id="btn-download" style="
        background: linear-gradient(135deg, #2563eb, #22c55e);
        color: white; border: none; padding: 10px 14px; border-radius: 10px;
        font-weight: 700; cursor: pointer; box-shadow: 0 8px 24px rgba(34,197,94,0.25);
      ">Descargar PDF</button>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js"></script>
    <script>
      const btn = document.getElementById("btn-download");
      btn?.addEventListener("click", () => {{
        const root = document.getElementById("quote-root");
        if (!root) return;

        const render = () => {{
          html2canvas(root, {{ scale: 2, useCORS: true, backgroundColor: "#ffffff" }}).then(canvas => {{
            const imgData = canvas.toDataURL("image/png");
            const pdf = new jspdf.jsPDF("p", "pt", "a4");
            const pageWidth = pdf.internal.pageSize.getWidth();
            const pageHeight = pdf.internal.pageSize.getHeight();
            const ratio = Math.min(pageWidth / canvas.width, pageHeight / canvas.height);
            const imgWidth = canvas.width * ratio;
            const imgHeight = canvas.height * ratio;
            const marginX = (pageWidth - imgWidth) / 2;
            const marginY = (pageHeight - imgHeight) / 2;
            pdf.addImage(imgData, "PNG", marginX, marginY, imgWidth, imgHeight);
            pdf.save("{filename}");
          }});
        }};

        if (document.fonts && document.fonts.ready) {{
          document.fonts.ready.then(render);
        }} else {{
          render();
        }}
      }});
    </script>
    """
    components.html(component_html, height=preview_height, scrolling=True)


# ---- Configuración de empresas (membrete) ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(os.path.dirname(BASE_DIR), "assets")

# Prefer paths proporcionados, luego assets de respaldo
RS_LOGO_PATH = os.path.join(ASSETS_DIR, "Logo RS Engineering.png")
RIR_LOGO_PATH = os.path.join(ASSETS_DIR, "Logo RIR Medical.png")
RS_LOGO_FALLBACK = os.path.join(ASSETS_DIR, "rs.png.png")
RIR_LOGO_FALLBACK = os.path.join(ASSETS_DIR, "rir.png.png")
BACKGROUND_PATH = os.path.join(ASSETS_DIR, "Fondo.png")
BACKGROUND_B64 = _load_logo_b64(BACKGROUND_PATH)
COMPANIES = {
    "RS Engineering": {
        "color": "#0f172a",
        "accent": "#0e4aa0",
        "logo_b64": _load_logo_b64(RS_LOGO_PATH, RS_LOGO_FALLBACK),
        "background_b64": BACKGROUND_B64,
        "logo_box_width": 440,
        "logo_box_height": 440,
        "logo_width": 420,
        "logo_height": 420,
        "logo_left": 20,
        "logo_top": 80,
        "header_left": 430,
        "header_top": 80,
        "header_height": 440,
        "content_offset_y": 200,
        "contacto_html": """<div style='text-align:left; line-height:1.35;'>
        R.U.C. 9-740-624 / DV: 80<br>
        PH Bonanza Plaza, Bella Vista<br>
        TELÉFONO: +507 68475616<br>
        EMAIL: rodrigojesus-@hotmail.com
        </div>""",
    },
    "RIR Medical": {
        "color": "#1d4ed8",
        "accent": "#22c55e",
        "logo_b64": _load_logo_b64(RIR_LOGO_PATH, RIR_LOGO_FALLBACK),
        "background_b64": BACKGROUND_B64,
        "logo_box_width": 320,
        "logo_box_height": 170,
        "logo_width": 310,
        "logo_height": 166,
        "logo_left": 90,
        "logo_top": 100,
        "header_left": 430,
        "header_top": 100,
        "header_height": 170,
        "content_offset_y": 160,
        "contacto_html": """<div style='text-align:left; line-height:1.35;'>
        RUC: 155750585-2-2024 DV40<br>
        PH Bonanza Plaza, Bella Vista<br>
        TELÉFONO: +507 68475616<br>
        Email: info@rirmedical.com
        </div>""",
    },
}


# ---- UI principal ----
st.title("Generador de cotizaciones")

sheet_id = st.secrets.get("app", {}).get("SHEET_ID")
sheet_error = None
cot_df = pd.DataFrame(columns=COT_COLUMNS)
client = None
creds = None
if sheet_id:
    try:
        client, creds = get_client()
        if "cotizaciones_cache_token" not in st.session_state:
            st.session_state["cotizaciones_cache_token"] = uuid.uuid4().hex
        token = st.session_state["cotizaciones_cache_token"]
        cot_df = _normalize_cotizaciones_df(_load_cotizaciones_cached(sheet_id, token))
    except Exception as exc:
        sheet_error = str(exc)
else:
    sheet_error = "No hay SHEET_ID configurado en st.secrets['app']."

EDIT_KEY = "cotizacion_edit"
if EDIT_KEY not in st.session_state:
    st.session_state[EDIT_KEY] = None

items_state_key = "cotizacion_privada_items_data"


def _apply_edit_state(row: dict) -> None:
    st.session_state[EDIT_KEY] = row
    st.session_state["cot_empresa"] = row.get("empresa") or "RS Engineering"
    st.session_state["cot_cliente"] = row.get("cliente_nombre", "")
    st.session_state["cot_direccion"] = row.get("cliente_direccion", "")
    st.session_state["cot_numero"] = row.get("numero_cotizacion", "")

    fecha_val = row.get("fecha_cotizacion") or ""
    fecha_dt = None
    if isinstance(fecha_val, str) and fecha_val:
        try:
            fecha_dt = datetime.fromisoformat(fecha_val).date()
        except ValueError:
            fecha_dt = None
    st.session_state["cot_fecha"] = fecha_dt or date.today()

    try:
        items = json.loads(row.get("items_json") or "[]")
        if not isinstance(items, list):
            items = []
    except Exception:
        items = []
    if not items:
        items = [{"producto_servicio": "Producto o servicio", "cantidad": 1, "precio_unitario": 100.0}]
    st.session_state[items_state_key] = items

    try:
        condiciones = json.loads(row.get("condiciones_json") or "{}")
    except Exception:
        condiciones = {}

    st.session_state["cot_vigencia"] = condiciones.get("Vigencia") or row.get("vigencia") or "15 días"
    st.session_state["cot_forma_pago"] = condiciones.get("Forma de pago") or row.get("forma_pago") or "Transferencia bancaria"
    st.session_state["cot_entrega"] = condiciones.get("Entrega") or row.get("entrega") or "15 días hábiles"

    impuesto_val = row.get("impuesto_pct")
    try:
        impuesto_val = float(impuesto_val)
    except (TypeError, ValueError):
        impuesto_val = 7.0
    st.session_state["cot_impuesto"] = impuesto_val


def _clear_edit_state() -> None:
    st.session_state[EDIT_KEY] = None


tab_panama, tab_privada, tab_historial = st.tabs(
    ["Cotización - Panamá Compra", "Cotización - Privada", "Historial de cotizaciones"]
)

with tab_panama:
    st.info("Placeholder: sección pendiente para cotizaciones de Panamá Compra.")

with tab_privada:
    if sheet_error:
        st.warning(sheet_error)

    edit_row = st.session_state.get(EDIT_KEY)
    if edit_row:
        st.info(f"Editando: {edit_row.get('numero_cotizacion', '')}")
        if st.button("Cancelar edición"):
            _clear_edit_state()
            st.rerun()

    if "cot_fecha" not in st.session_state:
        st.session_state["cot_fecha"] = date.today()
    if "cot_impuesto" not in st.session_state:
        st.session_state["cot_impuesto"] = 7.0

    st.subheader("Datos de la cotización")
    col_a, col_b, col_c = st.columns([1.2, 1, 1])
    with col_a:
        empresa = st.selectbox("Empresa", list(COMPANIES.keys()), key="cot_empresa")
        cliente = st.text_input("Nombre del cliente", key="cot_cliente")
        direccion = st.text_area("Dirección del cliente", height=70, key="cot_direccion")
    with col_b:
        prefijo = COT_PREFIX.get(empresa, "GEN")
        seq = _next_sequence(cot_df, prefijo)
        numero_auto = _build_numero_cot(prefijo, seq)
        if edit_row:
            numero_auto = edit_row.get("numero_cotizacion") or numero_auto
        if not edit_row:
            if st.session_state.get("cot_numero_pref") != prefijo:
                st.session_state["cot_numero"] = numero_auto
                st.session_state["cot_numero_pref"] = prefijo
        numero_cot = st.text_input("Número de cotización", key="cot_numero", disabled=True)
        fecha_cot = st.date_input("Fecha", key="cot_fecha")
        impuesto_pct = st.number_input("Impuesto (%)", min_value=0.0, max_value=25.0, step=0.5, key="cot_impuesto")
    with col_c:
        vigencia = st.text_input("Vigencia de la oferta", value="15 días", key="cot_vigencia")
        forma_pago = st.text_input("Forma de pago", value="Transferencia bancaria", key="cot_forma_pago")
        entrega = st.text_input("Entrega", value="15 días hábiles", key="cot_entrega")

    st.markdown("### Ítems de la cotización")
    if items_state_key not in st.session_state:
        st.session_state[items_state_key] = [
            {"producto_servicio": "Producto o servicio", "cantidad": 1, "precio_unitario": 100.0},
        ]

    items_raw = st.data_editor(
        pd.DataFrame(st.session_state[items_state_key]),
        num_rows="dynamic",
        use_container_width=True,
        key="cotizacion_privada_items",
        column_config={
            "producto_servicio": st.column_config.TextColumn("Producto / Servicio", width="large", required=True),
            "cantidad": st.column_config.NumberColumn("Cantidad", min_value=0.0, step=1.0, required=True),
            "precio_unitario": st.column_config.NumberColumn(
                "Precio unitario", min_value=0.0, step=10.0, format="$%0.2f", required=True
            ),
        },
        hide_index=True,
    )

    items_df = _build_items_dataframe(pd.DataFrame(items_raw))
    st.session_state[items_state_key] = items_df[
        ["producto_servicio", "cantidad", "precio_unitario"]
    ].to_dict(orient="records")
    subtotal = float(items_df["importe"].sum())
    impuesto_valor = subtotal * (float(impuesto_pct) / 100.0)
    total = subtotal + impuesto_valor

    st.markdown(
        f"**Resumen:** Subtotal {_format_money(subtotal)} | Impuesto ({impuesto_pct:.2f}%) {_format_money(impuesto_valor)} | Total {_format_money(total)}"
    )

    st.markdown("### Vista previa")
    preview_scale = st.slider(
        "Zoom de vista previa",
        min_value=0.5,
        max_value=1.1,
        value=0.7,
        step=0.05,
    )
    condiciones = {
        "Vigencia": vigencia or "—",
        "Forma de pago": forma_pago or "—",
        "Entrega": entrega or "—",
    }

    html_body = _build_invoice_html(
        empresa=empresa,
        branding=COMPANIES[empresa],
        numero=numero_cot,
        fecha_cot=fecha_cot,
        cliente=cliente,
        direccion=direccion,
        items=items_df,
        impuesto_pct=impuesto_pct,
        condiciones=condiciones,
    )

    _render_pdf_component(
        html_body,
        filename=f"{empresa.replace(' ', '_')}_{numero_cot}.pdf",
        preview_scale=preview_scale,
    )

    if st.button("Guardar cotización en Sheets/Drive"):
        if sheet_error or not sheet_id:
            st.error("No hay conexión a Google Sheets para guardar la cotización.")
        else:
            try:
                if client is None or creds is None:
                    client, creds = get_client()
                _ensure_cotizaciones_sheet(client, sheet_id)
                df_write = _normalize_cotizaciones_df(read_worksheet(client, sheet_id, SHEET_NAME_COT))

                now = datetime.now().isoformat(timespec="seconds")
                row_id = edit_row.get("id") if edit_row else uuid.uuid4().hex
                created_at = edit_row.get("created_at") if edit_row else now

                items_json = json.dumps(items_df.to_dict(orient="records"), ensure_ascii=False)
                condiciones_json = json.dumps(condiciones, ensure_ascii=False)

                drive_file_id = edit_row.get("drive_file_id") if edit_row else ""
                drive_file_name = edit_row.get("drive_file_name") if edit_row else ""
                drive_file_url = edit_row.get("drive_file_url") if edit_row else ""
                drive_folder = edit_row.get("drive_folder") if edit_row else ""

                if creds is not None:
                    drive = _get_drive_client(creds)
                    _, folders = _get_drive_folders(drive)
                    folder_id = folders.get(empresa)
                    if folder_id:
                        filename = f"{numero_cot}.html"
                        upload = _upload_quote_html(
                            drive,
                            folder_id,
                            filename,
                            html_body,
                            existing_file_id=drive_file_id or None,
                        )
                        drive_file_id = upload.get("id", drive_file_id)
                        drive_file_name = upload.get("name", filename)
                        drive_folder = folder_id
                        if drive_file_id:
                            drive_file_url = f"https://drive.google.com/file/d/{drive_file_id}/view"

                row = {
                    "id": row_id,
                    "numero_cotizacion": numero_cot,
                    "prefijo": prefijo,
                    "secuencia": seq,
                    "empresa": empresa,
                    "cliente_nombre": cliente,
                    "cliente_direccion": direccion,
                    "fecha_cotizacion": fecha_cot.isoformat(),
                    "created_at": created_at,
                    "updated_at": now,
                    "moneda": "USD",
                    "subtotal": subtotal,
                    "impuesto_pct": impuesto_pct,
                    "impuesto_monto": impuesto_valor,
                    "total": total,
                    "items_json": items_json,
                    "items_resumen": _items_resumen(items_df),
                    "condiciones_json": condiciones_json,
                    "vigencia": vigencia,
                    "forma_pago": forma_pago,
                    "entrega": entrega,
                    "estado": edit_row.get("estado", "vigente") if edit_row else "vigente",
                    "notas": edit_row.get("notas", "") if edit_row else "",
                    "drive_file_id": drive_file_id,
                    "drive_file_name": drive_file_name,
                    "drive_file_url": drive_file_url,
                    "drive_folder": drive_folder,
                }

                if edit_row and row_id in df_write["id"].values:
                    idx = df_write.index[df_write["id"] == row_id][0]
                    for col in COT_COLUMNS:
                        df_write.at[idx, col] = row.get(col, "")
                else:
                    df_write = pd.concat([df_write, pd.DataFrame([row])], ignore_index=True)

                write_worksheet(client, sheet_id, SHEET_NAME_COT, df_write)
                st.session_state["cotizaciones_cache_token"] = uuid.uuid4().hex
                _clear_edit_state()
                st.success("Cotización guardada correctamente.")
            except Exception as exc:
                st.error(f"No se pudo guardar la cotización: {exc}")

with tab_historial:
    if sheet_error:
        st.warning(sheet_error)
    else:
        if cot_df.empty:
            st.info("Aún no hay cotizaciones registradas.")
        else:
            display_cols = [
                "numero_cotizacion",
                "empresa",
                "fecha_cotizacion",
                "cliente_nombre",
                "total",
                "estado",
            ]
            st.dataframe(cot_df[display_cols], use_container_width=True)

            opciones = cot_df["id"].tolist()
            def _label(opt):
                row = cot_df[cot_df["id"] == opt].iloc[0]
                return f"{row.get('numero_cotizacion', '')} · {row.get('cliente_nombre', '')}"

            selected_id = st.selectbox("Selecciona una cotización", opciones, format_func=_label)
            sel_row = cot_df[cot_df["id"] == selected_id].iloc[0].to_dict()

            st.markdown("#### Detalle")
            st.write(
                {
                    "Número": sel_row.get("numero_cotizacion"),
                    "Empresa": sel_row.get("empresa"),
                    "Cliente": sel_row.get("cliente_nombre"),
                    "Fecha": sel_row.get("fecha_cotizacion"),
                    "Total": sel_row.get("total"),
                }
            )

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                if st.button("Cargar en formulario"):
                    _apply_edit_state(sel_row)
                    st.success("Cotización cargada en el formulario de edición.")
            with col_b:
                delete_key = f"delete_{selected_id}"
                if st.button("Eliminar"):
                    st.session_state[delete_key] = True
                if st.session_state.get(delete_key):
                    if st.button("Confirmar eliminación"):
                        try:
                            if client is None:
                                client, creds = get_client()
                            df_write = cot_df[cot_df["id"] != selected_id].copy()
                            write_worksheet(client, sheet_id, SHEET_NAME_COT, df_write)
                            if sel_row.get("drive_file_id") and creds is not None:
                                drive = _get_drive_client(creds)
                                drive.files().delete(fileId=sel_row["drive_file_id"]).execute()
                            st.session_state["cotizaciones_cache_token"] = uuid.uuid4().hex
                            st.success("Cotización eliminada.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"No se pudo eliminar: {exc}")
            with col_c:
                download_key = f"download_{selected_id}"
                if sel_row.get("drive_file_id"):
                    if st.button("Preparar descarga"):
                        try:
                            if creds is None:
                                client, creds = get_client()
                            drive = _get_drive_client(creds)
                            file_bytes = _download_drive_file(drive, sel_row["drive_file_id"])
                            st.session_state[download_key] = file_bytes
                        except Exception as exc:
                            st.error(f"No se pudo descargar: {exc}")
                    if st.session_state.get(download_key):
                        st.download_button(
                            "Descargar archivo",
                            data=st.session_state[download_key],
                            file_name=sel_row.get("drive_file_name") or f"{sel_row.get('numero_cotizacion')}.html",
                            mime="text/html",
                        )
                if sel_row.get("drive_file_url"):
                    st.link_button("Abrir en Drive", sel_row["drive_file_url"])
