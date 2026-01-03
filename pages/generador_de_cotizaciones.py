from __future__ import annotations

import base64
import html
import os
from datetime import date
from typing import Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
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
    color: #f8fafc;
    line-height: 1.35;
    text-shadow: 0 1px 2px rgba(0,0,0,0.35);
  }}
  .header-info .empresa {{
    font-size: 28px;
    font-weight: 800;
    margin: 0 0 8px 0;
  }}
  .header-info .datos {{
    font-size: 16px;
    color: #f8fafc;
  }}
  .header-info .meta {{
    margin-top: 10px;
    font-size: 16px;
    color: #f8fafc;
  }}
  .title {{
    position: absolute;
    top: 380px;
    left: 120px;
    font-size: 40px;
    font-weight: 800;
  }}
  .columns {{
    position: absolute;
    top: 480px;
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
  <div class="logo">
    {"<img src='data:image/png;base64," + logo_b64 + "' alt='logo' />" if logo_b64 else ""}
  </div>
  <div class="header-info">
    <div class="empresa">{html.escape(empresa)}</div>
    <div class="datos">{contacto_html}</div>
    <div class="meta">N.º cotización: <strong>{html.escape(numero)}</strong><br>Fecha: {fecha_cot.strftime('%Y-%m-%d')}</div>
  </div>
  <div class="title">Cotización</div>
  <div class="columns">
    <div class="block">
      <h4>Datos del Cliente</h4>
      <div>{html.escape(cliente or '-')}</div>
      <div>{html.escape(direccion or '-')}</div>
    </div>
    <div class="block">
      <h4>Datos del Emisor</h4>
      <div>{html.escape(empresa)}</div>
      {"<div>" + contacto_html + "</div>" if contacto_html else ""}
    </div>
  </div>
  <div class="table-wrap">
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
  <div class="totals">
    <div><span>Subtotal</span><span>{_format_money(subtotal)}</span></div>
    <div><span>Impuestos ({impuesto_pct:.2f}%)</span><span>{_format_money(impuesto)}</span></div>
    <div class="total"><span>TOTAL</span><span>{_format_money(total)}</span></div>
  </div>
  <div class="conditions">
    <h4>CONDICIONES</h4>
    <ul>
      {condiciones_html}
    </ul>
  </div>
</div>
    """

def _render_pdf_component(html_body: str, filename: str) -> None:
    """Renderiza la vista previa y un botón JS para exportar a PDF usando html2canvas + jsPDF."""
    component_html = f"""
    <div id="invoice-container">{html_body}</div>
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
        html2canvas(root, {{ scale: 2, useCORS: true }}).then(canvas => {{
          const imgData = canvas.toDataURL("image/png");
          const pdf = new jspdf.jsPDF("p", "pt", "a4");
          const pageWidth = pdf.internal.pageSize.getWidth();
          const pageHeight = pdf.internal.pageSize.getHeight();
          const ratio = Math.min(pageWidth / canvas.width, pageHeight / canvas.height);
          const imgWidth = canvas.width * ratio;
          const imgHeight = canvas.height * ratio;
          const marginX = (pageWidth - imgWidth) / 2;
          const marginY = 24;
          pdf.addImage(imgData, "PNG", marginX, marginY, imgWidth, imgHeight);
          pdf.save("{filename}");
        }});
      }});
    </script>
    """
    components.html(component_html, height=980, scrolling=True)


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
        "contacto_html": """<div style='text-align:left; line-height:1.35;'>
        R.U.C. 9-740-624 / DV: 80<br>
        PH Bonanza Plaza, Bella Vista<br>
        TELÉFONO: +507 68475616<br>
        EMAIL: RODRIGOSJESUS@HOTMAIL.COM
        </div>""",
    },
    "RIR Medical": {
        "color": "#1d4ed8",
        "accent": "#22c55e",
        "logo_b64": _load_logo_b64(RIR_LOGO_PATH, RIR_LOGO_FALLBACK),
        "background_b64": BACKGROUND_B64,
        "contacto_html": "",
    },
}

# ---- UI principal ----
st.title("Generador de cotizaciones")

with st.expander("Cotización - Panamá Compra", expanded=False):
    st.info("Placeholder: sección pendiente para cotizaciones de Panamá Compra.")


with st.expander("Cotización - Privada", expanded=False):
    st.subheader("Datos de la cotización")
    col_a, col_b, col_c = st.columns([1.2, 1, 1])
    with col_a:
        empresa = st.selectbox("Empresa", list(COMPANIES.keys()), index=0)
        cliente = st.text_input("Nombre del cliente")
        direccion = st.text_area("Dirección del cliente", height=70)
    with col_b:
        numero_cot = st.text_input("Número de cotización", value="COT-001")
        fecha_cot = st.date_input("Fecha", value=date.today())
        impuesto_pct = st.number_input("Impuesto (%)", min_value=0.0, max_value=25.0, value=7.0, step=0.5)
    with col_c:
        vigencia = st.text_input("Vigencia de la oferta", value="15 días")
        forma_pago = st.text_input("Forma de pago", value="Transferencia bancaria")
        entrega = st.text_input("Entrega", value="15 días hábiles")

    st.markdown("### Ítems de la cotización")
    items_state_key = "cotizacion_privada_items_data"
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
    impuesto_valor = subtotal * (impuesto_pct / 100.0)
    total = subtotal + impuesto_valor

    st.markdown(
        f"""
        **Resumen:** Subtotal {_format_money(subtotal)} | Impuesto ({impuesto_pct:.2f}%) {_format_money(impuesto_valor)} | Total {_format_money(total)}
        """
    )

    st.markdown("### Vista previa")
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

    _render_pdf_component(html_body, filename=f"{empresa.replace(' ', '_')}_{numero_cot}.pdf")


