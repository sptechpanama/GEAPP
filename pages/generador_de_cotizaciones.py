from __future__ import annotations

import base64
import html
import os
from datetime import date
from typing import Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Generador de cotizaciones", page_icon="🧾", layout="wide")


def _apply_visual_theme() -> None:
    """Tema oscuro/gradiente similar al usado en finanzas, sin tocar layout."""
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&display=swap');
:root {
  --pc-bg: #0b1224;
  --pc-surface: #0f172a;
  --pc-border: rgba(255,255,255,0.08);
  --pc-accent: #22c55e;
  --pc-accent-2: #0ea5e9;
  --pc-text: #e7edf7;
  --pc-muted: #9fb2c7;
}
.stApp {
  background: radial-gradient(140% 120% at 18% 10%, #1c3d7133 0%, transparent 40%),
              radial-gradient(120% 120% at 80% 0%, #0ea5e926 0%, transparent 45%),
              linear-gradient(125deg, #0b1224 0%, #0c1a30 45%, #10223f 100%);
  color: var(--pc-text);
  font-family: 'Manrope', system-ui, -apple-system, sans-serif;
}
.block-container { padding-top: 1.25rem; max-width: 1200px; }
h1, h2, h3, h4 { color: var(--pc-text); letter-spacing: -0.015em; }
label { color: #cdd6e5 !important; font-weight: 600; }
[data-testid="stMarkdown"] a { color: var(--pc-accent-2); text-decoration: none; }
[data-testid="stMarkdown"] a:hover { text-decoration: underline; }

div.stButton>button,
[data-testid="stForm"] button,
[data-testid="stFormSubmitButton"] button {
  background: linear-gradient(135deg, var(--pc-accent-2), var(--pc-accent));
  color: #f8fbff; border: 1px solid rgba(255,255,255,0.15);
  border-radius: 10px; padding: 0.45rem 0.85rem; font-weight: 700;
  box-shadow: 0 8px 24px rgba(14,165,233,0.18);
}
div.stButton>button:hover,
[data-testid="stForm"] button:hover,
[data-testid="stFormSubmitButton"] button:hover {
  transform: translateY(-1px);
  box-shadow: 0 12px 30px rgba(34,197,94,0.28);
}

div[data-testid="stExpander"] { background: rgba(255,255,255,0.04); border: 1px solid var(--pc-border); border-radius: 14px; }
div[data-testid="stExpander"] summary { color: var(--pc-text); font-weight: 700; }
div[data-testid="stExpander"] > details { background: var(--pc-surface); border-radius: 12px; overflow: hidden; border: 1px solid var(--pc-border); }
div[data-testid="stExpander"] > details > summary { background: linear-gradient(120deg, rgba(14,165,233,0.12), rgba(34,197,94,0.10)); color: var(--pc-text); padding: 10px 14px; border-bottom: 1px solid var(--pc-border); }
div[data-testid="stExpander"] > details[open] > summary { background: linear-gradient(120deg, rgba(14,165,233,0.16), rgba(34,197,94,0.14)); }
div[data-testid="stExpander"] > details > div[role="group"] { background: #0c1528; padding: 12px 14px 16px; }

.stTextInput>div>div>input, .stTextArea textarea, [data-baseweb="select"]>div {
  background: #0f172a; color: var(--pc-text); border: 1px solid var(--pc-border); border-radius: 10px;
  box-shadow: inset 0 0 0 1px rgba(14,165,233,0.08);
}
.stDataFrame, [data-testid="stDataEditor"] { background: rgba(15,23,42,0.45); border: 1px solid var(--pc-border); border-radius: 12px; }
.stDataFrame table, .stDataFrame tbody tr, .stDataFrame tbody td { background: transparent !important; color: #e4e9f3; }
.stDataFrame tbody tr:nth-child(odd) { background: rgba(255,255,255,0.02); }
.stDataFrame tbody tr:hover { background: rgba(14,165,233,0.08); }
</style>
""",
        unsafe_allow_html=True,
    )


_apply_visual_theme()

# ---- Guard simple ----
if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")


# ---- Helpers ----
def _load_logo_b64(path: str) -> str:
    try:
        with open(path, "rb") as fh:
            return base64.b64encode(fh.read()).decode()
    except Exception:
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
    color = branding.get("color", "#1e3a8a")
    acento = branding.get("accent", "#0ea5e9")

    subtotal = float(items["importe"].sum())
    impuesto = subtotal * (impuesto_pct / 100.0)
    total = subtotal + impuesto

    rows: List[str] = []
    for _, row in items.iterrows():
        rows.append(
            f"""
            <tr>
                <td>{html.escape(str(row.get('producto_servicio', '') or ''))}</td>
                <td class="num">{row.get('cantidad', 0):,.2f}</td>
                <td class="num">{_format_money(row.get('precio_unitario', 0))}</td>
                <td class="num">{_format_money(row.get('importe', 0))}</td>
            </tr>
            """
        )

    sample_rows = "".join(rows) or """
        <tr>
            <td colspan="4" style="text-align:center;color:#64748b;">Agrega Ítems para ver el desglose.</td>
        </tr>
    """

    condiciones_html = "".join(
        f"<li><strong>{html.escape(label)}:</strong> {html.escape(text)}</li>"
        for label, text in condiciones.items()
    )

    return f"""
<style>
  .quote-wrapper {{
    width: 900px;
    margin: 0 auto 24px auto;
    background: radial-gradient(140% 120% at 20% 10%, {color}14 0%, transparent 40%),
                radial-gradient(130% 120% at 80% -10%, {acento}1c 0%, transparent 45%),
                linear-gradient(135deg, #f8fbff 0%, #eef2ff 100%);
    padding: 28px 32px 36px 32px;
    border-radius: 18px;
    border: 1px solid #e2e8f0;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
    color: #0f172a;
    font-family: 'Manrope', system-ui, -apple-system, sans-serif;
  }}
  .quote-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }}
  .quote-header .brand h1 {{ margin: 0; font-size: 28px; letter-spacing: -0.02em; color: #0f172a; }}
  .quote-meta {{ margin-top: 6px; color: #475569; font-size: 13px; }}
  .quote-logo img {{ max-height: 70px; object-fit: contain; }}
  .quote-dates {{ background: {color}0d; border: 1px solid {color}33; padding: 10px 14px; border-radius: 12px; color: #0f172a; }}
  table.items {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
  table.items th {{ text-align: left; padding: 10px 8px; background: {color}12; color: #0f172a; font-size: 13px; border-bottom: 1px solid #cbd5e1; }}
  table.items td {{ padding: 10px 8px; border-bottom: 1px solid #e2e8f0; font-size: 13px; }}
  table.items td.num {{ text-align: right; white-space: nowrap; }}
  .totals {{ margin-top: 18px; width: 320px; margin-left: auto; font-size: 14px; color: #0f172a; }}
  .totals div {{ display: flex; justify-content: space-between; padding: 6px 0; }}
  .totals div.total {{ font-weight: 800; font-size: 16px; color: {color}; }}
  .condiciones {{ margin-top: 18px; padding: 14px 16px; border: 1px dashed {color}; border-radius: 12px; background: #fff; }}
  .condiciones h4 {{ margin: 0 0 6px 0; color: #0f172a; }}
  .condiciones ul {{ margin: 0; padding-left: 18px; color: #1f2937; }}
</style>
<div class="quote-wrapper" id="quote-root">
  <div class="quote-header">
    <div class="brand">
      <h1>{html.escape(empresa)}</h1>
      <div class="quote-meta">
        <div>N.º cotización: <strong>{html.escape(numero)}</strong></div>
        <div>Fecha: {fecha_cot.strftime("%d-%m-%Y")}</div>
      </div>
    </div>
    <div class="quote-logo">
      {"<img src='data:image/png;base64," + logo_b64 + "' />" if logo_b64 else ""}
    </div>
  </div>

  <div class="quote-dates">
    <div><strong>Cliente:</strong> {html.escape(cliente or "—")}</div>
    <div><strong>Dirección:</strong> {html.escape(direccion or "—")}</div>
  </div>

  <table class="items">
    <thead>
      <tr>
        <th>Producto / Servicio</th>
        <th>Cantidad</th>
        <th>Precio unitario</th>
        <th>Importe</th>
      </tr>
    </thead>
    <tbody>
      {sample_rows}
    </tbody>
  </table>

  <div class="totals">
    <div><span>Subtotal</span><span>{_format_money(subtotal)}</span></div>
    <div><span>Impuesto ({impuesto_pct:.2f}%)</span><span>{_format_money(impuesto)}</span></div>
    <div class="total"><span>Total</span><span>{_format_money(total)}</span></div>
  </div>

  <div class="condiciones">
    <h4>Condiciones</h4>
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

COMPANIES = {
    "RS Engineering": {
        "color": "#0f172a",
        "accent": "#0ea5e9",
        "logo_b64": _load_logo_b64(os.path.join(ASSETS_DIR, "rs.png.png")),
    },
    "RIR Medical": {
        "color": "#1d4ed8",
        "accent": "#22c55e",
        "logo_b64": _load_logo_b64(os.path.join(ASSETS_DIR, "rir.png.png")),
    },
}

# ---- UI principal ----
st.title("Generador de cotizaciones")

with st.expander("Cotizacion - Panama Compra", expanded=False):
    st.info("Placeholder: sección pendiente para cotizaciones de Panamá Compra.")


with st.expander("Cotizacion - Privada", expanded=False):
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
