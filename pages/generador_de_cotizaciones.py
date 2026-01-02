import base64
import html
import json
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

def _require_authentication() -> None:
    status = st.session_state.get("authentication_status")
    if status is True:
        return

    st.warning("Debes iniciar sesion para entrar.")
    try:
        st.switch_page("Inicio.py")
    except Exception:
        st.info("Abre la pagina de Inicio para iniciar sesion.")
    st.stop()


@st.cache_data(show_spinner=False)
def _load_logo_b64(path: Path) -> str:
    if not path.exists():
        return ""
    data = path.read_bytes()
    return base64.b64encode(data).decode("ascii")


def _safe_text(value: str) -> str:
    if value is None:
        return ""
    return html.escape(str(value)).replace("\n", "<br>")


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _money(value: float) -> str:
    return f"${value:,.2f}"


def _render_quote_preview(
    *,
    template: dict,
    client_name: str,
    client_address: str,
    quote_number: str,
    quote_date: date,
    items: pd.DataFrame,
    tax_pct: float,
    terms: dict,
    print_mode: bool = False,
) -> str:
    logo_b64 = _load_logo_b64(template["logo_path"])
    logo_html = (
        f'<img src="data:image/png;base64,{logo_b64}" alt="logo" />'
        if logo_b64
        else ""
    )
    brand_lines = [template["ruc"], template["address"], template["phone"], template["email"]]
    brand_lines = [line for line in brand_lines if line]
    brand_html = "<br>".join(_safe_text(line) for line in brand_lines)

    client_lines = [client_name, client_address]
    client_html = "<br>".join(_safe_text(line) for line in client_lines if line)

    issuer_lines = [
        template["display_name"],
        template["address"],
        template["phone"],
        template["email"],
    ]
    issuer_html = "<br>".join(_safe_text(line) for line in issuer_lines if line)

    rows_html = []
    subtotal = 0.0
    for _, row in items.iterrows():
        desc = _safe_text(row.get("Producto", ""))
        qty = _to_float(row.get("Cantidad", 0))
        unit_price = _to_float(row.get("Precio Unitario", 0))
        line_total = qty * unit_price
        subtotal += line_total
        rows_html.append(
            "<tr>"
            f"<td>{desc}</td>"
            f"<td class='center'>{qty:g}</td>"
            f"<td class='right'>{_money(line_total)}</td>"
            "</tr>"
        )

    tax_value = subtotal * (tax_pct / 100.0)
    total = subtotal + tax_value

    terms_lines = [
        terms.get("vigencia", ""),
        terms.get("pago", ""),
        terms.get("entrega", ""),
    ]
    terms_html = "<br>".join(_safe_text(line) for line in terms_lines if line)

    accent = template["accent"]
    accent_light = template["accent_light"]

    extra_css = ""
    if print_mode:
        extra_css = """
  @page { size: A4; margin: 10mm; }
  html, body {
    margin: 0;
    padding: 0;
    background: #ffffff;
  }
  * {
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }
  .quote-page {
    width: 210mm;
    min-height: 297mm;
    border: none;
    border-radius: 0;
    box-shadow: none;
  }
  @media print {
    .quote-page { box-shadow: none; border: none; }
  }
"""

    return f"""
<style>
{extra_css}
  .quote-wrap {{
    width: 100%;
  }}
  .quote-page {{
    position: relative;
    background: #ffffff;
    border: 1px solid #e4e6ef;
    border-radius: 18px;
    padding: 28px 32px 30px;
    box-shadow: 0 14px 30px rgba(17, 24, 39, 0.08);
    font-family: "Poppins", "Segoe UI", Arial, sans-serif;
    color: #1f2f6e;
    overflow: hidden;
  }}
  .quote-page > * {{
    position: relative;
    z-index: 2;
  }}
  .wave-top {{
    position: absolute;
    top: -120px;
    right: -140px;
    width: 460px;
    height: 300px;
    background: radial-gradient(circle at 20% 40%, {accent_light}, {accent});
    border-radius: 50%;
    opacity: 0.95;
    z-index: 0;
  }}
  .wave-top::after {{
    content: "";
    position: absolute;
    top: 70px;
    left: 80px;
    width: 380px;
    height: 260px;
    background: #e6f1ff;
    border-radius: 50%;
  }}
  .wave-bottom {{
    position: absolute;
    bottom: -200px;
    left: -220px;
    width: 420px;
    height: 420px;
    background: {accent};
    border-radius: 50%;
    opacity: 0.95;
    z-index: 0;
  }}
  .header {{
    display: flex;
    justify-content: space-between;
    gap: 20px;
    align-items: flex-start;
    position: relative;
    z-index: 2;
  }}
  .logo {{
    width: 110px;
    height: auto;
  }}
  .logo img {{
    width: 130px;
    height: auto;
    background: #ffffff;
    border-radius: 999px;
    padding: 6px;
  }}
  .brand-meta {{
    text-align: left;
    font-size: 0.78rem;
    color: #5a6a85;
    line-height: 1.35;
  }}
  .title {{
    margin: 30px 0 8px;
    font-size: 2.2rem;
    font-weight: 600;
  }}
  .subline {{
    font-size: 0.85rem;
    color: #5a6a85;
  }}
  .columns {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 40px;
    margin-top: 24px;
  }}
  .block-title {{
    font-size: 0.9rem;
    font-weight: 600;
    margin-bottom: 8px;
  }}
  .block-text {{
    font-size: 0.85rem;
    color: #2d3a5e;
    line-height: 1.45;
  }}
  .items {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 26px;
    font-size: 0.86rem;
  }}
  .items th {{
    background: {accent};
    color: #ffffff;
    padding: 10px;
    text-align: center;
  }}
  .items td {{
    border: 1px solid {accent};
    padding: 12px;
    vertical-align: top;
  }}
  .items td.center {{
    text-align: center;
    width: 90px;
  }}
  .items td.right {{
    text-align: right;
    width: 140px;
  }}
  .totals {{
    margin-top: 18px;
    display: flex;
    justify-content: flex-end;
    font-size: 0.9rem;
    color: #1f2f6e;
  }}
  .totals table {{
    border-collapse: collapse;
  }}
  .totals td {{
    padding: 4px 12px;
  }}
  .totals .label {{
    text-align: right;
    font-weight: 600;
  }}
  .totals .value {{
    text-align: right;
    min-width: 110px;
  }}
  .conditions {{
    margin-top: 32px;
  }}
  .conditions-title {{
    font-weight: 600;
    font-size: 0.9rem;
    margin-bottom: 8px;
  }}
  .conditions-text {{
    font-size: 0.82rem;
    color: #2d3a5e;
    line-height: 1.5;
  }}
</style>
<div class="quote-wrap">
  <div class="quote-page">
    <div class="wave-top"></div>
    <div class="wave-bottom"></div>
    <div class="header">
      <div class="logo">{logo_html}</div>
      <div class="brand-meta">{brand_html}</div>
    </div>
    <div class="title">Cotizacion</div>
    <div class="subline">No: {_safe_text(quote_number)} | Fecha: {_safe_text(quote_date.strftime("%Y-%m-%d"))}</div>
    <div class="columns">
      <div>
        <div class="block-title">Datos del Cliente</div>
        <div class="block-text">{client_html}</div>
      </div>
      <div>
        <div class="block-title">Datos del Emisor</div>
        <div class="block-text">{issuer_html}</div>
      </div>
    </div>
    <table class="items">
      <thead>
        <tr>
          <th>Producto</th>
          <th>Cantidad</th>
          <th>Precio</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html) if rows_html else '<tr><td colspan="3" class="center">Sin items</td></tr>'}
      </tbody>
    </table>
    <div class="totals">
      <table>
        <tr><td class="label">Subtotal</td><td class="value">{_money(subtotal)}</td></tr>
        <tr><td class="label">Impuestos ({tax_pct:.2f}%)</td><td class="value">{_money(tax_value)}</td></tr>
        <tr><td class="label">TOTAL</td><td class="value">{_money(total)}</td></tr>
      </table>
    </div>
    <div class="conditions">
      <div class="conditions-title">CONDICIONES</div>
      <div class="conditions-text">{terms_html}</div>
    </div>
  </div>
</div>
"""


def _print_script(fragment_html: str) -> str:
    payload = json.dumps(fragment_html)
    return f"""
<div id="print-root"></div>
<script>
  const html = {payload};
  const root = document.getElementById('print-root');
  root.innerHTML = html;
  setTimeout(() => {{
    window.print();
  }}, 300);
</script>
"""


st.set_page_config(page_title="Generador de Cotizaciones", layout="wide")
_require_authentication()

APP_ROOT = Path(__file__).resolve().parents[1]

TEMPLATES = {
    "RS Engineering": {
        "display_name": "RS Engineering",
        "ruc": "R.U.C. 9-740-624 / DV: 80",
        "address": "PH Bonanza plaza, Bella vista",
        "phone": "TELEFONO:+507 68475616",
        "email": "EMAIL: RODRIGOSJESUS@HOTMAIL.COM",
        "logo_path": APP_ROOT / "assets" / "rs.png.png",
        "accent": "#1f2f6e",
        "accent_light": "#5aa3e5",
    },
    "RIR Medical": {
        "display_name": "RIR Medical",
        "ruc": "",
        "address": "",
        "phone": "",
        "email": "",
        "logo_path": APP_ROOT / "assets" / "rir.png.png",
        "accent": "#1f2f6e",
        "accent_light": "#5aa3e5",
    },
}

st.title("Generador de Cotizaciones")

with st.expander("Cotizacion - Panama Compra", expanded=False):
    st.info("Seccion en construccion.")

with st.expander("Cotizacion - Privada", expanded=False):
    left, right = st.columns([0.45, 0.55], gap="large")
    with left:
        st.subheader("Datos generales")
        company = st.selectbox("Empresa", list(TEMPLATES.keys()))
        logo_path = TEMPLATES[company]["logo_path"]
        if not logo_path.exists():
            st.warning(f"Logo no encontrado: {logo_path.name}")
        else:
            st.caption(f"Logo: {logo_path.name}")
        quote_number = st.text_input("Numero de cotizacion", value="COT-001")
        quote_date = st.date_input("Fecha", value=date.today())

        st.subheader("Datos del cliente")
        client_name = st.text_input("Nombre del cliente", value="")
        client_address = st.text_area(
            "Direccion del cliente",
            value="",
            height=80,
        )

        st.subheader("Items")
        if "quote_items" not in st.session_state:
            st.session_state["quote_items"] = pd.DataFrame(
                [
                    {
                        "Producto": "Producto o servicio",
                        "Cantidad": 1,
                        "Precio Unitario": 0.0,
                    }
                ]
            )

        items_df = st.data_editor(
            st.session_state["quote_items"],
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "Producto": st.column_config.TextColumn("Producto"),
                "Cantidad": st.column_config.NumberColumn("Cantidad", min_value=0, step=1),
                "Precio Unitario": st.column_config.NumberColumn(
                    "Precio Unitario", min_value=0.0, step=0.01, format="$ %.2f"
                ),
            },
            key="quote_items_editor",
        )
        st.session_state["quote_items"] = items_df

        st.subheader("Impuesto")
        tax_pct = st.number_input(
            "Impuesto (%)",
            min_value=0.0,
            max_value=100.0,
            value=7.0,
            step=0.5,
        )

        st.subheader("Condiciones")
        vigencia = st.text_input("Vigencia de la cotizacion", "30 dias calendario")
        pago = st.text_input("Forma de pago", "Transferencia bancaria / Deposito")
        entrega = st.text_input("Tiempo estimado de entrega", "15 dias habiles a partir del pago")

    with right:
        st.subheader("Vista previa")
        terms = {"vigencia": f"Vigencia de la cotizacion: {vigencia}",
                 "pago": f"Forma de pago: {pago}",
                 "entrega": f"Tiempo estimado de entrega: {entrega}"}
        preview_html = _render_quote_preview(
            template=TEMPLATES[company],
            client_name=client_name,
            client_address=client_address,
            quote_number=quote_number,
            quote_date=quote_date,
            items=items_df,
            tax_pct=tax_pct,
            terms=terms,
            print_mode=False,
        )
        if st.button("Imprimir (PDF)", use_container_width=True):
            print_html = _render_quote_preview(
                template=TEMPLATES[company],
                client_name=client_name,
                client_address=client_address,
                quote_number=quote_number,
                quote_date=quote_date,
                items=items_df,
                tax_pct=tax_pct,
                terms=terms,
                print_mode=True,
            )
            components.html(_print_script(print_html), height=1, scrolling=False)
        st.caption("Para mantener el fondo, activa Background graphics al imprimir.")
        st.markdown(preview_html, unsafe_allow_html=True)
