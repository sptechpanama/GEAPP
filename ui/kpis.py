# ui/kpis.py

# Importamos Streamlit para renderizar UI
import streamlit as st  # Componentes visuales

def render_kpis(total_ing: float, total_gas: float, utilidad: float, margen: float):
    """
    Dibuja 4 tarjetas KPI: Ingresos, Gastos, Utilidad y Margen.
    """
    # Creamos cuatro columnas (tarjetas lado a lado)
    k1, k2, k3, k4 = st.columns(4)  # Distribución 4 columnas

    # Tarjeta 1: Ingresos (filtrados)
    with k1:
        st.markdown(  # HTML simple para estilos básicos
            '<div class="kpi-card"><p class="kpi-label">Ingresos (filtrados)</p>'
            f'<p class="kpi-value">${total_ing:,.2f}</p></div>',  # Valor con formato de miles y 2 decimales
            unsafe_allow_html=True  # Permitimos HTML
        )

    # Tarjeta 2: Gastos (filtrados)
    with k2:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Gastos (filtrados)</p>'
            f'<p class="kpi-value">${total_gas:,.2f}</p></div>',
            unsafe_allow_html=True
        )

    # Tarjeta 3: Utilidad (filtrada)
    with k3:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Utilidad (filtrada)</p>'
            f'<p class="kpi-value">${utilidad:,.2f}</p></div>',
            unsafe_allow_html=True
        )

    # Tarjeta 4: Margen (%)
    with k4:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Margen</p>'
            f'<p class="kpi-value">{margen:,.1f}%</p></div>',
            unsafe_allow_html=True
        )
