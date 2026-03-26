# ui/kpis.py

import streamlit as st


def _format_number_es(value: float, decimals: int = 2) -> str:
    try:
        n = float(value)
    except Exception:
        n = 0.0
    us = f"{n:,.{decimals}f}"
    return us.replace(",", "__tmp__").replace(".", ",").replace("__tmp__", ".")


def _format_money_es(value: float) -> str:
    return f"${_format_number_es(value, 2)}"


def _format_percent_es(value: float) -> str:
    return f"{_format_number_es(value, 1)}%"


def render_kpis(total_ing: float, total_gas: float, utilidad: float, margen: float):
    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Ingresos (filtrados)</p>'
            f'<p class="kpi-value">{_format_money_es(total_ing)}</p></div>',
            unsafe_allow_html=True,
        )

    with k2:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Gastos (filtrados)</p>'
            f'<p class="kpi-value">{_format_money_es(total_gas)}</p></div>',
            unsafe_allow_html=True,
        )

    with k3:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Utilidad (filtrada)</p>'
            f'<p class="kpi-value">{_format_money_es(utilidad)}</p></div>',
            unsafe_allow_html=True,
        )

    with k4:
        st.markdown(
            '<div class="kpi-card"><p class="kpi-label">Margen</p>'
            f'<p class="kpi-value">{_format_percent_es(margen)}</p></div>',
            unsafe_allow_html=True,
        )

