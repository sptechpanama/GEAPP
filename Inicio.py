# ============================
# app.py (mínimo y súper comentado)
# Página principal con un título y 1 botón que lleva a "Pendientes"
# ============================

# 1) Importamos Streamlit (framework de la app)
import streamlit as st  # UI y navegación

# 2) Configuración básica de la página (título del navegador, ícono y ancho)
st.set_page_config(                     # Ajustes globales de la página
    page_title="Panel Principal",       # Título que verás en la pestaña del navegador
    page_icon="🏠",                     # Ícono de la pestaña
    layout="wide"                       # Ocupa todo el ancho de la ventana
)

# 3) Encabezado visual dentro de la app
st.title("🏠 Panel Principal")          # Título grande en la página
st.caption("Bienvenido...")  # Subtítulo breve

