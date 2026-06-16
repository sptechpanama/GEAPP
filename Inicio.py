import streamlit as st

from services.access_control import build_authenticator
from ui.theme import apply_global_theme


st.set_page_config(page_title="Panel Principal", page_icon="🏠", layout="wide")
apply_global_theme()
st.title("🏠 Panel Principal")
st.caption("Bienvenido...")

authenticator = build_authenticator()

authenticator.login(
    location="main",
    fields={
        "Form name": "Login",
        "Username": "Usuario",
        "Password": "Contraseña",
        "Login": "Entrar",
    },
)

status = st.session_state.get("authentication_status", None)
name = st.session_state.get("name")

if status is True:
    st.success(f"Bienvenido, {name} 👋")
    authenticator.logout("Cerrar sesión", location="sidebar")
    st.write("✅ Sesión iniciada correctamente.")
elif status is False:
    st.error("Usuario/contraseña inválidos")
else:
    st.info("Introduce tus credenciales")
