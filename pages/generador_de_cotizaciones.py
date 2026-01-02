import streamlit as st


def _require_authentication() -> None:
    status = st.session_state.get("authentication_status")
    if status is True:
        return
    if status is False:
        st.error("Credenciales invalidas. Vuelve a la portada para iniciar sesion.")
    else:
        st.warning("Debes iniciar sesion para entrar.")
    try:
        st.switch_page("Inicio.py")
    except Exception:
        st.stop()
    st.stop()


st.set_page_config(page_title="Generador de Cotizaciones", layout="wide")
_require_authentication()

st.title("Generador de Cotizaciones")

with st.expander("Cotizacion - Panama Compra", expanded=False):
    st.info("Seccion en construccion.")

with st.expander("Cotizacion - Privada", expanded=False):
    st.info("Seccion en construccion.")
