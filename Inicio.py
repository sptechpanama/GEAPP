# app.py
import streamlit as st
import streamlit_authenticator as stauth

st.set_page_config(page_title="Panel Principal", page_icon="🏠", layout="wide")

st.title("🏠 Panel Principal")
st.caption("Bienvenido...")

# ========= 1) CREDENCIALES (HASH) =========
# Generamos el hash una sola vez en runtime y lo reutilizamos
hash_rs, hash_is, hash_ig = stauth.Hasher(
    ["Sptech-71", "Sptech-71", "Sptech-71"]
).generate()

credentials = {
    "usernames": {
        "rsanchez": {"name": "Rodrigo Sánchez",       "password": hash_rs},
        "isanchez": {"name": "Irvin Sánchez",         "password": hash_is},
        "igsanchez":{"name": "Iris Grisel Sánchez",   "password": hash_ig},
    }
}

# ========= 2) COOKIES / SESIÓN PERSISTENTE =========
authenticator = stauth.Authenticate(
    credentials,
    cookie_name="finapp_auth",
    key="clave_super_larga_unica_987654321" ,  # cámbiala por una aleatoria larga
    cookie_expiry_days=20                      # 20 días
)

# ========= 3) LOGIN (compatible con versiones) =========
try:
    # en varias versiones el primer arg es 'location'
    name, auth_status, username = authenticator.login(location="main")
except TypeError:
    # fallback para firmas antiguas (title, location)
    name, auth_status, username = authenticator.login("Iniciar sesión", "main")

# ========= 4) CONTROL DE ACCESO GLOBAL =========
if auth_status is True:
    st.session_state["auth_ok"] = True
    st.session_state["user"] = username
    authenticator.logout("Cerrar sesión", "sidebar")
    st.success(f"Bienvenido, {name}")
else:
    st.session_state["auth_ok"] = False
    if auth_status is False:
        st.error("Usuario o contraseña incorrectos.")
    else:
        st.info("Por favor inicia sesión para continuar.")
    st.stop()  # evita renderizar contenido sin login
