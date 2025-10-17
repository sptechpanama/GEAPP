# app.py
import streamlit as st
import streamlit_authenticator as stauth

st.set_page_config(page_title="Panel Principal", page_icon="🏠", layout="wide")

st.title("🏠 Panel Principal")
st.caption("Bienvenido...")

# Usuarios y contraseñas en TEXTO PLANO (puedes editarlos a gusto)
PLAINTEXT_PW = {
    "rsanchez": "Sptech-71",   # Rodrigo
    "isanchez": "Sptech-71",   # Irvin
    "igsanchez": "Sptech-71",  # Iris
}
NAMES = {
    "rsanchez": "Rodrigo Sánchez",
    "isanchez": "Irvin Sánchez",
    "igsanchez": "Iris Grisel Sánchez",
}

# Intentamos usar streamlit_authenticator + bcrypt (si están disponibles).
# Si no, caemos a un login básico para NO tumbar la app en Streamlit Cloud.
try:
    import streamlit_authenticator as stauth
    import bcrypt

    def _hash_pw_once(pw: str) -> str:
        return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

    # Construimos credenciales con HASH (a partir de tus claves en texto)
    credentials = {"usernames": {}}
    for username, plain in PLAINTEXT_PW.items():
        credentials["usernames"][username] = {
            "name": NAMES.get(username, username),
            "password": _hash_pw_once(plain),
        }

    authenticator = stauth.Authenticate(
        credentials,
        "finapp_auth_cookie",             # cookie_name
        "clave-cookie-larga-unica-123",   # cookie_key (pon una aleatoria)
        30,                                # días de expiración
    )

    # Firmas de login cambian por versión; probamos ambas
    try:
        name, auth_status, username = authenticator.login(location="main")
    except TypeError:
        name, auth_status, username = authenticator.login("Iniciar sesión", "main")

    if auth_status is True:
        st.session_state["auth_ok"] = True
        st.session_state["user"] = username
        st.session_state["auth_user_name"] = name  # opcional, compatibilidad
        authenticator.logout("Cerrar sesión", "sidebar")
        st.success(f"Bienvenido, {name}")
    else:
        st.session_state["auth_ok"] = False
        if auth_status is False:
            st.error("Usuario/contraseña inválidos")
        else:
            st.info("Introduce tus credenciales")
        st.stop()

except Exception:
    # ======= Fallback ultra simple (sin dependencias externas) =======
    st.warning("Autenticador no disponible. Usando login básico temporal.")

    u = st.text_input("Usuario", key="basic_user")
    p = st.text_input("Contraseña", type="password", key="basic_pass")
    ok = (u in PLAINTEXT_PW and p == PLAINTEXT_PW[u])

    if st.button("Entrar"):
        st.session_state["auth_ok"] = bool(ok)
        if ok:
            st.session_state["user"] = u
            st.session_state["auth_user_name"] = NAMES.get(u, u)
        else:
            st.error("Usuario/contraseña inválidos")

    if not st.session_state.get("auth_ok"):
        st.stop()
