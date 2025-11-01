# Inicio.py — Login estable para streamlit-authenticator 0.4.2
import streamlit as st
import bcrypt
import streamlit_authenticator as stauth

st.set_page_config(page_title="Panel Principal", page_icon="🏠", layout="wide")
st.title("🏠 Panel Principal")
st.caption("Bienvenido...")

# ========= Credenciales DEMO (se hashean en runtime) =========
USERS = {
    "rsanchez": ("Rodrigo Sánchez", "Sptech-71"),
    "isanchez": ("Irvin Sánchez",   "Sptech-71"),
    "igsanchez": ("Iris Grisel Sánchez", "Sptech-71"),
}

def _hash(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

credentials = {"usernames": {}}
for user, (name, plain) in USERS.items():
    credentials["usernames"][user] = {"name": name, "password": _hash(plain)}

# Usa estos DOS valores IGUALES en TODAS las páginas del multipage
COOKIE_NAME = "finapp_auth"
COOKIE_KEY  = "finapp_key_123"

authenticator = stauth.Authenticate(
    credentials,           # en vez de credentials=credentials
    COOKIE_NAME,           # en vez de cookie_name=...
    COOKIE_KEY,            # en vez de key=...
    30                     # en vez de cookie_expiry_days=30
)

# ======= PINTA EL FORMULARIO (0.4.2) =======
authenticator.login(
    location="main",
    fields={
        "Form name": "Login",
        "Username": "Usuario",
        "Password": "Contraseña",
        "Login": "Entrar"
    },
)

# ======= ESTADO DE AUTENTICACIÓN (vía session_state) =======
status = st.session_state.get("authentication_status", None)
name   = st.session_state.get("name")
user   = st.session_state.get("username")

if status is True:
    st.success(f"Bienvenido, {name} 👋")
    authenticator.logout("Cerrar sesión", location="sidebar")
    # --- Contenido de la página después del login:
    st.write("✅ Sesión iniciada correctamente.")
elif status is False:
    st.error("Usuario/contraseña inválidos")
else:
    # status is None -> aún no se han enviado credenciales
    st.info("Introduce tus credenciales")
