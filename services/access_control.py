from __future__ import annotations

import bcrypt
import streamlit as st
import streamlit_authenticator as stauth


USERS = {
    "rsanchez": ("Rodrigo Sánchez", "Sptech-71"),
    "isanchez": ("Irvin Sánchez", "Sptech-71"),
    "igsanchez": ("Iris Grisel Sánchez", "Sptech-71"),
    "jsilva": ("J Silva", "Jsilva2026"),
}

COOKIE_NAME = "finapp_auth"
COOKIE_KEY = "finapp_key_123"

USER_ALLOWED_PAGES: dict[str, set[str]] = {
    "jsilva": {
        "Inicio.py",
        "pages/finance.py",
        "pages/panel_financiero_gerencial.py",
    },
}


def _normalize_page_path(page_path: str | None) -> str:
    return str(page_path or "").replace("\\", "/").strip()


def _hash(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


@st.cache_data(show_spinner=False, ttl=86400)
def _hash_for_auth_cached(pw: str) -> str:
    return _hash(pw)


def build_credentials() -> dict[str, dict[str, dict[str, str]]]:
    return {
        "usernames": {
            username: {"name": name, "password": _hash_for_auth_cached(password)}
            for username, (name, password) in USERS.items()
        }
    }


def build_authenticator() -> stauth.Authenticate:
    return stauth.Authenticate(build_credentials(), COOKIE_NAME, COOKIE_KEY, 30)


def current_username() -> str:
    return str(st.session_state.get("username", "") or "").strip()


def user_has_page_access(username: str | None, page_path: str) -> bool:
    normalized_username = str(username or "").strip().lower()
    normalized_page = _normalize_page_path(page_path)
    if not normalized_username:
        return False
    allowed_pages = USER_ALLOWED_PAGES.get(normalized_username)
    if allowed_pages is None:
        return True
    return normalized_page in {_normalize_page_path(item) for item in allowed_pages}


def require_page_access(
    page_path: str,
    *,
    unauthenticated_redirect: str = "Inicio.py",
    forbidden_redirect: str = "pages/finance.py",
) -> str:
    if st.session_state.get("authentication_status") is not True:
        try:
            st.switch_page(unauthenticated_redirect)
        except Exception:
            st.stop()
        st.stop()

    username = current_username()
    st.session_state.setdefault("auth_user_name", st.session_state.get("name", ""))
    st.session_state.setdefault("auth_username", username)

    if user_has_page_access(username, page_path):
        return username

    st.error("No tienes acceso a esta sección con este usuario.")
    fallback = forbidden_redirect if user_has_page_access(username, forbidden_redirect) else unauthenticated_redirect
    try:
        st.switch_page(fallback)
    except Exception:
        st.stop()
    st.stop()

