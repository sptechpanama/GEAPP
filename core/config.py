"""Configuración base compartida por distintas páginas del proyecto."""

from os import environ
from pathlib import Path


# Raíz del repo (`finapp/` vive un nivel arriba de este archivo).
APP_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_ROOT.parent


def _path_from_env(var_name: str, default: Path) -> Path:
	"""Resuelve rutas desde env vars, expandiendo `~` y convirtiendo a Path."""
	raw = environ.get(var_name)
	return Path(raw).expanduser() if raw else default


# Carpeta base para archivos auxiliares. Permite sobrescribir vía env.
BASE_PATH = _path_from_env("FINAPP_BASE_PATH", REPO_ROOT)


# Archivos locales opcionales (no obligatorios en Streamlit Cloud).
EXCEL_FICHAS = _path_from_env("FINAPP_EXCEL_FICHAS", APP_ROOT / "todas_las_fichas.xlsx")
DB_PATH = _path_from_env("FINAPP_DB_PATH", APP_ROOT / "panamacompra.db")

# Compatibilidad con código antiguo que esperaba un JSON en disco.
_svc_env = environ.get("FINAPP_SERVICE_ACCOUNT_FILE")
SVC_KEY = Path(_svc_env).expanduser() if _svc_env else None


__all__ = [
	"APP_ROOT",
	"REPO_ROOT",
	"BASE_PATH",
	"EXCEL_FICHAS",
	"DB_PATH",
	"SVC_KEY",
]
