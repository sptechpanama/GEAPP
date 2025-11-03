# services/backups.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List
from zoneinfo import ZoneInfo

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st

# ================= Config desde secrets =================
_APP = st.secrets.get("app", {})
_TZ = ZoneInfo("America/Panama")

_BACKUP_PREFIX = _APP.get("BACKUP_PREFIX", "Finanzas Backup")
_FOLDER_ID     = _APP.get("DRIVE_BACKUP_FOLDER_ID", "")  # ID de carpeta en UNIDAD COMPARTIDA
_KEEP_LAST     = int(_APP.get("BACKUP_KEEP_LAST", 15))

# Preferible usar días (BACKUP_EVERY_DAYS). Si no, compat con 'daily/weekly/monthly'
_EVERY_DAYS = _APP.get("BACKUP_EVERY_DAYS", None)
_FREQ       = (_APP.get("BACKUP_FREQUENCY") or "").lower()

_STARTED = False  # evita doble ejecución por proceso
_WARNED_FOLDER = False


def _notify(kind: str, message: str) -> None:
    """Muestra mensajes en Streamlit cuando está disponible y hace fallback a print."""
    func = getattr(st, kind, None)
    if callable(func):
        try:
            func(message)
            return
        except Exception:
            pass
    print(f"[BACKUP][{kind.upper()}] {message}")

@dataclass
class BackupInfo:
    id: str
    name: str
    created_utc: datetime

def _drive(creds):
    # IMPORTANTÍSIMO: supportsAllDrives en todas las llamadas
    return build("drive", "v3", credentials=creds)

def _as_int_days(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    s = str(value).strip().lower()
    if s.endswith("d"):
        s = s[:-1]
    try:
        n = int(s)
        return n if n > 0 else None
    except Exception:
        return None

def _freq_delta() -> timedelta:
    n = _as_int_days(_EVERY_DAYS)
    if n:
        return timedelta(days=n)
    if _FREQ == "weekly":
        return timedelta(days=7)
    if _FREQ == "monthly":
        return timedelta(days=30)
    return timedelta(days=1)

def _list_backups(drive, folder_id: str) -> List[BackupInfo]:
    if not folder_id:
        return []
    q = (
        f"'{folder_id}' in parents and trashed=false "
        f"and name contains '{_BACKUP_PREFIX}'"
    )
    out: List[BackupInfo] = []
    token = None
    while True:
        resp = drive.files().list(
            q=q,
            orderBy="createdTime desc",
            fields="nextPageToken, files(id,name,createdTime)",
            pageToken=token,
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        for f in resp.get("files", []):
            ct = datetime.fromisoformat(f["createdTime"].replace("Z", "+00:00"))
            out.append(BackupInfo(id=f["id"], name=f["name"], created_utc=ct))
        token = resp.get("nextPageToken")
        if not token:
            break
    return out

def _delete_excess(drive, backups: List[BackupInfo], keep_last: int) -> int:
    removed = 0
    for b in backups[keep_last:]:
        try:
            drive.files().delete(
                fileId=b.id,
                supportsAllDrives=True,
            ).execute()
            removed += 1
        except HttpError as exc:
            _notify("warning", f"No se pudo eliminar el respaldo '{b.name}': {exc}")
    return removed

def _copy_sheet(drive, source_sheet_id: str, folder_id: str) -> BackupInfo:
    """
    Crea una copia del Google Sheet dentro de la UNIDAD COMPARTIDA.
    En Shared Drives NO se transfiere propiedad: la "propiedad" la tiene la unidad.
    """
    now_local = datetime.now(tz=_TZ)
    name = f"{_BACKUP_PREFIX} — {now_local.strftime('%Y-%m-%d %H%M')}"

    body = {"name": name, "parents": [folder_id]}
    resp = drive.files().copy(
        fileId=source_sheet_id,
        body=body,
        fields="id,name,createdTime",
        supportsAllDrives=True,
    ).execute()

    ct = datetime.fromisoformat(resp["createdTime"].replace("Z", "+00:00"))
    return BackupInfo(id=resp["id"], name=resp["name"], created_utc=ct)

def auto_backup_if_due(creds, sheet_id: str) -> Optional[BackupInfo]:
    """Crea respaldo solo si NO existe uno dentro de la ventana (N días)."""
    global _WARNED_FOLDER
    if not _FOLDER_ID:
        if not _WARNED_FOLDER:
            _notify("warning", "Falta DRIVE_BACKUP_FOLDER_ID en secrets.app; no se ejecutarán respaldos automáticos.")
            _WARNED_FOLDER = True
        return None
    try:
        drive = _drive(creds)
        backups = _list_backups(drive, _FOLDER_ID)
        delta = _freq_delta()

        now_utc = datetime.now(timezone.utc)
        if backups:
            last = backups[0]
            if (now_utc - last.created_utc) < delta:
                _delete_excess(drive, backups, _KEEP_LAST)
                return None

        new_bk = _copy_sheet(drive, sheet_id, _FOLDER_ID)
        backups = _list_backups(drive, _FOLDER_ID)
        _delete_excess(drive, backups, _KEEP_LAST)
        return new_bk
    except HttpError as exc:
        _notify("error", f"Fallo en la copia automática del respaldo: {exc}")
    except Exception as exc:  # pragma: no cover - salvaguarda genérica
        _notify("error", f"Error inesperado al crear el respaldo: {exc}")
    return None

def start_backup_scheduler_once(creds, sheet_id: str):
    """
    Idempotente: chequea y respalda si 'toca', una sola vez por proceso.
    """
    global _STARTED
    if _STARTED:
        return
    _STARTED = True
    try:
        created = auto_backup_if_due(creds, sheet_id)
        if created:
            _notify("info", f"Respaldo automático creado: {created.name}")
    except Exception as e:  # pragma: no cover - fallback
        _notify("error", f"Error en respaldo automático: {e}")

def get_last_backup_info(creds) -> Tuple[Optional[str], Optional[datetime]]:
    if not _FOLDER_ID:
        return None, None
    try:
        drive = _drive(creds)
        backups = _list_backups(drive, _FOLDER_ID)
        if not backups:
            return None, None
        last = backups[0]
        return last.name, last.created_utc.astimezone(_TZ)
    except Exception:
        return None, None

def create_backup_now(creds, sheet_id: str) -> Optional[BackupInfo]:
    """Botón manual."""
    global _WARNED_FOLDER
    if not _FOLDER_ID:
        if not _WARNED_FOLDER:
            _notify("warning", "Configura DRIVE_BACKUP_FOLDER_ID en secrets.app para habilitar los respaldos.")
            _WARNED_FOLDER = True
        return None
    try:
        drive = _drive(creds)
        return _copy_sheet(drive, sheet_id, _FOLDER_ID)
    except HttpError as exc:
        _notify("error", f"La API de Drive rechazó el respaldo manual: {exc}")
    except Exception as exc:  # pragma: no cover - fallback
        _notify("error", f"Error inesperado al crear el respaldo manual: {exc}")
    return None

def debug_sa_quota(creds):
    """
    Devuelve información de la cuenta autenticada (service account) y su 'storageQuota'
    tal como la espera finance.py:
      - 'sa_email' (email de la cuenta)
      - 'storageQuota' (dict con limit, usage, usageInDrive, usageInDriveTrash)
    Nota: En service accounts el 'limit' suele ser None porque no tienen cuota propia.
    """
    drive = build("drive", "v3", credentials=creds)
    about = drive.about().get(fields="user(emailAddress), storageQuota").execute() or {}
    return {
        "sa_email": about.get("user", {}).get("emailAddress"),
        "storageQuota": about.get("storageQuota", {}) or {},
    }

