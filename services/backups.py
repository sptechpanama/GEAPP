# services/backups.py
from __future__ import annotations
from dataclasses import dataclass
import json
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

_STARTED_KEYS: set[str] = set()  # evita doble ejecución por proceso y por módulo
_WARNED_FOLDER_KEYS: set[str] = set()


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

def _freq_delta(every_days=None, frequency: str | None = None) -> timedelta:
    n = _as_int_days(_EVERY_DAYS if every_days is None else every_days)
    if n:
        return timedelta(days=n)
    freq = (_FREQ if frequency is None else str(frequency or "")).lower()
    if freq == "weekly":
        return timedelta(days=7)
    if freq == "monthly":
        return timedelta(days=30)
    return timedelta(days=1)

def _list_backups(drive, folder_id: str, prefix: str) -> List[BackupInfo]:
    if not folder_id:
        return []
    q = (
        f"'{folder_id}' in parents and trashed=false "
        f"and name contains '{prefix}'"
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

def _is_not_found_error(exc: HttpError) -> bool:
    status = getattr(exc, "status_code", None)
    if status == 404:
        return True
    content = getattr(exc, "content", b"") or b""
    if content:
        try:
            payload = json.loads(content.decode("utf-8"))
            err = payload.get("error", {})
            if err.get("status") == "NOT_FOUND":
                return True
            for item in err.get("errors", []):
                if item.get("reason") == "notFound":
                    return True
        except Exception:
            pass
    return "notfound" in str(exc).lower()

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
            if _is_not_found_error(exc):
                continue
            _notify("warning", f"No se pudo eliminar el respaldo '{b.name}': {exc}")
    return removed

def _copy_sheet(drive, source_sheet_id: str, folder_id: str, prefix: str) -> BackupInfo:
    """
    Crea una copia del Google Sheet dentro de la UNIDAD COMPARTIDA.
    En Shared Drives NO se transfiere propiedad: la "propiedad" la tiene la unidad.
    """
    now_local = datetime.now(tz=_TZ)
    name = f"{prefix} — {now_local.strftime('%Y-%m-%d %H%M')}"

    body = {"name": name, "parents": [folder_id]}
    resp = drive.files().copy(
        fileId=source_sheet_id,
        body=body,
        fields="id,name,createdTime",
        supportsAllDrives=True,
    ).execute()

    ct = datetime.fromisoformat(resp["createdTime"].replace("Z", "+00:00"))
    return BackupInfo(id=resp["id"], name=resp["name"], created_utc=ct)

def _resolve_backup_config(
    *,
    prefix: str | None = None,
    folder_id: str | None = None,
    keep_last: int | None = None,
    every_days=None,
    frequency: str | None = None,
):
    resolved_prefix = str(prefix or _BACKUP_PREFIX).strip() or "Respaldo"
    resolved_folder_id = str(folder_id or _FOLDER_ID).strip()
    try:
        resolved_keep_last = int(keep_last if keep_last is not None else _KEEP_LAST)
    except Exception:
        resolved_keep_last = _KEEP_LAST
    if resolved_keep_last <= 0:
        resolved_keep_last = _KEEP_LAST
    return {
        "prefix": resolved_prefix,
        "folder_id": resolved_folder_id,
        "keep_last": resolved_keep_last,
        "delta": _freq_delta(every_days=every_days, frequency=frequency),
    }


def auto_backup_if_due(
    creds,
    sheet_id: str,
    *,
    prefix: str | None = None,
    folder_id: str | None = None,
    keep_last: int | None = None,
    every_days=None,
    frequency: str | None = None,
) -> Optional[BackupInfo]:
    """Crea respaldo solo si NO existe uno dentro de la ventana (N días)."""
    cfg = _resolve_backup_config(
        prefix=prefix,
        folder_id=folder_id,
        keep_last=keep_last,
        every_days=every_days,
        frequency=frequency,
    )
    warn_key = f"{cfg['prefix']}|{cfg['folder_id'] or '(sin_folder)'}"
    if not cfg["folder_id"]:
        if warn_key not in _WARNED_FOLDER_KEYS:
            _notify("warning", "Falta DRIVE_BACKUP_FOLDER_ID en secrets.app; no se ejecutarán respaldos automáticos.")
            _WARNED_FOLDER_KEYS.add(warn_key)
        return None
    try:
        drive = _drive(creds)
        backups = _list_backups(drive, cfg["folder_id"], cfg["prefix"])
        delta = cfg["delta"]

        now_utc = datetime.now(timezone.utc)
        if backups:
            last = backups[0]
            if (now_utc - last.created_utc) < delta:
                _delete_excess(drive, backups, cfg["keep_last"])
                return None

        new_bk = _copy_sheet(drive, sheet_id, cfg["folder_id"], cfg["prefix"])
        backups = _list_backups(drive, cfg["folder_id"], cfg["prefix"])
        _delete_excess(drive, backups, cfg["keep_last"])
        return new_bk
    except HttpError as exc:
        _notify("error", f"Fallo en la copia automática del respaldo: {exc}")
    except Exception as exc:  # pragma: no cover - salvaguarda genérica
        _notify("error", f"Error inesperado al crear el respaldo: {exc}")
    return None

def start_backup_scheduler_once(
    creds,
    sheet_id: str,
    *,
    scheduler_key: str | None = None,
    prefix: str | None = None,
    folder_id: str | None = None,
    keep_last: int | None = None,
    every_days=None,
    frequency: str | None = None,
):
    """
    Idempotente: chequea y respalda si 'toca', una sola vez por proceso.
    """
    key = str(scheduler_key or f"{prefix or _BACKUP_PREFIX}|{folder_id or _FOLDER_ID}|{sheet_id}").strip()
    if key in _STARTED_KEYS:
        return
    _STARTED_KEYS.add(key)
    try:
        created = auto_backup_if_due(
            creds,
            sheet_id,
            prefix=prefix,
            folder_id=folder_id,
            keep_last=keep_last,
            every_days=every_days,
            frequency=frequency,
        )
        if created:
            _notify("info", f"Respaldo automático creado: {created.name}")
    except Exception as e:  # pragma: no cover - fallback
        _notify("error", f"Error en respaldo automático: {e}")

def get_last_backup_info(creds, *, prefix: str | None = None, folder_id: str | None = None) -> Tuple[Optional[str], Optional[datetime]]:
    cfg = _resolve_backup_config(prefix=prefix, folder_id=folder_id)
    if not cfg["folder_id"]:
        return None, None
    try:
        drive = _drive(creds)
        backups = _list_backups(drive, cfg["folder_id"], cfg["prefix"])
        if not backups:
            return None, None
        last = backups[0]
        return last.name, last.created_utc.astimezone(_TZ)
    except Exception:
        return None, None

def create_backup_now(
    creds,
    sheet_id: str,
    *,
    prefix: str | None = None,
    folder_id: str | None = None,
) -> Optional[BackupInfo]:
    """Botón manual."""
    cfg = _resolve_backup_config(prefix=prefix, folder_id=folder_id)
    warn_key = f"{cfg['prefix']}|{cfg['folder_id'] or '(sin_folder)'}"
    if not cfg["folder_id"]:
        if warn_key not in _WARNED_FOLDER_KEYS:
            _notify("warning", "Configura DRIVE_BACKUP_FOLDER_ID en secrets.app para habilitar los respaldos.")
            _WARNED_FOLDER_KEYS.add(warn_key)
        return None
    try:
        drive = _drive(creds)
        return _copy_sheet(drive, sheet_id, cfg["folder_id"], cfg["prefix"])
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

