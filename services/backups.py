# services/backups.py
import time
import threading
import schedule
import pandas as pd
from datetime import datetime, timezone
from googleapiclient.discovery import build

# ---- Config por defecto (puedes sobreescribir desde finance.py si quieres) ----
BACKUP_PREFIX    = "Finanzas_Backup_"
KEEP_LATEST      = 10
FRECUENCIA_DIAS  = 1
BACKUP_TIME_HHMM = "02:15"
BACKUP_FOLDER_ID = "1eVwyEehX6oakMTrW95MLyoMdV5QSdg37"  # tu carpeta de Drive

def _get_drive_service(creds):
    return build("drive", "v3", credentials=creds)

def make_backup(creds, source_file_id: str, folder_id: str = BACKUP_FOLDER_ID):
    """
    Crea una copia del Google Sheet en la carpeta de backups y rota para mantener solo KEEP_LATEST.
    """
    drive = _get_drive_service(creds)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_name = f"{BACKUP_PREFIX}{ts}"

    # 1) Copiar archivo
    body = {"name": backup_name, "parents": [folder_id]}
    new_file = drive.files().copy(fileId=source_file_id, body=body).execute()
    new_id = new_file.get("id")
    print(f"[BACKUP] Copia creada: {backup_name} ({new_id})")

    # 2) Rotación
    _rotate_backups(drive, folder_id)

def _rotate_backups(drive_svc, folder_id: str):
    query = (
        f"'{folder_id}' in parents and "
        f"name contains '{BACKUP_PREFIX}' and "
        f"mimeType = 'application/vnd.google-apps.spreadsheet' and "
        f"trashed = false"
    )
    resp = drive_svc.files().list(
        q=query,
        fields="files(id, name, createdTime)",
        orderBy="createdTime asc",  # antiguas primero
        pageSize=1000
    ).execute()
    files = resp.get("files", [])
    total = len(files)

    if total <= KEEP_LATEST:
        print(f"[ROTATE] {total} copias. No hay que borrar.")
        return

    to_delete = total - KEEP_LATEST
    print(f"[ROTATE] {total} copias. Se eliminarán {to_delete} antiguas...")
    for i in range(to_delete):
        fid = files[i]["id"]
        fname = files[i]["name"]
        drive_svc.files().delete(fileId=fid).execute()
        print(f"[ROTATE] Eliminada: {fname} ({fid})")

def start_backup_scheduler_once(creds, source_file_id: str):
    """
    Lanza un hilo en segundo plano con schedule. Idempotente para Streamlit.
    """
    import streamlit as st

    if "backup_scheduler_started" in st.session_state:
        return

    schedule.clear("backups")

    schedule.every(FRECUENCIA_DIAS).days.at(BACKUP_TIME_HHMM).do(
        make_backup, creds=creds, source_file_id=source_file_id, folder_id=BACKUP_FOLDER_ID
    ).tag("backups")

    def _runner():
        while True:
            schedule.run_pending()
            time.sleep(30)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    st.session_state["backup_scheduler_started"] = True
    print(f"[SCHEDULER] Backups cada {FRECUENCIA_DIAS} día(s) a las {BACKUP_TIME_HHMM}.")

# -------- Mostrar "Último respaldo" --------
def get_last_backup_info(creds, folder_id: str = BACKUP_FOLDER_ID):
    """
    Devuelve (name, ts_local) del backup más reciente; o (None, None) si no hay.
    """
    drive = _get_drive_service(creds)
    query = (
        f"'{folder_id}' in parents and "
        f"name contains '{BACKUP_PREFIX}' and "
        f"mimeType = 'application/vnd.google-apps.spreadsheet' and "
        f"trashed = false"
    )
    resp = drive.files().list(
        q=query,
        fields="files(id, name, createdTime)",
        orderBy="createdTime desc",
        pageSize=1
    ).execute()
    files = resp.get("files", [])
    if not files:
        return None, None

    f = files[0]
    name = f.get("name")
    created = f.get("createdTime")  # RFC3339
    try:
        ts_local = pd.to_datetime(created, utc=True).tz_convert(None)
    except Exception:
        ts_local = pd.to_datetime(created)
    return name, ts_local
