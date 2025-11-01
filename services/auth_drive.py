# services/auth_drive.py
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

SERVICE_ACCOUNT_FILE = r"C:\Users\rodri\ge\finapp\service_account.json"
DOMAIN_USER = "soporte@sptechpanama.com"

def get_drive_delegated():
    """
    Retorna un cliente de Google Drive actuando como el usuario de dominio.
    Si la delegación no está activa, devuelve None.
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES,
            subject=DOMAIN_USER
        )
        drive = build("drive", "v3", credentials=creds)
        user = drive.about().get(fields="user").execute()["user"]["emailAddress"]
        print("Autenticado como:", user)
        return drive
    except Exception as e:
        print("⚠️ Error en autenticación delegada:", e)
        return None
