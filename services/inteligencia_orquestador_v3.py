from __future__ import annotations

"""Puente pequeno y reutilizable entre Inteligencia v3 y el orquestador."""

import json
import time
import uuid
from datetime import datetime
from typing import Any, Mapping, Sequence

try:
    from gspread.exceptions import APIError, WorksheetNotFound
except ImportError:  # Permite validar la lógica local sin instalar el cliente de Sheets.
    class APIError(Exception):
        pass

    class WorksheetNotFound(Exception):
        pass


PC_CONFIG_WORKSHEET = "pc_config"
PC_MANUAL_WORKSHEET = "pc_manual"
INTEL_VIEWS_WORKSHEET = "intel_v3_saved_views"
PC_CONFIG_HEADERS = ["name", "python", "script", "days", "times", "enabled"]
PC_MANUAL_HEADERS = [
    "id",
    "job",
    "requested_by",
    "requested_at",
    "status",
    "notes",
    "payload",
    "result_file_id",
    "result_file_url",
    "result_file_name",
    "result_error",
]
INTEL_VIEWS_HEADERS = ["id", "username", "name", "payload", "created_at", "updated_at"]
DEFAULT_JOB_NAME = "intel_estudio_ficha"
DEFAULT_JOB_PYTHON = r"C:\Users\rodri\scrapers_repo\.venv\Scripts\python.exe"
DEFAULT_JOB_SCRIPT = r"C:\Users\rodri\scrapers_repo\orquestador\intel_ficha_worker.py"


def _sheet_candidates(value: str | Sequence[str]) -> list[str]:
    raw_values = [value] if isinstance(value, str) else list(value)
    output: list[str] = []
    for raw in raw_values:
        sheet_id = str(raw or "").strip()
        if sheet_id and sheet_id not in output:
            output.append(sheet_id)
    return output


def _open_spreadsheet(client, sheet_ids: str | Sequence[str], *, purpose: str):
    """Abre la primera hoja nativa valida entre varios IDs configurados.

    Google Drive permite guardar archivos XLSX, pero gspread no puede agregarles
    pestanas. Si el primer candidato es un archivo Office, se prueba el siguiente
    ID (normalmente ``SHEET_ID``) sin interrumpir el flujo del usuario.
    """
    candidates = _sheet_candidates(sheet_ids)
    if not candidates:
        raise ValueError(f"No hay una hoja configurada para {purpose}.")
    last_error: Exception | None = None
    for sheet_id in candidates:
        try:
            return _retry(lambda sid=sheet_id: client.open_by_key(sid))
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        f"No se encontro una hoja nativa de Google Sheets valida para {purpose}. "
        "Revisa SHEET_ID y evita usar un archivo .xlsx. "
        f"Ultimo error: {last_error}"
    )


def _retry(action, *, attempts: int = 5):
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return action()
        except APIError as exc:
            last_error = exc
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", 0)
            if status not in {429, 500, 502, 503, 504}:
                raise
        except Exception as exc:
            last_error = exc
        if attempt < attempts - 1:
            time.sleep(min(8.0, 0.7 * (2**attempt)))
    if last_error:
        raise last_error
    raise RuntimeError("Operacion de Google Sheets no ejecutada.")


def _ensure_worksheet(spreadsheet, title: str, headers: Sequence[str]):
    try:
        worksheet = _retry(lambda: spreadsheet.worksheet(title))
    except WorksheetNotFound:
        worksheet = _retry(lambda: spreadsheet.add_worksheet(title=title, rows=500, cols=max(len(headers), 8)))
    current = _retry(lambda: worksheet.row_values(1)) or []
    normalized = [str(value).strip() for value in current]
    if normalized[: len(headers)] != list(headers):
        _retry(lambda: worksheet.update("A1", [list(headers)]))
    return worksheet


def ensure_study_job(
    client,
    config_sheet_id: str | Sequence[str],
    *,
    job_name: str = DEFAULT_JOB_NAME,
    python_executable: str = DEFAULT_JOB_PYTHON,
    script_path: str = DEFAULT_JOB_SCRIPT,
) -> None:
    spreadsheet = _open_spreadsheet(client, config_sheet_id, purpose="configuracion del orquestador")
    worksheet = _ensure_worksheet(spreadsheet, PC_CONFIG_WORKSHEET, PC_CONFIG_HEADERS)
    rows = _retry(lambda: worksheet.get_all_records()) or []
    header_map = {str(value).strip().lower(): index + 1 for index, value in enumerate(_retry(lambda: worksheet.row_values(1)))}
    desired = {
        "name": job_name,
        "python": python_executable,
        "script": script_path,
        "days": "",
        "times": "",
        "enabled": "si",
    }
    for row_index, row in enumerate(rows, start=2):
        if str(row.get("name", "")).strip().lower() != job_name.lower():
            continue
        for key, value in desired.items():
            column = header_map.get(key)
            if column and str(row.get(key, "")).strip() != value:
                _retry(lambda r=row_index, c=column, v=value: worksheet.update_cell(r, c, v))
        return
    _retry(lambda: worksheet.append_row([desired.get(column, "") for column in PC_CONFIG_HEADERS], value_input_option="USER_ENTERED"))


def queue_study(
    client,
    *,
    manual_sheet_id: str | Sequence[str],
    config_sheet_id: str | Sequence[str],
    requested_by: str,
    payload: Mapping[str, Any],
    notes: str = "",
    job_name: str = DEFAULT_JOB_NAME,
) -> str:
    ensure_study_job(client, config_sheet_id, job_name=job_name)
    spreadsheet = _open_spreadsheet(client, manual_sheet_id, purpose="solicitudes manuales")
    worksheet = _ensure_worksheet(spreadsheet, PC_MANUAL_WORKSHEET, PC_MANUAL_HEADERS)
    request_id = uuid.uuid4().hex
    payload_out = dict(payload)
    payload_out["request_id"] = request_id
    row = {
        "id": request_id,
        "job": job_name,
        "requested_by": str(requested_by or "desconocido").strip(),
        "requested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pending",
        "notes": str(notes or "").strip(),
        "payload": json.dumps(payload_out, ensure_ascii=False),
        "result_file_id": "",
        "result_file_url": "",
        "result_file_name": "",
        "result_error": "",
    }
    _retry(lambda: worksheet.append_row([row.get(column, "") for column in PC_MANUAL_HEADERS], value_input_option="USER_ENTERED"))
    return request_id


def get_request_status(
    client, *, manual_sheet_id: str | Sequence[str], request_id: str
) -> dict[str, str]:
    spreadsheet = _open_spreadsheet(client, manual_sheet_id, purpose="estado de solicitudes")
    worksheet = _ensure_worksheet(spreadsheet, PC_MANUAL_WORKSHEET, PC_MANUAL_HEADERS)
    rows = _retry(lambda: worksheet.get_all_records()) or []
    for row in reversed(rows):
        if str(row.get("id", "")).strip() == str(request_id).strip():
            return {str(key): str(value or "") for key, value in row.items()}
    return {}


def list_saved_views(
    client, *, sheet_id: str | Sequence[str], username: str
) -> list[dict[str, Any]]:
    """Devuelve únicamente las vistas del usuario autenticado."""
    spreadsheet = _open_spreadsheet(client, sheet_id, purpose="vistas guardadas")
    worksheet = _ensure_worksheet(spreadsheet, INTEL_VIEWS_WORKSHEET, INTEL_VIEWS_HEADERS)
    rows = _retry(lambda: worksheet.get_all_records()) or []
    owner = str(username or "").strip().lower()
    output: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("username", "") or "").strip().lower() != owner:
            continue
        try:
            payload = json.loads(str(row.get("payload", "") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        output.append(
            {
                "id": str(row.get("id", "") or "").strip(),
                "name": str(row.get("name", "") or "").strip(),
                "payload": payload if isinstance(payload, dict) else {},
                "created_at": str(row.get("created_at", "") or "").strip(),
                "updated_at": str(row.get("updated_at", "") or "").strip(),
            }
        )
    return sorted(output, key=lambda item: (item["name"].lower(), item["updated_at"]), reverse=False)


def save_saved_view(
    client,
    *,
    sheet_id: str | Sequence[str],
    username: str,
    name: str,
    payload: Mapping[str, Any],
) -> str:
    """Crea o reemplaza por nombre una vista del usuario."""
    owner = str(username or "").strip().lower()
    view_name = str(name or "").strip()
    if not owner or not view_name:
        raise ValueError("Usuario y nombre de vista son obligatorios.")
    spreadsheet = _open_spreadsheet(client, sheet_id, purpose="vistas guardadas")
    worksheet = _ensure_worksheet(spreadsheet, INTEL_VIEWS_WORKSHEET, INTEL_VIEWS_HEADERS)
    rows = _retry(lambda: worksheet.get_all_records()) or []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    serialized = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True)
    for row_index, row in enumerate(rows, start=2):
        if (
            str(row.get("username", "") or "").strip().lower() == owner
            and str(row.get("name", "") or "").strip().lower() == view_name.lower()
        ):
            view_id = str(row.get("id", "") or "").strip() or uuid.uuid4().hex
            created = str(row.get("created_at", "") or "").strip() or now
            values = [view_id, owner, view_name, serialized, created, now]
            _retry(lambda: worksheet.update(f"A{row_index}:F{row_index}", [values]))
            return view_id
    view_id = uuid.uuid4().hex
    values = [view_id, owner, view_name, serialized, now, now]
    _retry(lambda: worksheet.append_row(values, value_input_option="USER_ENTERED"))
    return view_id


def delete_saved_view(
    client, *, sheet_id: str | Sequence[str], username: str, view_id: str
) -> bool:
    """Elimina una vista solo si pertenece al usuario indicado."""
    owner = str(username or "").strip().lower()
    target = str(view_id or "").strip()
    if not owner or not target:
        return False
    spreadsheet = _open_spreadsheet(client, sheet_id, purpose="vistas guardadas")
    worksheet = _ensure_worksheet(spreadsheet, INTEL_VIEWS_WORKSHEET, INTEL_VIEWS_HEADERS)
    rows = _retry(lambda: worksheet.get_all_records()) or []
    for row_index, row in enumerate(rows, start=2):
        if (
            str(row.get("id", "") or "").strip() == target
            and str(row.get("username", "") or "").strip().lower() == owner
        ):
            _retry(lambda: worksheet.delete_rows(row_index))
            return True
    return False
