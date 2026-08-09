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
INTEL_STUDY_RUNS_WORKSHEET = "intel_study_runs_remote"
INTEL_PRIORITY_PORTFOLIO_WORKSHEET = "intel_priority_portfolio"
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
INTEL_PRIORITY_PORTFOLIO_HEADERS = [
    "batch_id",
    "scope_id",
    "created_at",
    "requested_by",
    "ficha",
    "nombre_ficha",
    "rank_score",
    "rank_monto_ficha_unica",
    "rank_actos_ficha_unica",
    "criterios_seleccion",
    "score_oportunidad",
    "monto_ficha_unica",
    "actos_ficha_unica",
    "estado",
    "intentos",
    "fecha_inicio",
    "fecha_fin",
    "request_id_ficha",
    "error",
]
DEFAULT_JOB_NAME = "intel_estudio_ficha"
DEFAULT_JOB_PYTHON = r"C:\Users\rodri\scrapers_repo\.venv\Scripts\python.exe"
DEFAULT_JOB_SCRIPT = r"C:\Users\rodri\scrapers_repo\orquestador\intel_ficha_worker.py"
PRIORITY_JOB_NAME = "intel_estudio_prioritario"
PRIORITY_JOB_SCRIPT = r"C:\Users\rodri\scrapers_repo\orquestador\intel_priority_worker.py"


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


def _sheet_value(value: Any) -> Any:
    """Convierte escalares de pandas/numpy en valores aceptados por Sheets."""

    if value is None:
        return ""
    try:
        if value != value:  # NaN
            return ""
    except (TypeError, ValueError):
        return ""
    if str(value).strip().lower() in {"<na>", "nan", "none", "null"}:
        return ""
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except (TypeError, ValueError):
            pass
    return value


def ensure_study_job(
    client,
    config_sheet_id: str,
    *,
    job_name: str = DEFAULT_JOB_NAME,
    python_executable: str = DEFAULT_JOB_PYTHON,
    script_path: str = DEFAULT_JOB_SCRIPT,
) -> None:
    spreadsheet = _retry(lambda: client.open_by_key(config_sheet_id))
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
    manual_sheet_id: str,
    config_sheet_id: str,
    requested_by: str,
    payload: Mapping[str, Any],
    notes: str = "",
    job_name: str = DEFAULT_JOB_NAME,
    python_executable: str = DEFAULT_JOB_PYTHON,
    script_path: str = DEFAULT_JOB_SCRIPT,
) -> str:
    ensure_study_job(
        client,
        config_sheet_id,
        job_name=job_name,
        python_executable=python_executable,
        script_path=script_path,
    )
    spreadsheet = _retry(lambda: client.open_by_key(manual_sheet_id))
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


def completed_study_fichas(
    client,
    *,
    sheet_id: str,
    scope_id: str = "",
) -> set[str]:
    """Fichas completadas dentro del mismo universo temporal y de filtros."""

    spreadsheet = _retry(lambda: client.open_by_key(sheet_id))
    try:
        worksheet = _retry(lambda: spreadsheet.worksheet(INTEL_STUDY_RUNS_WORKSHEET))
    except WorksheetNotFound:
        return set()
    rows = _retry(lambda: worksheet.get_all_records()) or []
    completed: set[str] = set()
    target_scope = str(scope_id or "").strip()
    for row in rows:
        state = str(row.get("estado_run", "") or "").strip().lower()
        row_scope = str(row.get("scope_id", "") or "").strip()
        if target_scope and row_scope != target_scope:
            continue
        ficha = "".join(character for character in str(row.get("ficha", "") or "") if character.isdigit())
        if ficha and state in {"completada", "completado", "done", "success", "completed"}:
            completed.add(ficha)
    return completed


def create_priority_portfolio(
    client,
    *,
    sheet_id: str,
    requested_by: str,
    scope_id: str,
    records: Sequence[Mapping[str, Any]],
    reuse_completed: bool = True,
    completed_fichas: set[str] | None = None,
) -> str:
    """Persiste una cartera deduplicada antes de encolar su procesamiento."""

    spreadsheet = _retry(lambda: client.open_by_key(sheet_id))
    worksheet = _ensure_worksheet(
        spreadsheet,
        INTEL_PRIORITY_PORTFOLIO_WORKSHEET,
        INTEL_PRIORITY_PORTFOLIO_HEADERS,
    )
    batch_id = uuid.uuid4().hex
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    completed = completed_fichas or set()
    rows: list[list[Any]] = []
    seen: set[str] = set()
    for raw_record in records:
        record = dict(raw_record)
        ficha = "".join(character for character in str(record.get("ficha", "") or "") if character.isdigit())
        if not ficha or ficha in seen:
            continue
        seen.add(ficha)
        state = "completado_previo" if reuse_completed and ficha in completed else "pendiente"
        output = {
            "batch_id": batch_id,
            "scope_id": str(scope_id or "").strip(),
            "created_at": now,
            "requested_by": str(requested_by or "desconocido").strip(),
            "ficha": ficha,
            "nombre_ficha": str(record.get("nombre_ficha", "") or "").strip(),
            "rank_score": record.get("rank_score", ""),
            "rank_monto_ficha_unica": record.get("rank_monto_ficha_unica", ""),
            "rank_actos_ficha_unica": record.get("rank_actos_ficha_unica", ""),
            "criterios_seleccion": str(record.get("criterios_seleccion", "") or ""),
            "score_oportunidad": record.get("score_oportunidad", 0),
            "monto_ficha_unica": record.get("monto_ficha_unica", 0),
            "actos_ficha_unica": record.get("actos_ficha_unica", 0),
            "estado": state,
            "intentos": 0,
            "fecha_inicio": "",
            "fecha_fin": now if state == "completado_previo" else "",
            "request_id_ficha": "",
            "error": "",
        }
        rows.append(
            [_sheet_value(output.get(column, "")) for column in INTEL_PRIORITY_PORTFOLIO_HEADERS]
        )
    if not rows:
        raise ValueError("La cartera prioritaria no contiene fichas válidas.")
    append_rows = getattr(worksheet, "append_rows", None)
    if callable(append_rows):
        _retry(lambda: append_rows(rows, value_input_option="USER_ENTERED"))
    else:
        for row in rows:
            _retry(lambda values=row: worksheet.append_row(values, value_input_option="USER_ENTERED"))
    return batch_id


def list_priority_portfolio(client, *, sheet_id: str, batch_id: str) -> list[dict[str, str]]:
    spreadsheet = _retry(lambda: client.open_by_key(sheet_id))
    worksheet = _ensure_worksheet(
        spreadsheet,
        INTEL_PRIORITY_PORTFOLIO_WORKSHEET,
        INTEL_PRIORITY_PORTFOLIO_HEADERS,
    )
    rows = _retry(lambda: worksheet.get_all_records()) or []
    target = str(batch_id or "").strip()
    return [
        {str(key): str(value if value is not None else "") for key, value in row.items()}
        for row in rows
        if str(row.get("batch_id", "") or "").strip() == target
    ]


def queue_priority_portfolio(
    client,
    *,
    manual_sheet_id: str,
    config_sheet_id: str,
    requested_by: str,
    payload: Mapping[str, Any],
    notes: str = "",
) -> str:
    return queue_study(
        client,
        manual_sheet_id=manual_sheet_id,
        config_sheet_id=config_sheet_id,
        requested_by=requested_by,
        payload=payload,
        notes=notes,
        job_name=PRIORITY_JOB_NAME,
        python_executable=DEFAULT_JOB_PYTHON,
        script_path=PRIORITY_JOB_SCRIPT,
    )


def get_request_status(client, *, manual_sheet_id: str, request_id: str) -> dict[str, str]:
    spreadsheet = _retry(lambda: client.open_by_key(manual_sheet_id))
    worksheet = _ensure_worksheet(spreadsheet, PC_MANUAL_WORKSHEET, PC_MANUAL_HEADERS)
    rows = _retry(lambda: worksheet.get_all_records()) or []
    for row in reversed(rows):
        if str(row.get("id", "")).strip() == str(request_id).strip():
            return {str(key): str(value or "") for key, value in row.items()}
    return {}


def list_saved_views(client, *, sheet_id: str, username: str) -> list[dict[str, Any]]:
    """Devuelve únicamente las vistas del usuario autenticado."""
    spreadsheet = _retry(lambda: client.open_by_key(sheet_id))
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
    sheet_id: str,
    username: str,
    name: str,
    payload: Mapping[str, Any],
) -> str:
    """Crea o reemplaza por nombre una vista del usuario."""
    owner = str(username or "").strip().lower()
    view_name = str(name or "").strip()
    if not owner or not view_name:
        raise ValueError("Usuario y nombre de vista son obligatorios.")
    spreadsheet = _retry(lambda: client.open_by_key(sheet_id))
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


def delete_saved_view(client, *, sheet_id: str, username: str, view_id: str) -> bool:
    """Elimina una vista solo si pertenece al usuario indicado."""
    owner = str(username or "").strip().lower()
    target = str(view_id or "").strip()
    if not owner or not target:
        return False
    spreadsheet = _retry(lambda: client.open_by_key(sheet_id))
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
