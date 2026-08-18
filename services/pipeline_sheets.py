from __future__ import annotations

"""Replica idempotente del Pipeline Estrategico hacia Google Sheets.

La hoja es una copia operacional/auditable; nunca se usa para confirmar una
escritura en Supabase.  Si Google falla, el outbox queda pendiente y se puede
reintentar sin duplicar filas.
"""

import time
from typing import Any, Mapping, Sequence

from services.pipeline_estrategico import MIRROR_COLUMNS, PipelineRepository, clean_text

try:
    from gspread.exceptions import APIError, WorksheetNotFound
except ImportError:  # pragma: no cover - solo para validaciones sin gspread
    class APIError(Exception):
        pass

    class WorksheetNotFound(Exception):
        pass


WORKSHEETS = {
    "card": "pipeline_cards",
    "checkpoint": "pipeline_checkpoints",
    "contact": "pipeline_contacts",
    "document": "pipeline_documents",
    "activity": "pipeline_activity",
}


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
        except Exception as exc:  # cortes SSL/transitorios tambien se reintentan
            last_error = exc
        if attempt < attempts - 1:
            time.sleep(min(8.0, 0.6 * (2**attempt)))
    if last_error:
        raise last_error
    raise RuntimeError("Operación de Google Sheets no ejecutada.")


def _open_spreadsheet(client, sheet_ids: str | Sequence[str]):
    candidates = [sheet_ids] if isinstance(sheet_ids, str) else list(sheet_ids)
    candidates = [clean_text(value) for value in candidates if clean_text(value)]
    if not candidates:
        raise RuntimeError(
            "Configura PIPELINE_SHEET_ID o SHEET_ID para activar la replica del pipeline."
        )
    last_error: Exception | None = None
    for sheet_id in candidates:
        try:
            return _retry(lambda sid=sheet_id: client.open_by_key(sid))
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        "No se encontro una hoja nativa de Google Sheets para el pipeline. "
        f"Ultimo error: {last_error}"
    )


def _ensure_worksheet(spreadsheet, title: str, headers: Sequence[str]):
    try:
        worksheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        worksheet = _retry(
            lambda: spreadsheet.add_worksheet(
                title=title, rows=1000, cols=max(10, len(headers))
            )
        )
    values = _retry(worksheet.get_all_values) or []
    if not values:
        _retry(lambda: worksheet.update("A1", [list(headers)]))
        return worksheet, []
    current_headers = [clean_text(value) for value in values[0]]
    if current_headers[: len(headers)] != list(headers):
        # Conserva las filas existentes por id y las reescribe con el esquema
        # canonico. Las columnas desconocidas no son fuente de verdad.
        records = []
        for raw_row in values[1:]:
            records.append(
                {
                    header: raw_row[index] if index < len(raw_row) else ""
                    for index, header in enumerate(current_headers)
                    if header
                }
            )
        return worksheet, records
    return worksheet, [
        {
            header: raw_row[index] if index < len(raw_row) else ""
            for index, header in enumerate(headers)
        }
        for raw_row in values[1:]
        if any(clean_text(value) for value in raw_row)
    ]


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _apply_events(
    existing: list[dict[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_id = {
        clean_text(row.get("id")): dict(row)
        for row in existing
        if clean_text(row.get("id"))
    }
    order = [clean_text(row.get("id")) for row in existing if clean_text(row.get("id"))]
    for event in events:
        payload = dict(event.get("payload_data") or {})
        entity_id = clean_text(event.get("entity_id") or payload.get("id"))
        if not entity_id:
            continue
        if clean_text(event.get("operation")) == "archive" and event.get("entity_type") == "checkpoint":
            by_id.pop(entity_id, None)
            order = [value for value in order if value != entity_id]
            continue
        payload["id"] = entity_id
        if entity_id not in by_id:
            order.append(entity_id)
        by_id[entity_id] = payload
    return [by_id[item_id] for item_id in order if item_id in by_id]


class PipelineSheetsMirror:
    def __init__(
        self,
        *,
        client,
        sheet_ids: str | Sequence[str],
        repository: PipelineRepository,
    ) -> None:
        self.client = client
        self.sheet_ids = sheet_ids
        self.repository = repository

    def sync_pending(self, *, limit: int = 200) -> dict[str, Any]:
        events = self.repository.list_outbox(limit=limit)
        if not events:
            return {"synced": 0, "errors": 0, "pending": 0}
        spreadsheet = _open_spreadsheet(self.client, self.sheet_ids)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            grouped.setdefault(clean_text(event.get("entity_type")), []).append(event)
        synced = 0
        errors = 0
        for entity_type, group in grouped.items():
            if entity_type not in WORKSHEETS or entity_type not in MIRROR_COLUMNS:
                for event in group:
                    self.repository.mark_outbox_error(
                        event["id"], f"Tipo de entidad no soportado: {entity_type}"
                    )
                errors += len(group)
                continue
            try:
                headers = MIRROR_COLUMNS[entity_type]
                worksheet, existing = _ensure_worksheet(
                    spreadsheet, WORKSHEETS[entity_type], headers
                )
                merged = _apply_events(existing, group)
                matrix = [list(headers)] + [
                    [_cell(row.get(column, "")) for column in headers]
                    for row in merged
                ]
                _retry(worksheet.clear)
                _retry(lambda ws=worksheet, data=matrix: ws.update("A1", data))
                self.repository.mark_outbox_synced(event["id"] for event in group)
                synced += len(group)
            except Exception as exc:
                for event in group:
                    self.repository.mark_outbox_error(event["id"], exc)
                errors += len(group)
        counts = self.repository.outbox_counts()
        return {
            "synced": synced,
            "errors": errors,
            "pending": counts["pending"] + counts["error"],
        }


__all__ = ["PipelineSheetsMirror", "WORKSHEETS"]
