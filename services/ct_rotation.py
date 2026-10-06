from __future__ import annotations

import calendar
import math
import re
from datetime import date, datetime, timedelta
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from services.anestesia_storage import SHEET_ID
from services.inteligencia_orquestador_v3 import _ensure_worksheet, _open_spreadsheet, _retry


FICHA = "43358"
START = date(2025, 1, 1)
PANAMA = ZoneInfo("America/Panama")
DAYS_PER_MONTH = 365.25 / 12
WIN_SHARE = 0.60
LEAD_MONTHS = 2
SHIPMENT_KITS = 6000
HISTORY_SHEET = "ct_rotacion_actos"
INVENTORY_SHEET = "ct_rotacion_inventario"
STATUS_SHEET = "ct_rotacion_estado"
HISTORY_HEADERS = [
    "ficha", "convocatoria", "publicacion", "entidad", "unidad_compra", "hospital",
    "kits", "estado", "actos_relacionados", "demanda_id", "requisicion", "enlace",
    "verificado_en", "actualizado_en",
]
INVENTORY_HEADERS = ["ficha", "kits", "fecha", "actualizado_en", "actualizado_por"]
STATUS_HEADERS = ["ficha", "ultimo_exito", "ultimo_intento", "error"]


def now() -> datetime:
    return datetime.now(PANAMA)


def _number(value: object) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("La cantidad debe ser un número válido no negativo.")
    return result


def _day(value: object) -> date:
    return date.fromisoformat(str(value)[:10])


def _codes(record: Mapping) -> set[str]:
    return {
        str(record.get("convocatoria", "")).strip(),
        *(code.strip() for code in str(record.get("actos_relacionados", "")).split(",")),
    } - {""}


def merge_history(existing: Sequence[Mapping], incoming: Sequence[Mapping]) -> list[dict]:
    output = []
    for raw in [*existing, *incoming]:
        row = {column: str(raw.get(column, "") if raw.get(column) is not None else "") for column in HISTORY_HEADERS}
        if row["ficha"] != FICHA or not row["convocatoria"]:
            raise ValueError("El histórico requiere ficha y convocatoria oficiales.")
        _day(row["publicacion"])
        if _number(row["kits"]) <= 0:
            raise ValueError("Cada solicitud debe tener una cantidad de kits verificada.")
        matches = [item for item in output if _codes(item) & _codes(row)]
        if not matches:
            output.append(row)
            continue
        versions = [*matches, row]
        originals = [item for item in versions if "-CL-" in item["convocatoria"]]
        first = min(originals or versions, key=lambda item: (item["publicacion"], item["convocatoria"]))
        latest = max(enumerate(versions), key=lambda pair: (pair[1]["actualizado_en"], pair[0]))[1]
        merged = dict(latest)
        for column in ("convocatoria", "publicacion", "kits", "enlace"):
            merged[column] = first[column]
        same_original = [item for item in versions if item["convocatoria"] == first["convocatoria"]]
        original_latest = max(enumerate(same_original), key=lambda pair: (pair[1]["actualizado_en"], pair[0]))[1]
        merged["kits"] = original_latest["kits"]
        for column in ("hospital", "unidad_compra", "entidad", "requisicion"):
            merged[column] = original_latest[column] or first[column]
        merged["demanda_id"] = original_latest["demanda_id"] or first["convocatoria"]
        merged["actos_relacionados"] = ",".join(sorted(set().union(*(_codes(item) for item in versions))))
        output = [item for item in output if item not in matches]
        output.append(merged)
    return sorted(output, key=lambda row: (row["publicacion"], row["convocatoria"]))


def distinct_needs(records: Sequence[Mapping], *, today: date) -> list[dict]:
    groups = {}
    for row in merge_history([], records):
        if _day(row["publicacion"]) > today:
            continue
        key = row["demanda_id"] or row["convocatoria"]
        groups.setdefault(key, []).append(row)
    output = []
    for versions in groups.values():
        usable = [row for row in versions if not re.search(r"cancel|anulad", row["estado"], re.I)]
        if not usable:
            continue
        first = min(versions, key=lambda row: (row["publicacion"], row["convocatoria"]))
        latest = max(usable, key=lambda row: (row["publicacion"], row["actualizado_en"]))
        row = dict(first)
        row["estado"] = latest["estado"]
        row["kits"] = latest["kits"]
        output.append(row)
    return sorted(output, key=lambda row: (row["publicacion"], row["convocatoria"]))


def elapsed_months(start: date, end: date) -> float:
    if end < start:
        return 0.0
    whole = (end.year - start.year) * 12 + end.month - start.month
    fraction = (end.day - 1) / calendar.monthrange(end.year, end.month)[1]
    fraction -= (start.day - 1) / calendar.monthrange(start.year, start.month)[1]
    return max(1 / calendar.monthrange(start.year, start.month)[1], whole + fraction)


def rotation_summary(records: Sequence[Mapping], *, today: date) -> dict:
    needs = distinct_needs(records, today=today)
    selected = [row for row in needs if _day(row["publicacion"]) >= START]
    exposure = elapsed_months(START, today)
    units = {}
    for row in needs:
        hospital = row["hospital"] or row["unidad_compra"] or row["entidad"]
        units.setdefault(hospital, []).append(row)
    forecast = []
    for hospital, rows in units.items():
        dates = sorted(_day(row["publicacion"]) for row in rows)
        interval_days = sum((right - left).days for left, right in zip(dates, dates[1:])) / (len(dates) - 1) if len(dates) > 1 else None
        expected = dates[-1] + timedelta(days=round(interval_days)) if interval_days is not None else None
        forecast.append({
            "Entidad / hospital": hospital,
            "Última solicitud": dates[-1],
            "Frecuencia promedio (meses)": interval_days / DAYS_PER_MONTH if interval_days is not None else None,
            "Próxima solicitud estimada": expected,
            "Días hasta la fecha estimada": (expected - today).days if expected else None,
        })
    forecast.sort(key=lambda row: (row["Próxima solicitud estimada"] or date.max, row["Entidad / hospital"]))
    kits = sum(_number(row["kits"]) for row in selected)
    return {
        "acts_per_month": len(selected) / exposure if exposure else 0.0,
        "kits_per_month": kits / exposure if exposure else 0.0,
        "acts": len(selected),
        "kits": kits,
        "forecast": pd.DataFrame(forecast),
    }


def shift_months(day: date, months: int) -> date:
    absolute = day.year * 12 + day.month - 1 + months
    year, month_index = divmod(absolute, 12)
    month = month_index + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def inventory_plan(kits: object, inventory_date: date, monthly_kits: float, *, today: date) -> dict:
    stock = _number(kits)
    if not stock.is_integer():
        raise ValueError("El inventario debe expresarse en kits enteros.")
    if inventory_date > today:
        raise ValueError("La fecha del inventario no puede estar en el futuro.")
    consumption = _number(monthly_kits) * WIN_SHARE
    elapsed_days = (today - inventory_date).days
    estimated_stock = max(0.0, stock - consumption * elapsed_days / DAYS_PER_MONTH)
    coverage = estimated_stock / consumption if consumption else None
    arrival = shift_months(today, LEAD_MONTHS)
    depletion = today + timedelta(days=math.floor(coverage * DAYS_PER_MONTH)) if coverage is not None else None
    order_date = shift_months(depletion, -LEAD_MONTHS) if depletion else None
    required = consumption * (arrival - today).days / DAYS_PER_MONTH
    deficit = max(0.0, required - estimated_stock)
    shipments = max(1, math.ceil(deficit / SHIPMENT_KITS)) if order_date is not None and order_date <= today else 0
    return {
        "monthly_consumption": consumption,
        "estimated_stock": estimated_stock,
        "coverage_months": coverage,
        "order_date": order_date,
        "shipments_now": shipments,
        "shipment_coverage_months": SHIPMENT_KITS / consumption if consumption else None,
    }


def _records(book, title: str, headers: list[str]) -> list[dict]:
    from gspread.exceptions import WorksheetNotFound

    try:
        worksheet = book.worksheet(title)
    except WorksheetNotFound:
        return []
    rows = _retry(lambda: worksheet.get_all_values())
    if not rows:
        return []
    if rows[0][:len(headers)] != headers:
        raise ValueError(f"Encabezados inesperados en {title}; se conserva el contenido.")
    return [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in rows[1:] if any(str(value).strip() for value in row)]


def load_snapshot(client, *, sheet_id: str = SHEET_ID) -> dict:
    book = _open_spreadsheet(client, sheet_id, purpose="rotación e inventario CT")
    records = _records(book, HISTORY_SHEET, HISTORY_HEADERS)
    inventory = next((row for row in _records(book, INVENTORY_SHEET, INVENTORY_HEADERS) if row["ficha"] == FICHA), None)
    status = next((row for row in _records(book, STATUS_SHEET, STATUS_HEADERS) if row["ficha"] == FICHA), {})
    if status.get("ultimo_exito") and not records:
        raise ValueError("El histórico publicado no puede sustituirse por una lectura vacía.")
    return {"records": records, "inventory": inventory, "status": status}


def publish_history(client, records: Sequence[Mapping], *, sheet_id: str = SHEET_ID) -> list[dict]:
    book = _open_spreadsheet(client, sheet_id, purpose="rotación CT")
    existing = _records(book, HISTORY_SHEET, HISTORY_HEADERS)
    merged = merge_history(existing, records)
    worksheet = _ensure_worksheet(book, HISTORY_SHEET, HISTORY_HEADERS)
    values = [HISTORY_HEADERS, *[[row.get(column, "") for column in HISTORY_HEADERS] for row in merged]]
    values.extend([[""] * len(HISTORY_HEADERS) for _ in range(max(0, len(existing) - len(merged)))])
    if len(values) > worksheet.row_count:
        _retry(lambda: worksheet.resize(rows=len(values) + 100))
    _retry(lambda: worksheet.update(range_name="A1", values=values, value_input_option="RAW"))
    return merged


def save_inventory(client, kits: object, inventory_date: date, actor: str, *, sheet_id: str = SHEET_ID) -> dict:
    inventory_plan(kits, inventory_date, 0, today=now().date())
    book = _open_spreadsheet(client, sheet_id, purpose="inventario CT")
    existing = _records(book, INVENTORY_SHEET, INVENTORY_HEADERS)
    row = {"ficha": FICHA, "kits": str(int(_number(kits))), "fecha": inventory_date.isoformat(), "actualizado_en": now().isoformat(), "actualizado_por": actor}
    worksheet = _ensure_worksheet(book, INVENTORY_SHEET, INVENTORY_HEADERS)
    index = next((position for position, record in enumerate(existing, start=2) if record["ficha"] == FICHA), None)
    values = [row[column] for column in INVENTORY_HEADERS]
    if index is None:
        _retry(lambda: worksheet.append_row(values, value_input_option="RAW"))
    else:
        _retry(lambda: worksheet.update(range_name=f"A{index}:E{index}", values=[values], value_input_option="RAW"))
    return row


def save_refresh_status(client, error: str = "", *, sheet_id: str = SHEET_ID) -> None:
    book = _open_spreadsheet(client, sheet_id, purpose="actualización de rotación CT")
    existing = _records(book, STATUS_SHEET, STATUS_HEADERS)
    previous = next((row for row in existing if row["ficha"] == FICHA), {})
    stamp = now().isoformat()
    values = [[FICHA, previous.get("ultimo_exito", "") if error else stamp, stamp, error]]
    worksheet = _ensure_worksheet(book, STATUS_SHEET, STATUS_HEADERS)
    index = next((position for position, row in enumerate(existing, start=2) if row["ficha"] == FICHA), 2)
    _retry(lambda: worksheet.update(range_name=f"A{index}:D{index}", values=values, value_input_option="RAW"))
