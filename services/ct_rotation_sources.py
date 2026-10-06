from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Mapping, Sequence
from urllib.parse import unquote

from services import ct_rotation as rotation
from services.ct_rotation_capture import document_ficha_codes, selected_kit_lines
from services.inteligencia_proveedores_v3 import normalize_text


PROCESS = re.compile(r"\b\d{4}(?:-\d+){2,6}-(?:CL|CM|LP|SCM|SCA|PE|LA|AV)-\d+\b", re.I)
SOURCE_COLUMNS = [
    "enlace", "publicacion", "fecha_actualizacion", "entidad", "unidad_solic",
    "estado", "items_json",
]


def process_code(value: object) -> str:
    match = PROCESS.search(unquote(str(value or "")))
    return match[0].upper() if match else ""


def source_day(value: object) -> date:
    text = str(value or "").strip()[:10]
    for pattern in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            pass
    raise ValueError("Fecha de publicación no verificable.")


def source_stamp(value: object) -> str:
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=rotation.PANAMA)
    return stamp.astimezone(rotation.PANAMA).isoformat()


def _connect(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"No existe la base extraída por las corridas normales: {path}")
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def read_existing_history(root: Path, existing: Sequence[Mapping], *, today: date) -> tuple[list[dict], list[str]]:
    known = {code: row for row in existing for code in rotation._codes(row)}
    with closing(_connect(root / "data/db/inteligencia_proveedores.db")) as analytics:
        indexed = analytics.execute(
            "SELECT source_id, enlace FROM intel_actos_fichas WHERE ficha = ?",
            (rotation.FICHA,),
        ).fetchall()
    identifiers = sorted({str(row["source_id"]) for row in indexed if row["source_id"]})
    links = sorted({str(row["enlace"]) for row in [*indexed, *existing] if row["enlace"]})
    incoming, pending = [], []
    with closing(_connect(root / "data/db/panamacompra.db")) as source:
        columns = ",".join(SOURCE_COLUMNS)
        candidates = {}
        for field, values in (("id", identifiers), ("enlace", links)):
            for offset in range(0, len(values), 400):
                batch = values[offset:offset + 400]
                placeholders = ",".join("?" for _ in batch)
                for row in source.execute(f"SELECT {columns} FROM actos_publicos WHERE {field} IN ({placeholders})", batch):
                    candidates[row["enlace"]] = dict(row)
        relations = {}
        lifecycle_columns = {row["name"] for row in source.execute("PRAGMA table_info(cl_cotizaciones)")}
        if {"numero_cl", "successor_process_number"} <= lifecycle_columns:
            for row in source.execute("SELECT numero_cl, successor_process_number FROM cl_cotizaciones WHERE successor_process_number IS NOT NULL"):
                original, successor = process_code(row["numero_cl"]), process_code(row["successor_process_number"])
                if original and successor:
                    relations[successor] = original
        for raw in candidates.values():
            code = process_code(raw["enlace"])
            if not code:
                continue
            previous = known.get(code) or known.get(relations.get(code)) or {}
            try:
                items = json.loads(raw["items_json"] or "[]")
                if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
                    raise ValueError("Renglones no estructurados.")
                explicit = [item for item in items if document_ficha_codes(str(item.get("descripcion", ""))) == {rotation.FICHA}]
                lines = explicit or (selected_kit_lines(items, confirmed=True) if previous else [])
                if not lines:
                    continue
                publication = source_day(raw["publicacion"])
                if publication > today:
                    continue
                if any(normalize_text(item.get("unidad", "")) not in {"unidad", "kit"} for item in lines):
                    raise ValueError("Unidad de cantidad pendiente.")
                quantity = sum(rotation._number(item.get("cantidad")) for item in lines)
                if quantity <= 0:
                    raise ValueError("Cantidad de kits pendiente.")
                stamp = source_stamp(raw["fecha_actualizacion"])
            except (TypeError, ValueError):
                if not previous:
                    pending.append(code)
                continue
            related = rotation._codes(previous) | {code}
            if code in relations:
                related.add(relations[code])
            incoming.append({
                "ficha": rotation.FICHA, "convocatoria": code,
                "publicacion": publication.isoformat(), "enlace": raw["enlace"],
                "entidad": raw["entidad"] or previous.get("entidad", ""),
                "unidad_compra": raw["unidad_solic"] or previous.get("unidad_compra", ""),
                "hospital": previous.get("hospital") or raw["unidad_solic"] or raw["entidad"],
                "kits": quantity, "estado": raw["estado"] or previous.get("estado", ""),
                "actos_relacionados": ",".join(sorted(related)),
                "demanda_id": previous.get("demanda_id") or relations.get(code) or code,
                "requisicion": previous.get("requisicion", ""),
                "verificado_en": stamp, "actualizado_en": stamp,
            })
    return rotation.merge_history(existing, incoming), sorted(set(pending))
