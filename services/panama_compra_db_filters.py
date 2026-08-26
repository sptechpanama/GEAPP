"""Filtros SQL seguros para el visor de la base de Panama Compra."""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Mapping, Sequence


_DATE_COLUMN_PRIORITY = (
    "fecha",
    "fecha_adjudicacion",
    "publicacion",
    "fecha_actualizacion",
    "ficha_detectada_at",
)


def _normalized_column_name(value: object) -> str:
    text = str(value or "").strip().lower()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^0-9a-z]+", "_", text).strip("_")


def _quote_identifier(identifier: str) -> str:
    return f'"{str(identifier).replace(chr(34), chr(34) * 2)}"'


def date_filter_columns(columns: Sequence[str]) -> list[str]:
    """Devuelve columnas que razonablemente contienen una fecha.

    Las columnas operativas mas utiles se muestran primero y el resto conserva
    el orden de la tabla. No se infiere una fecha a partir de columnas ajenas.
    """

    candidates: list[tuple[str, str]] = []
    for raw_column in columns:
        column = str(raw_column)
        normalized = _normalized_column_name(column)
        tokens = set(normalized.split("_"))
        if (
            "fecha" in tokens
            or "date" in tokens
            or normalized in {"publicacion", "publication", "created_at", "updated_at"}
            or normalized.endswith("_at")
        ):
            candidates.append((column, normalized))

    priority = {name: index for index, name in enumerate(_DATE_COLUMN_PRIORITY)}
    return [
        column
        for _position, (column, _normalized) in sorted(
            enumerate(candidates),
            key=lambda item: (
                priority.get(item[1][1], len(priority)),
                item[0],
            ),
        )
    ]


def normalized_date_sql_expression(*, backend: str, column: str) -> str:
    """Normaliza fechas ISO o dia-mes-anio a texto ISO comparable.

    Panama Compra contiene fechas historicas mixtas, por ejemplo
    ``2026-08-26 10:30:00``, ``26-08-2026`` y
    ``26/08/2026 - 02:00 PM a 04:00 PM``. La expresion toma la primera fecha
    valida sin intentar convertir el resto del texto.
    """

    quoted = _quote_identifier(column)
    raw = f"TRIM(CAST({quoted} AS TEXT))"
    backend_name = str(backend or "").strip().lower()

    if backend_name == "postgres":
        return (
            "(CASE "
            f"WHEN {raw} ~ '^[0-9]{{4}}[-/][0-9]{{2}}[-/][0-9]{{2}}' "
            f"THEN REPLACE(SUBSTRING({raw} FROM 1 FOR 10), '/', '-') "
            f"WHEN {raw} ~ '^[0-9]{{2}}[-/][0-9]{{2}}[-/][0-9]{{4}}' "
            f"THEN SUBSTRING({raw} FROM 7 FOR 4) || '-' || "
            f"SUBSTRING({raw} FROM 4 FOR 2) || '-' || SUBSTRING({raw} FROM 1 FOR 2) "
            "ELSE NULL END)"
        )

    if backend_name != "sqlite":
        raise ValueError(f"Backend no soportado para fechas: {backend}")

    return (
        "(CASE "
        f"WHEN SUBSTR({raw}, 5, 1) IN ('-', '/') "
        f"AND SUBSTR({raw}, 8, 1) IN ('-', '/') "
        f"THEN REPLACE(SUBSTR({raw}, 1, 10), '/', '-') "
        f"WHEN SUBSTR({raw}, 3, 1) IN ('-', '/') "
        f"AND SUBSTR({raw}, 6, 1) IN ('-', '/') "
        f"THEN SUBSTR({raw}, 7, 4) || '-' || SUBSTR({raw}, 4, 2) || '-' || "
        f"SUBSTR({raw}, 1, 2) "
        "ELSE NULL END)"
    )


def _iso_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    parsed = date.fromisoformat(text)
    return parsed.isoformat()


def append_date_range_condition(
    *,
    backend: str,
    columns: Sequence[str],
    where_sql: str,
    params: Sequence[object] | Mapping[str, object],
    column: str,
    start_date: date | datetime | str,
    end_date: date | datetime | str,
) -> tuple[str, list[object] | dict[str, object]]:
    """Anexa un rango inclusivo de fechas al filtro existente con ``AND``."""

    available = {str(item) for item in columns}
    if column not in available:
        raise ValueError("La columna seleccionada no pertenece a la tabla.")

    start_iso = _iso_date(start_date)
    end_iso = _iso_date(end_date)
    if start_iso > end_iso:
        raise ValueError("La fecha inicial no puede ser posterior a la fecha final.")

    expression = normalized_date_sql_expression(backend=backend, column=column)
    backend_name = str(backend or "").strip().lower()
    if backend_name == "postgres":
        output_params = dict(params)
        start_key = "_pc_date_start"
        end_key = "_pc_date_end"
        while start_key in output_params:
            start_key = "_" + start_key
        while end_key in output_params or end_key == start_key:
            end_key = "_" + end_key
        output_params[start_key] = start_iso
        output_params[end_key] = end_iso
        date_clause = (
            f"{expression} >= :{start_key} AND {expression} <= :{end_key}"
        )
    elif backend_name == "sqlite":
        output_params = list(params)
        output_params.extend([start_iso, end_iso])
        date_clause = f"{expression} >= ? AND {expression} <= ?"
    else:
        raise ValueError(f"Backend no soportado para fechas: {backend}")

    base = str(where_sql or "").strip()
    if base:
        return f"({base}) AND ({date_clause})", output_params
    return f"({date_clause})", output_params

