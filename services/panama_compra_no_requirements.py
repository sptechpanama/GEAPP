from __future__ import annotations

"""Clasificacion visible de actos que contienen fichas sin requisitos."""

import re
import unicodedata
from collections.abc import Iterable

import pandas as pd


NO_REQUIREMENTS_SCOPE_COLUMN = "Tipo de acto sin requisitos"
NO_REQUIREMENTS_ONLY = "Solo fichas sin requisitos"
NO_REQUIREMENTS_MIXED = "Acto mixto"
NO_REQUIREMENTS_ALL = "Todos los actos sin requisitos"
ADJUDICATION_TYPE_COLUMN = "Tipo de adjudicación"
ADJUDICATION_BY_LINE = "Renglón"
NO_REQUIREMENTS_FICHAS_COLUMN = "Fichas sin requisitos"
REQUIREMENTS_FICHAS_COLUMN = "Fichas con requisitos"
UNCLASSIFIED_FICHAS_COLUMN = "Fichas por verificar"
EMPTY_FICHAS_VALUE = "Ninguna"
UNKNOWN_ADJUDICATION_VALUE = "No identificado"
NO_REQUIREMENTS_SHEETS = frozenset(
    {
        "cl_abiertas_rir_sin_requisitos",
        "cl_prog_sin_requisitos",
        "ap_sin_requisitos",
    }
)


def _normalized(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split()).casefold()


def is_line_adjudication(value: object) -> bool:
    normalized = _normalized(value)
    return bool(
        re.search(r"\brenglon(?:es)?\b", normalized)
        or re.search(r"\bpor (?:linea|lineas|item|items)\b", normalized)
        or "parcial" in normalized
    )


def find_scope_column(columns: Iterable[object]) -> str | None:
    expected = _normalized(NO_REQUIREMENTS_SCOPE_COLUMN)
    return next((str(column) for column in columns if _normalized(column) == expected), None)


def find_adjudication_column(columns: Iterable[object]) -> str | None:
    expected = _normalized(ADJUDICATION_TYPE_COLUMN)
    return next((str(column) for column in columns if _normalized(column) == expected), None)


def _find_column(columns: Iterable[object], expected_name: str) -> str | None:
    expected = _normalized(expected_name)
    return next((str(column) for column in columns if _normalized(column) == expected), None)


def normalize_no_requirements_metadata(
    frame: pd.DataFrame,
    *,
    sheet_name: str,
) -> pd.DataFrame:
    """Evita valores nulos visibles durante migraciones o lecturas parciales.

    La clasificación verdadera se escribe en Google Sheets por los scrapers.
    Esta capa solo presenta un valor explícito cuando una celda antigua llega
    vacía, sin inventar una ficha ni una modalidad.
    """

    result = frame.copy()
    if sheet_name not in NO_REQUIREMENTS_SHEETS:
        return result

    fallbacks = {
        NO_REQUIREMENTS_FICHAS_COLUMN: EMPTY_FICHAS_VALUE,
        REQUIREMENTS_FICHAS_COLUMN: EMPTY_FICHAS_VALUE,
        UNCLASSIFIED_FICHAS_COLUMN: EMPTY_FICHAS_VALUE,
        ADJUDICATION_TYPE_COLUMN: UNKNOWN_ADJUDICATION_VALUE,
    }
    for expected_name, fallback in fallbacks.items():
        column = _find_column(result.columns, expected_name)
        if column is None:
            continue
        values = result[column]
        empty = values.isna() | values.astype(str).str.strip().eq("")
        result.loc[empty, column] = fallback
    return result


def filter_eligible_no_requirements(
    frame: pd.DataFrame,
    *,
    sheet_name: str,
) -> pd.DataFrame:
    """Admite solo actos puros o mixtos adjudicables por renglón.

    La validación es cerrada: si falta la clasificación o la modalidad oficial,
    la fila tampoco se muestra. Así un acto mixto global nunca puede filtrarse
    accidentalmente como una oportunidad sin requisitos.
    """

    normalized_frame = normalize_no_requirements_metadata(
        frame,
        sheet_name=sheet_name,
    )
    if sheet_name not in NO_REQUIREMENTS_SHEETS:
        return normalized_frame
    scope_column = find_scope_column(normalized_frame.columns)
    adjudication_column = find_adjudication_column(normalized_frame.columns)
    if scope_column is None or adjudication_column is None:
        # Sin ambas evidencias no es seguro presentar el acto como elegible.
        return normalized_frame.iloc[0:0].copy()

    scopes = normalized_frame[scope_column].map(_normalized)
    only_no_requirements = scopes.eq(_normalized(NO_REQUIREMENTS_ONLY))
    mixed = scopes.eq(_normalized(NO_REQUIREMENTS_MIXED))
    by_line = normalized_frame[adjudication_column].map(is_line_adjudication)
    eligible = only_no_requirements | (mixed & by_line)
    return normalized_frame.loc[eligible].copy()


def filter_no_requirements_scope(
    frame: pd.DataFrame,
    *,
    sheet_name: str,
    selection: str,
) -> pd.DataFrame:
    """Filtra solo las tres vistas sin requisitos; el resto queda intacto."""

    eligible = filter_eligible_no_requirements(frame, sheet_name=sheet_name)
    if sheet_name not in NO_REQUIREMENTS_SHEETS or selection == NO_REQUIREMENTS_ALL:
        return eligible
    if selection not in {NO_REQUIREMENTS_ONLY, NO_REQUIREMENTS_MIXED}:
        return eligible

    scope_column = find_scope_column(eligible.columns)
    if scope_column is None:
        return eligible.iloc[0:0].copy()
    wanted = _normalized(selection)
    mask = eligible[scope_column].map(_normalized).eq(wanted)
    return eligible.loc[mask].copy()
