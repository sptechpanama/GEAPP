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


def filter_eligible_no_requirements(
    frame: pd.DataFrame,
    *,
    sheet_name: str,
) -> pd.DataFrame:
    """Oculta mezclas globales o sin modalidad confirmada en las vistas SR.

    Durante una transición de esquema conserva la vista anterior si todavía no
    existe alguna de las dos columnas necesarias. En cuanto el scraper publica
    ambas, solo admite actos puros y mezclas oficiales por renglón.
    """

    if sheet_name not in NO_REQUIREMENTS_SHEETS:
        return frame.copy()
    scope_column = find_scope_column(frame.columns)
    adjudication_column = find_adjudication_column(frame.columns)
    if scope_column is None or adjudication_column is None:
        return frame.copy()

    scopes = frame[scope_column].map(_normalized)
    mixed = scopes.eq(_normalized(NO_REQUIREMENTS_MIXED))
    by_line = frame[adjudication_column].map(is_line_adjudication)
    return frame.loc[~mixed | by_line].copy()


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
