from __future__ import annotations

"""Clasificacion visible de actos que contienen fichas sin requisitos."""

import unicodedata
from collections.abc import Iterable

import pandas as pd


NO_REQUIREMENTS_SCOPE_COLUMN = "Tipo de acto sin requisitos"
NO_REQUIREMENTS_ONLY = "Solo fichas sin requisitos"
NO_REQUIREMENTS_MIXED = "Acto mixto"
NO_REQUIREMENTS_ALL = "Todos los actos sin requisitos"
NO_REQUIREMENTS_SHEETS = frozenset(
    {
        "cl_abiertas_rir_sin_requisitos",
        "cl_prog_sin_requisitos",
        "ap_sin_requisitos",
    }
)


def _normalized(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch)).strip().casefold()


def find_scope_column(columns: Iterable[object]) -> str | None:
    expected = _normalized(NO_REQUIREMENTS_SCOPE_COLUMN)
    return next((str(column) for column in columns if _normalized(column) == expected), None)


def filter_no_requirements_scope(
    frame: pd.DataFrame,
    *,
    sheet_name: str,
    selection: str,
) -> pd.DataFrame:
    """Filtra solo las tres vistas sin requisitos; el resto queda intacto."""

    if sheet_name not in NO_REQUIREMENTS_SHEETS or selection == NO_REQUIREMENTS_ALL:
        return frame.copy()
    if selection not in {NO_REQUIREMENTS_ONLY, NO_REQUIREMENTS_MIXED}:
        return frame.copy()

    scope_column = find_scope_column(frame.columns)
    if scope_column is None:
        return frame.iloc[0:0].copy()
    wanted = _normalized(selection)
    mask = frame[scope_column].map(_normalized).eq(wanted)
    return frame.loc[mask].copy()
