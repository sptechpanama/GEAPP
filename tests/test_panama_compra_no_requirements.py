from __future__ import annotations

import pandas as pd

from services.panama_compra_no_requirements import (
    NO_REQUIREMENTS_ALL,
    NO_REQUIREMENTS_MIXED,
    NO_REQUIREMENTS_ONLY,
    filter_no_requirements_scope,
    find_scope_column,
)


def _sample() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "acto": ["A", "B", "C"],
            "Tipo de acto sin requisitos": [
                NO_REQUIREMENTS_ONLY,
                NO_REQUIREMENTS_MIXED,
                "",
            ],
        }
    )


def test_filters_only_pure_or_mixed_acts() -> None:
    frame = _sample()
    pure = filter_no_requirements_scope(
        frame,
        sheet_name="cl_abiertas_rir_sin_requisitos",
        selection=NO_REQUIREMENTS_ONLY,
    )
    mixed = filter_no_requirements_scope(
        frame,
        sheet_name="cl_abiertas_rir_sin_requisitos",
        selection=NO_REQUIREMENTS_MIXED,
    )
    assert pure["acto"].tolist() == ["A"]
    assert mixed["acto"].tolist() == ["B"]


def test_all_option_and_unrelated_sheet_do_not_filter() -> None:
    frame = _sample()
    assert len(
        filter_no_requirements_scope(
            frame,
            sheet_name="cl_prog_sin_requisitos",
            selection=NO_REQUIREMENTS_ALL,
        )
    ) == 3
    assert len(
        filter_no_requirements_scope(
            frame,
            sheet_name="cl_abiertas",
            selection=NO_REQUIREMENTS_ONLY,
        )
    ) == 3


def test_missing_column_fails_closed_for_specific_filter() -> None:
    frame = pd.DataFrame({"acto": ["A"]})
    result = filter_no_requirements_scope(
        frame,
        sheet_name="ap_sin_requisitos",
        selection=NO_REQUIREMENTS_ONLY,
    )
    assert result.empty


def test_column_matching_is_accent_and_case_tolerant() -> None:
    assert (
        find_scope_column(["TIPO DE ACTO SIN REQUISITOS"])
        == "TIPO DE ACTO SIN REQUISITOS"
    )
