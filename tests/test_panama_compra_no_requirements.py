from __future__ import annotations

import pandas as pd

from services.panama_compra_no_requirements import (
    ADJUDICATION_TYPE_COLUMN,
    EMPTY_FICHAS_VALUE,
    NO_REQUIREMENTS_ALL,
    NO_REQUIREMENTS_FICHAS_COLUMN,
    NO_REQUIREMENTS_MIXED,
    NO_REQUIREMENTS_ONLY,
    REQUIREMENTS_FICHAS_COLUMN,
    UNCLASSIFIED_FICHAS_COLUMN,
    UNKNOWN_ADJUDICATION_VALUE,
    filter_eligible_no_requirements,
    filter_no_requirements_scope,
    find_adjudication_column,
    find_scope_column,
    is_line_adjudication,
    normalize_no_requirements_metadata,
)


def _sample() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "acto": ["A", "B", "C", "D"],
            "Tipo de acto sin requisitos": [
                NO_REQUIREMENTS_ONLY,
                NO_REQUIREMENTS_MIXED,
                NO_REQUIREMENTS_MIXED,
                NO_REQUIREMENTS_MIXED,
            ],
            ADJUDICATION_TYPE_COLUMN: ["Global", "Renglón", "Global", ""],
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


def test_global_and_unknown_mixed_acts_are_hidden_even_in_all_view() -> None:
    result = filter_eligible_no_requirements(
        _sample(), sheet_name="ap_sin_requisitos"
    )
    assert result["acto"].tolist() == ["A", "B"]


def test_all_option_and_unrelated_sheet_do_not_filter() -> None:
    frame = _sample()
    assert len(
        filter_no_requirements_scope(
            frame,
            sheet_name="cl_prog_sin_requisitos",
            selection=NO_REQUIREMENTS_ALL,
        )
    ) == 2
    assert len(
        filter_no_requirements_scope(
            frame,
            sheet_name="cl_abiertas",
            selection=NO_REQUIREMENTS_ONLY,
        )
    ) == 4


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
    assert find_adjudication_column(["TIPO DE ADJUDICACION"]) == "TIPO DE ADJUDICACION"


def test_transition_without_adjudication_column_preserves_all_view() -> None:
    frame = _sample().drop(columns=[ADJUDICATION_TYPE_COLUMN])
    result = filter_no_requirements_scope(
        frame,
        sheet_name="cl_prog_sin_requisitos",
        selection=NO_REQUIREMENTS_ALL,
    )
    assert result["acto"].tolist() == ["A", "B", "C", "D"]


def test_line_adjudication_variants_are_tolerated() -> None:
    assert is_line_adjudication("Renglón")
    assert is_line_adjudication("Por renglones")
    assert is_line_adjudication("Parcial por ítem")
    assert is_line_adjudication("Adjudicación parcial")
    assert is_line_adjudication("Por línea")
    assert not is_line_adjudication("Global")
    assert not is_line_adjudication("")


def test_null_metadata_is_rendered_explicitly_instead_of_none() -> None:
    frame = pd.DataFrame(
        {
            "acto": ["A", "B"],
            NO_REQUIREMENTS_FICHAS_COLUMN: [None, "100"],
            REQUIREMENTS_FICHAS_COLUMN: ["", "300 (CT)"],
            UNCLASSIFIED_FICHAS_COLUMN: [pd.NA, ""],
            ADJUDICATION_TYPE_COLUMN: [None, "Renglón"],
        }
    )

    result = normalize_no_requirements_metadata(
        frame,
        sheet_name="cl_abiertas_rir_sin_requisitos",
    )

    assert result[NO_REQUIREMENTS_FICHAS_COLUMN].tolist() == [
        EMPTY_FICHAS_VALUE,
        "100",
    ]
    assert result[REQUIREMENTS_FICHAS_COLUMN].tolist() == [
        EMPTY_FICHAS_VALUE,
        "300 (CT)",
    ]
    assert result[UNCLASSIFIED_FICHAS_COLUMN].tolist() == [
        EMPTY_FICHAS_VALUE,
        EMPTY_FICHAS_VALUE,
    ]
    assert result[ADJUDICATION_TYPE_COLUMN].tolist() == [
        UNKNOWN_ADJUDICATION_VALUE,
        "Renglón",
    ]


def test_null_metadata_normalization_does_not_touch_other_sheets() -> None:
    frame = pd.DataFrame({REQUIREMENTS_FICHAS_COLUMN: [None]})
    result = normalize_no_requirements_metadata(frame, sheet_name="ap_con_ct")
    assert pd.isna(result.iloc[0][REQUIREMENTS_FICHAS_COLUMN])
