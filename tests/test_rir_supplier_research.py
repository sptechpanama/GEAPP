from __future__ import annotations

import pandas as pd

from services.rir_supplier_research import (
    latest_top5_snapshot,
    latest_top10_snapshot,
    top_link_coverage,
    top5_link_coverage,
    top5_general_recommendation,
)


def test_latest_top5_snapshot_selects_latest_date_and_orders_all_five() -> None:
    rows = [
        {"fecha_corte": "2026-09-04", "ranking": 1, "ficha": "old"},
        *[
            {
                "fecha_corte": "2026-09-05",
                "ranking": ranking,
                "ficha": str(100000 + ranking),
                "actualizado_en": f"2026-09-05T08:0{ranking}:00-05:00",
            }
            for ranking in (5, 2, 4, 1, 3)
        ],
    ]

    result = latest_top5_snapshot(pd.DataFrame(rows))

    assert result["ranking"].tolist() == [1, 2, 3, 4, 5]
    assert result["ficha"].tolist() == ["100001", "100002", "100003", "100004", "100005"]


def test_latest_top5_snapshot_keeps_newest_daily_rerun_without_duplicates() -> None:
    frame = pd.DataFrame(
        [
            {
                "fecha_corte": "2026-09-05",
                "ranking": 1,
                "ficha": "108541",
                "actualizado_en": "2026-09-05T08:00:00-05:00",
            },
            {
                "fecha_corte": "2026-09-05",
                "ranking": 1,
                "ficha": "60939",
                "actualizado_en": "2026-09-05T09:00:00-05:00",
            },
        ]
    )

    result = latest_top5_snapshot(frame)

    assert len(result) == 1
    assert result.loc[0, "ficha"] == "60939"


def test_latest_top5_snapshot_keeps_previous_complete_cut_during_partial_write() -> None:
    frame = pd.DataFrame(
        [
            *[
                {
                    "fecha_corte": "2026-09-04",
                    "ranking": ranking,
                    "ficha": f"old-{ranking}",
                    "actualizado_en": "2026-09-04T08:00:00-05:00",
                }
                for ranking in range(1, 6)
            ],
            {
                "fecha_corte": "2026-09-05",
                "ranking": 1,
                "ficha": "partial-new",
                "actualizado_en": "2026-09-05T08:00:00-05:00",
            },
        ]
    )

    result = latest_top5_snapshot(frame)

    assert result["ficha"].tolist() == [f"old-{ranking}" for ranking in range(1, 6)]


def test_latest_top5_snapshot_rejects_invalid_schema_or_rank() -> None:
    assert latest_top5_snapshot(pd.DataFrame({"ficha": ["1"]})).empty
    assert latest_top5_snapshot(
        pd.DataFrame([{"fecha_corte": "2026-09-05", "ranking": 8}])
    ).empty


def test_latest_top10_snapshot_returns_all_ten_in_rank_order() -> None:
    frame = pd.DataFrame(
        [
            {
                "fecha_corte": "2026-09-06",
                "ranking": ranking,
                "ficha": f"ficha-{ranking}",
                "actualizado_en": f"2026-09-06T09:{ranking:02d}:00-05:00",
            }
            for ranking in range(10, 0, -1)
        ]
    )

    result = latest_top10_snapshot(frame)

    assert result["ranking"].tolist() == list(range(1, 11))
    assert result["ficha"].tolist() == [f"ficha-{rank}" for rank in range(1, 11)]


def test_latest_top10_snapshot_shows_newest_cut_even_with_fewer_candidates() -> None:
    frame = pd.DataFrame(
        [
            *[
                {
                    "fecha_corte": "2026-09-05",
                    "ranking": ranking,
                    "ficha": f"complete-{ranking}",
                    "actualizado_en": "2026-09-05T08:00:00-05:00",
                }
                for ranking in range(1, 11)
            ],
            *[
                {
                    "fecha_corte": "2026-09-06",
                    "ranking": ranking,
                    "ficha": f"partial-{ranking}",
                    "actualizado_en": "2026-09-06T08:00:00-05:00",
                }
                for ranking in range(1, 7)
            ],
        ]
    )

    result = latest_top10_snapshot(frame)

    assert result["ficha"].tolist() == [f"partial-{ranking}" for ranking in range(1, 7)]


def test_top5_general_recommendation_uses_first_non_empty_value() -> None:
    frame = pd.DataFrame(
        {"recomendacion_general": [None, "", "Trabajar primero 108541 y 60939."]}
    )
    assert top5_general_recommendation(frame) == "Trabajar primero 108541 y 60939."


def test_top5_link_coverage_counts_only_http_links() -> None:
    frame = pd.DataFrame(
        {
            "enlace_acto": ["https://panamacompra.example/1", ""],
            "enlace_ficha_minsa": [
                "https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha=1&idparam=0",
                "ftp://invalid.example/2",
            ],
            "enlace_producto_recomendado": [
                "http://supplier.example/product/1",
                None,
            ],
        }
    )

    assert top5_link_coverage(frame) == {
        "enlace_acto": 1,
        "enlace_ficha_minsa": 1,
        "enlace_producto_recomendado": 1,
    }
    assert top_link_coverage(frame) == top5_link_coverage(frame)


def test_top5_link_coverage_handles_legacy_snapshot() -> None:
    assert top5_link_coverage(pd.DataFrame({"enlace_acto": ["https://acto"]})) == {
        "enlace_acto": 1,
        "enlace_ficha_minsa": 0,
        "enlace_producto_recomendado": 0,
    }


def test_research_sorting_filters_and_preserves_history():
    from services.rir_supplier_research import prepare_research_table, research_column
    frame = pd.DataFrame([
        {"ficha": "1", "Estado Investigación": "No vigente", "actualizado_en": "2026-09-14T11:00:00-05:00"},
        {"ficha": "2", "Estado Investigación": "Parcial", "actualizado_en": "2026-09-14T10:55:00-05:00"},
        {"ficha": "3", "Estado Investigación": "Sin precio público", "actualizado_en": "2026-09-12T10:31:43-05:00"},
    ])
    original = frame.copy(deep=True)
    assert research_column(frame, "estado_investigacion") == "Estado Investigación"
    assert prepare_research_table(frame)["ficha"].tolist() == ["2", "3"]
    assert prepare_research_table(frame, include_inactive=True)["ficha"].tolist() == ["1", "2", "3"]
    pd.testing.assert_frame_equal(frame, original)


def test_sort_uses_research_date_when_update_is_missing():
    from services.rir_supplier_research import prepare_research_table
    frame = pd.DataFrame([
        {"ficha": "old", "actualizado_en": "", "fecha_investigacion": "2026-09-04"},
        {"ficha": "new", "actualizado_en": "2026-09-14T10:55:00-05:00"},
    ])
    assert prepare_research_table(frame)["ficha"].tolist() == ["new", "old"]


def test_freshness_converts_to_panama_time():
    from services.rir_supplier_research import research_updated_at
    value = research_updated_at(pd.DataFrame({"actualizado_en": ["2026-09-14T15:55:00Z", "bad"]}))
    assert value.strftime("%Y-%m-%d %H:%M") == "2026-09-14 10:55"
    assert research_updated_at(pd.DataFrame()) is None


def test_reader_error_keeps_last_success_and_next_success_recovers():
    from services.rir_supplier_research import read_research_safely, RIR_RESEARCH_SHEETS
    old = {name: pd.DataFrame({"ficha": ["old"]}) for name in RIR_RESEARCH_SHEETS}
    first = read_research_safely(lambda: old)
    def failing_reader():
        raise TimeoutError("Do not show transport credentials")
    failure = read_research_safely(failing_reader, first)
    assert failure.using_previous and failure.error
    assert failure.frames is first.frames
    assert failure.checked_at == first.checked_at
    assert "credentials" not in failure.error
    new = {name: pd.DataFrame({"ficha": ["new"]}) for name in RIR_RESEARCH_SHEETS}
    recovered = read_research_safely(lambda: new, first)
    assert not recovered.error and not recovered.using_previous
    assert recovered.frames is new
    assert read_research_safely(failing_reader).frames == {}


def test_temporary_empty_publication_does_not_erase_previous_read():
    from services.rir_supplier_research import read_research_safely, RIR_RESEARCH_SHEETS
    old = {name: pd.DataFrame({"ficha": ["old"]}) for name in RIR_RESEARCH_SHEETS}
    previous = read_research_safely(lambda: old)
    empty = {name: pd.DataFrame() for name in RIR_RESEARCH_SHEETS}
    result = read_research_safely(lambda: empty, previous)
    assert result.using_previous and result.frames is old


def test_batch_read_validates_headers_and_normalizes_filter_names():
    import pytest
    from services.rir_supplier_research import research_frames_from_values, RIR_RESEARCH_SHEET
    response = {"valueRanges": [
        {"values": [["fecha_corte", "ranking"], ["2026-09-14", "1"]]},
        {"values": [["ficha", "numero_acto", "Estado Investigación", "Medio Recomendado"], ["43358", "act", "Parcial", "Aéreo"]]},
        {"values": [["ficha", "actualizado_en"], ["43358", "2026-09-14"]]},
    ]}
    frames = research_frames_from_values(response)
    assert frames[RIR_RESEARCH_SHEET].iloc[0]["estado_investigacion"] == "Parcial"
    assert frames[RIR_RESEARCH_SHEET].iloc[0]["medio_recomendado"] == "Aéreo"
    with pytest.raises(ValueError):
        research_frames_from_values({"valueRanges": response["valueRanges"][:1]})
    response["valueRanges"][0] = {"values": [["ficha"], ["123"]]}
    with pytest.raises(ValueError):
        research_frames_from_values(response)


def test_fractional_rank_does_not_replace_a_real_position():
    frame = pd.DataFrame([
        {"fecha_corte": "2026-09-14", "ranking": 1.5, "ficha": "invalid"},
        {"fecha_corte": "2026-09-14", "ranking": 1, "ficha": "valid"},
    ])
    assert latest_top10_snapshot(frame)["ficha"].tolist() == ["valid"]
