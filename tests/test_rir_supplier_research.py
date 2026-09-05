from __future__ import annotations

import pandas as pd

from services.rir_supplier_research import (
    latest_top5_snapshot,
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


def test_top5_general_recommendation_uses_first_non_empty_value() -> None:
    frame = pd.DataFrame(
        {"recomendacion_general": [None, "", "Trabajar primero 108541 y 60939."]}
    )
    assert top5_general_recommendation(frame) == "Trabajar primero 108541 y 60939."
