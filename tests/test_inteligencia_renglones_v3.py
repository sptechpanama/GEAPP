from __future__ import annotations

import pandas as pd

from services.inteligencia_renglones_v3 import (
    display_line_results,
    enrich_line_results_context,
    prepare_line_results,
    summarize_line_results,
)


def _sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "request_id": "req-a",
                "ficha": "43358",
                "acto_id": "A-1",
                "renglon_id": "t1-r1",
                "renglon_numero": "1",
                "acto_url": "https://example.test/a1",
                "precio_referencia_total": "1000",
                "precio_participacion_unitario": "9.5",
                "precio_participacion_total": "950",
                "match_requires_review": "0",
                "proveedor": "Proveedor A",
            },
            {
                "request_id": "req-a",
                "ficha": "43358",
                "acto_id": "A-1",
                "renglon_id": "t1-r1",
                "renglon_numero": "1",
                "acto_url": "https://example.test/a1",
                "precio_referencia_total": "1000",
                "precio_participacion_unitario": "9.8",
                "precio_participacion_total": "980",
                "match_requires_review": "false",
                "proveedor": "Proveedor B",
            },
            {
                "request_id": "req-a",
                "ficha": "43358",
                "acto_id": "A-2",
                "renglon_id": "t1-r3",
                "renglon_numero": "3",
                "acto_url": "https://example.test/a2",
                "precio_referencia_total": "2500",
                "precio_participacion_unitario": "0",
                "precio_participacion_total": "0",
                "match_requires_review": "1",
                "proveedor": "",
            },
            {
                "request_id": "otra",
                "ficha": "99999",
                "acto_id": "X",
                "renglon_id": "x",
                "renglon_numero": "1",
                "precio_referencia_total": "999999",
            },
        ]
    )


def test_filters_exact_request_and_ficha_and_converts_numbers() -> None:
    result = prepare_line_results(_sample(), request_id="req-a", ficha="43358")
    assert len(result) == 3
    assert result["precio_referencia_total"].dtype.kind == "f"
    assert result["match_requires_review"].tolist() == [False, False, True]


def test_summary_does_not_duplicate_reference_per_provider() -> None:
    result = prepare_line_results(_sample(), request_id="req-a", ficha="43358")
    summary = summarize_line_results(result)
    assert summary["actos"] == 2
    assert summary["renglones"] == 2
    assert summary["ofertas"] == 2
    assert summary["referencia_atribuible"] == 3500.0
    assert summary["participacion_atribuible"] == 1930.0
    assert summary["pendientes_revision"] == 1


def test_unknown_request_never_falls_back_to_stale_result_of_same_ficha() -> None:
    result = prepare_line_results(
        _sample(),
        request_id="solicitud-nueva",
        ficha="43358",
    )
    assert result.empty


def test_display_keeps_auditable_columns_and_link() -> None:
    result = prepare_line_results(_sample(), request_id="req-a", ficha="43358")
    display = display_line_results(result)
    assert "Acto" in display
    assert "Referencia del renglón" in display
    assert "Entidad" not in display
    assert "Cómo se vinculó la oferta" not in display or len(display) == 3


def test_enrichment_adds_whole_act_total_and_minsa_without_changing_line_amounts() -> None:
    result = prepare_line_results(_sample(), request_id="req-a", ficha="43358")
    acts = pd.DataFrame(
        [
            {
                "acto_key": "A-1",
                "enlace": "https://example.test/a1",
                "reference_amount_context": 150_000.25,
            },
            {
                "acto_key": "A-2",
                "enlace": "https://example.test/a2",
                "reference_amount_context": 2_750.0,
            },
        ]
    )
    enriched = enrich_line_results_context(
        result,
        acts=acts,
        minsa_url="https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha=43358",
    )

    assert enriched.loc[enriched["acto_id"].eq("A-1"), "precio_total_acto"].tolist() == [
        150_000.25,
        150_000.25,
    ]
    assert enriched.loc[enriched["acto_id"].eq("A-2"), "precio_total_acto"].tolist() == [
        2_750.0
    ]
    assert enriched["precio_referencia_total"].tolist() == [1000.0, 1000.0, 2500.0]
    assert enriched["enlace_ficha_minsa"].str.startswith("https://ctni.minsa").all()

    display = display_line_results(enriched)
    columns = list(display.columns)
    assert columns.index("Precio total del acto") == columns.index("Oferta unitaria") + 1
    assert "Ficha MINSA" in display
