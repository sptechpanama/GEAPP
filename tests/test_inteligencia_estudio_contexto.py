import pandas as pd

from services.inteligencia_estudio_contexto import enrich_study_details


def test_adds_whole_act_total_and_minsa_without_changing_unit_price() -> None:
    details = pd.DataFrame(
        [
            {
                "acto_id": "A-1",
                "acto_url": "https://example.test/a1",
                "precio_unitario_participacion": 41.25,
            }
        ]
    )
    acts = pd.DataFrame(
        [
            {
                "acto_key": "A-1",
                "enlace": "https://example.test/a1",
                "reference_amount_context": 125_000.50,
            }
        ]
    )

    result = enrich_study_details(
        details,
        acts=acts,
        minsa_url="https://ctni.minsa.gob.pa/ficha/43358",
    )

    assert result.loc[0, "precio_unitario_participacion"] == 41.25
    assert result.loc[0, "precio_total_acto"] == 125_000.50
    assert result.loc[0, "enlace_ficha_minsa"].endswith("/43358")


def test_keeps_values_already_published_by_worker() -> None:
    details = pd.DataFrame(
        [
            {
                "acto_id": "A-1",
                "precio_total_acto": 900.0,
                "enlace_ficha_minsa": "https://minsa.test/original",
            }
        ]
    )
    acts = pd.DataFrame(
        [{"acto_key": "A-1", "reference_amount_context": 5_000.0}]
    )

    result = enrich_study_details(
        details,
        acts=acts,
        minsa_url="https://minsa.test/fallback",
    )

    assert result.loc[0, "precio_total_acto"] == 900.0
    assert result.loc[0, "enlace_ficha_minsa"] == "https://minsa.test/original"
