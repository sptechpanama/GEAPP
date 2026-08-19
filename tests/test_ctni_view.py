from datetime import date

import pandas as pd

from services.ctni_view import CTNI_VIEWS, display_ctni_records, filter_ctni_records


def _sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fecha": "2026-08-18",
                "producto": "Monitor materno fetal",
                "numero_formulario": "100",
                "subcomite": "Médico Quirúrgico",
                "estado": "Recibido",
                "condicion": "Nuevo",
                "enlace_oficial": "https://ctni.minsa.gob.pa/a",
            },
            {
                "fecha": "2026-07-01",
                "producto": "Reactivo de laboratorio",
                "numero_formulario": "99",
                "subcomite": "Laboratorio",
                "estado": "Finalizado",
                "condicion": "Línea base",
                "enlace_oficial": "https://ctni.minsa.gob.pa/b",
            },
        ]
    )


def test_three_required_views_are_declared():
    assert list(CTNI_VIEWS) == ["Solicitudes de fichas", "Homologaciones", "Fichas nuevas"]


def test_filters_support_comma_phrases_status_committee_and_dates():
    result = filter_ctni_records(
        _sample(),
        search="materno fetal, inexistente",
        states=["Recibido"],
        subcommittees=["Médico Quirúrgico"],
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 31),
    )
    assert result["numero_formulario"].tolist() == ["100"]


def test_display_uses_simple_spanish_columns_and_keeps_official_link():
    displayed = display_ctni_records(_sample(), "Solicitudes de fichas")
    assert "N.º formulario" in displayed.columns
    assert displayed.loc[0, "Enlace oficial"].startswith("https://ctni.minsa.gob.pa/")
