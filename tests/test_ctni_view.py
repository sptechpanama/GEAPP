import ast
from datetime import date
from pathlib import Path

import pandas as pd

from services.ctni_view import (
    CTNI_VIEWS,
    ctni_date_series,
    display_ctni_records,
    enrich_new_fichas,
    filter_ctni_records,
)


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


def test_spanish_homologation_dates_are_filterable_and_sorted():
    frame = pd.DataFrame(
        [
            {"fecha": "20 agosto 2026", "producto": "Primera"},
            {"fecha": "21 agosto 2026", "producto": "Segunda"},
        ]
    )
    parsed = ctni_date_series(frame)
    assert parsed.dt.strftime("%Y-%m-%d").tolist() == ["2026-08-20", "2026-08-21"]
    result = filter_ctni_records(
        frame,
        start_date=date(2026, 8, 21),
        end_date=date(2026, 8, 21),
    )
    assert result["producto"].tolist() == ["Segunda"]


def test_request_link_opens_official_report_instead_of_json_endpoint():
    frame = pd.DataFrame(
        [
            {
                "id_oficial": "43422",
                "tipo": "Elaboración Dispositivos y Otros",
                "fecha": "2026-08-18",
                "producto": "Solución limpiadora",
                "enlace_oficial": (
                    "https://ctni.minsa.gob.pa/Formularios/FormularioInfo?Id=43422"
                ),
            }
        ]
    )
    displayed = display_ctni_records(frame, "Solicitudes de fichas")
    assert list(displayed.columns)[0] == "Fecha de solicitud"
    assert displayed.loc[0, "Enlace oficial"] == (
        "https://ctni.minsa.gob.pa/Utilities/GenerateFormulario"
        "?IdFormulario=43422&IdTipoFormulario=2"
    )


def test_ficha_link_uses_official_internal_id_and_not_ficha_number():
    frame = pd.DataFrame(
        [
            {
                "id_oficial": "42593",
                "numero_ficha": "110827",
                "fecha": "2026-08-18",
                "producto": "Tubo al vacío",
                "enlace_oficial": "https://ctni.minsa.gob.pa/Home/ConsultarFichas",
            }
        ]
    )
    displayed = display_ctni_records(frame, "Fichas nuevas")
    assert list(displayed.columns)[0] == "Fecha de creación/modificación"
    assert displayed.loc[0, "Enlace oficial"] == (
        "https://ctni.minsa.gob.pa/Utilities/LoadFicha/"
        "?idficha=42593&idparam=0"
    )


def test_new_fichas_show_official_class_from_catalog_metadata():
    frame = pd.DataFrame(
        [
            {"numero_ficha": "043358", "producto": "Kit de circuito"},
            {"numero_ficha": "90000", "producto": "Sin metadata"},
        ]
    )
    enriched = enrich_new_fichas(
        frame,
        {
            "43358": {"clase": "B", "area": "Equipos y mobiliario médico"},
        },
    )
    assert enriched["clase"].tolist() == ["B", "Sin clase asignada"]
    displayed = display_ctni_records(enriched, "Fichas nuevas")
    assert "Clase oficial" in displayed.columns
    assert displayed.loc[0, "Clase oficial"] == "B"


def test_new_fichas_can_exclude_only_confirmed_medication_classification():
    frame = pd.DataFrame(
        [
            {
                "numero_ficha": "1",
                "producto": "Amoxicilina",
                "grupo": "Medicamentos y productos de nutrición",
            },
            {
                "numero_ficha": "2",
                "producto": "Dispositivo médico de laboratorio",
                "grupo": "Materiales e insumos de laboratorio",
            },
        ]
    )
    enriched = enrich_new_fichas(frame)
    assert enriched["es_medicamento"].tolist() == ["Si", "No"]
    filtered = filter_ctni_records(enriched, exclude_medications=True)
    assert filtered["numero_ficha"].tolist() == ["2"]


def test_class_filter_keeps_selected_classes_and_unclassified_records_when_selected():
    frame = enrich_new_fichas(
        pd.DataFrame(
            [
                {"numero_ficha": "101", "producto": "Uno"},
                {"numero_ficha": "102", "producto": "Dos"},
            ]
        ),
        {"101": {"clase": "A"}},
    )
    result = filter_ctni_records(frame, classes=["Sin clase asignada"])
    assert result["numero_ficha"].tolist() == ["102"]


def test_ctni_page_loads_only_the_selected_dataset() -> None:
    page_path = Path(__file__).parents[1] / "pages" / "panama_compra.py"
    tree = ast.parse(page_path.read_text(encoding="utf-8"))
    functions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)
    }

    module = functions["_render_ctni_module"]
    module_calls = [
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call)
    ]
    assert not any(
        isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "st"
        and call.func.attr == "tabs"
        for call in module_calls
    )
    assert sum(
        isinstance(call.func, ast.Name) and call.func.id == "_render_ctni_view"
        for call in module_calls
    ) == 1

    active_view = functions["_render_ctni_view"]
    assert sum(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_df"
        for node in ast.walk(active_view)
    ) == 1
