"""Exercise the actual Streamlit renderer, with isolated in-memory evidence."""
import ast
from pathlib import Path

from streamlit.testing.v1 import AppTest


def render_code():
    page = (Path(__file__).parents[1] / "pages/panama_compra.py").read_text(encoding="utf-8")
    tree = ast.parse(page)
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_render_rir_daily_top10")
    return ast.get_source_segment(page, function)


def harness():
    return '''
import re, hashlib
import pandas as pd
import streamlit as st
from services import rir_supplier_research as _rir_supplier_research
from services.rir_supplier_research import top_link_coverage
_clean_text = _rir_supplier_research._text
now = pd.Timestamp.now(tz="America/Panama")
close = (now + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
research = pd.DataFrame([dict(numero_acto="2026-0-12-19-08-CL-041935", ficha="41364", renglon=str(n),
    nombre_ficha="Carro de emergencia", descripcion_renglon="Carro de emergencia", estado_investigacion="Parcial",
    enlace_acto="https://www.panamacompra.gob.pa/acto", contacto_potencial="https://supplier.example/product",
    proveedor_potencial="Fabricante", actualizado_en=now.isoformat(), fecha_cierre=close,
    observaciones="Pendientes: Cotizar.\\nPróxima acción: Solicitar precio y stock.",
    fuentes="https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha=3368&idparam=0") for n in range(1,13)])
live = pd.DataFrame([dict(numero_acto="2026-0-12-19-08-CL-041935", fichas_sin_requisitos="41364", tipo_acto="Solo fichas sin requisitos",
    tipo_adjudicacion="Global", fecha_cierre=close, verificado_en=now.isoformat())])
''' + render_code() + '\n_render_rir_daily_top10(pd.DataFrame(), research, live)\n'


def test_candidate_table_filters_pagination_and_details_render_without_cloud_services(tmp_path):
    page = tmp_path / "view.py"
    page.write_text(harness(), encoding="utf-8")
    app = AppTest.from_file(str(page)).run(timeout=20)
    assert not app.exception
    assert len(app.dataframe[0].value) == 10
    assert "Qué falta confirmar" in app.dataframe[0].value
    app.selectbox(key="rir_candidates_amount").select("Todas las oportunidades").run()
    assert not app.exception and len(app.dataframe[0].value) == 12
    app.selectbox(key="rir_top10_detail_selection").select("2026-0-12-19-08-CL-041935|41364|12").run()
    assert not app.exception
    app.selectbox(key="rir_candidates_amount").select("Top 10").run()
    assert not app.exception and len(app.dataframe[0].value) == 10
    app.selectbox(key="rir_candidates_amount").select("Todas las oportunidades").run()
    app.selectbox(key="rir_candidates_situation").select("Lista para ofertar").run()
    assert not app.exception and len(app.info) == 1
    app.selectbox(key="rir_candidates_situation").select("Todas").run()
    assert not app.exception and len(app.dataframe[0].value) == 12


def test_source_outage_renders_history_without_claiming_live_opportunities(tmp_path):
    code = harness().replace('_render_rir_daily_top10(pd.DataFrame(), research, live)',
                             '_render_rir_daily_top10(pd.DataFrame(), research, None)')
    page = tmp_path / "view.py"
    page.write_text(code, encoding="utf-8")
    app = AppTest.from_file(str(page)).run(timeout=20)
    assert not app.exception
    assert len(app.info) == 1
    assert "fuera de la selección" in app.expander[0].label
