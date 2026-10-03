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


def test_full_research_module_consolidates_incidents_and_retains_saved_history(tmp_path):
    source = (Path(__file__).parents[1] / "pages/panama_compra.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    renderer = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_render_rir_supplier_research")
    code = harness().replace('_render_rir_daily_top10(pd.DataFrame(), research, live)', '')
    code += '''
from pathlib import Path
RIR_TOP10_SHEET = _rir_supplier_research.RIR_TOP10_SHEET
ROW_ID_COL = "__row_id"
SHEET_ID = "test-sheet"
_supabase_db_url = lambda: ""
_rir_snapshot_store = lambda url: None
_render_price_method_legend = lambda: None
_normalize_column_key = _rir_supplier_research.research_column_key
_coerce_money_series = lambda values: pd.to_numeric(values, errors="coerce")
frames = {name: pd.DataFrame() for name in _rir_supplier_research.RIR_RESEARCH_SHEETS}
frames[_rir_supplier_research.RIR_RESEARCH_SHEET] = research
st.session_state["__rir_research_last_good"] = _rir_supplier_research.ResearchRead(frames, now.to_pydatetime())
def _read_rir_research_frames():
    raise TimeoutError("private connection details")
def _read_rir_current_acts():
    raise TimeoutError("source unavailable")
_read_rir_research_frames.clear = lambda: None
_read_rir_current_acts.clear = lambda: None
''' + ast.get_source_segment(source, renderer) + '\n_render_rir_supplier_research()\n'
    page = tmp_path / "research.py"
    page.write_text(code, encoding="utf-8")
    app = AppTest.from_file(str(page)).run(timeout=20)
    assert not app.exception
    assert len(app.warning) == 1
    assert "private" not in app.warning[0].value
    assert any("parcial o por verificar" in item.label for item in app.expander)
    assert any("Historial" in item.label for item in app.expander)


def test_full_research_module_lists_new_work_and_offers_export(tmp_path):
    source = (Path(__file__).parents[1] / "pages/panama_compra.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    renderer = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_render_rir_supplier_research")
    code = harness().replace('_render_rir_daily_top10(pd.DataFrame(), research, live)', '')
    code += '''
from pathlib import Path
RIR_TOP10_SHEET = _rir_supplier_research.RIR_TOP10_SHEET
ROW_ID_COL = "__row_id"
SHEET_ID = "test-sheet"
_supabase_db_url = lambda: ""
_rir_snapshot_store = lambda url: None
_render_price_method_legend = lambda: None
_normalize_column_key = _rir_supplier_research.research_column_key
_coerce_money_series = lambda values: pd.to_numeric(values, errors="coerce")
frames = {name: pd.DataFrame() for name in _rir_supplier_research.RIR_RESEARCH_SHEETS}
frames[_rir_supplier_research.RIR_RESEARCH_SHEET] = research
def _read_rir_research_frames():
    return frames
def _read_rir_current_acts():
    return pd.concat([live, live.assign(numero_acto="2026-0-12-19-08-CL-099999", fichas_sin_requisitos="60939")])
_read_rir_research_frames.clear = lambda: None
_read_rir_current_acts.clear = lambda: None
''' + ast.get_source_segment(source, renderer) + '\n_render_rir_supplier_research()\n'
    page = tmp_path / "research_new.py"
    page.write_text(code, encoding="utf-8")
    app = AppTest.from_file(str(page)).run(timeout=20)
    assert not app.exception and not app.warning
    assert any("pendientes de investigar: 1" in item.label for item in app.expander)
    assert any("Descargar pendientes" in item.label for item in app.get("download_button"))
