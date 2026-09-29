"""Exercise the real Streamlit controls with fake storage; no external writes."""
from copy import deepcopy
from unittest.mock import patch
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest
from services import anestesia_view as view

SOURCE = {"url": "https://www.panamacompra.gob.pa", "number": "ACTO-PRUEBA", "purchase_unit": "Hospital de prueba",
    "registry_max_months": 12, "registry_rule": "Anexo, página 2", "closing": "2020-01-01", "attachments": [],
    "blocking_errors": [], "items": [{"cantidad": 900}], "explicit_fichas": ["43358"], "entity": "CSS", "info": {}}


class Storage:
    sheet_id = "test-sheet-ui"
    def __init__(self):
        self.tables = {"ANESTESIA_EXPEDIENTES": [{"id": "a" * 32, "number": "ACTO-PRUEBA", "source_id": "source",
            "state": "Datos capturados", "updated_at": "2026-09-28", "detail": "Ejemplo", "url": "https://www.panamacompra.gob.pa"}],
            "ANESTESIA_DOCUMENTOS": [], "ANESTESIA_REVISIONES": []}
    def ensure_tables(self): pass
    def rows(self, name): return deepcopy(self.tables[name])
    def json_file(self, name): return deepcopy(SOURCE)
    def save_job(self, data):
        row = next(r for r in self.tables["ANESTESIA_EXPEDIENTES"] if r["id"] == data["id"])
        row.update(data); return row


def test_can_open_historical_case_library_and_validate_without_an_exception():
    storage = Storage()
    view._records.clear(); view._json.clear()
    with patch.object(view, "AnestesiaStorage", return_value=storage), patch.object(view, "build"):
        app = AppTest.from_string("from services.anestesia_view import render_anestesia_docs\nrender_anestesia_docs(None, 'usuario_prueba')", default_timeout=20)
        app.secrets["app"] = {}
        app.run()
        assert not app.exception
        assert any("Registro Público" in x.value for x in app.info)
        button = next(b for b in app.button if b.label == "Comprobar requisitos y preparar borradores")
        button.click().run()
        assert not app.exception
        assert storage.tables["ANESTESIA_EXPEDIENTES"][0]["state"] == "Bloqueado"
        assert len(app.dataframe) >= 1
        app.radio[0].set_value("Biblioteca y vigencias").run()
        assert not app.exception
        assert any("Guardar nueva versión" in b.label for b in app.button)


def test_google_read_failure_is_visible_instead_of_erasing_library():
    view._records.clear()
    storage = Storage()
    with patch.object(view, "AnestesiaStorage", return_value=storage), patch.object(view, "build"), patch.object(storage, "rows", side_effect=TimeoutError("Prueba de corte de red")):
        app = AppTest.from_string("from services.anestesia_view import render_anestesia_docs\nrender_anestesia_docs(None, 'usuario_prueba')", default_timeout=20)
        app.secrets["app"] = {}
        app.run()
        assert not app.exception and any("corte de red" in e.value for e in app.error)
        assert storage.tables["ANESTESIA_EXPEDIENTES"]
