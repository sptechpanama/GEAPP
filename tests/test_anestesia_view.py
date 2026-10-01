"""Exercise the real Streamlit controls with fake storage; no external writes."""
from copy import deepcopy
from unittest.mock import patch, Mock
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest
from services import anestesia_view as view

SOURCE = {"url": "https://www.panamacompra.gob.pa", "number": "ACTO-PRUEBA", "purchase_unit": "Hospital de prueba",
    "registry_max_months": 12, "registry_rule": "Anexo, página 2", "closing": "2020-01-01", "attachments": [],
    "blocking_errors": [], "items": [{"cantidad": 900}], "explicit_fichas": ["43358"], "entity": "CSS", "info": {}}


class Storage:
    sheet_id = "test-sheet-ui"
    parent_id = "test-drive-ui"
    def __init__(self):
        self.tables = {"ANESTESIA_EXPEDIENTES": [{"id": "a" * 32, "number": "ACTO-PRUEBA", "source_id": "source",
            "state": "Datos capturados", "updated_at": "2026-09-28", "detail": "Ejemplo", "url": "https://www.panamacompra.gob.pa"}],
            "ANESTESIA_DOCUMENTOS": [], "ANESTESIA_REVISIONES": []}
    def ensure_tables(self): pass
    def rows(self, name): return deepcopy(self.tables[name])
    def json_file(self, name): return deepcopy(SOURCE)
    def job(self, ident):
        return next((r for r in self.rows("ANESTESIA_EXPEDIENTES") if r["id"] == ident), None)
    def queue_request(self, ident): return {"id": ident, "status": "pending"}
    def save_job(self, data):
        row = next((r for r in self.tables["ANESTESIA_EXPEDIENTES"] if r["id"] == data["id"]), None)
        if row is None:
            row = {}
            self.tables["ANESTESIA_EXPEDIENTES"].append(row)
        row.update(data); return row


@pytest.fixture(autouse=True)
def clear_live_cache():
    view._live_job.clear()
    yield
    view._live_job.clear()


def test_can_open_historical_case_library_and_validate_without_an_exception():
    storage = Storage()
    view._records.clear(); view._json.clear()
    with patch.object(view, "AnestesiaStorage", return_value=storage), patch.object(view, "build"):
        app = AppTest.from_string("from services.anestesia_view import render_anestesia_docs\nrender_anestesia_docs(None, 'usuario_prueba')", default_timeout=20)
        app.secrets["app"] = {}
        app.run()
        assert not app.exception
        assert any('Documentos actuales y vigencias' in x.value for x in app.markdown)
        health = app.dataframe[0].value
        assert len(health) == 12 and 'Falta' in health['Estado'].tolist()
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


def test_resolved_workbook_survives_reruns_and_configuration_changes():
    view._records.clear(); view._json.clear()
    connections = []
    class ResolvingStorage(Storage):
        def __init__(self, *args, sheet_id, parent_id):
            super().__init__()
            self.sheet_id, self.parent_id = sheet_id, parent_id
        def ensure_tables(self):
            connections.append(self.sheet_id)
            if self.sheet_id == "office": self.sheet_id = "native"
        def rows(self, name):
            assert self.sheet_id in {"native", "other-native"}
            return super().rows(name)
    with patch.object(view, "AnestesiaStorage", ResolvingStorage), patch.object(view, "build"):
        app = AppTest.from_string("from services.anestesia_view import render_anestesia_docs\nrender_anestesia_docs(None, 'usuario_prueba')", default_timeout=20)
        app.secrets["app"] = {"PC_MANUAL_SHEET_ID": "office"}
        app.session_state["anes_tables_ready"] = True  # session predating the fix
        app.run()
        assert not app.exception and not app.error
        app.radio[0].set_value("Biblioteca y vigencias").run()
        assert not app.exception and not app.error
        assert connections == ["office"]
        app.secrets["app"] = {"PC_MANUAL_SHEET_ID": "other-native"}
        app.run()
        assert not app.exception and not app.error
        assert connections == ["office", "other-native"]


def test_participation_evidence_is_visible_without_changing_offer_selection():
    view._records.clear(); view._json.clear()
    storage = Storage()
    job = storage.tables["ANESTESIA_EXPEDIENTES"][0]
    job["participation"] = {"model": "LB4330K", "catalog": "K",
        "quotation_url": "https://drive.google.com/file/d/quote/view", "folder_url": "https://drive.google.com/drive/folders/history",
        "observations": ["Primera cotización incorrecta: conservar solo como antecedente."]}
    with patch.object(view, "AnestesiaStorage", return_value=storage), patch.object(view, "build"):
        app = AppTest.from_string("from services.anestesia_view import render_anestesia_docs\nrender_anestesia_docs(None, 'usuario_prueba')", default_timeout=20)
        app.secrets["app"] = {}
        app.run()
        assert not app.exception and not app.error
        assert any("LB4330K" in m.value for m in app.markdown)
        assert "config" not in job  # archive does not authorize or create a new bid


APP = "from services.anestesia_view import render_anestesia_docs\nrender_anestesia_docs(None, 'usuario_prueba')"


def test_registry_is_automatic_and_removed_boxes_do_not_erase_saved_requirements():
    storage = Storage()
    job = storage.tables['ANESTESIA_EXPEDIENTES'][0]
    job['config'] = {'registry_max_months': 24, 'extra_requirements': ['Requisito ya registrado']}
    view._records.clear(); view._json.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        labels = [w.label for collection in (app.text_input, app.text_area, app.number_input) for w in collection]
        assert not any('Otros documentos' in label or 'Archivo/página' in label or 'Antigüedad máxima' in label for label in labels)
        assert any('máximo 12 meses' in x.value for x in app.info)
        assert any('Requisito ya registrado' in x.value for x in app.caption)
        next(b for b in app.button if b.label == 'Comprobar requisitos y preparar borradores').click().run()
        assert not app.exception
        saved = storage.tables['ANESTESIA_EXPEDIENTES'][0]['config']
        assert saved['registry_max_months'] == 12 and saved['registry_rule_evidence']
        assert saved['extra_requirements'] == ['Requisito ya registrado']


@pytest.mark.parametrize('publication,show_current', [
    ({'state': 'ready', 'request': 'a'*32, 'manifest': 'approved', 'count': '12'}, True),
    ({'state': 'ready', 'request': 'another-act', 'manifest': 'other', 'count': '12'}, False),
    ({'state': 'updating'}, False),
])
def test_final_link_never_presents_another_request_or_partial_folder_as_this_delivery(publication, show_current):
    storage = Storage()
    job = storage.tables['ANESTESIA_EXPEDIENTES'][0]
    job.update(state='Listo para entregar', final_url='https://drive.example/current',
               delivery_folder_id='current', published_manifest='approved', manifest_hash='approved', archive_url='https://drive.example/archive',
               zip_url='https://drive.example/zip')
    storage.delivery_status = lambda ident: publication
    view._records.clear(); view._json.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        assert not app.exception
        labels = [w.label for w in app.get('link_button')]
        assert ('Abrir 12 PDF para entregar' in labels) is show_current
        assert 'PDF revisados de este expediente (historial)' in labels
        assert 'Descargar ZIP de los PDF de este expediente' in labels


def test_old_delivery_is_not_presented_as_new_draft_ready_to_submit():
    storage = Storage()
    job = storage.tables['ANESTESIA_EXPEDIENTES'][0]
    job.update(state='Pendiente de revisión', final_url='https://drive.example/current',
               delivery_folder_id='current', published_manifest='old', manifest_hash='new')
    storage.delivery_status = lambda ident: {'state': 'ready', 'request': job['id'], 'manifest': 'old', 'count': '12'}
    view._records.clear(); view._json.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        assert not app.exception
        assert not any('PDF para entregar' in w.label for w in app.get('link_button'))
        assert any('conserva una versión anterior' in w.value for w in app.caption)
ACT_URL = "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-051598/0nM6ICc0JCL2AjN1UDMxojIpJye"


@pytest.mark.parametrize("catalog", ["K", "C"])
@pytest.mark.parametrize("tax_mode", ["exento", "adicional", "incluido"])
def test_new_offer_data_saved_before_capture_and_reused_after_completion(catalog, tax_mode):
    storage = Storage()
    view._records.clear(); view._json.clear()
    def enqueue(payload, **kwargs):
        # The worker can start immediately: inputs must already be persisted.
        pending = storage.job(payload['request_id'])
        assert pending['config'] == {'catalog': catalog, 'price': '19.875', 'tax_mode': tax_mode, 'tax_rate': 7.0}
        assert payload['action'] == 'capture'
        return 'queue-new'
    with patch.object(view, "AnestesiaStorage", return_value=storage), patch.object(view, "build"), patch.object(storage, "enqueue", side_effect=enqueue, create=True) as queued:
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        next(w for w in app.text_input if w.label == 'Enlace del acto en PanamáCompra').set_value(ACT_URL)
        app.selectbox(key='anes_new_catalog').set_value(catalog)
        app.number_input(key='anes_new_price').set_value(19.875)
        app.selectbox(key='anes_new_tax_mode').set_value(tax_mode)
        next(b for b in app.button if b.label == 'Consultar acto y anexos').click().run()
        assert not app.exception and not app.error
        assert queued.call_count == 1
        job = storage.tables['ANESTESIA_EXPEDIENTES'][-1]
        assert job['state'] == 'En cola' and job['queue_id'] == 'queue-new'
        assert any('cada 5 segundos' in i.value for i in app.info)
        assert not any(b.label == 'Comprobar requisitos y preparar borradores' for b in app.button)
        # Old 20-second table cache is intentionally retained. Live polling wins.
        storage.save_job({'id': job['id'], 'state': 'Datos capturados', 'source_id': 'source', 'updated_at': '2026-09-30T14:00:00'})
        view._live_job.clear()
        app.run()
        assert not app.exception and not app.error
        assert any('Acto y anexos listos' in s.value for s in app.success)
        assert next(w for w in app.selectbox if w.label == 'Mascarilla / catálogo').value == catalog
        assert next(w for w in app.number_input if w.label == 'Precio UNITARIO de participación (USD)').value == 19.875
        assert next(w for w in app.selectbox if w.label == 'ITBMS').value == tax_mode
        assert queued.call_count == 1  # polling never submits another request


@pytest.mark.parametrize('catalog,price', [(None, 20), ('K', 0)])
def test_incomplete_offer_is_not_queued(catalog, price):
    storage = Storage()
    view._records.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'), patch.object(storage, 'enqueue', create=True) as queued:
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        next(w for w in app.text_input if w.label == 'Enlace del acto en PanamáCompra').set_value(ACT_URL)
        app.selectbox(key='anes_new_catalog').set_value(catalog)
        app.number_input(key='anes_new_price').set_value(price)
        next(b for b in app.button if b.label == 'Consultar acto y anexos').click().run()
        assert not app.exception and any('precio unitario mayor que cero' in e.value for e in app.error)
        queued.assert_not_called()
        assert len(storage.tables['ANESTESIA_EXPEDIENTES']) == 1


def test_duplicate_active_capture_selects_existing_without_replacing_offer():
    storage = Storage()
    job = storage.tables['ANESTESIA_EXPEDIENTES'][0]
    job.update(number='2026-1-10-01-08-CL-051598', state='Procesando', queue_id='q', config={'catalog': 'C', 'price': '20'})
    view._records.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'), patch.object(storage, 'enqueue', create=True) as queued:
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        next(w for w in app.text_input if w.label == 'Enlace del acto en PanamáCompra').set_value(ACT_URL)
        next(b for b in app.button if b.label == 'Consultar acto y anexos').click().run()
        assert not app.exception and not app.error
        queued.assert_not_called()
        assert len(storage.tables['ANESTESIA_EXPEDIENTES']) == 1
        assert storage.job(job['id'])['config'] == {'catalog': 'C', 'price': '20'}


@pytest.mark.parametrize('state', ['Procesando', 'Datos capturados', 'Bloqueado', 'Pendiente de revisión', 'Listo para entregar', 'Error'])
def test_background_transition_refreshes_once_and_never_enqueues(state):
    previous = {'id': 'job', 'state': 'En cola'}
    with patch.object(view, '_live_job', return_value={**previous, 'state': state}), patch.object(view, '_refresh') as refresh, patch.object(view, '_enqueue') as enqueue:
        app = AppTest.from_string("from services.anestesia_view import _watch_job\nfrom types import SimpleNamespace\n_watch_job(SimpleNamespace(sheet_id='test'), {'id': 'job', 'state': 'En cola'})")
        app.run()
        assert not app.exception
        refresh.assert_called_once()
        enqueue.assert_not_called()


def test_poll_same_state_does_not_rerun_forms_and_network_failure_retries():
    job = {'id': 'job', 'state': 'Procesando'}
    with patch.object(view, '_live_job', return_value=job) as poll, patch.object(view, '_refresh') as refresh:
        app = AppTest.from_string("from services.anestesia_view import _watch_job\nfrom types import SimpleNamespace\n_watch_job(SimpleNamespace(sheet_id='test'), {'id': 'job', 'state': 'Procesando'})")
        app.run()
        assert not app.exception
        refresh.assert_not_called()
        poll.side_effect = TimeoutError('Temporary outage')
        app.run()
        assert not app.exception and any('reintentará automáticamente' in w.value for w in app.warning)
        refresh.assert_not_called()


def test_terminal_queue_error_is_visible_without_rerun_loop_or_altering_stored_data():
    storage = Storage()
    job = storage.tables['ANESTESIA_EXPEDIENTES'][0]
    job.update(state='En cola', queue_id='broken')
    before = deepcopy(job)
    view._records.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'), patch.object(storage, 'queue_request', return_value={'status': 'error', 'result_error': 'Worker no pudo iniciar'}):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        assert not app.exception
        assert any('Worker no pudo iniciar' in w.value for w in app.warning)
        assert any(b.label == 'Volver a capturar acto y anexos' for b in app.button)
        assert job == before  # reading a failed queue never mutates history


def test_worker_completing_between_reads_is_not_reported_as_failed():
    storage = Mock()
    waiting = {'id': 'job', 'state': 'Procesando', 'queue_id': 'q'}
    complete = {**waiting, 'state': 'Datos capturados', 'source_id': 'source'}
    storage.job.side_effect = [waiting, complete]
    storage.queue_request.return_value = {'status': 'done'}
    assert view._live_job('sheet-race', 'job', storage) == complete
    storage.save_job.assert_not_called()


def test_simplified_form_uses_source_destination_and_no_longer_asks_removed_fields():
    storage = Storage()
    source = deepcopy(SOURCE)
    source['info']['lugar de entrega'] = 'PANAMA J.J.VALLARINO.Z - ALMACEN GENERAL'
    view._records.clear(); view._json.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'), patch.object(storage, 'json_file', return_value=source):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        assert not app.exception and not app.error
        labels = [w.label for w in app.text_input] + [w.label for w in app.text_area] + [w.label for w in app.checkbox]
        assert not any('representante de la entidad' in label.lower() for label in labels)
        assert not any('Registro Sanitario' in label or 'apoderado' in label or 'Autorizo usar' in label for label in labels)
        assert not any('Lugar' in label or 'Garantía' in label for label in labels)
        assert any('J.J.VALLARINO' in m.value for m in app.markdown)
        assert any('24 meses de garantía' in m.value for m in app.markdown)
        next(b for b in app.button if b.label == 'Comprobar requisitos y preparar borradores').click().run()
        assert not app.exception
        config = storage.tables['ANESTESIA_EXPEDIENTES'][0]['config']
        assert config['delivery_place'] == source['info']['lugar de entrega']
        assert config['signature_authorized'] and not config['require_rs'] and not config['require_power']
        assert not any('entity_' in k for k in config)


def test_delivery_checkbox_persists_manual_schedule_and_resets_after_source_change():
    storage = Storage()
    source = deepcopy(SOURCE)
    source['info'].update({'termino de entrega': '30 Días hábiles', 'lugar de entrega': 'Almacén general'})
    source['fingerprint'] = 'first-capture'
    view._records.clear(); view._json.clear()
    def checkbox(app): return next(w for w in app.checkbox if w.label.startswith('Revisé los adjuntos'))
    def manual(app): return next(w for w in app.text_area if w.label == 'Calendario de entregas completo (manual)')
    def submit(app): next(b for b in app.button if b.label == 'Comprobar requisitos y preparar borradores').click().run()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'), patch.object(storage, 'json_file', return_value=source):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        assert not app.exception and not checkbox(app).value
        assert not any(w.label in {'Respaldo del tratamiento tributario', 'Marca', 'Modelo / referencia exacta'} for w in app.text_input)
        manual(app).set_value('50 unidades a 15 días; 100 unidades a 30 días hábiles.')
        checkbox(app).check()
        next(w for w in app.selectbox if w.label == 'Mascarilla / catálogo').set_value('C')
        submit(app)
        assert not app.exception
        cfg = storage.tables['ANESTESIA_EXPEDIENTES'][0]['config']
        assert cfg['delivery'] == '30 Días hábiles' and cfg['delivery_use_portal'] is True
        assert cfg['catalog_brand'] == 'MFLAB' and cfg['catalog_model'] == 'LB4330C'
        assert checkbox(app).value and '50 unidades' in manual(app).value
        checkbox(app).uncheck()
        submit(app)
        assert not app.exception
        cfg = storage.tables['ANESTESIA_EXPEDIENTES'][0]['config']
        assert cfg['delivery'] == manual(app).value and not cfg['delivery_use_portal']
        checkbox(app).check()
        submit(app)
        source['fingerprint'] = 'new-annex'
        view._json.clear()
        app.run()
        assert not app.exception and not checkbox(app).value
        assert '50 unidades' in manual(app).value


def test_delivery_checkbox_disabled_when_no_portal_period_and_manual_remains_available():
    storage = Storage()
    view._records.clear(); view._json.clear()
    with patch.object(view, 'AnestesiaStorage', return_value=storage), patch.object(view, 'build'):
        app = AppTest.from_string(APP, default_timeout=20)
        app.secrets['app'] = {}
        app.run()
        assert not app.exception
        checkbox = next(w for w in app.checkbox if w.label.startswith('Revisé los adjuntos'))
        assert checkbox.disabled and not checkbox.value
        assert any(w.label == 'Calendario de entregas completo (manual)' for w in app.text_area)
