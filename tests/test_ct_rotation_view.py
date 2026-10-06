from datetime import datetime

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest

from services import ct_rotation as rotation
from services import ct_rotation_view as view


@pytest.fixture
def snapshot(monkeypatch):
    stamp = datetime(2026, 10, 6, 8, tzinfo=rotation.PANAMA)
    monkeypatch.setattr(rotation, "now", lambda: stamp)
    records = []
    for index, publication in enumerate(("2025-01-01", "2025-04-01"), start=1):
        row = dict.fromkeys(rotation.HISTORY_HEADERS, "")
        row.update(ficha="43358", convocatoria=f"2025-1-10-CL-{index:06}", publicacion=publication, kits=600, estado="Adjudicado", hospital="Hospital de prueba", actualizado_en=stamp.isoformat())
        records.append(row)
    snapshot = {"records": records, "inventory": None, "status": {"ultimo_exito": stamp.isoformat()}}
    def read(identifier):
        return snapshot
    read.clear = lambda: None
    monkeypatch.setattr(view, "_snapshot", read)
    import sheets

    client = object()
    credentials = object()
    monkeypatch.setattr(sheets, "get_client", lambda: (client, credentials))
    def save(client, kits, inventory_date, actor, **settings):
        assert client is sheets.get_client()[0]
        snapshot["inventory"] = {"ficha": "43358", "kits": str(kits), "fecha": inventory_date.isoformat()}
        return snapshot["inventory"]
    monkeypatch.setattr(rotation, "save_inventory", save)
    return snapshot


def app():
    return AppTest.from_string("from services.ct_rotation_view import render_rotation_view\nrender_rotation_view('usuario')").run(timeout=20)


def test_snapshot_uses_the_client_from_the_real_credentials_contract(monkeypatch):
    import sheets

    client = object()
    credentials = object()
    expected = {"records": [], "inventory": None, "status": {}}
    monkeypatch.setattr(sheets, "get_client", lambda: (client, credentials))

    def load(authorised_client, *, sheet_id):
        assert authorised_client is client
        assert sheet_id == "test-rotation-client-contract"
        return expected

    monkeypatch.setattr(rotation, "load_snapshot", load)
    view._snapshot.clear()
    try:
        assert view._snapshot("test-rotation-client-contract") == expected
    finally:
        view._snapshot.clear()


def test_only_the_requested_two_tabs_and_inventory_controls_are_rendered(snapshot):
    tested = app()
    assert not tested.exception
    assert [tab.label for tab in tested.tabs] == ["Próximas solicitudes por entidad", "Gestión de inventario"]
    assert len(tested.metric) == 2
    assert len(tested.dataframe) == 1
    assert len(tested.number_input) == 1
    assert len(tested.date_input) == 1
    assert [button.label for button in tested.button] == ["Guardar inventario"]


def test_saving_inventory_keeps_the_form_and_shows_the_calculated_replenishment(snapshot):
    tested = app()
    tested.number_input[0].set_value(6000)
    tested.button[0].click().run()
    assert not tested.exception
    assert tested.success[0].value == "Inventario guardado."
    assert snapshot["inventory"]["kits"] == "6000"
    assert len(tested.metric) == 5
    tested.run()
    assert not tested.exception
    assert tested.number_input[0].value == 6000


def test_refresh_failure_preserves_visible_history(snapshot, monkeypatch):
    tested = app()
    def unavailable(identifier):
        raise RuntimeError("Fuente temporalmente caída")
    monkeypatch.setattr(view, "_snapshot", unavailable)
    tested.run()
    assert not tested.exception
    assert len(tested.warning) == 1
    assert len(tested.dataframe) == 1


def test_failed_save_does_not_report_success_or_invent_inventory(snapshot, monkeypatch):
    tested = app()
    def unavailable(*args, **settings):
        raise RuntimeError("No se pudo guardar")
    monkeypatch.setattr(rotation, "save_inventory", unavailable)
    tested.number_input[0].set_value(6000)
    tested.button[0].click().run()
    assert not tested.exception
    assert len(tested.error) == 1
    assert not tested.success
    assert snapshot["inventory"] is None


def test_empty_history_and_zero_inventory_do_not_cause_division_by_zero(snapshot):
    snapshot["records"] = []
    snapshot["inventory"] = {"kits": "0", "fecha": "2026-10-06"}
    tested = app()
    assert not tested.exception
    assert len(tested.metric) == 2
    assert not tested.dataframe


def test_invalid_saved_inventory_does_not_break_the_hospital_view(snapshot):
    snapshot["inventory"] = {"kits": "desconocido", "fecha": "sin fecha"}
    tested = app()
    assert not tested.exception
    assert len(tested.warning) == 1
    assert len(tested.dataframe) == 1


def test_one_call_has_no_invented_forecast_and_does_not_break_the_table(snapshot):
    snapshot["records"] = snapshot["records"][:1]
    tested = app()
    assert not tested.exception
    assert len(tested.dataframe) == 1
