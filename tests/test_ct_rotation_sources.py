from datetime import date
import json
from pathlib import Path
import sqlite3

import pytest

from services import ct_rotation as rotation
from services import ct_rotation_sources as sources


TODAY = date(2026, 10, 6)
CODE = "2026-1-10-CL-000001"
URL = "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/" + CODE + "/token"


def line(description="KIT DE CIRCUITO ANESTESIA FICHA TECNICA 43358", quantity=600, unit="Unidad"):
    return {"descripcion": description, "cantidad": quantity, "unidad": unit}


def source_record(identifier=1, **changes):
    row = {
        "id": identifier, "enlace": URL, "publicacion": "01-01-2026",
        "fecha_actualizacion": "2026-10-06 09:00:00", "entidad": "CSS",
        "unidad_solic": "Hospital de prueba", "estado": "Adjudicado",
        "items_json": json.dumps([line()]),
    }
    row.update(changes)
    return row


def saved_record(**changes):
    row = dict.fromkeys(rotation.HISTORY_HEADERS, "")
    row.update(
        ficha="43358", convocatoria=CODE, publicacion="2026-01-01", kits="600",
        hospital="Hospital auditado", unidad_compra="Hospital de prueba",
        estado="Cerrada", demanda_id="requisicion-auditada", requisicion="1234",
        actos_relacionados=CODE, enlace=URL, actualizado_en="2026-10-06T08:00:00-05:00",
        verificado_en="2026-10-06T08:00:00-05:00",
    )
    row.update(changes)
    return row


def database(root, rows, *, indexed=None, relations=()):
    directory = root / "data/db"
    directory.mkdir(parents=True)
    with sqlite3.connect(directory / "panamacompra.db") as source:
        source.execute("CREATE TABLE actos_publicos (id INTEGER PRIMARY KEY, enlace TEXT UNIQUE, publicacion TEXT, fecha_actualizacion TEXT, entidad TEXT, unidad_solic TEXT, estado TEXT, items_json TEXT)")
        for row in rows:
            source.execute("INSERT INTO actos_publicos VALUES (?,?,?,?,?,?,?,?)", tuple(row.values()))
        source.execute("CREATE TABLE cl_cotizaciones (numero_cl TEXT, successor_process_number TEXT)")
        source.executemany("INSERT INTO cl_cotizaciones VALUES (?,?)", relations)
    with sqlite3.connect(directory / "inteligencia_proveedores.db") as analytics:
        analytics.execute("CREATE TABLE intel_actos_fichas (source_id TEXT, enlace TEXT, ficha TEXT)")
        analytics.executemany("INSERT INTO intel_actos_fichas VALUES (?,?,?)", indexed if indexed is not None else [(row["id"], row["enlace"], "43358") for row in rows])


def test_reads_only_existing_data_without_network_and_preserves_databases(tmp_path, monkeypatch):
    import requests

    database(tmp_path, [source_record()])
    before = {path.name: path.read_bytes() for path in (tmp_path / "data/db").glob("*.db")}
    def forbidden(*args, **kwargs):
        pytest.fail("La rotación no debe consultar PanamáCompra ni documentos.")
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)
    records, pending = sources.read_existing_history(tmp_path, [], today=TODAY)
    assert not pending
    assert len(records) == 1
    assert records[0]["kits"] == "600.0"
    assert records[0]["verificado_en"] == "2026-10-06T09:00:00-05:00"
    assert before == {path.name: path.read_bytes() for path in (tmp_path / "data/db").glob("*.db")}


def test_only_counts_the_explicit_kit_line_not_other_products(tmp_path):
    rows = [line(), line("Ficha técnica 22287 otra mercancía", 100000)]
    database(tmp_path, [source_record(items_json=json.dumps(rows))])
    records, pending = sources.read_existing_history(tmp_path, [], today=TODAY)
    assert not pending
    assert float(records[0]["kits"]) == 600


@pytest.mark.parametrize("description", ["Código de clasificación 43358", "Kit circuito anestesia", "Ficha técnica 43358 y ficha técnica 22287"])
def test_unverified_names_or_classification_numbers_cannot_create_demands(tmp_path, description):
    database(tmp_path, [source_record(items_json=json.dumps([line(description)]))])
    records, pending = sources.read_existing_history(tmp_path, [], today=TODAY)
    assert records == []
    assert pending == []


def test_known_audited_kit_can_reuse_its_unique_existing_line(tmp_path):
    database(tmp_path, [source_record(items_json=json.dumps([line("KIT CIRCUITO DE PACIENTE PARA ANESTESIA", 900)]))])
    records, pending = sources.read_existing_history(tmp_path, [saved_record()], today=TODAY)
    assert not pending
    assert float(records[0]["kits"]) == 900
    assert records[0]["hospital"] == "Hospital auditado"
    assert records[0]["requisicion"] == "1234"
    assert records[0]["demanda_id"] == "requisicion-auditada"


@pytest.mark.parametrize("quantity,unit", [(None, "Kit"), (0, "Kit"), (-1, "Kit"), (600, "Caja")])
def test_invalid_quantities_do_not_replace_confirmed_history(tmp_path, quantity, unit):
    database(tmp_path, [source_record(items_json=json.dumps([line(quantity=quantity, unit=unit)]))])
    original = saved_record()
    records, pending = sources.read_existing_history(tmp_path, [original], today=TODAY)
    assert records == [original]
    assert not pending


def test_new_explicit_kit_without_quantity_is_left_pending_not_invented(tmp_path):
    database(tmp_path, [source_record(items_json=json.dumps([line(quantity=None)]))])
    records, pending = sources.read_existing_history(tmp_path, [], today=TODAY)
    assert records == []
    assert pending == [CODE]


def test_absent_history_and_inventory_are_never_deleted_by_empty_index(tmp_path):
    database(tmp_path, [], indexed=[])
    original = saved_record()
    records, pending = sources.read_existing_history(tmp_path, [original], today=TODAY)
    assert records == [original]
    assert pending == []


def test_original_and_official_successor_are_not_counted_twice(tmp_path):
    successor = "2026-1-10-SCM-000002"
    derived = source_record(2, enlace=URL.replace(CODE, successor), publicacion="02-02-2026", items_json=json.dumps([line(quantity=700)]))
    database(tmp_path, [source_record(), derived], relations=[(CODE, successor)])
    records, pending = sources.read_existing_history(tmp_path, [], today=TODAY)
    assert not pending
    assert len(records) == 1
    assert records[0]["convocatoria"] == CODE
    assert records[0]["publicacion"] == "2026-01-01"
    assert float(records[0]["kits"]) == 600
    assert successor in records[0]["actos_relacionados"]
    repeated, _ = sources.read_existing_history(tmp_path, records, today=TODAY)
    assert repeated == records


def test_older_source_cannot_overwrite_a_newer_audit(tmp_path):
    database(tmp_path, [source_record(fecha_actualizacion="2026-10-06 07:00:00", items_json=json.dumps([line(quantity=100)]))])
    original = saved_record()
    records, pending = sources.read_existing_history(tmp_path, [original], today=TODAY)
    assert not pending
    assert float(records[0]["kits"]) == 600
    assert records[0]["estado"] == "Cerrada"


def test_missing_database_is_not_created_and_no_history_is_written(tmp_path):
    with pytest.raises(FileNotFoundError):
        sources.read_existing_history(tmp_path, [saved_record()], today=TODAY)
    assert list(tmp_path.rglob("*.db")) == []


def test_future_publications_do_not_create_demands(tmp_path):
    database(tmp_path, [source_record(publicacion="07-10-2026")])
    records, pending = sources.read_existing_history(tmp_path, [], today=TODAY)
    assert not records
    assert not pending


def test_regular_publisher_has_no_official_capture_call():
    source = (Path(__file__).resolve().parents[1] / "scripts/update_ct_rotation.py").read_text("utf-8")
    assert "OfficialCapture" not in source
    assert "read_existing_history" in source
    assert "--dry-run" in source
