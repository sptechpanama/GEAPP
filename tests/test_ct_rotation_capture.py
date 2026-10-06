from datetime import date, datetime
from types import SimpleNamespace

from services import ct_rotation as rotation
from services.ct_rotation_capture import OfficialCapture


def collector(details):
    capture = OfficialCapture.__new__(OfficialCapture)
    capture.errors = []
    capture.api = SimpleNamespace(
        date_to_api_start=lambda value: datetime.combine(value, datetime.min.time(), rotation.PANAMA),
        parse_source_date=lambda value: date.fromisoformat(value[:10]) if value else None,
        process_link=lambda record: ("solicitud-de-cotizacion/" if record.get("prefijo") == "CL" else "pliego-de-cargos/") + record["numProceso"],
    )
    capture._listing = lambda kind, term, start, end: [detail["record"] for detail in details] if term == "43358" else []
    capture._detail = lambda record: next(detail for detail in details if detail["record"]["idProcesosContratacionFlujos"] == record["idProcesosContratacionFlujos"])
    capture._documents = lambda detail: (False, "")
    return capture


def detail(flow=1, kind=2, code="2026-1-10-CL-000001", day="2026-01-01", quantity=600, description="KIT DE CIRCUITO ANESTESIA FICHA TECNICA 43358", **changes):
    record = {"idProcesosContratacionFlujos": flow, "idTipoProceso": kind, "numProceso": code, "fechaPublicacion": day, "nombreRealizado": "Cerrada"}
    labels = {"fecha de publicacion": day, "unidad de compra": "Hospital de prueba", "entidad": "CSS", "titulo": "Kit de circuito para anestesia"}
    row = {"record": record, "labels": labels, "items": [{"descripcion": description, "cantidad": quantity, "unidad": "Kit"}], "relations": [], "files": []}
    row.update(changes)
    return row


def test_live_capture_deduplicates_generated_act_and_uses_original_line_quantity():
    original = detail()
    original["items"].append({"descripcion": "Otro producto ficha técnica 29209", "cantidad": 100000, "unidad": "Unidad"})
    derived = detail(2, 9, "2026-1-10-SCM-000002", "2026-02-01", 700)
    derived["relations"] = [{"generacionProcesos": [original["record"]]}]
    capture = collector([original, derived])
    records = capture.capture([])
    assert len(records) == 1
    assert records[0]["publicacion"] == "2026-01-01"
    assert float(records[0]["kits"]) == 600
    assert records[0]["enlace"].startswith("solicitud-de-cotizacion/")
    assert len(capture.capture(records)) == 1


def test_missing_capture_detail_retains_previous_verified_history():
    capture = collector([detail()])
    previous = capture.capture([])
    def unavailable(record):
        raise RuntimeError("Detalle no disponible")
    capture._detail = unavailable
    assert capture.capture(previous) == previous
    assert capture.errors


def test_generic_kit_without_explicit_ficha_proof_does_not_enter_history():
    capture = collector([detail(description="KIT DE CIRCUITO PARA ANESTESIA")])
    assert capture.capture([]) == []
    capture._documents = lambda detail: (True, "2026-113")
    confirmed = capture.capture([])
    assert len(confirmed) == 1
    assert confirmed[0]["requisicion"] == "2026-113"


def test_invalid_quantity_is_reported_without_losing_other_confirmed_calls():
    capture = collector([detail(quantity=None), detail(3, code="2026-1-10-CL-000003", quantity=900)])
    records = capture.capture([])
    assert len(records) == 1
    assert float(records[0]["kits"]) == 900
    assert capture.errors


def test_documented_short_relaunch_merges_demand_but_not_already_awarded_requests():
    first = detail(day="2026-01-01", description="KIT DE CIRCUITO PARA ANESTESIA")
    second = detail(2, code="2026-1-10-CL-000002", day="2026-01-20", description="KIT DE CIRCUITO PARA ANESTESIA")
    capture = collector([second, first])
    capture._documents = lambda detail: (True, "2026-113")
    records = capture.capture([])
    assert len(records) == 2
    assert len(rotation.distinct_needs(records, today=date(2026, 10, 6))) == 1
    first["record"]["nombreRealizado"] = "Adjudicado"
    records = capture.capture([])
    assert len(rotation.distinct_needs(records, today=date(2026, 10, 6))) == 2
