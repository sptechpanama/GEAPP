from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from gspread.exceptions import WorksheetNotFound

from services import ct_rotation as rotation
from services.ct_rotation_capture import document_ficha_codes, selected_kit_lines


def request(code="2025-1-10-CL-000001", day="2025-01-01", kits=600, **changes):
    row = dict.fromkeys(rotation.HISTORY_HEADERS, "")
    row.update(ficha="43358", convocatoria=code, publicacion=day, kits=kits, estado="Adjudicado", hospital="Hospital de prueba", unidad_compra="Hospital de prueba", demanda_id=code, actos_relacionados=code, actualizado_en="2026-10-06T03:00:00-05:00")
    row.update(changes)
    return row


def test_original_and_derived_are_counted_once_with_original_quantity_and_date():
    original = request(kits=600)
    derived = request("2025-1-10-SCM-000002", "2025-02-01", 700, actos_relacionados="2025-1-10-CL-000001,2025-1-10-SCM-000002", actualizado_en="2026-10-06T04:00:00-05:00")
    merged = rotation.merge_history([original], [derived])
    assert len(merged) == 1
    assert merged[0]["convocatoria"] == original["convocatoria"]
    assert merged[0]["publicacion"] == "2025-01-01"
    assert float(merged[0]["kits"]) == 600
    assert len(rotation.merge_history(merged, [derived])) == 1


def test_request_update_changes_quantity_without_duplicating_or_dropping_other_calls():
    original = request()
    other = request("2025-1-10-CL-000003", "2025-04-01")
    updated = {**original, "kits": 900, "actualizado_en": "2026-10-06T04:00:00-05:00"}
    merged = rotation.merge_history([original, other], [updated])
    assert len(merged) == 2
    assert float(merged[0]["kits"]) == 900
    assert len(rotation.merge_history(merged, [])) == 2


def test_new_relaunch_evidence_updates_the_same_original_call():
    original = request()
    corrected = {**original, "demanda_id": "req-113", "requisicion": "113"}
    merged = rotation.merge_history([original], [corrected])
    assert len(merged) == 1
    assert merged[0]["demanda_id"] == "req-113"


def test_documented_relaunch_and_cancellation_do_not_inflate_demand():
    rows = [
        request(estado="Cancelado", demanda_id="req-113"),
        request("2025-1-10-CL-000002", "2025-01-10", demanda_id="req-113"),
        request("2025-1-10-CL-000003", "2025-01-20", estado="Cancelado"),
    ]
    summary = rotation.rotation_summary(rows, today=date(2026, 1, 1))
    assert summary["acts"] == 1
    assert summary["kits"] == 600
    assert summary["acts_per_month"] == pytest.approx(1 / 12)
    assert summary["kits_per_month"] == 50


def test_complete_months_and_partial_month_are_included_without_rounding_up():
    assert rotation.elapsed_months(date(2025, 1, 1), date(2026, 10, 6)) == pytest.approx(21 + 5 / 31)
    assert rotation.elapsed_months(date(2025, 1, 1), date(2025, 1, 1)) > 0
    assert rotation.elapsed_months(date(2025, 1, 1), date(2024, 12, 31)) == 0


def test_frequency_uses_entire_history_and_proximity_changes_with_time():
    rows = [request(day="2024-12-01"), request("2025-1-10-CL-000002", "2025-02-01")]
    first = rotation.rotation_summary(rows, today=date(2025, 3, 1))
    later = rotation.rotation_summary(rows, today=date(2025, 4, 10))
    assert first["acts"] == 1
    assert first["forecast"].iloc[0]["Frecuencia promedio (meses)"] == pytest.approx(62 / rotation.DAYS_PER_MONTH)
    assert first["forecast"].iloc[0]["Próxima solicitud estimada"] == date(2025, 4, 4)
    assert later["forecast"].iloc[0]["Días hasta la fecha estimada"] == -6
    updated = rotation.rotation_summary([*rows, request("2025-1-10-CL-000003", "2025-04-15")], today=date(2025, 4, 15))
    assert updated["forecast"].iloc[0]["Frecuencia promedio (meses)"] != first["forecast"].iloc[0]["Frecuencia promedio (meses)"]


def test_single_request_has_no_invented_frequency_or_forecast():
    summary = rotation.rotation_summary([request()], today=date(2025, 2, 1))
    row = summary["forecast"].iloc[0]
    assert row["Frecuencia promedio (meses)"] is None
    assert row["Próxima solicitud estimada"] is None


def test_future_requests_do_not_enter_the_totals():
    summary = rotation.rotation_summary([request(day="2027-01-01")], today=date(2026, 10, 6))
    assert summary["acts"] == 0
    assert summary["kits"] == 0
    assert summary["forecast"].empty


def test_inventory_uses_sixty_percent_two_month_lead_and_six_thousand_shipments():
    plan = rotation.inventory_plan(6000, date(2026, 10, 6), 3000, today=date(2026, 10, 6))
    assert plan["monthly_consumption"] == 1800
    assert plan["coverage_months"] == pytest.approx(6000 / 1800)
    assert plan["shipment_coverage_months"] == pytest.approx(6000 / 1800)
    assert plan["shipments_now"] == 0
    assert plan["order_date"] > date(2026, 10, 6)
    empty = rotation.inventory_plan(0, date(2026, 10, 6), 3000, today=date(2026, 10, 6))
    assert empty["shipments_now"] == 1
    assert empty["order_date"] < date(2026, 10, 6)


def test_inventory_projection_is_estimated_and_does_not_go_negative():
    first = rotation.inventory_plan(6000, date(2026, 1, 1), 3000, today=date(2026, 2, 1))
    assert first["estimated_stock"] == pytest.approx(6000 - 1800 * 31 / rotation.DAYS_PER_MONTH)
    expired = rotation.inventory_plan(6000, date(2026, 1, 1), 3000, today=date(2026, 10, 6))
    assert expired["estimated_stock"] == 0


def test_inventory_without_demand_does_not_invent_a_reorder_date():
    plan = rotation.inventory_plan(6000, date(2026, 10, 6), 0, today=date(2026, 10, 6))
    assert plan["coverage_months"] is None
    assert plan["order_date"] is None
    assert plan["shipments_now"] == 0


@pytest.mark.parametrize("stock", [-1, math.nan, math.inf, 1.5])
def test_invalid_inventory_is_rejected(stock):
    with pytest.raises(ValueError):
        rotation.inventory_plan(stock, date(2026, 10, 6), 3000, today=date(2026, 10, 6))


def test_future_inventory_date_is_rejected_and_calendar_month_end_is_valid():
    with pytest.raises(ValueError):
        rotation.inventory_plan(6000, date(2026, 10, 7), 3000, today=date(2026, 10, 6))
    assert rotation.shift_months(date(2026, 12, 31), 2) == date(2027, 2, 28)


def test_only_matching_product_lines_are_used_not_classification_codes():
    lines = [
        {"descripcion": "KIT DE CIRCUITO FICHA TECNICA 43358", "cantidad": 600},
        {"descripcion": "Otro producto ficha técnica 29209", "cantidad": 100000},
        {"descripcion": "Código de clasificación", "codigo": 43358, "cantidad": 200000},
    ]
    assert selected_kit_lines(lines, confirmed=True) == [lines[0]]
    assert selected_kit_lines([lines[2]], confirmed=False) == []


def test_unconfirmed_generic_or_other_ficha_is_not_assigned_to_43358():
    generic = {"descripcion": "Kit de circuito de paciente para anestesia"}
    wrong = {"descripcion": "Kit de circuito para anestesia ficha técnica 29209"}
    assert selected_kit_lines([generic], confirmed=False) == []
    assert selected_kit_lines([wrong], confirmed=True) == []
    assert selected_kit_lines([generic], confirmed=True) == [generic]
    assert selected_kit_lines([generic, generic], confirmed=True) == []


@pytest.mark.parametrize("text", ["Ficha técnica No. 43358", "C.T.N.I.: 43358", "F.T. N° 43358", "Ficha técnica: 43358"])
def test_explicit_document_labels_accept_number_prefixes_and_punctuation(text):
    assert document_ficha_codes(text) == {"43358"}


def test_document_classification_and_unlabelled_codes_are_not_ficha_evidence():
    assert document_ficha_codes("Clasificación 43358. Cantidad 600. Código CSS 02002908.") == set()


def test_conflicting_ficha_in_same_line_is_not_counted():
    assert selected_kit_lines([{"descripcion": "Kit circuito Ficha técnica No. 29209, referencia 43358"}], confirmed=True) == []


class Worksheet:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.row_count = 500

    def get_all_values(self):
        return [list(row) for row in self.rows]

    def row_values(self, number):
        return self.rows[number - 1] if number <= len(self.rows) else []

    def update(self, range_name, values, value_input_option=None):
        import re

        position = int(re.search(r"\d+", range_name)[0]) - 1
        while len(self.rows) < position + len(values):
            self.rows.append([])
        self.rows[position:position + len(values)] = [list(row) for row in values]

    def append_row(self, values, value_input_option=None):
        self.rows.append(list(values))

    def resize(self, rows):
        self.row_count = rows


class Book:
    def __init__(self):
        self.sheets = {}

    def worksheet(self, title):
        if title not in self.sheets:
            raise WorksheetNotFound(title)
        return self.sheets[title]

    def add_worksheet(self, title, rows, cols):
        self.sheets[title] = Worksheet()
        return self.sheets[title]


class Client:
    def __init__(self):
        self.book = Book()

    def open_by_key(self, identifier):
        return self.book


def test_history_publication_is_idempotent_and_never_erases_missing_history():
    client = Client()
    rotation.publish_history(client, [request()])
    rotation.publish_history(client, [request()])
    rotation.publish_history(client, [])
    snapshot = rotation.load_snapshot(client)
    assert len(snapshot["records"]) == 1
    assert snapshot["inventory"] is None


def test_inventory_is_persisted_once_and_zero_survives_reloading():
    client = Client()
    stamp = rotation.now()
    with patch.object(rotation, "now", return_value=stamp):
        rotation.save_inventory(client, 6000, stamp.date(), "usuario")
        rotation.save_inventory(client, 0, stamp.date(), "usuario")
    snapshot = rotation.load_snapshot(client)
    assert snapshot["inventory"]["kits"] == "0"
    assert len(client.book.sheets[rotation.INVENTORY_SHEET].rows) == 2


def test_failed_refresh_preserves_last_success_and_history():
    client = Client()
    rotation.publish_history(client, [request()])
    rotation.save_refresh_status(client)
    success = rotation.load_snapshot(client)["status"]["ultimo_exito"]
    rotation.save_refresh_status(client, "Fuente temporalmente caída")
    snapshot = rotation.load_snapshot(client)
    assert snapshot["status"]["ultimo_exito"] == success
    assert snapshot["status"]["error"]
    assert len(snapshot["records"]) == 1


def test_published_history_cannot_be_replaced_by_an_empty_read():
    client = Client()
    rotation.publish_history(client, [request()])
    rotation.save_refresh_status(client)
    client.book.sheets[rotation.HISTORY_SHEET].rows = [rotation.HISTORY_HEADERS]
    with pytest.raises(ValueError):
        rotation.load_snapshot(client)


def test_rotation_page_is_after_deep_study_and_does_not_require_supabase():
    source = (Path(__file__).resolve().parents[1] / "pages/inteligencia_oportunidades_proveedores.py").read_text("utf-8")
    runtime = source[source.rindex("\n_apply_pending_saved_view()\n"):]
    assert runtime.index('"Estudio profundo"') < runtime.index('"Rotación e inventario"')
    assert runtime.index('if selected_view == "Rotación e inventario":') < runtime.index("if advanced_filters:")
