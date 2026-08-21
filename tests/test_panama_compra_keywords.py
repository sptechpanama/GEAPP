from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from services.panama_compra_keywords import (
    KeywordRegistryConflictError,
    KeywordRegistryStore,
    apply_keyword_changes,
    match_keywords_in_text,
    normalize_keyword_terms,
    parse_keyword_registry_values,
)


class WorksheetNotFound(Exception):
    pass


class FakeWorksheet:
    def __init__(self, values=None, *, fail_reads: int = 0):
        self.values = [list(row) for row in (values or [])]
        self.fail_reads = fail_reads
        self.col_count = 3
        self.events: list[str] = []

    def get_all_values(self):
        self.events.append("read")
        if self.fail_reads:
            self.fail_reads -= 1
            raise ConnectionError("falla transitoria")
        return [list(row) for row in self.values]

    def update(
        self,
        values,
        range_name=None,
        *,
        value_input_option=None,
        **_kwargs,
    ):
        # Replica la firma de gspread 6.x para detectar argumentos invertidos.
        assert isinstance(values, list)
        assert isinstance(range_name, str)
        assert value_input_option == "RAW"
        self.events.append(f"update:{range_name}")
        rows = [list(row) for row in values]
        if len(self.values) < len(rows):
            self.values.extend([[] for _ in range(len(rows) - len(self.values))])
        self.values[: len(rows)] = rows

    def batch_clear(self, ranges):
        self.events.append(f"batch_clear:{ranges[0]}")
        start_match = re.search(r"A(\d+)", ranges[0])
        assert start_match
        self.values = self.values[: int(start_match.group(1)) - 1]

    def add_cols(self, count):
        self.col_count += count

    def clear(self):  # pragma: no cover - fallaria si el codigo vuelve a usarlo
        raise AssertionError("El registro nunca debe vaciar la hoja antes de escribir")


class FailFirstVerificationWorksheet(FakeWorksheet):
    def __init__(self, values=None):
        super().__init__(values)
        self.read_number = 0

    def get_all_values(self):
        self.read_number += 1
        self.events.append("read")
        if self.read_number == 2:
            raise ConnectionError("la escritura llego pero fallo la verificacion")
        return [list(row) for row in self.values]


class FakeSpreadsheet:
    def __init__(self, worksheet=None, *, missing: bool = False):
        self.ws = worksheet
        self.missing = missing
        self.created = False

    def worksheet(self, _name):
        if self.missing:
            raise WorksheetNotFound()
        return self.ws

    def add_worksheet(self, **_kwargs):
        self.ws = FakeWorksheet()
        self.missing = False
        self.created = True
        return self.ws


class FakeClient:
    def __init__(self, spreadsheet):
        self.spreadsheet = spreadsheet

    def open_by_key(self, _sheet_id):
        return self.spreadsheet


def make_store(worksheet=None, *, missing=False, attempts=3):
    spreadsheet = FakeSpreadsheet(worksheet, missing=missing)
    client = FakeClient(spreadsheet)
    store = KeywordRegistryStore(
        lambda: client,
        sheet_id="sheet-id",
        worksheet_name="pc_palabras_clave",
        attempts=attempts,
        sleeper=lambda _seconds: None,
    )
    return store, spreadsheet


def test_normalization_and_phrase_matching_are_stable():
    assert normalize_keyword_terms(
        [" Solar ", "fotovoltaico", "SOLAR", "Manejadora de aire"]
    ) == ["solar", "fotovoltaico", "manejadora de aire"]
    assert match_keywords_in_text(
        "SUMINISTRO DE MANEJADORA DE AIRE Y PANELES FOTOVOLTAICOS",
        ["manejadora de aire", "solar", "fotovoltaico"],
    ) == ["manejadora de aire"]


def test_trailing_asterisk_matches_a_root_without_changing_exact_terms():
    assert normalize_keyword_terms(
        [" Fotovolta* ", "FOTOVOLTA*", "serpentín", "UMA"]
    ) == ["fotovolta*", "serpentin", "uma"]

    text = "Paneles fotovoltaicos y solución fotovoltaica para una UMA."
    assert match_keywords_in_text(
        text,
        ["fotovolta*", "fotovolta", "uma"],
    ) == ["fotovolta*", "uma"]

    assert match_keywords_in_text("equipo prefotovoltaico", ["fotovolta*"]) == []
    assert match_keywords_in_text("equipo UMAC", ["uma"]) == []


def test_registry_round_trip_preserves_root_marker():
    values = [
        ["Palabra clave", "Actualizado por", "Actualizado"],
        ["Fotovolta*", "ana", "hoy"],
        ["Agua helada", "ana", "hoy"],
    ]
    assert parse_keyword_registry_values(values) == ["fotovolta*", "agua helada"]


def test_registry_parser_accepts_header_and_deduplicates():
    values = [
        ["Palabra clave", "Actualizado por", "Actualizado"],
        ["Solar", "ana", "hoy"],
        ["fotovoltaico", "ana", "hoy"],
        ["SOLAR", "ana", "hoy"],
    ]
    assert parse_keyword_registry_values(values) == ["solar", "fotovoltaico"]


def test_apply_changes_preserves_order_and_handles_multiple_terms():
    assert apply_keyword_changes(
        ["chiller", "solar", "manejadora"],
        add=["fotovoltaico", "solar"],
        remove=["chiller"],
    ) == ["solar", "manejadora", "fotovoltaico"]


def test_load_retries_and_returns_the_complete_remote_list():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "user", "now"],
            ["solar", "user", "now"],
            ["fotovoltaico", "user", "now"],
            ["manejadora", "user", "now"],
        ],
        fail_reads=2,
    )
    store, _ = make_store(ws)
    snapshot = store.load(last_good=["respaldo"])
    assert snapshot.remote_ok is True
    assert list(snapshot.terms) == ["chiller", "solar", "fotovoltaico", "manejadora"]
    assert ws.events.count("read") == 3


def test_load_failure_uses_last_good_instead_of_defaults():
    ws = FakeWorksheet([], fail_reads=5)
    store, _ = make_store(ws, attempts=3)
    snapshot = store.load(last_good=["solar", "fotovoltaico", "manejadora"])
    assert snapshot.remote_ok is False
    assert list(snapshot.terms) == ["solar", "fotovoltaico", "manejadora"]
    assert snapshot.source == "ultima lectura valida"
    assert snapshot.warning


def test_empty_remote_sheet_restores_last_good_copy():
    ws = FakeWorksheet([])
    store, _ = make_store(ws)
    snapshot = store.load(last_good=["solar", "fotovoltaico", "manejadora"])
    assert snapshot.remote_ok is True
    assert list(snapshot.terms) == ["solar", "fotovoltaico", "manejadora"]
    assert parse_keyword_registry_values(ws.values) == [
        "solar",
        "fotovoltaico",
        "manejadora",
    ]


def test_save_writes_first_clears_only_surplus_and_verifies():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "old", "old"],
            ["solar", "old", "old"],
            ["fotovoltaico", "old", "old"],
            ["manejadora", "old", "old"],
        ]
    )
    store, _ = make_store(ws)
    verified = store.save(
        ["solar", "manejadora"],
        updated_by="rsanchez",
        expected_current=["chiller", "solar", "fotovoltaico", "manejadora"],
    )
    assert verified == ["solar", "manejadora"]
    update_index = next(i for i, event in enumerate(ws.events) if event.startswith("update:"))
    clear_index = next(i for i, event in enumerate(ws.events) if event.startswith("batch_clear:"))
    assert update_index < clear_index
    assert parse_keyword_registry_values(ws.values) == ["solar", "manejadora"]


def test_stale_session_cannot_overwrite_a_newer_remote_change():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "other", "now"],
            ["solar", "other", "now"],
        ]
    )
    store, _ = make_store(ws)
    with pytest.raises(KeywordRegistryConflictError):
        store.save(
            ["chiller", "manejadora"],
            updated_by="rsanchez",
            expected_current=["chiller"],
        )
    assert parse_keyword_registry_values(ws.values) == ["chiller", "solar"]
    assert not any(event.startswith("update:") for event in ws.events)


def test_save_retry_is_idempotent_if_write_succeeded_before_read_failed():
    ws = FailFirstVerificationWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "old", "old"],
            ["solar", "old", "old"],
        ]
    )
    store, _ = make_store(ws)
    verified = store.save(
        ["chiller", "solar", "manejadora"],
        updated_by="rsanchez",
        expected_current=["chiller", "solar"],
    )
    assert verified == ["chiller", "solar", "manejadora"]
    assert parse_keyword_registry_values(ws.values) == verified


def test_missing_worksheet_is_created_with_safe_defaults():
    store, spreadsheet = make_store(missing=True)
    snapshot = store.load()
    assert snapshot.remote_ok is True
    assert spreadsheet.created is True
    assert list(snapshot.terms) == ["chiller", "york", "daikin"]


def test_mutate_adds_root_term_from_latest_remote_state_and_verifies():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "old", "old"],
            ["solar", "other", "now"],
        ]
    )
    store, _ = make_store(ws)

    verified, changed = store.mutate(
        add=[" Fotovolta* ", "solar"],
        updated_by="rsanchez",
    )

    assert changed is True
    assert verified == ["chiller", "solar", "fotovolta*"]
    assert parse_keyword_registry_values(ws.values) == verified
    assert ws.events.count("read") == 2
    assert sum(event.startswith("update:") for event in ws.events) == 1


def test_mutate_avoids_rewriting_when_term_is_already_configured():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "old", "old"],
            ["fotovolta*", "old", "old"],
        ]
    )
    store, _ = make_store(ws)

    verified, changed = store.mutate(
        add=["fotovolta*"],
        updated_by="rsanchez",
    )

    assert changed is False
    assert verified == ["chiller", "fotovolta*"]
    assert ws.events == ["read"]


def test_mutate_removes_root_term_without_touching_other_terms():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["chiller", "old", "old"],
            ["fotovolta*", "old", "old"],
            ["agua helada", "old", "old"],
        ]
    )
    store, _ = make_store(ws)

    verified, changed = store.mutate(
        remove=["fotovolta*"],
        updated_by="rsanchez",
    )

    assert changed is True
    assert verified == ["chiller", "agua helada"]
    assert parse_keyword_registry_values(ws.values) == verified


def test_keyword_manager_uses_atomic_form_without_forced_remote_reload():
    """El texto y el boton deben enviarse juntos sin una segunda lectura previa."""

    page_path = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"
    tree = ast.parse(page_path.read_text(encoding="utf-8"))
    manager = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_render_keyword_watch_manager"
    )

    attribute_calls = {
        node.func.attr
        for node in ast.walk(manager)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "form" in attribute_calls
    assert "form_submit_button" in attribute_calls

    form_keys = {
        value.value
        for node in ast.walk(manager)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "form"
        for keyword in node.keywords
        if keyword.arg == "key"
        and isinstance(keyword.value, ast.JoinedStr)
        for value in keyword.value.values
        if isinstance(value, ast.Constant) and isinstance(value.value, str)
    }
    assert "_add_form" in form_keys
    assert "_remove_form" in form_keys

    forced_loads = [
        node
        for node in ast.walk(manager)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_load_panama_keyword_terms"
        and any(
            keyword.arg == "force"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value is True
            for keyword in node.keywords
        )
    ]
    assert forced_loads == []
