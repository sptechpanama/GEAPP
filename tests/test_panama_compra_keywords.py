from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from services.panama_compra_keywords import (
    DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    HVAC_OVER_15K_KEYWORDS,
    KeywordRegistryConflictError,
    KeywordRegistryStore,
    apply_keyword_changes,
    keyword_table_column_order,
    match_keywords_in_text,
    match_negative_keywords_in_text,
    negative_keywords_in_matching_context,
    normalize_keyword_terms,
    parse_keyword_input,
    parse_keyword_rule,
    parse_keyword_registry_values,
    parse_reference_amount,
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


def make_store(worksheet=None, *, missing=False, attempts=3, defaults=None):
    spreadsheet = FakeSpreadsheet(worksheet, missing=missing)
    client = FakeClient(spreadsheet)
    kwargs = {
        "sheet_id": "sheet-id",
        "worksheet_name": "pc_palabras_clave",
        "attempts": attempts,
        "sleeper": lambda _seconds: None,
    }
    if defaults is not None:
        kwargs["defaults"] = defaults
    store = KeywordRegistryStore(lambda: client, **kwargs)
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


def test_default_negative_terms_are_minimal_and_canonical():
    assert list(DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS) == [
        "automotriz",
        "habitacion de hotel",
        "bloqueador solar",
        "protector solar",
        "oracle solaris",
        "correa del serpentin",
    ]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Servicio automotriz preventivo", "automotriz"),
        ("Alquiler de HABITACIONES (HOTEL)", "habitacion de hotel"),
        ("Compra de protector solar", "protector solar"),
        ("Licencias Oracle Solaris", "oracle solaris"),
        ("Cambio de correas del serpentín", "correa del serpentin"),
    ],
)
def test_negative_terms_match_only_the_configured_obvious_contexts(text, expected):
    assert match_negative_keywords_in_text(
        text,
        DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    ) == [expected]


def test_negative_context_does_not_expand_to_unrelated_terms():
    assert match_negative_keywords_in_text(
        "Mantenimiento de serpentín de aire acondicionado para un hotel",
        DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    ) == []
    assert match_negative_keywords_in_text(
        "Panel solar fotovoltaico",
        DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    ) == []


def test_negative_filter_only_uses_title_and_positive_matching_fields():
    matches = negative_keywords_in_matching_context(
        title="Servicio general",
        matched_field_values=["Aire acondicionado automotriz"],
        negative_keywords=DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    )
    assert matches == ["automotriz"]

    # Un renglon no relacionado no se pasa como campo coincidente y, por lo
    # tanto, no puede ocultar una oportunidad valida.
    assert negative_keywords_in_matching_context(
        title="Sistema fotovoltaico para edificio",
        matched_field_values=["Paneles fotovoltaicos"],
        negative_keywords=DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    ) == []


def test_amount_modifier_preserves_exact_or_root_matching():
    assert normalize_keyword_terms(
        [" Aire Acondicion*>15K ", "VRF > 15000", "split>$15,000.00"]
    ) == ["aire acondicion*>15k", "vrf>15k", "split>15k"]

    root_rule = parse_keyword_rule("aire acondicion*>15k")
    assert root_rule is not None
    assert root_rule.is_root is True
    assert root_rule.minimum_amount == 15_000

    text = "Suministro de aires acondicionados tipo split y sistema VRF"
    assert match_keywords_in_text(
        text,
        ["aires acondicion*>15k", "split>15k", "vrf>15k"],
        reference_amount="$14,999.99",
    ) == []
    assert match_keywords_in_text(
        text,
        ["aires acondicion*>15k", "split>15k", "vrf>15k"],
        reference_amount="$15,000.00",
    ) == []
    assert match_keywords_in_text(
        text,
        ["aires acondicion*>15k", "split>15k", "vrf>15k"],
        reference_amount="B/. 15.000,50",
    ) == ["aires acondicion*>15k", "split>15k", "vrf>15k"]


def test_known_legacy_hvac_rows_are_recovered_without_guessing_other_text():
    assert normalize_keyword_terms(
        ["aire acondicion 15k", "split 15k", "climatizacion 15k"]
    ) == ["aire acondicion*>15k", "split>15k", "climatizacion*>15k"]
    assert normalize_keyword_terms(["proyecto especial 15k"]) == [
        "proyecto especial 15k"
    ]


def test_add_remove_text_boxes_parse_multiple_rules_and_deduplicate():
    assert parse_keyword_input(
        " fotovolta*, split>15k; VRF > 15000\nFotovolta* "
    ) == ["fotovolta*", "split>15k", "vrf>15k"]


def test_rs_sp_detection_columns_are_between_description_and_items():
    columns = [
        "Enlace",
        "Descripción",
        "Item_1",
        "Item_2",
        "Entidad",
        "Tipo convocatoria",
        "Pestana origen",
        "Palabras clave detectadas",
        "Campos con coincidencia",
    ]

    assert keyword_table_column_order(columns) == [
        "Enlace",
        "Descripción",
        "Palabras clave detectadas",
        "Campos con coincidencia",
        "Tipo convocatoria",
        "Pestana origen",
        "Item_1",
        "Item_2",
        "Entidad",
    ]


def test_column_order_is_unchanged_when_detection_context_is_absent():
    columns = ["Enlace", "Descripción", "Item_1"]
    assert keyword_table_column_order(columns) == columns


def test_reference_amount_parser_accepts_common_panama_compra_formats():
    assert parse_reference_amount("$15,000.50") == 15_000.50
    assert parse_reference_amount("B/. 15.000,50") == 15_000.50
    assert parse_reference_amount("15000,50") == 15_000.50
    assert parse_reference_amount(16_250) == 16_250
    assert parse_reference_amount("") is None


def test_specific_threshold_rule_wins_over_same_unrestricted_term():
    assert match_keywords_in_text(
        "Mantenimiento de chiller",
        ["chiller", "chiller>15k"],
        reference_amount=20_000,
    ) == ["chiller>15k"]
    assert match_keywords_in_text(
        "Mantenimiento de chiller",
        ["chiller", "chiller>15k"],
        reference_amount=10_000,
    ) == ["chiller"]


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


def test_load_repairs_all_legacy_hvac_rows_in_the_persisted_sheet():
    damaged = [rule.replace("*", "").replace(">", " ") for rule in HVAC_OVER_15K_KEYWORDS]
    ws = FakeWorksheet(
        [["Palabra clave", "Actualizado por", "Actualizado"]]
        + [[term, "old", "old"] for term in damaged]
    )
    store, _ = make_store(ws)

    snapshot = store.load()

    assert snapshot.remote_ok is True
    assert list(snapshot.terms) == list(HVAC_OVER_15K_KEYWORDS)
    assert [row[0] for row in ws.values[1:]] == list(HVAC_OVER_15K_KEYWORDS)
    assert sum(event.startswith("update:") for event in ws.events) == 1


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
    assert list(snapshot.terms[:3]) == ["chiller", "york", "daikin"]
    assert "aire acondicion*>15k" in snapshot.terms
    assert "vrf>15k" in snapshot.terms


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


def test_mutate_repairs_legacy_rows_even_when_logical_change_is_empty():
    ws = FakeWorksheet(
        [
            ["Palabra clave", "Actualizado por", "Actualizado"],
            ["split 15k", "old", "old"],
            ["fotovolta*", "old", "old"],
        ]
    )
    store, _ = make_store(ws)

    verified, changed = store.mutate(
        add=["split>15k"],
        updated_by="rsanchez",
    )

    assert changed is True
    assert verified == ["split>15k", "fotovolta*"]
    assert [row[0] for row in ws.values[1:]] == verified


def test_mutate_add_then_remove_preserves_every_unrelated_rule():
    original = ["chiller", "fotovolta*", "vrf>15k"]
    ws = FakeWorksheet(
        [["Palabra clave", "Actualizado por", "Actualizado"]]
        + [[term, "old", "old"] for term in original]
    )
    store, _ = make_store(ws)

    added, changed_add = store.mutate(
        add=["prueba temporal*>15k"],
        updated_by="prueba",
    )
    removed, changed_remove = store.mutate(
        remove=["prueba temporal*>15k"],
        updated_by="prueba",
    )

    assert changed_add is True
    assert changed_remove is True
    assert added == original + ["prueba temporal*>15k"]
    assert removed == original


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


def test_missing_negative_worksheet_is_seeded_with_only_requested_defaults():
    store, spreadsheet = make_store(
        missing=True,
        defaults=DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS,
    )
    snapshot = store.load()
    assert snapshot.remote_ok is True
    assert spreadsheet.created is True
    assert list(snapshot.terms) == list(DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS)


def test_negative_panel_precedes_database_and_unused_sections_are_not_rendered():
    page_path = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"
    source = page_path.read_text(encoding="utf-8")
    assert source.index('with st.expander("Palabras negativas RS/SP"') < source.index(
        'with st.expander("Base de datos de actos publicos'
    )

    tree = ast.parse(source)
    rendered_calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "render_prospeccion_rir_panel" not in rendered_calls
    assert "render_panamacompra_ai_chat" not in rendered_calls


def test_negative_manager_has_persistent_add_and_remove_forms():
    page_path = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"
    tree = ast.parse(page_path.read_text(encoding="utf-8"))
    manager = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_render_negative_keyword_manager"
    )
    form_suffixes = {
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
    assert {"_add_form", "_remove_form"}.issubset(form_suffixes)
