import pandas as pd
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest

from services import finance_financing_view as view


@pytest.fixture
def line():
    return {
        "RowID": "line-1", "Empresa": "RS-SP", "Nombre linea": "Kit de anestecia", "Banco": "Banistmo",
        "Limite vigente": 55000.0, "Tasa diaria pct": 0.025333, "Fecha vigencia tasa": "2026-05-04",
        "Cargo anual pct": 1.0, "Cargo desembolso fijo": 150.0, "Cargo banca en linea mensual": 0.0,
        "Seguro incendio 1 anual": 58.93, "Seguro incendio 2 anual": 87.99,
        "Poliza vida mensual": 17.2, "Activa": "Sí",
    }


def app(monkeypatch, lines=None, empresa="Todas"):
    monkeypatch.setattr(view, "test_lines", pd.DataFrame([] if lines is None else lines), raising=False)
    source = (
        "import pandas as pd\n"
        "from services import finance_financing_view as view\n"
        f"view.render_financing_comparison(lambda: view.test_lines, pd.DataFrame(), pd.DataFrame(), {empresa!r})"
    )
    return AppTest.from_string(source, default_timeout=20).run()


def set_available(tested, *keys):
    tested.number_input(key="fin_compare_amount").set_value(10000)
    for key in keys:
        tested.checkbox(key=f"fin_compare_{key}").check()
    return tested.run()


def complete_factoring(tested):
    tested.number_input(key="fin_compare_factoring_rate").set_value(1.0)
    tested.number_input(key="fin_compare_factoring_commission").set_value(2.0)
    tested.number_input(key="fin_compare_factoring_advance").set_value(90.0)
    tested.selectbox(key="fin_compare_factoring_basis").select("factura")
    return tested.run()


def test_initial_screen_is_small_and_does_not_assume_availability(monkeypatch):
    tested = app(monkeypatch)
    assert not tested.exception
    assert [control.label for control in tested.checkbox] == ["Línea de crédito", "Inversionistas privados", "Factoring"]
    assert all(not control.value for control in tested.checkbox)
    assert len(tested.number_input) == 2
    assert not tested.dataframe
    assert not tested.success


def test_a_single_option_shows_cost_but_does_not_claim_to_be_best(monkeypatch):
    tested = set_available(app(monkeypatch), "private")
    assert not tested.exception
    assert tested.dataframe[0].value.iloc[0]["Costo total (USD)"] == 750
    assert not tested.success


def test_two_options_compare_actual_bank_rate_and_only_marked_options(monkeypatch, line):
    tested = set_available(app(monkeypatch, [line]), "private", "credit")
    assert not tested.exception
    table = tested.dataframe[0].value
    assert table["Opción"].tolist() == ["Línea de crédito", "Inversionistas privados"]
    assert table.iloc[0]["Costo total (USD)"] == 429.6
    assert "Línea de crédito" in tested.success[0].value


def test_factoring_missing_conditions_is_excluded_not_free(monkeypatch):
    tested = set_available(app(monkeypatch), "private", "factoring")
    assert not tested.exception
    assert tested.dataframe[0].value["Opción"].tolist() == ["Inversionistas privados"]
    assert any("pendiente de condiciones" in message.value for message in tested.warning)
    assert not tested.success


def test_all_three_options_appear_when_conditions_are_complete(monkeypatch, line):
    tested = set_available(app(monkeypatch, [line]), "credit", "private", "factoring")
    tested = complete_factoring(tested)
    assert not tested.exception
    assert set(tested.dataframe[0].value["Opción"]) == {"Línea de crédito", "Inversionistas privados", "Factoring"}
    assert any("factura necesaria $11,363.64" in caption.value for caption in tested.caption)


def test_unchecking_an_option_removes_it_immediately(monkeypatch, line):
    tested = set_available(app(monkeypatch, [line]), "credit", "private")
    tested.checkbox(key="fin_compare_credit").uncheck().run()
    assert not tested.exception
    assert tested.dataframe[0].value["Opción"].tolist() == ["Inversionistas privados"]


def test_missing_credit_configuration_does_not_break_other_options(monkeypatch):
    tested = set_available(app(monkeypatch), "credit", "private")
    assert not tested.exception
    assert len(tested.warning) == 1
    assert len(tested.dataframe[0].value) == 1


def test_company_filter_does_not_offer_another_companys_credit(monkeypatch, line):
    tested = set_available(app(monkeypatch, [line], empresa="RIR"), "credit", "private")
    assert not tested.exception
    assert len(tested.dataframe[0].value) == 1
    assert any("No hay líneas activas" in warning.value for warning in tested.warning)


def test_inactive_and_future_credit_rates_are_not_used(monkeypatch, line):
    line["Fecha vigencia tasa"] = "2099-01-01"
    tested = set_available(app(monkeypatch, [line]), "credit", "private")
    assert not tested.exception
    assert len(tested.dataframe[0].value) == 1
    line["Activa"] = "No"
    tested = set_available(app(monkeypatch, [line]), "credit", "private")
    assert not tested.exception
    assert len(tested.dataframe[0].value) == 1


def test_funding_beyond_limit_excludes_only_credit(monkeypatch, line):
    line["Limite vigente"] = 5000
    tested = set_available(app(monkeypatch, [line]), "credit", "private")
    assert not tested.exception
    assert tested.dataframe[0].value["Opción"].tolist() == ["Inversionistas privados"]
    assert any("cupo" in warning.value for warning in tested.warning)


def test_bad_factoring_conditions_do_not_break_credit_or_investors(monkeypatch, line):
    tested = set_available(app(monkeypatch, [line]), "credit", "private", "factoring")
    tested = complete_factoring(tested)
    tested.number_input(key="fin_compare_factoring_commission").set_value(90.0).run()
    assert not tested.exception
    assert len(tested.dataframe[0].value) == 2
    assert any("anticipo" in warning.value.lower() for warning in tested.warning)


def test_annual_cost_toggle_recalculates_without_changing_bank_configuration(monkeypatch, line):
    tested = set_available(app(monkeypatch, [line]), "credit", "private")
    tested.checkbox(key="fin_compare_annual").check().run()
    assert not tested.exception
    assert tested.dataframe[0].value.iloc[0]["Costo total (USD)"] == 603.83
    assert line["Cargo anual pct"] == 1


def test_repeated_runs_keep_inputs_and_recalculate_the_term(monkeypatch):
    tested = set_available(app(monkeypatch), "private")
    tested.number_input(key="fin_compare_months").set_value(8.0).run()
    tested.run()
    assert not tested.exception
    assert tested.checkbox(key="fin_compare_private").value
    assert tested.number_input(key="fin_compare_amount").value == 10000
    assert tested.dataframe[0].value.iloc[0]["Costo total (USD)"] == 2000


def test_zero_principal_does_not_offer_a_false_zero_cost_comparison(monkeypatch):
    tested = app(monkeypatch)
    tested.checkbox(key="fin_compare_private").check().run()
    assert not tested.exception
    assert not tested.dataframe
    assert not tested.success


def test_zero_bank_rate_is_not_presented_as_confirmed_free_credit(monkeypatch, line):
    line["Tasa diaria pct"] = 0
    tested = set_available(app(monkeypatch, [line]), "credit", "private")
    assert not tested.exception
    assert tested.dataframe[0].value["Opción"].tolist() == ["Inversionistas privados"]
    assert any("tasa diaria está en cero" in warning.value for warning in tested.warning)


def test_credit_read_failure_does_not_remove_the_other_option(monkeypatch, line):
    tested = app(monkeypatch, [line])
    def unavailable(*args, **kwargs):
        raise RuntimeError("Fuente no disponible")
    monkeypatch.setattr(view, "credit_terms_from_row", unavailable)
    tested = set_available(tested, "credit", "private")
    assert not tested.exception
    assert tested.dataframe[0].value["Opción"].tolist() == ["Inversionistas privados"]


def test_fewer_interests_is_identified_separately_from_total_cost(monkeypatch):
    tested = set_available(app(monkeypatch), "private", "factoring")
    tested = complete_factoring(tested)
    tested.number_input(key="fin_compare_factoring_rate").set_value(0.0)
    tested.number_input(key="fin_compare_factoring_commission").set_value(20.0).run()
    assert not tested.exception
    assert "Inversionistas privados" in tested.success[0].value
    assert any("Menor interés: Factoring" in caption.value for caption in tested.caption)
