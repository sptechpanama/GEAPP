import ast
import math
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from services.finance_financing import (
    CreditTerms,
    FactoringTerms,
    FinancingCost,
    cheapest,
    cost_curve,
    credit_balance,
    credit_cost,
    credit_terms_from_row,
    factoring_cost,
    private_cost,
)


@pytest.fixture
def line():
    return {
        "RowID": "line-1", "Empresa": "RS-SP", "Nombre linea": "Kit de anestecia", "Banco": "Banistmo",
        "Limite vigente": 55000.0, "Tasa diaria pct": 0.025333, "Fecha vigencia tasa": "2026-05-04",
        "Cargo anual pct": 1.0, "Cargo desembolso fijo": 150.0, "Cargo banca en linea mensual": 0.0,
        "Seguro incendio 1 anual": 58.93, "Seguro incendio 2 anual": 87.99,
        "Poliza vida mensual": 17.2, "Activa": "Sí",
    }


@pytest.mark.parametrize("months, expected", [(0, 0), (0.5, 125), (1, 250), (3, 750), (8, 2000)])
def test_private_interest_is_simple_at_2_point_5_monthly(months, expected):
    result = private_cost(10000, months)
    assert result.interest == expected
    assert result.charges == 0
    assert result.total == expected


@pytest.mark.parametrize("amount, months", [(0, 1), (-1, 1), (100, -1), (math.nan, 1), (100, math.inf), (None, 1), ("bad", 1)])
def test_invalid_principal_and_term_are_rejected(amount, months):
    with pytest.raises(ValueError):
        private_cost(amount, months)


def test_half_cent_uses_financial_rounding():
    assert private_cost(1, 1).interest == 0.03


def test_saved_bank_conditions_are_used_without_changing_them(line):
    original = line.copy()
    terms = credit_terms_from_row(line, 5000)
    assert terms.available == 50000
    assert terms.annual_cost == 696.92
    result = credit_cost(10000, 3, terms)
    assert result.interest == 228.0
    assert result.charges == 201.6
    assert result.total == 429.6
    assert line == original


def test_annual_costs_are_only_allocated_when_explicitly_requested(line):
    terms = credit_terms_from_row(line)
    assert credit_cost(10000, 3, terms, allocate_annual=True).charges == 375.83
    assert credit_cost(10000, 0, terms).total == 150


def test_bank_daily_interest_is_not_compounded():
    terms = CreditTerms(0.1, 10000)
    assert credit_cost(10000, 8, terms).interest == 2400


@pytest.mark.parametrize("field", ["Tasa diaria pct", "Limite vigente"])
def test_missing_bank_rate_or_limit_is_not_free_credit(line, field):
    del line[field]
    with pytest.raises(ValueError):
        credit_terms_from_row(line)


def test_credit_limit_is_checked_and_repaid_capital_is_released(line):
    terms = credit_terms_from_row(line, 54000)
    assert credit_cost(1000, 1, terms).interest == 7.6
    with pytest.raises(ValueError, match="cupo"):
        credit_cost(1001, 1, terms)
    assert credit_terms_from_row(line, 56000).available == 0


def test_credit_balance_separates_companies_and_ignores_interest_payments(line):
    incomes = pd.DataFrame([
        {"Empresa": "RS-SP", "Instrumento financiero": "Kit de anestecia", "Registro financiamiento": "Desembolso", "Monto real cobrado": 10000},
        {"Empresa": "RIR", "Instrumento financiero": "Kit de anestecia", "Registro financiamiento": "Desembolso", "Monto real cobrado": 40000},
        {"Empresa": "RS-SP", "Instrumento financiero": "Otra", "Registro financiamiento": "Desembolso", "Monto real cobrado": 60000},
    ])
    expenses = pd.DataFrame([
        {"Empresa": "RS-SP", "Instrumento financiero": "Kit de anestecia", "Registro financiamiento": "Pago capital", "Monto real pagado": 2000},
        {"Empresa": "RS-SP", "Instrumento financiero": "Kit de anestecia", "Registro financiamiento": "Pago interes", "Monto real pagado": 500},
    ])
    assert credit_balance(line, incomes, expenses) == 8000
    assert credit_terms_from_row(line, credit_balance(line, incomes, expenses)).available == 47000


def test_unknown_credit_movements_are_not_treated_as_unused_limit(line):
    with pytest.raises(ValueError, match="movimientos"):
        credit_balance(line, pd.DataFrame([{"Monto": 10000}]), pd.DataFrame())


def test_empty_movements_and_excess_repayments_do_not_make_negative_debt(line):
    assert credit_balance(line, pd.DataFrame(), pd.DataFrame()) == 0
    expenses = pd.DataFrame([{"Empresa": "RS-SP", "Instrumento financiero": "Kit de anestecia", "Registro financiamiento": "Pago capital", "Monto real pagado": 100}])
    assert credit_balance(line, pd.DataFrame(), expenses) == 0


def test_factoring_compares_the_same_net_cash_and_does_not_expense_the_reserve():
    terms = FactoringTerms(1, 2, 90, "factura")
    result = factoring_cost(8800, 3, terms)
    assert result.invoice_amount == 10000
    assert result.retained == 1000
    assert result.charges == 200
    assert result.interest == 300
    assert result.total == 500
    assert result.invoice_amount - result.retained - result.charges == 8800


def test_factoring_interest_on_advance_is_not_interest_on_invoice():
    terms = FactoringTerms(1, 2, 90, "anticipo")
    assert factoring_cost(8800, 3, terms).interest == 270


def test_factoring_fixed_fee_is_included_in_net_funding():
    result = factoring_cost(8700, 0, FactoringTerms(0, 2, 90, "factura", 100))
    assert result.invoice_amount == 10000
    assert result.charges == 300
    assert result.total == 300


@pytest.mark.parametrize("overrides", [
    {"advance_pct": 0}, {"advance_pct": 101}, {"advance_pct": 2},
    {"initial_commission_pct": 91}, {"monthly_pct": -1},
    {"monthly_pct": math.nan}, {"fixed_fee": math.inf}, {"interest_basis": "unknown"},
])
def test_invalid_factoring_conditions_are_not_ranked(overrides):
    terms = replace(FactoringTerms(1, 2, 90, "factura"), **overrides)
    with pytest.raises(ValueError):
        factoring_cost(10000, 3, terms)


def test_curve_has_only_available_options_and_exact_zero_and_eight_endpoints():
    curve = cost_curve({"Privados": lambda months: private_cost(10000, months)})
    assert list(curve.columns) == ["Privados"]
    assert len(curve) == 81
    assert curve.index.min() == 0
    assert curve.index.max() == 8
    assert curve.iloc[0, 0] == 0
    assert curve.iloc[-1, 0] == 2000


def test_cheapest_uses_total_cost_not_interest_and_preserves_ties():
    costs = [FinancingCost("A", 0, 1000), FinancingCost("B", 500, 0)]
    assert cheapest(costs) == ["B"]
    assert cheapest([FinancingCost("A", 100, 0), FinancingCost("B", 0, 100)]) == ["A", "B"]
    assert cheapest([]) == []


def test_choice_changes_as_the_term_increases():
    terms = CreditTerms(0.01, 10000, disbursement_fee=150)
    assert cheapest([private_cost(10000, 0.5), credit_cost(10000, 0.5, terms)]) == ["Inversionistas privados"]
    assert cheapest([private_cost(10000, 3), credit_cost(10000, 3, terms)]) == ["Línea de crédito"]


def test_finance_page_integrates_the_small_comparison_tab():
    source = (Path(__file__).resolve().parents[1] / "pages" / "finance.py").read_text(encoding="utf-8")
    parsed = ast.parse(source)
    tabs = [node for node in ast.walk(parsed) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "tabs"]
    assert any("Elección de financiamiento" in ast.literal_eval(node.args[0]) for node in tabs)
    assert "render_financing_comparison(" in source
