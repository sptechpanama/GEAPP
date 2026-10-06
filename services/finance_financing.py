from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd


PRIVATE_MONTHLY_PCT = Decimal("2.5")
DAYS_PER_MONTH = Decimal("30")


def _number(value, label: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Falta un valor válido para {label}.") from None
    if not number.is_finite() or number < 0:
        raise ValueError(f"{label} debe ser un número finito, mayor o igual a cero.")
    return number


def _money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class CreditTerms:
    daily_pct: float
    available: float
    disbursement_fee: float = 0.0
    monthly_fees: float = 0.0
    annual_cost: float = 0.0


@dataclass(frozen=True)
class FactoringTerms:
    monthly_pct: float
    initial_commission_pct: float
    advance_pct: float
    interest_basis: str
    fixed_fee: float = 0.0


@dataclass(frozen=True)
class FinancingCost:
    option: str
    interest: float
    charges: float
    invoice_amount: float | None = None
    retained: float | None = None

    @property
    def total(self) -> float:
        return _money(Decimal(str(self.interest)) + Decimal(str(self.charges)))


def _principal_and_months(amount, months) -> tuple[Decimal, Decimal]:
    principal = _number(amount, "el monto a financiar")
    period = _number(months, "el plazo")
    if principal == 0:
        raise ValueError("Ingresa un monto a financiar mayor a cero.")
    return principal, period


def private_cost(amount, months) -> FinancingCost:
    principal, period = _principal_and_months(amount, months)
    return FinancingCost("Inversionistas privados", _money(principal * PRIVATE_MONTHLY_PCT / 100 * period), 0.0)


def credit_cost(amount, months, terms: CreditTerms, *, allocate_annual: bool = False) -> FinancingCost:
    principal, period = _principal_and_months(amount, months)
    available = _number(terms.available, "el cupo disponible")
    if principal > available:
        raise ValueError("El monto supera el cupo estimado disponible de la línea de crédito.")
    rate = _number(terms.daily_pct, "la tasa diaria")
    interest = principal * rate / 100 * DAYS_PER_MONTH * period
    charges = _number(terms.disbursement_fee, "el cargo por desembolso")
    charges += _number(terms.monthly_fees, "los cargos mensuales") * period
    annual = _number(terms.annual_cost, "los costos anuales")
    if allocate_annual:
        charges += annual * period / 12
    return FinancingCost("Línea de crédito", _money(interest), _money(charges))


def factoring_cost(amount, months, terms: FactoringTerms) -> FinancingCost:
    principal, period = _principal_and_months(amount, months)
    rate = _number(terms.monthly_pct, "la tasa mensual de factoring")
    commission_pct = _number(terms.initial_commission_pct, "la comisión inicial de factoring") / 100
    advance_pct = _number(terms.advance_pct, "el anticipo de factoring") / 100
    fixed_fee = _number(terms.fixed_fee, "el cargo fijo de factoring")
    if not 0 < advance_pct <= 1 or commission_pct >= advance_pct:
        raise ValueError("El anticipo debe ser mayor a la comisión y estar entre 0 y 100%.")
    if terms.interest_basis not in {"factura", "anticipo"}:
        raise ValueError("Indica si la tasa de factoring se aplica a la factura o al anticipo.")
    invoice = (principal + fixed_fee) / (advance_pct - commission_pct)
    advance = invoice * advance_pct
    basis = invoice if terms.interest_basis == "factura" else advance
    interest = basis * rate / 100 * period
    charges = invoice * commission_pct + fixed_fee
    return FinancingCost("Factoring", _money(interest), _money(charges), _money(invoice), _money(invoice - advance))


def credit_balance(row: dict, ingresos: pd.DataFrame, gastos: pd.DataFrame) -> float:
    name = str(row.get("Nombre linea", "")).strip()
    company = str(row.get("Empresa", "")).strip().upper()
    if not name or not company:
        raise ValueError("La línea debe tener nombre y empresa.")

    def total(frame: pd.DataFrame, event: str, amount_column: str) -> Decimal:
        if frame.empty:
            return Decimal("0")
        required = {"Instrumento financiero", "Empresa", "Registro financiamiento", amount_column}
        if not required.issubset(frame.columns):
            raise ValueError("No fue posible comprobar los movimientos de la línea de crédito.")
        selected = frame.loc[
            frame["Instrumento financiero"].astype(str).str.strip().eq(name)
            & frame["Empresa"].astype(str).str.strip().str.upper().eq(company)
            & frame["Registro financiamiento"].astype(str).str.strip().eq(event)
        ]
        return sum((_number(value, "el capital registrado") for value in selected[amount_column]), Decimal("0"))

    drawn = total(ingresos, "Desembolso", "Monto real cobrado")
    repaid = total(gastos, "Pago capital", "Monto real pagado")
    return _money(max(Decimal("0"), drawn - repaid))


def credit_terms_from_row(row: dict, balance: float = 0.0) -> CreditTerms:
    limit = _number(row.get("Limite vigente"), "el límite vigente")
    available = max(Decimal("0"), limit - _number(balance, "el saldo usado"))
    monthly = _number(row.get("Cargo banca en linea mensual", 0), "la banca mensual")
    monthly += _number(row.get("Poliza vida mensual", 0), "la póliza mensual")
    annual = limit * _number(row.get("Cargo anual pct", 0), "el cargo anual") / 100
    annual += _number(row.get("Seguro incendio 1 anual", 0), "el seguro anual 1")
    annual += _number(row.get("Seguro incendio 2 anual", 0), "el seguro anual 2")
    return CreditTerms(
        daily_pct=float(_number(row.get("Tasa diaria pct"), "la tasa diaria")),
        available=_money(available),
        disbursement_fee=float(_number(row.get("Cargo desembolso fijo", 0), "el cargo por desembolso")),
        monthly_fees=_money(monthly),
        annual_cost=_money(annual),
    )


def cost_curve(calculators: dict) -> pd.DataFrame:
    rows = []
    for index in range(81):
        months = index / 10
        rows.append({"Meses": months, **{name: calculate(months).total for name, calculate in calculators.items()}})
    return pd.DataFrame(rows).set_index("Meses")


def cheapest(costs: list[FinancingCost]) -> list[str]:
    if not costs:
        return []
    minimum = min(cost.total for cost in costs)
    return [cost.option for cost in costs if cost.total == minimum]
