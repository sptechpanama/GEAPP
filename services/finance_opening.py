from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
import streamlit as st


DEFAULT_FINANCE_OPENING_DATE = date(2026, 4, 25)
DEFAULT_FINANCE_OPENING_CASH = {
    "RIR": 0.0,
    "RS-SP": 4102.62,
}
DEFAULT_FINANCE_OPENING_CXC = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_INVENTORY = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_INVENTORY_TRANSIT = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_PREPAYMENTS = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_FIXED_ASSETS = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_LOANS_RECEIVED = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_LOANS_GRANTED = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_INVESTMENTS = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_CAPITAL = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_OTHER_DEBTS = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}
DEFAULT_FINANCE_OPENING_FACTORING_RETAINED = {
    "RIR": 0.0,
    "RS-SP": 0.0,
}


@dataclass(frozen=True)
class FinanceOpeningConfig:
    effective_date: date
    cash_by_company: dict[str, float]
    cxc_by_company: dict[str, float]
    inventory_by_company: dict[str, float]
    inventory_transit_by_company: dict[str, float]
    prepayments_by_company: dict[str, float]
    fixed_assets_by_company: dict[str, float]
    loans_received_by_company: dict[str, float]
    loans_granted_by_company: dict[str, float]
    investments_by_company: dict[str, float]
    capital_by_company: dict[str, float]
    other_debts_by_company: dict[str, float]
    factoring_retained_by_company: dict[str, float]
    preserve_existing_cxp: bool = False


def _normalize_company_key(raw_value: str) -> str:
    text = str(raw_value or "").strip().upper().replace("_", "-")
    return text


def _parse_date(raw_value, fallback: date) -> date:
    parsed = pd.to_datetime(raw_value, errors="coerce")
    if pd.isna(parsed):
        return fallback
    return parsed.date()


def _parse_float(raw_value, fallback: float) -> float:
    parsed = pd.to_numeric(pd.Series([raw_value]), errors="coerce").fillna(fallback).iloc[0]
    return float(parsed)


def get_finance_opening_config() -> FinanceOpeningConfig:
    app_cfg = st.secrets.get("app", {})
    effective_date = _parse_date(
        app_cfg.get("FINANCE_OPENING_EFFECTIVE_DATE", DEFAULT_FINANCE_OPENING_DATE.isoformat()),
        DEFAULT_FINANCE_OPENING_DATE,
    )
    cash_by_company: dict[str, float] = {}
    cxc_by_company: dict[str, float] = {}
    inventory_by_company: dict[str, float] = {}
    inventory_transit_by_company: dict[str, float] = {}
    prepayments_by_company: dict[str, float] = {}
    fixed_assets_by_company: dict[str, float] = {}
    loans_received_by_company: dict[str, float] = {}
    loans_granted_by_company: dict[str, float] = {}
    investments_by_company: dict[str, float] = {}
    capital_by_company: dict[str, float] = {}
    other_debts_by_company: dict[str, float] = {}
    factoring_retained_by_company: dict[str, float] = {}
    for company, fallback_cash in DEFAULT_FINANCE_OPENING_CASH.items():
        key_suffix = company.replace("-", "_")
        cash_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_CASH_{key_suffix}", fallback_cash),
            fallback_cash,
        )
        cxc_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_CXC_{key_suffix}", DEFAULT_FINANCE_OPENING_CXC.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_CXC.get(company, 0.0),
        )
        inventory_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_INVENTORY_{key_suffix}", DEFAULT_FINANCE_OPENING_INVENTORY.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_INVENTORY.get(company, 0.0),
        )
        inventory_transit_by_company[company] = _parse_float(
            app_cfg.get(
                f"FINANCE_OPENING_INVENTORY_TRANSIT_{key_suffix}",
                DEFAULT_FINANCE_OPENING_INVENTORY_TRANSIT.get(company, 0.0),
            ),
            DEFAULT_FINANCE_OPENING_INVENTORY_TRANSIT.get(company, 0.0),
        )
        prepayments_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_PREPAYMENTS_{key_suffix}", DEFAULT_FINANCE_OPENING_PREPAYMENTS.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_PREPAYMENTS.get(company, 0.0),
        )
        fixed_assets_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_FIXED_ASSETS_{key_suffix}", DEFAULT_FINANCE_OPENING_FIXED_ASSETS.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_FIXED_ASSETS.get(company, 0.0),
        )
        loans_received_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_LOANS_RECEIVED_{key_suffix}", DEFAULT_FINANCE_OPENING_LOANS_RECEIVED.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_LOANS_RECEIVED.get(company, 0.0),
        )
        loans_granted_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_LOANS_GRANTED_{key_suffix}", DEFAULT_FINANCE_OPENING_LOANS_GRANTED.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_LOANS_GRANTED.get(company, 0.0),
        )
        investments_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_INVESTMENTS_{key_suffix}", DEFAULT_FINANCE_OPENING_INVESTMENTS.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_INVESTMENTS.get(company, 0.0),
        )
        capital_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_CAPITAL_{key_suffix}", DEFAULT_FINANCE_OPENING_CAPITAL.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_CAPITAL.get(company, 0.0),
        )
        other_debts_by_company[company] = _parse_float(
            app_cfg.get(f"FINANCE_OPENING_OTHER_DEBTS_{key_suffix}", DEFAULT_FINANCE_OPENING_OTHER_DEBTS.get(company, 0.0)),
            DEFAULT_FINANCE_OPENING_OTHER_DEBTS.get(company, 0.0),
        )
        factoring_retained_by_company[company] = _parse_float(
            app_cfg.get(
                f"FINANCE_OPENING_FACTORING_RETAINED_{key_suffix}",
                DEFAULT_FINANCE_OPENING_FACTORING_RETAINED.get(company, 0.0),
            ),
            DEFAULT_FINANCE_OPENING_FACTORING_RETAINED.get(company, 0.0),
        )
    preserve_existing_cxp = str(app_cfg.get("FINANCE_OPENING_PRESERVE_CXP", "false")).strip().lower() not in {
        "0",
        "false",
        "no",
    }
    return FinanceOpeningConfig(
        effective_date=effective_date,
        cash_by_company=cash_by_company,
        cxc_by_company=cxc_by_company,
        inventory_by_company=inventory_by_company,
        inventory_transit_by_company=inventory_transit_by_company,
        prepayments_by_company=prepayments_by_company,
        fixed_assets_by_company=fixed_assets_by_company,
        loans_received_by_company=loans_received_by_company,
        loans_granted_by_company=loans_granted_by_company,
        investments_by_company=investments_by_company,
        capital_by_company=capital_by_company,
        other_debts_by_company=other_debts_by_company,
        factoring_retained_by_company=factoring_retained_by_company,
        preserve_existing_cxp=preserve_existing_cxp,
    )


def opening_amount_for_filter(amounts_by_company: dict[str, float], empresa_filter: str) -> float:
    empresa = _normalize_company_key(empresa_filter)
    if empresa in {"", "TODAS"}:
        return float(sum(float(v) for v in amounts_by_company.values()))
    return float(amounts_by_company.get(empresa, 0.0))
