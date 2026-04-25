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


@dataclass(frozen=True)
class FinanceOpeningConfig:
    effective_date: date
    cash_by_company: dict[str, float]
    cxc_by_company: dict[str, float]
    preserve_existing_cxp: bool = True


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
    preserve_existing_cxp = str(app_cfg.get("FINANCE_OPENING_PRESERVE_CXP", "true")).strip().lower() not in {
        "0",
        "false",
        "no",
    }
    return FinanceOpeningConfig(
        effective_date=effective_date,
        cash_by_company=cash_by_company,
        cxc_by_company=cxc_by_company,
        preserve_existing_cxp=preserve_existing_cxp,
    )


def opening_amount_for_filter(amounts_by_company: dict[str, float], empresa_filter: str) -> float:
    empresa = _normalize_company_key(empresa_filter)
    if empresa in {"", "TODAS"}:
        return float(sum(float(v) for v in amounts_by_company.values()))
    return float(amounts_by_company.get(empresa, 0.0))

