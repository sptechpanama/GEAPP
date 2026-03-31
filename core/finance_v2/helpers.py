from __future__ import annotations

from datetime import date
import re
import unicodedata

import pandas as pd

from .constants import COMISION_CATEGORY_ALIASES, MISC_CATEGORY_ALIASES


def parse_number_maybe_es(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return 0.0
        except Exception:
            pass
        return float(value)

    s = str(value).strip()
    if not s:
        return 0.0

    s = s.replace("$", "").replace("B/.", "").replace("B/", "").replace("USD", "")
    s = s.replace(" ", "")

    comma_pos = s.rfind(",")
    dot_pos = s.rfind(".")
    if comma_pos >= 0 and dot_pos >= 0:
        if comma_pos > dot_pos:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif comma_pos >= 0:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")

    s = re.sub(r"[^0-9.-]", "", s)
    if s in {"", "-", ".", "-."}:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def yes_no_flag(value) -> str:
    s = str(value or "").strip().lower()
    if s in {"si", "s\u00ed", "si\u0301", "yes", "y", "true", "1"}:
        return "Si"
    return "No"


def normalize_text(value) -> str:
    return str(value or "").strip()


def normalize_text_key(value) -> str:
    raw = normalize_text(value).lower()
    raw = unicodedata.normalize("NFKD", raw)
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def normalize_category(value) -> str:
    cat = normalize_text(value)
    if not cat:
        return "Sin categoria"
    key = normalize_text_key(cat)
    if key in MISC_CATEGORY_ALIASES:
        return "Miscelaneos"
    if key in COMISION_CATEGORY_ALIASES:
        return "Comisiones"
    return cat


def is_miscelaneos(value) -> bool:
    return normalize_text_key(value) in MISC_CATEGORY_ALIASES


def include_by_category(value, include_miscelaneos: bool) -> bool:
    if include_miscelaneos:
        return True
    return not is_miscelaneos(value)


def format_number_es(value, decimals: int = 2) -> str:
    try:
        n = float(value)
    except Exception:
        n = 0.0
    us = f"{n:,.{decimals}f}"
    return us.replace(",", "__tmp__").replace(".", ",").replace("__tmp__", ".")


def format_money_es(value) -> str:
    return f"${format_number_es(value, 2)}"


def format_percent_es(value) -> str:
    return f"{format_number_es(value, 1)}%"


def safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def ensure_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def ensure_timestamp(x) -> pd.Timestamp:
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return pd.Timestamp(date.today())
    return pd.Timestamp(ts)


def daterange_label(start: date, end: date) -> str:
    return f"{start.isoformat()} a {end.isoformat()}"
