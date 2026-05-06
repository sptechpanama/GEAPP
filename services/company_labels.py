from __future__ import annotations

import pandas as pd

COMPANY_DISPLAY_LABELS = {
    "RS-SP": "RS/SP",
}


def display_company_label(value: object) -> str:
    raw = str(value or "").strip()
    return COMPANY_DISPLAY_LABELS.get(raw, raw)


def apply_company_labels_df(
    df: pd.DataFrame,
    columns: tuple[str, ...] = ("Empresa", "empresa"),
) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].map(display_company_label)
    return out
