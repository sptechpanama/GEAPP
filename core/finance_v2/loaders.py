from __future__ import annotations

import streamlit as st
import pandas as pd

from sheets import get_client, read_worksheet
from entities import WS_CLIENTES, WS_PROYECTOS


def get_finance_sheet_config() -> dict:
    app_cfg = st.secrets.get("app", {})
    sheet_id = app_cfg.get("SHEET_ID")
    ws_ing = app_cfg.get("WS_ING")
    ws_gas = app_cfg.get("WS_GAS")
    if not sheet_id or not ws_ing or not ws_gas:
        raise RuntimeError(
            "Falta configuracion en secrets: app.SHEET_ID, app.WS_ING o app.WS_GAS."
        )
    return {
        "sheet_id": str(sheet_id),
        "ws_ing": str(ws_ing),
        "ws_gas": str(ws_gas),
    }


@st.cache_data(ttl=120, show_spinner=False)
def load_worksheet_cached(sheet_id: str, worksheet: str, cache_token: str) -> pd.DataFrame:
    client, _ = get_client()
    try:
        df = read_worksheet(client, sheet_id, worksheet)
    except Exception:
        # En Finanzas 2 preferimos degradar a vacio para no romper el tablero completo.
        return pd.DataFrame()
    return df


@st.cache_data(ttl=120, show_spinner=False)
def load_finance_inputs(sheet_id: str, ws_ing: str, ws_gas: str, cache_token: str) -> dict[str, pd.DataFrame]:
    client, _ = get_client()
    out: dict[str, pd.DataFrame] = {}
    for key, ws in (("ingresos", ws_ing), ("gastos", ws_gas), ("clientes", WS_CLIENTES), ("proyectos", WS_PROYECTOS)):
        try:
            out[key] = read_worksheet(client, sheet_id, ws)
        except Exception:
            out[key] = pd.DataFrame()
    return out
