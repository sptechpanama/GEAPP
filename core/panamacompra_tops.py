"""Constantes y utilidades para los tops precomputados de PanamaCompra."""

from __future__ import annotations

from core.config import APP_ROOT


DATA_TOPS_DIR = APP_ROOT / "data" / "tops"
ALT_TOPS_DIR = APP_ROOT / "outputs" / "tops"
TOPS_EXCEL_PATH = DATA_TOPS_DIR / "tops_panamacompra.xlsx"
TOPS_EXCEL_FALLBACK = ALT_TOPS_DIR / "tops_panamacompra.xlsx"
TOPS_METADATA_SHEET = "metadata"


SUPPLIER_TOP_CONFIG = [
    {
        "key": "sin_ct_count",
        "tab_label": "Más actos ganados · Sin ficha",
        "title": "Proveedores con más actos públicos ganados sin ficha técnica",
        "require_ct": False,
        "require_registro": None,
        "metric": "count",
        "mode": "supplier",
    },
    {
        "key": "sin_ct_amount",
        "tab_label": "Más dinero adjudicado · Sin ficha",
        "title": "Proveedores con más dinero adjudicado sin ficha técnica",
        "require_ct": False,
        "require_registro": None,
        "metric": "amount",
        "mode": "supplier",
    },
    {
        "key": "con_ct_count",
        "tab_label": "Más actos ganados · Con ficha",
        "title": "Proveedores con más actos públicos ganados con ficha técnica",
        "require_ct": True,
        "require_registro": None,
        "metric": "count",
        "mode": "supplier",
    },
    {
        "key": "con_ct_amount",
        "tab_label": "Más dinero adjudicado · Con ficha",
        "title": "Proveedores con más dinero adjudicado con ficha técnica",
        "require_ct": True,
        "require_registro": None,
        "metric": "amount",
        "mode": "supplier",
    },
    {
        "key": "con_ct_sin_reg_count",
        "tab_label": "Más actos ganados · Con ficha, sin registro",
        "title": "Fichas con más actos adjudicados (proveedores sin registro sanitario)",
        "require_ct": True,
        "require_registro": False,
        "metric": "count",
        "mode": "ct",
    },
    {
        "key": "con_ct_sin_reg_amount",
        "tab_label": "Más dinero adjudicado · Con ficha, sin registro",
        "title": "Fichas con más monto adjudicado (proveedores sin registro sanitario)",
        "require_ct": True,
        "require_registro": False,
        "metric": "amount",
        "mode": "ct",
    },
]

SUPPLIER_TOP_DEFAULT_ROWS = 10


def sheet_name_for_top(key: str) -> str:
    """Normaliza el nombre de hoja para cada top (máx. 31 caracteres)."""
    normalized = key.strip() or "top"
    return normalized[:31]


__all__ = [
    "SUPPLIER_TOP_CONFIG",
    "SUPPLIER_TOP_DEFAULT_ROWS",
    "DATA_TOPS_DIR",
    "ALT_TOPS_DIR",
    "TOPS_EXCEL_PATH",
    "TOPS_EXCEL_FALLBACK",
    "TOPS_METADATA_SHEET",
    "sheet_name_for_top",
]
