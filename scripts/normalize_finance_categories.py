from __future__ import annotations

import argparse
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

# Permite ejecutar el script desde cualquier cwd.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MISC_CATEGORY_ALIASES = {
    "miscelaneos",
    "miscelaneo",
    "miscelaneos y otros",
    "miscelaneos/otros",
    "miscelaneos-otros",
    "miscelaneos.",
    "misc",
    "miscellaneous",
    "miscelaneos varios",
}

COMISION_CATEGORY_ALIASES = {
    "comisiones",
    "comision",
    "comisiones venta",
    "comisiones ventas",
}


def _normalize_text_key(value) -> str:
    raw = str(value or "").strip().lower()
    raw = unicodedata.normalize("NFKD", raw)
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def normalize_category(value) -> str:
    cat = str(value or "").strip()
    if not cat:
        return "Sin categoria"
    key = _normalize_text_key(cat)
    if key in MISC_CATEGORY_ALIASES:
        return "Miscelaneos"
    if key in COMISION_CATEGORY_ALIASES:
        return "Comisiones"
    return cat


def _resolve_sheet_config(args: argparse.Namespace) -> tuple[str, str, str]:
    if args.sheet_id and args.ws_ing and args.ws_gas:
        return args.sheet_id.strip(), args.ws_ing.strip(), args.ws_gas.strip()

    # 1) Intenta desde streamlit secrets (si existe contexto local/secrets.toml)
    try:
        import streamlit as st  # noqa: WPS433

        app_cfg = st.secrets.get("app", {})
        sheet_id = str(args.sheet_id or app_cfg.get("SHEET_ID") or "").strip()
        ws_ing = str(args.ws_ing or app_cfg.get("WS_ING") or "").strip()
        ws_gas = str(args.ws_gas or app_cfg.get("WS_GAS") or "").strip()
        if sheet_id and ws_ing and ws_gas:
            return sheet_id, ws_ing, ws_gas
    except Exception:
        pass

    # 2) Fallback env vars
    sheet_id = str(args.sheet_id or os.environ.get("FINAPP_SHEET_ID") or "").strip()
    ws_ing = str(args.ws_ing or os.environ.get("FINAPP_WS_ING") or "").strip()
    ws_gas = str(args.ws_gas or os.environ.get("FINAPP_WS_GAS") or "").strip()
    if sheet_id and ws_ing and ws_gas:
        return sheet_id, ws_ing, ws_gas

    raise RuntimeError(
        "No se pudo resolver configuracion. Define --sheet-id --ws-ing --ws-gas, "
        "o app.SHEET_ID/app.WS_ING/app.WS_GAS en secrets, o env FINAPP_*"
    )


def _normalize_categories(df: pd.DataFrame, worksheet_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "Categoria" not in df.columns:
        return df.copy(), pd.DataFrame(columns=["worksheet", "row_idx", "row_id", "categoria_old", "categoria_new"])

    out = df.copy()
    before = out["Categoria"].fillna("").astype(str)
    after = before.map(normalize_category)
    changed_mask = before != after
    out["Categoria"] = after

    rowid_col = "RowID" if "RowID" in out.columns else None
    report = pd.DataFrame(
        {
            "worksheet": worksheet_name,
            "row_idx": out.index + 2,  # +2 por encabezado y 1-based
            "row_id": out[rowid_col] if rowid_col else "",
            "categoria_old": before,
            "categoria_new": after,
        }
    )
    report = report[changed_mask].copy().reset_index(drop=True)
    return out, report


def _print_summary(title: str, report_df: pd.DataFrame) -> None:
    print(f"\n=== {title} ===")
    print(f"Cambios: {len(report_df)}")
    if report_df.empty:
        return
    sample = report_df.head(12)
    with pd.option_context("display.max_colwidth", 60):
        print(sample[["row_idx", "row_id", "categoria_old", "categoria_new"]].to_string(index=False))


def _save_report_csv(report_df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"finance_category_normalization_report_{ts}.csv"
    report_df.to_csv(path, index=False, encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normaliza categorias de Finanzas (ingresos/gastos) en Google Sheets.",
    )
    parser.add_argument("--sheet-id", default="", help="Spreadsheet ID")
    parser.add_argument("--ws-ing", default="", help="Worksheet de ingresos")
    parser.add_argument("--ws-gas", default="", help="Worksheet de gastos")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Aplica cambios en Google Sheets. Sin este flag solo hace dry-run.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "data" / "reports"),
        help="Directorio para guardar reporte CSV.",
    )
    args = parser.parse_args()

    # Import diferido: permite mostrar --help aunque falte gspread en el entorno.
    from sheets import get_client, read_worksheet, write_worksheet  # noqa: WPS433

    sheet_id, ws_ing, ws_gas = _resolve_sheet_config(args)
    client, _ = get_client()

    ing_df = read_worksheet(client, sheet_id, ws_ing)
    gas_df = read_worksheet(client, sheet_id, ws_gas)

    ing_norm, ing_report = _normalize_categories(ing_df, ws_ing)
    gas_norm, gas_report = _normalize_categories(gas_df, ws_gas)

    full_report = pd.concat([ing_report, gas_report], ignore_index=True)

    print(f"\nSheet ID: {sheet_id}")
    print(f"Ingresos sheet: {ws_ing} | filas: {len(ing_df)}")
    print(f"Gastos sheet:   {ws_gas} | filas: {len(gas_df)}")

    _print_summary("INGRESOS", ing_report)
    _print_summary("GASTOS", gas_report)

    report_path = _save_report_csv(full_report, Path(args.report_dir))
    print(f"\nReporte CSV: {report_path}")

    if not args.apply:
        print("\nModo DRY-RUN: no se escribio nada en Sheets. Usa --apply para guardar cambios.")
        return

    if ing_report.empty and gas_report.empty:
        print("\nNo hay cambios para aplicar.")
        return

    if not ing_report.empty:
        write_worksheet(client, sheet_id, ws_ing, ing_norm)
        print(f"Aplicado: {ws_ing} ({len(ing_report)} cambios)")
    if not gas_report.empty:
        write_worksheet(client, sheet_id, ws_gas, gas_norm)
        print(f"Aplicado: {ws_gas} ({len(gas_report)} cambios)")

    print("\nListo. Normalizacion aplicada.")


if __name__ == "__main__":
    main()
