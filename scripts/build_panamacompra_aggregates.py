"""Genera tablas agregadas (TOPs y resúmenes) a partir de panama_compra.db."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import sys
from typing import Dict, Iterable, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config import APP_ROOT, DB_PATH

# Ajusta estos nombres si tu base utiliza otros alias
ID_ACTO_COL = "id_acto"
MONTO_COL = "monto_ganador"
PROVEEDOR_COL = "proveedor_ganador"
ENTIDAD_COL = "entidad"
FICHA_COL = "num_ficha"
NOMBRE_FICHA_COL = "nombre_ficha"
PARTICIPANTES_COL = "cantidad_participantes"
TIENE_FICHA_COL = "tiene_ficha"
TIENE_CT_COL = "tiene_criterio_tecnico"
TIENE_RS_COL = "tiene_registro_sanitario"
FECHA_ADJ_COL = "fecha_adjudicacion"

DEFAULT_OUTPUT_DIR = APP_ROOT / "outputs" / "tops"


def _as_bool(series: pd.Series) -> pd.Series:
    """Convierte columnas a booleanos robustamente."""
    return (
        series.fillna(False)
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"1", "true", "t", "si", "sí", "y", "yes"})
    )


def load_base_dataframe(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"No se encontró la base {db_path}")
    query = f"""
        SELECT *
        FROM actos_publicos
        WHERE estado = 'Adjudicado'
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    df = pd.read_sql_query(query, conn)
    conn.close()
    if df.empty:
        return df

    required = [ID_ACTO_COL, MONTO_COL, PROVEEDOR_COL, ENTIDAD_COL]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(
            f"Las columnas requeridas {missing} no existen. Ajusta los alias al inicio del script."
        )

    if FECHA_ADJ_COL in df.columns:
        df[FECHA_ADJ_COL] = pd.to_datetime(
            df[FECHA_ADJ_COL], errors="coerce", dayfirst=True
        )
    for col in (TIENE_FICHA_COL, TIENE_CT_COL, TIENE_RS_COL):
        if col in df.columns:
            df[col] = _as_bool(df[col])
        else:
            df[col] = False
    for optional_col in (FICHA_COL, NOMBRE_FICHA_COL, PARTICIPANTES_COL):
        if optional_col not in df.columns:
            df[optional_col] = pd.NA
    df[MONTO_COL] = pd.to_numeric(df[MONTO_COL], errors="coerce").fillna(0.0)
    df[PARTICIPANTES_COL] = pd.to_numeric(df[PARTICIPANTES_COL], errors="coerce")
    return df


def build_resumen_global(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    total_monto = df[MONTO_COL].sum()
    base = {
        "total_actos_adjudicados": len(df),
        "total_monto_adjudicado": total_monto,
    }
    base["actos_con_ficha"] = int(df[TIENE_FICHA_COL].sum())
    base["actos_sin_ficha"] = base["total_actos_adjudicados"] - base["actos_con_ficha"]
    monto_con_ficha = df.loc[df[TIENE_FICHA_COL], MONTO_COL].sum()
    base["monto_con_ficha"] = monto_con_ficha
    base["monto_sin_ficha"] = total_monto - monto_con_ficha

    mask_ct = df[TIENE_FICHA_COL] & df[TIENE_CT_COL]
    base["actos_con_ct"] = int(mask_ct.sum())
    mask_ct_sin_rs = mask_ct & ~df[TIENE_RS_COL]
    base["actos_ct_sin_rs"] = int(mask_ct_sin_rs.sum())
    base["monto_ct_sin_rs"] = df.loc[mask_ct_sin_rs, MONTO_COL].sum()
    base["num_proveedores_distintos"] = df[PROVEEDOR_COL].nunique(dropna=True)
    base["num_entidades_distintas"] = df[ENTIDAD_COL].nunique(dropna=True)
    base["num_fichas_distintas"] = (
        df[FICHA_COL].dropna().nunique() if FICHA_COL in df.columns else 0
    )
    return pd.DataFrame([base])


def _supplier_group(df: pd.DataFrame) -> pd.DataFrame:
    grouping = df.groupby(PROVEEDOR_COL, dropna=False).agg(
        actos_ganados=(ID_ACTO_COL, "count"),
        monto_total=(MONTO_COL, "sum"),
        num_entidades_distintas=(ENTIDAD_COL, "nunique"),
        num_fichas_distintas=(FICHA_COL, "nunique"),
        num_participantes_promedio=(PARTICIPANTES_COL, "mean"),
    )
    return grouping.reset_index().rename(columns={PROVEEDOR_COL: "proveedor_ganador"})


def build_top_proveedores(
    df: pd.DataFrame,
    *,
    mask: Optional[pd.Series],
    order_field: str,
    ascending: bool,
) -> pd.DataFrame:
    if mask is not None:
        df = df[mask]
    if df.empty:
        return pd.DataFrame()
    grouped = _supplier_group(df)
    grouped = grouped.sort_values(order_field, ascending=ascending)
    grouped["num_participantes_promedio"] = grouped[
        "num_participantes_promedio"
    ].round(2)
    return grouped


def build_top_entidades(df: pd.DataFrame, *, mask: Optional[pd.Series]) -> pd.DataFrame:
    if mask is not None:
        df = df[mask]
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(ENTIDAD_COL, dropna=False)
        .agg(
            actos_adjudicados=(ID_ACTO_COL, "count"),
            monto_total=(MONTO_COL, "sum"),
            proveedores_distintos=(PROVEEDOR_COL, "nunique"),
            fichas_distintas=(FICHA_COL, "nunique"),
        )
        .reset_index()
        .rename(columns={ENTIDAD_COL: "entidad"})
    )
    return grouped.sort_values("monto_total", ascending=False)


def build_top_entidades_ct_sin_rs(df: pd.DataFrame) -> pd.DataFrame:
    mask = df[TIENE_FICHA_COL] & df[TIENE_CT_COL] & ~df[TIENE_RS_COL]
    if not mask.any():
        return pd.DataFrame()
    grouped = (
        df[mask]
        .groupby(ENTIDAD_COL, dropna=False)
        .agg(
            actos_ct_sin_rs=(ID_ACTO_COL, "count"),
            monto_ct_sin_rs=(MONTO_COL, "sum"),
            fichas_distintas=(FICHA_COL, "nunique"),
            proveedores_distintos=(PROVEEDOR_COL, "nunique"),
        )
        .reset_index()
        .rename(columns={ENTIDAD_COL: "entidad"})
    )
    return grouped.sort_values("monto_ct_sin_rs", ascending=False)


def build_top_fichas(df: pd.DataFrame, *, mask: Optional[pd.Series]) -> pd.DataFrame:
    if mask is not None:
        df = df[mask]
    df = df[df[FICHA_COL].notna()]
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby(FICHA_COL, dropna=False)
        .agg(
            nombre_ficha=(NOMBRE_FICHA_COL, "first"),
            actos_con_esa_ficha=(ID_ACTO_COL, "count"),
            monto_total=(MONTO_COL, "sum"),
            proveedores_distintos=(PROVEEDOR_COL, "nunique"),
            entidades_distintas=(ENTIDAD_COL, "nunique"),
        )
        .reset_index()
        .rename(columns={FICHA_COL: "num_ficha"})
    )
    grouped["nombre_ficha"] = grouped["nombre_ficha"].fillna("").astype(str)
    monto_sorted = grouped.sort_values("monto_total", ascending=False)
    actos_sorted = grouped.sort_values("actos_con_esa_ficha", ascending=False)
    return monto_sorted, actos_sorted


def build_top_fichas_ct_sin_rs(df: pd.DataFrame) -> pd.DataFrame:
    mask = df[TIENE_FICHA_COL] & df[TIENE_CT_COL] & ~df[TIENE_RS_COL] & df[FICHA_COL].notna()
    if not mask.any():
        return pd.DataFrame()
    grouped = (
        df[mask]
        .groupby(FICHA_COL, dropna=False)
        .agg(
            nombre_ficha=(NOMBRE_FICHA_COL, "first"),
            actos_ct_sin_rs=(ID_ACTO_COL, "count"),
            monto_ct_sin_rs=(MONTO_COL, "sum"),
            proveedores_distintos=(PROVEEDOR_COL, "nunique"),
            entidades_distintas=(ENTIDAD_COL, "nunique"),
        )
        .reset_index()
        .rename(columns={FICHA_COL: "num_ficha"})
    )
    return grouped.sort_values("monto_ct_sin_rs", ascending=False)


def build_resumen_competencia(df: pd.DataFrame) -> pd.DataFrame:
    segments = {
        "todos": df,
        "sin_ficha": df[~df[TIENE_FICHA_COL]],
        "ct_sin_rs": df[df[TIENE_FICHA_COL] & df[TIENE_CT_COL] & ~df[TIENE_RS_COL]],
    }
    rows = []
    for key, subset in segments.items():
        if subset.empty or PARTICIPANTES_COL not in subset.columns:
            rows.append(
                {
                    "segmento": key,
                    "prom_participantes_por_acto": 0.0,
                    "porcentaje_actos_con_1_participante": 0.0,
                    "porcentaje_actos_con_2_3_participantes": 0.0,
                    "porcentaje_actos_con_mas_de_3_participantes": 0.0,
                }
            )
            continue
        participantes = subset[PARTICIPANTES_COL].fillna(0)
        total = len(participantes.index)
        prom = participantes.mean()
        pct1 = (participantes == 1).mean() * 100
        pct23 = participantes.between(2, 3).mean() * 100
        pct4 = (participantes >= 4).mean() * 100
        rows.append(
            {
                "segmento": key,
                "prom_participantes_por_acto": round(prom, 2),
                "porcentaje_actos_con_1_participante": round(pct1, 2),
                "porcentaje_actos_con_2_3_participantes": round(pct23, 2),
                "porcentaje_actos_con_mas_de_3_participantes": round(pct4, 2),
            }
        )
    return pd.DataFrame(rows)


def save_table(df: pd.DataFrame, name: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"{name}.parquet"
    try:
        df.to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception:
        csv_path = output_dir / f"{name}.csv"
        df.to_csv(csv_path, index=False)
        return csv_path


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera TOPs y resúmenes de PanamaCompra.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Ruta al archivo panama_compra.db.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Carpeta donde se guardarán los DataFrames agregados.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    db_path = Path(args.db_path).expanduser()
    output_dir = Path(args.output_dir).expanduser()

    df = load_base_dataframe(db_path)
    if df.empty:
        print("[WARN] La consulta no devolvió adjudicaciones; no se generaron tablas.")
        return 0

    generated: Dict[str, Path] = {}

    resumen_global = build_resumen_global(df)
    generated["resumen_global"] = save_table(resumen_global, "resumen_global", output_dir)

    top_gen = build_top_proveedores(df, mask=None, order_field="monto_total", ascending=False)
    generated["top_proveedores_general"] = save_table(top_gen, "top_proveedores_general", output_dir)

    sin_ficha = ~df[TIENE_FICHA_COL]
    top_sf_actos = build_top_proveedores(df, mask=sin_ficha, order_field="actos_ganados", ascending=False)
    generated["top_proveedores_sin_ficha_por_actos"] = save_table(
        top_sf_actos, "top_proveedores_sin_ficha_por_actos", output_dir
    )
    top_sf_monto = build_top_proveedores(df, mask=sin_ficha, order_field="monto_total", ascending=False)
    generated["top_proveedores_sin_ficha_por_monto"] = save_table(
        top_sf_monto, "top_proveedores_sin_ficha_por_monto", output_dir
    )

    con_ficha = df[TIENE_FICHA_COL]
    top_cf_actos = build_top_proveedores(df, mask=con_ficha, order_field="actos_ganados", ascending=False)
    generated["top_proveedores_con_ficha_por_actos"] = save_table(
        top_cf_actos, "top_proveedores_con_ficha_por_actos", output_dir
    )
    top_cf_monto = build_top_proveedores(df, mask=con_ficha, order_field="monto_total", ascending=False)
    generated["top_proveedores_con_ficha_por_monto"] = save_table(
        top_cf_monto, "top_proveedores_con_ficha_por_monto", output_dir
    )

    mask_ct_sin_rs = df[TIENE_FICHA_COL] & df[TIENE_CT_COL] & ~df[TIENE_RS_COL]
    top_ct_actos = build_top_proveedores(
        df, mask=mask_ct_sin_rs, order_field="actos_ganados", ascending=False
    )
    generated["top_proveedores_ct_sin_rs_por_actos"] = save_table(
        top_ct_actos, "top_proveedores_ct_sin_rs_por_actos", output_dir
    )
    top_ct_monto = build_top_proveedores(
        df, mask=mask_ct_sin_rs, order_field="monto_total", ascending=False
    )
    generated["top_proveedores_ct_sin_rs_por_monto"] = save_table(
        top_ct_monto, "top_proveedores_ct_sin_rs_por_monto", output_dir
    )

    top_entidades = build_top_entidades(df, mask=None)
    generated["top_entidades_por_monto_total"] = save_table(
        top_entidades, "top_entidades_por_monto_total", output_dir
    )
    top_entidades_ct = build_top_entidades_ct_sin_rs(df)
    generated["top_entidades_ct_sin_rs"] = save_table(
        top_entidades_ct, "top_entidades_ct_sin_rs", output_dir
    )

    fichas_monto, fichas_actos = build_top_fichas(df, mask=df[TIENE_FICHA_COL])
    generated["top_fichas_por_monto"] = save_table(fichas_monto, "top_fichas_por_monto", output_dir)
    generated["top_fichas_por_actos"] = save_table(fichas_actos, "top_fichas_por_actos", output_dir)

    fichas_ct_sin_rs = build_top_fichas_ct_sin_rs(df)
    generated["top_fichas_ct_sin_rs"] = save_table(
        fichas_ct_sin_rs, "top_fichas_ct_sin_rs", output_dir
    )

    resumen_comp = build_resumen_competencia(df)
    generated["resumen_competencia_global"] = save_table(
        resumen_comp, "resumen_competencia_global", output_dir
    )

    for name, path in generated.items():
        if isinstance(path, Path):
            print(f"[OK] {name}: {path} ({path.stat().st_size} bytes)")
        else:
            print(f"[OK] {name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
