"""Carga tablas de referencia (Excel) a Supabase/PostgreSQL.

Uso recomendado:
python scripts/upload_reference_tables_supabase.py --project-id <id> --password "<password>"

Opcional:
- --base-dir: carpeta donde estan los Excel (por defecto: raiz del repo)
- --if-exists: replace (default) o append
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text


DATASETS = [
    ("fichas_ctni.xlsx", "fichas_tecnicas"),
    ("criterios_tecnicos.xlsx", "criterios_tecnicos"),
    ("oferentes_catalogos.xlsx", "oferentes_catalogos"),
]


def _build_pooler_url(project_id: str, password: str) -> str:
    safe_password = quote_plus(password)
    return (
        "postgresql+psycopg2://"
        f"postgres.{project_id}:{safe_password}"
        "@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )


def _load_excel(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")
    df = pd.read_excel(file_path, dtype=str)
    # Limpieza minima para evitar columnas vacias/duplicadas y preservar texto.
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _upload_dataframe(engine, df: pd.DataFrame, table_name: str, if_exists: str) -> int:
    if df.empty:
        print(f"[WARN] {table_name}: sin filas, no se sube.")
        return 0
    df.to_sql(
        table_name,
        con=engine,
        schema="public",
        if_exists=if_exists,
        index=False,
        chunksize=1000,
        method="multi",
    )
    with engine.connect() as conn:
        total = conn.execute(text(f'SELECT COUNT(1) FROM public."{table_name}"')).scalar()
    return int(total or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sube tablas de referencia a Supabase.")
    parser.add_argument(
        "--project-id",
        required=True,
        help="Project ID de Supabase (ej: tlrwnoflnvutmhtljnma)",
    )
    parser.add_argument("--password", required=True, help="Password del usuario DB en Supabase.")
    parser.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parents[1]),
        help="Carpeta base que contiene los xlsx.",
    )
    parser.add_argument(
        "--if-exists",
        default="replace",
        choices=["replace", "append"],
        help="replace sobrescribe tabla; append agrega filas.",
    )
    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()
    db_url = _build_pooler_url(args.project_id, args.password)
    print(f"[INFO] Base dir: {base_dir}")
    print("[INFO] Conectando a Supabase...")

    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[OK] Conexion establecida.")

    for file_name, table_name in DATASETS:
        source = base_dir / file_name
        print(f"\n[INFO] Cargando {source.name} -> public.{table_name}")
        df = _load_excel(source)
        print(f"[INFO] Filas origen: {len(df):,} | Columnas: {len(df.columns)}")
        total = _upload_dataframe(engine, df, table_name, args.if_exists)
        print(f"[OK] Tabla public.{table_name} lista con {total:,} filas.")

    print("\n[OK] Carga finalizada.")
    print(
        "Configura en Streamlit secrets (seccion [app]): "
        "SUPABASE_FICHAS_TABLE='fichas_tecnicas' y "
        "SUPABASE_CATALOGOS_TABLE='oferentes_catalogos'."
    )


if __name__ == "__main__":
    main()
