from __future__ import annotations

"""Construye y publica la capa analitica no medica de Inteligencia PC."""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pandas as pd
from sqlalchemy import create_engine, text


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from services.inteligencia_pc import (  # noqa: E402
    CORE_COLUMNS,
    PCFilters,
    clean_text,
    normalize_provider,
    prepare_pc_acts,
    unpivot_proposals,
)


DEFAULT_SOURCE = Path.home() / "scrapers_repo" / "data" / "db" / "panamacompra.db"
DEFAULT_OUTPUT = Path.home() / "scrapers_repo" / "data" / "db" / "inteligencia_pc.db"
CHUNK_SIZE = 5_000
PC_ACT_COLUMNS = [
    "acto_key", "source_id", "fecha_analitica", "publicacion", "fecha", "fecha_adjudicacion",
    "fecha_actualizacion", "enlace", "titulo", "descripcion", "entidad", "unidad_solic",
    "estado", "precio_referencia", "monto_referencia", "razon_social", "nombre_comercial",
    "num_participantes", "total_items_ofertados", "familia", "confianza_familia",
    "evidencia_familia", "mercado_pc", "evidencia_mercado", "source_tipo_proceso",
]
PC_PROPOSAL_COLUMNS = [
    "acto_key", "ordinal", "proveedor", "proveedor_norm", "monto_ofertado", "ganador",
    "ganador_norm", "ganado", "monto_ganado",
]


def _qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _source_columns(connection: sqlite3.Connection) -> list[str]:
    existing = {str(row[1]) for row in connection.execute("PRAGMA table_info(actos_publicos)")}
    requested = list(CORE_COLUMNS)
    for ordinal in range(1, 15):
        requested.extend((f"Proponente {ordinal}", f"Precio Proponente {ordinal}"))
    return [column for column in requested if column in existing]


def _prepare_chunk(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    acts = prepare_pc_acts(raw, PCFilters(include_ambiguous=False))
    if acts.empty:
        return pd.DataFrame(columns=PC_ACT_COLUMNS), pd.DataFrame(columns=PC_PROPOSAL_COLUMNS)
    acts = acts.drop_duplicates("acto_key", keep="last").copy()
    acts["source_id"] = acts.get("id")
    acts["fecha_analitica"] = pd.to_datetime(acts["fecha_analitica"], errors="coerce").dt.strftime("%Y-%m-%d")
    for column in PC_ACT_COLUMNS:
        if column not in acts.columns:
            acts[column] = ""
    act_output = acts[PC_ACT_COLUMNS].copy()

    proposals = unpivot_proposals(acts)
    if proposals.empty:
        return act_output, pd.DataFrame(columns=PC_PROPOSAL_COLUMNS)
    winners = acts[["acto_key", "razon_social", "nombre_comercial"]].copy()
    winners["ganador"] = winners["razon_social"].where(
        winners["razon_social"].fillna("").astype(str).str.strip() != "",
        winners["nombre_comercial"],
    )
    proposals = proposals.merge(winners[["acto_key", "ganador"]], on="acto_key", how="left")
    proposals["ganador_norm"] = proposals["ganador"].map(normalize_provider)
    proposals["ganado"] = (
        proposals["proveedor_norm"].fillna("").ne("")
        & proposals["proveedor_norm"].eq(proposals["ganador_norm"])
    )
    proposals["monto_ganado"] = proposals["monto_ofertado"].where(proposals["ganado"], 0.0)
    for column in PC_PROPOSAL_COLUMNS:
        if column not in proposals.columns:
            proposals[column] = ""
    return act_output, proposals[PC_PROPOSAL_COLUMNS].copy()


def _initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE pc_actos (
            acto_key TEXT PRIMARY KEY,
            source_id INTEGER,
            fecha_analitica TEXT,
            publicacion TEXT,
            fecha TEXT,
            fecha_adjudicacion TEXT,
            fecha_actualizacion TEXT,
            enlace TEXT,
            titulo TEXT,
            descripcion TEXT,
            entidad TEXT,
            unidad_solic TEXT,
            estado TEXT,
            precio_referencia TEXT,
            monto_referencia REAL,
            razon_social TEXT,
            nombre_comercial TEXT,
            num_participantes TEXT,
            total_items_ofertados TEXT,
            familia TEXT,
            confianza_familia REAL,
            evidencia_familia TEXT,
            mercado_pc TEXT,
            evidencia_mercado TEXT,
            source_tipo_proceso TEXT
        );
        CREATE TABLE pc_propuestas (
            acto_key TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            proveedor TEXT,
            proveedor_norm TEXT,
            monto_ofertado REAL,
            ganador TEXT,
            ganador_norm TEXT,
            ganado INTEGER,
            monto_ganado REAL,
            PRIMARY KEY (acto_key, ordinal)
        );
        CREATE TABLE pc_build_metadata (key TEXT PRIMARY KEY, value TEXT);
        """
    )


def _finish_database(connection: sqlite3.Connection, metadata: dict[str, str]) -> None:
    connection.executescript(
        """
        CREATE INDEX idx_pc_actos_fecha ON pc_actos(fecha_analitica);
        CREATE INDEX idx_pc_actos_familia ON pc_actos(familia);
        CREATE INDEX idx_pc_actos_entidad ON pc_actos(entidad);
        CREATE INDEX idx_pc_actos_estado ON pc_actos(estado);
        CREATE INDEX idx_pc_propuestas_empresa ON pc_propuestas(proveedor_norm);
        CREATE INDEX idx_pc_propuestas_acto ON pc_propuestas(acto_key);
        CREATE INDEX idx_pc_propuestas_ganado ON pc_propuestas(ganado);
        CREATE TABLE pc_proveedores_catalogo AS
        SELECT proveedor_norm,
               MIN(proveedor) AS proveedor,
               COUNT(DISTINCT acto_key) AS participaciones,
               SUM(CASE WHEN ganado=1 THEN 1 ELSE 0 END) AS adjudicaciones,
               SUM(monto_ofertado) AS monto_ofertado,
               SUM(monto_ganado) AS monto_ganado
        FROM pc_propuestas
        WHERE trim(COALESCE(proveedor_norm,'')) <> ''
        GROUP BY proveedor_norm;
        CREATE UNIQUE INDEX ux_pc_provider_catalog_norm ON pc_proveedores_catalogo(proveedor_norm);
        CREATE TABLE pc_proveedores_dia AS
        SELECT a.fecha_analitica,
               p.proveedor_norm,
               MIN(p.proveedor) AS proveedor,
               COUNT(DISTINCT p.acto_key) AS participaciones,
               COUNT(DISTINCT CASE WHEN p.ganado=1 THEN p.acto_key END) AS adjudicaciones,
               SUM(p.monto_ofertado) AS monto_ofertado,
               SUM(p.monto_ganado) AS monto_ganado,
               MIN(p.monto_ofertado) AS oferta_minima,
               MAX(p.monto_ofertado) AS oferta_maxima,
               COUNT(*) AS ofertas_validas
        FROM pc_propuestas p
        JOIN pc_actos a ON a.acto_key=p.acto_key
        WHERE trim(COALESCE(p.proveedor_norm,'')) <> ''
        GROUP BY a.fecha_analitica,p.proveedor_norm;
        CREATE INDEX ix_pc_provider_day_date ON pc_proveedores_dia(fecha_analitica);
        CREATE INDEX ix_pc_provider_day_provider ON pc_proveedores_dia(proveedor_norm);
        CREATE TABLE pc_proveedores_contexto_dia AS
        SELECT a.fecha_analitica,
               p.proveedor_norm,
               a.familia,
               a.entidad,
               COUNT(DISTINCT p.acto_key) AS participaciones,
               COUNT(DISTINCT CASE WHEN p.ganado=1 THEN p.acto_key END) AS adjudicaciones,
               SUM(p.monto_ofertado) AS monto_ofertado,
               SUM(p.monto_ganado) AS monto_ganado,
               MIN(p.monto_ofertado) AS oferta_minima,
               MAX(p.monto_ofertado) AS oferta_maxima,
               COUNT(*) AS ofertas_validas
        FROM pc_propuestas p
        JOIN pc_actos a ON a.acto_key=p.acto_key
        WHERE trim(COALESCE(p.proveedor_norm,'')) <> ''
        GROUP BY a.fecha_analitica,p.proveedor_norm,a.familia,a.entidad;
        CREATE INDEX ix_pc_provider_context_date ON pc_proveedores_contexto_dia(fecha_analitica);
        CREATE INDEX ix_pc_provider_context_provider ON pc_proveedores_contexto_dia(proveedor_norm);
        CREATE INDEX ix_pc_provider_context_family ON pc_proveedores_contexto_dia(familia);
        CREATE INDEX ix_pc_provider_context_entity ON pc_proveedores_contexto_dia(entidad);
        CREATE TABLE pc_familias_dia_entidad AS
        SELECT fecha_analitica,
               familia,
               entidad,
               COUNT(*) AS actos,
               SUM(monto_referencia) AS monto_total,
               SUM(COALESCE(CAST(NULLIF(num_participantes,'') AS REAL),0)) AS participantes_suma,
               SUM(CASE WHEN trim(COALESCE(num_participantes,''))<>'' THEN 1 ELSE 0 END) AS participantes_con_dato
        FROM pc_actos
        GROUP BY fecha_analitica,familia,entidad;
        CREATE INDEX ix_pc_family_day_date ON pc_familias_dia_entidad(fecha_analitica);
        CREATE INDEX ix_pc_family_day_family ON pc_familias_dia_entidad(familia);
        CREATE INDEX ix_pc_family_day_entity ON pc_familias_dia_entidad(entidad);
        """
    )
    connection.executemany(
        "INSERT OR REPLACE INTO pc_build_metadata(key,value) VALUES (?,?)",
        list(metadata.items()),
    )
    connection.commit()


def build(source: Path, output: Path, *, chunk_size: int = CHUNK_SIZE) -> dict[str, int | str]:
    if not source.exists() or source.stat().st_size <= 0:
        raise FileNotFoundError(f"Base operacional no disponible: {source}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(".tmp.db")
    if temp.exists():
        temp.unlink()
    source_connection = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True, timeout=60)
    target = sqlite3.connect(temp, timeout=60)
    _initialize_database(target)
    columns = _source_columns(source_connection)
    query = "SELECT " + ",".join(_qident(column) for column in columns) + " FROM actos_publicos"
    source_rows = int(source_connection.execute("SELECT COUNT(*) FROM actos_publicos").fetchone()[0])
    act_rows = 0
    proposal_rows = 0
    try:
        for index, raw in enumerate(pd.read_sql_query(query, source_connection, chunksize=chunk_size), start=1):
            acts, proposals = _prepare_chunk(raw)
            if not acts.empty:
                acts.to_sql("pc_actos", target, if_exists="append", index=False)
                act_rows += len(acts)
            if not proposals.empty:
                proposals.to_sql("pc_propuestas", target, if_exists="append", index=False)
                proposal_rows += len(proposals)
            if index % 5 == 0:
                print(f"[PC] chunks={index} actos={act_rows:,} propuestas={proposal_rows:,}", flush=True)
        metadata = {
            "built_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_db": str(source),
            "source_rows": str(source_rows),
            "act_rows": str(act_rows),
            "proposal_rows": str(proposal_rows),
            "classifier_version": "pc-market-1.0.0",
        }
        _finish_database(target, metadata)
    finally:
        source_connection.close()
        target.close()
    temp.replace(output)
    return {"source_rows": source_rows, "act_rows": act_rows, "proposal_rows": proposal_rows, "output": str(output)}


def publish_postgres(database: Path, database_url: str, *, chunk_size: int = 10_000) -> dict[str, int]:
    if not database_url:
        raise RuntimeError("Falta SUPABASE_DB_URL/DATABASE_URL para publicar Inteligencia PC.")
    engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=240, connect_args={"connect_timeout": 20})
    tables = (
        "pc_actos", "pc_propuestas", "pc_proveedores_catalogo", "pc_proveedores_dia",
        "pc_proveedores_contexto_dia", "pc_familias_dia_entidad", "pc_build_metadata",
    )
    counts: dict[str, int] = {}
    source = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    try:
        for table in tables:
            target = f"{table}__new"
            with engine.begin() as connection:
                connection.execute(text(f'DROP TABLE IF EXISTS "{target}"'))
            first = True
            for frame in pd.read_sql_query(f'SELECT * FROM "{table}"', source, chunksize=chunk_size):
                frame.to_sql(target, engine, if_exists="replace" if first else "append", index=False, chunksize=2000, method="multi")
                first = False
            if first:
                pd.read_sql_query(f'SELECT * FROM "{table}" LIMIT 0', source).to_sql(target, engine, if_exists="replace", index=False)
            counts[table] = int(source.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        with engine.begin() as connection:
            for table in tables:
                connection.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
                connection.execute(text(f'ALTER TABLE "{table}__new" RENAME TO "{table}"'))
            connection.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ux_pc_actos_key ON pc_actos(acto_key)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_fecha ON pc_actos(fecha_analitica)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_monto ON pc_actos(monto_referencia DESC)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_familia ON pc_actos(familia)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_entidad ON pc_actos(entidad)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_estado ON pc_actos(estado)'))
            connection.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ux_pc_propuestas_key ON pc_propuestas(acto_key,ordinal)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_propuestas_empresa ON pc_propuestas(proveedor_norm)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_propuestas_acto ON pc_propuestas(acto_key)'))
            connection.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ux_pc_provider_catalog_norm ON pc_proveedores_catalogo(proveedor_norm)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_provider_day_date ON pc_proveedores_dia(fecha_analitica)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_provider_day_provider ON pc_proveedores_dia(proveedor_norm)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_provider_context_date ON pc_proveedores_contexto_dia(fecha_analitica)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_provider_context_provider ON pc_proveedores_contexto_dia(proveedor_norm)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_provider_context_family ON pc_proveedores_contexto_dia(familia)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_provider_context_entity ON pc_proveedores_contexto_dia(entidad)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_family_day_date ON pc_familias_dia_entidad(fecha_analitica)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_family_day_family ON pc_familias_dia_entidad(familia)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_family_day_entity ON pc_familias_dia_entidad(entidad)'))
    finally:
        source.close()
        engine.dispose()
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Construye Inteligencia PC no medica")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--publish-postgres", action="store_true")
    parser.add_argument("--postgres-url", default="")
    parser.add_argument("--require-postgres", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build(args.source.resolve(), args.output.resolve(), chunk_size=max(500, args.chunk_size))
    print("PC_BUILD_JSON=" + json.dumps(result, ensure_ascii=False, default=str), flush=True)
    if args.publish_postgres or args.require_postgres:
        url = clean_text(args.postgres_url or os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"))
        if not url:
            if args.require_postgres:
                raise RuntimeError("Falta SUPABASE_DB_URL/DATABASE_URL.")
            return 0
        uploaded = publish_postgres(args.output.resolve(), url)
        print("PC_POSTGRES_JSON=" + json.dumps(uploaded, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
