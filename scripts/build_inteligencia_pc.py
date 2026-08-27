from __future__ import annotations

"""Construye y publica la capa analitica no medica de Inteligencia PC."""

import argparse
import json
import os
import re
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
    parse_money,
    prepare_pc_acts,
    provider_matches,
    unpivot_proposals,
    winner_entries,
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
    "numero_proceso", "source_layer", "resultado_provisional",
]
PC_PROPOSAL_COLUMNS = [
    "acto_key", "ordinal", "proveedor", "proveedor_norm", "monto_ofertado", "ganador",
    "ganador_norm", "ganado", "monto_ganado", "resultado_empresa", "fuente_resultado",
    "resultado_provisional", "monto_ganado_fuente",
]

PROCESS_NUMBER_RE = re.compile(
    r"\b20\d{2}(?:-\d+){4}-(?:CL|CM|LP)-\d+\b",
    flags=re.IGNORECASE,
)
FINAL_CL_STATES = {"cerrada_con_propuestas", "cerrada_sin_propuestas"}


def _process_number(*values: object) -> str:
    for value in values:
        match = PROCESS_NUMBER_RE.search(clean_text(value))
        if match:
            return match.group(0).upper()
    return ""


def _deserted(value: object) -> bool:
    return "desiert" in clean_text(value).lower()


def _winner_amount(record: dict[str, object]) -> tuple[float, str]:
    total = parse_money(record.get("total_items_ofertados"))
    reference = parse_money(record.get("precio_referencia"))
    # En fuentes historicas ``total_items_ofertados`` puede ser un conteo. No
    # se acepta como dinero cuando es insignificante frente a la referencia.
    if total > 0 and (reference <= 0 or total >= reference * 0.05):
        return total, "total_items_ofertados"
    if reference > 0:
        return reference, "precio_referencia_estimado"
    return 0.0, "sin_monto"


def _official_proposals(acts: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    proposals = unpivot_proposals(acts)
    proposals_by_act = {
        str(key): group.to_dict("records")
        for key, group in proposals.groupby("acto_key")
    } if not proposals.empty else {}

    for record in acts.to_dict("records"):
        act_key = clean_text(record.get("acto_key"))
        winners = winner_entries(record)
        winner_candidates = [clean_text(value.get("proveedor")) for value in winners]
        winner = ", ".join(value for value in winner_candidates if value)
        act_proposals = proposals_by_act.get(act_key, [])
        materialized_winners: set[str] = set()
        is_deserted = _deserted(record.get("estado"))
        for proposal in act_proposals:
            matching_winner = next(
                (
                    value
                    for value in winners
                    if provider_matches(proposal.get("proveedor"), value.get("proveedor"))
                ),
                None,
            )
            won = bool(matching_winner) and not is_deserted
            if won:
                materialized_winners.add(normalize_provider(proposal.get("proveedor")))
            if is_deserted:
                result = "Desierto"
            elif won:
                result = "Adjudicado"
            elif winner:
                result = "No adjudicado"
            else:
                result = "En evaluacion"
            amount = float(proposal.get("monto_ofertado") or 0)
            rows.append(
                {
                    **proposal,
                    "ganador": winner,
                    "ganador_norm": normalize_provider(winner),
                    "ganado": bool(won),
                    "monto_ganado": amount if won else 0.0,
                    "resultado_empresa": result,
                    "fuente_resultado": "resultado_oficial",
                    "resultado_provisional": 0,
                    "monto_ganado_fuente": (
                        str(matching_winner.get("fuente") or "precio_proponente")
                        if won and matching_winner
                        else ""
                    ),
                }
            )

        # Materializa cada adjudicatario oficial omitido en la tabla de
        # propuestas. Esto cubre adjudicaciones multiples sin inventar un
        # ganador en actos desiertos.
        next_synthetic_ordinal = max(
            (int(value.get("ordinal") or 0) for value in act_proposals),
            default=0,
        ) + 1
        for winner_entry in ([] if is_deserted else winners):
            winner_name = clean_text(winner_entry.get("proveedor"))
            if not winner_name:
                continue
            if normalize_provider(winner_name) in materialized_winners:
                continue
            amount = parse_money(winner_entry.get("monto_ganado"))
            amount_source = clean_text(winner_entry.get("fuente"))
            if amount <= 0:
                amount, amount_source = _winner_amount(record)
            rows.append(
                {
                    "acto_key": act_key,
                    "ordinal": next_synthetic_ordinal,
                    "proveedor": winner_name,
                    "proveedor_norm": normalize_provider(winner_name),
                    "monto_ofertado": amount,
                    "ganador": winner,
                    "ganador_norm": normalize_provider(winner),
                    "ganado": True,
                    "monto_ganado": amount,
                    "resultado_empresa": "Adjudicado",
                    "fuente_resultado": "resultado_oficial_sintetico",
                    "resultado_provisional": 0,
                    "monto_ganado_fuente": amount_source,
                }
            )
            next_synthetic_ordinal += 1
    return pd.DataFrame(rows, columns=PC_PROPOSAL_COLUMNS)


def _qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _source_columns(connection: sqlite3.Connection) -> list[str]:
    existing = {str(row[1]) for row in connection.execute("PRAGMA table_info(actos_publicos)")}
    requested = list(CORE_COLUMNS)
    for ordinal in range(1, 15):
        requested.extend((f"Proponente {ordinal}", f"Precio Proponente {ordinal}"))
    return [column for column in requested if column in existing]


def _prepare_chunk(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Se materializan tambien los ambiguos. La vista normal los excluye en SQL,
    # pero el usuario puede recuperarlos con el selector correspondiente.
    acts = prepare_pc_acts(raw, PCFilters(include_ambiguous=True))
    if acts.empty:
        return pd.DataFrame(columns=PC_ACT_COLUMNS), pd.DataFrame(columns=PC_PROPOSAL_COLUMNS)
    acts = acts.drop_duplicates("acto_key", keep="last").copy()
    acts["source_id"] = acts.get("id")
    acts["fecha_analitica"] = pd.to_datetime(acts["fecha_analitica"], errors="coerce").dt.strftime("%Y-%m-%d")
    acts["numero_proceso"] = acts.apply(
        lambda row: _process_number(row.get("enlace"), row.get("titulo")), axis=1
    )
    acts["source_layer"] = "resultado_oficial"
    acts["resultado_provisional"] = 0
    for column in PC_ACT_COLUMNS:
        if column not in acts.columns:
            acts[column] = ""
    act_output = acts[PC_ACT_COLUMNS].copy()
    return act_output, _official_proposals(acts)


def _safe_json(value: object, fallback: object) -> object:
    try:
        decoded = json.loads(clean_text(value) or json.dumps(fallback))
        return decoded
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


def _lifecycle_items(payload: dict[str, object]) -> str:
    pairs: list[tuple[int, str]] = []
    for key, value in payload.items():
        match = re.fullmatch(r"item_(\d+)", str(key))
        text_value = clean_text(value)
        if match and text_value:
            pairs.append((int(match.group(1)), text_value))
    return json.dumps([value for _, value in sorted(pairs)], ensure_ascii=False)


def _prepare_lifecycle(
    connection: sqlite3.Connection,
    *,
    official_processes: set[str],
    official_keys: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "cl_cotizaciones" not in tables:
        return (
            pd.DataFrame(columns=PC_ACT_COLUMNS),
            pd.DataFrame(columns=PC_PROPOSAL_COLUMNS),
            0,
        )
    connection.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in FINAL_CL_STATES)
    records = [
        dict(row)
        for row in connection.execute(
            f"SELECT * FROM cl_cotizaciones WHERE estado_derivado IN ({placeholders})",
            tuple(sorted(FINAL_CL_STATES)),
        )
    ]
    raw_rows: list[dict[str, object]] = []
    proposal_map: dict[str, list[dict[str, object]]] = {}
    for record in records:
        process_number = _process_number(record.get("numero_cl"), record.get("enlace"))
        act_key = clean_text(record.get("enlace")) or clean_text(record.get("cl_key")) or process_number
        if not act_key or act_key in official_keys or (process_number and process_number in official_processes):
            continue
        payload = _safe_json(record.get("source_payload_json"), {})
        payload = payload if isinstance(payload, dict) else {}
        proponents = _safe_json(record.get("proponents_json"), [])
        proponents = proponents if isinstance(proponents, list) else []
        status = "Desierto" if clean_text(record.get("estado_derivado")) == "cerrada_sin_propuestas" else "En evaluacion"
        payload_reference = parse_money(payload.get("precio_referencia"))
        table_reference = parse_money(record.get("precio_referencia"))
        reference = payload_reference or table_reference
        ficha = clean_text(payload.get("ficha_detectada"))
        raw_rows.append(
            {
                "id": None,
                "publicacion": clean_text(record.get("fecha_publicacion")) or clean_text(payload.get("publicacion")),
                "fecha": clean_text(record.get("fecha_cierre")) or clean_text(payload.get("fecha")),
                "fecha_adjudicacion": "",
                "fecha_actualizacion": clean_text(record.get("updated_at")) or clean_text(record.get("last_seen_at")),
                "enlace": act_key,
                "titulo": clean_text(record.get("titulo")) or clean_text(payload.get("titulo")),
                "descripcion": clean_text(payload.get("descripcion")) or clean_text(record.get("titulo")),
                "entidad": clean_text(record.get("entidad")) or clean_text(payload.get("entidad")),
                "unidad_solic": clean_text(record.get("unidad_solicitante")) or clean_text(payload.get("unidad_solic")),
                "estado": status,
                "precio_referencia": reference,
                "razon_social": "",
                "nombre_comercial": "",
                "num_participantes": len(proponents),
                "total_items_ofertados": "",
                "ficha_detectada": ficha or "No Detectada",
                "fichas_detectadas_json": json.dumps(re.findall(r"\d{3,}", ficha), ensure_ascii=False),
                "items_json": _lifecycle_items(payload),
                "source_tipo_proceso": "CL",
                "acto_key": act_key,
                "numero_proceso": process_number,
            }
        )
        proposal_map[act_key] = [item for item in proponents if isinstance(item, dict)]

    if not raw_rows:
        return pd.DataFrame(columns=PC_ACT_COLUMNS), pd.DataFrame(columns=PC_PROPOSAL_COLUMNS), len(records)
    acts = prepare_pc_acts(pd.DataFrame(raw_rows), PCFilters(include_ambiguous=True))
    if acts.empty:
        return pd.DataFrame(columns=PC_ACT_COLUMNS), pd.DataFrame(columns=PC_PROPOSAL_COLUMNS), len(records)
    acts = acts.drop_duplicates("acto_key", keep="last").copy()
    # Las cotizaciones en linea no tienen el id numerico del API oficial.
    # Debe conservarse como NULL para que PostgreSQL no intente insertar una
    # cadena vacia en la columna BIGINT inferida de los actos oficiales.
    acts["source_id"] = None
    acts["fecha_analitica"] = pd.to_datetime(acts["fecha_analitica"], errors="coerce").dt.strftime("%Y-%m-%d")
    acts["numero_proceso"] = acts.apply(
        lambda row: clean_text(row.get("numero_proceso")) or _process_number(row.get("enlace")), axis=1
    )
    acts["source_layer"] = "cotizacion_linea"
    acts["resultado_provisional"] = 1
    for column in PC_ACT_COLUMNS:
        if column not in acts.columns:
            acts[column] = ""
    act_output = acts[PC_ACT_COLUMNS].copy()

    proposal_rows: list[dict[str, object]] = []
    eligible_keys = set(act_output["acto_key"].astype(str))
    for act_key in eligible_keys:
        for ordinal, proposal in enumerate(proposal_map.get(act_key, []), start=1):
            provider = clean_text(proposal.get("name") or proposal.get("proveedor"))
            if not provider:
                continue
            proposal_rows.append(
                {
                    "acto_key": act_key,
                    "ordinal": ordinal,
                    "proveedor": provider,
                    "proveedor_norm": normalize_provider(provider),
                    "monto_ofertado": parse_money(proposal.get("total") or proposal.get("monto")),
                    "ganador": "",
                    "ganador_norm": "",
                    "ganado": False,
                    "monto_ganado": 0.0,
                    "resultado_empresa": "En evaluacion",
                    "fuente_resultado": "cotizacion_linea_cerrada",
                    "resultado_provisional": 1,
                    "monto_ganado_fuente": "",
                }
            )
    return act_output, pd.DataFrame(proposal_rows, columns=PC_PROPOSAL_COLUMNS), len(records)


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
            source_tipo_proceso TEXT,
            numero_proceso TEXT,
            source_layer TEXT,
            resultado_provisional INTEGER
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
            resultado_empresa TEXT,
            fuente_resultado TEXT,
            resultado_provisional INTEGER,
            monto_ganado_fuente TEXT,
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
        CREATE INDEX idx_pc_propuestas_resultado ON pc_propuestas(resultado_empresa);
        CREATE INDEX idx_pc_actos_source_layer ON pc_actos(source_layer);
        CREATE TABLE pc_proveedores_catalogo AS
        SELECT p.proveedor_norm,
               MIN(p.proveedor) AS proveedor,
               COUNT(DISTINCT p.acto_key) AS participaciones,
               COUNT(DISTINCT CASE WHEN p.ganado=1 THEN p.acto_key END) AS adjudicaciones,
               SUM(p.monto_ofertado) AS monto_ofertado,
               SUM(p.monto_ganado) AS monto_ganado
        FROM pc_propuestas p
        JOIN pc_actos a ON a.acto_key=p.acto_key
        WHERE trim(COALESCE(p.proveedor_norm,'')) <> '' AND a.mercado_pc='no_medico'
        GROUP BY p.proveedor_norm;
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
        WHERE trim(COALESCE(p.proveedor_norm,'')) <> '' AND a.mercado_pc='no_medico'
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
        WHERE trim(COALESCE(p.proveedor_norm,'')) <> '' AND a.mercado_pc='no_medico'
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
        WHERE mercado_pc='no_medico'
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
    lifecycle_source_rows = 0
    lifecycle_act_rows = 0
    lifecycle_proposal_rows = 0
    materialized_keys: set[str] = set()
    official_processes: set[str] = set()
    try:
        for index, raw in enumerate(pd.read_sql_query(query, source_connection, chunksize=chunk_size), start=1):
            acts, proposals = _prepare_chunk(raw)
            if not acts.empty:
                acts = acts[~acts["acto_key"].astype(str).isin(materialized_keys)].copy()
                eligible_keys = set(acts["acto_key"].astype(str))
                proposals = proposals[proposals["acto_key"].astype(str).isin(eligible_keys)].copy()
                acts.to_sql("pc_actos", target, if_exists="append", index=False)
                act_rows += len(acts)
                materialized_keys.update(eligible_keys)
                official_processes.update(
                    value for value in acts["numero_proceso"].map(clean_text).tolist() if value
                )
            if not proposals.empty:
                proposals.to_sql("pc_propuestas", target, if_exists="append", index=False)
                proposal_rows += len(proposals)
            if index % 5 == 0:
                print(f"[PC] chunks={index} actos={act_rows:,} propuestas={proposal_rows:,}", flush=True)

        lifecycle_acts, lifecycle_proposals, lifecycle_source_rows = _prepare_lifecycle(
            source_connection,
            official_processes=official_processes,
            official_keys=materialized_keys,
        )
        if not lifecycle_acts.empty:
            lifecycle_acts = lifecycle_acts[
                ~lifecycle_acts["acto_key"].astype(str).isin(materialized_keys)
            ].drop_duplicates("acto_key", keep="last")
            lifecycle_keys = set(lifecycle_acts["acto_key"].astype(str))
            lifecycle_proposals = lifecycle_proposals[
                lifecycle_proposals["acto_key"].astype(str).isin(lifecycle_keys)
            ].drop_duplicates(["acto_key", "ordinal"], keep="last")
            lifecycle_acts.to_sql("pc_actos", target, if_exists="append", index=False)
            lifecycle_act_rows = len(lifecycle_acts)
            act_rows += lifecycle_act_rows
            materialized_keys.update(lifecycle_keys)
            if not lifecycle_proposals.empty:
                lifecycle_proposals.to_sql("pc_propuestas", target, if_exists="append", index=False)
                lifecycle_proposal_rows = len(lifecycle_proposals)
                proposal_rows += lifecycle_proposal_rows
        print(
            f"[PC] ciclo CL fuente={lifecycle_source_rows:,} actos={lifecycle_act_rows:,} "
            f"propuestas={lifecycle_proposal_rows:,}",
            flush=True,
        )

        if source_rows > 0 and act_rows <= 0:
            raise RuntimeError("Control de calidad: la fuente tiene datos pero Inteligencia PC quedo vacia.")
        max_date_row = target.execute("SELECT MAX(fecha_analitica) FROM pc_actos").fetchone()
        max_date = clean_text(max_date_row[0] if max_date_row else "")
        metadata = {
            "built_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_db": str(source),
            "source_rows": str(source_rows),
            "act_rows": str(act_rows),
            "proposal_rows": str(proposal_rows),
            "lifecycle_source_rows": str(lifecycle_source_rows),
            "lifecycle_act_rows": str(lifecycle_act_rows),
            "lifecycle_proposal_rows": str(lifecycle_proposal_rows),
            "max_analytic_date": max_date,
            "classifier_version": "pc-market-1.1.0",
            "participation_model_version": "pc-participations-2.0.0",
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
                if table == "pc_actos" and "source_id" in frame.columns:
                    numeric_ids = pd.to_numeric(frame["source_id"], errors="coerce")
                    frame["source_id"] = pd.Series(
                        [int(value) if pd.notna(value) else None for value in numeric_ids],
                        index=frame.index,
                        dtype=object,
                    )
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
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_source_layer ON pc_actos(source_layer)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_actos_process ON pc_actos(numero_proceso)'))
            connection.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ux_pc_propuestas_key ON pc_propuestas(acto_key,ordinal)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_propuestas_empresa ON pc_propuestas(proveedor_norm)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_propuestas_acto ON pc_propuestas(acto_key)'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS ix_pc_propuestas_resultado ON pc_propuestas(resultado_empresa)'))
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
