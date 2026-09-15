from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


API_VERSION = 4

REQUIRED_TABLES = {
    "external_sources",
    "external_monitor_runs",
    "external_opportunities",
    "external_opportunity_documents",
}

SOURCE_LABELS = {
    'naturgy': 'Naturgy Panamá · Acceso pendiente',
    'aes': 'AES Panamá · Acceso pendiente',
    "acp": "ACP · Estudios de mercado",
    "acp_sli": "ACP · Licitaciones SLI",
    "ifrc": "IFRC · Compras humanitarias",
    "ensa": "ENSA",
    "idaan": "IDAAN",
    "ena": "ENA",
    "ungm": "UNGM",
    "ungm_international": "UNGM regional/global",
    "idb": "BID",
    "world_bank": "Banco Mundial",
    "unicef": "UNICEF",
    "cruz_roja": "Cruz Roja",
    "ciudad_saber": "Ciudad del Saber",
}


@dataclass(frozen=True, slots=True)
class OpportunityFilters:
    search: str = ""
    sources: tuple[str, ...] = ()
    companies: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ()
    priorities: tuple[str, ...] = ()
    start_date: str = ""
    end_date: str = ""
    only_active: bool = False
    sort_by: str = "published_desc"
    limit: int = 100
    offset: int = 0
    view: str = "relevant"
    scopes: tuple[str, ...] = ()
    deduplicate: bool = True


SORT_ORDERS = {
    "published_desc": (
        "CASE WHEN NULLIF(o.publication_date,'') IS NULL THEN 1 ELSE 0 END, "
        "NULLIF(o.publication_date,'') DESC, "
        "o.first_seen_at DESC"
    ),
    "detected_desc": "o.first_seen_at DESC",
    "deadline_asc": (
        "CASE WHEN NULLIF(o.deadline,'') IS NULL THEN 1 ELSE 0 END, "
        "NULLIF(o.deadline,'') ASC, o.first_seen_at DESC"
    ),
    "amount_desc": "o.estimated_value DESC NULLS LAST, o.first_seen_at DESC",
    "priority_score": (
        "CASE o.priority WHEN 'Alta' THEN 1 WHEN 'Media' THEN 2 ELSE 3 END, "
        "o.fit_score DESC, COALESCE(NULLIF(o.deadline,''), '9999-12-31'), "
        "o.last_seen_at DESC"
    ),
}


VIEW_LABELS = {"relevant": "Para evaluar", "review": "Por revisar", "historical": "Histórico", "no_match": "Sin encaje", "all": "Todos"}


def _review_cte(dialect: str = "postgresql") -> str:
    def field(name):
        if dialect == "sqlite":
            return f"json_extract(COALESCE(NULLIF(o.raw_payload_json,''),'{{}}'), '$.qualification.{name}')"
        return f"(COALESCE(NULLIF(o.raw_payload_json,''),'{{}}')::jsonb -> 'qualification' ->> '{name}')"
    day, at, bucket, reason, scope, closed = [field(k) for k in ('deadline_date','deadline_at','bucket','reason','scope','closed')]
    display_title = field('display_title')
    detail_text = ("json_extract(COALESCE(NULLIF(o.raw_payload_json,''),'{}'), '$.document_analysis.text')"
                   if dialect == 'sqlite' else "(COALESCE(NULLIF(o.raw_payload_json,''),'{}')::jsonb -> 'document_analysis' ->> 'text')")
    superseded = ("json_extract(COALESCE(NULLIF(o.raw_payload_json,''),'{}'), '$.superseded_by')"
                  if dialect == 'sqlite' else "(COALESCE(NULLIF(o.raw_payload_json,''),'{}')::jsonb ->> 'superseded_by')")
    official_code = ("json_extract(COALESCE(NULLIF(o.raw_payload_json,''),'{}'), '$.official_number')"
                     if dialect == 'sqlite' else "(COALESCE(NULLIF(o.raw_payload_json,''),'{}')::jsonb ->> 'official_number')")
    return f"""WITH enriched AS (
        SELECT o.*, COALESCE({day}, '') AS deadline_date,
               COALESCE(NULLIF({official_code},''), o.external_id) AS display_code,
               COALESCE(NULLIF({display_title},''), o.title) AS display_title,
               COALESCE({detail_text}, '') AS document_text,
               COALESCE({at}, '') AS deadline_at,
               COALESCE({bucket}, 'review') AS stored_bucket,
               COALESCE({reason}, 'Clasificación pendiente; el anuncio se conserva para revisión') AS stored_reason,
               COALESCE({scope}, 'Global') AS market_scope,
               CASE WHEN CAST({closed} AS TEXT) IN ('true','1') OR o.is_active = 0 THEN 1 ELSE 0 END AS explicitly_closed
        FROM external_opportunities o WHERE COALESCE({superseded}, '') = ''
    ), reviewed AS (
        SELECT e.*, CASE
            WHEN explicitly_closed = 1
              OR (deadline_at <> '' AND deadline_at < :now)
              OR (deadline_at = '' AND deadline_date <> '' AND deadline_date < :today) THEN 'historical'
            WHEN stored_bucket = 'relevant' AND COALESCE(last_seen_at,'') < :stale_before THEN 'review'
            ELSE stored_bucket END AS review_bucket,
            CASE WHEN explicitly_closed = 1 THEN 'Cerrada o adjudicada; conservada en el histórico'
              WHEN (deadline_at <> '' AND deadline_at < :now)
                OR (deadline_at = '' AND deadline_date <> '' AND deadline_date < :today) THEN 'Fecha límite vencida; conservada en el histórico'
              WHEN stored_bucket = 'relevant' AND COALESCE(last_seen_at,'') < :stale_before THEN 'Sin reconfirmar durante más de 7 días; revisar vigencia oficial'
              ELSE stored_reason END AS review_reason
        FROM enriched e
    )"""


def _time_params(now: datetime | None = None) -> dict[str, str]:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None: now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(timezone(timedelta(hours=-5)))
    return {"now": now.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "today": local.date().isoformat(),
            "stale_before": (local - timedelta(days=7)).date().isoformat()}


def load_review_counts(engine: Engine) -> dict[str, int]:
    query = _review_cte(engine.dialect.name) + """,
        distinct_notices AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY COALESCE(NULLIF(canonical_url,''), id), external_id
                ORDER BY last_seen_at DESC, id) AS position
            FROM reviewed
        ) SELECT review_bucket, COUNT(*) AS count FROM distinct_notices
          WHERE position=1 GROUP BY review_bucket"""
    frame = pd.read_sql_query(text(query), engine, params=_time_params())
    counts = {key: 0 for key in VIEW_LABELS if key != 'all'}
    counts.update({str(row.review_bucket): int(row.count) for row in frame.itertuples()})
    return counts


def schema_ready(engine: Engine) -> tuple[bool, set[str]]:
    # Consultar solo las tablas del modulo evita cargar todo el catalogo de
    # Supabase, que es sensiblemente mas lento en conexiones remotas.
    names = ",".join(f"'{name}'" for name in sorted(REQUIRED_TABLES))
    frame = pd.read_sql_query(
        text(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = 'public' AND table_name IN ({names})"
        ),
        engine,
    )
    available = set(frame.get("table_name", pd.Series(dtype=str)).astype(str))
    return REQUIRED_TABLES.issubset(available), available


def load_dashboard_snapshot(
    engine: Engine,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, int], dict[str, list[str]]]:
    """Carga estado, metricas y opciones en un solo viaje a Supabase."""
    query = text(
        """
        WITH last_run AS (
            SELECT run_id, started_at, finished_at, status, source_count, success_count,
                   error_count, total_records, new_records, changed_records, event_count,
                   postgres_synced, error_json
            FROM external_monitor_runs
            ORDER BY started_at DESC
            LIMIT 1
        ),
        overview AS (
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active,
                   SUM(CASE WHEN matched_company <> '' THEN 1 ELSE 0 END) AS relevant,
                   SUM(CASE WHEN priority = 'Alta' THEN 1 ELSE 0 END) AS high_priority,
                   SUM(CASE WHEN NULLIF(first_seen_at, '')::timestamptz >= CURRENT_DATE - INTERVAL '7 days'
                            THEN 1 ELSE 0 END) AS new_7d,
                   SUM(CASE WHEN deadline >= CURRENT_DATE::text
                             AND deadline <= (CURRENT_DATE + INTERVAL '14 days')::date::text
                            THEN 1 ELSE 0 END) AS closing_14d
            FROM external_opportunities
        ),
        filter_options AS (
            SELECT jsonb_build_object(
                'source', COALESCE(jsonb_agg(DISTINCT source)
                    FILTER (WHERE NULLIF(BTRIM(source),'') IS NOT NULL), '[]'::jsonb),
                'matched_company', COALESCE(jsonb_agg(DISTINCT matched_company)
                    FILTER (WHERE NULLIF(BTRIM(matched_company),'') IS NOT NULL), '[]'::jsonb),
                'status', COALESCE(jsonb_agg(DISTINCT status)
                    FILTER (WHERE NULLIF(BTRIM(status),'') IS NOT NULL), '[]'::jsonb),
                'priority', COALESCE(jsonb_agg(DISTINCT priority)
                    FILTER (WHERE NULLIF(BTRIM(priority),'') IS NOT NULL), '[]'::jsonb)
            ) AS values
            FROM external_opportunities
        )
        SELECT
            COALESCE((
                SELECT jsonb_agg(to_jsonb(s) ORDER BY s.source)
                FROM external_sources s
            ), '[]'::jsonb) AS health,
            COALESCE((SELECT to_jsonb(l) FROM last_run l), '{}'::jsonb) AS last_run,
            COALESCE((SELECT to_jsonb(o) FROM overview o), '{}'::jsonb) AS overview,
            COALESCE((SELECT values FROM filter_options), '{}'::jsonb) AS options
        """
    )
    frame = pd.read_sql_query(query, engine)
    if frame.empty:
        return pd.DataFrame(), {}, {}, {}
    row = frame.iloc[0]
    health_payload = row.get("health") or []
    last_run = dict(row.get("last_run") or {})
    overview_payload = dict(row.get("overview") or {})
    options_payload = dict(row.get("options") or {})
    overview = {
        key: 0 if value is None or pd.isna(value) else int(value)
        for key, value in overview_payload.items()
    }
    options = {
        key: sorted(str(value).strip() for value in (options_payload.get(key) or []) if str(value).strip())
        for key in ("source", "matched_company", "status", "priority")
    }
    overview.update(load_review_counts(engine))
    return pd.DataFrame(health_payload), last_run, overview, options


def load_source_health(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT s.source, s.display_name, s.baseline_completed, s.last_success_at, s.last_error_at,
               s.last_error, s.last_count, s.last_run_id, s.updated_at,
               r.status AS capture_status, r.coverage, r.record_count
        FROM external_sources s LEFT JOIN external_source_runs r
          ON r.source=s.source AND r.run_id=s.last_run_id
        ORDER BY s.source
        """
    )
    return pd.read_sql_query(query, engine)


def load_last_run(engine: Engine) -> dict[str, Any]:
    query = text(
        """
        SELECT run_id, started_at, finished_at, status, source_count, success_count,
               error_count, total_records, new_records, changed_records, event_count,
               postgres_synced, error_json
        FROM external_monitor_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    frame = pd.read_sql_query(query, engine)
    return frame.iloc[0].to_dict() if not frame.empty else {}


def load_overview(engine: Engine) -> dict[str, int | float]:
    query = text(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN matched_company <> '' THEN 1 ELSE 0 END) AS relevant,
               SUM(CASE WHEN priority = 'Alta' THEN 1 ELSE 0 END) AS high_priority,
               SUM(CASE WHEN NULLIF(first_seen_at, '')::timestamptz >= CURRENT_DATE - INTERVAL '7 days'
                        THEN 1 ELSE 0 END) AS new_7d,
               SUM(CASE WHEN deadline >= CURRENT_DATE::text
                         AND deadline <= (CURRENT_DATE + INTERVAL '14 days')::date::text
                        THEN 1 ELSE 0 END) AS closing_14d
        FROM external_opportunities
        """
    )
    frame = pd.read_sql_query(query, engine)
    if frame.empty:
        return {"total": 0, "active": 0, "relevant": 0, "high_priority": 0, "new_7d": 0, "closing_14d": 0}
    return {
        key: 0 if pd.isna(value) else int(value)
        for key, value in frame.iloc[0].to_dict().items()
    }


def load_filter_options(engine: Engine) -> dict[str, list[str]]:
    frame = pd.read_sql_query(
        text("SELECT DISTINCT source, matched_company, status, priority FROM external_opportunities"),
        engine,
    )
    options: dict[str, list[str]] = {}
    for column in ("source", "matched_company", "status", "priority"):
        values = [] if column not in frame else frame[column].dropna().astype(str).str.strip().tolist()
        options[column] = sorted({value for value in values if value})
    return options


def _add_in_filter(
    clauses: list[str], params: dict[str, Any], column: str, prefix: str, values: Iterable[str]
) -> None:
    clean = [str(value).strip() for value in values if str(value).strip()]
    if not clean:
        return
    names = []
    for index, value in enumerate(clean):
        name = f"{prefix}_{index}"
        names.append(f":{name}")
        params[name] = value
    clauses.append(f"{column} IN ({','.join(names)})")


def build_search_query(filters: OpportunityFilters, *, dialect: str = "postgresql", now: datetime | None = None) -> tuple[str, dict[str, Any]]:
    clauses = ["1=1"]
    params: dict[str, Any] = {
        "limit": max(1, min(int(filters.limit), 500)),
        "offset": max(0, int(filters.offset)),
        **_time_params(now),
    }
    search = str(filters.search or "").strip()
    if search:
        params["search"] = f"%{search.lower()}%"
        clauses.append(
            "(LOWER(o.title) LIKE :search OR LOWER(o.display_title) LIKE :search OR LOWER(o.document_text) LIKE :search OR LOWER(COALESCE(o.description,'')) LIKE :search "
            "OR LOWER(COALESCE(o.buyer,'')) LIKE :search OR LOWER(COALESCE(o.external_id,'')) LIKE :search)"
        )
    _add_in_filter(clauses, params, "o.source", "source", filters.sources)
    source_clauses: list[str] = []
    _add_in_filter(source_clauses, params, "o.source", "source", filters.sources)
    company_clauses = []
    for index, company in enumerate(filters.companies):
        params[f"company_{index}"] = f"%{company}%"
        company_clauses.append(f"o.matched_company LIKE :company_{index}")
    if company_clauses: clauses.append('(' + ' OR '.join(company_clauses) + ')')
    _add_in_filter(clauses, params, "o.market_scope", "scope", filters.scopes)
    if filters.view == 'current':
        clauses.append("o.review_bucket IN ('relevant','review')")
    elif filters.view != "all":
        params["view"] = filters.view if filters.view in VIEW_LABELS else "relevant"
        clauses.append("o.review_bucket = :view")
    _add_in_filter(clauses, params, "o.status", "status", filters.statuses)
    _add_in_filter(clauses, params, "o.priority", "priority", filters.priorities)
    if filters.start_date:
        clauses.append("COALESCE(NULLIF(o.publication_date,''), o.first_seen_at) >= :start_date")
        params["start_date"] = filters.start_date
    if filters.end_date:
        clauses.append("COALESCE(NULLIF(o.publication_date,''), o.first_seen_at) <= :end_date")
        params["end_date"] = filters.end_date + "T23:59:59"
    if filters.only_active:
        clauses.append("o.is_active = 1 AND o.review_bucket <> 'historical'")

    order_by = SORT_ORDERS.get(filters.sort_by, SORT_ORDERS["published_desc"])
    order_by = f"{order_by}, o.id ASC"
    query = _review_cte(dialect) + f""",
        ranked AS (
            SELECT o.*, ROW_NUMBER() OVER (
                PARTITION BY COALESCE(NULLIF(o.canonical_url,''), o.id), o.external_id
                ORDER BY o.last_seen_at DESC, o.id) AS duplicate_position,
                COUNT(*) OVER (PARTITION BY COALESCE(NULLIF(o.canonical_url,''), o.id), o.external_id) AS fuentes_coincidentes
            FROM reviewed o WHERE {' AND '.join(source_clauses) if source_clauses else '1=1'}
        ), filtered AS (
            SELECT o.* FROM ranked o WHERE {' AND '.join(clauses)}
              AND {'o.duplicate_position = 1' if filters.deduplicate else '1=1'}
        )
        SELECT o.id, o.source, o.display_code AS external_id, o.display_title AS title, o.source_type, o.buyer, o.country,
               o.publication_date, COALESCE(NULLIF(o.deadline_date,''), o.deadline) AS deadline,
               o.status, o.estimated_value, o.currency,
               o.matched_company, o.priority, o.fit_score, o.source_url,
               o.first_seen_at, o.last_seen_at, o.cross_source_key,
               o.review_bucket, o.review_reason, o.market_scope, o.fuentes_coincidentes,
               COUNT(*) OVER() AS total_resultados
        FROM filtered o
        ORDER BY {order_by}
        LIMIT :limit OFFSET :offset
    """
    return query, params


def search_opportunities(engine: Engine, filters: OpportunityFilters) -> pd.DataFrame:
    query, params = build_search_query(filters, dialect=engine.dialect.name)
    return pd.read_sql_query(text(query), engine, params=params)


def load_documents(engine: Engine, opportunity_id: str) -> pd.DataFrame:
    return pd.read_sql_query(
        text(
            """
            SELECT title, document_type, url, first_seen_at, last_seen_at
            FROM external_opportunity_documents
            WHERE opportunity_id = :opportunity_id
            ORDER BY document_type, title
            """
        ),
        engine,
        params={"opportunity_id": opportunity_id},
    )


def load_opportunity_detail(engine: Engine, opportunity_id: str) -> dict:
    frame = pd.read_sql_query(text('''SELECT id, title, description, source_url, external_id,
        registration_required, submission_channel, eligibility, procurement_method,
        raw_payload_json, matched_keywords_json, matched_fields_json, sector,
        publication_date, deadline, first_seen_at, last_seen_at, last_changed_at
        FROM external_opportunities WHERE id=:id'''), engine, params={'id': opportunity_id})
    return frame.iloc[0].to_dict() if not frame.empty else {}
