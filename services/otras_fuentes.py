from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


REQUIRED_TABLES = {
    "external_sources",
    "external_monitor_runs",
    "external_opportunities",
    "external_opportunity_documents",
}

SOURCE_LABELS = {
    "acp": "ACP",
    "ensa": "ENSA",
    "idaan": "IDAAN",
    "ena": "ENA",
    "ungm": "UNGM",
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
    limit: int = 1000


def schema_ready(engine: Engine) -> tuple[bool, set[str]]:
    available = set(inspect(engine).get_table_names())
    return REQUIRED_TABLES.issubset(available), available


def load_source_health(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT source, display_name, baseline_completed, last_success_at, last_error_at,
               last_error, last_count, last_run_id, updated_at
        FROM external_sources
        ORDER BY source
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


def build_search_query(filters: OpportunityFilters) -> tuple[str, dict[str, Any]]:
    clauses = ["1=1"]
    params: dict[str, Any] = {"limit": max(1, min(int(filters.limit), 5000))}
    search = str(filters.search or "").strip()
    if search:
        params["search"] = f"%{search.lower()}%"
        clauses.append(
            "(LOWER(o.title) LIKE :search OR LOWER(COALESCE(o.description,'')) LIKE :search "
            "OR LOWER(COALESCE(o.buyer,'')) LIKE :search OR LOWER(COALESCE(o.external_id,'')) LIKE :search)"
        )
    _add_in_filter(clauses, params, "o.source", "source", filters.sources)
    _add_in_filter(clauses, params, "o.matched_company", "company", filters.companies)
    _add_in_filter(clauses, params, "o.status", "status", filters.statuses)
    _add_in_filter(clauses, params, "o.priority", "priority", filters.priorities)
    if filters.start_date:
        clauses.append("COALESCE(NULLIF(o.publication_date,''), o.first_seen_at) >= :start_date")
        params["start_date"] = filters.start_date
    if filters.end_date:
        clauses.append("COALESCE(NULLIF(o.publication_date,''), o.first_seen_at) <= :end_date")
        params["end_date"] = filters.end_date + "T23:59:59"
    if filters.only_active:
        clauses.append("o.is_active = 1")

    query = f"""
        SELECT o.id, o.source, o.external_id, o.title, o.source_type, o.buyer,
               o.publication_date, o.deadline, o.status, o.estimated_value, o.currency,
               o.matched_company, o.priority, o.fit_score, o.source_url,
               o.first_seen_at, o.last_seen_at, o.cross_source_key,
               COALESCE(d.duplicate_count, 1) AS fuentes_coincidentes
        FROM external_opportunities o
        LEFT JOIN (
            SELECT cross_source_key, COUNT(*) AS duplicate_count
            FROM external_opportunities
            WHERE cross_source_key <> ''
            GROUP BY cross_source_key
        ) d ON d.cross_source_key = o.cross_source_key
        WHERE {' AND '.join(clauses)}
        ORDER BY CASE o.priority WHEN 'Alta' THEN 1 WHEN 'Media' THEN 2 ELSE 3 END,
                 o.fit_score DESC, COALESCE(NULLIF(o.deadline,''), '9999-12-31'), o.last_seen_at DESC
        LIMIT :limit
    """
    return query, params


def search_opportunities(engine: Engine, filters: OpportunityFilters) -> pd.DataFrame:
    query, params = build_search_query(filters)
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
