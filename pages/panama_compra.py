"""Vista PanamáCompra para GE FinApp."""

# pages/visualizador.py
import os
import math
import json
import re
import sqlite3
import time
import unicodedata
from collections import Counter
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import uuid
from datetime import date, timedelta, datetime, timezone
from ui.theme import apply_global_theme
from sqlalchemy import create_engine, text
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from core.config import DB_PATH
from sheets import get_client, read_worksheet
from services.auth_drive import get_drive_delegated

apply_global_theme()


ROW_ID_COL = "__row__"
CHECKBOX_FLAG_NAMES = {
    "prioritario",
    "prioritarios",
    "descartar",
    "descarte",
}
TRUE_VALUES = {"true", "1", "si", "sí", "yes", "y", "t", "x", "on"}


def _ensure_scroll_top_on_page_entry() -> None:
    """
    Fuerza scroll arriba solo al entrar a esta pagina.
    Evita brincar al top en cada rerun normal por interacciones.
    """
    page_key = "__current_page__"
    current_page = "panama_compra"
    previous_page = st.session_state.get(page_key)
    st.session_state[page_key] = current_page
    if previous_page == current_page:
        return

    components.html(
        """
        <script>
        try { window.parent.scrollTo({top: 0, behavior: "auto"}); } catch (e) {}
        </script>
        """,
        height=0,
    )


def _require_authentication() -> None:
    status = st.session_state.get("authentication_status")
    if status is True:
        st.session_state.setdefault("username", st.session_state.get("username"))
        return
    if status is False:
        st.error("Credenciales inválidas. Vuelve a la portada para iniciar sesión.")
    else:
        st.warning("Debes iniciar sesión para entrar.")

    # Redirige al home, igual que otras páginas protegidas del multipage.
    try:
        st.switch_page("Inicio.py")
    except Exception:
        st.stop()
    st.stop()

PC_STATE_WORKSHEET = "pc_state"
PC_CONFIG_WORKSHEET = "pc_config"
PC_MANUAL_SHEET_ID = "1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
PC_MANUAL_WORKSHEET = "pc_manual"
JOB_NAME_LABELS = {
    "clrir": "Cotizaciones Programadas",
    "clv": "Cotizaciones Abiertas",
    "rir1": "Licitaciones",
}
JOB_NAME_ORDER = ["clrir", "clv", "rir1"]
JOB_SOURCE_SHEETS = {
    "clrir": ["cl_prog_sin_ficha", "cl_prog_sin_requisitos", "cl_prog_con_ct"],
    "clv": ["cl_abiertas", "cl_abiertas_rir_sin_requisitos", "cl_abiertas_rir_con_ct"],
    "rir1": ["ap_con_ct", "ap_sin_ficha", "ap_sin_requisitos"],
}
STATUS_BADGES = {
    "success": ("🟢", "Éxito"),
    "running": ("🟡", "En curso"),
    "failed": ("🔴", "Error"),
    "error": ("🔴", "Error"),
}

HEADER_ALIASES = {
    "request_id": {"request_id", "id", "uuid", "solicitud_id"},
    "timestamp": {"timestamp", "requested_at", "fecha", "fecha_solicitud", "created_at"},
    "job_name": {"job_name", "job", "bot", "proceso"},
    "job_label": {"job_label", "job_desc", "descripcion", "descripción"},
    "requested_by": {"requested_by", "user", "usuario", "solicitado_por"},
    "note": {"note", "nota", "comentario", "observacion", "observación"},
    "status": {"status", "estado"},
}

FALLBACK_DB_PATH = Path(r"C:\Users\rodri\OneDrive\cl\panamacompra.db")
CHAT_MAX_RAW_ROWS = 2000
CHAT_MAX_DISPLAY_ROWS = 300
CHAT_SUMMARY_SAMPLE_ROWS = 80
CHAT_HISTORY_LIMIT = 12
CHAT_SQL_MAX_CHARS = 12000
CHAT_FORBIDDEN_SQL_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "attach",
    "detach",
    "vacuum",
    "pragma",
    "copy",
    "execute",
    "call",
)


def _candidate_db_paths() -> list[Path]:
    candidates: list[Path] = []
    for raw in (DB_PATH, FALLBACK_DB_PATH):
        if not raw:
            continue
        try:
            candidate = Path(raw).expanduser()
        except Exception:
            continue
        if candidate in candidates:
            continue
        candidates.append(candidate)
    return candidates


def _preferred_db_path() -> Path | None:
    candidates = _candidate_db_paths()
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else None


def _supabase_db_url() -> str:
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    candidates = [
        app_cfg.get("SUPABASE_DB_URL"),
        app_cfg.get("DATABASE_URL"),
        os.environ.get("SUPABASE_DB_URL"),
        os.environ.get("DATABASE_URL"),
    ]
    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return ""


def _active_db_backend() -> str:
    return "postgres" if _supabase_db_url() else "sqlite"


@st.cache_resource
def _pg_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)


def _quote_identifier(identifier: str) -> str:
    return f"\"{identifier.replace('\"', '\"\"')}\""


def _connect_sqlite(db_path: str):
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


@st.cache_data(ttl=300)
def list_postgres_tables(db_url: str) -> list[str]:
    engine = _pg_engine(db_url)
    query = text(
        "SELECT table_name "
        "FROM information_schema.tables "
        "WHERE table_schema = 'public' "
        "ORDER BY table_name"
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return [str(r[0]) for r in rows]


@st.cache_data(ttl=300)
def count_postgres_rows(db_url: str, table_name: str) -> int:
    identifier = _quote_identifier(table_name)
    query = text(f"SELECT COUNT(1) FROM {identifier}")
    engine = _pg_engine(db_url)
    with engine.connect() as conn:
        row = conn.execute(query).first()
    return int(row[0]) if row and row[0] is not None else 0


@st.cache_data(ttl=300)
def load_postgres_preview(db_url: str, table_name: str, limit: int) -> pd.DataFrame:
    identifier = _quote_identifier(table_name)
    limit = max(1, int(limit))
    query = f"SELECT * FROM {identifier} LIMIT {limit}"
    engine = _pg_engine(db_url)
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)


@st.cache_data(ttl=300)
def list_sqlite_tables(db_path: str) -> list[str]:
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]


@st.cache_data(ttl=300)
def count_sqlite_rows(db_path: str, table_name: str) -> int:
    identifier = _quote_identifier(table_name)
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(f"SELECT COUNT(1) FROM {identifier}")
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


@st.cache_data(ttl=300)
def load_sqlite_preview(db_path: str, table_name: str, limit: int) -> pd.DataFrame:
    identifier = _quote_identifier(table_name)
    limit = max(1, int(limit))
    query = f"SELECT * FROM {identifier} LIMIT {limit}"
    with _connect_sqlite(db_path) as conn:
        return pd.read_sql_query(query, conn)


@st.cache_data(ttl=300)
def list_sqlite_columns(db_path: str, table_name: str) -> list[str]:
    identifier = _quote_identifier(table_name)
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(f"PRAGMA table_info({identifier})")
        rows = cur.fetchall()
    return [str(r[1]) for r in rows]


@st.cache_data(ttl=300)
def list_postgres_columns(db_url: str, table_name: str) -> list[str]:
    engine = _pg_engine(db_url)
    query = text(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = :table_name "
        "ORDER BY ordinal_position"
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"table_name": table_name}).fetchall()
    return [str(r[0]) for r in rows]


def _split_search_terms(raw: str) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in re.split(r"[\s,;]+", raw) if part.strip()]


def _build_sql_conditions(
    *,
    backend: str,
    columns: list[str],
    search_terms: list[str],
    search_mode: str,
    filters: list[dict[str, str]],
    filters_mode: str,
    combine_mode: str,
):
    """
    Devuelve (where_sql, params) para SQLite y PostgreSQL.
    En SQLite params es list; en PostgreSQL params es dict.
    """
    if backend == "postgres":
        params: dict[str, object] = {}
    else:
        params: list[object] = []

    def _add_param(value):
        if backend == "postgres":
            key = f"p{len(params)}"
            params[key] = value
            return f":{key}"
        params.append(value)
        return "?"

    clauses: list[str] = []

    # Buscador de texto multi-columna.
    if search_terms and columns:
        term_clauses: list[str] = []
        for term in search_terms:
            col_clauses: list[str] = []
            for col in columns:
                qcol = _quote_identifier(col)
                if backend == "postgres":
                    ph = _add_param(f"%{term}%")
                    col_clauses.append(f"CAST({qcol} AS TEXT) ILIKE {ph}")
                else:
                    ph = _add_param(f"%{term.lower()}%")
                    col_clauses.append(f"LOWER(CAST({qcol} AS TEXT)) LIKE {ph}")
            if col_clauses:
                term_clauses.append("(" + " OR ".join(col_clauses) + ")")
        if term_clauses:
            joiner = " AND " if search_mode == "AND" else " OR "
            clauses.append("(" + joiner.join(term_clauses) + ")")

    # Filtros manuales.
    filter_clauses: list[str] = []
    for f in filters:
        col = f.get("column", "")
        op = f.get("operator", "")
        raw_val = (f.get("value", "") or "").strip()
        if not col or not raw_val:
            continue

        qcol = _quote_identifier(col)
        expr = f"CAST({qcol} AS TEXT)"
        expr_lower = f"LOWER({expr})"
        val_lower = raw_val.lower()

        if op == "contiene":
            if backend == "postgres":
                ph = _add_param(f"%{raw_val}%")
                filter_clauses.append(f"{expr} ILIKE {ph}")
            else:
                ph = _add_param(f"%{val_lower}%")
                filter_clauses.append(f"{expr_lower} LIKE {ph}")
        elif op == "igual":
            ph = _add_param(val_lower if backend == "sqlite" else raw_val)
            if backend == "postgres":
                filter_clauses.append(f"{expr_lower} = LOWER({ph})")
            else:
                filter_clauses.append(f"{expr_lower} = {ph}")
        elif op == "distinto":
            ph = _add_param(val_lower if backend == "sqlite" else raw_val)
            if backend == "postgres":
                filter_clauses.append(f"{expr_lower} <> LOWER({ph})")
            else:
                filter_clauses.append(f"{expr_lower} <> {ph}")
        elif op == "empieza con":
            if backend == "postgres":
                ph = _add_param(f"{raw_val}%")
                filter_clauses.append(f"{expr} ILIKE {ph}")
            else:
                ph = _add_param(f"{val_lower}%")
                filter_clauses.append(f"{expr_lower} LIKE {ph}")
        elif op == "termina con":
            if backend == "postgres":
                ph = _add_param(f"%{raw_val}")
                filter_clauses.append(f"{expr} ILIKE {ph}")
            else:
                ph = _add_param(f"%{val_lower}")
                filter_clauses.append(f"{expr_lower} LIKE {ph}")

    if filter_clauses:
        joiner = " AND " if filters_mode == "AND" else " OR "
        clauses.append("(" + joiner.join(filter_clauses) + ")")

    if len(clauses) == 2:
        joiner = " AND " if combine_mode == "AND" else " OR "
        where_sql = "(" + clauses[0] + joiner + clauses[1] + ")"
    elif clauses:
        where_sql = clauses[0]
    else:
        where_sql = ""

    return where_sql, params


def query_sqlite_preview(
    db_path: str,
    table_name: str,
    where_sql: str,
    params: list[object],
    limit: int,
    offset: int,
) -> pd.DataFrame:
    identifier = _quote_identifier(table_name)
    query = f"SELECT * FROM {identifier}"
    if where_sql:
        query += f" WHERE {where_sql}"
    query += " LIMIT ? OFFSET ?"
    final_params = list(params) + [int(limit), int(offset)]
    with _connect_sqlite(db_path) as conn:
        return pd.read_sql_query(query, conn, params=final_params)


def count_sqlite_filtered_rows(
    db_path: str,
    table_name: str,
    where_sql: str,
    params: list[object],
) -> int:
    identifier = _quote_identifier(table_name)
    query = f"SELECT COUNT(1) FROM {identifier}"
    if where_sql:
        query += f" WHERE {where_sql}"
    with _connect_sqlite(db_path) as conn:
        row = conn.execute(query, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def query_postgres_preview(
    db_url: str,
    table_name: str,
    where_sql: str,
    params: dict[str, object],
    limit: int,
    offset: int,
) -> pd.DataFrame:
    identifier = _quote_identifier(table_name)
    query = f"SELECT * FROM {identifier}"
    if where_sql:
        query += f" WHERE {where_sql}"
    query += " LIMIT :_limit OFFSET :_offset"
    final_params = dict(params)
    final_params["_limit"] = int(limit)
    final_params["_offset"] = int(offset)
    engine = _pg_engine(db_url)
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=final_params)


def count_postgres_filtered_rows(
    db_url: str,
    table_name: str,
    where_sql: str,
    params: dict[str, object],
) -> int:
    identifier = _quote_identifier(table_name)
    query = f"SELECT COUNT(1) FROM {identifier}"
    if where_sql:
        query += f" WHERE {where_sql}"
    engine = _pg_engine(db_url)
    with engine.connect() as conn:
        row = conn.execute(text(query), params).first()
    return int(row[0]) if row and row[0] is not None else 0


def _openai_api_key() -> str:
    candidates: list[str | None] = []
    try:
        app_cfg = st.secrets.get("app", {})
        candidates.append(app_cfg.get("OPENAI_API_KEY"))
    except Exception:
        pass
    try:
        candidates.append(st.secrets.get("OPENAI_API_KEY"))
    except Exception:
        pass
    candidates.append(os.environ.get("OPENAI_API_KEY"))

    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return ""


def _openai_model_name() -> str:
    candidates: list[str | None] = []
    try:
        app_cfg = st.secrets.get("app", {})
        candidates.append(app_cfg.get("OPENAI_MODEL"))
        candidates.append(app_cfg.get("OPENAI_CHAT_MODEL"))
    except Exception:
        pass
    try:
        candidates.append(st.secrets.get("OPENAI_MODEL"))
    except Exception:
        pass
    candidates.append(os.environ.get("OPENAI_MODEL"))

    for raw in candidates:
        if raw and str(raw).strip():
            return str(raw).strip()
    return "gpt-4o-mini"


def _call_openai_chat(
    *,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float,
    max_tokens: int,
) -> str:
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": messages,
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        },
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI no devolvio contenido.")
    content = choices[0].get("message", {}).get("content", "")
    if isinstance(content, list):
        out = []
        for item in content:
            if isinstance(item, dict):
                out.append(str(item.get("text", "")))
            else:
                out.append(str(item))
        return "".join(out).strip()
    return str(content).strip()


def _strip_code_fences(value: str) -> str:
    text_value = (value or "").strip()
    if text_value.startswith("```"):
        text_value = re.sub(r"^```[a-zA-Z]*\s*", "", text_value)
        text_value = re.sub(r"\s*```$", "", text_value)
    return text_value.strip()


def _extract_sql_payload(raw_response: str) -> tuple[str, str]:
    text_value = _strip_code_fences(raw_response)
    if not text_value:
        return "", ""

    possible_json_blocks = [text_value]
    possible_json_blocks.extend(re.findall(r"\{.*\}", text_value, flags=re.DOTALL))

    for block in possible_json_blocks:
        try:
            parsed = json.loads(block)
        except Exception:
            continue
        if isinstance(parsed, dict):
            sql_value = str(parsed.get("sql") or parsed.get("query") or "").strip()
            analysis_value = str(
                parsed.get("analysis")
                or parsed.get("brief")
                or parsed.get("explanation")
                or ""
            ).strip()
            if sql_value:
                return sql_value, analysis_value

    # Fallback: intenta extraer llaves "sql"/"analysis" aunque el JSON venga malformado.
    sql_key_match = re.search(
        r'"sql"\s*:\s*"((?:\\.|[^"\\])*)"',
        text_value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if sql_key_match:
        sql_raw = sql_key_match.group(1)
        try:
            sql_value = json.loads(f'"{sql_raw}"')
        except Exception:
            sql_value = (
                sql_raw.replace('\\"', '"')
                .replace("\\n", " ")
                .replace("\\t", " ")
            )

        analysis_value = ""
        analysis_key_match = re.search(
            r'"analysis"\s*:\s*"((?:\\.|[^"\\])*)"',
            text_value,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if analysis_key_match:
            analysis_raw = analysis_key_match.group(1)
            try:
                analysis_value = json.loads(f'"{analysis_raw}"')
            except Exception:
                analysis_value = (
                    analysis_raw.replace('\\"', '"')
                    .replace("\\n", " ")
                    .replace("\\t", " ")
                )

        sql_value = str(sql_value).strip()
        if sql_value:
            return sql_value, str(analysis_value).strip()

    sql_match = re.search(
        r"(WITH\s+.+?SELECT.+|SELECT.+)",
        text_value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if sql_match:
        sql_value = sql_match.group(1).strip()
        # Recorta basura JSON residual si viene pegada al SQL.
        sql_value = re.sub(
            r'"\s*,\s*"analysis"\s*:\s*.+$',
            "",
            sql_value,
            flags=re.IGNORECASE | re.DOTALL,
        )
        sql_value = (
            sql_value.replace('\\"', '"')
            .replace("\\n", " ")
            .replace("\\t", " ")
            .strip()
        )
        return sql_value, ""
    return "", ""


def _normalize_sql_table_name(raw_table: str) -> str:
    value = str(raw_table or "").strip()
    if not value:
        return ""
    cleaned = value.replace('"', "")
    parts = [p.strip() for p in cleaned.split(".") if p.strip()]
    if not parts:
        return ""
    return parts[-1].lower()


def _extract_sql_tables(sql_text: str) -> list[str]:
    non_table_tokens = {
        "lateral",
        "values",
        "unnest",
    }

    def _inside_extract_from(text: str, keyword_pos: int) -> bool:
        # Evita falso positivo en expresiones como EXTRACT(YEAR FROM "fecha_adjudicacion")
        prefix = text[:keyword_pos].lower()
        last_extract = prefix.rfind("extract(")
        if last_extract < 0:
            return False
        last_close = prefix.rfind(")")
        return last_extract > last_close

    pattern = re.compile(
        r"\b(from|join)\s+((?:\"[^\"]+\"|[a-zA-Z0-9_]+)(?:\.(?:\"[^\"]+\"|[a-zA-Z0-9_]+))?)",
        flags=re.IGNORECASE,
    )
    tables: list[str] = []
    for match in pattern.finditer(sql_text):
        keyword = str(match.group(1) or "").lower()
        if keyword == "from" and _inside_extract_from(sql_text, match.start(1)):
            continue

        normalized = _normalize_sql_table_name(match.group(2))
        if normalized and normalized not in non_table_tokens:
            tables.append(normalized)
    return tables


def _extract_cte_names(sql_text: str) -> set[str]:
    """
    Extrae nombres de CTEs para no confundirlos con tablas fisicas permitidas.
    Ejemplo: WITH fecha_adjudicacion AS (...), top_proveedores AS (...)
    """
    lowered = sql_text.lower()
    with_pos = lowered.find("with ")
    if with_pos < 0:
        return set()

    # Tomamos una ventana amplia desde WITH para detectar alias CTE.
    window = sql_text[with_pos : with_pos + 8000]
    pattern = re.compile(
        r"(?:^|\s|,)(\"?[a-zA-Z_][a-zA-Z0-9_]*\"?)\s+as\s*\(",
        flags=re.IGNORECASE,
    )
    names: set[str] = set()
    for match in pattern.finditer(window):
        names.add(_normalize_sql_table_name(match.group(1)))
    return names


def _is_aggregate_sql(sql_text: str) -> bool:
    lowered = f" {sql_text.lower()} "
    return any(
        marker in lowered
        for marker in (" count(", " sum(", " avg(", " min(", " max(", " group by ", " having ")
    )


def _validate_and_prepare_sql(sql_text: str, allowed_tables: list[str]) -> str:
    cleaned = _strip_code_fences(sql_text).strip()
    cleaned = (
        cleaned.replace('\\"', '"')
        .replace("\\n", " ")
        .replace("\\t", " ")
        .strip()
    )
    if not cleaned:
        raise ValueError("No se genero SQL.")
    if len(cleaned) > CHAT_SQL_MAX_CHARS:
        raise ValueError("SQL demasiado largo.")

    if ";" in cleaned:
        first_stmt, rest = cleaned.split(";", 1)
        first_stmt = first_stmt.strip()
        rest_strip = rest.strip()
        if not first_stmt:
            raise ValueError("No se encontro una consulta SQL valida.")
        # Permite basura no-SQL despues del primer ';' (p.ej. campo analysis),
        # pero bloquea si detecta una segunda sentencia SQL.
        if rest_strip and re.search(
            r"\b(select|with|insert|update|delete|drop|alter|truncate|create)\b",
            rest_strip,
            flags=re.IGNORECASE,
        ):
            raise ValueError("Solo se permite una consulta por mensaje.")
        cleaned = first_stmt

    cleaned = cleaned.rstrip(";").strip()

    lowered = cleaned.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Solo se permiten consultas de lectura (SELECT).")

    for keyword in CHAT_FORBIDDEN_SQL_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", lowered):
            raise ValueError(f"Consulta bloqueada por seguridad ({keyword}).")

    referenced_tables = _extract_sql_tables(cleaned)
    if not referenced_tables:
        raise ValueError("La consulta no incluye tablas en FROM/JOIN.")

    allowed_norm = {_normalize_sql_table_name(t) for t in allowed_tables}
    cte_names = _extract_cte_names(cleaned)
    invalid = sorted({t for t in referenced_tables if t not in allowed_norm and t not in cte_names})
    if invalid:
        raise ValueError(
            "La consulta intenta usar tablas no permitidas: " + ", ".join(invalid)
        )

    if not _is_aggregate_sql(cleaned):
        match = re.search(r"\blimit\s+(\d+)\b", cleaned, flags=re.IGNORECASE)
        if match:
            current = int(match.group(1))
            if current > CHAT_MAX_RAW_ROWS:
                cleaned = (
                    cleaned[: match.start(1)]
                    + str(CHAT_MAX_RAW_ROWS)
                    + cleaned[match.end(1) :]
                )
        else:
            cleaned = f"{cleaned} LIMIT {CHAT_MAX_RAW_ROWS}"

    return cleaned


def _run_chat_sql(
    *,
    backend: str,
    db_url: str,
    db_path: str,
    sql_text: str,
) -> pd.DataFrame:
    if backend == "postgres":
        engine = _pg_engine(db_url)
        with engine.connect() as conn:
            return pd.read_sql_query(text(sql_text), conn)

    if not db_path:
        raise RuntimeError("No hay base local disponible para ejecutar el chat.")
    with _connect_sqlite(db_path) as conn:
        return pd.read_sql_query(sql_text, conn)


def _rewrite_postgres_year_extract(sql_text: str) -> str:
    """
    Reescribe EXTRACT(YEAR FROM <expr>) por una version robusta para columnas texto.
    Evita errores como: extract(unknown, text) does not exist.
    """
    if not sql_text:
        return sql_text

    pattern = re.compile(
        r"EXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+?)\s*\)",
        flags=re.IGNORECASE,
    )

    def _replacement(match: re.Match) -> str:
        expr = match.group(1).strip()
        return (
            "CAST(NULLIF(SUBSTRING(CAST("
            + expr
            + " AS TEXT) FROM '[12][09][0-9]{2}'), '') AS INTEGER)"
        )

    rewritten = pattern.sub(_replacement, sql_text)
    # Evita el patron con grupo capturado que devuelve solo 19/20.
    rewritten = rewritten.replace("(19|20)[0-9]{2}", "[12][09][0-9]{2}")
    return rewritten


def _find_matching_paren(text_value: str, open_idx: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    i = open_idx
    while i < len(text_value):
        ch = text_value[i]
        nxt = text_value[i + 1] if i + 1 < len(text_value) else ""

        if in_single:
            if ch == "'" and nxt == "'":
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if ch == '"' and nxt == '"':
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _split_sql_top_level_args(args_text: str) -> list[str]:
    args: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    start = 0
    i = 0
    while i < len(args_text):
        ch = args_text[i]
        nxt = args_text[i + 1] if i + 1 < len(args_text) else ""

        if in_single:
            if ch == "'" and nxt == "'":
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            if ch == '"' and nxt == '"':
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            args.append(args_text[start:i].strip())
            start = i + 1
        i += 1

    tail = args_text[start:].strip()
    if tail:
        args.append(tail)
    return args


def _rewrite_postgres_numeric_nullif_cast(sql_text: str) -> str:
    """
    Convierte expresiones tipo NULLIF(expr,'')::numeric a un cast seguro:
    CASE WHEN expr ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN expr::numeric ELSE 0 END
    para evitar errores por texto sucio en montos.
    """
    if not sql_text:
        return sql_text

    pattern = re.compile(r"nullif\s*\(", flags=re.IGNORECASE)
    out: list[str] = []
    pos = 0

    while True:
        match = pattern.search(sql_text, pos)
        if not match:
            out.append(sql_text[pos:])
            break

        out.append(sql_text[pos : match.start()])
        open_idx = match.end() - 1  # posicion del '(' de NULLIF
        close_idx = _find_matching_paren(sql_text, open_idx)
        if close_idx < 0:
            out.append(sql_text[match.start() :])
            break

        cast_cursor = close_idx + 1
        while cast_cursor < len(sql_text) and sql_text[cast_cursor].isspace():
            cast_cursor += 1

        if not sql_text.startswith("::", cast_cursor):
            out.append(sql_text[match.start() : close_idx + 1])
            pos = close_idx + 1
            continue

        type_cursor = cast_cursor + 2
        while type_cursor < len(sql_text) and sql_text[type_cursor].isspace():
            type_cursor += 1
        type_start = type_cursor
        while type_cursor < len(sql_text) and (
            sql_text[type_cursor].isalnum() or sql_text[type_cursor] == "_"
        ):
            type_cursor += 1
        cast_type = sql_text[type_start:type_cursor].lower()

        if cast_type != "numeric":
            out.append(sql_text[match.start() : type_cursor])
            pos = type_cursor
            continue

        inner = sql_text[open_idx + 1 : close_idx]
        args = _split_sql_top_level_args(inner)
        if len(args) != 2:
            out.append(sql_text[match.start() : type_cursor])
            pos = type_cursor
            continue

        second_arg = args[1].replace(" ", "").lower()
        if second_arg not in {"''", "e''"}:
            out.append(sql_text[match.start() : type_cursor])
            pos = type_cursor
            continue

        value_expr = args[0].strip()
        # Extrae el primer numero valido (soporta miles con coma) y luego castea.
        # Esto evita errores cuando quedan residuos tipo ".2525.00" o tokens sucios.
        token_expr = (
            "(regexp_match(CAST("
            + value_expr
            + " AS TEXT), "
            + "'-?[0-9]{1,3}(?:,[0-9]{3})*(?:\\.[0-9]+)?|-?[0-9]+(?:\\.[0-9]+)?'))[1]"
        )
        safe_expr = (
            "(CASE WHEN "
            + token_expr
            + " IS NOT NULL THEN REPLACE("
            + token_expr
            + ", ',', '')::numeric ELSE 0 END)"
        )
        out.append(safe_expr)
        pos = type_cursor

    return "".join(out)


def _prepare_sql_for_backend(backend: str, sql_text: str) -> str:
    if backend == "postgres":
        rewritten = _rewrite_postgres_year_extract(sql_text)
        rewritten = _rewrite_postgres_numeric_nullif_cast(rewritten)
        return rewritten
    return sql_text


def _extract_year_from_prompt(prompt: str) -> int | None:
    if not prompt:
        return None
    match = re.search(r"\b([12][09][0-9]{2})\b", prompt)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _build_builtin_proponentes_sql(actos_table: str, year: int | None) -> str:
    table_id = _quote_identifier(actos_table)
    values_rows = []
    for idx in range(1, 15):
        values_rows.append(
            f'(ap."Proponente {idx}", ap."Precio Proponente {idx}")'
        )
    values_sql = ",\n            ".join(values_rows)

    year_filter = ""
    if year is not None:
        year_filter = (
            " AND CAST(NULLIF(SUBSTRING(CAST(ap.\"fecha_adjudicacion\" AS TEXT) "
            "FROM '[12][09][0-9]{2}'), '') AS INTEGER) = "
            + str(int(year))
        )

    return f"""
SELECT x.proponente,
       COUNT(*) FILTER (WHERE x.monto > 0) AS num_licitaciones,
       SUM(x.monto) AS total_ganado
FROM (
    SELECT vp.proponente,
           CASE
               WHEN (regexp_match(
                    CAST(vp.precio AS TEXT),
                    '-?[0-9]{{1,3}}(?:,[0-9]{{3}})*(?:\\.[0-9]+)?|-?[0-9]+(?:\\.[0-9]+)?'
               ))[1] IS NOT NULL
               THEN REPLACE(
                    (regexp_match(
                        CAST(vp.precio AS TEXT),
                        '-?[0-9]{{1,3}}(?:,[0-9]{{3}})*(?:\\.[0-9]+)?|-?[0-9]+(?:\\.[0-9]+)?'
                    ))[1],
                    ',',
                    ''
               )::numeric
               ELSE 0
           END AS monto
    FROM {table_id} ap
    CROSS JOIN LATERAL (
        VALUES
            {values_sql}
    ) AS vp(proponente, precio)
    WHERE COALESCE(TRIM(CAST(vp.proponente AS TEXT)), '') <> ''
    {year_filter}
) x
GROUP BY x.proponente
ORDER BY total_ganado DESC, num_licitaciones DESC
LIMIT 50
""".strip()


def _maybe_builtin_sql(prompt: str, backend: str, schema_map: dict[str, list[str]]) -> str:
    if backend != "postgres":
        return ""
    raw_prompt = str(prompt or "")
    lower_prompt = raw_prompt.lower()

    looks_like_proponentes_ranking = (
        ("proponent" in lower_prompt or "proveedor" in lower_prompt)
        and ("licitac" in lower_prompt or "adjudic" in lower_prompt or "ganad" in lower_prompt)
    )
    if not looks_like_proponentes_ranking:
        return ""

    actos_table = ""
    for table_name, cols in schema_map.items():
        normalized = _normalize_sql_table_name(table_name)
        has_core_cols = ("Proponente 1" in cols) and ("Precio Proponente 1" in cols)
        if has_core_cols and (
            normalized in {"actos_publicos", "actos", "panamacompra_actos"} or "acto" in normalized
        ):
            actos_table = table_name
            break

    if not actos_table:
        return ""

    year = _extract_year_from_prompt(raw_prompt)
    return _build_builtin_proponentes_sql(actos_table, year)


def _build_sql_retry_feedback(backend: str, sql_text: str, error: Exception) -> str:
    raw_error = str(error)
    compact_error = raw_error[:1400]

    cast_hint = ""
    if backend == "postgres":
        cast_hint = (
            "Si usas SUM/AVG o aritmetica en columnas de precio/monto textuales, "
            "convierte cada columna con: "
            "COALESCE(NULLIF(regexp_replace(CAST(\"col\" AS TEXT), '[^0-9\\.-]', '', 'g'), '')::numeric, 0). "
            "Si filtras por anio en fecha textual, usa SUBSTRING con patron '[12][09][0-9]{2}' y luego CAST a INTEGER. "
        )
    else:
        cast_hint = (
            "Si usas SUM/AVG o aritmetica en columnas de precio/monto textuales, "
            "convierte cada columna con CAST(REPLACE(REPLACE(CAST(\"col\" AS TEXT), ',', ''), 'B/.', '') AS REAL). "
        )

    return (
        "El SQL anterior fallo al ejecutarse. Corrigelo y devuelve solo SQL valido. "
        + cast_hint
        + f"SQL previo: {sql_text}. Error BD: {compact_error}"
    )


def _format_fallback_answer(question: str, df: pd.DataFrame) -> str:
    if df.empty:
        return (
            "No se encontraron resultados para esa pregunta. "
            "Prueba con otro rango de fechas o con terminos mas especificos."
        )
    cols = ", ".join([str(c) for c in df.columns[:6]])
    return (
        f"Se encontraron {len(df):,} filas para: '{question}'. "
        f"Columnas principales: {cols}."
    )


def _generate_sql_from_question(
    *,
    question: str,
    backend: str,
    schema_map: dict[str, list[str]],
    api_key: str,
    model: str,
    feedback: str = "",
) -> tuple[str, str]:
    sql_dialect = "PostgreSQL" if backend == "postgres" else "SQLite"
    numeric_hint = ""
    if backend == "postgres":
        numeric_hint = (
            "- Si haces sumas/promedios con columnas de monto/precio que puedan venir como texto, "
            "usa COALESCE(NULLIF(regexp_replace(CAST(col AS TEXT), '[^0-9\\.-]', '', 'g'), '')::numeric, 0).\n"
        )
    else:
        numeric_hint = (
            "- Si haces sumas/promedios con columnas de monto/precio que puedan venir como texto, "
            "usa CAST(REPLACE(REPLACE(CAST(col AS TEXT), ',', ''), 'B/.', '') AS REAL).\n"
        )
    schema_lines = []
    for table_name, columns in schema_map.items():
        columns_list = ", ".join(columns[:80])
        schema_lines.append(f"- {table_name}: {columns_list}")
    schema_text = "\n".join(schema_lines)

    system_prompt = (
        "Eres un asistente SQL para analisis de compras publicas. "
        "Debes devolver SOLO JSON valido con dos llaves: "
        '{"sql":"...","analysis":"..."}.\n'
        "Reglas obligatorias:\n"
        f"- Dialecto: {sql_dialect}.\n"
        "- Solo SELECT (lectura). No uses UPDATE/DELETE/INSERT.\n"
        "- Usa solo tablas del esquema entregado.\n"
        "- Si una columna tiene espacios o simbolos, encierrala en comillas dobles.\n"
        "- Si la pregunta no especifica tabla y habla de actos, usa actos_publicos.\n"
        "- Para busqueda de texto en PostgreSQL usa ILIKE.\n"
        "- Si filtras por anio y la fecha puede venir como texto, NO uses EXTRACT directo sobre texto. "
        "Usa: CAST(NULLIF(SUBSTRING(CAST(col AS TEXT) FROM '[12][09][0-9]{2}'), '') AS INTEGER).\n"
        + numeric_hint
    )
    user_prompt = (
        f"Pregunta del usuario:\n{question}\n\n"
        f"Esquema disponible:\n{schema_text}\n\n"
        + (
            f"Error del intento anterior (corrigelo): {feedback}\n\n"
            if feedback
            else ""
        )
        + "Devuelve un SQL correcto para responder la pregunta."
    )

    raw = _call_openai_chat(
        api_key=api_key,
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=700,
    )
    sql_text, analysis_text = _extract_sql_payload(raw)
    if not sql_text:
        raise ValueError("No se pudo extraer SQL de la respuesta del modelo.")
    return sql_text, analysis_text


def _summarize_results_with_openai(
    *,
    question: str,
    sql_text: str,
    df: pd.DataFrame,
    api_key: str,
    model: str,
) -> str:
    if df.empty:
        return (
            "No se encontraron filas para esa consulta. "
            "Puedes probar ampliando rango de fecha o cambiando filtros."
        )

    sample_df = df.head(CHAT_SUMMARY_SAMPLE_ROWS).copy()
    sample_json = sample_df.to_json(orient="records", force_ascii=False)
    user_prompt = (
        "Responde en espanol claro y breve.\n"
        f"Pregunta: {question}\n"
        f"SQL usado: {sql_text}\n"
        f"Filas devueltas: {len(df)}\n"
        f"Muestra JSON (hasta {CHAT_SUMMARY_SAMPLE_ROWS} filas): {sample_json}\n"
        "Incluye hallazgo principal y, si aplica, recomendacion corta."
    )

    return _call_openai_chat(
        api_key=api_key,
        model=model,
        messages=[
            {
                "role": "system",
                "content": "Eres un analista de datos de compras publicas.",
            },
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        max_tokens=450,
    )


def render_panamacompra_ai_chat(
    *,
    backend: str,
    db_url: str,
    db_path: str,
    allowed_tables: list[str],
) -> None:
    st.divider()
    st.subheader("Asistente GPT multi-tabla")
    st.caption(
        "Pregunta en lenguaje natural y el asistente consultara SQL sobre las 3 tablas."
    )

    api_key = _openai_api_key()
    if not api_key:
        st.info("Configura OPENAI_API_KEY en secrets para habilitar este chat.")
        return

    dedup_tables: list[str] = []
    for table in allowed_tables:
        if table and table not in dedup_tables:
            dedup_tables.append(table)
    if not dedup_tables:
        st.warning("No se detectaron tablas habilitadas para el chat.")
        return

    schema_map: dict[str, list[str]] = {}
    for table in dedup_tables:
        try:
            if backend == "postgres":
                cols = list_postgres_columns(db_url, table)
            else:
                cols = list_sqlite_columns(db_path, table)
        except Exception:
            cols = []
        if cols:
            schema_map[table] = cols

    if not schema_map:
        st.warning("No se pudieron leer columnas de las tablas para el chat.")
        return

    st.caption("Tablas habilitadas: " + ", ".join([f"`{t}`" for t in schema_map.keys()]))

    chat_key = "pc_ai_multi_table_chat"
    history: list[dict] = st.session_state.setdefault(chat_key, [])

    clear_col, _ = st.columns([1, 4])
    with clear_col:
        if st.button("Limpiar chat", key="pc_ai_clear_chat"):
            st.session_state[chat_key] = []
            st.rerun()

    for item in history:
        with st.chat_message(item.get("role", "assistant")):
            st.markdown(str(item.get("content", "")))
            if item.get("sql"):
                st.code(str(item["sql"]), language="sql")
            if item.get("rows") is not None:
                st.caption(f"Filas devueltas: {int(item['rows']):,}")
            preview = item.get("preview")
            if isinstance(preview, list) and preview:
                st.dataframe(
                    pd.DataFrame(preview),
                    use_container_width=True,
                    height=260,
                )

    with st.form("pc_ai_chat_form", clear_on_submit=True):
        prompt = st.text_input(
            "Pregunta para el asistente",
            placeholder="Ej: Cual proveedor suma mas monto adjudicado en 2025?",
            label_visibility="collapsed",
            key="pc_ai_chat_prompt_text",
        )
        submit = st.form_submit_button("Enviar")

    prompt = (prompt or "").strip()
    if not submit or not prompt:
        return

    history.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    model_name = _openai_model_name()
    with st.chat_message("assistant"):
        generated_sql = ""
        with st.spinner("Analizando consulta..."):
            try:
                model_hint = ""
                final_sql = ""
                result_df = pd.DataFrame()
                retry_feedback = ""
                builtin_sql = _maybe_builtin_sql(
                    prompt=prompt,
                    backend=backend,
                    schema_map=schema_map,
                )

                if builtin_sql:
                    model_hint = (
                        "Se uso una consulta optimizada interna para ranking de proponentes."
                    )
                    candidate_sql = _validate_and_prepare_sql(
                        builtin_sql, list(schema_map.keys())
                    )
                    candidate_sql_exec = _prepare_sql_for_backend(backend, candidate_sql)
                    result_df = _run_chat_sql(
                        backend=backend,
                        db_url=db_url,
                        db_path=db_path,
                        sql_text=candidate_sql_exec,
                    )
                    final_sql = candidate_sql_exec
                    generated_sql = candidate_sql_exec
                else:
                    for _attempt in range(4):
                        generated_sql, model_hint = _generate_sql_from_question(
                            question=prompt,
                            backend=backend,
                            schema_map=schema_map,
                            api_key=api_key,
                            model=model_name,
                            feedback=retry_feedback,
                        )
                        try:
                            candidate_sql = _validate_and_prepare_sql(
                                generated_sql, list(schema_map.keys())
                            )
                        except ValueError as validation_exc:
                            retry_feedback = (
                                "El SQL fue invalido por validacion de seguridad/esquema. "
                                f"Corrigelo. Detalle: {validation_exc}"
                            )
                            continue

                        candidate_sql_exec = _prepare_sql_for_backend(backend, candidate_sql)
                        try:
                            result_df = _run_chat_sql(
                                backend=backend,
                                db_url=db_url,
                                db_path=db_path,
                                sql_text=candidate_sql_exec,
                            )
                            final_sql = candidate_sql_exec
                            generated_sql = candidate_sql_exec
                            break
                        except Exception as exec_exc:
                            retry_feedback = _build_sql_retry_feedback(
                                backend=backend,
                                sql_text=candidate_sql_exec,
                                error=exec_exc,
                            )
                            generated_sql = candidate_sql_exec
                            continue

                if not final_sql:
                    raise ValueError(
                        retry_feedback
                        or "No fue posible generar una consulta SQL valida."
                    )

                try:
                    answer_text = _summarize_results_with_openai(
                        question=prompt,
                        sql_text=final_sql,
                        df=result_df,
                        api_key=api_key,
                        model=model_name,
                    )
                except Exception:
                    answer_text = _format_fallback_answer(prompt, result_df)

                if model_hint:
                    st.caption(model_hint)
                st.markdown(answer_text)
                st.code(final_sql, language="sql")
                st.caption(f"Filas devueltas: {len(result_df):,}")
                if result_df.empty:
                    st.info("La consulta no devolvio filas.")
                else:
                    st.dataframe(
                        result_df.head(CHAT_MAX_DISPLAY_ROWS),
                        use_container_width=True,
                        height=320,
                    )

                history.append(
                    {
                        "role": "assistant",
                        "content": answer_text,
                        "sql": final_sql,
                        "rows": int(len(result_df)),
                        "preview": result_df.head(40).to_dict(orient="records"),
                    }
                )
            except Exception as exc:
                error_msg = f"No pude procesar la consulta: {exc}"
                st.error(error_msg)
                if generated_sql:
                    st.caption("SQL propuesto (no ejecutado):")
                    st.code(generated_sql, language="sql")
                history.append(
                    {
                        "role": "assistant",
                        "content": error_msg,
                        "sql": generated_sql if generated_sql else None,
                    }
                )

    if len(history) > CHAT_HISTORY_LIMIT:
        st.session_state[chat_key] = history[-CHAT_HISTORY_LIMIT:]


def _normalize_drive_file_id(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        return value

    parsed = urlparse(value)
    qs = parse_qs(parsed.query)
    if "id" in qs and qs["id"]:
        return qs["id"][0]

    # /file/d/<id>/view
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", parsed.path)
    if match:
        return match.group(1)
    return value


@st.cache_data(ttl=600)
def load_drive_excel(file_id: str) -> pd.DataFrame:
    fid = _normalize_drive_file_id(file_id)
    if not fid:
        return pd.DataFrame()

    drive = get_drive_delegated()
    if drive is None:
        raise RuntimeError("No se pudo inicializar el cliente de Google Drive.")

    try:
        meta = drive.files().get(
            fileId=fid,
            fields="id,name,mimeType",
            supportsAllDrives=True,
        ).execute()
    except HttpError as exc:
        raise RuntimeError(
            f"Archivo no encontrado o sin permisos en Drive. ID usado: {fid}. "
            "Verifica el ID y comparte el archivo con la cuenta de servicio."
        ) from exc

    mime_type = str(meta.get("mimeType", ""))
    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = drive.files().export_media(
            fileId=fid,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = drive.files().get_media(fileId=fid, supportsAllDrives=True)

    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_excel(fh)


def _filter_dataframe_text(df: pd.DataFrame, raw_search: str, mode: str) -> pd.DataFrame:
    search_terms = _split_search_terms(raw_search)
    if df.empty or not search_terms:
        return df

    lowered = df.fillna("").astype(str).apply(lambda s: s.str.lower())
    masks: list[pd.Series] = []

    for term in search_terms:
        term_l = term.lower()
        term_mask = lowered.apply(
            lambda col: col.str.contains(term_l, regex=False, na=False)
        ).any(axis=1)
        masks.append(term_mask)

    if not masks:
        return df

    combined = masks[0]
    for m in masks[1:]:
        if mode == "AND":
            combined = combined & m
        else:
            combined = combined | m
    return df.loc[combined].copy()


def _normalize_search_value(value: object, *, strip_accents: bool) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if strip_accents:
        text = "".join(
            ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
        )
    return text


def _parse_advanced_search_query(raw_query: str) -> tuple[list[str], list[str], list[str]]:
    raw = (raw_query or "").strip()
    if not raw:
        return [], [], []

    phrases = [p.strip() for p in re.findall(r'"([^"]+)"', raw) if p.strip()]
    remainder = re.sub(r'"[^"]+"', " ", raw)

    include_terms: list[str] = []
    exclude_terms: list[str] = []
    for token in re.split(r"[\s,;]+", remainder):
        t = token.strip()
        if not t:
            continue
        if t.startswith("-") and len(t) > 1:
            exclude_terms.append(t[1:])
        else:
            include_terms.append(t)

    return include_terms, phrases, exclude_terms


def _apply_advanced_text_search(
    df: pd.DataFrame,
    *,
    raw_query: str,
    mode: str,
    target_column: str | None,
    ignore_accents: bool,
) -> tuple[pd.DataFrame, pd.Series]:
    if df.empty or not raw_query.strip():
        return df, pd.Series(0, index=df.index, dtype="int64")

    search_columns = [target_column] if target_column and target_column in df.columns else list(df.columns)
    if not search_columns:
        return df, pd.Series(0, index=df.index, dtype="int64")

    include_terms, exact_phrases, exclude_terms = _parse_advanced_search_query(raw_query)
    positive_terms = include_terms + exact_phrases
    if not positive_terms and not exclude_terms:
        return df, pd.Series(0, index=df.index, dtype="int64")

    normalized = df[search_columns].fillna("").astype(str).apply(
        lambda col: col.map(lambda value: _normalize_search_value(value, strip_accents=ignore_accents))
    )

    score = pd.Series(0, index=df.index, dtype="int64")
    positive_masks: list[pd.Series] = []

    for term in positive_terms:
        norm_term = _normalize_search_value(term, strip_accents=ignore_accents)
        if not norm_term:
            continue
        term_hits_by_col = normalized.apply(
            lambda col: col.str.contains(norm_term, regex=False, na=False)
        )
        term_mask = term_hits_by_col.any(axis=1)
        positive_masks.append(term_mask)
        # Relevancia = cuantas columnas coinciden por termino (frases pesan un poco mas).
        weight = 2 if term in exact_phrases else 1
        score = score + (term_hits_by_col.sum(axis=1) * weight).astype("int64")

    if positive_masks:
        combined = positive_masks[0]
        for m in positive_masks[1:]:
            if mode == "AND":
                combined = combined & m
            else:
                combined = combined | m
    else:
        combined = pd.Series(True, index=df.index)

    for term in exclude_terms:
        norm_term = _normalize_search_value(term, strip_accents=ignore_accents)
        if not norm_term:
            continue
        excluded = normalized.apply(
            lambda col: col.str.contains(norm_term, regex=False, na=False)
        ).any(axis=1)
        combined = combined & (~excluded)

    filtered = df.loc[combined].copy()
    return filtered, score.loc[filtered.index]


def _find_reference_table_name(
    *,
    backend: str,
    db_url: str,
    db_path_str: str,
    candidates: list[str],
) -> str:
    if not candidates:
        return ""

    if backend == "postgres":
        available = list_postgres_tables(db_url)
    else:
        available = list_sqlite_tables(db_path_str)

    # Respeta prioridad del caller: primer candidato existente gana.
    available_map = {str(table).strip().lower(): str(table) for table in available}
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key and key in available_map:
            return available_map[key]
    return ""


def render_db_reference_panel(
    *,
    title: str,
    key_prefix: str,
    backend: str,
    db_url: str,
    db_path_str: str,
    table_name: str,
    show_header: bool = True,
) -> None:
    if show_header:
        st.subheader(title)
    st.caption(f"Fuente: tabla `{table_name}` ({'Supabase' if backend == 'postgres' else 'SQLite'})")

    try:
        if backend == "postgres":
            table_columns = list_postgres_columns(db_url, table_name)
        else:
            table_columns = list_sqlite_columns(db_path_str, table_name)
    except Exception as exc:
        st.error(f"No se pudieron listar columnas de {table_name}: {exc}")
        return

    search_text = st.text_input(
        f"Buscar en {title}",
        key=f"{key_prefix}_search",
        placeholder="Palabras separadas por espacio, coma o punto y coma",
    )
    mode = st.radio(
        "Modo de busqueda",
        options=["OR", "AND"],
        horizontal=True,
        key=f"{key_prefix}_mode",
    )
    page_size = st.slider(
        "Filas por pagina",
        min_value=50,
        max_value=5000,
        value=300,
        step=50,
        key=f"{key_prefix}_page_size",
    )

    search_terms = _split_search_terms(search_text)
    where_sql, query_params = _build_sql_conditions(
        backend=backend,
        columns=table_columns,
        search_terms=search_terms,
        search_mode=mode,
        filters=[],
        filters_mode="AND",
        combine_mode="AND",
    )

    try:
        if backend == "postgres":
            total = count_postgres_filtered_rows(db_url, table_name, where_sql, query_params)
        else:
            total = count_sqlite_filtered_rows(db_path_str, table_name, where_sql, query_params)
    except Exception as exc:
        st.error(f"No se pudo contar registros de {table_name}: {exc}")
        return

    show_all_default = key_prefix.startswith("pc_fichas")
    show_all_rows = st.toggle(
        "Mostrar todas las filas en una sola pagina",
        value=show_all_default,
        key=f"{key_prefix}_show_all",
    )

    if show_all_rows:
        effective_page_size = max(1, int(total))
        page = 1
        total_pages = 1
        offset = 0
    else:
        effective_page_size = int(page_size)
        total_pages = max(1, math.ceil(total / max(1, effective_page_size)))
        page = st.number_input(
            "Pagina",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key=f"{key_prefix}_page",
        )
        offset = (int(page) - 1) * int(effective_page_size)

    try:
        if backend == "postgres":
            page_df = query_postgres_preview(
                db_url, table_name, where_sql, query_params, int(effective_page_size), int(offset)
            )
        else:
            page_df = query_sqlite_preview(
                db_path_str, table_name, where_sql, query_params, int(effective_page_size), int(offset)
            )
    except Exception as exc:
        st.error(f"No se pudo consultar {table_name}: {exc}")
        return

    page_view, money_cfg = _prepare_money_columns_for_sorting(page_df)
    st.dataframe(
        page_view,
        use_container_width=True,
        height=420,
        column_config=money_cfg,
    )
    st.caption(
        f"Coincidencias: {total:,}. Pagina {int(page)} de {total_pages}. "
        f"Mostrando hasta {effective_page_size if not show_all_rows else total:,} filas."
    )


def render_drive_reference_panel(
    *,
    title: str,
    file_id: str,
    key_prefix: str,
    show_header: bool = True,
) -> None:
    if show_header:
        st.subheader(title)
    if not file_id:
        st.info("No hay archivo configurado para este cuadro.")
        return

    try:
        df = load_drive_excel(file_id)
    except Exception as exc:
        st.error(f"No se pudo cargar '{title}': {exc}")
        return

    if df.empty:
        st.info("El archivo no tiene datos.")
        return

    st.caption("Fuente: archivo en Google Drive")

    search_text = st.text_input(
        f"Buscar en {title}",
        key=f"{key_prefix}_search",
        placeholder="Palabras separadas por espacio, coma o punto y coma",
    )
    mode = st.radio(
        "Modo de busqueda",
        options=["OR", "AND"],
        horizontal=True,
        key=f"{key_prefix}_mode",
    )

    filtered = _filter_dataframe_text(df, search_text, mode)

    page_size = st.slider(
        "Filas por pagina",
        min_value=50,
        max_value=2000,
        value=200,
        step=50,
        key=f"{key_prefix}_page_size",
    )
    total = len(filtered)
    show_all_default = key_prefix.startswith("pc_fichas")
    show_all_rows = st.toggle(
        "Mostrar todas las filas en una sola pagina",
        value=show_all_default,
        key=f"{key_prefix}_show_all",
    )
    if show_all_rows:
        page = 1
        total_pages = 1
        start = 0
        end = total
        effective_page_size = max(1, total)
    else:
        effective_page_size = int(page_size)
        total_pages = max(1, math.ceil(total / max(1, effective_page_size)))
        page = st.number_input(
            "Pagina",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key=f"{key_prefix}_page",
        )
        start = (int(page) - 1) * int(effective_page_size)
        end = start + int(effective_page_size)
    page_df = filtered.iloc[start:end].copy()

    page_view, money_cfg = _prepare_money_columns_for_sorting(page_df)
    st.dataframe(
        page_view,
        use_container_width=True,
        height=420,
        column_config=money_cfg,
    )
    st.caption(
        f"Coincidencias: {total:,}. Pagina {int(page)} de {total_pages}. "
        f"Mostrando hasta {effective_page_size if not show_all_rows else total:,} filas."
    )


def _normalize_column_key(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )
    text = re.sub(r"\s+", " ", text)
    return text


def _resolve_column_by_alias(columns: list[str], aliases: list[str]) -> str:
    if not columns:
        return ""
    normalized = {_normalize_column_key(col): col for col in columns}
    alias_norm = [_normalize_column_key(alias) for alias in aliases if alias]

    for alias in alias_norm:
        if alias in normalized:
            return normalized[alias]

    for alias in alias_norm:
        for normalized_col, original_col in normalized.items():
            if alias and alias in normalized_col:
                return original_col
    return ""


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"nan", "none", "null", "n/a", "<na>"}:
        return ""
    return text


def _normalize_ficha_token(value: object) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return ""
    normalized = digits.lstrip("0")
    return normalized or "0"


def _extract_ficha_tokens(value: object) -> list[str]:
    raw = _clean_text(value)
    if not raw:
        return []
    normalized = _normalize_column_key(raw)
    if normalized in {"no detectada", "sin ficha", "no detectado"}:
        return []

    tokens = re.findall(r"\d{3,}", raw)
    seen: set[str] = set()
    out: list[str] = []
    for token in tokens:
        cleaned = _normalize_ficha_token(token)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _coerce_ct_label(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "No"
    if isinstance(value, bool):
        return "Si" if value else "No"

    text = _clean_text(value)
    if not text:
        return "No"
    norm = _normalize_column_key(text)
    if norm in {"si", "s", "true", "1", "x", "con ct", "ct"}:
        return "Si"
    return "No"


def _coerce_registro_sanitario_label(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "No"
    if isinstance(value, bool):
        return "Si" if value else "No"

    text = _clean_text(value)
    if not text:
        return "No"
    norm = _normalize_column_key(text)
    if norm.startswith("si"):
        return "Si"
    if norm in {"true", "1", "x", "con registro sanitario", "con registro"}:
        return "Si"
    return "No"


@st.cache_data(ttl=300)
def _load_table_subset(
    backend: str,
    db_url: str,
    db_path_str: str,
    table_name: str,
    columns: tuple[str, ...],
    where_sql: str,
) -> pd.DataFrame:
    if not table_name or not columns:
        return pd.DataFrame()

    selected = [str(col) for col in columns if str(col).strip()]
    if not selected:
        return pd.DataFrame()

    quoted_cols = ", ".join(_quote_identifier(col) for col in selected)
    table_id = _quote_identifier(table_name)
    query = f"SELECT {quoted_cols} FROM {table_id}"
    if where_sql.strip():
        query += f" {where_sql.strip()}"

    if backend == "postgres":
        engine = _pg_engine(db_url)
        with engine.connect() as conn:
            return pd.read_sql_query(text(query), conn)

    with _connect_sqlite(db_path_str) as conn:
        return pd.read_sql_query(query, conn)


@st.cache_data(ttl=300)
def _build_prospeccion_rir_dataframe(
    backend: str,
    db_url: str,
    db_path_str: str,
    actos_table: str,
    fichas_table: str,
    fichas_drive_file_id: str = "",
    meta_source_mode: str = "auto",
) -> pd.DataFrame:
    if not actos_table:
        return pd.DataFrame()

    try:
        if backend == "postgres":
            actos_columns = list_postgres_columns(db_url, actos_table)
        else:
            actos_columns = list_sqlite_columns(db_path_str, actos_table)
    except Exception:
        return pd.DataFrame()

    ficha_col = _resolve_column_by_alias(
        actos_columns,
        ["ficha_detectada", "ficha detectada", "ficha", "numero de ficha"],
    )
    if not ficha_col:
        return pd.DataFrame()

    precio_col = _resolve_column_by_alias(
        actos_columns,
        ["precio_referencia", "precio referencia", "monto referencia"],
    )
    enlace_col = _resolve_column_by_alias(
        actos_columns,
        ["enlace", "url", "link"],
    )
    ganador_col = _resolve_column_by_alias(
        actos_columns,
        ["razon_social", "razon social", "adjudicatario", "proveedor ganador"],
    )

    proponente_cols = [
        col
        for col in actos_columns
        if re.fullmatch(r"proponente\s+\d+", _normalize_column_key(col))
    ]

    selected_cols = [ficha_col]
    for candidate in [precio_col, enlace_col, ganador_col]:
        if candidate and candidate not in selected_cols:
            selected_cols.append(candidate)
    for candidate in proponente_cols:
        if candidate not in selected_cols:
            selected_cols.append(candidate)

    ficha_q = _quote_identifier(ficha_col)
    where_sql = (
        f"WHERE {ficha_q} IS NOT NULL "
        f"AND TRIM(CAST({ficha_q} AS TEXT)) <> '' "
        f"AND LOWER(TRIM(CAST({ficha_q} AS TEXT))) NOT IN ('no detectada', 'sin ficha')"
    )
    try:
        actos_df = _load_table_subset(
            backend,
            db_url,
            db_path_str,
            actos_table,
            tuple(selected_cols),
            where_sql,
        )
    except Exception:
        return pd.DataFrame()
    if actos_df.empty:
        return pd.DataFrame()

    fichas_meta: dict[str, dict[str, str]] = {}

    def _merge_fichas_meta_from_dataframe(
        ficha_meta_df: pd.DataFrame,
        *,
        ficha_num_col: str,
        ficha_name_col: str,
        ficha_ct_col: str,
        ficha_rs_col: str,
        ficha_link_col: str,
        ficha_class_col: str,
    ) -> None:
        if ficha_meta_df.empty or not ficha_num_col:
            return
        for _, meta_row in ficha_meta_df.iterrows():
            tokens = _extract_ficha_tokens(meta_row.get(ficha_num_col))
            if not tokens:
                continue
            payload = {
                "nombre": _clean_text(meta_row.get(ficha_name_col)) if ficha_name_col else "",
                "tiene_ct": _coerce_ct_label(meta_row.get(ficha_ct_col)) if ficha_ct_col else "No",
                "registro_sanitario": (
                    _coerce_registro_sanitario_label(meta_row.get(ficha_rs_col)) if ficha_rs_col else "No"
                ),
                "enlace_minsa": _clean_text(meta_row.get(ficha_link_col)) if ficha_link_col else "",
                "clase": _clean_text(meta_row.get(ficha_class_col)) if ficha_class_col else "",
            }
            for token in tokens:
                current = fichas_meta.setdefault(
                    token,
                    {
                        "nombre": "",
                        "tiene_ct": "No",
                        "registro_sanitario": "No",
                        "enlace_minsa": "",
                        "clase": "",
                    },
                )
                if payload["nombre"] and not current["nombre"]:
                    current["nombre"] = payload["nombre"]
                if payload["tiene_ct"] == "Si":
                    current["tiene_ct"] = payload["tiene_ct"]
                if payload["registro_sanitario"] == "Si":
                    current["registro_sanitario"] = payload["registro_sanitario"]
                if payload["enlace_minsa"] and not current["enlace_minsa"]:
                    current["enlace_minsa"] = payload["enlace_minsa"]
                if payload["clase"] and not current["clase"]:
                    current["clase"] = payload["clase"]

    use_drive_meta = meta_source_mode in {"auto", "drive"}
    use_db_meta = meta_source_mode in {"auto", "db"}

    # 1) Fuente prioritaria: archivo de fichas (Drive), p.ej. fichas_ctni_con_enlace.xlsx
    if use_drive_meta and fichas_drive_file_id:
        try:
            drive_fichas_df = load_drive_excel(fichas_drive_file_id)
        except Exception:
            drive_fichas_df = pd.DataFrame()

        if not drive_fichas_df.empty:
            drive_fichas_columns = list(drive_fichas_df.columns)
            drive_num_col = _resolve_column_by_alias(
                drive_fichas_columns,
                ["ficha", "numero ficha", "numero_ficha", "n ficha", "ficha_tecnica", "codigo ficha", "id ficha"],
            )
            drive_name_col = _resolve_column_by_alias(
                drive_fichas_columns,
                ["nombre ficha", "nombre", "descripcion", "detalle", "denominacion"],
            )
            drive_ct_col = _resolve_column_by_alias(
                drive_fichas_columns,
                ["tiene ct", "con ct", "ct", "criterio tecnico", "criterio"],
            )
            drive_rs_col = _resolve_column_by_alias(
                drive_fichas_columns,
                ["registro sanitario", "registro_sanitario", "reg sanitario"],
            )
            drive_link_col = _resolve_column_by_alias(
                drive_fichas_columns,
                [
                    "enlace_ficha_tecnica",
                    "enlace ficha tecnica",
                    "enlace minsa",
                    "link minsa",
                    "url minsa",
                    "enlace",
                    "url",
                ],
            )
            drive_class_col = _resolve_column_by_alias(
                drive_fichas_columns,
                ["clase", "categoria", "clasificacion", "tipo"],
            )
            _merge_fichas_meta_from_dataframe(
                drive_fichas_df,
                ficha_num_col=drive_num_col,
                ficha_name_col=drive_name_col,
                ficha_ct_col=drive_ct_col,
                ficha_rs_col=drive_rs_col,
                ficha_link_col=drive_link_col,
                ficha_class_col=drive_class_col,
            )

    # 2) Fuente secundaria: tabla DB de fichas (completa vacíos si faltan)
    if use_db_meta and fichas_table:
        try:
            if backend == "postgres":
                fichas_columns = list_postgres_columns(db_url, fichas_table)
            else:
                fichas_columns = list_sqlite_columns(db_path_str, fichas_table)
        except Exception:
            fichas_columns = []

        ficha_num_col = _resolve_column_by_alias(
            fichas_columns,
            ["ficha", "numero ficha", "ficha_tecnica", "codigo ficha", "id ficha"],
        )
        ficha_name_col = _resolve_column_by_alias(
            fichas_columns,
            ["nombre ficha", "nombre", "descripcion", "detalle", "denominacion"],
        )
        ficha_ct_col = _resolve_column_by_alias(
            fichas_columns,
            ["tiene ct", "con ct", "ct", "criterio tecnico", "criterio"],
        )
        ficha_rs_col = _resolve_column_by_alias(
            fichas_columns,
            ["registro sanitario", "registro_sanitario", "reg sanitario"],
        )
        ficha_link_col = _resolve_column_by_alias(
            fichas_columns,
            [
                "enlace_ficha_tecnica",
                "enlace ficha tecnica",
                "enlace minsa",
                "link minsa",
                "url minsa",
                "enlace",
                "url",
            ],
        )
        ficha_class_col = _resolve_column_by_alias(
            fichas_columns,
            ["clase", "categoria", "clasificacion", "tipo"],
        )

        ficha_meta_cols = [
            col
            for col in [ficha_num_col, ficha_name_col, ficha_ct_col, ficha_rs_col, ficha_link_col, ficha_class_col]
            if col
        ]
        if ficha_num_col and ficha_meta_cols:
            try:
                ficha_meta_df = _load_table_subset(
                    backend,
                    db_url,
                    db_path_str,
                    fichas_table,
                    tuple(dict.fromkeys(ficha_meta_cols)),
                    "",
                )
            except Exception:
                ficha_meta_df = pd.DataFrame()
            _merge_fichas_meta_from_dataframe(
                ficha_meta_df,
                ficha_num_col=ficha_num_col,
                ficha_name_col=ficha_name_col,
                ficha_ct_col=ficha_ct_col,
                ficha_rs_col=ficha_rs_col,
                ficha_link_col=ficha_link_col,
                ficha_class_col=ficha_class_col,
            )

    col_index = {col: idx for idx, col in enumerate(selected_cols)}
    idx_ficha = col_index[ficha_col]
    idx_precio = col_index.get(precio_col, -1)
    idx_enlace = col_index.get(enlace_col, -1)
    idx_ganador = col_index.get(ganador_col, -1)
    idx_proponentes = [col_index[c] for c in proponente_cols if c in col_index]

    stats: dict[str, dict[str, object]] = {}

    def _state(ficha_token: str) -> dict[str, object]:
        if ficha_token not in stats:
            stats[ficha_token] = {
                "actos_presentes": 0,
                "actos_unicos": 0,
                "monto_unicos": 0.0,
                "links": [],
                "proponentes": set(),
                "ganadores": Counter(),
                "ganadores_unicos": Counter(),
            }
        return stats[ficha_token]

    for values in actos_df[selected_cols].itertuples(index=False, name=None):
        fichas = _extract_ficha_tokens(values[idx_ficha])
        if not fichas:
            continue

        unique_ficha = fichas[0] if len(fichas) == 1 else ""
        precio_ref = _parse_money_value(values[idx_precio]) if idx_precio >= 0 else None
        enlace = _clean_text(values[idx_enlace]) if idx_enlace >= 0 else ""
        ganador = _clean_text(values[idx_ganador]) if idx_ganador >= 0 else ""

        proponentes_row = set()
        for idx in idx_proponentes:
            proponente = _clean_text(values[idx])
            if proponente:
                proponentes_row.add(proponente)

        for ficha_token in fichas:
            state = _state(ficha_token)
            state["actos_presentes"] = int(state["actos_presentes"]) + 1
            if enlace:
                sort_monto = float(precio_ref) if precio_ref is not None else -1.0
                state["links"].append((sort_monto, enlace))
            if ganador:
                state["ganadores"][ganador] += 1
            if proponentes_row:
                state["proponentes"].update(proponentes_row)

            if unique_ficha and ficha_token == unique_ficha:
                state["actos_unicos"] = int(state["actos_unicos"]) + 1
                if precio_ref is not None:
                    state["monto_unicos"] = float(state["monto_unicos"]) + float(precio_ref)
                if ganador:
                    state["ganadores_unicos"][ganador] += 1

    rows: list[dict[str, object]] = []
    for ficha_token, state in stats.items():
        actos_presentes = int(state["actos_presentes"])
        actos_unicos = int(state["actos_unicos"])
        monto_unicos = float(state["monto_unicos"])
        proponentes_distintos = len(state["proponentes"])
        ganadores_sorted = sorted(
            state["ganadores"].items(),
            key=lambda item: (-item[1], item[0].lower()),
        )

        def _winner_text(rank: int) -> tuple[str, int]:
            if len(ganadores_sorted) < rank:
                return "—", 0
            name, count = ganadores_sorted[rank - 1]
            return f"{name} ({count})", int(count)

        top1_text, top1_count = _winner_text(1)
        top2_text, top2_count = _winner_text(2)

        top1_unique = 0
        top2_unique = 0
        if len(ganadores_sorted) >= 1:
            top1_unique = int(state["ganadores_unicos"].get(ganadores_sorted[0][0], 0))
        if len(ganadores_sorted) >= 2:
            top2_unique = int(state["ganadores_unicos"].get(ganadores_sorted[1][0], 0))

        top1_pct_present = (top1_count / actos_presentes * 100.0) if actos_presentes else 0.0
        top1_pct_unique = (top1_unique / actos_unicos * 100.0) if actos_unicos else 0.0
        top2_pct_present = (top2_count / actos_presentes * 100.0) if actos_presentes else 0.0
        top2_pct_unique = (top2_unique / actos_unicos * 100.0) if actos_unicos else 0.0

        links_sorted = sorted(
            state["links"],
            key=lambda pair: pair[0],
            reverse=True,
        )
        links_unique: list[str] = []
        seen_links: set[str] = set()
        for _, raw_link in links_sorted:
            link = _clean_text(raw_link)
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            links_unique.append(link)

        has_meta = ficha_token in fichas_meta
        meta = fichas_meta.get(
            ficha_token,
            {"nombre": "", "tiene_ct": "No", "registro_sanitario": "No", "enlace_minsa": "", "clase": ""},
        )
        row: dict[str, object] = {
            "Ficha #": ficha_token,
            "Nombre ficha": meta.get("nombre") or f"Ficha {ficha_token}",
            "Actos con ficha": actos_presentes,
            "Actos ficha unica": actos_unicos,
            "Monto total (ficha unica)": monto_unicos,
            "Tiene criterio tecnico": meta.get("tiene_ct") or "No",
            "Registro sanitario": meta.get("registro_sanitario") or "No",
            "Enlace ficha MINSA": meta.get("enlace_minsa") or "",
            "Clase ficha": meta.get("clase") or "",
            "__actos_links__": "\n".join(links_unique),
            "Proponentes distintos": proponentes_distintos,
            "Top 1 ganador": top1_text,
            "% Top 1 (total, unica)": f"{top1_pct_present:.1f}%, {top1_pct_unique:.1f}%",
            "Top 2 ganador": top2_text,
            "% Top 2 (total, unica)": f"{top2_pct_present:.1f}%, {top2_pct_unique:.1f}%",
            "__meta_found__": "Si" if has_meta else "No",
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out = out.sort_values(
        by=["Actos con ficha", "Actos ficha unica", "Monto total (ficha unica)"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    ordered_cols = [
        "Ficha #",
        "Nombre ficha",
        "Actos con ficha",
        "Actos ficha unica",
        "Monto total (ficha unica)",
        "Tiene criterio tecnico",
        "Registro sanitario",
        "Enlace ficha MINSA",
        "Clase ficha",
    ] + [
        "Proponentes distintos",
        "Top 1 ganador",
        "% Top 1 (total, unica)",
        "Top 2 ganador",
        "% Top 2 (total, unica)",
        "__actos_links__",
    ]
    out = out[[col for col in ordered_cols if col in out.columns]]
    return out


def render_prospeccion_rir_panel(
    *,
    backend: str,
    db_url: str,
    db_path_str: str,
    actos_table: str,
    fichas_table: str,
    fichas_drive_file_id: str = "",
    key_prefix: str = "pc_prospeccion_rir",
) -> None:
    if not actos_table:
        st.info("No encontramos tabla de actos para construir Prospeccion RIR.")
        return

    cfg_cols = st.columns([2.2, 1.2, 1.2])
    with cfg_cols[0]:
        meta_source_label = st.selectbox(
            "Fuente metadata fichas",
            options=[
                "Auto (Drive + DB)",
                "Solo Drive",
                "Solo tabla DB",
            ],
            index=0,
            key=f"{key_prefix}_meta_source",
        )
    with cfg_cols[1]:
        st.caption(" ")
        if st.button("Recargar prospeccion", key=f"{key_prefix}_reload"):
            _build_prospeccion_rir_dataframe.clear()
            st.rerun()
    with cfg_cols[2]:
        st.caption(" ")
        show_full_names_inline = st.toggle(
            "Nombres completos",
            value=True,
            key=f"{key_prefix}_show_full_names_inline",
        )

    meta_source_mode = {
        "Auto (Drive + DB)": "auto",
        "Solo Drive": "drive",
        "Solo tabla DB": "db",
    }.get(meta_source_label, "auto")

    try:
        with st.spinner("Construyendo Prospeccion RIR..."):
            df = _build_prospeccion_rir_dataframe(
                backend=backend,
                db_url=db_url,
                db_path_str=db_path_str,
                actos_table=actos_table,
                fichas_table=fichas_table,
                fichas_drive_file_id=fichas_drive_file_id,
                meta_source_mode=meta_source_mode,
            )
    except Exception as exc:
        st.error(f"No se pudo construir Prospeccion RIR: {exc}")
        return

    if df.empty:
        st.info("No hay datos suficientes para construir Prospeccion RIR.")
        return

    def _yn_counts(in_df: pd.DataFrame, column: str) -> tuple[int, int]:
        if in_df.empty or column not in in_df.columns:
            return 0, 0
        normalized = (
            in_df[column]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )
        yes = int((normalized == "si").sum())
        no = int((normalized == "no").sum())
        return yes, no

    def _parse_prospeccion_pct(value: object) -> float | None:
        text = _clean_text(value)
        if not text:
            return None
        first_part = text.split(",")[0].replace("%", "").strip()
        return _parse_money_value(first_part)

    def _sort_prospeccion_dataframe(
        in_df: pd.DataFrame,
        *,
        sort_column: str,
        descending: bool,
    ) -> pd.DataFrame:
        if in_df.empty or sort_column not in in_df.columns:
            return in_df

        out_df = in_df.copy()
        numeric_cols = {
            "Actos con ficha",
            "Actos ficha unica",
            "Monto total (ficha unica)",
            "Proponentes distintos",
        }
        percent_cols = {
            "% Top 1 (total, unica)",
            "% Top 2 (total, unica)",
        }
        text_cols = {
            "Ficha #",
            "Nombre ficha",
            "Top 1 ganador",
            "Top 2 ganador",
            "Tiene criterio tecnico",
            "Registro sanitario",
            "Clase ficha",
        }

        key_col = "__sort_key__"
        if sort_column in numeric_cols:
            out_df[key_col] = _coerce_money_series(out_df[sort_column])
        elif sort_column in percent_cols:
            out_df[key_col] = pd.to_numeric(
                out_df[sort_column].map(_parse_prospeccion_pct),
                errors="coerce",
            )
        elif sort_column in text_cols:
            out_df[key_col] = out_df[sort_column].fillna("").astype(str).str.lower()
        else:
            # Fallback defensivo: intenta numérico y si no, texto.
            numeric_try = _coerce_money_series(out_df[sort_column])
            if numeric_try.notna().any():
                out_df[key_col] = numeric_try
            else:
                out_df[key_col] = out_df[sort_column].fillna("").astype(str).str.lower()

        tie_breakers = ["Monto total (ficha unica)", "Actos con ficha", "Actos ficha unica", "Ficha #"]
        sort_cols = [key_col]
        ascending_flags = [not descending]
        for tie_col in tie_breakers:
            if tie_col in out_df.columns and tie_col != sort_column:
                sort_cols.append(tie_col)
                # Los desempates deben mantener prioridad analítica (montos/actos altos primero).
                ascending_flags.append(False if tie_col != "Ficha #" else True)

        out_df = out_df.sort_values(
            by=sort_cols,
            ascending=ascending_flags,
            kind="mergesort",
            na_position="last",
        ).drop(columns=[key_col], errors="ignore")
        return out_df.reset_index(drop=True)

    search_col_options = [
        "Todas las columnas",
        "Ficha #",
        "Nombre ficha",
        "Tiene criterio tecnico",
        "Registro sanitario",
        "Top 1 ganador",
        "Top 2 ganador",
    ]
    search_col = st.selectbox(
        "Columna de busqueda",
        options=search_col_options,
        key=f"{key_prefix}_search_col",
    )
    search_text = st.text_input(
        "Buscar ficha/proveedor",
        key=f"{key_prefix}_search_text",
        placeholder='Ej: 109491 "insumo laboratorio" -reactivo',
    )
    search_mode = st.radio(
        "Modo de busqueda",
        options=["OR", "AND"],
        horizontal=True,
        key=f"{key_prefix}_search_mode",
    )

    filtered = df.copy()
    if search_text.strip():
        target = None if search_col == "Todas las columnas" else search_col
        filtered, _ = _apply_advanced_text_search(
            filtered,
            raw_query=search_text,
            mode=search_mode,
            target_column=target,
            ignore_accents=True,
        )

    base_meta_si, base_meta_no = _yn_counts(filtered, "__meta_found__")
    base_ct_si, base_ct_no = _yn_counts(filtered, "Tiene criterio tecnico")
    base_rs_si, base_rs_no = _yn_counts(filtered, "Registro sanitario")

    filter_cols = st.columns([1.4, 1.9])
    with filter_cols[0]:
        only_ct_yes = st.toggle(
            "Solo actos con CT = Si",
            value=False,
            key=f"{key_prefix}_only_ct_yes",
        )
    with filter_cols[1]:
        only_without_rs = st.toggle(
            "Solo actos sin registro sanitario",
            value=False,
            key=f"{key_prefix}_only_without_rs",
        )
    if st.button("Restablecer filtros", key=f"{key_prefix}_reset_filters"):
        st.session_state[f"{key_prefix}_only_ct_yes"] = False
        st.session_state[f"{key_prefix}_only_without_rs"] = False
        st.rerun()

    if only_ct_yes and "Tiene criterio tecnico" in filtered.columns:
        filtered = filtered[
            filtered["Tiene criterio tecnico"].fillna("").astype(str).str.strip().str.lower() == "si"
        ].copy()
    if only_without_rs and "Registro sanitario" in filtered.columns:
        filtered = filtered[
            filtered["Registro sanitario"].fillna("").astype(str).str.strip().str.lower() == "no"
        ].copy()

    post_meta_si, post_meta_no = _yn_counts(filtered, "__meta_found__")
    post_ct_si, post_ct_no = _yn_counts(filtered, "Tiene criterio tecnico")
    post_rs_si, post_rs_no = _yn_counts(filtered, "Registro sanitario")
    st.caption(
        "Cobertura metadata fichas (antes filtros -> despues): "
        f"Con metadata {base_meta_si:,}/Sin metadata {base_meta_no:,} -> "
        f"Con metadata {post_meta_si:,}/Sin metadata {post_meta_no:,}"
    )
    st.caption(
        "Conteo CT/RS (antes filtros -> despues): "
        f"CT Si {base_ct_si:,}/No {base_ct_no:,} -> Si {post_ct_si:,}/No {post_ct_no:,} | "
        f"RS Si {base_rs_si:,}/No {base_rs_no:,} -> Si {post_rs_si:,}/No {post_rs_no:,}"
    )

    sort_col_options = [
        "Monto total (ficha unica)",
        "Actos con ficha",
        "Actos ficha unica",
        "Proponentes distintos",
        "Ficha #",
        "Nombre ficha",
        "Top 1 ganador",
        "Top 2 ganador",
        "% Top 1 (total, unica)",
        "% Top 2 (total, unica)",
    ]
    sort_ui_cols = st.columns([2.6, 1.4])
    with sort_ui_cols[0]:
        sort_col = st.selectbox(
            "Orden global por",
            options=[c for c in sort_col_options if c in filtered.columns],
            index=0 if "Monto total (ficha unica)" in filtered.columns else 0,
            key=f"{key_prefix}_sort_col",
        )
    with sort_ui_cols[1]:
        sort_desc = st.toggle(
            "Descendente",
            value=True,
            key=f"{key_prefix}_sort_desc",
        )

    filtered = _sort_prospeccion_dataframe(
        filtered,
        sort_column=sort_col,
        descending=bool(sort_desc),
    )

    page_size = st.slider(
        "Filas por pagina",
        min_value=20,
        max_value=1000,
        value=120,
        step=20,
        key=f"{key_prefix}_page_size",
    )
    total = len(filtered)
    show_all_rows = st.toggle(
        "Mostrar todas las fichas en una sola pagina",
        value=True,
        key=f"{key_prefix}_show_all",
    )
    if show_all_rows:
        page = 1
        total_pages = 1
        start = 0
        end = total
        effective_page_size = max(1, total)
    else:
        effective_page_size = int(page_size)
        total_pages = max(1, math.ceil(total / max(1, effective_page_size)))
        page = st.number_input(
            "Pagina",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key=f"{key_prefix}_page",
        )
        start = (int(page) - 1) * int(effective_page_size)
        end = start + int(effective_page_size)
    page_df = filtered.iloc[start:end].copy()
    page_df, money_cfg = _prepare_money_columns_for_sorting(page_df)
    links_col = "__actos_links__"
    display_df = page_df.drop(
        columns=[links_col, "__meta_found__", "Actos (monto desc)", "actos monto desc"],
        errors="ignore",
    )

    column_config = {
        "Nombre ficha": st.column_config.TextColumn(
            "Nombre ficha",
            width="large",
        ),
        "Enlace ficha MINSA": st.column_config.LinkColumn(
            "Enlace ficha MINSA",
            display_text="MINSA",
        ),
    }
    column_config.update(money_cfg)

    st.dataframe(
        display_df,
        use_container_width=True,
        height=520,
        column_config=column_config,
    )
    st.caption(
        f"Coincidencias: {total:,}. Pagina {int(page)} de {total_pages}. "
        f"Mostrando hasta {effective_page_size if not show_all_rows else total:,} filas. "
        "Porcentaje ganador: % sobre actos con ficha, % sobre actos con ficha unica. "
        "El orden se aplica sobre toda la tabla filtrada antes de paginar."
    )

    if show_full_names_inline and not page_df.empty:
        st.markdown("**Nombres ficha completos (pagina actual):**")
        names_df = (
            page_df[["Ficha #", "Nombre ficha"]]
            .copy()
            .rename(columns={"Ficha #": "Ficha", "Nombre ficha": "Nombre completo"})
        )
        if len(names_df) > 500:
            st.caption(
                f"Mostrando 500 de {len(names_df):,} nombres completos en esta pagina "
                "(para mantener rendimiento)."
            )
            names_df = names_df.head(500)
        st.table(
            names_df
        )

    with st.expander("Ver nombres completos (pagina actual)", expanded=False):
        if page_df.empty:
            st.caption("Sin filas en la pagina actual.")
        else:
            for _, row in page_df.iterrows():
                ficha_id = str(row.get("Ficha #", "") or "").strip()
                full_name = str(row.get("Nombre ficha", "") or "").strip()
                st.markdown(f"- **{ficha_id}**: {full_name}")

    if not page_df.empty and links_col in page_df.columns:
        selector_options = list(range(len(page_df)))
        selected_idx = st.selectbox(
            "Ver enlaces clickeables de la fila",
            options=selector_options,
            key=f"{key_prefix}_links_row_selector",
            format_func=lambda i: (
                f"{page_df.iloc[i].get('Ficha #', 'N/D')} | "
                f"{page_df.iloc[i].get('Nombre ficha', 'Sin nombre')}"
            ),
        )
        selected_name = str(page_df.iloc[int(selected_idx)].get("Nombre ficha", "") or "").strip()
        if selected_name:
            st.markdown(f"**Nombre ficha completo:** {selected_name}")
        raw_links = str(page_df.iloc[int(selected_idx)].get(links_col, "") or "")
        link_list = [u.strip() for u in raw_links.splitlines() if u.strip()]
        if link_list:
            links_md = ", ".join(f"[{idx + 1}]({url})" for idx, url in enumerate(link_list))
            st.markdown(f"**Enlaces actos (monto desc):** {links_md}")
        else:
            st.caption("Sin enlaces disponibles para esta fila.")


PC_CONFIG_OVERRIDE_TTL_SECONDS = 180
PC_CONFIG_NAME_ALIASES = ("name", "bot", "job", "proceso")
PC_CONFIG_DAYS_ALIASES = ("days", "dias", "días")
PC_CONFIG_TIMES_ALIASES = ("times", "horas", "hora", "hours")


def _normalize_job_key(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _resolve_config_column(df: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    if df is None or df.empty:
        return None
    col_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in aliases:
        key = str(candidate).strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _pc_config_overrides() -> dict[str, dict[str, str]]:
    overrides = st.session_state.setdefault("pc_config_overrides", {})
    now = time.time()
    stale_keys = [
        key
        for key, payload in list(overrides.items())
        if now - float(payload.get("ts", now)) > PC_CONFIG_OVERRIDE_TTL_SECONDS
    ]
    for key in stale_keys:
        overrides.pop(key, None)
    return overrides


def _sanitize_config_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _canonicalize_schedule_text(value: str) -> str:
    """
    Normaliza listas separadas por comas/semicolons a un formato consistente.
    Mantiene el orden de entrada y elimina espacios duplicados.
    """
    text = _sanitize_config_value(value)
    if not text:
        return ""
    parts = [p.strip() for p in re.split(r"[;,]+", text) if p.strip()]
    if not parts:
        return ""
    return ", ".join(parts)


def _apply_pc_config_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    name_col = _resolve_config_column(df, PC_CONFIG_NAME_ALIASES)
    if not name_col:
        return df

    overrides = _pc_config_overrides()
    if not overrides:
        return df

    work_df = df.copy()
    name_keys = work_df[name_col].astype(str).str.strip().str.lower()
    days_column = _resolve_config_column(work_df, PC_CONFIG_DAYS_ALIASES)
    times_column = _resolve_config_column(work_df, PC_CONFIG_TIMES_ALIASES)
    resolved_keys: list[str] = []

    for key, payload in list(overrides.items()):
        job_key = _normalize_job_key(key)
        if not job_key:
            resolved_keys.append(key)
            continue

        mask = name_keys == job_key
        if not mask.any():
            resolved_keys.append(key)
            continue

        row_index = mask[mask].index[0]

        if days_column and "days" in payload:
            desired_value = _canonicalize_schedule_text(payload["days"])
            work_df.at[row_index, days_column] = desired_value
            payload["days"] = desired_value

        if times_column and "times" in payload:
            desired_value = _canonicalize_schedule_text(payload["times"])
            work_df.at[row_index, times_column] = desired_value
            payload["times"] = desired_value

    for key in resolved_keys:
        overrides.pop(key, None)

    # Conserva metadatos como los encabezados originales.
    work_df.attrs = dict(df.attrs)
    return work_df


def _manual_sheet_id() -> str | None:
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    manual_id = app_cfg.get("PC_MANUAL_SHEET_ID") if isinstance(app_cfg, dict) else None
    manual_id = manual_id or PC_MANUAL_SHEET_ID
    return manual_id or None


def _pc_config_sheet_id() -> str | None:
    """Obtiene el Sheet ID donde vive pc_config, con fallbacks sensatos."""
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    sheet_id = None
    if isinstance(app_cfg, dict):
        sheet_id = (
            app_cfg.get("PC_CONFIG_SHEET_ID")
            or app_cfg.get("PC_MANUAL_SHEET_ID")
            or app_cfg.get("SHEET_ID")
        )

    return sheet_id or _manual_sheet_id()


def _current_user() -> str:
    for key in ("username", "user", "email", "correo", "name", "nombre"):
        value = st.session_state.get(key)
        if value:
            return str(value)
    return "desconocido"


def append_manual_request(job_name: str, job_label: str, note: str) -> bool:
    sheet_id = _manual_sheet_id()
    if not sheet_id:
        st.error("No hay hoja configurada para registrar ejecuciones manuales.")
        return False

    client = get_gc()
    try:
        sh = client.open_by_key(sheet_id)
    except Exception as exc:
        st.error(f"No se pudo abrir la hoja manual: {exc}")
        return False

    ws = None
    try:
        ws = sh.worksheet(PC_MANUAL_WORKSHEET)
    except Exception:
        try:
            ws = sh.sheet1
        except Exception:
            st.error("No encontramos la pestaña pc_manual en la hoja configurada.")
            return False

    headers = ws.row_values(1)
    cleaned_headers = [h.strip() for h in headers if h.strip()]
    if not cleaned_headers:
        cleaned_headers = ["request_id", "timestamp", "job_name", "job_label", "requested_by", "note"]
        ws.update("A1", [cleaned_headers])

    payload_map = {
        "request_id": uuid.uuid4().hex,
        "timestamp": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "job_name": job_name,
        "job_label": job_label,
        "requested_by": _current_user(),
        "note": note.strip(),
        "status": "pending",
    }

    def _value_for_header(header: str) -> str:
        normalized = header.strip().lower()
        for key, aliases in HEADER_ALIASES.items():
            if normalized == key or normalized in aliases:
                return payload_map.get(key, "")
        return payload_map.get(normalized, "")

    row = [_value_for_header(header) for header in cleaned_headers]
    status_idx = None
    for idx, header in enumerate(cleaned_headers, start=1):
        header_norm = header.strip().lower()
        if header_norm in {"status", "estado"}:
            status_idx = idx
            break

    try:
        append_result = ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as exc:
        st.error(f"No se pudo registrar la ejecución manual: {exc}")
        return False

    if status_idx:
        updated_range = ""
        try:
            updated_range = (
                append_result.get("updates", {}).get("updatedRange", "")
                if isinstance(append_result, dict)
                else ""
            )
        except AttributeError:
            updated_range = ""

        row_number = None
        if updated_range:
            try:
                range_bounds = updated_range.split("!")[-1]
                start_cell = range_bounds.split(":")[0]
                row_number = int("".join(filter(str.isdigit, start_cell)))
            except Exception:
                row_number = None

        if not row_number:
            try:
                row_number = len(ws.col_values(1))
            except Exception:
                row_number = None

        if row_number:
            try:
                ws.update_cell(row_number, status_idx, payload_map.get("status", "pending"))
            except Exception as exc:
                st.warning(f"No se pudo actualizar el estado 'pending' en la fila {row_number}: {exc}")

    return True


@st.cache_data(ttl=180)
def load_pc_state() -> pd.DataFrame:
    """Carga el estado de los jobs PanamáCompra desde Finanzas Operativas."""
    try:
        sheet_id = st.secrets["app"]["SHEET_ID"]
    except Exception:
        return pd.DataFrame()

    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_STATE_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    keep_cols = [
        col
        for col in (
            "job_name",
            "status",
            "started_at",
            "finished_at",
            "duration_display",
            "duration_seconds",
        )
        if col in df.columns
    ]

    if not keep_cols:
        return pd.DataFrame()

    data = df[keep_cols].copy()
    if "job_name" not in data.columns:
        return pd.DataFrame()

    data["job_name"] = data["job_name"].astype(str).str.strip()

    if "status" in data.columns:
        data["status"] = data["status"].astype(str).str.strip()
    else:
        data["status"] = ""
    if "started_at" in data.columns:
        data["__started_ts"] = pd.to_datetime(data["started_at"], errors="coerce")
    else:
        data["__started_ts"] = pd.NaT

    # Conserva el último registro por job.
    data = data.sort_values("__started_ts", ascending=False)
    data = data.drop_duplicates(subset=["job_name"], keep="first")

    order_map = {name: idx for idx, name in enumerate(JOB_NAME_ORDER)}
    data["__order"] = data["job_name"].str.lower().map(order_map).fillna(len(order_map))
    data = data.sort_values(["__order", "__started_ts"], ascending=[True, False]).reset_index(drop=True)
    return data.drop(columns=["__started_ts", "__order"], errors="ignore")


@st.cache_data(ttl=180)
def load_pc_config() -> pd.DataFrame:
    """Obtiene la configuración de programación (días/horas) desde la hoja pc_config."""
    sheet_id = _pc_config_sheet_id()
    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_CONFIG_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    original_columns = list(df.columns)
    df = df.rename(columns=lambda col: str(col).strip().lower())
    df.attrs["__original_columns__"] = original_columns
    return _apply_pc_config_overrides(df)


def _format_pc_datetime(value) -> str:
    if value is None or value == "":
        return "—"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "—"
    return ts.strftime("%d/%m %H:%M")


def _format_pc_duration(row: pd.Series) -> str:
    display = str(row.get("duration_display", "")).strip()
    if display:
        return display
    seconds = row.get("duration_seconds")
    try:
        return f"{float(seconds):.0f} s"
    except Exception:
        return "—"


@st.cache_data(ttl=180)
def _latest_sheet_update_by_job() -> dict[str, str]:
    """Obtiene la fecha mas reciente de actualizacion por job a partir de sus hojas."""

    def _normalize_col(name: str) -> str:
        value = str(name or "").strip().lower()
        value = "".join(
            ch for ch in unicodedata.normalize("NFD", value) if unicodedata.category(ch) != "Mn"
        )
        value = re.sub(r"\s+", " ", value)
        return value

    latest_map: dict[str, str] = {}
    for job_key, sheets in JOB_SOURCE_SHEETS.items():
        latest_ts = pd.NaT
        for sheet_name in sheets:
            try:
                sheet_df = load_df(sheet_name)
            except Exception:
                continue

            if sheet_df is None or sheet_df.empty:
                continue

            update_col = None
            for col in sheet_df.columns:
                if col == ROW_ID_COL:
                    continue
                normalized = _normalize_col(col)
                if (
                    normalized == "fecha de actualizacion"
                    or normalized.startswith("fecha de actualizacion")
                    or ("fecha" in normalized and "actualiz" in normalized)
                ):
                    update_col = col
                    break

            if not update_col:
                continue

            parsed = _parse_sheet_date_column(sheet_df[update_col])
            if parsed.empty:
                continue

            current_max = parsed.max()
            if pd.isna(current_max):
                continue

            if pd.isna(latest_ts) or current_max > latest_ts:
                latest_ts = current_max

        if pd.notna(latest_ts):
            latest_map[job_key] = pd.to_datetime(latest_ts, errors="coerce").isoformat()

    return latest_map


def render_pc_state_cards(
    pc_state_df: pd.DataFrame | None,
    pc_config_df: pd.DataFrame | None,
    suffix: str | None = None,
) -> None:
    """Renderiza tarjetas discretas con el estado de los bots de PanamáCompra."""
    if pc_state_df is None or pc_state_df.empty:
        return

    st.markdown(
        """
<div style="margin-top:14px;margin-bottom:6px;padding:8px 16px;background:rgba(34,52,82,0.55);border-radius:8px;border:1px solid rgba(120,170,255,0.15);font-size:1.12rem;font-weight:600;color:#f4f6fb;letter-spacing:0.01em;">
  🔧 Resumen de últimas actualizaciones y controles
</div>
""",
        unsafe_allow_html=True,
    )
    rows = pc_state_df.drop(columns=[col for col in pc_state_df.columns if col.startswith("__")]).copy()
    if "job_name" in rows.columns:
        rows["__job_key"] = rows["job_name"].astype(str).str.strip().str.lower()
        allowed = {k.lower() for k in JOB_NAME_LABELS.keys()}
        rows = rows[rows["__job_key"].isin(allowed)].copy()
    if rows.empty:
        return

    config_map = {}
    name_col = None
    days_col = None
    times_col = None
    if pc_config_df is not None and not pc_config_df.empty:
        name_col = _resolve_config_column(pc_config_df, PC_CONFIG_NAME_ALIASES)
        if name_col:
            tmp = pc_config_df.copy()
            tmp["__pc_key__"] = tmp[name_col].astype(str).str.strip().str.lower()
            days_col = _resolve_config_column(tmp, PC_CONFIG_DAYS_ALIASES)
            times_col = _resolve_config_column(tmp, PC_CONFIG_TIMES_ALIASES)
            for _, cfg_row in tmp.iterrows():
                config_map[cfg_row["__pc_key__"]] = cfg_row

    buffer = st.session_state.setdefault("pc_config_buffer", {})
    missing_config_jobs: set[str] = set()

    full_key = f"pc_manual_full_{suffix or 'main'}"
    if st.button(
        "▶ Ejecución manual completa (clrir → clv → rir1)",
        key=full_key,
        type="primary",
        use_container_width=True,
    ):
        ok: list[str] = []
        fail: list[str] = []
        for job_key in JOB_NAME_ORDER:
            label = JOB_NAME_LABELS.get(job_key, job_key)
            if append_manual_request(job_key, label, "Ejecución manual completa"):
                ok.append(label)
            else:
                fail.append(label)
        st.session_state["pc_manual_full_feedback"] = {"ok": ok, "fail": fail}

    feedback = st.session_state.pop("pc_manual_full_feedback", None)
    if feedback:
        if feedback.get("fail"):
            st.error(
                "No se pudieron encolar: "
                + ", ".join(feedback["fail"])
            )
        else:
            st.success("Ejecución manual completa encolada correctamente.")

    latest_updates = _latest_sheet_update_by_job()

    for start in range(0, len(rows), 3):
        chunk = rows.iloc[start : start + 3]
        cols = st.columns(len(chunk))
        for col_widget, (_, row) in zip(cols, chunk.iterrows()):
            job_raw = str(row.get("job_name", "")).strip()
            job_label = JOB_NAME_LABELS.get(job_raw.lower(), job_raw or "Job sin nombre")
            status_key = str(row.get("status", "")).strip().lower()
            icon, status_label = STATUS_BADGES.get(status_key, ("⚪", status_key.capitalize() or "Sin dato"))
            started_text = _format_pc_datetime(row.get("started_at"))
            duration_text = _format_pc_duration(row)
            job_key_norm = job_raw.strip().lower()

            sheet_latest_raw = latest_updates.get(job_key_norm, "")
            sheet_latest_ts = pd.to_datetime(sheet_latest_raw, errors="coerce")
            card_started_ts = pd.to_datetime(row.get("started_at"), errors="coerce")
            if pd.notna(sheet_latest_ts):
                if pd.isna(card_started_ts) or (sheet_latest_ts - card_started_ts) >= timedelta(days=1):
                    started_text = _format_pc_datetime(sheet_latest_ts)
                    duration_text = "se ejecuto manual"

            job_key = job_key_norm
            cfg = config_map.get(job_key) if config_map else None
            if cfg is not None:
                cfg_key = _sanitize_config_value(cfg.get("__pc_key__", job_key))
                job_key = cfg_key or job_key
            cfg_name_value = (
                _sanitize_config_value(cfg.get(name_col, job_raw))
                if (cfg is not None and name_col)
                else job_raw
            )
            key_suffix = f"_{suffix}" if suffix else ""
            days_key = f"pc_days_{job_raw.lower()}{key_suffix}"
            times_key = f"pc_times_{job_raw.lower()}{key_suffix}"
            days_value = (
                _canonicalize_schedule_text(cfg.get(days_col, ""))
                if cfg is not None and days_col
                else ""
            )
            times_value = (
                _canonicalize_schedule_text(cfg.get(times_col, ""))
                if cfg is not None and times_col
                else ""
            )

            card = f"""
<div style="border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:12px 14px;margin-top:8px;background-color:rgba(17,20,24,0.35);">
  <div style="font-weight:600;font-size:0.95rem;">{job_label}</div>
  <div style="font-size:0.8rem;color:#9aa0a6;margin-top:4px;">{icon} {status_label}</div>
  <div style="font-size:0.76rem;color:#d6d8dc;margin-top:10px;line-height:1.45;">
    <div>Fecha: {started_text}</div>
    <div>Duración: {duration_text}</div>
  </div>
</div>
"""
            col_widget.markdown(card, unsafe_allow_html=True)

            manual_col = col_widget.container()
            if manual_col.button(
                "▶ Actualización manual",
                key=f"pc_manual_btn_{job_raw.lower()}{key_suffix}",
                use_container_width=True,
            ):
                if append_manual_request(job_raw, job_label, ""):
                    st.session_state["pc_manual_feedback"] = job_label

            fields_col = col_widget.container()
            with fields_col:
                days_input = st.text_input(
                    f"Días programados ({job_label})",
                    value=days_value,
                    key=days_key,
                    placeholder="Días separados por comas",
                    label_visibility="collapsed",
                    help="Días separados por comas",
                )
                times_input = st.text_input(
                    f"Horas programadas ({job_label})",
                    value=times_value,
                    key=times_key,
                    placeholder="Horas separadas por comas",
                    label_visibility="collapsed",
                    help="Horas separadas por comas",
                )


            if cfg is None:
                missing_config_jobs.add(job_label)
                continue

            cleaned_days = _canonicalize_schedule_text(days_input) if days_col else ""
            cleaned_times = _canonicalize_schedule_text(times_input) if times_col else ""

            diff_days = bool(days_col and cleaned_days != days_value)
            diff_times = bool(times_col and cleaned_times != times_value)

            entry = buffer.get(job_key)

            if diff_days or diff_times:
                entry = buffer.setdefault(
                    job_key,
                    {"name": cfg_name_value, "label": job_label},
                )
                if diff_days:
                    entry["days"] = cleaned_days
                elif "days" in entry:
                    entry.pop("days", None)

                if diff_times:
                    entry["times"] = cleaned_times
                elif "times" in entry:
                    entry.pop("times", None)

                entry["ts"] = time.time()
            else:
                if entry:
                    entry.pop("days", None)
                    entry.pop("times", None)
                    if not any(k in entry for k in ("days", "times")):
                        buffer.pop(job_key, None)

            if entry and any(k in entry for k in ("days", "times")):
                col_widget.caption("⬆ Cambios pendientes")
            else:
                col_widget.caption("")

    if missing_config_jobs:
        st.warning(
            "No encontramos filas de configuración para: "
            + ", ".join(sorted(missing_config_jobs))
            + ". Revisa la hoja pc_config."
        )

    feedback_key = "pc_manual_feedback"
    if feedback_key in st.session_state:
        job_label = st.session_state.pop(feedback_key)
        st.success(f"Ejecución manual iniciada, scraping en curso para {job_label} (status: pending).")

    unsaved_jobs = [
        info.get("label") or key
        for key, info in buffer.items()
        if any(k in info for k in ("days", "times"))
    ]

    if unsaved_jobs:
        st.info(
            "Cambios pendientes: "
            + ", ".join(sorted(set(unsaved_jobs)))
        )

    sync_key = f"pc_sync_btn_{suffix or 'main'}"
    if st.button(
        "Sincronizar cambios",
        key=sync_key,
        type="primary",
        disabled=not unsaved_jobs,
        use_container_width=True,
    ):
        st.session_state["pc_config_sync_trigger"] = True

    if st.session_state.pop("pc_config_sync_trigger", False):
        success = sync_pc_config_updates(pc_config_df)
        if success:
            st.success("Se sincronizaron los cambios en pc_config.")
        else:
            st.warning("No se detectaron cambios para sincronizar.")


def sync_pc_config_updates(pc_config_df: pd.DataFrame | None) -> bool:
    """Sincroniza con Google Sheets los cambios almacenados en pc_config_buffer."""

    buffer = st.session_state.get("pc_config_buffer", {}) or {}
    pending: list[dict[str, str]] = []
    for job_key, info in buffer.items():
        payload = {"key": job_key}
        if info.get("name"):
            payload["name"] = info["name"]
        if "days" in info:
            payload["days"] = info["days"]
        if "times" in info:
            payload["times"] = info["times"]
        if len(payload) > 1:
            payload["label"] = info.get("label", job_key)
            pending.append(payload)

    if not pending:
        return False

    sheet_id = _pc_config_sheet_id()
    if not sheet_id:
        st.warning("No hay SHEET_ID configurado para sincronizar pc_config.")
        return False

    client = get_gc()
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(PC_CONFIG_WORKSHEET)
    except Exception as exc:
        st.error(f"No se pudo acceder a la hoja pc_config: {exc}")
        return False

    try:
        df = read_worksheet(client, sheet_id, PC_CONFIG_WORKSHEET)
    except Exception as exc:
        st.error(f"No se pudo leer pc_config: {exc}")
        return False

    if df is None or df.empty:
        st.warning("pc_config está vacía; no se aplicaron cambios.")
        return False

    headers = [str(c) for c in df.columns]
    work_df = df.copy()
    work_df.columns = [str(c).strip().lower() for c in work_df.columns]

    name_col = _resolve_config_column(work_df, PC_CONFIG_NAME_ALIASES)
    if not name_col:
        st.warning("No se encontró ninguna columna equivalente a 'name' en pc_config; no se aplicaron cambios.")
        return False

    days_col = _resolve_config_column(work_df, PC_CONFIG_DAYS_ALIASES)
    times_col = _resolve_config_column(work_df, PC_CONFIG_TIMES_ALIASES)

    def _column_index(lower_name: str | None) -> int | None:
        if not lower_name:
            return None
        target = lower_name.strip().lower()
        for idx, header in enumerate(headers, start=1):
            if str(header).strip().lower() == target:
                return idx
        return None

    col_idx_days = _column_index(days_col)
    col_idx_times = _column_index(times_col)

    overrides = _pc_config_overrides()
    missing_jobs: list[str] = []
    updated_any = False
    now = time.time()

    name_series = work_df[name_col].astype(str).str.strip().str.lower()

    for entry in pending:
        job_key = _normalize_job_key(entry.get("key") or entry.get("name"))
        if not job_key:
            continue

        mask = name_series == job_key
        if not mask.any():
            missing_jobs.append(entry.get("label", job_key))
            continue

        pos = mask[mask].index[0]
        row_number = int(pos) + 2  # +1 header row, +1 porque DataFrame inicia en 0

        override_entry = overrides.setdefault(job_key, {})
        override_entry["name"] = _sanitize_config_value(work_df.at[pos, name_col])

        if col_idx_days and "days" in entry and entry["days"] is not None:
            value = _canonicalize_schedule_text(entry["days"])
            try:
                ws.update_cell(row_number, col_idx_days, value)
            except Exception as exc:
                st.error(f"No se pudo actualizar los días para '{job_key}': {exc}")
                return
            work_df.at[pos, days_col] = value
            override_entry["days"] = value
            updated_any = True

        if col_idx_times and "times" in entry and entry["times"] is not None:
            value = _canonicalize_schedule_text(entry["times"])
            try:
                ws.update_cell(row_number, col_idx_times, value)
            except Exception as exc:
                st.error(f"No se pudo actualizar las horas para '{job_key}': {exc}")
                return
            work_df.at[pos, times_col] = value
            override_entry["times"] = value
            updated_any = True

        override_entry["pending"] = False
        override_entry["ts"] = now

    if missing_jobs:
        st.warning(
            "No se encontraron filas en pc_config para: "
            + ", ".join(sorted(missing_jobs))
        )

    if not updated_any:
        return False

    load_pc_config.clear()
    st.session_state["pc_config_buffer"] = {
        key: info
        for key, info in buffer.items()
        if key not in {entry["key"] for entry in pending}
    }
    return True


def _make_unique(headers):
    out, seen = [], {}
    for i, h in enumerate(headers):
        h = (h or "").strip() or f"col_{i+1}"
        if h in seen:
            seen[h] += 1
            h = f"{h}_{seen[h]}"
        else:
            seen[h] = 0
        out.append(h)
    return out


def _coerce_to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return value != 0
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in TRUE_VALUES:
            return True
        if norm in {"false", "0", "no", "n"}:
            return False
    return False


def _is_checkbox_target(col_name: str) -> bool:
    return col_name.strip().lower() in CHECKBOX_FLAG_NAMES


def _parse_sheet_date_column(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], index=series.index, dtype="datetime64[ns]", name=series.name)

    if pd.api.types.is_datetime64_any_dtype(series):
        tz_info = getattr(series.dt, "tz", None)
        return series.dt.tz_convert(None) if tz_info is not None else series

    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.replace({
        "": pd.NA,
        "none": pd.NA,
        "null": pd.NA,
        "nan": pd.NA,
        "nat": pd.NA,
        "n/a": pd.NA,
    }, regex=False)

    parsed = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)

    mask_serial = parsed.isna() & cleaned.str.fullmatch(r"\d+(\.0+)?", na=False)
    if mask_serial.any():
        serial = cleaned[mask_serial].astype(float)
        parsed.loc[mask_serial] = pd.to_datetime(
            serial,
            errors="coerce",
            origin="1899-12-30",
            unit="D",
        )

    mask_pattern = parsed.isna() & cleaned.notna()
    if mask_pattern.any():
        date_pattern = re.compile(r"(\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4})")

        def _last_match(text: str):
            matches = date_pattern.findall(text)
            if matches:
                return matches[-1]
            return text

        extracted = cleaned[mask_pattern].map(_last_match)
        parsed.loc[mask_pattern] = pd.to_datetime(extracted, errors="coerce", dayfirst=True)

    tz_info = getattr(parsed.dt, "tz", None)
    if tz_info is not None:
        parsed = parsed.dt.tz_convert(None)

    return parsed


def _parse_money_value(value):
    """Convierte textos de monto a float para visualización, sin tocar la data base."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("B/.", "").replace("B/", "").replace("$", "")
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text or text in {"-", ".", ",", "-.", "-,"}:
        return None

    comma_pos = text.rfind(",")
    dot_pos = text.rfind(".")

    # Si tiene ambos separadores, tomamos como decimal el ultimo que aparezca.
    if comma_pos >= 0 and dot_pos >= 0:
        if comma_pos > dot_pos:
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif comma_pos >= 0:
        # Solo coma: decimal si termina en 1-2 digitos, si no miles.
        if re.search(r",\d{1,2}$", text):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif dot_pos >= 0:
        # Solo punto: si hay varios, asumimos que los anteriores son miles.
        if text.count(".") > 1:
            head, tail = text.rsplit(".", 1)
            text = head.replace(".", "") + "." + tail
        elif re.search(r"\.\d{3}$", text):
            # Caso comun de miles: 10.000
            text = text.replace(".", "")

    try:
        return float(text)
    except Exception:
        return None


def _coerce_money_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    parsed = series.map(_parse_money_value)
    return pd.to_numeric(parsed, errors="coerce")


def _format_money_series(series: pd.Series) -> pd.Series:
    """
    Formato visual fijo de moneda para tablas (sin afectar calculos/filtros).
    Ej: $ 10,000.50
    """
    if series.empty:
        return pd.Series(index=series.index, dtype="string")
    numeric = _coerce_money_series(series)
    formatted = numeric.map(lambda x: f"$ {x:,.2f}" if pd.notna(x) else "")
    return formatted.astype("string")


def _is_money_column_name(column_name: str) -> bool:
    name = str(column_name or "").strip().lower()
    if not name:
        return False
    if name.startswith("actos (monto desc)") or name == "actos monto desc":
        return False
    # Deteccion conservadora para no tocar columnas no monetarias.
    money_tokens = (
        "precio",
        "monto",
        "estimado",
        "referencia",
        "adjudic",
        "importe",
        "valor",
    )
    return any(token in name for token in money_tokens)


def _prepare_money_columns_for_sorting(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Convierte columnas monetarias a numerico para que el ordenamiento sea correcto.
    Devuelve (df_convertido, column_config_para_streamlit).
    """
    if df is None or df.empty:
        return df, {}
    out = df.copy()
    cfg: dict[str, object] = {}
    for col in out.columns:
        if _is_money_column_name(col):
            coerced = _coerce_money_series(out[col])
            if coerced.notna().any():
                out[col] = coerced
                cfg[col] = st.column_config.NumberColumn(col, format="$ %.2f")
    return out, cfg


def _format_money_columns_for_display(df: pd.DataFrame) -> pd.DataFrame:
    out, _ = _prepare_money_columns_for_sorting(df)
    return out

st.set_page_config(page_title="Visualizador de Actos", layout="wide")
_require_authentication()
_ensure_scroll_top_on_page_entry()
st.title("📋 Visualizador de Actos Panamá Compra")

# ---- Config ----
SHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"


DEFAULT_SHEET = "cl_abiertas_rir_sin_requisitos"

SHEET_LABELS = {
    "cl_abiertas_rir_sin_requisitos": "CL abiertas RIR sin requisitos",
    "cl_abiertas": "CL abiertas",
    "cl_abiertas_rir_con_ct": "CL abiertas RIR con CT",
    "cl_prog_sin_ficha": "CL programadas sin ficha",
    "cl_prog_sin_requisitos": "CL programadas sin requisitos",
    "cl_prog_con_ct": "CL programadas con CT",
    "cl_prioritarios": "CL prioritarios",
    "ap_con_ct": "AP con CT",
    "ap_sin_ficha": "AP sin ficha",
    "ap_sin_requisitos": "AP sin requisitos",
}

SHEET_GROUPS = {
    "Cotizaciones Abiertas": [
        "cl_abiertas_rir_sin_requisitos",
        "cl_abiertas",
        "cl_abiertas_rir_con_ct",
    ],
    "Cotizaciones Programadas": [
        "cl_prog_sin_ficha",
        "cl_prog_sin_requisitos",
        "cl_prog_con_ct",
    ],
    "Licitaciones": [
        "ap_con_ct",
        "ap_sin_ficha",
        "ap_sin_requisitos",
    ],
    "Prioritarias": [
        "cl_prioritarios",
    ],
}

CATEGORY_ORDER = [
    "Cotizaciones Abiertas",
    "Cotizaciones Programadas",
    "Licitaciones",
    "Prioritarias",
]


def _sheet_label(name: str) -> str:
    return SHEET_LABELS.get(name, name.replace("_", " ").title())

def get_gc():
    # Reutiliza las credenciales centralizadas en sheets.get_client()
    client, _ = get_client()
    return client


def apply_checkbox_updates(sheet_name: str, updates):
    if not updates:
        return

    ws = get_gc().open_by_key(SHEET_ID).worksheet(sheet_name)
    headers = _make_unique(ws.row_values(1))

    for row_number, column_name, value in updates:
        try:
            col_idx = headers.index(column_name) + 1
        except ValueError:
            continue
        ws.update_cell(int(row_number), col_idx, "TRUE" if value else "FALSE")

# --- reemplaza tu load_df y añade el helper _make_unique ---

@st.cache_data(ttl=300)
def load_df(sheet_name: str) -> pd.DataFrame:
    sh = get_gc().open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    raw_headers = ws.row_values(1)
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    if not any(c.strip() for c in raw_headers):
        header_row_idx = None
        for r in range(min(10, len(values))):
            non_empty = sum(1 for c in values[r] if c.strip())
            if non_empty >= 3:
                header_row_idx = r
                break

        if header_row_idx is None:
            return pd.DataFrame()

        headers = _make_unique(values[header_row_idx])
        width = len(headers)

        data_rows, row_numbers = [], []
        for idx, row in enumerate(values[header_row_idx + 1 :], start=header_row_idx + 2):
            trimmed = row[:width] + [""] * (width - len(row))
            data_rows.append(trimmed)
            row_numbers.append(idx)

        if not data_rows:
            df = pd.DataFrame(columns=headers)
            df[ROW_ID_COL] = pd.Series(dtype=int)
            return df

        df = pd.DataFrame(data_rows, columns=headers)
        df[ROW_ID_COL] = pd.Series(row_numbers, dtype=int)
    else:
        headers = _make_unique(raw_headers)
        width = len(headers)
        data_rows = values[1:] if len(values) > 1 else []
        padded_rows = [row[:width] + [""] * (width - len(row)) for row in data_rows]
        df = pd.DataFrame(padded_rows, columns=headers)
        if df.empty:
            df[ROW_ID_COL] = pd.Series(dtype=int)
        else:
            df[ROW_ID_COL] = pd.Series(range(2, len(df) + 2), dtype=int)

    df = df.replace("", pd.NA)
    data_cols = [c for c in df.columns if c != ROW_ID_COL]
    if data_cols:
        df = df.dropna(how="all", subset=data_cols)
    df = df.reset_index(drop=True)
    return df


def render_df(
    df: pd.DataFrame,
    sheet_name: str,
    pc_state_df: pd.DataFrame | None = None,
    pc_config_df: pd.DataFrame | None = None,
    suffix: str | None = None,
):
    keyp = f"{sheet_name}_"

    notice_key = keyp + "update_notice"
    if notice_key in st.session_state:
        count = st.session_state.pop(notice_key)
        if count:
            st.success(f"Se guardaron {count} cambio(s) en la hoja.")

    df = df.copy()
    displayable_columns = [c for c in df.columns if c != ROW_ID_COL]

    def _normalize_label(value: str) -> str:
        value = (value or "").strip().lower()
        for src, tgt in (
            ("á", "a"),
            ("é", "e"),
            ("í", "i"),
            ("ó", "o"),
            ("ú", "u"),
            ("ü", "u"),
        ):
            value = value.replace(src, tgt)
        value = re.sub(r"\s+", " ", value)
        return value

    def _is_date_header(name: str) -> bool:
        normalized = _normalize_label(name)
        if not normalized:
            return False
        if normalized.startswith("fecha"):
            return True
        if "publicacion" in normalized:
            return True
        return False

    date_columns = [c for c in df.columns if _is_date_header(c)]

    filter_date_col = None
    for col in date_columns:
        norm = _normalize_label(col)
        if "publicacion" in norm:
            filter_date_col = col
            break

    if filter_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha sola":
                filter_date_col = col
                break

    if filter_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha":
                filter_date_col = col
                break

    if filter_date_col is None and date_columns:
        filter_date_col = date_columns[0]

    event_date_col = None
    for col in date_columns:
        if _normalize_label(col) == "fecha sola":
            event_date_col = col
            break

    if event_date_col is None:
        for col in date_columns:
            norm = _normalize_label(col)
            if any(token in norm for token in ("acto", "celebr")):
                event_date_col = col
                break

    if event_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha":
                event_date_col = col
                break

    parsed_date_series = {}
    for col in date_columns:
        parsed = _parse_sheet_date_column(df[col])
        parsed_date_series[col] = parsed
        normalized_name = _normalize_label(col)
        keep_original = (col == filter_date_col) or (normalized_name == "fecha")
        if not keep_original:
            df[col] = parsed

    # Detectar columnas de monto/precio
    money_cols = [c for c in df.columns if _is_money_column_name(c)]

    today = date.today()
    today_ts = pd.Timestamp(today)

    with st.expander("🔎 Filtros", expanded=True):
        if "Entidad" in df.columns:
            opciones = sorted([e for e in df["Entidad"].dropna().unique()])
            sel = st.multiselect("Entidad", opciones, key=keyp+"ent")
            if sel:
                df = df[df["Entidad"].isin(sel)]

        if "Estado" in df.columns:
            opciones = sorted([e for e in df["Estado"].dropna().unique()])
            sel = st.multiselect("Estado", opciones, key=keyp+"estado")
            if sel:
                df = df[df["Estado"].isin(sel)]

        date_filter_series = None
        if filter_date_col:
            date_filter_series = parsed_date_series.get(filter_date_col)
            if date_filter_series is not None and date_filter_series.notna().any():
                normalized_dates = date_filter_series.dt.normalize()
                valid_dates = normalized_dates.dropna()
                mind = valid_dates.min() if not valid_dates.empty else pd.Timestamp(today)
                maxd = valid_dates.max() if not valid_dates.empty else pd.Timestamp(today)
                default_fin = today
                default_ini = today - timedelta(days=30)
                if pd.notna(mind) and mind.date() > default_fin:
                    default_ini = mind.date()
                    default_fin = mind.date()
                if default_ini > default_fin:
                    default_ini = default_fin
                r = st.date_input(
                    "Rango de fechas",
                    value=(default_ini, default_fin),
                    key=keyp+"fecha",
                )
                if isinstance(r, tuple) and len(r) == 2:
                    ini = pd.Timestamp(r[0]).normalize()
                    fin = pd.Timestamp(r[1]).normalize()
                    normalized = date_filter_series.dt.normalize()
                    mask_valid = (normalized >= ini) & (normalized <= fin)
                    mask = normalized.isna() | mask_valid
                    df = df[mask]
            else:
                st.info("No encontramos fechas válidas en esa columna todavía.")

        if money_cols:
            colm = money_cols[0]
            v = _coerce_money_series(df[colm])
            if v.notna().any():
                price_min, price_max = 1000.0, 2000000.0
                if price_min < price_max:
                    r = st.slider(
                        f"Rango de {colm}",
                        min_value=price_min,
                        max_value=price_max,
                        value=(price_min, price_max),
                        step=1000.0,
                        key=keyp+"monto",
                    )
                    parsed_money = _coerce_money_series(df[colm])
                    df = df[(parsed_money >= r[0]) & (parsed_money <= r[1])]

        q = st.text_input(
            "Búsqueda rápida (todas las columnas)",
            key=keyp+"q",
            placeholder='Ej: chiller "aire acondicionado" -mantenimiento',
        )
        search_ui_cols = st.columns([0.9, 1.4, 1.0])
        with search_ui_cols[0]:
            search_mode = st.radio(
                "Modo",
                options=["OR", "AND"],
                horizontal=True,
                key=keyp+"q_mode",
                label_visibility="collapsed",
            )
        with search_ui_cols[1]:
            search_col_options = ["Todas las columnas"] + displayable_columns
            search_col = st.selectbox(
                "Columna",
                options=search_col_options,
                key=keyp+"q_col",
                label_visibility="collapsed",
            )
        with search_ui_cols[2]:
            ignore_accents = st.toggle(
                "Ignorar acentos",
                value=True,
                key=keyp+"q_strip_accents",
            )

        if q:
            target_column = None if search_col == "Todas las columnas" else search_col
            filtered_df, _ = _apply_advanced_text_search(
                df,
                raw_query=q,
                mode=search_mode,
                target_column=target_column,
                ignore_accents=ignore_accents,
            )
            df = filtered_df

        st.caption('Tip: usa comillas para frase exacta y `-palabra` para excluir.')
        def _is_item_column(col_name: str) -> bool:
            normalized = col_name.strip().lower().replace("í", "i")
            return normalized.startswith("item")

        item_cols_sorted = [c for c in displayable_columns if _is_item_column(c)]

        cols_key = keyp + "cols"
        state_applied_key = keyp + "items_all_applied"

        if item_cols_sorted:
            non_item_cols = [c for c in displayable_columns if c not in item_cols_sorted]
            base_items = item_cols_sorted[:2] if len(item_cols_sorted) >= 2 else item_cols_sorted
            base_default = non_item_cols + base_items
            show_all = st.toggle("➡️ Mostrar todos los Item_n", key=keyp+"toggle_items")

            if cols_key not in st.session_state:
                st.session_state[cols_key] = base_default
                st.session_state[state_applied_key] = False
            st.session_state.setdefault(state_applied_key, False)

            if show_all and not st.session_state.get(state_applied_key, False):
                st.session_state[cols_key] = non_item_cols + item_cols_sorted
                st.session_state[state_applied_key] = True
            elif not show_all and st.session_state.get(state_applied_key, False):
                st.session_state[cols_key] = base_default
                st.session_state[state_applied_key] = False
        else:
            if cols_key not in st.session_state:
                st.session_state[cols_key] = displayable_columns

        selected_cols = st.multiselect(
            "Columnas a mostrar",
            options=displayable_columns,
            key=cols_key,
        )
        if selected_cols:
            cols = [c for c in displayable_columns if c in selected_cols]
        else:
            cols = displayable_columns

    parsed_filtered = {col: series.loc[df.index] for col, series in parsed_date_series.items()}

    df_base = df.copy()

    metric_state_key = keyp + "metric_filter"
    active_metric = st.session_state.get(metric_state_key)

    metrics_defs = []
    metrics_defs.append({
        "key": "total",
        "label": "Total de actos públicos",
        "count": int(len(df_base)),
        "filter": None,
    })

    public_col = next((c for c in df_base.columns if "public" in c.lower()), None)
    public_series = None
    public_series_normalized = None

    if public_col:
        public_series = _parse_sheet_date_column(df_base[public_col])
        count_public_today = 0
        if public_series.notna().any():
            public_series_normalized = public_series.dt.normalize()
            count_public_today = int((public_series_normalized == today_ts).sum())
        metrics_defs.append({
            "key": "publicados_hoy",
            "label": "Actos publicados hoy",
            "count": count_public_today,
            "filter": "publicados_hoy",
        })

    event_series = None
    event_series_normalized = None
    count_date_today = 0

    if event_date_col:
        event_series = parsed_filtered.get(event_date_col)
        if event_series is not None:
            event_series_normalized = event_series.dt.normalize()
            if event_series_normalized.notna().any():
                count_date_today = int((event_series_normalized == today_ts).sum())

    if event_date_col:
        metrics_defs.append({
            "key": "fecha_hoy",
            "label": "Actos a celebrarse hoy",
            "count": count_date_today,
            "filter": "fecha_hoy",
        })

    cols_metrics = st.columns(len(metrics_defs))
    for metric_col, metric in zip(cols_metrics, metrics_defs):
        with metric_col:
            label = metric["label"]
            if metric.get("count") is not None:
                label = f"{label}\n{metric['count']}"

            prefix = "✅ " if active_metric == metric.get("filter") else ""
            if metric["key"] == "total" and active_metric is None:
                prefix = "✅ "

            clicked = st.button(
                prefix + label,
                key=keyp + f"metric_btn_{metric['key']}",
                use_container_width=True,
            )

            if clicked:
                if metric["key"] == "total":
                    st.session_state[metric_state_key] = None
                else:
                    current = st.session_state.get(metric_state_key)
                    st.session_state[metric_state_key] = None if current == metric["filter"] else metric["filter"]

    active_metric = st.session_state.get(metric_state_key)

    df = df_base
    if active_metric == "fecha_hoy" and event_series_normalized is not None:
        mask = event_series_normalized == today_ts
        df = df_base.loc[mask.fillna(False)]
    elif active_metric == "publicados_hoy" and public_series_normalized is not None:
        mask = public_series_normalized == today_ts
        df = df_base.loc[mask.fillna(False)]

    displayable_columns = [c for c in df.columns if c != ROW_ID_COL]

    table_columns = cols if cols else displayable_columns
    display_df = df[table_columns].copy()

    editable_cols = [c for c in display_df.columns if _is_checkbox_target(c)]
    for col in editable_cols:
        display_df[col] = display_df[col].map(_coerce_to_bool)

    col_cfg = {}
    for date_field in date_columns:
        if date_field in display_df.columns and pd.api.types.is_datetime64_any_dtype(display_df[date_field]):
            col_cfg[date_field] = st.column_config.DateColumn(date_field, help="Fecha")

    for c in money_cols:
        if c in display_df.columns:
            coerced = _coerce_money_series(display_df[c])
            if coerced.notna().any():
                display_df[c] = coerced
                col_cfg[c] = st.column_config.NumberColumn(c, format="$ %.2f")

    link_col = next((c for c in display_df.columns if c.strip().lower() in {"enlace", "link", "url"}), None)
    if link_col:
        col_cfg[link_col] = st.column_config.LinkColumn(
            label="🔗",
            display_text="🔗",
            help="Abrir acto en PanamáCompra",
        )

    for col in editable_cols:
        col_cfg[col] = st.column_config.CheckboxColumn(col, help="Sincroniza al marcar", default=False)

    table_height = 620
    disabled_columns = [c for c in display_df.columns if c not in editable_cols]
    disabled_config = {"columns": disabled_columns} if disabled_columns else False

    editor_key = keyp + "editor"

    if editable_cols:
        original_display = display_df.copy()
        edited_df = st.data_editor(
            display_df,
            hide_index=True,
            width="stretch",
            height=table_height,
            column_config=col_cfg,
            disabled=disabled_config,
            key=editor_key,
        )

        changes = []
        for col in editable_cols:
            orig_series = original_display[col].fillna(False).astype(bool)
            new_series = edited_df[col].fillna(False).astype(bool)
            diff_mask = orig_series != new_series
            if diff_mask.any():
                for idx in edited_df.index[diff_mask]:
                    row_number = df.loc[idx, ROW_ID_COL]
                    if pd.isna(row_number):
                        continue
                    changes.append((int(row_number), col, bool(new_series.loc[idx])))

        if changes:
            apply_checkbox_updates(sheet_name, changes)
            st.session_state[notice_key] = len(changes)
            load_df.clear()
            st.rerun()

        df_view = edited_df
    else:
        df_view = display_df
        st.data_editor(
            df_view,
            hide_index=True,
            width="stretch",
            height=table_height,
            column_config=col_cfg,
            disabled=True,
            key=editor_key,
        )

    st.caption(f"Mostrando {len(df)} filas")

    render_pc_state_cards(pc_state_df, pc_config_df, suffix=suffix)


def render_panamacompra_db_panel(*, show_header: bool = True) -> None:
    """Muestra una vista de tablas desde SQLite local o Supabase."""
    if show_header:
        st.divider()
        st.subheader("Base panamacompra.db")

    backend = _active_db_backend()
    db_path_str = ""
    db_url = ""

    if backend == "postgres":
        db_url = _supabase_db_url()
        st.caption("Origen configurado: `Supabase (PostgreSQL)`")
        try:
            db_tables = list_postgres_tables(db_url)
        except Exception as exc:
            st.error(f"No fue posible conectar a Supabase: {exc}")
            return
    else:
        db_path = _preferred_db_path()
        if db_path is None:
            st.info("No hay rutas configuradas para la base panamacompra.db.")
            return

        st.caption(f"Origen configurado: `{db_path}`")
        if not db_path.exists():
            st.warning(
                "No pudimos abrir el archivo local. "
                "Si usas Streamlit Cloud, configura `SUPABASE_DB_URL` en secrets."
            )
            return

        db_path_str = str(db_path)
        try:
            db_tables = list_sqlite_tables(db_path_str)
        except sqlite3.OperationalError as exc:
            st.error(f"No fue posible conectar a la base: {exc}")
            return
        except Exception as exc:
            st.error(f"No fue posible listar las tablas: {exc}")
            return

    if not db_tables:
        st.info("No hay tablas visibles en la base configurada.")
        return

    selected_table = st.selectbox(
        "Tabla disponible en la base",
        db_tables,
        key="pc_db_table_selector",
    )

    try:
        if backend == "postgres":
            table_columns = list_postgres_columns(db_url, selected_table)
        else:
            table_columns = list_sqlite_columns(db_path_str, selected_table)
    except Exception as exc:
        st.error(f"No se pudieron listar columnas de {selected_table}: {exc}")
        return

    if not table_columns:
        st.warning("La tabla seleccionada no tiene columnas visibles.")
        return

    st.caption(
        "Nota: el limite es solo de visualizacion por pagina. "
        "El motor consulta toda la tabla y filtra antes de traer resultados."
    )

    search_text = st.text_input(
        "Buscador de texto (todas las columnas)",
        key="pc_db_search_text",
        placeholder="Ej: jeringa hospital enero",
    )
    search_mode = st.radio(
        "Modo del buscador",
        options=["OR", "AND"],
        horizontal=True,
        key="pc_db_search_mode",
    )

    num_filters = st.number_input(
        "Cantidad de filtros",
        min_value=0,
        max_value=5,
        value=0,
        step=1,
        key="pc_db_num_filters",
    )
    filters_mode = st.radio(
        "Combinar filtros con",
        options=["AND", "OR"],
        horizontal=True,
        key="pc_db_filters_mode",
    )

    filters: list[dict[str, str]] = []
    operator_options = ["contiene", "igual", "distinto", "empieza con", "termina con"]
    for idx in range(int(num_filters)):
        c1, c2, c3 = st.columns([2.2, 1.2, 2.6])
        filter_col = c1.selectbox(
            f"Columna filtro {idx + 1}",
            table_columns,
            key=f"pc_db_filter_col_{idx}",
            label_visibility="collapsed",
        )
        filter_op = c2.selectbox(
            f"Operador filtro {idx + 1}",
            operator_options,
            key=f"pc_db_filter_op_{idx}",
            label_visibility="collapsed",
        )
        filter_value = c3.text_input(
            f"Valor filtro {idx + 1}",
            key=f"pc_db_filter_value_{idx}",
            label_visibility="collapsed",
            placeholder="valor",
        )
        if filter_value.strip():
            filters.append(
                {
                    "column": filter_col,
                    "operator": filter_op,
                    "value": filter_value.strip(),
                }
            )

    combine_mode = st.radio(
        "Combinar buscador + filtros con",
        options=["AND", "OR"],
        horizontal=True,
        key="pc_db_combine_mode",
    )

    rows_per_page = st.slider(
        "Filas por pagina",
        min_value=100,
        max_value=5000,
        value=1000,
        step=100,
        help="Puedes recorrer todas las coincidencias usando paginacion.",
    )

    search_terms = _split_search_terms(search_text)
    where_sql, query_params = _build_sql_conditions(
        backend=backend,
        columns=table_columns,
        search_terms=search_terms,
        search_mode=search_mode,
        filters=filters,
        filters_mode=filters_mode,
        combine_mode=combine_mode,
    )

    try:
        if backend == "postgres":
            total_rows = count_postgres_filtered_rows(
                db_url, selected_table, where_sql, query_params
            )
        else:
            total_rows = count_sqlite_filtered_rows(
                db_path_str, selected_table, where_sql, query_params
            )
    except Exception as exc:
        st.error(f"No se pudo contar registros en {selected_table}: {exc}")
        return

    total_pages = max(1, math.ceil(total_rows / max(1, rows_per_page)))
    page_number = st.number_input(
        "Pagina",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1,
        key="pc_db_page_number",
    )
    offset = (int(page_number) - 1) * int(rows_per_page)

    try:
        if backend == "postgres":
            preview_df = query_postgres_preview(
                db_url,
                selected_table,
                where_sql,
                query_params,
                rows_per_page,
                offset,
            )
        else:
            preview_df = query_sqlite_preview(
                db_path_str,
                selected_table,
                where_sql,
                query_params,
                rows_per_page,
                offset,
            )
    except Exception as exc:
        st.error(f"Error al consultar {selected_table}: {exc}")
        return

    if preview_df.empty:
        st.info("No hay filas para los filtros actuales.")
    else:
        preview_view, money_cfg = _prepare_money_columns_for_sorting(preview_df)
        st.dataframe(
            preview_view,
            use_container_width=True,
            height=520,
            column_config=money_cfg,
        )

    st.caption(
        f"Coincidencias totales: {total_rows:,}. "
        f"Pagina {int(page_number)} de {total_pages}. "
        f"Mostrando hasta {rows_per_page} filas por pagina."
    )

# ---- UI: pestañas de categorías + desplegable de hojas ----
pc_state_df = load_pc_state()
pc_config_df = load_pc_config()
ordered_categories = [c for c in CATEGORY_ORDER if c in SHEET_GROUPS]
category_tabs = st.tabs(ordered_categories)

for tab, category_name in zip(category_tabs, ordered_categories):
    with tab:
        st.subheader(category_name)
        sheets = SHEET_GROUPS.get(category_name, [])
        if not sheets:
            st.info("Sin hojas configuradas para esta categoría.")
            continue

        selector_slug = re.sub(r"[^0-9a-z]+", "_", category_name.lower())
        selector_key = f"sheet_selector_{selector_slug.strip('_')}"
        tab_suffix = selector_slug.strip("_") or None

        if len(sheets) == 1:
            sheet_name = sheets[0]
            st.caption(_sheet_label(sheet_name))
        else:
            default_idx = sheets.index(DEFAULT_SHEET) if DEFAULT_SHEET in sheets else 0
            sheet_name = st.selectbox(
                "Seleccione la hoja",
                sheets,
                index=default_idx,
                key=selector_key,
                format_func=_sheet_label,
                label_visibility="collapsed",
            )

        df = load_df(sheet_name)
        if df.empty:
            st.info("Sin datos en esta pestaña.")
        else:
            render_df(df, sheet_name, pc_state_df, pc_config_df, suffix=tab_suffix)

try:
    _app_cfg = st.secrets.get("app", {})
except Exception:
    _app_cfg = {}

def _first_app_value(cfg: dict, keys: list[str]) -> str:
    for k in keys:
        value = cfg.get(k)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


fichas_file_id = _first_app_value(
    _app_cfg,
    [
        "DRIVE_FICHAS_CTNI_CON_ENLACE_FILE_ID",
        "DRIVE_FICHAS_CON_ENLACE_FILE_ID",
        "DRIVE_FICHAS_CTNI_FILE_ID",
        "DRIVE_CRITERIOS_TECNICOS_FILE_ID",
        "DRIVE_FICHAS_TECNICAS_FILE_ID",
    ],
)
catalogos_file_id = _first_app_value(
    _app_cfg,
    [
        "DRIVE_OFERENTES_CATALOGOS_FILE_ID",
        "DRIVE_OFERENTES_CATALOGO_FILE_ID",
        "DRIVE_OFERENTES_FILE_ID",
    ],
)

backend_refs = _active_db_backend()
db_url_refs = _supabase_db_url() if backend_refs == "postgres" else ""
db_path_refs = ""
if backend_refs != "postgres":
    db_path_obj = _preferred_db_path()
    if db_path_obj and db_path_obj.exists():
        db_path_refs = str(db_path_obj)

fichas_table = _first_app_value(
    _app_cfg,
    ["SUPABASE_FICHAS_TABLE", "FICHAS_TABLE_NAME"],
) or _find_reference_table_name(
    backend=backend_refs,
    db_url=db_url_refs,
    db_path_str=db_path_refs,
    candidates=["fichas_tecnicas", "fichas_ctni", "criterios_tecnicos"],
)
actos_table = _first_app_value(
    _app_cfg,
    ["SUPABASE_ACTOS_TABLE", "ACTOS_TABLE_NAME"],
) or _find_reference_table_name(
    backend=backend_refs,
    db_url=db_url_refs,
    db_path_str=db_path_refs,
    candidates=["actos_publicos", "actos", "panamacompra_actos"],
)
catalogos_table = _first_app_value(
    _app_cfg,
    ["SUPABASE_CATALOGOS_TABLE", "CATALOGOS_TABLE_NAME"],
) or _find_reference_table_name(
    backend=backend_refs,
    db_url=db_url_refs,
    db_path_str=db_path_refs,
    candidates=["oferentes_catalogos", "catalogos_oferentes", "oferentes"],
)

st.divider()
with st.expander("Base de datos de actos publicos, fichas y oferentes", expanded=False):
    render_panamacompra_db_panel(show_header=False)

with st.expander("Prospeccion RIR", expanded=False):
    render_prospeccion_rir_panel(
        backend=backend_refs,
        db_url=db_url_refs,
        db_path_str=db_path_refs,
        actos_table=actos_table,
        fichas_table=fichas_table,
        fichas_drive_file_id=str(fichas_file_id or ""),
        key_prefix="pc_prospeccion_rir",
    )

with st.expander("Fichas tecnicas", expanded=False):
    # Prioriza el documento de fichas con enlace (Drive) por encima de la tabla antigua.
    if fichas_file_id:
        render_drive_reference_panel(
            title="Fichas tecnicas",
            file_id=str(fichas_file_id or ""),
            key_prefix="pc_fichas",
            show_header=False,
        )
    elif fichas_table:
        render_db_reference_panel(
            title="Fichas tecnicas",
            key_prefix="pc_fichas",
            backend=backend_refs,
            db_url=db_url_refs,
            db_path_str=db_path_refs,
            table_name=fichas_table,
            show_header=False,
        )
    else:
        st.info("No hay fuente configurada para Fichas tecnicas.")

with st.expander("Oferentes y catalogos", expanded=False):
    if catalogos_table:
        render_db_reference_panel(
            title="Oferentes y catalogos",
            key_prefix="pc_catalogos",
            backend=backend_refs,
            db_url=db_url_refs,
            db_path_str=db_path_refs,
            table_name=catalogos_table,
            show_header=False,
        )
    else:
        render_drive_reference_panel(
            title="Oferentes y catalogos",
            file_id=str(catalogos_file_id or ""),
            key_prefix="pc_catalogos",
            show_header=False,
        )

render_panamacompra_ai_chat(
    backend=backend_refs,
    db_url=db_url_refs,
    db_path=db_path_refs,
    allowed_tables=[actos_table, fichas_table, catalogos_table],
)
