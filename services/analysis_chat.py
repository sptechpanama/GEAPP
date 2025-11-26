"""
Pequeño asistente para convertir preguntas en consultas SQL seguras y responder con un resumen.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Tuple

import pandas as pd

try:
    from openai import OpenAI  # type: ignore
except ImportError:  # pragma: no cover
    OpenAI = None  # type: ignore

from core.config import DB_PATH

DEFAULT_DB = Path(DB_PATH) if DB_PATH else Path("panamacompra.db")
MAX_ROWS = 200


def _db_path() -> Path:
    return DEFAULT_DB


def list_tables() -> list[str]:
    db = _db_path()
    if not db.exists():
        return []
    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
            conn,
        )
    return df["name"].tolist()


def describe_table(table: str) -> list[str]:
    db = _db_path()
    if not db.exists():
        return []
    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query(f"PRAGMA table_info('{table}')", conn)
    cols = []
    for _, row in df.iterrows():
        cols.append(f"{row['name']} ({row['type']})")
    return cols


def _ensure_select(sql: str) -> str:
    sql_lower = sql.strip().lower()
    if not sql_lower.startswith("select"):
        raise ValueError("Solo se permiten consultas SELECT.")
    if any(bad in sql_lower for bad in ("delete", "update", "insert", "drop", "alter")):
        raise ValueError("Solo se permiten consultas de lectura.")
    if "limit" not in sql_lower:
        sql = f"{sql.rstrip(';')} LIMIT {MAX_ROWS}"
    return sql


def run_query(sql: str) -> pd.DataFrame:
    sql = _ensure_select(sql)
    db = _db_path()
    if not db.exists():
        raise FileNotFoundError(f"No se encontró la base {db}")
    with sqlite3.connect(db) as conn:
        return pd.read_sql_query(sql, conn)


def _extract_sql(text: str) -> str | None:
    # Busca bloque ```sql ... ```
    match = re.search(r"```sql\s*(select[\s\S]+?)```", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Fallback: primera línea que empiece con select
    for line in text.splitlines():
        if line.strip().lower().startswith("select"):
            return line.strip()
    return None


def answer_question(question: str, api_key: str) -> Tuple[str, pd.DataFrame | None, str]:
    if OpenAI is None:
        return "OpenAI no está instalado en este entorno.", None, ""

    tables = list_tables()
    schema_info = []
    for tbl in tables:
        cols = describe_table(tbl)
        schema_info.append({"table": tbl, "columns": cols})

    system_prompt = (
        "Eres un asistente de análisis de datos. Genera UNA consulta SQL segura sobre SQLite. "
        f"Máximo {MAX_ROWS} filas. Solo SELECT. Siempre incluye FROM con el nombre de la tabla (ej: actos_publicos). "
        "Usa los nombres reales de columnas. Si la pregunta no es clara, responde con un mensaje corto en español."
    )

    user_prompt = (
        f"Esquema:\n{json.dumps(schema_info, ensure_ascii=False, indent=2)}\n\n"
        f"Pregunta del usuario: {question}\n"
        "Devuelve la consulta en un bloque ```sql ... ``` y un breve plan."
    )

    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_output_tokens=600,
    )
    raw_text = response.output_text
    sql = _extract_sql(raw_text)
    if not sql:
        return "No se pudo generar una consulta clara. Reformula tu pregunta.", None, raw_text
    if " from " not in sql.lower():
        hint = ", ".join(tables) if tables else "sin tablas detectadas"
        return (
            "La consulta generada no especificó tabla. Intenta mencionar el nombre de la tabla, por ejemplo: "
            f"{hint}.",
            None,
            raw_text,
        )

    try:
        df = run_query(sql)
    except Exception as exc:
        return f"Error al ejecutar la consulta: {exc}", None, raw_text

    summary = f"Consulta ejecutada (máx {MAX_ROWS} filas):\n{sql}\nFilas devueltas: {len(df)}"
    return summary, df, raw_text
