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
# Límite seguro de filas crudas devueltas al modelo (ajusta aquí si lo necesitas)
MAX_ROWS = 5000
# Límite máximo permitido si el modelo pide un LIMIT demasiado alto
MAX_LIMIT_ALLOWED = 10000


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
    return sql


def _has_agg(sql_lower: str) -> bool:
    return any(func in sql_lower for func in ("count(", "sum(", "avg(", "min(", "max(", "group by"))


def _has_where(sql_lower: str) -> bool:
    return " where " in f" {sql_lower} "


def _has_limit(sql_lower: str) -> int | None:
    match = re.search(r"\blimit\s+(\d+)", sql_lower)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return None
    return None


def _enforce_limit(sql: str) -> tuple[str, str]:
    """
    Decide el límite a aplicar.
    - Consultas agregadas (COUNT, SUM, AVG, MIN, MAX, GROUP BY): permiten sin LIMIT.
    - Consultas de filas crudas: imponen un LIMIT (agregan si falta o ajustan si es muy alto).
    Retorna (sql_normalizado, nota_sobre_limit).
    """
    sql = sql.strip().rstrip(";")
    sql_lower = sql.lower()

    # Evita SELECT * sin filtros ni agrupación para tablas completas
    if "select *" in sql_lower and not _has_where(sql_lower) and not _has_agg(sql_lower):
        raise ValueError(
            "La consulta intenta traer todas las filas sin filtros. Añade un WHERE o usa agregaciones (COUNT, SUM, etc.)."
        )

    is_agg = _has_agg(sql_lower)
    existing_limit = _has_limit(sql_lower)

    if is_agg:
        # No forzamos LIMIT para agregados; la salida suele ser pequeña
        return sql, "Consulta agregada: se permite sin LIMIT en filas."

    # Consultas de filas: aplica límite seguro
    if existing_limit is None:
        return f"{sql} LIMIT {MAX_ROWS}", f"Se añadió LIMIT {MAX_ROWS} para filas crudas."

    if existing_limit > MAX_LIMIT_ALLOWED:
        # Capamos un límite muy alto
        sql = re.sub(r"\blimit\s+\d+", f"LIMIT {MAX_ROWS}", sql, flags=re.IGNORECASE)
        return sql, f"Límite solicitado era muy alto; se ajustó a {MAX_ROWS}."

    return sql, ""  # ya traía un LIMIT aceptable


def run_query(sql: str) -> tuple[pd.DataFrame, str]:
    """
    Ejecuta la consulta con salvaguardas.
    - Valida que sea SELECT.
    - Ajusta/agrega LIMIT si es necesario.
    - Devuelve el DataFrame y una nota sobre el límite aplicado (para mostrar en el resumen).
    """
    sql = _ensure_select(sql)
    sql, limit_note = _enforce_limit(sql)
    db = _db_path()
    if not db.exists():
        raise FileNotFoundError(f"No se encontró la base {db}")
    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query(sql, conn)
    return df, limit_note


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


def _normalize_sql(sql: str) -> str:
    # Normaliza símbolos y espacios para detectar FROM y comparadores.
    replacements = {
        "≥": ">=",
        "≤": "<=",
        "==": "=",
    }
    for src, dst in replacements.items():
        sql = sql.replace(src, dst)
    return sql.strip()


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
        "Usa los nombres reales de columnas. Si la pregunta no es clara, responde con un mensaje corto en español. "
        "Ejemplo: SELECT * FROM actos_publicos WHERE precio_referencia > 50000 LIMIT 50;"
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
    sql = _normalize_sql(sql)
    sql_lower = sql.lower()
    if not re.search(r"\bfrom\b", sql_lower):
        # Intenta asumir la primera tabla disponible (fallback)
        default_table = tables[0] if tables else None
        if default_table:
            # si viene "select *" sin from, lo completamos
            sql = re.sub(r";\s*$", "", sql.strip())
            sql = f"{sql} FROM {default_table}"
            sql_lower = sql.lower()
        else:
            hint = ", ".join(tables) if tables else "sin tablas detectadas"
            return (
                "La consulta generada no especificó tabla. Menciona el nombre de la tabla, por ejemplo: "
                f"{hint}.",
                None,
                raw_text,
            )

    try:
        df, limit_note = run_query(sql)
    except Exception as exc:
        return f"Error al ejecutar la consulta: {exc}", None, raw_text

    note = f"\n{limit_note}" if limit_note else ""
    summary = f"Consulta ejecutada (límite filas crudas {MAX_ROWS}):\n{sql}\nFilas devueltas: {len(df)}{note}"
    return summary, df, raw_text
