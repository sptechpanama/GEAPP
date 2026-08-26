from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from services.panama_compra_db_filters import (
    append_date_range_condition,
    date_filter_columns,
    normalized_date_sql_expression,
)


def test_date_columns_prioritize_business_dates_without_inventing_columns():
    columns = [
        "id",
        "fecha_actualizacion",
        "titulo",
        "publicacion",
        "fecha_adjudicacion",
        "fecha",
        "created_at",
    ]

    assert date_filter_columns(columns) == [
        "fecha",
        "fecha_adjudicacion",
        "publicacion",
        "fecha_actualizacion",
        "created_at",
    ]


def test_sqlite_date_range_filters_iso_dash_slash_and_date_ranges():
    conn = sqlite3.connect(":memory:")
    conn.execute('CREATE TABLE actos ("fecha" TEXT, "titulo" TEXT)')
    conn.executemany(
        'INSERT INTO actos ("fecha", "titulo") VALUES (?, ?)',
        [
            ("2026-01-05 12:00:00", "iso"),
            ("15-02-2026", "guion"),
            ("20/03/2026 - 02:00 PM a 04:00 PM", "barra"),
            ("31-12-2025", "anterior"),
            ("fecha pendiente", "invalida"),
        ],
    )

    where_sql, params = append_date_range_condition(
        backend="sqlite",
        columns=["fecha", "titulo"],
        where_sql="",
        params=[],
        column="fecha",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
    )
    rows = conn.execute(
        f'SELECT "titulo" FROM actos WHERE {where_sql} ORDER BY "titulo"',
        params,
    ).fetchall()

    assert rows == [("barra",), ("guion",), ("iso",)]


def test_date_range_is_always_combined_with_existing_filters_using_and():
    where_sql, params = append_date_range_condition(
        backend="sqlite",
        columns=["fecha", "titulo"],
        where_sql='LOWER(CAST("titulo" AS TEXT)) LIKE ?',
        params=["%hospital%"],
        column="fecha",
        start_date="2026-01-01",
        end_date="2026-12-31",
    )

    assert where_sql.startswith('(LOWER(CAST("titulo" AS TEXT)) LIKE ?) AND (')
    assert params == ["%hospital%", "2026-01-01", "2026-12-31"]


def test_postgres_date_range_uses_named_parameters_and_mixed_format_parser():
    expression = normalized_date_sql_expression(
        backend="postgres",
        column="fecha_adjudicacion",
    )
    where_sql, params = append_date_range_condition(
        backend="postgres",
        columns=["fecha_adjudicacion"],
        where_sql="",
        params={},
        column="fecha_adjudicacion",
        start_date="2026-01-01",
        end_date="2026-08-26",
    )

    assert "SUBSTRING" in expression
    assert "^[0-9]{4}" in expression
    assert ":_pc_date_start" in where_sql
    assert ":_pc_date_end" in where_sql
    assert params == {
        "_pc_date_start": "2026-01-01",
        "_pc_date_end": "2026-08-26",
    }


def test_panama_compra_database_panel_exposes_server_side_date_filter():
    page_path = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"
    source = page_path.read_text(encoding="utf-8")
    panel_start = source.index("def render_panamacompra_db_panel")
    panel_end = source.index("# ---- UI:", panel_start)
    panel_source = source[panel_start:panel_end]

    assert '"Aplicar rango de fechas"' in panel_source
    assert '"Fecha a filtrar"' in panel_source
    assert '"Desde"' in panel_source
    assert '"Hasta"' in panel_source
    assert "append_date_range_condition(" in panel_source
    assert panel_source.index("append_date_range_condition(") < panel_source.index(
        "count_postgres_filtered_rows("
    )
