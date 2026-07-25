from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from services.cl_opportunities import _load_rows, _prepare_rows


def test_prepare_rows_formats_states_counts_money_and_panama_time():
    source = pd.DataFrame(
        [
            {
                "estado_derivado": "cerrada_sin_propuestas",
                "proposal_count": 0,
                "precio_referencia": 1250.50,
                "fecha_cierre": "2026-07-23T14:00:00-05:00",
                "last_check_at": "2026-07-23T15:00:00-05:00",
            }
        ]
    )
    result = _prepare_rows(source)
    assert result.loc[0, "Estado"] == "Sin propuestas (confirmado)"
    assert int(result.loc[0, "Proponentes"]) == 0
    assert result.loc[0, "Monto referencia"] == 1250.50
    assert str(result.loc[0, "Cierre"].tz) == "America/Panama"


def test_sqlite_loader_excludes_open_records(tmp_path: Path):
    db_path = tmp_path / "panamacompra.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            CREATE TABLE cl_cotizaciones (
                numero_cl TEXT, enlace TEXT, titulo TEXT, entidad TEXT,
                unidad_solicitante TEXT, precio_referencia REAL,
                fecha_publicacion TEXT, fecha_cierre TEXT,
                fichas_detectadas TEXT, estado_derivado TEXT,
                proposal_count INTEGER, proponents_json TEXT,
                evidence_type TEXT, evidence_url TEXT, confidence REAL,
                last_check_at TEXT, next_check_at TEXT, last_error TEXT,
                updated_at TEXT
            )
            """
        )
        rows = [
            (
                "2026-1-10-01-08-CL-000001",
                "https://example.test/1",
                "Sin propuestas",
                "Entidad",
                "",
                1000.0,
                "",
                "2026-07-23T14:00:00-05:00",
                "43358",
                "cerrada_sin_propuestas",
                0,
                "[]",
                "contador_oficial_cero",
                "https://example.test/cuadro/1",
                1.0,
                "2026-07-23T15:00:00-05:00",
                "",
                "",
                "2026-07-23T15:00:00-05:00",
            ),
            (
                "2026-1-10-01-08-CL-000002",
                "https://example.test/2",
                "Abierta",
                "Entidad",
                "",
                2000.0,
                "",
                "2026-07-24T14:00:00-05:00",
                "43358",
                "abierta",
                None,
                "[]",
                "",
                "",
                0.7,
                "",
                "",
                "",
                "2026-07-23T15:00:00-05:00",
            ),
        ]
        connection.executemany(
            "INSERT INTO cl_cotizaciones VALUES "
            f"({','.join('?' for _ in range(19))})",
            rows,
        )
        connection.commit()
    finally:
        connection.close()

    data, status = _load_rows(
        backend="sqlite",
        db_url="",
        db_path=str(db_path),
    )
    assert status == "ok"
    assert data["numero_cl"].tolist() == ["2026-1-10-01-08-CL-000001"]


def test_loader_reports_missing_table_without_crashing(tmp_path: Path):
    db_path = tmp_path / "empty.db"
    sqlite3.connect(db_path).close()
    data, status = _load_rows(
        backend="sqlite",
        db_url="",
        db_path=str(db_path),
    )
    assert status == "missing"
    assert data.empty
