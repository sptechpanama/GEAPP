from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.inteligencia_orquestador_v3 import (  # noqa: E402
    INTEL_PRIORITY_PORTFOLIO_HEADERS,
    WorksheetNotFound,
    create_priority_portfolio,
    list_priority_portfolio,
)


class FakeWorksheet:
    def __init__(self, title: str, rows: int = 100, cols: int = 20) -> None:
        self.title = title
        self.rows: list[list[object]] = []

    def row_values(self, row: int):
        return list(self.rows[row - 1]) if 0 < row <= len(self.rows) else []

    def update(self, range_name: str, values):
        while not self.rows:
            self.rows.append([])
        self.rows[0] = list(values[0])

    def get_all_records(self):
        if not self.rows:
            return []
        headers = [str(value) for value in self.rows[0]]
        return [
            dict(zip(headers, list(row) + [""] * (len(headers) - len(row))))
            for row in self.rows[1:]
        ]

    def append_rows(self, values, value_input_option=None):
        self.rows.extend([list(row) for row in values])


class FakeSpreadsheet:
    def __init__(self) -> None:
        self.worksheets: dict[str, FakeWorksheet] = {}

    def worksheet(self, title: str):
        if title not in self.worksheets:
            raise WorksheetNotFound(title)
        return self.worksheets[title]

    def add_worksheet(self, *, title: str, rows: int, cols: int):
        worksheet = FakeWorksheet(title, rows, cols)
        self.worksheets[title] = worksheet
        return worksheet


class FakeClient:
    def __init__(self) -> None:
        self.spreadsheet = FakeSpreadsheet()

    def open_by_key(self, sheet_id: str):
        return self.spreadsheet


class PriorityPortfolioPersistenceTests(unittest.TestCase):
    def test_portfolio_is_deduplicated_and_reuses_completed_fichas(self) -> None:
        client = FakeClient()
        batch_id = create_priority_portfolio(
            client,
            sheet_id="sheet",
            requested_by="rsanchez",
            scope_id="scope-1",
            records=[
                {
                    "ficha": "43358",
                    "nombre_ficha": "Circuito",
                    "rank_score": 2,
                    "rank_monto_ficha_unica": pd.NA,
                    "rank_actos_ficha_unica": 4,
                    "criterios_seleccion": "Score, Actos",
                },
                {"ficha": "43358", "nombre_ficha": "Duplicada"},
                {"ficha": "100523", "nombre_ficha": "Otra", "rank_score": 3},
            ],
            completed_fichas={"43358"},
        )

        rows = list_priority_portfolio(client, sheet_id="sheet", batch_id=batch_id)

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            client.spreadsheet.worksheets["intel_priority_portfolio"].rows[0],
            INTEL_PRIORITY_PORTFOLIO_HEADERS,
        )
        by_ficha = {row["ficha"]: row for row in rows}
        self.assertEqual(by_ficha["43358"]["estado"], "completado_previo")
        self.assertEqual(by_ficha["100523"]["estado"], "pendiente")
        self.assertEqual(by_ficha["43358"]["rank_monto_ficha_unica"], "")


if __name__ == "__main__":
    unittest.main()
