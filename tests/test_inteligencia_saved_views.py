from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.inteligencia_orquestador_v3 import (  # noqa: E402
    INTEL_VIEWS_HEADERS,
    delete_saved_view,
    list_saved_views,
    save_saved_view,
)


class FakeWorksheet:
    def __init__(self) -> None:
        self.rows = [list(INTEL_VIEWS_HEADERS)]

    def row_values(self, row: int):
        return list(self.rows[row - 1]) if 0 < row <= len(self.rows) else []

    def get_all_records(self):
        headers = self.rows[0]
        return [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in self.rows[1:]]

    def append_row(self, values, value_input_option=None):
        self.rows.append(list(values))

    def update(self, range_name: str, values):
        match = re.search(r"(\d+)", range_name)
        row = int(match.group(1)) if match else 1
        while len(self.rows) < row:
            self.rows.append([])
        self.rows[row - 1] = list(values[0])

    def delete_rows(self, row: int):
        self.rows.pop(row - 1)


class FakeSpreadsheet:
    def __init__(self) -> None:
        self.sheet = FakeWorksheet()

    def worksheet(self, title: str):
        return self.sheet


class FakeClient:
    def __init__(self) -> None:
        self.book = FakeSpreadsheet()

    def open_by_key(self, sheet_id: str):
        return self.book


class SavedViewsTests(unittest.TestCase):
    def test_saved_views_are_isolated_updated_and_deleted_by_user(self) -> None:
        client = FakeClient()
        first_id = save_saved_view(
            client,
            sheet_id="sheet",
            username="rsanchez",
            name="Mercado 2026",
            payload={"fecha_desde": "2026-01-01", "score_weights": {"demanda": 28}},
        )
        save_saved_view(
            client,
            sheet_id="sheet",
            username="isanchez",
            name="Mercado 2026",
            payload={"fecha_desde": "2025-01-01"},
        )
        updated_id = save_saved_view(
            client,
            sheet_id="sheet",
            username="rsanchez",
            name="Mercado 2026",
            payload={"fecha_desde": "2026-02-01", "entidades": ["CSS"]},
        )
        self.assertEqual(first_id, updated_id)
        views = list_saved_views(client, sheet_id="sheet", username="rsanchez")
        self.assertEqual(len(views), 1)
        self.assertEqual(views[0]["payload"]["fecha_desde"], "2026-02-01")
        self.assertEqual(views[0]["payload"]["entidades"], ["CSS"])
        self.assertTrue(delete_saved_view(client, sheet_id="sheet", username="rsanchez", view_id=first_id))
        self.assertEqual(list_saved_views(client, sheet_id="sheet", username="rsanchez"), [])
        self.assertEqual(len(list_saved_views(client, sheet_id="sheet", username="isanchez")), 1)


if __name__ == "__main__":
    unittest.main()
