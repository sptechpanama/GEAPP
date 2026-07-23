from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.inteligencia_orquestador_v3 import (  # noqa: E402
    INTEL_REMOTE_DETAIL_HEADERS,
    INTEL_REMOTE_DETAIL_WORKSHEET,
    INTEL_REMOTE_RUNS_HEADERS,
    INTEL_REMOTE_RUNS_WORKSHEET,
    INTEL_TRACKING_HEADERS,
    INTEL_TRACKING_WORKSHEET,
    WorksheetNotFound,
    get_study_result,
    list_study_runs,
    list_tracking_fichas,
    remove_tracking_ficha,
    upsert_tracking_ficha,
)


class FakeWorksheet:
    def __init__(self, headers: list[str] | None = None) -> None:
        self.rows = [list(headers or [])] if headers else []

    def row_values(self, row: int):
        return list(self.rows[row - 1]) if 0 < row <= len(self.rows) else []

    def get_all_records(self):
        if not self.rows:
            return []
        headers = self.rows[0]
        return [
            dict(zip(headers, row + [""] * (len(headers) - len(row))))
            for row in self.rows[1:]
        ]

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
        self.sheets: dict[str, FakeWorksheet] = {}

    def worksheet(self, title: str):
        if title not in self.sheets:
            raise WorksheetNotFound(title)
        return self.sheets[title]

    def add_worksheet(self, title: str, rows: int, cols: int):
        sheet = FakeWorksheet()
        self.sheets[title] = sheet
        return sheet


class FakeClient:
    def __init__(self) -> None:
        self.book = FakeSpreadsheet()

    def open_by_key(self, sheet_id: str):
        return self.book


def _values(headers: list[str], record: dict[str, object]) -> list[object]:
    return [record.get(column, "") for column in headers]


class TrackingAndResultsTests(unittest.TestCase):
    def test_tracking_upsert_is_persistent_unique_and_removable(self) -> None:
        client = FakeClient()
        first = upsert_tracking_ficha(
            client,
            sheet_id="sheet",
            record={
                "ficha": "100523*",
                "nombre_ficha": "CINTA INDICADORA",
                "estado": "pendiente de estudio profundo",
                "actos": 7,
            },
        )
        created_at = first["created_at"]
        updated = upsert_tracking_ficha(
            client,
            sheet_id="sheet",
            record={
                "ficha": "100523",
                "estado": "en estudio",
                "notas": "Validar proveedores",
            },
        )
        self.assertEqual(updated["created_at"], created_at)
        records = list_tracking_fichas(client, sheet_id="sheet")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["ficha"], "100523")
        self.assertEqual(records[0]["nombre_ficha"], "CINTA INDICADORA")
        self.assertEqual(records[0]["estado"], "en estudio")
        self.assertEqual(records[0]["notas"], "Validar proveedores")
        self.assertTrue(
            remove_tracking_ficha(client, sheet_id="sheet", ficha="100523*")
        )
        self.assertEqual(list_tracking_fichas(client, sheet_id="sheet"), [])
        self.assertIn(INTEL_TRACKING_WORKSHEET, client.book.sheets)
        self.assertEqual(
            client.book.sheets[INTEL_TRACKING_WORKSHEET].rows[0],
            INTEL_TRACKING_HEADERS,
        )

    def test_result_lookup_uses_request_and_latest_run(self) -> None:
        client = FakeClient()
        runs = FakeWorksheet(INTEL_REMOTE_RUNS_HEADERS)
        runs.append_row(
            _values(
                INTEL_REMOTE_RUNS_HEADERS,
                {
                    "request_id": "req-old",
                    "run_id_remote": "run-old",
                    "ficha": "43358",
                    "nombre_ficha": "KIT DE CIRCUITO",
                    "estado_run": "completada",
                    "fecha_fin": "2026-07-20T10:00:00",
                },
            )
        )
        runs.append_row(
            _values(
                INTEL_REMOTE_RUNS_HEADERS,
                {
                    "request_id": "req-new",
                    "run_id_remote": "run-new",
                    "ficha": "43358",
                    "nombre_ficha": "KIT DE CIRCUITO",
                    "estado_run": "completada",
                    "fecha_fin": "2026-07-22T10:00:00",
                },
            )
        )
        detail = FakeWorksheet(INTEL_REMOTE_DETAIL_HEADERS)
        detail.append_row(
            _values(
                INTEL_REMOTE_DETAIL_HEADERS,
                {
                    "request_id": "req-old",
                    "run_id_remote": "run-old",
                    "ficha": "43358",
                    "acto_id": "ACTO-1",
                    "proveedor": "Proveedor anterior",
                },
            )
        )
        detail.append_row(
            _values(
                INTEL_REMOTE_DETAIL_HEADERS,
                {
                    "request_id": "req-new",
                    "run_id_remote": "run-new",
                    "ficha": "43358",
                    "acto_id": "ACTO-2",
                    "proveedor": "Proveedor reciente",
                },
            )
        )
        client.book.sheets[INTEL_REMOTE_RUNS_WORKSHEET] = runs
        client.book.sheets[INTEL_REMOTE_DETAIL_WORKSHEET] = detail

        listed = list_study_runs(client, sheet_id="sheet", ficha="43358")
        self.assertEqual(
            [item["request_id"] for item in listed], ["req-new", "req-old"]
        )
        latest, latest_detail = get_study_result(
            client, sheet_id="sheet", ficha="43358"
        )
        self.assertEqual(latest["request_id"], "req-new")
        self.assertEqual(
            [item["acto_id"] for item in latest_detail], ["ACTO-2"]
        )
        old, old_detail = get_study_result(
            client,
            sheet_id="sheet",
            ficha="43358",
            request_id="req-old",
        )
        self.assertEqual(old["run_id_remote"], "run-old")
        self.assertEqual(old_detail[0]["proveedor"], "Proveedor anterior")

    def test_missing_result_sheets_are_read_only_and_return_empty(self) -> None:
        client = FakeClient()
        run, details = get_study_result(
            client, sheet_id="sheet", ficha="99999"
        )
        self.assertEqual(run, {})
        self.assertEqual(details, [])
        self.assertNotIn(INTEL_REMOTE_RUNS_WORKSHEET, client.book.sheets)
        self.assertNotIn(INTEL_REMOTE_DETAIL_WORKSHEET, client.book.sheets)


if __name__ == "__main__":
    unittest.main()
