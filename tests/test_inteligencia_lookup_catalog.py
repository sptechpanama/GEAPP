import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from time import monotonic
from unittest.mock import MagicMock, patch

import pandas as pd

from services.inteligencia_lookup_catalog import COLUMNS, POLICY, DirectFichaLookup


class LookupCatalogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.seed = self.root / "seed.json"
        self.stamp = "2026-11-15T11:09:03+00:00"
        self.frame = pd.DataFrame([{"ficha": "43358", "nombre_ficha": "Circuito anestesia", "clase_riesgo": "B", "enlace_minsa": "https://ctni.minsa.gob.pa/example"}])
        self.seed.write_text(json.dumps({"policy": POLICY, "checked_at": self.stamp, "records": self.frame.to_dict("records")}), encoding="utf-8")
        self.repository = MagicMock()
        self.repository.ficha_search_options.return_value = self.frame.copy()
        self.factory = MagicMock(return_value=self.repository)
        self.executor = ThreadPoolExecutor(max_workers=1)
        for name, value in (("_WORKERS", self.executor), ("_SLOTS", BoundedSemaphore(1))):
            patcher = patch(f"services.inteligencia_lookup_catalog.{name}", value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.addCleanup(self.executor.shutdown, wait=True)

    def lookup(self, source="example"):
        return DirectFichaLookup(self.factory, source_key=source, seed_path=self.seed, cache_dir=self.root / "cache")

    def complete_refresh(self, lookup):
        lookup.catalog_snapshot()
        lookup._future.result(timeout=3)
        return lookup.catalog_snapshot()

    def test_first_load_does_not_wait_for_connection_and_refresh_is_single_flight(self):
        release = Event()
        entered = Event()

        def connect():
            entered.set()
            release.wait(2)
            return self.repository

        self.factory.side_effect = connect
        lookup = self.lookup()
        self.factory.assert_not_called()
        try:
            start = monotonic()
            frame = lookup.catalog_snapshot()
            self.assertLess(monotonic() - start, 0.5)
            self.assertTrue(entered.wait(1))
            self.assertEqual(frame.ficha.tolist(), ["43358"])
            self.assertEqual(frame.attrs["catalog_origin"], "backup")
            self.assertEqual(frame.attrs["catalog_checked_at"], self.stamp)
            for repeat in range(8):
                lookup.catalog_snapshot()
            self.factory.assert_called_once()
        finally:
            release.set()
        lookup._future.result(timeout=3)
        self.assertEqual(lookup.catalog_snapshot().attrs["catalog_origin"], "database")

    def test_failure_preserves_names_and_date_without_automatic_retry_storm(self):
        self.factory.side_effect = TimeoutError("connection unavailable")
        lookup = self.lookup()
        failed = self.complete_refresh(lookup)
        self.assertEqual(failed.ficha.tolist(), ["43358"])
        self.assertTrue(failed.attrs["catalog_failed"])
        self.assertEqual(failed.attrs["catalog_checked_at"], self.stamp)
        for repeat in range(8):
            lookup.catalog_snapshot()
        self.factory.assert_called_once()
        self.factory.side_effect = None
        lookup.retry_catalog()
        recovered = self.complete_refresh(lookup)
        self.assertFalse(recovered.attrs["catalog_failed"])
        self.assertEqual(recovered.attrs["catalog_origin"], "database")
        self.assertEqual(self.factory.call_count, 2)

    def test_verified_snapshot_survives_new_instance_and_is_isolated_by_source(self):
        updated = self.frame.copy()
        updated.loc[0, "nombre_ficha"] = "Nombre actualizado"
        self.repository.ficha_search_options.return_value = updated
        first = self.lookup()
        fresh = self.complete_refresh(first)
        self.factory.side_effect = TimeoutError("offline")
        second = self.lookup()
        backup = self.complete_refresh(second)
        self.assertEqual(backup.nombre_ficha.iloc[0], "Nombre actualizado")
        self.assertEqual(backup.attrs["catalog_checked_at"], fresh.attrs["catalog_checked_at"])
        other = self.lookup(source="different")
        self.assertEqual(other._frame.nombre_ficha.iloc[0], "Circuito anestesia")

    def test_empty_or_invalid_refresh_never_replaces_good_snapshot(self):
        lookup = self.lookup()
        good = self.complete_refresh(lookup)
        for invalid in (pd.DataFrame(columns=COLUMNS), pd.concat([self.frame, self.frame]), pd.DataFrame([{"ficha": "wrong"}])):
            self.repository.ficha_search_options.return_value = invalid
            lookup.retry_catalog()
            result = self.complete_refresh(lookup)
            self.assertEqual(result.ficha.tolist(), ["43358"])
            self.assertEqual(result.attrs["catalog_checked_at"], good.attrs["catalog_checked_at"])
            self.assertTrue(result.attrs["catalog_failed"])

    def test_unwritable_disk_preserves_successful_memory_read(self):
        (self.root / "cache").write_text("not a directory", encoding="utf-8")
        result = self.complete_refresh(self.lookup())
        self.assertFalse(result.attrs["catalog_failed"])
        self.assertEqual(result.attrs["catalog_origin"], "database")

    def test_corrupt_persisted_file_falls_back_to_packaged_catalog(self):
        lookup = self.lookup()
        lookup._cache_path.parent.mkdir(parents=True)
        for payload in ("not json", "[]", "null", "42", '{"policy":"other"}'):
            with self.subTest(payload=payload):
                lookup._cache_path.write_text(payload, encoding="utf-8")
                reloaded = self.lookup()
                self.assertEqual(reloaded._frame.ficha.tolist(), ["43358"])

    def test_failed_replace_and_cleanup_do_not_discard_valid_remote_read(self):
        with patch("services.inteligencia_lookup_catalog.os.replace", side_effect=PermissionError), patch("services.inteligencia_lookup_catalog.Path.unlink", side_effect=PermissionError):
            result = self.complete_refresh(self.lookup())
        self.assertFalse(result.attrs["catalog_failed"])
        self.assertEqual(result.attrs["catalog_origin"], "database")

    def test_missing_seed_has_no_invented_options_and_recovers_from_database(self):
        self.seed.unlink()
        lookup = self.lookup()
        self.assertTrue(lookup._frame.empty)
        result = self.complete_refresh(lookup)
        self.assertEqual(result.ficha.tolist(), ["43358"])

    def test_ficha_acts_are_read_from_repository_not_from_catalog_backup(self):
        lookup = self.lookup()
        expected = pd.DataFrame([{"acto_key": "real-act"}])
        self.repository.all_acts_for_ficha.return_value = expected
        self.assertIs(lookup.all_acts_for_ficha("43358"), expected)
        self.repository.all_acts_for_ficha.assert_called_once_with("43358")
        lookup.price_benchmarks_for_fichas(("43358",))
        self.repository.price_benchmarks_for_fichas.assert_called_once_with(("43358",))
        self.factory.assert_called_once()

    def test_packaged_seed_has_valid_policy_and_real_catalog_structure(self):
        seed = Path(__file__).resolve().parents[1] / "assets" / "inteligencia" / "ficha_search_catalog.json"
        lookup = DirectFichaLookup(self.factory, source_key="packaged", seed_path=seed, cache_dir=self.root / "packaged-cache")
        self.assertGreater(len(lookup._frame), 3000)
        self.assertIn("43358", lookup._frame.ficha.tolist())
        self.factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
