import ast
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import streamlit as st
from streamlit.testing.v1 import AppTest


ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "pages" / "inteligencia_oportunidades_proveedores.py"
FUNCTIONS = {
    "_all_acts_data", "_ficha_search_options", "_price_benchmark_data",
    "_price_benchmark_for_fichas", "_enrich_acts_with_price_benchmark",
    "_price_benchmark_column_config", "_render_price_benchmark_summary",
    "_render_price_benchmark_legend", "_money", "_render_direct_ficha_lookup",
    "_direct_lookup_labels", "_remember_direct_lookup",
    "_catalog_option_labels", "_retry_direct_lookup_catalog",
}


def application_source():
    module = ast.parse(PAGE.read_text(encoding="utf-8"))
    functions = "\n\n".join(ast.unparse(node) for node in module.body if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS)
    return f'''
import sys, re, logging
from datetime import date, datetime, timezone
import pandas as pd
import streamlit as st
sys.path.insert(0, {str(ROOT)!r})
from services.inteligencia_proveedores_v3 import AnalyticsFilters, AnalyticsRepository, normalize_ficha_list, intelligence_view_frame, dataframe_to_csv_bytes
ANALYTICS_REPOSITORY_API_VERSION = {str(uuid.uuid4())!r}
PRICE_BENCHMARK_VALUE_COLUMNS = ("unidad_comparable", "precio_participacion_tipico")

class FakeRepository:
    def ficha_search_options(self):
        st.session_state["catalog_calls"] = st.session_state.get("catalog_calls", 0) + 1
        if st.session_state.get("fail_catalog"):
            raise RuntimeError("catalog unavailable")
        if st.session_state.get("empty_catalog"):
            return pd.DataFrame(columns=["ficha", "nombre_ficha"])
        return pd.DataFrame([{{"ficha":"43358", "nombre_ficha":"Circuito anestesia"}}, {{"ficha":"107110", "nombre_ficha":"Cilindro"}}, *[{{"ficha":str(100001 + index), "nombre_ficha":f"Producto {{index}}"}} for index in range(4)]])

    def all_acts_for_ficha(self, ficha):
        st.session_state["act_calls"] = st.session_state.get("act_calls", 0) + 1
        if st.session_state.get("fail_acts"):
            raise RuntimeError("temporary timeout")
        count = 123 if ficha == "43358" else 2
        return pd.DataFrame([{{"acto_key":f"{{ficha}}-{{index}}", "enlace":f"https://example.test/{{index}}", "titulo":f"Acto {{index}}", "reference_amount_attributed":10.0, "award_amount_attributed":8.0, "publication_date":"2026-11-01", "is_unique_ficha":1}} for index in range(count)])

    def price_benchmarks_for_fichas(self, fichas):
        st.session_state["price_calls"] = st.session_state.get("price_calls", 0) + 1
        return pd.DataFrame()

{functions}

_render_direct_ficha_lookup(FakeRepository())
'''


class DirectLookupUITests(unittest.TestCase):
    def setUp(self):
        source = application_source().encode("ascii", "backslashreplace").decode("ascii")
        self.app = AppTest.from_string(source, default_timeout=15).run()
        self.assertEqual(len(self.app.exception), 0)

    def submit(self, index=0):
        self.app.selectbox(key="intel_v3_direct_lookup_selector_v2").select_index(index)
        next(button for button in self.app.button if button.label == "Consultar ficha").click().run()
        self.assertEqual(len(self.app.exception), 0)

    def test_lookup_paginates_without_requery_and_preserves_full_totals(self):
        self.assertNotIn("act_calls", self.app.session_state.filtered_state)
        self.submit()
        self.assertEqual(self.app.metric[1].value, "123")
        self.assertEqual(self.app.metric[2].value, "$1,230.00")
        self.assertEqual(len(self.app.dataframe[0].value), 50)
        self.assertEqual(self.app.session_state["act_calls"], 1)
        self.assertNotIn("price_calls", self.app.session_state.filtered_state)
        self.app.number_input(key="intel_v3_direct_lookup_page").set_value(3).run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(len(self.app.dataframe[0].value), 23)
        self.assertEqual(self.app.dataframe[0].value.iloc[0]["acto_key"], "43358-100")
        self.assertEqual(self.app.session_state["act_calls"], 1)
        self.assertEqual(self.app.session_state["catalog_calls"], 1)

    def test_refresh_failure_keeps_same_ficha_but_not_other_fichas(self):
        self.submit()
        timestamp = self.app.session_state["intel_v3_direct_lookup_snapshot"]["read_at"]
        self.app.session_state["fail_acts"] = True
        self.app.button(key="intel_v3_direct_lookup_refresh").click().run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(self.app.metric[1].value, "123")
        self.assertTrue(any("último resultado" in warning.value for warning in self.app.warning))
        self.assertEqual(self.app.session_state["intel_v3_direct_lookup_snapshot"]["read_at"], timestamp)
        self.submit(1)
        self.assertEqual(len(self.app.dataframe), 0)
        self.assertEqual(len(self.app.error), 1)

    def test_switch_ficha_resets_page_and_clear_removes_snapshot(self):
        self.submit()
        self.app.number_input(key="intel_v3_direct_lookup_page").set_value(3).run()
        self.submit(1)
        self.assertEqual(self.app.number_input(key="intel_v3_direct_lookup_page").value, 1)
        self.assertEqual(self.app.metric[1].value, "2")
        next(button for button in self.app.button if button.label == "Limpiar").click().run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(len(self.app.dataframe), 0)
        self.assertNotIn("intel_v3_direct_lookup_snapshot", self.app.session_state.filtered_state)

    def test_prices_are_loaded_only_when_requested(self):
        self.submit()
        self.app.toggle(key="intel_v3_direct_lookup_prices").set_value(True).run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(self.app.session_state["price_calls"], 1)

    def test_pagination_uses_session_snapshot_even_after_shared_cache_expires(self):
        self.submit()
        st.cache_data.clear()
        self.app.session_state["fail_acts"] = True
        self.app.number_input(key="intel_v3_direct_lookup_page").set_value(2).run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(self.app.session_state["act_calls"], 1)
        self.assertEqual(len(self.app.dataframe[0].value), 50)
        self.assertEqual(self.app.dataframe[0].value.iloc[0]["acto_key"], "43358-50")

    def test_return_to_recent_ficha_never_waits_for_remote_after_shared_cache_expires(self):
        self.submit()
        timestamp = self.app.session_state["intel_v3_direct_lookup_snapshot"]["read_at"]
        self.submit(1)
        st.cache_data.clear()
        self.app.session_state["fail_acts"] = True
        self.app.session_state["fail_catalog"] = True
        self.submit()
        self.assertEqual(self.app.metric[1].value, "123")
        self.assertEqual(self.app.session_state["act_calls"], 2)
        self.assertEqual(self.app.session_state["catalog_calls"], 1)
        self.assertEqual(self.app.session_state["intel_v3_direct_lookup_snapshot"]["read_at"], timestamp)
        self.assertEqual(len(self.app.error), 0)

    def test_expired_catalog_keeps_labels_and_does_not_retry_on_every_widget(self):
        self.submit()
        self.app.session_state["intel_v3_direct_lookup_catalog"]["retry_at"] = 0
        st.cache_data.clear()
        self.app.session_state["fail_catalog"] = True
        self.app.number_input(key="intel_v3_direct_lookup_page").set_value(2).run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(len(self.app.selectbox(key="intel_v3_direct_lookup_selector_v2").options), 6)
        self.assertEqual(self.app.session_state["catalog_calls"], 2)
        self.app.number_input(key="intel_v3_direct_lookup_page").set_value(3).run()
        self.assertEqual(self.app.session_state["catalog_calls"], 2)
        self.assertEqual(self.app.session_state["act_calls"], 1)

    def test_recent_history_is_bounded_and_clear_removes_it(self):
        for index in range(6):
            self.submit(index)
        history = self.app.session_state["intel_v3_direct_lookup_history"]
        self.assertEqual(len(history), 4)
        self.assertNotIn("43358", history)
        self.assertNotIn("107110", history)
        next(button for button in self.app.button if button.label == "Limpiar").click().run()
        self.assertNotIn("intel_v3_direct_lookup_history", self.app.session_state.filtered_state)

    def test_submit_same_ficha_uses_snapshot_but_refresh_queries_database(self):
        self.submit()
        st.cache_data.clear()
        self.submit()
        self.assertEqual(self.app.session_state["act_calls"], 1)
        self.app.button(key="intel_v3_direct_lookup_refresh").click().run()
        self.assertEqual(self.app.session_state["act_calls"], 2)
        self.assertEqual(len(self.app.exception), 0)

    def test_empty_catalog_does_not_remove_previously_loaded_suggestions(self):
        self.app.session_state["intel_v3_direct_lookup_catalog"]["retry_at"] = 0
        self.app.session_state["empty_catalog"] = True
        st.cache_data.clear()
        self.app.run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(len(self.app.selectbox(key="intel_v3_direct_lookup_selector_v2").options), 6)
        self.assertTrue(self.app.session_state["intel_v3_direct_lookup_catalog"]["failed"])
        self.app.session_state["empty_catalog"] = False
        self.app.session_state["intel_v3_direct_lookup_catalog"]["retry_at"] = 0
        self.app.run()
        self.assertFalse(self.app.session_state["intel_v3_direct_lookup_catalog"]["failed"])
        self.assertEqual(self.app.session_state["catalog_calls"], 3)

    def test_history_has_memory_budget_and_drops_previous_code_versions(self):
        module = ast.parse(PAGE.read_text(encoding="utf-8"))
        function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "_remember_direct_lookup")
        state = {"intel_v3_direct_lookup_history": {"old": {"version": "previous"}}}
        namespace = {"st": SimpleNamespace(session_state=state), "ANALYTICS_REPOSITORY_API_VERSION": "current"}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(PAGE), "exec"), namespace)
        snapshot = {"ficha": "43358", "version": "current", "acts": pd.DataFrame([{"large": "x" * (8 * 1024 * 1024)}])}
        namespace["_remember_direct_lookup"](snapshot)
        self.assertEqual(state["intel_v3_direct_lookup_history"], {})
        self.assertIs(state["intel_v3_direct_lookup_snapshot"], snapshot)

    def test_backup_catalog_remains_searchable_when_remote_is_unavailable(self):
        source = application_source().replace(
            "_render_direct_ficha_lookup(FakeRepository())",
            '''
class SnapshotRepository(FakeRepository):
    def catalog_snapshot(self):
        result = pd.DataFrame([{"ficha":"43358", "nombre_ficha":"Circuito anestesia"}])
        result.attrs.update(catalog_origin="backup", catalog_checked_at="2026-11-15T11:09:03+00:00", catalog_failed=not st.session_state.get("catalog_retried", False))
        return result

    def retry_catalog(self):
        st.session_state["catalog_retried"] = True

_render_direct_ficha_lookup(SnapshotRepository())
''',
        )
        self.app = AppTest.from_string(source.encode("ascii", "backslashreplace").decode("ascii"), default_timeout=15).run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertEqual(len(self.app.warning), 0)
        self.assertEqual(self.app.selectbox(key="intel_v3_direct_lookup_selector_v2").options, ["43358 | Circuito anestesia"])
        self.assertNotIn("catalog_calls", self.app.session_state.filtered_state)
        self.assertTrue(any("15/11/2026 06:09" in caption.value for caption in self.app.caption))
        self.submit()
        self.assertEqual(self.app.metric[1].value, "123")
        self.app.button(key="intel_v3_direct_lookup_retry_catalog").click().run()
        self.assertEqual(len(self.app.exception), 0)
        self.assertTrue(self.app.session_state["catalog_retried"])
        self.assertEqual(self.app.session_state["act_calls"], 1)


if __name__ == "__main__":
    unittest.main()
