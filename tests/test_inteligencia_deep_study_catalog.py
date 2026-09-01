from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.inteligencia_proveedores_v3 import (  # noqa: E402
    build_deep_study_catalog,
)


class DeepStudyCatalogTests(unittest.TestCase):
    def test_saved_and_tracked_fichas_survive_current_catalog_filters(self) -> None:
        current = pd.DataFrame(
            [
                {
                    "ficha": "43358",
                    "nombre_ficha": "KIT DE CIRCUITO DE PACIENTE",
                    "clase_riesgo": "B",
                    "enlace_minsa": "https://ctni.minsa.gob.pa/ficha/43358",
                }
            ]
        )
        tracking = [
            {
                "ficha": "100523*",
                "nombre_ficha": "CINTA INDICADORA",
                "score_inicial": "88.5",
                "actos": "47",
                "actos_solo_ficha": "31",
            }
        ]
        runs = [
            {
                "ficha": "100523",
                "nombre_ficha": "CINTA INDICADORA",
                "fecha_fin": "2026-08-31T10:00:00",
            },
            {
                "ficha": "99999",
                "nombre_ficha": "FICHA HISTÓRICA FUERA DEL RANKING",
                "fecha_fin": "2026-08-30T09:00:00",
            },
        ]

        result = build_deep_study_catalog(current, tracking, runs).set_index("ficha")

        self.assertEqual(set(result.index), {"43358", "100523", "99999"})
        self.assertTrue(bool(result.loc["100523", "tiene_estudio"]))
        self.assertTrue(bool(result.loc["100523", "en_seguimiento"]))
        self.assertEqual(int(result.loc["100523", "actos"]), 47)
        self.assertEqual(int(result.loc["100523", "actos_ficha_unica"]), 31)
        self.assertTrue(bool(result.loc["99999", "tiene_estudio"]))
        self.assertEqual(
            result.loc["99999", "nombre_ficha"],
            "FICHA HISTÓRICA FUERA DEL RANKING",
        )

    def test_current_metadata_has_priority_and_runs_are_deduplicated(self) -> None:
        current = pd.DataFrame(
            [
                {
                    "ficha": "43358",
                    "nombre_ficha": "NOMBRE OFICIAL ACTUAL",
                    "clase_riesgo": "B",
                    "enlace_minsa": "https://ctni.minsa.gob.pa/ficha/43358",
                }
            ]
        )
        runs = [
            {
                "ficha": "43358*",
                "nombre_ficha": "NOMBRE ANTERIOR",
                "fecha_fin": "2026-08-01T10:00:00",
            },
            {
                "ficha": "43358",
                "nombre_ficha": "OTRO NOMBRE ANTERIOR",
                "fecha_fin": "2026-08-31T10:00:00",
            },
        ]

        result = build_deep_study_catalog(current, (), runs)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["nombre_ficha"], "NOMBRE OFICIAL ACTUAL")
        self.assertEqual(result.iloc[0]["ultima_ejecucion"], "2026-08-31T10:00:00")
        self.assertTrue(bool(result.iloc[0]["tiene_estudio"]))

    def test_empty_sources_return_a_stable_schema(self) -> None:
        result = build_deep_study_catalog(pd.DataFrame(), (), ())

        self.assertTrue(result.empty)
        self.assertIn("ficha", result.columns)
        self.assertIn("score_oportunidad", result.columns)
        self.assertIn("tiene_estudio", result.columns)


if __name__ == "__main__":
    unittest.main()
