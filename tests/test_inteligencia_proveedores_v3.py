from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.inteligencia_proveedores_v3 import (  # noqa: E402
    AnalyticsFilters,
    AnalyticsRepository,
    apply_master_filters,
    preset_range,
    score_opportunities,
    sort_and_page,
    split_search_groups,
)


class ServiceUnitTests(unittest.TestCase):
    def test_search_groups_preserve_phrases(self) -> None:
        self.assertEqual(
            split_search_groups("Chiller, refrigeración, aires acondicionados"),
            ("chiller", "refrigeracion", "aires acondicionados"),
        )

    def test_period_2026_is_bounded_by_today(self) -> None:
        self.assertEqual(preset_range("2026", today=date(2026, 7, 22)), (date(2026, 1, 1), date(2026, 7, 22)))

    def test_scoring_and_global_sort_are_deterministic(self) -> None:
        frame = pd.DataFrame(
            [
                {"ficha": "A", "actos": 20, "actos_ficha_unica": 10, "entidades": 5, "meses_activos": 8, "monto_referencia": 200000, "monto_adjudicado": 150000, "ticket_promedio": 10000, "participantes_promedio": 1.2, "proporcion_unico_proponente": .6, "proponentes_distintos": 3, "proveedores_catalogo": 2, "proveedores_contactables": 2, "confianza_deteccion": 98, "cobertura_monto_adjudicado_pct": 90, "concentracion_hhi": 3500, "tendencia_6m_pct": 20, "tiene_ct": "Si", "registro_sanitario": "No", "nombre_ficha": "Ficha A", "enlace_minsa": "https://a"},
                {"ficha": "B", "actos": 2, "actos_ficha_unica": 0, "entidades": 1, "meses_activos": 1, "monto_referencia": 1000, "monto_adjudicado": 0, "ticket_promedio": 500, "participantes_promedio": 5, "proporcion_unico_proponente": 0, "proponentes_distintos": 8, "proveedores_catalogo": 0, "proveedores_contactables": 0, "confianza_deteccion": 70, "cobertura_monto_adjudicado_pct": 0, "concentracion_hhi": 9000, "tendencia_6m_pct": -50, "tiene_ct": "No", "registro_sanitario": "Si", "nombre_ficha": "Ficha B", "enlace_minsa": ""},
            ]
        )
        scored = score_opportunities(frame)
        self.assertGreater(scored.loc[scored.ficha.eq("A"), "score_oportunidad"].iloc[0], scored.loc[scored.ficha.eq("B"), "score_oportunidad"].iloc[0])
        page, pages, total = sort_and_page(scored, sort_by="monto_referencia", ascending=False, page=1, page_size=1)
        self.assertEqual((pages, total, page.iloc[0]["ficha"]), (2, 2, "A"))
        self.assertEqual(len(apply_master_filters(scored, min_acts=10)), 1)

    def test_opportunity_score_uses_only_the_five_declared_dimensions(self) -> None:
        frame = pd.DataFrame(
            [
                {"ficha": "A", "actos": 10, "actos_ficha_unica": 8, "entidades": 4, "meses_activos": 6, "monto_referencia": 100000, "monto_adjudicado": 80000, "ticket_promedio": 10000, "ticket_mediano": 9000, "participantes_promedio": 1.5, "participantes_mediana": 1, "proponentes_distintos": 3, "proveedores_catalogo": 2, "proveedores_contactables": 2, "confianza_deteccion": 95, "cobertura_monto_referencia_pct": 100, "cobertura_monto_adjudicado_pct": 80, "cobertura_ganador_pct": 80, "cobertura_participantes_pct": 90, "pct_ficha_unica": 80, "tiene_ct": "Si", "registro_sanitario": "No", "nombre_ficha": "A", "enlace_minsa": "https://a"},
                {"ficha": "B", "actos": 2, "actos_ficha_unica": 0, "entidades": 1, "meses_activos": 1, "monto_referencia": 1000, "monto_adjudicado": 0, "ticket_promedio": 500, "ticket_mediano": 500, "participantes_promedio": 5, "participantes_mediana": 5, "proponentes_distintos": 8, "proveedores_catalogo": 0, "proveedores_contactables": 0, "confianza_deteccion": 70, "cobertura_monto_referencia_pct": 50, "cobertura_monto_adjudicado_pct": 0, "cobertura_ganador_pct": 0, "cobertura_participantes_pct": 50, "pct_ficha_unica": 0, "tiene_ct": "No", "registro_sanitario": "Si", "nombre_ficha": "B", "enlace_minsa": ""},
            ]
        )
        scored = score_opportunities(frame)
        expected = (
            scored["score_demanda"] * 0.28
            + scored["score_economia"] * 0.27
            + scored["score_competencia"] * 0.18
            + scored["score_viabilidad"] * 0.17
            + scored["score_complejidad"] * 0.10
        ).round(1)
        pd.testing.assert_series_equal(scored["score_oportunidad"], expected, check_names=False)
        self.assertTrue(scored["score_confianza"].between(0, 100).all())


class RepositoryIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "analytics.db"
        connection = sqlite3.connect(self.db_path)
        connection.executescript(
            """
            CREATE TABLE intel_actos_fichas (
                acto_key TEXT, source_id TEXT, ficha TEXT, is_unique_ficha INTEGER,
                detected_ficha_count INTEGER, detection_score REAL, detection_method TEXT,
                detection_field TEXT, detection_evidence TEXT, detector_version TEXT,
                catalog_version TEXT, enlace TEXT, titulo TEXT, entidad TEXT,
                unidad_solicitante TEXT, estado TEXT, publication_date TEXT,
                celebration_date TEXT, celebration_end_date TEXT, award_date TEXT,
                update_date TEXT, reference_amount REAL, award_amount REAL,
                award_amount_source TEXT, winner TEXT, winner_short TEXT, participant_count INTEGER
                , search_text_norm TEXT
            );
            CREATE TABLE intel_acto_proponentes (
                acto_key TEXT, source_id TEXT, ordinal INTEGER, proveedor TEXT,
                proveedor_norm TEXT, offered_amount REAL, is_winner INTEGER
            );
            CREATE TABLE intel_ficha_metadata (
                ficha TEXT, nombre_ficha TEXT, descripcion TEXT, area TEXT,
                tipo_producto TEXT, especialidad TEXT, tiene_ct TEXT,
                registro_sanitario TEXT, enlace_minsa TEXT, metadata_source TEXT
                , search_text_norm TEXT
            );
            CREATE TABLE intel_ficha_catalogo (
                ficha TEXT, oferente TEXT, contacto TEXT, telefono TEXT, correo TEXT,
                catalogo TEXT, producto TEXT, fabricante TEXT, marca TEXT,
                modelo_web TEXT, estado_catalogo TEXT
            );
            CREATE TABLE intel_build_metadata (key TEXT, value TEXT);
            """
        )
        facts = [
            ("a1", "1", "43358", 1, 1, 96, "nombre_exacto", "titulo", "kit", "3.1", "cat", "https://acto/1", "KIT CIRCUITO", "CSS", "Compras", "Adjudicado", "2026-01-10", "2026-01-15", "2026-01-15", "2026-01-20", "2026-01-21", 10000, 9000, "ganador", "BTS", "BTS", 1, "kit circuito refrigeracion css adjudicado"),
            ("a2", "2", "43358", 1, 1, 90, "nombre_compacto", "titulo", "kit", "3.1", "cat", "https://acto/2", "KIT CIRCUITO", "MINSA", "Compras", "Adjudicado", "2025-01-10", "2025-01-15", "2025-01-15", "2025-01-20", "2025-01-21", 5000, 4500, "ganador", "OTRO", "OTRO", 3, "kit circuito minsa adjudicado"),
            ("a3", "3", "103169", 1, 1, 100, "codigo_contextual", "descripcion", "ficha", "3.1", "cat", "https://acto/3", "ESTERILIZACION", "CSS", "Compras", "Adjudicado", "2026-02-10", "2026-02-15", "2026-02-15", "2026-02-20", "2026-02-21", 20000, 18000, "ganador", "MEDICAL", "MEDICAL", 2, "esterilizacion css adjudicado"),
        ]
        connection.executemany("INSERT INTO intel_actos_fichas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", facts)
        connection.executemany(
            "INSERT INTO intel_acto_proponentes VALUES (?,?,?,?,?,?,?)",
            [("a1", "1", 1, "BTS", "bts", 9000, 1), ("a2", "2", 1, "OTRO", "otro", 4500, 1), ("a3", "3", 1, "MEDICAL", "medical", 18000, 1)],
        )
        connection.executemany(
            "INSERT INTO intel_ficha_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [("43358", "KIT CIRCUITO PACIENTE", "ANESTESIA", "MEDICO", "INSUMO", "ANESTESIA", "Si", "No", "https://minsa/43358", "test", "43358 kit circuito paciente anestesia medico insumo"), ("103169", "ESTERILIZACION", "", "MEDICO", "INSUMO", "", "Si", "No", "https://minsa/103169", "test", "103169 esterilizacion medico insumo")],
        )
        connection.execute("INSERT INTO intel_ficha_catalogo VALUES (?,?,?,?,?,?,?,?,?,?,?)", ("43358", "PROVEEDOR C", "Ana", "123", "a@test", "C1", "KIT", "LAB", "M", "X", "Activo"))
        connection.commit()
        connection.close()
        self.repo = AnalyticsRepository(create_engine(f"sqlite:///{self.db_path.as_posix()}"), source_label="test")

    def tearDown(self) -> None:
        self.repo.close()
        self.temp.cleanup()

    def test_date_and_detection_profile_filter_full_dataset(self) -> None:
        filters = AnalyticsFilters(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
            date_basis="publicacion",
            detection_profile="moderado",
        )
        result = self.repo.master_metrics(filters)
        row = result[result.ficha.eq("43358")].iloc[0]
        self.assertEqual(int(row["actos"]), 1)
        self.assertEqual(float(row["monto_referencia"]), 10000.0)
        self.assertEqual(str(row["top_1_ganador"]), "BTS")
        self.assertEqual(int(row["proveedores_catalogo"]), 1)

    def test_strict_profile_excludes_score_90(self) -> None:
        filters = AnalyticsFilters(detection_profile="estricto")
        result = self.repo.master_metrics(filters)
        row = result[result.ficha.eq("43358")].iloc[0]
        self.assertEqual(int(row["actos"]), 1)

    def test_provider_and_act_details_use_same_scope(self) -> None:
        filters = AnalyticsFilters(start_date=date(2026, 1, 1), end_date=date(2026, 12, 31))
        providers = self.repo.providers_for_ficha("43358", filters)
        acts = self.repo.acts_for_ficha("43358", filters)
        self.assertEqual(providers.iloc[0]["proveedor"], "BTS")
        self.assertEqual(len(acts), 1)

    def test_accent_insensitive_search_and_and_or_groups(self) -> None:
        filters = AnalyticsFilters(
            detection_profile="muy_flexible",
            search_groups=split_search_groups("refrigeraci\u00f3n, anestesia"),
            search_mode="OR",
        )
        result = self.repo.master_metrics(filters)
        self.assertEqual(set(result["ficha"]), {"43358"})

        strict_groups = AnalyticsFilters(
            detection_profile="muy_flexible",
            search_groups=split_search_groups("refrigeraci\u00f3n, anestesia"),
            search_mode="AND",
        )
        result_and = self.repo.master_metrics(strict_groups)
        self.assertEqual(set(result_and["ficha"]), {"43358"})

    def test_medians_concentration_and_coverage_are_exact(self) -> None:
        result = self.repo.master_metrics(AnalyticsFilters(detection_profile="muy_flexible"))
        row = result[result.ficha.eq("43358")].iloc[0]
        self.assertEqual(float(row["ticket_mediano"]), 7500.0)
        self.assertEqual(float(row["participantes_mediana"]), 2.0)
        self.assertEqual(float(row["top_1_pct"]), 50.0)
        self.assertEqual(float(row["top_3_concentracion_pct"]), 100.0)
        self.assertEqual(float(row["cobertura_monto_referencia_pct"]), 100.0)
        self.assertEqual(float(row["cobertura_ganador_pct"]), 100.0)

    def test_aggregate_and_availability_filters_run_before_returning_rows(self) -> None:
        result = self.repo.master_metrics(
            AnalyticsFilters(
                detection_profile="muy_flexible",
                min_acts=2,
                min_entities=2,
                min_active_months=2,
                max_average_participants=2.0,
                catalog_only=True,
            )
        )
        self.assertEqual(result["ficha"].tolist(), ["43358"])

    def test_act_amount_and_metadata_filters_are_applied_in_sql(self) -> None:
        result = self.repo.master_metrics(
            AnalyticsFilters(
                detection_profile="muy_flexible",
                areas=("MEDICO",),
                product_types=("INSUMO",),
                min_award_amount=10_000,
            )
        )
        self.assertEqual(result["ficha"].tolist(), ["103169"])

    def test_favorite_list_and_contactable_provider_filters_are_exact(self) -> None:
        selected = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible", fichas=("103169",))
        )
        self.assertEqual(selected["ficha"].tolist(), ["103169"])
        contactable = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible", contactable_only=True)
        )
        self.assertEqual(contactable["ficha"].tolist(), ["43358"])


if __name__ == "__main__":
    unittest.main()
