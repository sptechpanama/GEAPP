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
    RISK_CLASS_NONE,
    RISK_CLASS_OTHER,
    apply_master_filters,
    intelligence_view_frame,
    normalize_ficha_list,
    preset_range,
    score_opportunities,
    sort_and_page,
    split_search_groups,
)


class ServiceUnitTests(unittest.TestCase):
    def test_visible_tables_hide_internal_scores_coverage_and_entities(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ficha": "43358",
                    "entidad": "CSS",
                    "entidades": 4,
                    "score_demanda": 90,
                    "score_economia": 80,
                    "score_competencia": 70,
                    "score_viabilidad": 60,
                    "score_complejidad": 50,
                    "score_confianza": 40,
                    "cobertura_monto_adjudicado_pct": 30,
                    "cobertura_monto_referencia_pct": 20,
                    "cobertura_ganador_pct": 10,
                    "cobertura_participantes_pct": 5,
                    "monto_referencia": 1000,
                }
            ]
        )
        visible = intelligence_view_frame(frame)
        self.assertEqual(visible.columns.tolist(), ["ficha", "monto_referencia"])

    def test_multiple_ficha_parser_accepts_common_separators_and_deduplicates(self) -> None:
        self.assertEqual(
            normalize_ficha_list("52617, 23009\n*21833; 21834 52617"),
            ("52617", "23009", "21833", "21834"),
        )
        self.assertEqual(normalize_ficha_list("ficha-52617, 2x10"), ())

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
                {"ficha": "A", "clase_riesgo": "A", "actos": 10, "actos_ficha_unica": 8, "entidades": 4, "meses_activos": 6, "monto_referencia": 100000, "monto_adjudicado": 80000, "ticket_promedio": 10000, "ticket_mediano": 9000, "participantes_promedio": 1.5, "participantes_mediana": 1, "proponentes_distintos": 3, "proveedores_catalogo": 2, "proveedores_contactables": 2, "confianza_deteccion": 95, "cobertura_monto_referencia_pct": 100, "cobertura_monto_adjudicado_pct": 80, "cobertura_ganador_pct": 80, "cobertura_participantes_pct": 90, "pct_ficha_unica": 80, "tiene_ct": "Si", "registro_sanitario": "No", "nombre_ficha": "A", "enlace_minsa": "https://a"},
                {"ficha": "B", "clase_riesgo": "B", "actos": 2, "actos_ficha_unica": 0, "entidades": 1, "meses_activos": 1, "monto_referencia": 1000, "monto_adjudicado": 0, "ticket_promedio": 500, "ticket_mediano": 500, "participantes_promedio": 5, "participantes_mediana": 5, "proponentes_distintos": 8, "proveedores_catalogo": 0, "proveedores_contactables": 0, "confianza_deteccion": 70, "cobertura_monto_referencia_pct": 50, "cobertura_monto_adjudicado_pct": 0, "cobertura_ganador_pct": 0, "cobertura_participantes_pct": 50, "pct_ficha_unica": 0, "tiene_ct": "No", "registro_sanitario": "Si", "nombre_ficha": "B", "enlace_minsa": ""},
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

    def test_economic_score_prioritizes_total_and_unique_amounts_not_attributed_amount(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ficha": "TOTAL",
                    "monto_total_actos": 100_000,
                    "monto_ficha_unica": 80_000,
                    "monto_referencia": 1,
                    "monto_adjudicado": 1,
                },
                {
                    "ficha": "ATRIBUIBLE",
                    "monto_total_actos": 10_000,
                    "monto_ficha_unica": 0,
                    "monto_referencia": 1_000_000,
                    "monto_adjudicado": 1_000_000,
                },
            ]
        )
        scored = score_opportunities(frame).set_index("ficha")
        self.assertGreater(
            float(scored.loc["TOTAL", "score_economia"]),
            float(scored.loc["ATRIBUIBLE", "score_economia"]),
        )

    def test_economic_score_uses_only_unique_ficha_amount(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ficha": "MAYOR_UNICA",
                    "monto_total_actos": 1_000,
                    "monto_ficha_unica": 900_000,
                    "monto_referencia": 1,
                },
                {
                    "ficha": "MAYOR_CONTEXTO",
                    "monto_total_actos": 9_000_000,
                    "monto_ficha_unica": 10_000,
                    "monto_referencia": 9_000_000,
                },
            ]
        )
        scored = score_opportunities(frame).set_index("ficha")
        self.assertGreater(
            float(scored.loc["MAYOR_UNICA", "score_economia"]),
            float(scored.loc["MAYOR_CONTEXTO", "score_economia"]),
        )

    def test_manual_act_weights_are_independent_and_strict(self) -> None:
        frame = pd.DataFrame(
            [
                {"ficha": "MAS_TOTAL", "actos": 100, "actos_ficha_unica": 2},
                {"ficha": "MAS_UNICA", "actos": 50, "actos_ficha_unica": 40},
            ]
        )
        total_score = score_opportunities(
            frame,
            {"actos_totales": 100.0},
            strict_manual=True,
        ).set_index("ficha")
        unique_score = score_opportunities(
            frame,
            {"actos_ficha_unica": 100.0},
            strict_manual=True,
        ).set_index("ficha")
        self.assertGreater(
            float(total_score.loc["MAS_TOTAL", "score_oportunidad"]),
            float(total_score.loc["MAS_UNICA", "score_oportunidad"]),
        )
        self.assertGreater(
            float(unique_score.loc["MAS_UNICA", "score_oportunidad"]),
            float(unique_score.loc["MAS_TOTAL", "score_oportunidad"]),
        )

    def test_manual_unique_amount_weight_is_strict(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ficha": "UNICA_ALTA",
                    "monto_total_actos": 1_000,
                    "monto_ficha_unica": 500_000,
                    "monto_referencia": 1,
                },
                {
                    "ficha": "TOTAL_ALTO",
                    "monto_total_actos": 10_000_000,
                    "monto_ficha_unica": 2_000,
                    "monto_referencia": 10_000_000,
                },
            ]
        )
        scored = score_opportunities(
            frame,
            {"monto_ficha_unica": 100.0},
            strict_manual=True,
        ).set_index("ficha")
        self.assertGreater(
            float(scored.loc["UNICA_ALTA", "score_oportunidad"]),
            float(scored.loc["TOTAL_ALTO", "score_oportunidad"]),
        )

    def test_risk_class_controls_complexity_score(self) -> None:
        common = {
            "actos": 10,
            "actos_ficha_unica": 5,
            "pct_ficha_unica": 50,
            "tiene_ct": "Si",
            "registro_sanitario": "No",
        }
        frame = pd.DataFrame(
            [
                {"ficha": "A", "clase_riesgo": "A", **common},
                {"ficha": "B", "clase_riesgo": "B", **common},
                {"ficha": "C", "clase_riesgo": "C", **common},
                {"ficha": "D", "clase_riesgo": "D", **common},
                {"ficha": "SIN", "clase_riesgo": "", **common},
            ]
        )
        scored = score_opportunities(frame).set_index("ficha")
        self.assertEqual(float(scored.loc["A", "score_complejidad"]), 100.0)
        self.assertEqual(float(scored.loc["B", "score_complejidad"]), 50.0)
        self.assertEqual(float(scored.loc["C", "score_complejidad"]), 0.0)
        self.assertEqual(float(scored.loc["D", "score_complejidad"]), 0.0)
        self.assertTrue(pd.isna(scored.loc["SIN", "score_complejidad"]))
        self.assertTrue(pd.notna(scored.loc["SIN", "score_oportunidad"]))


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
            ("a1", "1", "43358", 0, 2, 96, "nombre_exacto", "titulo", "kit", "3.1", "cat", "https://acto/1", "KIT CIRCUITO", "CSS", "Compras", "Adjudicado", "2026-01-10", "2026-01-15", "2026-01-15", "2026-01-20", "2026-01-21", 10000, 9000, "ganador", "BTS", "BTS", 1, "kit circuito refrigeracion css adjudicado"),
            ("a1", "1-low", "77777", 0, 2, 80, "alias_flexible", "titulo", "secundaria", "3.1", "cat", "https://acto/1", "KIT CIRCUITO", "CSS", "Compras", "Adjudicado", "2026-01-10", "2026-01-15", "2026-01-15", "2026-01-20", "2026-01-21", 10000, 9000, "ganador", "BTS", "BTS", 1, "kit circuito secundaria css adjudicado"),
            ("a2", "2", "43358", 1, 1, 90, "nombre_compacto", "titulo", "kit", "3.1", "cat", "https://acto/2", "KIT CIRCUITO", "MINSA", "Compras", "Adjudicado", "2025-01-10", "2025-01-15", "2025-01-15", "2025-01-20", "2025-01-21", 5000, 4500, "ganador", "OTRO", "OTRO", 3, "kit circuito minsa adjudicado"),
            ("a3", "3", "103169", 1, 1, 100, "codigo_contextual", "descripcion", "ficha", "3.1", "cat", "https://acto/3", "ESTERILIZACION", "CSS", "Compras", "Adjudicado", "2026-02-10", "2026-02-15", "2026-02-15", "2026-02-20", "2026-02-21", 20000, 18000, "ganador", "MEDICAL", "MEDICAL", 2, "esterilizacion css adjudicado"),
            ("a4", "4", "99999", 1, 1, 100, "codigo_contextual", "descripcion", "ficha", "3.1", "cat", "https://acto/4", "PRODUCTO CON REGISTRO", "CSS", "Compras", "Adjudicado", "2026-03-10", "2026-03-15", "2026-03-15", "2026-03-20", "2026-03-21", 50000, 45000, "ganador", "RS GANADOR", "RS GANADOR", 1, "producto con registro sanitario"),
            ("a5", "5", "88888", 1, 1, 100, "codigo_contextual", "descripcion", "ficha", "3.1", "cat", "https://acto/5", "PRODUCTO SIN CLASIFICAR", "CSS", "Compras", "Adjudicado", "2026-04-10", "2026-04-15", "2026-04-15", "2026-04-20", "2026-04-21", 60000, 55000, "ganador", "SIN CLASIFICAR", "SIN CLASIFICAR", 1, "producto sin clasificar"),
        ]
        connection.executemany("INSERT INTO intel_actos_fichas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", facts)
        connection.executemany(
            "INSERT INTO intel_acto_proponentes VALUES (?,?,?,?,?,?,?)",
            [("a1", "1", 1, "BTS", "bts", 9000, 1), ("a2", "2", 1, "OTRO", "otro", 4500, 1), ("a3", "3", 1, "MEDICAL", "medical", 18000, 1), ("a4", "4", 1, "RS GANADOR", "rs ganador", 45000, 1), ("a5", "5", 1, "SIN CLASIFICAR", "sin clasificar", 55000, 1)],
        )
        connection.executemany(
            "INSERT INTO intel_ficha_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [("43358", "KIT CIRCUITO PACIENTE", "ANESTESIA", "MEDICO", "INSUMO", "ANESTESIA", "Si", "No", "https://minsa/43358", "test", "43358 kit circuito paciente anestesia medico insumo"), ("103169", "ESTERILIZACION", "", "MEDICO", "INSUMO", "", "Si", "No", "https://minsa/103169", "test", "103169 esterilizacion medico insumo"), ("99999", "PRODUCTO CON REGISTRO", "", "MEDICO", "INSUMO", "", "Si", "Si", "https://minsa/99999", "test", "99999 producto con registro sanitario"), ("88888", "PRODUCTO SIN CLASIFICAR", "", "MEDICO", "INSUMO", "", "Si", "", "https://minsa/88888", "test", "88888 producto sin clasificar"), ("77777", "FICHA SECUNDARIA", "", "MEDICO", "INSUMO", "", "No", "Si", "", "test", "77777 ficha secundaria")],
        )
        connection.execute("ALTER TABLE intel_ficha_metadata ADD COLUMN clase_riesgo TEXT")
        connection.execute("UPDATE intel_ficha_metadata SET clase_riesgo = 'A' WHERE ficha = '43358'")
        connection.execute("UPDATE intel_ficha_metadata SET clase_riesgo = 'C' WHERE ficha = '103169'")
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
        self.assertEqual(int(row["actos_ficha_unica"]), 1)
        self.assertEqual(float(row["monto_referencia"]), 10000.0)
        self.assertEqual(str(row["top_1_ganador"]), "BTS")
        self.assertEqual(int(row["proveedores_catalogo"]), 1)

    def test_global_policy_excludes_rs_required_and_unclassified(self) -> None:
        result = self.repo.master_metrics(AnalyticsFilters(detection_profile="muy_flexible"))
        self.assertEqual(set(result["ficha"]), {"43358", "103169"})

        # Una vista guardada antigua no puede desactivar la política global.
        legacy_filter = AnalyticsFilters(
            detection_profile="muy_flexible",
            rs_status="Si",
            fichas=("99999", "88888"),
        )
        self.assertTrue(self.repo.master_metrics(legacy_filter).empty)
        self.assertEqual(legacy_filter.as_payload()["registro_sanitario"], "No")
        self.assertTrue(self.repo.acts_for_ficha("99999", legacy_filter).empty)
        self.assertTrue(self.repo.providers_for_ficha("99999", legacy_filter).empty)

    def test_risk_class_filters_are_applied_in_sql(self) -> None:
        class_a = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible", risk_classes=("A",))
        )
        class_c = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible", risk_classes=("C",))
        )
        other = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible", risk_classes=(RISK_CLASS_OTHER,))
        )
        none = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible", risk_classes=(RISK_CLASS_NONE,))
        )
        self.assertEqual(class_a["ficha"].tolist(), ["43358"])
        self.assertEqual(class_c["ficha"].tolist(), ["103169"])
        self.assertTrue(other.empty)
        self.assertTrue(none.empty)

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

    def test_direct_ficha_lookup_returns_full_history_and_keeps_rs_policy(self) -> None:
        acts = self.repo.all_acts_for_ficha("43358")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts["acto_key"].tolist(), ["a1", "a2"])
        self.assertEqual(acts["reference_amount"].astype(float).tolist(), [10000.0, 5000.0])

        self.assertTrue(self.repo.all_acts_for_ficha("99999").empty)
        self.assertTrue(self.repo.all_acts_for_ficha("88888").empty)

    def test_ficha_search_options_exposes_code_and_name_for_typeahead(self) -> None:
        options = self.repo.ficha_search_options().set_index("ficha")
        self.assertEqual(
            options.loc["43358", "nombre_ficha"],
            "KIT CIRCUITO PACIENTE",
        )
        self.assertIn("103169", options.index)

    def test_master_uses_catalog_product_when_metadata_name_is_missing(self) -> None:
        with self.repo.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE intel_ficha_metadata SET nombre_ficha = '' WHERE ficha = ?",
                ("103169",),
            )
            connection.exec_driver_sql(
                "INSERT INTO intel_ficha_catalogo VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "103169", "PROVEEDOR", "", "", "", "1",
                    "NOMBRE GENERICO RECUPERADO DEL CATALOGO", "FAB", "MARCA", "M", "Activo",
                ),
            )

        result = self.repo.master_metrics(AnalyticsFilters(detection_profile="muy_flexible"))
        row = result[result["ficha"].eq("103169")].iloc[0]
        self.assertEqual(row["nombre_ficha"], "NOMBRE GENERICO RECUPERADO DEL CATALOGO")

    def test_multi_ficha_lookup_unions_and_deduplicates_acts(self) -> None:
        # El acto a1 contiene dos fichas seleccionadas. Debe aparecer una sola
        # vez y conservar ambas coincidencias sin duplicar su monto.
        with self.repo.engine.begin() as connection:
            connection.exec_driver_sql(
                "INSERT INTO intel_actos_fichas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "a1", "1", "103169", 0, 2, 92, "codigo_contextual", "descripcion",
                    "103169", "3.1", "cat", "https://acto/1", "KIT CIRCUITO",
                    "CSS", "Compras", "Adjudicado", "2026-01-10", "2026-01-15",
                    "2026-01-15", "2026-01-20", "2026-01-21", 10000, 9000,
                    "ganador", "BTS", "BTS", 1,
                    "kit circuito esterilizacion css adjudicado",
                ),
            )

        acts = self.repo.all_acts_for_fichas(["43358", "103169", "43358"])
        self.assertEqual(acts["acto_key"].tolist(), ["a3", "a1", "a2"])
        self.assertEqual(len(acts), 3)
        a1 = acts[acts["acto_key"].eq("a1")].iloc[0]
        self.assertEqual(a1["fichas_coincidentes"], "43358, 103169")
        self.assertEqual(int(a1["fichas_coincidentes_count"]), 2)
        self.assertEqual(float(acts["reference_amount"].sum()), 35000.0)

        self.assertTrue(self.repo.all_acts_for_fichas(["99999", "88888"]).empty)

    def test_direct_provider_lookup_finds_participations_even_without_winning(self) -> None:
        with self.repo.engine.begin() as connection:
            connection.exec_driver_sql(
                "INSERT INTO intel_acto_proponentes VALUES (?,?,?,?,?,?,?)",
                ("a1", "1", 2, "COMPETIDOR MEDICO, S.A.", "competidor medico s a", 8000, 0),
            )

        candidates = self.repo.find_providers("competidor medico")
        self.assertEqual(candidates["proveedor_norm"].tolist(), ["competidor medico s a"])
        self.assertEqual(int(candidates.iloc[0]["actos"]), 1)

        acts = self.repo.all_acts_for_provider("COMPETIDOR MÉDICO, S.A.")
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts.iloc[0]["acto_key"], "a1")
        self.assertEqual(acts.iloc[0]["ficha"], "43358")
        self.assertEqual(float(acts.iloc[0]["offered_amount"]), 8000.0)
        self.assertEqual(int(acts.iloc[0]["is_winner"]), 0)

    def test_direct_provider_lookup_keeps_registration_policy(self) -> None:
        candidates = self.repo.find_providers("medical")
        self.assertEqual(candidates["proveedor_norm"].tolist(), ["medical"])
        self.assertEqual(self.repo.all_acts_for_provider("medical")["acto_key"].tolist(), ["a3"])

        self.assertTrue(self.repo.find_providers("rs ganador").empty)
        self.assertTrue(self.repo.all_acts_for_provider("rs ganador").empty)
        self.assertTrue(self.repo.find_providers("sin clasificar").empty)

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


class AttributedAmountRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "analytics_attributed.db"
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
                update_date TEXT, source_line_count INTEGER, attributed_line_count INTEGER,
                reference_amount REAL, reference_amount_context REAL,
                reference_amount_attributed REAL, reference_amount_attribution_source TEXT,
                reference_amount_reliable INTEGER, award_amount REAL,
                award_amount_context REAL, award_amount_attributed REAL,
                award_amount_attribution_source TEXT, award_amount_reliable INTEGER,
                award_amount_source TEXT, winner TEXT, winner_short TEXT,
                participant_count INTEGER, search_text_norm TEXT
            );
            CREATE TABLE intel_acto_proponentes (
                acto_key TEXT, source_id TEXT, ordinal INTEGER, proveedor TEXT,
                proveedor_norm TEXT, offered_amount REAL, is_winner INTEGER
            );
            CREATE TABLE intel_ficha_metadata (
                ficha TEXT, nombre_ficha TEXT, descripcion TEXT, area TEXT,
                tipo_producto TEXT, especialidad TEXT, tiene_ct TEXT,
                registro_sanitario TEXT, enlace_minsa TEXT, metadata_source TEXT,
                search_text_norm TEXT
            );
            CREATE TABLE intel_ficha_catalogo (
                ficha TEXT, oferente TEXT, contacto TEXT, telefono TEXT, correo TEXT,
                catalogo TEXT, producto TEXT, fabricante TEXT, marca TEXT,
                modelo_web TEXT, estado_catalogo TEXT
            );
            CREATE TABLE intel_build_metadata (key TEXT, value TEXT);
            """
        )
        insert_sql = """
            INSERT INTO intel_actos_fichas (
                acto_key, source_id, ficha, is_unique_ficha, detected_ficha_count,
                detection_score, detection_method, detection_field, detection_evidence,
                detector_version, catalog_version, enlace, titulo, entidad,
                unidad_solicitante, estado, publication_date, celebration_date,
                celebration_end_date, award_date, update_date, source_line_count,
                attributed_line_count, reference_amount, reference_amount_context,
                reference_amount_attributed, reference_amount_attribution_source,
                reference_amount_reliable, award_amount, award_amount_context,
                award_amount_attributed, award_amount_attribution_source,
                award_amount_reliable, award_amount_source, winner, winner_short,
                participant_count, search_text_norm
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """
        connection.executemany(
            insert_sql,
            [
                (
                    "a1", "1", "43358", 0, 2, 96, "codigo_contextual", "item_1",
                    "circuito", "3.3", "cat", "https://acto/mixto", "ACTO MIXTO", "CSS",
                    "Compras", "Adjudicado", "2026-01-10", "2026-01-15", "2026-01-15",
                    "2026-01-20", "2026-01-21", 3, 1, 201000, 201000, 1000,
                    "api_renglon_detectado", 1, 190000, 190000, 0,
                    "sin_adjudicacion_por_renglon_confirmada", 0, "proponente_ganador",
                    "BTS", "BTS", 2, "acto mixto circuito css adjudicado",
                ),
                (
                    "a2", "2", "43358", 1, 1, 98, "codigo_contextual", "item_1",
                    "circuito", "3.3", "cat", "https://acto/unico", "ACTO UNICO", "MINSA",
                    "Compras", "Adjudicado", "2026-02-10", "2026-02-15", "2026-02-15",
                    "2026-02-20", "2026-02-21", 1, 1, 5000, 5000, 5000,
                    "api_renglon_detectado", 1, 4500, 4500, 4500,
                    "acto_un_renglon_ficha_unica", 1, "proponente_ganador",
                    "OTRO", "OTRO", 1, "acto unico circuito minsa adjudicado",
                ),
                (
                    "a1", "1-low", "99999", 0, 2, 80, "alias_flexible", "item_2",
                    "coincidencia secundaria", "3.3", "cat", "https://acto/mixto",
                    "ACTO MIXTO", "CSS", "Compras", "Adjudicado", "2026-01-10",
                    "2026-01-15", "2026-01-15", "2026-01-20", "2026-01-21",
                    3, 0, 201000, 201000, 0, "sin_renglon_atribuible", 0,
                    190000, 190000, 0, "sin_adjudicacion_por_renglon_confirmada",
                    0, "proponente_ganador", "BTS", "BTS", 2,
                    "acto mixto coincidencia secundaria",
                ),
            ],
        )
        connection.executemany(
            "INSERT INTO intel_acto_proponentes VALUES (?,?,?,?,?,?,?)",
            [
                ("a1", "1", 1, "BTS", "bts", 190000, 1),
                ("a2", "2", 1, "OTRO", "otro", 4500, 1),
            ],
        )
        connection.execute(
            "INSERT INTO intel_ficha_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "43358", "KIT CIRCUITO PACIENTE", "ANESTESIA", "MEDICO", "INSUMO",
                "ANESTESIA", "Si", "No", "https://minsa/43358", "test",
                "43358 kit circuito paciente anestesia",
            ),
        )
        connection.execute(
            "INSERT INTO intel_ficha_metadata VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "99999", "FICHA SECUNDARIA", "", "MEDICO", "INSUMO", "",
                "No", "Si", "", "test", "99999 ficha secundaria",
            ),
        )
        connection.commit()
        connection.close()
        self.repo = AnalyticsRepository(
            create_engine(f"sqlite:///{self.db_path.as_posix()}"),
            source_label="test-attributed",
        )

    def tearDown(self) -> None:
        self.repo.close()
        self.temp.cleanup()

    def test_master_uses_attributed_amount_and_keeps_global_context_separate(self) -> None:
        result = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible")
        )
        row = result.iloc[0]
        self.assertEqual(float(row["monto_referencia"]), 6000.0)
        self.assertEqual(float(row["monto_total_actos"]), 206000.0)
        self.assertEqual(float(row["monto_ficha_unica"]), 5000.0)
        self.assertEqual(float(row["monto_adjudicado_ficha_unica"]), 4500.0)
        self.assertEqual(float(row["monto_referencia_contexto"]), 206000.0)
        self.assertEqual(float(row["monto_adjudicado"]), 4500.0)
        self.assertEqual(float(row["monto_adjudicado_contexto"]), 194500.0)
        self.assertEqual(float(row["cobertura_monto_referencia_pct"]), 100.0)
        self.assertEqual(float(row["cobertura_monto_adjudicado_pct"]), 50.0)

    def test_money_filter_uses_attributed_amount_not_the_whole_act(self) -> None:
        result = self.repo.master_metrics(
            AnalyticsFilters(
                detection_profile="muy_flexible",
                min_reference_amount=2000,
            )
        )
        row = result.iloc[0]
        self.assertEqual(int(row["actos"]), 1)
        self.assertEqual(float(row["monto_referencia"]), 5000.0)
        self.assertEqual(float(row["monto_total_actos"]), 5000.0)
        self.assertEqual(float(row["monto_ficha_unica"]), 5000.0)
        self.assertEqual(float(row["monto_referencia_contexto"]), 5000.0)

    def test_unique_ficha_respects_the_selected_detection_profile(self) -> None:
        moderate = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="moderado")
        ).iloc[0]
        flexible = self.repo.master_metrics(
            AnalyticsFilters(detection_profile="muy_flexible")
        ).iloc[0]
        self.assertEqual(int(moderate["actos_ficha_unica"]), 2)
        self.assertEqual(int(flexible["actos_ficha_unica"]), 1)
        moderate_acts = self.repo.acts_for_ficha(
            "43358", AnalyticsFilters(detection_profile="moderado")
        ).set_index("acto_key")
        flexible_acts = self.repo.acts_for_ficha(
            "43358", AnalyticsFilters(detection_profile="muy_flexible")
        ).set_index("acto_key")
        self.assertEqual(int(moderate_acts.loc["a1", "is_unique_ficha"]), 1)
        self.assertEqual(int(flexible_acts.loc["a1", "is_unique_ficha"]), 0)

    def test_act_evidence_exposes_attributed_and_context_amounts(self) -> None:
        acts = self.repo.acts_for_ficha(
            "43358",
            AnalyticsFilters(detection_profile="muy_flexible"),
        )
        mixed = acts.loc[acts["acto_key"].eq("a1")].iloc[0]
        self.assertEqual(float(mixed["reference_amount_attributed"]), 1000.0)
        self.assertEqual(float(mixed["reference_amount_context"]), 201000.0)
        self.assertEqual(int(mixed["source_line_count"]), 3)
        self.assertEqual(int(mixed["attributed_line_count"]), 1)

    def test_provider_money_uses_attributed_award_and_keeps_offer_as_context(self) -> None:
        providers = self.repo.providers_for_ficha(
            "43358",
            AnalyticsFilters(detection_profile="muy_flexible"),
        ).set_index("proveedor")
        self.assertEqual(float(providers.loc["BTS", "monto_ganado"]), 0.0)
        self.assertEqual(float(providers.loc["BTS", "monto_ganado_contexto"]), 190000.0)
        self.assertEqual(float(providers.loc["OTRO", "monto_ganado"]), 4500.0)
        self.assertEqual(float(providers.loc["OTRO", "monto_ganado_contexto"]), 4500.0)

    def test_multi_ficha_and_provider_lookups_keep_attributed_amounts(self) -> None:
        combined = self.repo.all_acts_for_fichas(("43358",))
        mixed = combined.loc[combined["acto_key"].eq("a1")].iloc[0]
        self.assertEqual(float(mixed["reference_amount_attributed"]), 1000.0)
        self.assertEqual(float(mixed["reference_amount_context"]), 201000.0)

        provider = self.repo.all_acts_for_provider("bts")
        self.assertEqual(len(provider), 1)
        self.assertEqual(float(provider.iloc[0]["reference_amount_attributed"]), 1000.0)
        self.assertEqual(float(provider.iloc[0]["reference_amount_context"]), 201000.0)


if __name__ == "__main__":
    unittest.main()
