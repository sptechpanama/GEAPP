from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from services.inteligencia_pc import (
    PCFilters,
    InteligenciaPCRepository,
    build_company_acts,
    classify_pc_market,
    classify_project_family,
    company_summary,
    family_market_concentration,
    family_summary,
    near_miss_opportunities,
    normalize_provider,
    parse_money,
    provider_growth_ranking,
    provider_ranking,
    score_entity_opportunities,
    score_family_opportunities,
    score_provider_opportunities,
    unpivot_proposals,
)
from scripts.build_inteligencia_pc import _official_proposals, build


def test_market_classifier_excludes_valid_fichas_and_medical_terms() -> None:
    assert classify_pc_market(title="Compra general", ficha_detectada="* 43358")[0] == "medico"
    assert classify_pc_market(title="Compra de catéter para paciente", ficha_detectada="No Detectada")[0] == "medico"


def test_health_entity_does_not_exclude_non_medical_project() -> None:
    market, evidence = classify_pc_market(
        title="Mantenimiento de chiller y aire acondicionado",
        description="Hospital regional",
        ficha_detectada="No Detectada",
        fichas_json="[]",
    )
    assert market == "no_medico"
    assert evidence == "sin_evidencia_medica"


def test_mixed_medical_and_infrastructure_is_reviewable() -> None:
    market, _ = classify_pc_market(
        title="Remodelación de laboratorio clínico y aire acondicionado",
        ficha_detectada="No Detectada",
    )
    assert market == "ambiguo"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("B/. 2,525.00", 2525.0), ("1.234,50", 1234.5), ("$900", 900.0), ("", 0.0)],
)
def test_money_parser(raw: str, expected: float) -> None:
    assert parse_money(raw) == pytest.approx(expected)


def test_provider_normalization_and_family() -> None:
    assert normalize_provider("RS Engineering, S.A.") == "rs engineering"
    family, confidence, _ = classify_project_family("Mantenimiento de chiller y sistema HVAC")
    assert family == "Climatizacion, refrigeracion y HVAC"
    assert confidence >= 70


def test_company_metrics_use_link_when_remote_ids_are_null() -> None:
    source = pd.DataFrame(
        [
            {
                "id": None,
                "acto_key": "https://acto/1",
                "enlace": "https://acto/1",
                "Proponente 1": "RS ENGINEERING, S.A.",
                "Precio Proponente 1": "B/. 1,200.00",
                "Proponente 2": "COMPETIDOR, S.A.",
                "Precio Proponente 2": "B/. 1,100.00",
                "razon_social": "RS ENGINEERING, S.A.",
            }
        ]
    )
    acts = build_company_acts(source, "RS Engineering")
    summary = company_summary(acts)
    assert summary["participaciones"] == 1
    assert summary["ganados"] == 1
    assert summary["monto_participado"] == pytest.approx(1200.0)


def test_score_honors_one_hundred_percent_weight() -> None:
    frame = pd.DataFrame(
        {
            "familia": ["A", "B", "C"],
            "actos": [1, 100, 5],
            "monto_total": [1_000_000, 10, 500],
            "participantes_promedio": [10, 1, 5],
            "meses_activos": [1, 12, 3],
            "entidades": [1, 20, 2],
        }
    )
    result = score_family_opportunities(
        frame,
        {"actos": 0, "monto": 100, "competencia": 0, "recurrencia": 0, "diversificacion": 0},
    )
    assert result.iloc[0]["familia"] == "A"
    assert result.iloc[0]["score_oportunidad"] == pytest.approx(100.0)


def test_provider_score_honors_one_hundred_percent_weight() -> None:
    frame = pd.DataFrame(
        {
            "proveedor": ["A", "B", "C"],
            "proveedor_norm": ["a", "b", "c"],
            "participaciones": [100, 5, 40],
            "adjudicaciones": [90, 1, 20],
            "monto_ganado": [100, 1_000_000, 500],
            "tasa_exito": [90, 20, 50],
            "familias": [10, 1, 5],
            "entidades": [10, 1, 5],
        }
    )
    result = score_provider_opportunities(
        frame,
        {"adjudicaciones": 0, "monto_ganado": 100, "tasa_exito": 0, "participaciones": 0, "diversificacion": 0},
    )
    assert result.iloc[0]["proveedor"] == "B"
    assert result.iloc[0]["score_proveedor"] == pytest.approx(100.0)


def test_entity_score_honors_one_hundred_percent_weight() -> None:
    frame = pd.DataFrame(
        {
            "entidad": ["A", "B"], "actos": [3, 20], "monto_total": [1_000_000, 100],
            "meses_activos": [2, 10], "participantes_promedio": [1, 5], "familias": [1, 5],
        }
    )
    result = score_entity_opportunities(
        frame,
        {"actos": 100, "monto": 0, "recurrencia": 0, "competencia": 0, "diversificacion": 0},
    )
    assert result.iloc[0]["entidad"] == "B"
    assert result.iloc[0]["score_entidad"] == pytest.approx(100.0)


def test_growth_concentration_and_near_miss_helpers() -> None:
    current = pd.DataFrame(
        {
            "proveedor": ["Nuevo", "Estable"], "proveedor_norm": ["nuevo", "estable"],
            "participaciones": [8, 10], "adjudicaciones": [4, 3], "monto_ganado": [8000, 5000],
        }
    )
    previous = pd.DataFrame(
        {
            "proveedor": ["Nuevo", "Estable"], "proveedor_norm": ["nuevo", "estable"],
            "participaciones": [1, 10], "adjudicaciones": [0, 3], "monto_ganado": [0, 5000],
        }
    )
    assert provider_growth_ranking(current, previous).iloc[0]["proveedor"] == "Nuevo"

    concentration = family_market_concentration(pd.DataFrame(
        {
            "familia": ["HVAC", "HVAC"], "proveedor": ["A", "B"], "proveedor_norm": ["a", "b"],
            "participaciones": [8, 4], "adjudicaciones": [6, 2], "monto_ganado": [6000, 2000],
        }
    ))
    assert concentration.iloc[0]["proveedor_dominante"] == "A"
    assert concentration.iloc[0]["concentracion_top"] == pytest.approx(75.0)

    company_acts = pd.DataFrame(
        [{"acto_key": "1", "fecha_analitica": "2026-01-01", "titulo": "Proyecto", "familia": "HVAC", "entidad": "Entidad", "enlace": "https://acto/1"}]
    )
    proposals = pd.DataFrame(
        [
            {"acto_key": "1", "proveedor": "RS Engineering", "proveedor_norm": "rs engineering", "monto_ofertado": 1050, "ganado": False},
            {"acto_key": "1", "proveedor": "Competidor", "proveedor_norm": "competidor", "monto_ofertado": 1000, "ganado": True},
        ]
    )
    near = near_miss_opportunities(company_acts, proposals, "RS Engineering")
    assert near.iloc[0]["brecha"] == pytest.approx(50.0)
    assert near.iloc[0]["brecha_porcentual"] == pytest.approx(5.0)


def _create_test_database(path: Path) -> None:
    columns = [
        'id INTEGER', 'publicacion TEXT', 'fecha TEXT', 'fecha_adjudicacion TEXT',
        'fecha_actualizacion TEXT', 'enlace TEXT', 'titulo TEXT', 'descripcion TEXT',
        'entidad TEXT', 'unidad_solic TEXT', 'estado TEXT', 'precio_referencia TEXT',
        'razon_social TEXT', 'nombre_comercial TEXT', 'num_participantes TEXT',
        'total_items_ofertados TEXT', 'ficha_detectada TEXT', 'fichas_detectadas_json TEXT',
        'items_json TEXT', 'source_tipo_proceso TEXT',
    ]
    for index in range(1, 15):
        columns.extend([f'"Proponente {index}" TEXT', f'"Precio Proponente {index}" TEXT'])
    connection = sqlite3.connect(path)
    connection.execute(f"CREATE TABLE actos_publicos ({', '.join(columns)})")
    base = {
        "publicacion": "15-07-2026",
        "fecha": "",
        "fecha_adjudicacion": "20-07-2026",
        "fecha_actualizacion": "21-07-2026",
        "descripcion": "",
        "unidad_solic": "Compras",
        "estado": "Adjudicado",
        "nombre_comercial": "",
        "num_participantes": "2",
        "total_items_ofertados": "1",
        "fichas_detectadas_json": "[]",
        "items_json": "[]",
        "source_tipo_proceso": "CL",
    }
    rows = [
        {
            **base, "id": 1, "enlace": "https://acto/1", "titulo": "Mantenimiento de chiller",
            "entidad": "Entidad A", "precio_referencia": "B/. 10,000.00", "razon_social": "RS ENGINEERING",
            "ficha_detectada": "No Detectada", "Proponente 1": "RS ENGINEERING", "Precio Proponente 1": "B/. 9,000.00",
            "Proponente 2": "OTRA EMPRESA", "Precio Proponente 2": "B/. 9,500.00",
        },
        {
            **base, "id": 2, "enlace": "https://acto/2", "titulo": "Compra de catéter para paciente",
            "entidad": "Entidad B", "precio_referencia": "B/. 2,000.00", "razon_social": "MEDICA",
            "ficha_detectada": "No Detectada", "Proponente 1": "MEDICA", "Precio Proponente 1": "B/. 1,800.00",
        },
        {
            **base, "id": 3, "enlace": "https://acto/3", "titulo": "Equipo con ficha",
            "entidad": "Entidad C", "precio_referencia": "B/. 5,000.00", "razon_social": "MEDICA",
            "ficha_detectada": "* 43358", "fichas_detectadas_json": '["43358"]',
        },
    ]
    all_columns = [row[1] for row in connection.execute("PRAGMA table_info(actos_publicos)")]
    placeholders = ",".join("?" for _ in all_columns)
    connection.executemany(
        f"INSERT INTO actos_publicos ({','.join(chr(34) + col + chr(34) for col in all_columns)}) VALUES ({placeholders})",
        [[row.get(column, "") for column in all_columns] for row in rows],
    )
    connection.commit()
    connection.close()


def test_repository_filters_medical_and_builds_company_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    database = tmp_path / "pc.db"
    _create_test_database(database)
    repo = InteligenciaPCRepository.connect(local_candidates=[database])
    filters = PCFilters(start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))
    acts = repo.load_acts(filters)
    assert acts["acto_key"].tolist() == ["https://acto/1"]
    assert family_summary(acts).iloc[0]["actos"] == 1
    company = repo.company_acts("RS Engineering", filters)
    assert company_summary(company)["ganados"] == 1
    proposals = repo.load_proposals(filters)
    ranking = provider_ranking(proposals)
    assert ranking.iloc[0]["proveedor"] == "RS ENGINEERING"
    repo.close()


def test_materialized_layer_supports_fast_views(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    source = tmp_path / "operational.db"
    output = tmp_path / "inteligencia_pc.db"
    _create_test_database(source)
    result = build(source, output, chunk_size=500)
    assert result["act_rows"] == 1
    assert result["proposal_rows"] == 2

    repo = InteligenciaPCRepository.connect(local_candidates=[output])
    filters = PCFilters(start_date=date(2026, 7, 1), end_date=date(2026, 7, 31))
    assert repo.has_pc_layer
    assert repo.has_provider_daily
    assert repo.has_provider_context
    assert repo.has_family_daily
    assert int(repo.family_market_summary(filters)["actos"].sum()) == 1
    assert int(repo.monthly_market_trend(filters)["actos"].sum()) == 1
    projects, total = repo.project_page(filters, sort_column="monto_referencia", limit=50)
    assert total == 1
    assert projects.iloc[0]["acto_key"] == "https://acto/1"
    ranking = repo.provider_market_ranking(filters, detailed=True)
    assert ranking.iloc[0]["proveedor"] == "RS ENGINEERING"
    assert int(ranking.iloc[0]["familias"]) == 1
    assert repo.entity_market_ranking(filters).iloc[0]["entidad"] == "Entidad A"
    assert repo.provider_entity_ranking(filters).iloc[0]["entidad"] == "Entidad A"
    targeted_relations = repo.provider_entity_ranking(filters, provider="RS Engineering")
    assert set(targeted_relations["proveedor_norm"].astype(str)) == {"rs engineering"}
    assert repo.family_provider_ranking(filters).iloc[0]["familia"] == "Climatizacion, refrigeracion y HVAC"
    assert repo.low_competition_projects(filters, maximum_participants=2, minimum_amount=5000).iloc[0]["acto_key"] == "https://acto/1"
    repo.close()


def test_company_results_distinguish_winner_loser_deserted_and_pending() -> None:
    frame = pd.DataFrame(
        [
            {
                "acto_key": "won", "estado": "Adjudicado", "razon_social": "RS ENGINEERING",
                "Proponente 1": "RS ENGINEERING", "Precio Proponente 1": "1000",
            },
            {
                "acto_key": "lost", "estado": "Adjudicado", "razon_social": "OTRA EMPRESA",
                "Proponente 1": "RS ENGINEERING", "Precio Proponente 1": "1100",
            },
            {
                "acto_key": "deserted", "estado": "Desierto",
                "Proponente 1": "RS ENGINEERING", "Precio Proponente 1": "1200",
            },
            {
                "acto_key": "pending", "estado": "En evaluacion",
                "Proponente 1": "RS ENGINEERING", "Precio Proponente 1": "1300",
            },
        ]
    )
    result = build_company_acts(frame, "RS Engineering")
    assert dict(zip(result["acto_key"], result["resultado_empresa"])) == {
        "won": "Adjudicado",
        "lost": "No adjudicado",
        "deserted": "Desierto",
        "pending": "En evaluacion",
    }


def test_official_json_keeps_all_participants_and_multiple_winners() -> None:
    frame = pd.DataFrame(
        [
            {
                "acto_key": "multi",
                "estado": "Adjudicado",
                "proponentes_json": (
                    '[{"nombre":"RS ENGINEERING, S.A.","monto":72000},'
                    '{"nombre":"OTRA EMPRESA","monto":30000}]'
                ),
                "ganadores_json": (
                    '[{"nombre":"RS ENGINEERING, S.A.","monto":72000},'
                    '{"nombre":"OTRA EMPRESA","monto":30000}]'
                ),
            }
        ]
    )
    proposals = _official_proposals(frame)
    assert set(proposals["proveedor"]) == {"RS ENGINEERING, S.A.", "OTRA EMPRESA"}
    assert proposals["ganado"].all()
    assert float(proposals["monto_ganado"].sum()) == pytest.approx(102000)


def test_official_json_deduplication_keeps_unique_consecutive_ordinals() -> None:
    frame = pd.DataFrame(
        [
            {
                "acto_key": "dedupe",
                "estado": "Adjudicado",
                "proponentes_json": (
                    '[{"nombre":"EMPRESA A","monto":100},'
                    '{"nombre":"EMPRESA A","monto":100},'
                    '{"nombre":"EMPRESA B","monto":200}]'
                ),
                "ganadores_json": '[{"nombre":"EMPRESA B","monto":200}]',
                "Proponente 1": "EMPRESA C",
                "Precio Proponente 1": "300",
            }
        ]
    )
    proposals = _official_proposals(frame)
    assert proposals["proveedor"].tolist() == ["EMPRESA A", "EMPRESA B", "EMPRESA C"]
    assert proposals["ordinal"].tolist() == [1, 2, 3]
    assert not proposals.duplicated(["acto_key", "ordinal"]).any()


def test_deserted_act_never_keeps_stale_winner_or_won_amount() -> None:
    frame = pd.DataFrame(
        [
            {
                "acto_key": "deserted",
                "estado": "Desierto",
                "razon_social": "RS ENGINEERING",
                "Proponente 1": "RS ENGINEERING",
                "Precio Proponente 1": "72000",
            }
        ]
    )
    proposals = _official_proposals(frame)
    assert proposals.iloc[0]["resultado_empresa"] == "Desierto"
    assert not bool(proposals.iloc[0]["ganado"])
    assert float(proposals.iloc[0]["monto_ganado"]) == 0.0


def test_builder_materializes_synthetic_winner_and_rs_72k_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    source = tmp_path / "operational.db"
    output = tmp_path / "inteligencia_pc.db"
    _create_test_database(source)
    connection = sqlite3.connect(source)
    columns = [row[1] for row in connection.execute("PRAGMA table_info(actos_publicos)")]
    row = {column: "" for column in columns}
    row.update(
        {
            "id": 72,
            "publicacion": "25-08-2026",
            "fecha_adjudicacion": "26-08-2026",
            "enlace": "https://www.panamacompra.gob.pa/Inicio/#/pliego-de-cargos/2026-1-10-01-02-CM-072000/token",
            "titulo": "Adecuacion de climatizacion de policlinica",
            "descripcion": "Sistema HVAC central",
            "entidad": "Caja de Seguro Social - Policlinica",
            "estado": "Adjudicado",
            "precio_referencia": "72000",
            "razon_social": "RS ENGINEERING, S.A.",
            "nombre_comercial": "RS ENGINEERING",
            "ficha_detectada": "No Detectada",
            "fichas_detectadas_json": "[]",
            "items_json": "[]",
            "source_tipo_proceso": "CM",
        }
    )
    placeholders = ",".join("?" for _ in columns)
    connection.execute(
        f"INSERT INTO actos_publicos ({','.join(chr(34) + col + chr(34) for col in columns)}) VALUES ({placeholders})",
        [row[column] for column in columns],
    )
    connection.commit()
    connection.close()

    build(source, output, chunk_size=500)
    repo = InteligenciaPCRepository.connect(local_candidates=[output])
    acts = repo.company_acts(
        "RS Engineering",
        PCFilters(start_date=date(2026, 8, 1), end_date=date(2026, 8, 31)),
    )
    acceptance = acts[acts["titulo"].str.contains("policlinica", case=False, na=False)]
    assert len(acceptance) == 1
    assert acceptance.iloc[0]["resultado_empresa"] == "Adjudicado"
    assert float(acceptance.iloc[0]["monto_participacion"]) == pytest.approx(72_000.0)
    repo.close()


def test_builder_adds_closed_online_quotes_without_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    source = tmp_path / "operational.db"
    output = tmp_path / "inteligencia_pc.db"
    _create_test_database(source)
    connection = sqlite3.connect(source)
    connection.execute(
        """
        CREATE TABLE cl_cotizaciones (
            cl_key TEXT, numero_cl TEXT, enlace TEXT, titulo TEXT, entidad TEXT,
            unidad_solicitante TEXT, precio_referencia REAL, fecha_publicacion TEXT,
            fecha_cierre TEXT, estado_derivado TEXT, proponents_json TEXT,
            source_payload_json TEXT, updated_at TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO cl_cotizaciones VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "cl-1", "2026-1-10-01-02-CL-099999", "https://acto/cl-1",
            "Mantenimiento de sistema HVAC", "Policlinica A", "Compras", 75_000,
            "26-08-2026", "2026-08-26T14:00:00-05:00", "cerrada_con_propuestas",
            '[{"name":"RS ENGINEERING","total":72000},{"name":"COMPETIDOR","total":73000}]',
            '{"descripcion":"Mantenimiento de aire acondicionado central","ficha_detectada":"No Detectada"}',
            "2026-08-26T15:00:00-05:00",
        ),
    )
    connection.commit()
    connection.close()

    build(source, output, chunk_size=500)
    repo = InteligenciaPCRepository.connect(local_candidates=[output])
    acts = repo.company_acts(
        "RS Engineering",
        PCFilters(start_date=date(2026, 8, 1), end_date=date(2026, 8, 31)),
    )
    quote = acts[acts["acto_key"] == "https://acto/cl-1"]
    assert len(quote) == 1
    assert quote.iloc[0]["resultado_empresa"] == "En evaluacion"
    assert bool(quote.iloc[0]["resultado_provisional"])
    assert float(quote.iloc[0]["monto_participacion"]) == pytest.approx(72_000.0)
    repo.close()

    connection = sqlite3.connect(output)
    try:
        source_id_type = connection.execute(
            "SELECT typeof(source_id) FROM pc_actos WHERE acto_key=?",
            ("https://acto/cl-1",),
        ).fetchone()[0]
    finally:
        connection.close()
    assert source_id_type == "null"
