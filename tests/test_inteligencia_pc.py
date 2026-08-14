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
    family_summary,
    normalize_provider,
    parse_money,
    provider_ranking,
    score_family_opportunities,
    unpivot_proposals,
)
from scripts.build_inteligencia_pc import build


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
    assert repo.has_family_daily
    assert int(repo.family_market_summary(filters)["actos"].sum()) == 1
    assert int(repo.monthly_market_trend(filters)["actos"].sum()) == 1
    projects, total = repo.project_page(filters, sort_column="monto_referencia", limit=50)
    assert total == 1
    assert projects.iloc[0]["acto_key"] == "https://acto/1"
    ranking = repo.provider_market_ranking(filters)
    assert ranking.iloc[0]["proveedor"] == "RS ENGINEERING"
    repo.close()
