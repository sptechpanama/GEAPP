from __future__ import annotations

"""Motor compartido para Inteligencia de Oportunidades y Proveedores v3."""

import math
import os
import re
import sqlite3
import unicodedata
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


DATE_COLUMNS = {
    "publicacion": "publication_date",
    "celebracion": "celebration_date",
    "adjudicacion": "award_date",
    "actualizacion": "update_date",
}

PROFILE_THRESHOLDS = {
    "estricto": 96.0,
    "moderado": 90.0,
    "flexible": 85.0,
    "muy_flexible": 70.0,
}

PROFILE_LABELS = {
    "estricto": "Estricto (score ≥ 96)",
    "moderado": "Moderado (score ≥ 90)",
    "flexible": "Flexible (score ≥ 85)",
    "muy_flexible": "Muy flexible (score ≥ 70)",
}

# Regla comercial global de esta inteligencia: solo se analizan fichas cuya
# metadata confirme expresamente que NO requieren registro sanitario. Esto
# excluye tanto las fichas marcadas "Si" como aquellas sin clasificar.
ELIGIBLE_RS_STATUS = "No"

DEFAULT_SCORE_WEIGHTS = {
    "demanda": 28.0,
    "economia": 27.0,
    "competencia": 18.0,
    "viabilidad": 17.0,
    "complejidad": 10.0,
}

SCORE_PRESETS = {
    "equilibrado": DEFAULT_SCORE_WEIGHTS,
    "volumen": {"demanda": 40.0, "economia": 35.0, "competencia": 10.0, "viabilidad": 10.0, "complejidad": 5.0},
    "baja_competencia": {"demanda": 20.0, "economia": 20.0, "competencia": 40.0, "viabilidad": 12.0, "complejidad": 8.0},
    "buscar_proveedor": {"demanda": 28.0, "economia": 22.0, "competencia": 15.0, "viabilidad": 28.0, "complejidad": 7.0},
    "baja_complejidad": {"demanda": 22.0, "economia": 23.0, "competencia": 15.0, "viabilidad": 15.0, "complejidad": 25.0},
}


def clean_text(value: object) -> str:
    result = str(value if value is not None else "").strip()
    return "" if result.lower() in {"", "nan", "none", "null", "<na>"} else re.sub(r"\s+", " ", result)


def normalize_text(value: object) -> str:
    result = unicodedata.normalize("NFKD", clean_text(value).lower())
    result = "".join(ch for ch in result if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", result)).strip()


def normalize_ficha_list(value: object, *, limit: int = 100) -> tuple[str, ...]:
    """Normaliza una lista de fichas escrita con comas, espacios o saltos.

    Acepta tambien el asterisco visual usado por Panama Compra (``*43358``),
    elimina duplicados conservando el orden y limita la consulta para evitar
    formularios accidentales excesivamente grandes.
    """
    if isinstance(value, str):
        tokens = re.split(r"[\s,;]+", value.strip())
    elif isinstance(value, Iterable):
        tokens = [str(item).strip() for item in value]
    else:
        tokens = [str(value).strip()]

    max_items = max(1, min(int(limit), 100))
    normalized: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        match = re.fullmatch(r"\*?(\d{3,8})\*?", token)
        if not match:
            continue
        code = match.group(1).lstrip("0") or "0"
        if code in seen:
            continue
        seen.add(code)
        normalized.append(code)
        if len(normalized) >= max_items:
            break
    return tuple(normalized)


def split_search_groups(value: object) -> tuple[str, ...]:
    groups: list[str] = []
    for raw in str(value or "").split(","):
        group = normalize_text(raw)
        if group and group not in groups:
            groups.append(group)
    return tuple(groups)


def parse_money(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        result = float(value)
        return result if math.isfinite(result) else 0.0
    raw = clean_text(value)
    raw = re.sub(r"(?i)(B/\.?|USD|US\$|PAB|\$)", "", raw).replace(" ", "")
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".") if raw.rfind(",") > raw.rfind(".") else raw.replace(",", "")
    elif "," in raw:
        tail = raw.rsplit(",", 1)[-1]
        raw = raw.replace(",", "." if len(tail) <= 2 else "")
    raw = re.sub(r"[^0-9.\-]", "", raw)
    try:
        result = float(raw)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True)
class AnalyticsFilters:
    start_date: date | None = None
    end_date: date | None = None
    date_basis: str = "publicacion"
    detection_profile: str = "moderado"
    states: tuple[str, ...] = field(default_factory=tuple)
    entities: tuple[str, ...] = field(default_factory=tuple)
    areas: tuple[str, ...] = field(default_factory=tuple)
    product_types: tuple[str, ...] = field(default_factory=tuple)
    fichas: tuple[str, ...] = field(default_factory=tuple)
    ct_status: str = "Todos"
    # Se conserva el campo por compatibilidad con vistas guardadas anteriores,
    # pero la política global siempre aplica ELIGIBLE_RS_STATUS.
    rs_status: str = ELIGIBLE_RS_STATUS
    search_groups: tuple[str, ...] = field(default_factory=tuple)
    search_mode: str = "OR"
    min_reference_amount: float = 0.0
    max_reference_amount: float = 0.0
    min_award_amount: float = 0.0
    max_award_amount: float = 0.0
    min_acts: int = 0
    min_entities: int = 0
    min_active_months: int = 0
    max_average_participants: float = 0.0
    catalog_only: bool = False
    contactable_only: bool = False

    @property
    def date_column(self) -> str:
        return DATE_COLUMNS.get(self.date_basis, DATE_COLUMNS["publicacion"])

    @property
    def detection_threshold(self) -> float:
        return PROFILE_THRESHOLDS.get(self.detection_profile, PROFILE_THRESHOLDS["moderado"])

    def as_payload(self) -> dict[str, Any]:
        return {
            "fecha_desde": self.start_date.isoformat() if self.start_date else "",
            "fecha_hasta": self.end_date.isoformat() if self.end_date else "",
            "tipo_fecha": self.date_basis,
            "perfil_deteccion": self.detection_profile,
            "score_minimo": self.detection_threshold,
            "estados": list(self.states),
            "entidades": list(self.entities),
            "areas": list(self.areas),
            "tipos_producto": list(self.product_types),
            "fichas": list(self.fichas),
            "criterio_tecnico": self.ct_status,
            "registro_sanitario": ELIGIBLE_RS_STATUS,
            "busqueda": list(self.search_groups),
            "modo_busqueda": self.search_mode,
            "monto_minimo": self.min_reference_amount,
            "monto_maximo": self.max_reference_amount,
            "adjudicado_minimo": self.min_award_amount,
            "adjudicado_maximo": self.max_award_amount,
            "actos_minimos": self.min_acts,
            "entidades_minimas": self.min_entities,
            "meses_activos_minimos": self.min_active_months,
            "participantes_promedio_maximo": self.max_average_participants,
            "solo_catalogo": self.catalog_only,
            "solo_contactables": self.contactable_only,
        }


class AnalyticsUnavailable(RuntimeError):
    pass


class AnalyticsRepository:
    def __init__(self, engine: Engine, *, source_label: str, owns_engine: bool = True) -> None:
        self.engine = engine
        self.source_label = source_label
        self.owns_engine = owns_engine
        self.dialect = engine.dialect.name
        self._assert_schema()

    @classmethod
    def connect(
        cls,
        *,
        database_url: str = "",
        local_candidates: Sequence[Path] = (),
    ) -> "AnalyticsRepository":
        errors: list[str] = []
        url = clean_text(database_url or os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"))
        if url:
            try:
                engine = create_engine(url, pool_pre_ping=True, pool_recycle=240, connect_args={"connect_timeout": 12})
                repository = cls(engine, source_label="Supabase (capa analítica)")
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                return repository
            except Exception as exc:
                errors.append(f"Supabase: {exc}")

        for candidate in local_candidates:
            path = Path(candidate)
            if not path.exists() or path.stat().st_size <= 0:
                continue
            try:
                engine = create_engine(f"sqlite:///{path.as_posix()}")
                return cls(engine, source_label=f"SQLite analítico ({path.name})")
            except Exception as exc:
                errors.append(f"{path}: {exc}")
        detail = " | ".join(errors) if errors else "No se encontró una capa analítica local ni una URL remota."
        raise AnalyticsUnavailable(detail)

    def close(self) -> None:
        if self.owns_engine:
            self.engine.dispose()

    def _assert_schema(self) -> None:
        inspector = inspect(self.engine)
        required = {"intel_actos_fichas", "intel_acto_proponentes", "intel_ficha_metadata", "intel_ficha_catalogo"}
        tables = set(inspector.get_table_names())
        missing = sorted(required - tables)
        if missing:
            raise AnalyticsUnavailable("Faltan tablas analíticas: " + ", ".join(missing))

        fact_columns = {column["name"] for column in inspector.get_columns("intel_actos_fichas")}
        metadata_columns = {column["name"] for column in inspector.get_columns("intel_ficha_metadata")}
        self._has_normalized_search = (
            "search_text_norm" in fact_columns and "search_text_norm" in metadata_columns
        )

    def build_metadata(self) -> dict[str, str]:
        try:
            frame = pd.read_sql_query(text("SELECT key, value FROM intel_build_metadata"), self.engine)
        except Exception:
            return {}
        return {clean_text(row["key"]): clean_text(row["value"]) for _, row in frame.iterrows()}

    def coverage(self) -> dict[str, Any]:
        query = """
            SELECT COUNT(*) AS fact_rows,
                   COUNT(DISTINCT acto_key) AS acts,
                   COUNT(DISTINCT ficha) AS fichas,
                   MIN(NULLIF(publication_date, '')) AS min_publication,
                   MAX(publication_date) AS max_publication,
                   MIN(NULLIF(update_date, '')) AS min_update,
                   MAX(update_date) AS max_update
            FROM intel_actos_fichas
        """
        frame = pd.read_sql_query(text(query), self.engine)
        return frame.iloc[0].to_dict() if not frame.empty else {}

    def _filter_sql(self, filters: AnalyticsFilters, *, alias: str = "f") -> tuple[str, dict[str, Any]]:
        params: dict[str, Any] = {"score_min": filters.detection_threshold}
        clauses = [f"{alias}.detection_score >= :score_min"]
        date_col = f"{alias}.{filters.date_column}"
        if filters.start_date:
            clauses.extend([f"{date_col} IS NOT NULL", f"{date_col} <> ''", f"{date_col} >= :start_date"])
            params["start_date"] = filters.start_date.isoformat()
        if filters.end_date:
            clauses.extend([f"{date_col} IS NOT NULL", f"{date_col} <> ''", f"{date_col} <= :end_date"])
            params["end_date"] = filters.end_date.isoformat()
        if filters.states:
            placeholders = []
            for index, state in enumerate(filters.states):
                key = f"state_{index}"
                params[key] = state
                placeholders.append(f":{key}")
            clauses.append(f"{alias}.estado IN ({', '.join(placeholders)})")
        if filters.entities:
            placeholders = []
            for index, entity in enumerate(filters.entities):
                key = f"entity_{index}"
                params[key] = entity
                placeholders.append(f":{key}")
            clauses.append(f"{alias}.entidad IN ({', '.join(placeholders)})")
        if filters.areas:
            placeholders = []
            for index, area in enumerate(filters.areas):
                key = f"area_{index}"
                params[key] = area
                placeholders.append(f":{key}")
            clauses.append(f"m.area IN ({', '.join(placeholders)})")
        if filters.product_types:
            placeholders = []
            for index, product_type in enumerate(filters.product_types):
                key = f"product_type_{index}"
                params[key] = product_type
                placeholders.append(f":{key}")
            clauses.append(f"m.tipo_producto IN ({', '.join(placeholders)})")
        if filters.fichas:
            placeholders = []
            for index, ficha in enumerate(filters.fichas):
                key = f"ficha_{index}"
                params[key] = str(ficha)
                placeholders.append(f":{key}")
            clauses.append(f"{alias}.ficha IN ({', '.join(placeholders)})")
        if filters.min_reference_amount > 0:
            clauses.append(f"{alias}.reference_amount >= :min_reference")
            params["min_reference"] = float(filters.min_reference_amount)
        if filters.max_reference_amount > 0:
            clauses.append(f"{alias}.reference_amount <= :max_reference")
            params["max_reference"] = float(filters.max_reference_amount)
        if filters.min_award_amount > 0:
            clauses.append(f"{alias}.award_amount >= :min_award")
            params["min_award"] = float(filters.min_award_amount)
        if filters.max_award_amount > 0:
            clauses.append(f"{alias}.award_amount <= :max_award")
            params["max_award"] = float(filters.max_award_amount)
        if filters.ct_status in {"Si", "No"}:
            clauses.append("COALESCE(m.tiene_ct, '') = :ct_status")
            params["ct_status"] = filters.ct_status
        # La exclusión ocurre dentro del SQL antes de agregar, puntuar u ordenar.
        # De esta manera una ficha que requiere registro sanitario no influye en
        # métricas, rankings, exportaciones ni estudios detallados.
        clauses.append("LOWER(TRIM(COALESCE(m.registro_sanitario, ''))) = :eligible_rs_status")
        params["eligible_rs_status"] = ELIGIBLE_RS_STATUS.lower()
        if filters.search_groups:
            group_clauses: list[str] = []
            if self._has_normalized_search:
                search_expr = (
                    "(COALESCE(f.search_text_norm, '') || ' ' || "
                    "COALESCE(m.search_text_norm, ''))"
                )
            else:
                # Backward-compatible fallback for analytical schema 3.0.
                search_expr = (
                    "LOWER(COALESCE(f.ficha, '') || ' ' || COALESCE(f.titulo, '') || ' ' || "
                    "COALESCE(f.entidad, '') || ' ' || COALESCE(m.nombre_ficha, '') || ' ' || "
                    "COALESCE(m.descripcion, '') || ' ' || COALESCE(m.area, '') || ' ' || "
                    "COALESCE(m.tipo_producto, '') || ' ' || COALESCE(m.especialidad, ''))"
                )
            for index, group in enumerate(filters.search_groups):
                key = f"search_{index}"
                params[key] = f"%{group}%"
                group_clauses.append(f"{search_expr} LIKE :{key}")
            connector = " AND " if filters.search_mode.upper() == "AND" else " OR "
            clauses.append("(" + connector.join(group_clauses) + ")")
        return " AND ".join(clauses), params

    def filter_options(self) -> dict[str, list[str]]:
        states = pd.read_sql_query(text("SELECT DISTINCT estado FROM intel_actos_fichas WHERE COALESCE(estado, '') <> '' ORDER BY estado"), self.engine)
        entities = pd.read_sql_query(text("SELECT DISTINCT entidad FROM intel_actos_fichas WHERE COALESCE(entidad, '') <> '' ORDER BY entidad"), self.engine)
        areas = pd.read_sql_query(text("SELECT DISTINCT area FROM intel_ficha_metadata WHERE COALESCE(area, '') <> '' ORDER BY area"), self.engine)
        product_types = pd.read_sql_query(text("SELECT DISTINCT tipo_producto FROM intel_ficha_metadata WHERE COALESCE(tipo_producto, '') <> '' ORDER BY tipo_producto"), self.engine)
        return {
            "states": states.iloc[:, 0].astype(str).tolist() if not states.empty else [],
            "entities": entities.iloc[:, 0].astype(str).tolist() if not entities.empty else [],
            "areas": areas.iloc[:, 0].astype(str).tolist() if not areas.empty else [],
            "product_types": product_types.iloc[:, 0].astype(str).tolist() if not product_types.empty else [],
        }

    def master_metrics(self, filters: AnalyticsFilters) -> pd.DataFrame:
        where_sql, params = self._filter_sql(filters)
        end = filters.end_date or date.today()
        params.update(
            {
                "recent_start": (end - timedelta(days=182)).isoformat(),
                "previous_start": (end - timedelta(days=365)).isoformat(),
                "previous_end": (end - timedelta(days=183)).isoformat(),
            }
        )
        date_column = filters.date_column
        having: list[str] = []
        if filters.min_acts > 0:
            having.append("COUNT(DISTINCT acto_key) >= :min_acts")
            params["min_acts"] = int(filters.min_acts)
        if filters.min_entities > 0:
            having.append("COUNT(DISTINCT entidad) >= :min_entities")
            params["min_entities"] = int(filters.min_entities)
        if filters.min_active_months > 0:
            having.append(f"COUNT(DISTINCT SUBSTR({date_column}, 1, 7)) >= :min_active_months")
            params["min_active_months"] = int(filters.min_active_months)
        if filters.max_average_participants > 0:
            having.append("AVG(participant_count) <= :max_average_participants")
            params["max_average_participants"] = float(filters.max_average_participants)
        having_sql = " HAVING " + " AND ".join(having) if having else ""
        if filters.contactable_only:
            catalog_filter_sql = "WHERE COALESCE(c.proveedores_contactables, 0) > 0"
        elif filters.catalog_only:
            catalog_filter_sql = "WHERE COALESCE(c.proveedores_catalogo, 0) > 0"
        else:
            catalog_filter_sql = ""
        query = f"""
            WITH filtered AS MATERIALIZED (
                SELECT f.*, m.nombre_ficha, m.descripcion, m.area, m.tipo_producto,
                       m.especialidad, m.tiene_ct, m.registro_sanitario, m.enlace_minsa
                FROM intel_actos_fichas f
                LEFT JOIN intel_ficha_metadata m ON m.ficha = f.ficha
                WHERE {where_sql}
            ),
            agg AS (
                SELECT ficha,
                       MAX(COALESCE(nombre_ficha, '')) AS nombre_ficha,
                       MAX(COALESCE(area, '')) AS area,
                       MAX(COALESCE(tipo_producto, '')) AS tipo_producto,
                       MAX(COALESCE(especialidad, '')) AS especialidad,
                       MAX(COALESCE(tiene_ct, '')) AS tiene_ct,
                       MAX(COALESCE(registro_sanitario, '')) AS registro_sanitario,
                       MAX(COALESCE(enlace_minsa, '')) AS enlace_minsa,
                       COUNT(DISTINCT acto_key) AS actos,
                       COUNT(DISTINCT CASE WHEN is_unique_ficha = 1 THEN acto_key END) AS actos_ficha_unica,
                       COUNT(DISTINCT entidad) AS entidades,
                       COUNT(DISTINCT SUBSTR({date_column}, 1, 7)) AS meses_activos,
                       SUM(reference_amount) AS monto_referencia,
                       AVG(reference_amount) AS ticket_promedio,
                       MAX(reference_amount) AS ticket_maximo,
                       SUM(award_amount) AS monto_adjudicado,
                       COUNT(DISTINCT CASE WHEN award_amount > 0 THEN acto_key END) AS actos_monto_adjudicado,
                       COUNT(DISTINCT CASE WHEN reference_amount > 0 THEN acto_key END) AS actos_monto_referencia,
                       COUNT(DISTINCT CASE WHEN COALESCE(winner, '') <> '' OR COALESCE(winner_short, '') <> '' THEN acto_key END) AS actos_con_ganador,
                       COUNT(DISTINCT CASE WHEN participant_count > 0 THEN acto_key END) AS actos_con_participantes,
                       AVG(participant_count) AS participantes_promedio,
                       AVG(CASE WHEN participant_count <= 1 THEN 1.0 ELSE 0.0 END) AS proporcion_unico_proponente,
                       AVG(detection_score) AS confianza_deteccion,
                       MIN({date_column}) AS primera_fecha,
                       MAX({date_column}) AS ultima_fecha,
                       COUNT(DISTINCT CASE WHEN {date_column} >= :recent_start THEN acto_key END) AS actos_ultimos_6m,
                       COUNT(DISTINCT CASE WHEN {date_column} BETWEEN :previous_start AND :previous_end THEN acto_key END) AS actos_6m_previos
                FROM filtered
                GROUP BY ficha
                {having_sql}
            ),
            ticket_ranked AS (
                SELECT ficha, reference_amount,
                       ROW_NUMBER() OVER (PARTITION BY ficha ORDER BY reference_amount) AS rn,
                       COUNT(*) OVER (PARTITION BY ficha) AS cnt
                FROM filtered
                WHERE reference_amount > 0
            ),
            ticket_median AS (
                SELECT ficha, AVG(reference_amount) AS ticket_mediano
                FROM ticket_ranked
                WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
                GROUP BY ficha
            ),
            participant_ranked AS (
                SELECT ficha, participant_count,
                       ROW_NUMBER() OVER (PARTITION BY ficha ORDER BY participant_count) AS rn,
                       COUNT(*) OVER (PARTITION BY ficha) AS cnt
                FROM filtered
                WHERE participant_count > 0
            ),
            participant_median AS (
                SELECT ficha, AVG(participant_count) AS participantes_mediana
                FROM participant_ranked
                WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
                GROUP BY ficha
            ),
            act_keys AS (
                SELECT DISTINCT ficha, acto_key FROM filtered
            ),
            provider_agg AS (
                SELECT a.ficha,
                       COUNT(DISTINCT CASE WHEN COALESCE(p.proveedor_norm, '') <> '' THEN p.proveedor_norm END) AS proponentes_distintos
                FROM act_keys a
                LEFT JOIN intel_acto_proponentes p ON p.acto_key = a.acto_key
                GROUP BY a.ficha
            ),
            catalog_agg AS (
                SELECT ficha,
                       COUNT(DISTINCT CASE WHEN COALESCE(oferente, '') <> '' THEN LOWER(oferente) END) AS proveedores_catalogo,
                       COUNT(DISTINCT CASE WHEN COALESCE(contacto, '') <> '' OR COALESCE(telefono, '') <> '' OR COALESCE(correo, '') <> '' THEN LOWER(oferente) END) AS proveedores_contactables
                FROM intel_ficha_catalogo
                GROUP BY ficha
            ),
            catalog_name_counts AS (
                SELECT ficha, TRIM(producto) AS nombre_catalogo, COUNT(*) AS apariciones
                FROM intel_ficha_catalogo
                WHERE COALESCE(TRIM(producto), '') <> ''
                GROUP BY ficha, TRIM(producto)
            ),
            catalog_name_ranked AS (
                SELECT ficha, nombre_catalogo,
                       ROW_NUMBER() OVER (
                           PARTITION BY ficha
                           ORDER BY apariciones DESC, LENGTH(nombre_catalogo) DESC, nombre_catalogo
                       ) AS name_rank
                FROM catalog_name_counts
            ),
            catalog_name AS (
                SELECT ficha, nombre_catalogo
                FROM catalog_name_ranked
                WHERE name_rank = 1
            ),
            winner_counts AS (
                SELECT ficha,
                       CASE WHEN COALESCE(winner, '') <> '' THEN winner ELSE winner_short END AS ganador,
                       COUNT(DISTINCT acto_key) AS actos_ganados,
                       SUM(award_amount) AS monto_ganado
                FROM filtered
                WHERE COALESCE(winner, '') <> '' OR COALESCE(winner_short, '') <> ''
                GROUP BY ficha, CASE WHEN COALESCE(winner, '') <> '' THEN winner ELSE winner_short END
            ),
            winner_ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ficha
                           ORDER BY actos_ganados DESC, monto_ganado DESC, ganador
                       ) AS winner_rank
                FROM winner_counts
            ),
            winner_agg AS (
                SELECT w.ficha,
                       MAX(CASE WHEN winner_rank = 1 THEN ganador ELSE '' END) AS top_1_ganador,
                       MAX(CASE WHEN winner_rank = 1 THEN actos_ganados ELSE 0 END) AS top_1_actos,
                       MAX(CASE WHEN winner_rank = 1 THEN 100.0 * actos_ganados / NULLIF(a.actos, 0) ELSE 0 END) AS top_1_pct,
                       MAX(CASE WHEN winner_rank = 2 THEN ganador ELSE '' END) AS top_2_ganador,
                       MAX(CASE WHEN winner_rank = 2 THEN actos_ganados ELSE 0 END) AS top_2_actos,
                       MAX(CASE WHEN winner_rank = 2 THEN 100.0 * actos_ganados / NULLIF(a.actos, 0) ELSE 0 END) AS top_2_pct,
                       MAX(CASE WHEN winner_rank = 3 THEN ganador ELSE '' END) AS top_3_ganador,
                       MAX(CASE WHEN winner_rank = 3 THEN actos_ganados ELSE 0 END) AS top_3_actos,
                       MAX(CASE WHEN winner_rank = 3 THEN 100.0 * actos_ganados / NULLIF(a.actos, 0) ELSE 0 END) AS top_3_pct,
                       CASE
                           WHEN SUM(CASE WHEN winner_rank <= 3 THEN 100.0 * actos_ganados / NULLIF(a.actos, 0) ELSE 0 END) > 100.0 THEN 100.0
                           ELSE SUM(CASE WHEN winner_rank <= 3 THEN 100.0 * actos_ganados / NULLIF(a.actos, 0) ELSE 0 END)
                       END AS top_3_concentracion_pct,
                       SUM((1.0 * actos_ganados / NULLIF(a.actos, 0)) * (1.0 * actos_ganados / NULLIF(a.actos, 0))) * 10000.0 AS concentracion_hhi
                FROM winner_ranked w
                INNER JOIN agg a ON a.ficha = w.ficha
                GROUP BY w.ficha
            )
            SELECT a.*,
                   COALESCE(p.proponentes_distintos, 0) AS proponentes_distintos,
                   COALESCE(c.proveedores_catalogo, 0) AS proveedores_catalogo,
                   COALESCE(c.proveedores_contactables, 0) AS proveedores_contactables,
                   COALESCE(tm.ticket_mediano, 0) AS ticket_mediano,
                   COALESCE(pm.participantes_mediana, 0) AS participantes_mediana,
                   COALESCE(w.top_1_ganador, '') AS top_1_ganador,
                   COALESCE(w.top_1_actos, 0) AS top_1_actos,
                   COALESCE(w.top_1_pct, 0) AS top_1_pct,
                   COALESCE(w.top_2_ganador, '') AS top_2_ganador,
                   COALESCE(w.top_2_actos, 0) AS top_2_actos,
                   COALESCE(w.top_2_pct, 0) AS top_2_pct,
                   COALESCE(w.top_3_ganador, '') AS top_3_ganador,
                   COALESCE(w.top_3_actos, 0) AS top_3_actos,
                   COALESCE(w.top_3_pct, 0) AS top_3_pct,
                   COALESCE(w.top_3_concentracion_pct, 0) AS top_3_concentracion_pct,
                   COALESCE(w.concentracion_hhi, 0) AS concentracion_hhi,
                   COALESCE(n.nombre_catalogo, '') AS nombre_ficha_catalogo
            FROM agg a
            LEFT JOIN provider_agg p ON p.ficha = a.ficha
            LEFT JOIN catalog_agg c ON c.ficha = a.ficha
            LEFT JOIN ticket_median tm ON tm.ficha = a.ficha
            LEFT JOIN participant_median pm ON pm.ficha = a.ficha
            LEFT JOIN winner_agg w ON w.ficha = a.ficha
            LEFT JOIN catalog_name n ON n.ficha = a.ficha
            {catalog_filter_sql}
        """
        frame = pd.read_sql_query(text(query), self.engine, params=params)
        if frame.empty:
            return frame

        frame["cobertura_monto_adjudicado_pct"] = (
            pd.to_numeric(frame["actos_monto_adjudicado"], errors="coerce").fillna(0)
            / pd.to_numeric(frame["actos"], errors="coerce").replace(0, pd.NA)
            * 100
        ).fillna(0.0)
        frame["cobertura_monto_referencia_pct"] = (
            pd.to_numeric(frame["actos_monto_referencia"], errors="coerce").fillna(0)
            / pd.to_numeric(frame["actos"], errors="coerce").replace(0, pd.NA)
            * 100
        ).fillna(0.0)
        frame["cobertura_ganador_pct"] = (
            pd.to_numeric(frame["actos_con_ganador"], errors="coerce").fillna(0)
            / pd.to_numeric(frame["actos"], errors="coerce").replace(0, pd.NA)
            * 100
        ).fillna(0.0)
        frame["cobertura_participantes_pct"] = (
            pd.to_numeric(frame["actos_con_participantes"], errors="coerce").fillna(0)
            / pd.to_numeric(frame["actos"], errors="coerce").replace(0, pd.NA)
            * 100
        ).fillna(0.0)
        frame["pct_ficha_unica"] = (
            pd.to_numeric(frame["actos_ficha_unica"], errors="coerce").fillna(0)
            / pd.to_numeric(frame["actos"], errors="coerce").replace(0, pd.NA)
            * 100
        ).fillna(0.0)
        frame["tendencia_6m_pct"] = (
            (pd.to_numeric(frame["actos_ultimos_6m"], errors="coerce").fillna(0) - pd.to_numeric(frame["actos_6m_previos"], errors="coerce").fillna(0))
            / pd.to_numeric(frame["actos_6m_previos"], errors="coerce").replace(0, 1)
            * 100
        ).clip(-100, 500)
        frame["nombre_ficha"] = frame["nombre_ficha"].fillna("").astype(str)
        catalog_names = frame.get("nombre_ficha_catalogo", pd.Series("", index=frame.index))
        catalog_names = catalog_names.fillna("").astype(str)
        missing_names = frame["nombre_ficha"].str.strip().eq("")
        frame.loc[missing_names, "nombre_ficha"] = catalog_names[missing_names]
        frame.loc[frame["nombre_ficha"].str.strip().eq(""), "nombre_ficha"] = frame["ficha"].map(lambda value: f"Ficha {value}")
        frame = frame.drop(columns=["nombre_ficha_catalogo"], errors="ignore")
        return frame

    @staticmethod
    def _merge_winner_metrics(master: pd.DataFrame, winners: pd.DataFrame) -> pd.DataFrame:
        result = master.copy()
        defaults: dict[str, Any] = {
            "top_1_ganador": "",
            "top_1_actos": 0,
            "top_1_pct": 0.0,
            "top_2_ganador": "",
            "top_2_actos": 0,
            "top_2_pct": 0.0,
            "top_3_ganador": "",
            "top_3_actos": 0,
            "top_3_pct": 0.0,
            "top_3_concentracion_pct": 0.0,
            "concentracion_hhi": 0.0,
        }
        for column, default in defaults.items():
            result[column] = default
        if winners.empty:
            return result
        ordered = winners.copy()
        ordered["actos_ganados"] = pd.to_numeric(ordered["actos_ganados"], errors="coerce").fillna(0).astype(int)
        ordered["monto_ganado"] = pd.to_numeric(ordered["monto_ganado"], errors="coerce").fillna(0.0)
        ordered["ganador"] = ordered["ganador"].fillna("").astype(str)
        ordered = ordered.sort_values(
            ["ficha", "actos_ganados", "monto_ganado", "ganador"],
            ascending=[True, False, False, True],
            kind="stable",
        )
        ordered["rank"] = ordered.groupby("ficha", sort=False).cumcount() + 1
        acts_by_ficha = pd.to_numeric(result.set_index("ficha")["actos"], errors="coerce").fillna(0).clip(lower=1)
        ordered["total_actos_ficha"] = ordered["ficha"].map(acts_by_ficha).fillna(1.0)
        ordered["share"] = (ordered["actos_ganados"] / ordered["total_actos_ficha"]).clip(0, 1)
        hhi = (ordered["share"].pow(2).groupby(ordered["ficha"]).sum() * 10_000).rename("concentracion_hhi")
        top3_share = (
            ordered.loc[ordered["rank"].le(3)]
            .groupby("ficha", sort=False)["share"]
            .sum()
            .mul(100.0)
            .clip(upper=100.0)
            .rename("top_3_concentracion_pct")
        )
        winner_metrics = pd.concat([hhi, top3_share], axis=1).reset_index()
        top = ordered[ordered["rank"].le(3)].copy()
        for rank in range(1, 4):
            ranked = top[top["rank"].eq(rank)].set_index("ficha")
            winner_metrics[f"top_{rank}_ganador"] = winner_metrics["ficha"].map(ranked["ganador"])
            winner_metrics[f"top_{rank}_actos"] = winner_metrics["ficha"].map(ranked["actos_ganados"])
            winner_metrics[f"top_{rank}_pct"] = winner_metrics["ficha"].map(ranked["share"] * 100.0)
        if winner_metrics.empty:
            return result
        result = result.drop(columns=[column for column in defaults if column in result.columns]).merge(winner_metrics, on="ficha", how="left")
        for column, default in defaults.items():
            if column not in result.columns:
                result[column] = default
            result[column] = result[column].fillna(default)
        return result

    def monthly_trend(self, filters: AnalyticsFilters, *, fichas: Sequence[str] = ()) -> pd.DataFrame:
        where_sql, params = self._filter_sql(filters)
        ficha_clause = ""
        if fichas:
            placeholders: list[str] = []
            for index, ficha in enumerate(fichas):
                key = f"trend_ficha_{index}"
                params[key] = str(ficha)
                placeholders.append(f":{key}")
            ficha_clause = " AND f.ficha IN (" + ", ".join(placeholders) + ")"
        query = f"""
            SELECT f.ficha,
                   SUBSTR(f.{filters.date_column}, 1, 7) AS mes,
                   COUNT(DISTINCT f.acto_key) AS actos,
                   SUM(f.reference_amount) AS monto_referencia,
                   SUM(f.award_amount) AS monto_adjudicado,
                   AVG(f.participant_count) AS participantes_promedio
            FROM intel_actos_fichas f
            LEFT JOIN intel_ficha_metadata m ON m.ficha = f.ficha
            WHERE {where_sql}{ficha_clause}
            GROUP BY f.ficha, SUBSTR(f.{filters.date_column}, 1, 7)
            ORDER BY mes, f.ficha
        """
        return pd.read_sql_query(text(query), self.engine, params=params)

    def acts_for_ficha(self, ficha: str, filters: AnalyticsFilters) -> pd.DataFrame:
        where_sql, params = self._filter_sql(filters)
        params["selected_ficha"] = str(ficha)
        query = f"""
            SELECT f.acto_key, f.enlace, f.titulo, f.entidad, f.estado,
                   f.publication_date, f.celebration_date, f.award_date, f.update_date,
                   f.reference_amount, f.award_amount, f.award_amount_source,
                   f.winner, f.winner_short, f.participant_count, f.is_unique_ficha,
                   f.detection_score, f.detection_method, f.detection_evidence
            FROM intel_actos_fichas f
            LEFT JOIN intel_ficha_metadata m ON m.ficha = f.ficha
            WHERE {where_sql} AND f.ficha = :selected_ficha
            ORDER BY f.reference_amount DESC, f.enlace
        """
        return pd.read_sql_query(text(query), self.engine, params=params)

    def all_acts_for_ficha(self, ficha: str) -> pd.DataFrame:
        """Devuelve el histórico completo aceptado de una ficha exacta.

        La consulta usa el perfil más flexible disponible, no hereda el rango
        temporal ni los filtros del mapa maestro y conserva todas las políticas
        globales, incluida la exclusión por registro sanitario.
        """
        return self.acts_for_ficha(
            str(ficha),
            AnalyticsFilters(detection_profile="muy_flexible"),
        )

    def all_acts_for_fichas(self, fichas: Sequence[str]) -> pd.DataFrame:
        """Devuelve la union historica de actos para varias fichas.

        La base filtra todas las fichas solicitadas en una sola consulta. Luego
        se consolida una fila por acto para que un acto que contenga dos o mas
        fichas seleccionadas no duplique conteos ni montos. La columna
        ``fichas_coincidentes`` conserva todas las coincidencias del acto.
        """
        selected = normalize_ficha_list(fichas)
        if not selected:
            return pd.DataFrame()

        where_sql, params = self._filter_sql(
            AnalyticsFilters(detection_profile="muy_flexible", fichas=selected)
        )
        query = f"""
            SELECT f.ficha AS ficha_coincidente,
                   f.acto_key, f.enlace, f.titulo, f.entidad, f.estado,
                   f.publication_date, f.celebration_date, f.award_date, f.update_date,
                   f.reference_amount, f.award_amount, f.award_amount_source,
                   f.winner, f.winner_short, f.participant_count, f.is_unique_ficha,
                   f.detection_score, f.detection_method, f.detection_evidence
            FROM intel_actos_fichas f
            LEFT JOIN intel_ficha_metadata m ON m.ficha = f.ficha
            WHERE {where_sql}
            ORDER BY f.reference_amount DESC, f.acto_key, f.ficha
        """
        associations = pd.read_sql_query(text(query), self.engine, params=params)
        if associations.empty:
            return associations

        associations = associations.drop_duplicates(
            subset=["acto_key", "ficha_coincidente"], keep="first"
        ).copy()
        associations["_score"] = pd.to_numeric(
            associations.get("detection_score"), errors="coerce"
        ).fillna(0.0)
        selected_order = {ficha: index for index, ficha in enumerate(selected)}
        rows: list[dict[str, Any]] = []

        for _acto_key, group in associations.groupby("acto_key", sort=False, dropna=False):
            ranked = group.sort_values(
                ["_score", "reference_amount"], ascending=[False, False], kind="stable"
            )
            record = ranked.iloc[0].drop(labels=["_score"]).to_dict()
            record.pop("ficha_coincidente", None)
            codes = sorted(
                {clean_text(value) for value in group["ficha_coincidente"] if clean_text(value)},
                key=lambda value: (selected_order.get(value, len(selected_order)), value),
            )
            record["fichas_coincidentes"] = ", ".join(codes)
            record["fichas_coincidentes_count"] = len(codes)
            record["detection_score"] = float(ranked["_score"].max())
            methods = [
                clean_text(value)
                for value in group.get("detection_method", pd.Series(dtype=str))
                if clean_text(value)
            ]
            record["detection_method"] = ", ".join(dict.fromkeys(methods))
            evidence: list[str] = []
            for row in group.itertuples(index=False):
                ficha = clean_text(getattr(row, "ficha_coincidente", ""))
                detail = clean_text(getattr(row, "detection_evidence", ""))
                if ficha and detail:
                    evidence.append(f"{ficha}: {detail}")
            record["detection_evidence"] = " | ".join(dict.fromkeys(evidence))
            rows.append(record)

        result = pd.DataFrame(rows)
        if result.empty:
            return result
        result["_reference_sort"] = pd.to_numeric(
            result.get("reference_amount"), errors="coerce"
        ).fillna(0.0)
        result = result.sort_values(
            ["_reference_sort", "publication_date", "acto_key"],
            ascending=[False, False, True],
            kind="stable",
        ).drop(columns=["_reference_sort"])
        return result.reset_index(drop=True)

    def find_providers(self, query: str, *, limit: int = 50) -> pd.DataFrame:
        """Busca empresas participantes por nombre dentro del universo elegible.

        ``proveedor_norm`` ya contiene el nombre sin tildes, puntuación ni
        diferencias de mayúsculas. La búsqueda se limita a empresas vinculadas
        con al menos un acto/ficha aceptado y conserva la exclusion global de
        fichas que requieren registro sanitario.
        """
        normalized_query = normalize_text(query)
        if not normalized_query:
            return pd.DataFrame(columns=["proveedor_norm", "proveedor", "actos"])

        where_sql, params = self._filter_sql(AnalyticsFilters(detection_profile="muy_flexible"))
        params.update(
            {
                "provider_query": f"%{normalized_query}%",
                "provider_exact": normalized_query,
                "provider_limit": max(1, min(int(limit), 100)),
            }
        )
        query_sql = f"""
            WITH eligible_acts AS (
                SELECT DISTINCT f.acto_key
                FROM intel_actos_fichas f
                LEFT JOIN intel_ficha_metadata m ON m.ficha = f.ficha
                WHERE {where_sql}
            )
            SELECT p.proveedor_norm,
                   MAX(p.proveedor) AS proveedor,
                   COUNT(DISTINCT p.acto_key) AS actos
            FROM intel_acto_proponentes p
            INNER JOIN eligible_acts a ON a.acto_key = p.acto_key
            WHERE COALESCE(p.proveedor_norm, '') <> ''
              AND p.proveedor_norm LIKE :provider_query
            GROUP BY p.proveedor_norm
            ORDER BY CASE WHEN p.proveedor_norm = :provider_exact THEN 0 ELSE 1 END,
                     actos DESC, proveedor
            LIMIT :provider_limit
        """
        return pd.read_sql_query(text(query_sql), self.engine, params=params)

    def all_acts_for_provider(self, provider_norm: str) -> pd.DataFrame:
        """Devuelve todos los actos/fichas elegibles donde participó una empresa.

        La empresa se identifica por su nombre normalizado exacto, pero no tiene
        que haber ganado el acto. Cada asociación acto/ficha aparece una sola vez.
        El periodo es histórico completo y la política de registro sanitario se
        aplica en SQL antes de devolver resultados.
        """
        normalized_provider = normalize_text(provider_norm)
        if not normalized_provider:
            return pd.DataFrame()

        where_sql, params = self._filter_sql(AnalyticsFilters(detection_profile="muy_flexible"))
        params["selected_provider"] = normalized_provider
        query = f"""
            WITH selected_participations AS (
                SELECT p.acto_key,
                       MAX(p.proveedor) AS proveedor,
                       MAX(p.offered_amount) AS offered_amount,
                       MAX(CASE WHEN p.is_winner = 1 THEN 1 ELSE 0 END) AS is_winner
                FROM intel_acto_proponentes p
                WHERE p.proveedor_norm = :selected_provider
                GROUP BY p.acto_key
            ),
            ficha_metadata AS (
                SELECT ficha,
                       MAX(COALESCE(nombre_ficha, '')) AS nombre_ficha,
                       MAX(COALESCE(registro_sanitario, '')) AS registro_sanitario
                FROM intel_ficha_metadata
                GROUP BY ficha
            )
            SELECT DISTINCT sp.proveedor, :selected_provider AS proveedor_norm,
                   f.ficha, m.nombre_ficha, f.acto_key, f.enlace, f.titulo,
                   f.entidad, f.estado, f.publication_date, f.celebration_date,
                   f.award_date, f.update_date, f.reference_amount, f.award_amount,
                   f.award_amount_source, sp.offered_amount, sp.is_winner,
                   f.winner, f.winner_short, f.participant_count,
                   f.is_unique_ficha, f.detection_score, f.detection_method,
                   f.detection_evidence
            FROM selected_participations sp
            INNER JOIN intel_actos_fichas f ON f.acto_key = sp.acto_key
            LEFT JOIN ficha_metadata m ON m.ficha = f.ficha
            WHERE {where_sql}
            ORDER BY f.reference_amount DESC, f.acto_key, f.ficha
        """
        return pd.read_sql_query(text(query), self.engine, params=params)

    def providers_for_ficha(self, ficha: str, filters: AnalyticsFilters) -> pd.DataFrame:
        where_sql, params = self._filter_sql(filters)
        params["selected_ficha"] = str(ficha)
        query = f"""
            WITH selected_acts AS (
                SELECT DISTINCT f.acto_key
                FROM intel_actos_fichas f
                LEFT JOIN intel_ficha_metadata m ON m.ficha = f.ficha
                WHERE {where_sql} AND f.ficha = :selected_ficha
            )
            SELECT p.proveedor,
                   COUNT(DISTINCT p.acto_key) AS participaciones,
                   COUNT(DISTINCT CASE WHEN p.is_winner = 1 THEN p.acto_key END) AS actos_ganados,
                   SUM(CASE WHEN p.is_winner = 1 THEN p.offered_amount ELSE 0 END) AS monto_ganado,
                   AVG(CASE WHEN p.offered_amount > 0 THEN p.offered_amount END) AS oferta_promedio
            FROM intel_acto_proponentes p
            INNER JOIN selected_acts a ON a.acto_key = p.acto_key
            WHERE COALESCE(p.proveedor, '') <> ''
            GROUP BY p.proveedor
            ORDER BY actos_ganados DESC, monto_ganado DESC, participaciones DESC
        """
        frame = pd.read_sql_query(text(query), self.engine, params=params)
        if not frame.empty:
            frame["tasa_exito_pct"] = (
                pd.to_numeric(frame["actos_ganados"], errors="coerce").fillna(0)
                / pd.to_numeric(frame["participaciones"], errors="coerce").replace(0, pd.NA)
                * 100
            ).fillna(0.0)
        return frame

    def catalog_for_ficha(self, ficha: str) -> pd.DataFrame:
        return pd.read_sql_query(
            text("SELECT * FROM intel_ficha_catalogo WHERE ficha = :ficha ORDER BY oferente, producto"),
            self.engine,
            params={"ficha": str(ficha)},
        )


def _percentile(series: pd.Series, *, higher_is_better: bool = True) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").replace([math.inf, -math.inf], pd.NA).fillna(0.0)
    if numeric.nunique(dropna=False) <= 1:
        return pd.Series(50.0, index=numeric.index)
    ranked = numeric.rank(method="average", pct=True) * 100.0
    return ranked if higher_is_better else 100.0 - ranked


def _weighted_mean(parts: Sequence[tuple[pd.Series, float]]) -> pd.Series:
    total_weight = sum(weight for _, weight in parts) or 1.0
    result = sum((series * weight for series, weight in parts), start=pd.Series(0.0, index=parts[0][0].index))
    return (result / total_weight).clip(0, 100)


def normalize_score_weights(weights: Mapping[str, float] | None = None) -> dict[str, float]:
    raw = dict(DEFAULT_SCORE_WEIGHTS if weights is None else weights)
    output = {key: max(0.0, float(raw.get(key, 0.0) or 0.0)) for key in DEFAULT_SCORE_WEIGHTS}
    total = sum(output.values())
    if total <= 0:
        return dict(DEFAULT_SCORE_WEIGHTS)
    return {key: value / total * 100.0 for key, value in output.items()}


def score_opportunities(frame: pd.DataFrame, weights: Mapping[str, float] | None = None) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    numeric_columns = [
        "actos", "actos_ficha_unica", "entidades", "meses_activos", "monto_referencia", "monto_adjudicado",
        "ticket_promedio", "ticket_mediano", "participantes_promedio", "participantes_mediana",
        "proporcion_unico_proponente", "proponentes_distintos",
        "proveedores_catalogo", "proveedores_contactables", "confianza_deteccion", "cobertura_monto_adjudicado_pct",
        "cobertura_monto_referencia_pct", "cobertura_ganador_pct", "cobertura_participantes_pct",
        "concentracion_hhi", "top_3_concentracion_pct", "pct_ficha_unica", "tendencia_6m_pct",
    ]
    for column in numeric_columns:
        if column not in result.columns:
            result[column] = 0.0
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    result["score_demanda"] = _weighted_mean(
        [
            (_percentile(result["actos"]), 0.38),
            (_percentile(result["actos_ficha_unica"]), 0.20),
            (_percentile(result["entidades"]), 0.18),
            (_percentile(result["meses_activos"]), 0.12),
            (_percentile(result["tendencia_6m_pct"]), 0.12),
        ]
    )
    result["score_economia"] = _weighted_mean(
        [
            (_percentile(result["monto_referencia"]), 0.42),
            (_percentile(result["monto_adjudicado"]), 0.33),
            (_percentile(result["ticket_mediano"]), 0.17),
            (_percentile(result["ticket_promedio"]), 0.08),
        ]
    )
    result["score_competencia"] = _weighted_mean(
        [
            (_percentile(result["participantes_promedio"], higher_is_better=False), 0.38),
            (_percentile(result["participantes_mediana"], higher_is_better=False), 0.14),
            (_percentile(result["proponentes_distintos"] / result["actos"].replace(0, 1), higher_is_better=False), 0.20),
            (_percentile(result["concentracion_hhi"], higher_is_better=False), 0.13),
            (_percentile(result["proporcion_unico_proponente"]), 0.15),
        ]
    )
    ct_component = result.get("tiene_ct", pd.Series("", index=result.index)).astype(str).str.lower().eq("si").astype(float) * 100
    rs_component = result.get("registro_sanitario", pd.Series("", index=result.index)).astype(str).str.lower().eq("no").astype(float) * 100
    result["score_viabilidad"] = _weighted_mean(
        [
            (_percentile(result["proveedores_catalogo"]), 0.38),
            (_percentile(result["proveedores_contactables"]), 0.32),
            (result["cobertura_monto_adjudicado_pct"].clip(0, 100), 0.18),
            (result["cobertura_ganador_pct"].clip(0, 100), 0.12),
        ]
    )
    result["score_complejidad"] = _weighted_mean(
        [
            (ct_component, 0.45),
            (rs_component, 0.35),
            (_percentile(result["pct_ficha_unica"]), 0.20),
        ]
    )
    metadata_component = (
        result.get("nombre_ficha", pd.Series("", index=result.index)).astype(str).str.strip().ne("").astype(float) * 55
        + result.get("enlace_minsa", pd.Series("", index=result.index)).astype(str).str.strip().ne("").astype(float) * 45
    )
    result["score_preparacion"] = _weighted_mean(
        [
            (metadata_component, 0.35),
            (_percentile(result["meses_activos"]), 0.20),
            (_percentile(result["proveedores_contactables"]), 0.25),
            (_percentile(result["cobertura_monto_adjudicado_pct"]), 0.20),
        ]
    )
    result["score_confianza"] = _weighted_mean(
        [
            (result["confianza_deteccion"].clip(0, 100), 0.35),
            (result["cobertura_monto_referencia_pct"].clip(0, 100), 0.18),
            (result["cobertura_monto_adjudicado_pct"].clip(0, 100), 0.17),
            (result["cobertura_ganador_pct"].clip(0, 100), 0.12),
            (result["cobertura_participantes_pct"].clip(0, 100), 0.08),
            (metadata_component, 0.10),
        ]
    )

    normalized_weights = normalize_score_weights(weights)
    result["score_oportunidad"] = sum(
        result[f"score_{key}"] * (weight / 100.0)
        for key, weight in normalized_weights.items()
    ).clip(0, 100)
    result["recomendacion"] = result.apply(_recommendation, axis=1)
    result["razones"] = result.apply(_explain_score, axis=1)
    for column in [column for column in result.columns if column.startswith("score_")]:
        result[column] = result[column].round(1)
    return result


def _recommendation(row: pd.Series) -> str:
    score = float(row.get("score_oportunidad", 0) or 0)
    confidence = float(row.get("score_confianza", 0) or 0)
    catalog = float(row.get("proveedores_catalogo", 0) or 0)
    if confidence < 45:
        return "Requiere validación"
    if score >= 76 and catalog > 0:
        return "Atacar ahora"
    if score >= 67 and catalog <= 0:
        return "Vale la pena buscar proveedor"
    if score >= 56:
        return "Observación activa"
    return "Baja prioridad"


def _explain_score(row: pd.Series) -> str:
    dimensions = [
        ("demanda", float(row.get("score_demanda", 0) or 0)),
        ("economía", float(row.get("score_economia", 0) or 0)),
        ("competencia", float(row.get("score_competencia", 0) or 0)),
        ("viabilidad", float(row.get("score_viabilidad", 0) or 0)),
        ("preparación", float(row.get("score_preparacion", 0) or 0)),
        ("complejidad favorable", float(row.get("score_complejidad", 0) or 0)),
        ("confianza", float(row.get("score_confianza", 0) or 0)),
    ]
    ordered = sorted(dimensions, key=lambda item: item[1], reverse=True)
    strengths = ", ".join(name for name, value in ordered[:2] if value >= 55) or "sin fortaleza dominante"
    weakness = min(dimensions, key=lambda item: item[1])
    return f"Fortalezas: {strengths}. Principal freno: {weakness[0]} ({weakness[1]:.0f}/100)."


def apply_master_filters(
    frame: pd.DataFrame,
    *,
    min_acts: int = 0,
    min_entities: int = 0,
    max_participants: float = 0.0,
    min_score: float = 0.0,
    recommendations: Sequence[str] = (),
    catalog_only: bool = False,
) -> pd.DataFrame:
    result = frame.copy()
    if min_acts > 0:
        result = result[pd.to_numeric(result["actos"], errors="coerce").fillna(0) >= min_acts]
    if min_entities > 0:
        result = result[pd.to_numeric(result["entidades"], errors="coerce").fillna(0) >= min_entities]
    if max_participants > 0:
        result = result[pd.to_numeric(result["participantes_promedio"], errors="coerce").fillna(0) <= max_participants]
    if min_score > 0:
        result = result[pd.to_numeric(result["score_oportunidad"], errors="coerce").fillna(0) >= min_score]
    if recommendations:
        result = result[result["recomendacion"].isin(recommendations)]
    if catalog_only:
        result = result[pd.to_numeric(result["proveedores_catalogo"], errors="coerce").fillna(0) > 0]
    return result.copy()


def sort_and_page(
    frame: pd.DataFrame,
    *,
    sort_by: str,
    ascending: bool,
    page: int,
    page_size: int,
) -> tuple[pd.DataFrame, int, int]:
    if frame.empty:
        return frame.copy(), 0, 0
    column = sort_by if sort_by in frame.columns else "score_oportunidad"
    ordered = frame.sort_values(column, ascending=ascending, kind="stable", na_position="last")
    size = max(1, int(page_size))
    pages = max(1, math.ceil(len(ordered) / size))
    current = min(max(1, int(page)), pages)
    start = (current - 1) * size
    return ordered.iloc[start : start + size].copy(), pages, len(ordered)


def dataframe_to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8-sig")


def preset_range(key: str, *, today: date | None = None) -> tuple[date | None, date | None]:
    now = today or date.today()
    if key == "2026":
        return date(2026, 1, 1), min(now, date(2026, 12, 31))
    if key == "2025":
        return date(2025, 1, 1), date(2025, 12, 31)
    if key == "ultimos_6_meses":
        return now - timedelta(days=182), now
    if key == "ultimos_12_meses":
        return now - timedelta(days=365), now
    if key == "ultimos_24_meses":
        return now - timedelta(days=730), now
    if key == "historico":
        return None, None
    return now - timedelta(days=365), now
