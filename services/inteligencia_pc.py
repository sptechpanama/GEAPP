from __future__ import annotations

"""Motor analitico para el mercado no medico de Panama Compra.

La capa se apoya directamente en ``actos_publicos`` y evita depender de las
tablas centradas en fichas MINSA.  Las funciones puras de clasificacion y
normalizacion se mantienen separadas del repositorio para poder probarlas sin
conectarse a Supabase.
"""

import json
import math
import os
import re
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


NO_FICHA_VALUES = {
    "",
    "no detectada",
    "no_detectada",
    "no detectado",
    "sin ficha",
    "ninguna",
    "none",
    "null",
    "nan",
}

# Terminos que describen productos o procedimientos clinicos. Una entidad de
# salud no basta para excluir un acto: un hospital tambien compra chillers,
# obras civiles, electricidad o tecnologia.
STRONG_MEDICAL_TERMS = (
    "medicamento",
    "farmaceut",
    "dispositivo medico",
    "insumo medico",
    "material medico",
    "equipo medico",
    "quirurg",
    "hemodial",
    "esteriliza",
    "reactivo de laboratorio",
    "laboratorio clinico",
    "paciente",
    "diagnostico clinico",
    "odontolog",
    "protesis",
    "implante",
    "sutura",
    "cateter",
    "jeringa",
    "guante de examen",
    "registro sanitario",
)

NON_MEDICAL_OVERRIDE_TERMS = (
    "aire acondicionado",
    "chiller",
    "refrigeracion",
    "ventilacion mecanica",
    "obra civil",
    "construccion",
    "remodelacion",
    "mantenimiento de infraestructura",
    "instalacion electrica",
    "planta electrica",
    "transformador",
    "bomba de agua",
    "plomeria",
    "sistema contra incendio",
    "software",
    "computadora",
    "servidor",
    "telecomunicacion",
    "vehiculo",
    "mobiliario",
    "limpieza",
)

FAMILY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Climatizacion, refrigeracion y HVAC", ("chiller", "aire acondicionado", "refrigeracion", "hvac", "ventilacion", "unidad manejadora", "torre de enfriamiento")),
    ("Electricidad y energia", ("electrico", "electricidad", "transformador", "planta electrica", "generador", "tablero electrico", "luminaria", "panel solar", "fotovolta")),
    ("Construccion y remodelacion", ("construccion", "obra civil", "remodelacion", "rehabilitacion", "impermeabilizacion", "pintura", "albanileria", "cubierta de techo")),
    ("Agua, bombeo y plomeria", ("bomba de agua", "plomeria", "tuberia", "acueducto", "alcantarillado", "tratamiento de agua", "tanque de agua", "pozo")),
    ("Tecnologia y telecomunicaciones", ("software", "computadora", "servidor", "switch", "router", "telecomunicacion", "licencia informatica", "ciberseguridad", "impresora")),
    ("Seguridad y sistemas contra incendio", ("contra incendio", "extintor", "deteccion de incendio", "videovigilancia", "camara de seguridad", "control de acceso", "alarma")),
    ("Mantenimiento industrial", ("mantenimiento industrial", "mantenimiento preventivo", "mantenimiento correctivo", "compresor", "motor industrial", "soldadura", "maquinaria")),
    ("Transporte, flota y repuestos", ("vehiculo", "automovil", "camion", "bus", "llanta", "neumatico", "repuesto", "lubricante", "combustible")),
    ("Mobiliario y equipamiento general", ("mobiliario", "escritorio", "silla de oficina", "archivador", "estanteria", "electrodomestico")),
    ("Limpieza, logistica y servicios", ("limpieza", "aseo", "fumigacion", "transporte de carga", "logistica", "mensajeria", "recoleccion de desechos")),
    ("Alimentos y abastecimiento", ("alimento", "comida", "viveres", "bebida", "catering", "producto alimenticio")),
    ("Impresion, publicidad y oficina", ("impresion", "publicidad", "papeleria", "utiles de oficina", "material promocional", "rotulacion")),
)


def _normalize_literal(value: str) -> str:
    result = unicodedata.normalize("NFKD", value.lower())
    result = "".join(ch for ch in result if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", result)).strip()


NORMALIZED_MEDICAL_TERMS = tuple((_normalize_literal(term), term) for term in STRONG_MEDICAL_TERMS)
NORMALIZED_OVERRIDE_TERMS = tuple((_normalize_literal(term), term) for term in NON_MEDICAL_OVERRIDE_TERMS)
NORMALIZED_FAMILY_RULES = tuple(
    (family, tuple((_normalize_literal(term), term) for term in terms))
    for family, terms in FAMILY_RULES
)


def clean_text(value: object) -> str:
    result = str(value if value is not None else "").strip()
    if result.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return re.sub(r"\s+", " ", result)


def normalize_text(value: object) -> str:
    result = unicodedata.normalize("NFKD", clean_text(value).lower())
    result = "".join(ch for ch in result if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", result)).strip()


def normalize_provider(value: object) -> str:
    value_norm = normalize_text(value)
    suffixes = {
        "sa", "s a", "inc", "corp", "corporation", "ltd", "limitada", "llc",
        "panama", "de panama", "sociedad anonima",
    }
    tokens = value_norm.split()
    while tokens and " ".join(tokens[-2:]) in suffixes:
        tokens = tokens[:-2]
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    return " ".join(tokens) or value_norm


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


def parse_date(value: object) -> pd.Timestamp | pd.NaT:
    raw = clean_text(value)
    if not raw:
        return pd.NaT
    candidates = (
        ("%d-%m-%Y", raw[:10]),
        ("%Y-%m-%d", raw[:10]),
        ("%d/%m/%Y", raw[:10]),
    )
    for date_format, candidate in candidates:
        parsed = pd.to_datetime(candidate, errors="coerce", format=date_format)
        if not pd.isna(parsed):
            return parsed.normalize()
    parsed = pd.to_datetime(raw, errors="coerce")
    if not pd.isna(parsed):
        return parsed.normalize()
    return pd.NaT


def _has_valid_ficha(ficha_value: object, fichas_json: object = "") -> bool:
    raw = normalize_text(ficha_value)
    if raw not in NO_FICHA_VALUES and re.search(r"\d{3,}", raw):
        return True
    payload = clean_text(fichas_json)
    if not payload or payload.lower() in {"[]", "{}", "null", "none"}:
        return False
    try:
        decoded = json.loads(payload)
        return bool(decoded)
    except (TypeError, ValueError, json.JSONDecodeError):
        return bool(re.search(r"\d{3,}", payload))


def classify_pc_market(
    *,
    title: object,
    description: object = "",
    items: object = "",
    ficha_detectada: object = "",
    fichas_json: object = "",
) -> tuple[str, str]:
    """Clasifica un acto en no medico, medico o ambiguo con evidencia."""

    if _has_valid_ficha(ficha_detectada, fichas_json):
        return "medico", "ficha_tecnica_detectada"
    corpus = normalize_text(f"{clean_text(title)} {clean_text(description)} {clean_text(items)}")
    overrides = [original for normalized, original in NORMALIZED_OVERRIDE_TERMS if normalized in corpus]
    medical = [original for normalized, original in NORMALIZED_MEDICAL_TERMS if normalized in corpus]
    if medical and not overrides:
        return "medico", f"termino_medico:{medical[0]}"
    if medical and overrides:
        return "ambiguo", f"evidencia_mixta:{medical[0]}|{overrides[0]}"
    return "no_medico", "sin_evidencia_medica"


def classify_project_family(*values: object) -> tuple[str, float, str]:
    corpus = normalize_text(" ".join(clean_text(value) for value in values))
    best_family = "Otros rubros no medicos"
    best_hits: list[str] = []
    for family, terms in NORMALIZED_FAMILY_RULES:
        hits = [original for normalized, original in terms if normalized in corpus]
        if len(hits) > len(best_hits):
            best_family = family
            best_hits = hits
    if not best_hits:
        return best_family, 35.0, "sin_regla_especifica"
    confidence = min(100.0, 70.0 + 10.0 * (len(best_hits) - 1))
    return best_family, confidence, ", ".join(best_hits[:4])


@dataclass(frozen=True)
class PCFilters:
    start_date: date | None = None
    end_date: date | None = None
    states: tuple[str, ...] = field(default_factory=tuple)
    entities: tuple[str, ...] = field(default_factory=tuple)
    families: tuple[str, ...] = field(default_factory=tuple)
    search_groups: tuple[str, ...] = field(default_factory=tuple)
    search_mode: str = "OR"
    min_amount: float = 0.0
    max_amount: float = 0.0
    include_ambiguous: bool = False


class PCAnalyticsUnavailable(RuntimeError):
    pass


CORE_COLUMNS = (
    "id", "publicacion", "fecha", "fecha_adjudicacion", "fecha_actualizacion",
    "enlace", "titulo", "descripcion", "entidad", "unidad_solic", "estado",
    "precio_referencia", "razon_social", "nombre_comercial", "num_participantes",
    "total_items_ofertados", "ficha_detectada", "fichas_detectadas_json",
    "items_json", "source_tipo_proceso",
)


def _existing_columns(engine: Engine, table: str) -> set[str]:
    return {str(column["name"]) for column in inspect(engine).get_columns(table)}


class InteligenciaPCRepository:
    def __init__(self, engine: Engine, *, source_label: str, owns_engine: bool = True) -> None:
        self.engine = engine
        self.source_label = source_label
        self.owns_engine = owns_engine
        self.dialect = engine.dialect.name
        tables = set(inspect(engine).get_table_names())
        self.tables = tables
        self.has_pc_layer = {"pc_actos", "pc_propuestas", "pc_proveedores_catalogo"}.issubset(tables)
        self.has_provider_daily = "pc_proveedores_dia" in tables
        self.has_family_daily = "pc_familias_dia_entidad" in tables
        if "actos_publicos" not in tables and not self.has_pc_layer:
            raise PCAnalyticsUnavailable("No existe actos_publicos ni la capa pc_actos.")
        self.columns = _existing_columns(engine, "actos_publicos") if "actos_publicos" in tables else set()

    @classmethod
    def connect(cls, *, database_url: str = "", local_candidates: Sequence[Path] = ()) -> "InteligenciaPCRepository":
        errors: list[str] = []
        url = clean_text(database_url or os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"))
        if url:
            try:
                engine = create_engine(url, pool_pre_ping=True, pool_recycle=240, connect_args={"connect_timeout": 12})
                repository = cls(engine, source_label="Supabase (Panama Compra)")
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
                return cls(engine, source_label=f"SQLite ({path.name})")
            except Exception as exc:
                errors.append(f"{path}: {exc}")
        raise PCAnalyticsUnavailable(" | ".join(errors) or "No hay una base operacional disponible.")

    def close(self) -> None:
        if self.owns_engine:
            self.engine.dispose()

    def _quoted_columns(self, columns: Iterable[str]) -> str:
        selected = [column for column in columns if column in self.columns]
        return ", ".join(f'"{column}"' for column in selected)

    def _date_expression(self) -> str:
        candidates = [column for column in ("publicacion", "fecha", "fecha_adjudicacion", "fecha_actualizacion") if column in self.columns]
        if self.dialect == "postgresql":
            pieces = [
                f"CASE WHEN COALESCE(\"{column}\", '') ~ '^([0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}})' "
                f"THEN TO_DATE(SUBSTRING(\"{column}\" FROM 1 FOR 10), 'DD-MM-YYYY') END"
                for column in candidates
            ]
        else:
            pieces = [
                f"CASE WHEN length(COALESCE(\"{column}\", '')) >= 10 THEN date(substr(\"{column}\",7,4)||'-'||substr(\"{column}\",4,2)||'-'||substr(\"{column}\",1,2)) END"
                for column in candidates
            ]
        return f"COALESCE({', '.join(pieces)})" if pieces else "NULL"

    def _sql_where(self, filters: PCFilters, *, provider_search: str = "") -> tuple[str, dict[str, Any]]:
        clauses = ["1=1"]
        params: dict[str, Any] = {}
        date_expr = self._date_expression()
        if filters.start_date:
            clauses.append(f"{date_expr} >= :start_date")
            params["start_date"] = filters.start_date.isoformat()
        if filters.end_date:
            clauses.append(f"{date_expr} <= :end_date")
            params["end_date"] = filters.end_date.isoformat()
        if filters.states and "estado" in self.columns:
            names = []
            for idx, state in enumerate(filters.states):
                key = f"state_{idx}"
                names.append(f":{key}")
                params[key] = state
            clauses.append(f'"estado" IN ({", ".join(names)})')
        if filters.entities and "entidad" in self.columns:
            names = []
            for idx, entity in enumerate(filters.entities):
                key = f"entity_{idx}"
                names.append(f":{key}")
                params[key] = entity
            clauses.append(f'"entidad" IN ({", ".join(names)})')
        # La primera barrera se ejecuta en SQL para reducir transferencia. La
        # segunda barrera semantica se aplica en Python y deja trazabilidad.
        if "ficha_detectada" in self.columns:
            clauses.append("lower(trim(COALESCE(\"ficha_detectada\", ''))) IN ('', 'no detectada', 'no_detectada', 'no detectado', 'sin ficha', 'ninguna', 'none', 'null', 'nan')")
        if "fichas_detectadas_json" in self.columns:
            clauses.append("lower(trim(COALESCE(\"fichas_detectadas_json\", ''))) IN ('', '[]', '{}', 'null', 'none')")
        if provider_search:
            provider_clauses: list[str] = []
            params["provider_search"] = f"%{provider_search.lower()}%"
            for index in range(1, 15):
                column = f"Proponente {index}"
                if column in self.columns:
                    provider_clauses.append(f'lower(COALESCE("{column}", \'\')) LIKE :provider_search')
            for column in ("razon_social", "nombre_comercial"):
                if column in self.columns:
                    provider_clauses.append(f'lower(COALESCE("{column}", \'\')) LIKE :provider_search')
            if provider_clauses:
                clauses.append(f"({' OR '.join(provider_clauses)})")
        return " AND ".join(clauses), params

    def filter_options(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {"states": [], "entities": [], "families": [family for family, _ in FAMILY_RULES] + ["Otros rubros no medicos"]}
        with self.engine.connect() as connection:
            if self.has_pc_layer:
                for target, column in (("states", "estado"), ("entities", "entidad"), ("families", "familia")):
                    frame = pd.read_sql_query(text(f'SELECT DISTINCT "{column}" value FROM pc_actos WHERE trim(COALESCE("{column}", \'\')) <> \'\' ORDER BY 1'), connection)
                    result[target] = [clean_text(value) for value in frame["value"].tolist() if clean_text(value)]
                return result
            for target, column in (("states", "estado"), ("entities", "entidad")):
                if column not in self.columns:
                    continue
                frame = pd.read_sql_query(text(f'SELECT DISTINCT "{column}" value FROM actos_publicos WHERE trim(COALESCE("{column}", \'\')) <> \'\' ORDER BY 1'), connection)
                result[target] = [clean_text(value) for value in frame["value"].tolist() if clean_text(value)]
        return result

    def load_acts(self, filters: PCFilters) -> pd.DataFrame:
        if self.has_pc_layer:
            where, params = self._pc_where(filters)
            with self.engine.connect() as connection:
                frame = pd.read_sql_query(text(f"SELECT * FROM pc_actos WHERE {where}"), connection, params=params)
            frame["fecha_analitica"] = pd.to_datetime(frame.get("fecha_analitica"), errors="coerce")
            frame["monto_referencia"] = pd.to_numeric(frame.get("monto_referencia", 0), errors="coerce").fillna(0)
            return frame.reset_index(drop=True)
        where, params = self._sql_where(filters)
        columns = self._quoted_columns(CORE_COLUMNS)
        query = f"SELECT {columns} FROM actos_publicos WHERE {where}"
        with self.engine.connect() as connection:
            frame = pd.read_sql_query(text(query), connection, params=params)
        return prepare_pc_acts(frame, filters)

    def market_summary(self, filters: PCFilters) -> dict[str, float]:
        """Resume el universo completo en SQL sin transferir sus filas a Streamlit."""

        if not self.has_pc_layer:
            acts = self.load_acts(filters)
            return {
                "actos": float(acts["acto_key"].nunique()) if not acts.empty else 0.0,
                "monto": float(acts.get("monto_referencia", pd.Series(dtype=float)).sum()) if not acts.empty else 0.0,
                "entidades": float(acts["entidad"].nunique()) if not acts.empty else 0.0,
                "ticket_promedio": float(acts["monto_referencia"].mean()) if not acts.empty else 0.0,
                "familias": float(acts["familia"].nunique()) if not acts.empty else 0.0,
            }
        where, params = self._pc_where(filters)
        query = f"""
            SELECT COUNT(*) AS actos,
                   COALESCE(SUM(monto_referencia),0) AS monto,
                   COUNT(DISTINCT entidad) AS entidades,
                   COALESCE(AVG(monto_referencia),0) AS ticket_promedio,
                   COUNT(DISTINCT familia) AS familias
            FROM pc_actos
            WHERE {where}
        """
        with self.engine.connect() as connection:
            row = connection.execute(text(query), params).mappings().one()
        return {key: float(row.get(key) or 0) for key in ("actos", "monto", "entidades", "ticket_promedio", "familias")}

    def family_market_summary(self, filters: PCFilters) -> pd.DataFrame:
        """Agrega las familias en la BD; el score se aplica luego a estas pocas filas."""

        if not self.has_pc_layer:
            return family_summary(self.load_acts(filters))
        aggregate_compatible = self.has_family_daily and not any(
            (
                filters.states,
                filters.search_groups,
                filters.min_amount > 0,
                filters.max_amount > 0,
                filters.include_ambiguous,
            )
        )
        if aggregate_compatible:
            clauses = ["1=1"]
            params: dict[str, Any] = {}
            if filters.start_date:
                clauses.append("fecha_analitica >= :family_start")
                params["family_start"] = filters.start_date.isoformat()
            if filters.end_date:
                clauses.append("fecha_analitica <= :family_end")
                params["family_end"] = filters.end_date.isoformat()
            for values, column, label in (
                (filters.entities, "entidad", "family_entity"),
                (filters.families, "familia", "family_name"),
            ):
                if not values:
                    continue
                keys = []
                for index, value in enumerate(values):
                    key = f"{label}_{index}"
                    keys.append(f":{key}")
                    params[key] = value
                clauses.append(f'"{column}" IN ({", ".join(keys)})')
            query = f"""
                SELECT familia,
                       SUM(actos) AS actos,
                       SUM(monto_total) AS monto_total,
                       SUM(monto_total) / NULLIF(SUM(actos),0) AS ticket_promedio,
                       SUM(participantes_suma) / NULLIF(SUM(participantes_con_dato),0) AS participantes_promedio,
                       COUNT(DISTINCT entidad) AS entidades,
                       COUNT(DISTINCT SUBSTRING(fecha_analitica,1,7)) AS meses_activos
                FROM pc_familias_dia_entidad
                WHERE {' AND '.join(clauses)}
                GROUP BY familia
                ORDER BY monto_total DESC
            """
            with self.engine.connect() as connection:
                return pd.read_sql_query(text(query), connection, params=params)
        where, params = self._pc_where(filters)
        if self.dialect == "postgresql":
            participants = "NULLIF(regexp_replace(COALESCE(num_participantes,''),'[^0-9.]','','g'),'')::numeric"
        else:
            participants = "CAST(NULLIF(num_participantes,'') AS REAL)"
        query = f"""
            SELECT familia,
                   COUNT(DISTINCT acto_key) AS actos,
                   COALESCE(SUM(monto_referencia),0) AS monto_total,
                   COALESCE(AVG(monto_referencia),0) AS ticket_promedio,
                   COALESCE(AVG({participants}),0) AS participantes_promedio,
                   COUNT(DISTINCT entidad) AS entidades,
                   COUNT(DISTINCT SUBSTRING(fecha_analitica,1,7)) AS meses_activos
            FROM pc_actos
            WHERE {where}
            GROUP BY familia
            ORDER BY monto_total DESC
        """
        with self.engine.connect() as connection:
            return pd.read_sql_query(text(query), connection, params=params)

    def monthly_market_trend(self, filters: PCFilters) -> pd.DataFrame:
        if not self.has_pc_layer:
            return monthly_trend(self.load_acts(filters))
        aggregate_compatible = self.has_family_daily and not any(
            (
                filters.states,
                filters.search_groups,
                filters.min_amount > 0,
                filters.max_amount > 0,
                filters.include_ambiguous,
            )
        )
        if aggregate_compatible:
            clauses = ["fecha_analitica IS NOT NULL"]
            params: dict[str, Any] = {}
            if filters.start_date:
                clauses.append("fecha_analitica >= :trend_start")
                params["trend_start"] = filters.start_date.isoformat()
            if filters.end_date:
                clauses.append("fecha_analitica <= :trend_end")
                params["trend_end"] = filters.end_date.isoformat()
            for values, column, label in (
                (filters.entities, "entidad", "trend_entity"),
                (filters.families, "familia", "trend_family"),
            ):
                if not values:
                    continue
                keys = []
                for index, value in enumerate(values):
                    key = f"{label}_{index}"
                    keys.append(f":{key}")
                    params[key] = value
                clauses.append(f'"{column}" IN ({", ".join(keys)})')
            query = f"""
                SELECT SUBSTRING(fecha_analitica,1,7) AS periodo,
                       SUM(actos) AS actos,
                       SUM(monto_total) AS monto,
                       COUNT(DISTINCT entidad) AS entidades
                FROM pc_familias_dia_entidad
                WHERE {' AND '.join(clauses)}
                GROUP BY SUBSTRING(fecha_analitica,1,7)
                ORDER BY periodo
            """
            with self.engine.connect() as connection:
                frame = pd.read_sql_query(text(query), connection, params=params)
            frame["periodo"] = pd.to_datetime(frame.get("periodo"), errors="coerce")
            return frame
        where, params = self._pc_where(filters)
        query = f"""
            SELECT SUBSTRING(fecha_analitica,1,7) AS periodo,
                   COUNT(DISTINCT acto_key) AS actos,
                   COALESCE(SUM(monto_referencia),0) AS monto,
                   COUNT(DISTINCT entidad) AS entidades
            FROM pc_actos
            WHERE {where} AND fecha_analitica IS NOT NULL
            GROUP BY SUBSTRING(fecha_analitica,1,7)
            ORDER BY periodo
        """
        with self.engine.connect() as connection:
            frame = pd.read_sql_query(text(query), connection, params=params)
        frame["periodo"] = pd.to_datetime(frame.get("periodo"), errors="coerce")
        return frame

    def project_page(
        self,
        filters: PCFilters,
        *,
        sort_column: str = "fecha_analitica",
        ascending: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[pd.DataFrame, int]:
        allowed = {"fecha_analitica", "monto_referencia", "num_participantes", "titulo", "entidad", "familia"}
        sort_column = sort_column if sort_column in allowed else "fecha_analitica"
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        if not self.has_pc_layer:
            frame = self.load_acts(filters)
            if sort_column == "num_participantes":
                frame[sort_column] = pd.to_numeric(frame.get(sort_column, 0), errors="coerce").fillna(0)
            frame = frame.sort_values(sort_column, ascending=ascending)
            return frame.iloc[offset : offset + limit].copy(), len(frame)
        where, params = self._pc_where(filters)
        params.update({"pc_limit": limit, "pc_offset": offset})
        if sort_column == "num_participantes":
            order_expression = (
                "COALESCE(NULLIF(regexp_replace(COALESCE(num_participantes,''),'[^0-9.]','','g'),'')::numeric,0)"
                if self.dialect == "postgresql"
                else "COALESCE(CAST(NULLIF(num_participantes,'') AS REAL),0)"
            )
        else:
            order_expression = f'"{sort_column}"'
        direction = "ASC" if ascending else "DESC"
        columns = "fecha_analitica,titulo,familia,entidad,estado,monto_referencia,num_participantes,enlace,acto_key"
        with self.engine.connect() as connection:
            total = int(connection.execute(text(f"SELECT COUNT(*) FROM pc_actos WHERE {where}"), params).scalar_one())
            frame = pd.read_sql_query(
                text(f"SELECT {columns} FROM pc_actos WHERE {where} ORDER BY {order_expression} {direction} NULLS LAST LIMIT :pc_limit OFFSET :pc_offset"),
                connection,
                params=params,
            )
        frame["fecha_analitica"] = pd.to_datetime(frame.get("fecha_analitica"), errors="coerce")
        frame["monto_referencia"] = pd.to_numeric(frame.get("monto_referencia", 0), errors="coerce").fillna(0)
        frame["num_participantes"] = pd.to_numeric(frame.get("num_participantes", 0), errors="coerce").fillna(0)
        return frame, total

    def provider_market_ranking(self, filters: PCFilters, *, limit: int = 300) -> pd.DataFrame:
        """Ranking completo calculado en PostgreSQL, sin enviar todas las ofertas al navegador."""

        if not self.has_pc_layer:
            return provider_ranking(self.load_proposals(filters)).head(limit)
        simple_period_filter = not any(
            (
                filters.states,
                filters.entities,
                filters.families,
                filters.search_groups,
                filters.min_amount > 0,
                filters.max_amount > 0,
                filters.include_ambiguous,
            )
        )
        if self.has_provider_daily and simple_period_filter:
            clauses = ["1=1"]
            params: dict[str, Any] = {"pc_provider_limit": max(1, min(int(limit), 1000))}
            if filters.start_date:
                clauses.append("fecha_analitica >= :provider_start")
                params["provider_start"] = filters.start_date.isoformat()
            if filters.end_date:
                clauses.append("fecha_analitica <= :provider_end")
                params["provider_end"] = filters.end_date.isoformat()
            query = f"""
                SELECT MIN(proveedor) AS proveedor,
                       proveedor_norm,
                       SUM(participaciones) AS participaciones,
                       SUM(adjudicaciones) AS adjudicaciones,
                       100.0 * SUM(adjudicaciones) / NULLIF(SUM(participaciones),0) AS tasa_exito,
                       SUM(monto_ofertado) AS monto_ofertado,
                       SUM(monto_ganado) AS monto_ganado,
                       MIN(oferta_minima) AS oferta_minima,
                       SUM(monto_ofertado) / NULLIF(SUM(ofertas_validas),0) AS oferta_promedio,
                       MAX(oferta_maxima) AS oferta_maxima
                FROM pc_proveedores_dia
                WHERE {' AND '.join(clauses)}
                GROUP BY proveedor_norm
                ORDER BY monto_ganado DESC, adjudicaciones DESC, participaciones DESC
                LIMIT :pc_provider_limit
            """
            with self.engine.connect() as connection:
                return pd.read_sql_query(text(query), connection, params=params)
        where, params = self._pc_where(filters, alias="a")
        params["pc_provider_limit"] = max(1, min(int(limit), 1000))
        query = f"""
            SELECT MIN(p.proveedor) AS proveedor,
                   p.proveedor_norm,
                   COUNT(DISTINCT p.acto_key) AS participaciones,
                   COUNT(DISTINCT CASE WHEN p.ganado=1 THEN p.acto_key END) AS adjudicaciones,
                   100.0 * COUNT(DISTINCT CASE WHEN p.ganado=1 THEN p.acto_key END)
                         / NULLIF(COUNT(DISTINCT p.acto_key),0) AS tasa_exito,
                   COALESCE(SUM(p.monto_ofertado),0) AS monto_ofertado,
                   COALESCE(SUM(p.monto_ganado),0) AS monto_ganado,
                   COALESCE(MIN(p.monto_ofertado),0) AS oferta_minima,
                   COALESCE(AVG(p.monto_ofertado),0) AS oferta_promedio,
                   COALESCE(MAX(p.monto_ofertado),0) AS oferta_maxima,
                   COUNT(DISTINCT a.familia) AS familias,
                   COUNT(DISTINCT a.entidad) AS entidades
            FROM pc_propuestas p
            JOIN pc_actos a ON a.acto_key=p.acto_key
            WHERE {where} AND trim(COALESCE(p.proveedor_norm,'')) <> ''
            GROUP BY p.proveedor_norm
            ORDER BY monto_ganado DESC, adjudicaciones DESC, participaciones DESC
            LIMIT :pc_provider_limit
        """
        with self.engine.connect() as connection:
            return pd.read_sql_query(text(query), connection, params=params)

    def company_options(self, search: str, *, limit: int = 80) -> list[str]:
        term = clean_text(search)
        if len(normalize_text(term)) < 2:
            return []
        if self.has_pc_layer:
            with self.engine.connect() as connection:
                frame = pd.read_sql_query(
                    text("SELECT proveedor FROM pc_proveedores_catalogo WHERE lower(proveedor) LIKE :pattern OR proveedor_norm LIKE :normalized ORDER BY participaciones DESC LIMIT :limit"),
                    connection,
                    params={"pattern": f"%{term.lower()}%", "normalized": f"%{normalize_provider(term)}%", "limit": int(limit)},
                )
            return [clean_text(value) for value in frame.get("proveedor", pd.Series(dtype=str)).tolist() if clean_text(value)]
        selects: list[str] = []
        for index in range(1, 15):
            column = f"Proponente {index}"
            if column in self.columns:
                selects.append(f'SELECT "{column}" proveedor FROM actos_publicos WHERE lower(COALESCE("{column}", \'\')) LIKE :pattern')
        for column in ("razon_social", "nombre_comercial"):
            if column in self.columns:
                selects.append(f'SELECT "{column}" proveedor FROM actos_publicos WHERE lower(COALESCE("{column}", \'\')) LIKE :pattern')
        if not selects:
            return []
        query = "SELECT proveedor, COUNT(*) frecuencia FROM (" + " UNION ALL ".join(selects) + ") p WHERE trim(COALESCE(proveedor,'')) <> '' GROUP BY proveedor ORDER BY frecuencia DESC LIMIT :limit"
        with self.engine.connect() as connection:
            frame = pd.read_sql_query(text(query), connection, params={"pattern": f"%{term.lower()}%", "limit": int(limit)})
        return [clean_text(value) for value in frame.get("proveedor", pd.Series(dtype=str)).tolist() if clean_text(value)]

    def company_acts(self, company: str, filters: PCFilters) -> pd.DataFrame:
        if self.has_pc_layer:
            return self._company_acts_pc_layer(company, filters)
        where, params = self._sql_where(filters, provider_search=clean_text(company))
        provider_columns: list[str] = []
        for index in range(1, 15):
            provider_columns.extend((f"Proponente {index}", f"Precio Proponente {index}"))
        columns = self._quoted_columns((*CORE_COLUMNS, *provider_columns))
        query = f"SELECT {columns} FROM actos_publicos WHERE {where}"
        with self.engine.connect() as connection:
            raw = pd.read_sql_query(text(query), connection, params=params)
        acts = prepare_pc_acts(raw, filters)
        return build_company_acts(acts, company)

    def load_proposals(self, filters: PCFilters) -> pd.DataFrame:
        """Carga propuestas solo cuando una vista empresarial las necesita."""

        if self.has_pc_layer:
            where, params = self._pc_where(filters, alias="a")
            query = f"""
                SELECT p.*, a.fecha_analitica, a.familia, a.entidad, a.estado,
                       a.enlace, a.titulo, a.monto_referencia
                FROM pc_propuestas p
                JOIN pc_actos a ON a.acto_key=p.acto_key
                WHERE {where}
            """
            with self.engine.connect() as connection:
                frame = pd.read_sql_query(text(query), connection, params=params)
            frame["fecha_analitica"] = pd.to_datetime(frame.get("fecha_analitica"), errors="coerce")
            frame["ganado"] = frame.get("ganado", False).fillna(False).astype(bool)
            return frame

        where, params = self._sql_where(filters)
        provider_columns: list[str] = []
        for index in range(1, 15):
            provider_columns.extend((f"Proponente {index}", f"Precio Proponente {index}"))
        columns = self._quoted_columns((*CORE_COLUMNS, *provider_columns))
        query = f"SELECT {columns} FROM actos_publicos WHERE {where}"
        with self.engine.connect() as connection:
            raw = pd.read_sql_query(text(query), connection, params=params)
        acts = prepare_pc_acts(raw, filters)
        if acts.empty:
            return pd.DataFrame()
        proposals = unpivot_proposals(acts)
        context_columns = [
            column for column in (
                "acto_key", "id", "fecha_analitica", "familia", "entidad", "estado", "enlace",
                "titulo", "monto_referencia", "razon_social", "nombre_comercial",
            ) if column in acts.columns
        ]
        proposals = proposals.merge(acts[context_columns].drop_duplicates("acto_key"), on="acto_key", how="left", suffixes=("", "_acto"))
        winner = proposals.get("razon_social", pd.Series("", index=proposals.index)).fillna("")
        fallback = proposals.get("nombre_comercial", pd.Series("", index=proposals.index)).fillna("")
        proposals["ganador"] = winner.where(winner.astype(str).str.strip() != "", fallback)
        proposals["ganado"] = proposals.apply(
            lambda row: bool(
                normalize_provider(row.get("proveedor"))
                and normalize_provider(row.get("proveedor")) == normalize_provider(row.get("ganador"))
            ),
            axis=1,
        )
        proposals["monto_ganado"] = proposals["monto_ofertado"].where(proposals["ganado"], 0.0)
        return proposals

    def _pc_where(self, filters: PCFilters, *, alias: str = "") -> tuple[str, dict[str, Any]]:
        prefix = f"{alias}." if alias else ""
        clauses = ["1=1"]
        params: dict[str, Any] = {}
        if filters.start_date:
            clauses.append(f"{prefix}fecha_analitica >= :pc_start")
            params["pc_start"] = filters.start_date.isoformat()
        if filters.end_date:
            clauses.append(f"{prefix}fecha_analitica <= :pc_end")
            params["pc_end"] = filters.end_date.isoformat()
        for values, column, label in (
            (filters.states, "estado", "pc_state"),
            (filters.entities, "entidad", "pc_entity"),
            (filters.families, "familia", "pc_family"),
        ):
            if not values:
                continue
            keys = []
            for index, value in enumerate(values):
                key = f"{label}_{index}"
                keys.append(f":{key}")
                params[key] = value
            clauses.append(f'{prefix}"{column}" IN ({", ".join(keys)})')
        if filters.min_amount > 0:
            clauses.append(f"{prefix}monto_referencia >= :pc_min_amount")
            params["pc_min_amount"] = float(filters.min_amount)
        if filters.max_amount > 0:
            clauses.append(f"{prefix}monto_referencia <= :pc_max_amount")
            params["pc_max_amount"] = float(filters.max_amount)
        if filters.search_groups:
            search_clauses = []
            for index, group in enumerate(filters.search_groups):
                key = f"pc_search_{index}"
                params[key] = f"%{clean_text(group).lower()}%"
                search_clauses.append(
                    f"(lower(COALESCE({prefix}titulo,'')) LIKE :{key} OR lower(COALESCE({prefix}descripcion,'')) LIKE :{key} OR lower(COALESCE({prefix}entidad,'')) LIKE :{key})"
                )
            operator = " AND " if filters.search_mode.upper() == "AND" else " OR "
            clauses.append("(" + operator.join(search_clauses) + ")")
        return " AND ".join(clauses), params

    def _company_acts_pc_layer(self, company: str, filters: PCFilters) -> pd.DataFrame:
        target = normalize_provider(company)
        if not target:
            return pd.DataFrame()
        where, params = self._pc_where(filters, alias="a")
        params["company"] = f"%{target}%"
        params["company_target"] = target
        query = f"""
            SELECT a.*, p.proveedor AS empresa_consultada, p.monto_ofertado AS monto_participacion,
                   p.ganado, p.monto_ganado, p.ganador
            FROM pc_propuestas p
            JOIN pc_actos a ON a.acto_key=p.acto_key
            WHERE {where}
              AND (p.proveedor_norm LIKE :company OR :company_target LIKE ('%' || p.proveedor_norm || '%'))
        """
        with self.engine.connect() as connection:
            matched = pd.read_sql_query(text(query), connection, params=params)
        if matched.empty:
            return matched
        matched["fecha_analitica"] = pd.to_datetime(matched["fecha_analitica"], errors="coerce")
        matched["ganado"] = matched["ganado"].fillna(False).astype(bool)
        keys = matched["acto_key"].dropna().astype(str).unique().tolist()
        competitor_frames: list[pd.DataFrame] = []
        with self.engine.connect() as connection:
            for start in range(0, len(keys), 500):
                chunk = keys[start : start + 500]
                key_names = [f"pc_act_{start}_{index}" for index in range(len(chunk))]
                key_params = dict(zip(key_names, chunk))
                competitor_frames.append(
                    pd.read_sql_query(
                        text(f"SELECT acto_key,proveedor,proveedor_norm FROM pc_propuestas WHERE acto_key IN ({', '.join(':' + key for key in key_names)})"),
                        connection,
                        params=key_params,
                    )
                )
        all_proposals = pd.concat(competitor_frames, ignore_index=True) if competitor_frames else pd.DataFrame()
        competitor_map: dict[str, list[str]] = {}
        if not all_proposals.empty:
            for acto_key, group in all_proposals.groupby("acto_key"):
                competitor_map[str(acto_key)] = [
                    clean_text(row.proveedor)
                    for row in group.itertuples()
                    if clean_text(row.proveedor) and normalize_provider(row.proveedor) != target
                ]
        matched["competidores"] = matched["acto_key"].astype(str).map(competitor_map).map(lambda value: value if isinstance(value, list) else [])
        matched["cantidad_participantes_calculada"] = matched["acto_key"].astype(str).map(
            all_proposals.groupby("acto_key")["proveedor_norm"].nunique().to_dict() if not all_proposals.empty else {}
        ).fillna(0).astype(int)
        return matched.drop_duplicates("acto_key", keep="first").reset_index(drop=True)

    def proposals_for_acts(self, act_ids: Sequence[object]) -> pd.DataFrame:
        ids = [value for value in act_ids if value is not None]
        if not ids:
            return pd.DataFrame(columns=["id", "proveedor", "proveedor_norm", "monto_ofertado", "ordinal"])
        frames: list[pd.DataFrame] = []
        provider_columns: list[str] = []
        for index in range(1, 15):
            provider_columns.extend((f"Proponente {index}", f"Precio Proponente {index}"))
        columns = self._quoted_columns(("id", *provider_columns))
        with self.engine.connect() as connection:
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                keys = [f"id_{start}_{idx}" for idx in range(len(chunk))]
                params = dict(zip(keys, chunk))
                query = f'SELECT {columns} FROM actos_publicos WHERE "id" IN ({", ".join(":" + key for key in keys)})'
                frames.append(pd.read_sql_query(text(query), connection, params=params))
        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return unpivot_proposals(raw)

    def save_study(self, *, study_type: str, target: str, report: str, payload: Mapping[str, Any], username: str) -> str:
        study_id = uuid.uuid4().hex
        statement = """
            CREATE TABLE IF NOT EXISTS pc_estudios (
                study_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                username TEXT,
                study_type TEXT NOT NULL,
                target TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                report TEXT NOT NULL
            )
        """
        with self.engine.begin() as connection:
            connection.execute(text(statement))
            connection.execute(
                text("INSERT INTO pc_estudios(study_id,created_at,username,study_type,target,payload_json,report) VALUES (:study_id,:created_at,:username,:study_type,:target,:payload_json,:report)"),
                {
                    "study_id": study_id,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "username": clean_text(username),
                    "study_type": clean_text(study_type),
                    "target": clean_text(target),
                    "payload_json": json.dumps(dict(payload), ensure_ascii=False, default=str),
                    "report": report,
                },
            )
        return study_id

    def list_studies(self, *, limit: int = 50) -> pd.DataFrame:
        if "pc_estudios" not in set(inspect(self.engine).get_table_names()):
            return pd.DataFrame()
        with self.engine.connect() as connection:
            return pd.read_sql_query(text("SELECT study_id,created_at,username,study_type,target,report FROM pc_estudios ORDER BY created_at DESC LIMIT :limit"), connection, params={"limit": int(limit)})

    def add_watch(self, *, username: str, watch_type: str, target: str) -> None:
        statement = """
            CREATE TABLE IF NOT EXISTS pc_seguimiento (
                watch_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                username TEXT NOT NULL,
                watch_type TEXT NOT NULL,
                target TEXT NOT NULL
            )
        """
        normalized_target = clean_text(target)
        with self.engine.begin() as connection:
            connection.execute(text(statement))
            connection.execute(
                text("DELETE FROM pc_seguimiento WHERE username=:username AND watch_type=:watch_type AND lower(target)=lower(:target)"),
                {"username": clean_text(username), "watch_type": clean_text(watch_type), "target": normalized_target},
            )
            connection.execute(
                text("INSERT INTO pc_seguimiento(watch_id,created_at,username,watch_type,target) VALUES (:watch_id,:created_at,:username,:watch_type,:target)"),
                {
                    "watch_id": uuid.uuid4().hex,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "username": clean_text(username),
                    "watch_type": clean_text(watch_type),
                    "target": normalized_target,
                },
            )

    def remove_watch(self, *, username: str, watch_id: str) -> None:
        if "pc_seguimiento" not in set(inspect(self.engine).get_table_names()):
            return
        with self.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM pc_seguimiento WHERE watch_id=:watch_id AND username=:username"),
                {"watch_id": clean_text(watch_id), "username": clean_text(username)},
            )

    def list_watches(self, *, username: str) -> pd.DataFrame:
        if "pc_seguimiento" not in set(inspect(self.engine).get_table_names()):
            return pd.DataFrame()
        with self.engine.connect() as connection:
            return pd.read_sql_query(
                text("SELECT watch_id,created_at,watch_type,target FROM pc_seguimiento WHERE username=:username ORDER BY created_at DESC"),
                connection,
                params={"username": clean_text(username)},
            )


def prepare_pc_acts(frame: pd.DataFrame, filters: PCFilters) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    for column in CORE_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    result["acto_key"] = result.apply(
        lambda row: clean_text(row.get("enlace")) or clean_text(row.get("id")),
        axis=1,
    )
    result["fecha_analitica"] = result.apply(
        lambda row: next(
            (parsed for parsed in (parse_date(row.get("publicacion")), parse_date(row.get("fecha")), parse_date(row.get("fecha_adjudicacion")), parse_date(row.get("fecha_actualizacion"))) if not pd.isna(parsed)),
            pd.NaT,
        ),
        axis=1,
    )
    result["monto_referencia"] = result["precio_referencia"].map(parse_money)
    classifications = result.apply(
        lambda row: classify_pc_market(
            title=row.get("titulo"),
            description=row.get("descripcion"),
            items=row.get("items_json"),
            ficha_detectada=row.get("ficha_detectada"),
            fichas_json=row.get("fichas_detectadas_json"),
        ),
        axis=1,
    )
    result[["mercado_pc", "evidencia_mercado"]] = pd.DataFrame(classifications.tolist(), index=result.index)
    allowed = {"no_medico", "ambiguo"} if filters.include_ambiguous else {"no_medico"}
    result = result[result["mercado_pc"].isin(allowed)].copy()
    families = result.apply(lambda row: classify_project_family(row.get("titulo"), row.get("descripcion"), row.get("items_json")), axis=1)
    result[["familia", "confianza_familia", "evidencia_familia"]] = pd.DataFrame(families.tolist(), index=result.index)
    if filters.families:
        result = result[result["familia"].isin(filters.families)]
    if filters.min_amount > 0:
        result = result[result["monto_referencia"] >= float(filters.min_amount)]
    if filters.max_amount > 0:
        result = result[result["monto_referencia"] <= float(filters.max_amount)]
    if filters.search_groups:
        searchable = result[["titulo", "descripcion", "entidad", "unidad_solic"]].fillna("").astype(str).agg(" ".join, axis=1).map(normalize_text)
        masks = [searchable.str.contains(re.escape(normalize_text(group)), regex=True, na=False) for group in filters.search_groups if normalize_text(group)]
        if masks:
            combined = masks[0]
            for mask in masks[1:]:
                combined = combined & mask if filters.search_mode.upper() == "AND" else combined | mask
            result = result[combined]
    return result.reset_index(drop=True)


def unpivot_proposals(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame(columns=["acto_key", "id", "proveedor", "proveedor_norm", "monto_ofertado", "ordinal"])
    for record in frame.to_dict("records"):
        for ordinal in range(1, 15):
            provider = clean_text(record.get(f"Proponente {ordinal}"))
            if not provider:
                continue
            rows.append(
                {
                    "acto_key": clean_text(record.get("acto_key")) or clean_text(record.get("enlace")) or clean_text(record.get("id")),
                    "id": record.get("id"),
                    "proveedor": provider,
                    "proveedor_norm": normalize_provider(provider),
                    "monto_ofertado": parse_money(record.get(f"Precio Proponente {ordinal}")),
                    "ordinal": ordinal,
                }
            )
    return pd.DataFrame(rows)


def build_company_acts(frame: pd.DataFrame, company: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    target = normalize_provider(company)
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict("records"):
        matched: list[tuple[str, float]] = []
        participants: list[str] = []
        for ordinal in range(1, 15):
            provider = clean_text(record.get(f"Proponente {ordinal}"))
            if not provider:
                continue
            participants.append(provider)
            provider_norm = normalize_provider(provider)
            if target and (target in provider_norm or provider_norm in target):
                matched.append((provider, parse_money(record.get(f"Precio Proponente {ordinal}"))))
        winner = clean_text(record.get("razon_social") or record.get("nombre_comercial"))
        winner_norm = normalize_provider(winner)
        winner_match = bool(target and winner_norm and (target in winner_norm or winner_norm in target))
        if not matched and not winner_match:
            continue
        output = dict(record)
        output["empresa_consultada"] = matched[0][0] if matched else company
        output["monto_participacion"] = sum(amount for _, amount in matched)
        output["ganado"] = winner_match
        output["monto_ganado"] = output["monto_participacion"] if winner_match else 0.0
        output["ganador"] = winner
        output["participantes_lista"] = participants
        output["competidores"] = [name for name in participants if normalize_provider(name) != target]
        output["cantidad_participantes_calculada"] = len({normalize_provider(name) for name in participants if normalize_provider(name)})
        rows.append(output)
    return pd.DataFrame(rows)


def company_summary(acts: pd.DataFrame) -> dict[str, float]:
    if acts.empty:
        return {
            "participaciones": 0, "ganados": 0, "tasa_exito": 0.0,
            "monto_participado": 0.0, "monto_ganado": 0.0,
            "oferta_minima": 0.0, "oferta_promedio": 0.0,
            "oferta_mediana": 0.0, "oferta_maxima": 0.0,
        }
    offered = pd.to_numeric(acts.get("monto_participacion", 0), errors="coerce").fillna(0)
    won = acts.get("ganado", pd.Series(False, index=acts.index)).fillna(False).astype(bool)
    return {
        "participaciones": int(acts["acto_key"].nunique() if "acto_key" in acts else len(acts)),
        "ganados": int(won.sum()),
        "tasa_exito": float(won.mean() * 100.0),
        "monto_participado": float(offered.sum()),
        "monto_ganado": float(pd.to_numeric(acts.get("monto_ganado", 0), errors="coerce").fillna(0).sum()),
        "oferta_minima": float(offered[offered > 0].min()) if (offered > 0).any() else 0.0,
        "oferta_promedio": float(offered[offered > 0].mean()) if (offered > 0).any() else 0.0,
        "oferta_mediana": float(offered[offered > 0].median()) if (offered > 0).any() else 0.0,
        "oferta_maxima": float(offered.max()),
    }


def competitor_summary(acts: pd.DataFrame) -> pd.DataFrame:
    counts: dict[str, dict[str, Any]] = {}
    for record in acts.to_dict("records") if not acts.empty else []:
        winner_norm = normalize_provider(record.get("ganador"))
        for competitor in record.get("competidores") or []:
            key = normalize_provider(competitor)
            if not key:
                continue
            current = counts.setdefault(key, {"competidor": competitor, "coincidencias": 0, "victorias_competidor": 0})
            current["coincidencias"] += 1
            current["victorias_competidor"] += int(bool(winner_norm and winner_norm == key))
    frame = pd.DataFrame(counts.values())
    if frame.empty:
        return frame
    frame["tasa_victoria_competidor"] = frame["victorias_competidor"] / frame["coincidencias"] * 100.0
    return frame.sort_values(["coincidencias", "victorias_competidor"], ascending=False).reset_index(drop=True)


def provider_ranking(proposals: pd.DataFrame) -> pd.DataFrame:
    if proposals.empty:
        return pd.DataFrame()
    work = proposals.copy()
    work["monto_ofertado"] = pd.to_numeric(work["monto_ofertado"], errors="coerce").fillna(0)
    work["monto_ganado"] = pd.to_numeric(work.get("monto_ganado", 0), errors="coerce").fillna(0)
    work["ganado"] = work.get("ganado", False).fillna(False).astype(bool)
    ranking = work.groupby("proveedor_norm", dropna=False).agg(
        proveedor=("proveedor", lambda values: values.value_counts().index[0]),
        participaciones=("acto_key", "nunique"),
        adjudicaciones=("ganado", "sum"),
        monto_ofertado=("monto_ofertado", "sum"),
        monto_ganado=("monto_ganado", "sum"),
        oferta_minima=("monto_ofertado", lambda values: values[values > 0].min() if (values > 0).any() else 0.0),
        oferta_promedio=("monto_ofertado", lambda values: values[values > 0].mean() if (values > 0).any() else 0.0),
        oferta_mediana=("monto_ofertado", lambda values: values[values > 0].median() if (values > 0).any() else 0.0),
        oferta_maxima=("monto_ofertado", "max"),
        familias=("familia", "nunique"),
        entidades=("entidad", "nunique"),
    ).reset_index(drop=True)
    ranking["tasa_exito"] = ranking["adjudicaciones"] / ranking["participaciones"].clip(lower=1) * 100.0
    return ranking.sort_values(["monto_ganado", "participaciones"], ascending=False).reset_index(drop=True)


def family_summary(acts: pd.DataFrame) -> pd.DataFrame:
    if acts.empty:
        return pd.DataFrame()
    work = acts.copy()
    work["fecha_analitica"] = pd.to_datetime(work["fecha_analitica"], errors="coerce")
    work["mes"] = work["fecha_analitica"].dt.to_period("M").astype(str)
    grouped = work.groupby("familia", dropna=False).agg(
        actos=("acto_key", "nunique"),
        monto_total=("monto_referencia", "sum"),
        ticket_promedio=("monto_referencia", "mean"),
        ticket_mediano=("monto_referencia", "median"),
        participantes_promedio=("num_participantes", lambda values: pd.to_numeric(values, errors="coerce").mean()),
        entidades=("entidad", "nunique"),
        meses_activos=("mes", "nunique"),
    ).reset_index()
    grouped["participantes_promedio"] = grouped["participantes_promedio"].fillna(0)
    return score_family_opportunities(grouped)


def _minmax(series: pd.Series, *, inverse: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0).astype(float)
    lo, hi = float(numeric.min()), float(numeric.max())
    result = pd.Series(50.0, index=numeric.index) if hi <= lo else (numeric - lo) / (hi - lo) * 100.0
    return 100.0 - result if inverse else result


def score_family_opportunities(frame: pd.DataFrame, weights: Mapping[str, float] | None = None) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    default = {"actos": 25.0, "monto": 25.0, "competencia": 20.0, "recurrencia": 15.0, "diversificacion": 15.0}
    selected = {**default, **dict(weights or {})}
    total = sum(max(0.0, float(value)) for value in selected.values()) or 1.0
    work = frame.copy()
    components = {
        "actos": _minmax(work["actos"]),
        "monto": _minmax(work["monto_total"]),
        "competencia": _minmax(work["participantes_promedio"], inverse=True),
        "recurrencia": _minmax(work["meses_activos"]),
        "diversificacion": _minmax(work["entidades"]),
    }
    work["score_oportunidad"] = sum(components[key] * max(0.0, float(selected.get(key, 0.0))) for key in components) / total
    return work.sort_values(["score_oportunidad", "monto_total"], ascending=False).reset_index(drop=True)


def monthly_trend(acts: pd.DataFrame) -> pd.DataFrame:
    if acts.empty:
        return pd.DataFrame()
    work = acts.copy()
    work["fecha_analitica"] = pd.to_datetime(work["fecha_analitica"], errors="coerce")
    work = work.dropna(subset=["fecha_analitica"])
    work["periodo"] = work["fecha_analitica"].dt.to_period("M").dt.to_timestamp()
    return work.groupby("periodo").agg(actos=("acto_key", "nunique"), monto=("monto_referencia", "sum"), entidades=("entidad", "nunique")).reset_index().sort_values("periodo")


def company_yearly_trend(acts: pd.DataFrame) -> pd.DataFrame:
    if acts.empty:
        return pd.DataFrame()
    work = acts.copy()
    work["fecha_analitica"] = pd.to_datetime(work["fecha_analitica"], errors="coerce")
    work = work.dropna(subset=["fecha_analitica"])
    work["ano"] = work["fecha_analitica"].dt.year
    return work.groupby("ano").agg(
        participaciones=("acto_key", "nunique"),
        ganados=("ganado", "sum"),
        monto_participado=("monto_participacion", "sum"),
        monto_ganado=("monto_ganado", "sum"),
    ).reset_index().sort_values("ano")


def build_deep_report(*, target: str, acts: pd.DataFrame, competitors: pd.DataFrame, filters: PCFilters) -> str:
    summary = company_summary(acts)
    families = acts.groupby("familia").agg(actos=("acto_key", "nunique"), monto=("monto_referencia", "sum")).sort_values("monto", ascending=False).head(8) if not acts.empty else pd.DataFrame()
    entities = acts.groupby("entidad").agg(actos=("acto_key", "nunique"), monto=("monto_referencia", "sum")).sort_values("monto", ascending=False).head(8) if not acts.empty else pd.DataFrame()
    lines = [
        f"# Estudio profundo de {target}",
        "",
        f"Periodo: {filters.start_date or 'inicio'} a {filters.end_date or 'actualidad'}.",
        f"Participaciones: {summary['participaciones']:,}.",
        f"Adjudicaciones observadas: {summary['ganados']:,} ({summary['tasa_exito']:.1f}%).",
        f"Monto ofertado observado: ${summary['monto_participado']:,.2f}.",
        f"Monto ganado observado: ${summary['monto_ganado']:,.2f}.",
        f"Oferta mediana: ${summary['oferta_mediana']:,.2f}; rango ${summary['oferta_minima']:,.2f} - ${summary['oferta_maxima']:,.2f}.",
        "",
        "## Familias principales",
    ]
    for family, row in families.iterrows():
        lines.append(f"- {family}: {int(row['actos'])} actos; ${float(row['monto']):,.2f} de referencia.")
    lines.extend(["", "## Entidades principales"])
    for entity, row in entities.iterrows():
        lines.append(f"- {entity}: {int(row['actos'])} actos; ${float(row['monto']):,.2f} de referencia.")
    lines.extend(["", "## Competidores mas frecuentes"])
    for row in competitors.head(10).to_dict("records") if not competitors.empty else []:
        lines.append(f"- {row['competidor']}: {int(row['coincidencias'])} coincidencias; {int(row['victorias_competidor'])} victorias observadas.")
    lines.extend([
        "",
        "## Lectura estrategica",
        "- Priorizar las familias con demanda recurrente donde la empresa ya presenta evidencia de adjudicacion.",
        "- Revisar individualmente los actos con alta referencia y pocos participantes antes de definir una banda de precio.",
        "- La diferencia entre referencia y oferta es una referencia competitiva, no una estimacion de margen sin costos internos.",
        "- Validar documentos, alcance y renglones del acto antes de presentar una oferta.",
    ])
    return "\n".join(lines)
