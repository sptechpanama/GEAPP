from __future__ import annotations

import os
import importlib
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from core.config import APP_ROOT
from services.access_control import build_authenticator, current_username, require_page_access
import services.inteligencia_pc as inteligencia_pc_service
from ui.theme import apply_global_theme


# Streamlit puede conservar en memoria un modulo auxiliar durante el cambio de
# commit. Si la pagina nueva llega antes que el servicio actualizado, un
# ``from ... import`` directo deja toda la pagina inutilizable. Comprobamos el
# contrato y solo recargamos cuando detectamos ese estado transitorio.
_REPOSITORY_CONTRACT = (
    "filter_options",
    "family_market_summary",
    "monthly_market_trend",
    "project_page",
    "provider_market_ranking",
    "entity_market_ranking",
    "provider_entity_ranking",
    "family_provider_ranking",
    "low_competition_projects",
    "proposals_for_act_keys",
    "company_options",
    "company_acts",
    "save_study",
    "list_studies",
    "add_watch",
    "list_watches",
    "remove_watch",
)
_SERVICE_EXPORTS = (
    "INTELIGENCIA_PC_SERVICE_VERSION",
    "FAMILY_RULES",
    "PCAnalyticsUnavailable",
    "PCFilters",
    "InteligenciaPCRepository",
    "build_deep_report",
    "clean_text",
    "company_summary",
    "company_yearly_trend",
    "comparable_providers",
    "competitor_summary",
    "family_market_concentration",
    "near_miss_opportunities",
    "provider_growth_ranking",
    "score_entity_opportunities",
    "score_family_opportunities",
    "score_provider_opportunities",
)


def _service_contract_is_incomplete(module: object) -> bool:
    if any(not hasattr(module, name) for name in _SERVICE_EXPORTS):
        return True
    repository_class = getattr(module, "InteligenciaPCRepository", None)
    return repository_class is None or any(
        not callable(getattr(repository_class, method_name, None))
        for method_name in _REPOSITORY_CONTRACT
    )


if _service_contract_is_incomplete(inteligencia_pc_service):
    inteligencia_pc_service = importlib.reload(inteligencia_pc_service)

INTELIGENCIA_PC_SERVICE_VERSION = inteligencia_pc_service.INTELIGENCIA_PC_SERVICE_VERSION
FAMILY_RULES = inteligencia_pc_service.FAMILY_RULES
PCAnalyticsUnavailable = inteligencia_pc_service.PCAnalyticsUnavailable
PCFilters = inteligencia_pc_service.PCFilters
InteligenciaPCRepository = inteligencia_pc_service.InteligenciaPCRepository
build_deep_report = inteligencia_pc_service.build_deep_report
clean_text = inteligencia_pc_service.clean_text
company_summary = inteligencia_pc_service.company_summary
company_yearly_trend = inteligencia_pc_service.company_yearly_trend
comparable_providers = inteligencia_pc_service.comparable_providers
competitor_summary = inteligencia_pc_service.competitor_summary
family_market_concentration = inteligencia_pc_service.family_market_concentration
near_miss_opportunities = inteligencia_pc_service.near_miss_opportunities
provider_growth_ranking = inteligencia_pc_service.provider_growth_ranking
score_entity_opportunities = inteligencia_pc_service.score_entity_opportunities
score_family_opportunities = inteligencia_pc_service.score_family_opportunities
score_provider_opportunities = inteligencia_pc_service.score_provider_opportunities


PAGE_PATH = "pages/inteligencia_pc.py"
LOCAL_DB_CANDIDATES = (
    APP_ROOT / "data" / "db" / "panamacompra.db",
    APP_ROOT / "data" / "panamacompra.db",
    APP_ROOT / "panamacompra.db",
    Path.home() / "scrapers_repo" / "data" / "db" / "panamacompra.db",
)


st.set_page_config(page_title="Inteligencia PC", page_icon="📊", layout="wide")
apply_global_theme()

authenticator = build_authenticator()
try:
    authenticator.login(" ", location="sidebar", key="auth_inteligencia_pc_silent")
    st.sidebar.empty()
except Exception:
    pass
require_page_access(PAGE_PATH)
authenticator.logout("Cerrar sesión", location="sidebar")


def _app_secrets() -> dict[str, object]:
    try:
        raw = st.secrets.get("app", {})
        return dict(raw) if isinstance(raw, Mapping) else {}
    except Exception:
        return {}


def _config_value(key: str, default: str = "") -> str:
    environment = clean_text(os.getenv(key, ""))
    if environment:
        return environment
    app_value = clean_text(_app_secrets().get(key, ""))
    if app_value:
        return app_value
    try:
        return clean_text(st.secrets.get(key, default))
    except Exception:
        return default


@st.cache_resource(show_spinner=False)
def _repository(database_url: str, service_version: str) -> InteligenciaPCRepository:
    # ``service_version`` se usa deliberadamente como parte de la clave de
    # cache. Evita reutilizar una instancia creada por un despliegue anterior.
    del service_version
    return InteligenciaPCRepository.connect(database_url=database_url, local_candidates=LOCAL_DB_CANDIDATES)


def _missing_repository_methods(repository: object) -> tuple[str, ...]:
    return tuple(
        method_name
        for method_name in _REPOSITORY_CONTRACT
        if not callable(getattr(repository, method_name, None))
    )


def _open_repository(database_url: str) -> InteligenciaPCRepository:
    repository = _repository(database_url, INTELIGENCIA_PC_SERVICE_VERSION)
    missing = _missing_repository_methods(repository)
    if missing:
        # Una sesion viva de Streamlit puede conservar recursos de un commit
        # anterior. Limpiamos ambas capas una sola vez y reconstruimos con la
        # clase actual antes de renderizar cualquier subseccion.
        _repository.clear()
        st.cache_data.clear()
        repository = _repository(database_url, INTELIGENCIA_PC_SERVICE_VERSION)
        missing = _missing_repository_methods(repository)
    if missing:
        missing_text = ", ".join(missing)
        raise PCAnalyticsUnavailable(
            "El servicio de Inteligencia PC no termino de actualizarse. "
            f"Operaciones ausentes: {missing_text}."
        )
    return repository


@st.cache_data(show_spinner=False, ttl=600)
def _options(_repo: InteligenciaPCRepository) -> dict[str, list[str]]:
    return _repo.filter_options()


@st.cache_data(show_spinner=False, ttl=600)
def _family_market_summary(filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    return _repo.family_market_summary(filters)


@st.cache_data(show_spinner=False, ttl=600)
def _monthly_market_trend(filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    return _repo.monthly_market_trend(filters)


@st.cache_data(show_spinner=False, ttl=600)
def _project_page(
    filters: PCFilters,
    sort_column: str,
    ascending: bool,
    page_size: int,
    page: int,
    _repo: InteligenciaPCRepository,
) -> tuple[pd.DataFrame, int]:
    return _repo.project_page(
        filters,
        sort_column=sort_column,
        ascending=ascending,
        limit=page_size,
        offset=max(0, page - 1) * page_size,
    )


@st.cache_data(show_spinner=False, ttl=600)
def _provider_market_ranking(filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    return _repo.provider_market_ranking(filters, limit=300)


@st.cache_data(show_spinner=False, ttl=600)
def _provider_top_ranking(filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    # El scoring personalizado debe evaluar el universo completo de proveedores,
    # no una muestra previa ordenada por monto ganado.
    return _repo.provider_market_ranking(filters, limit=20_000, detailed=True)


@st.cache_data(show_spinner=False, ttl=600)
def _entity_market_ranking(filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    return _repo.entity_market_ranking(filters, limit=1000)


@st.cache_data(show_spinner=False, ttl=600)
def _provider_entity_ranking(
    filters: PCFilters,
    provider: str,
    _repo: InteligenciaPCRepository,
) -> pd.DataFrame:
    # La consulta se restringe en SQL al proveedor elegido para recuperar todas
    # sus entidades sin descargar las decenas de miles de relaciones globales.
    return _repo.provider_entity_ranking(filters, provider=provider, limit=3000)


@st.cache_data(show_spinner=False, ttl=600)
def _family_provider_ranking(filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    # La concentracion de mercado exige todas las relaciones categoria-proveedor.
    return _repo.family_provider_ranking(filters, limit=50_000)


@st.cache_data(show_spinner=False, ttl=600)
def _low_competition_projects(
    filters: PCFilters,
    maximum_participants: int,
    minimum_amount: float,
    limit: int,
    _repo: InteligenciaPCRepository,
) -> pd.DataFrame:
    return _repo.low_competition_projects(
        filters,
        maximum_participants=maximum_participants,
        minimum_amount=minimum_amount,
        limit=limit,
    )


@st.cache_data(show_spinner=False, ttl=600)
def _proposals_for_act_keys(act_keys: tuple[str, ...], _repo: InteligenciaPCRepository) -> pd.DataFrame:
    return _repo.proposals_for_act_keys(act_keys)


@st.cache_data(show_spinner=False, ttl=600)
def _company_options(search: str, _repo: InteligenciaPCRepository) -> list[str]:
    return _repo.company_options(search)


@st.cache_data(show_spinner=False, ttl=600)
def _company_acts(company: str, filters: PCFilters, _repo: InteligenciaPCRepository) -> pd.DataFrame:
    return _repo.company_acts(company, filters)


def _money(value: object) -> str:
    try:
        return f"${float(value or 0):,.2f}"
    except Exception:
        return "$0.00"


def _number(value: object) -> str:
    try:
        return f"{int(float(value or 0)):,}"
    except Exception:
        return "0"


def _number_config(frame: pd.DataFrame, overrides: Mapping[str, object] | None = None) -> dict[str, object]:
    config: dict[str, object] = {}
    money_tokens = ("monto", "oferta", "precio", "ticket", "referencia", "ganado")
    for column in frame.columns:
        if not pd.api.types.is_numeric_dtype(frame[column]):
            continue
        normalized = str(column).lower().replace(" ", "_")
        if normalized in {"id", "ano", "ordinal"} or normalized.endswith("_id"):
            continue
        is_percent = "tasa" in normalized or "score" in normalized or "confianza" in normalized
        is_money = not is_percent and any(token in normalized for token in money_tokens)
        config[column] = st.column_config.NumberColumn(
            str(column),
            format="dollar" if is_money else ("%.1f%%" if is_percent else "localized"),
        )
    if overrides:
        config.update(dict(overrides))
    return config


def _company_selector(repo: InteligenciaPCRepository, *, key_prefix: str) -> str:
    search = st.text_input(
        "Buscar empresa por nombre",
        key=f"{key_prefix}_company_search",
        placeholder="Ej. RS Engineering",
        help="Escribe al menos dos caracteres; se mostrarán las coincidencias registradas.",
    )
    if len(search.strip()) < 2:
        return ""
    options = _company_options(search, repo)
    if not options:
        st.info("No se encontraron empresas con ese texto en el periodo histórico.")
        return ""
    return st.selectbox("Empresa", options, key=f"{key_prefix}_company_option")


def _render_company_kpis(frame: pd.DataFrame) -> dict[str, float]:
    summary = company_summary(frame)
    columns = st.columns(5)
    columns[0].metric("Participaciones", _number(summary["participaciones"]))
    columns[1].metric("Adjudicaciones", _number(summary["ganados"]))
    columns[2].metric("Tasa de éxito", f"{summary['tasa_exito']:.1f}%")
    columns[3].metric("Monto ofertado", _money(summary["monto_participado"]))
    columns[4].metric("Monto ganado", _money(summary["monto_ganado"]))
    second = st.columns(4)
    second[0].metric("Oferta mínima", _money(summary["oferta_minima"]))
    second[1].metric("Oferta promedio", _money(summary["oferta_promedio"]))
    second[2].metric("Oferta mediana", _money(summary["oferta_mediana"]))
    second[3].metric("Oferta máxima", _money(summary["oferta_maxima"]))
    return summary


def _company_table(frame: pd.DataFrame) -> None:
    if frame.empty:
        st.info("No hay participaciones no médicas para esta empresa y filtros.")
        return
    display_columns = [
        "fecha_analitica", "titulo", "familia", "entidad", "estado", "monto_referencia",
        "monto_participacion", "cantidad_participantes_calculada", "ganado", "ganador", "enlace",
    ]
    display = frame[[column for column in display_columns if column in frame.columns]].copy()
    display = display.sort_values("fecha_analitica", ascending=False)
    config = _number_config(
        display,
        {
            "enlace": st.column_config.LinkColumn("Acto", display_text="Abrir"),
            "ganado": st.column_config.CheckboxColumn("Ganado"),
            "fecha_analitica": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
        },
    )
    st.dataframe(display, use_container_width=True, hide_index=True, height=520, column_config=config)


def _family_score_weights(*, key_prefix: str, defaults: Mapping[str, float] | None = None) -> dict[str, float]:
    values = dict(defaults or {"actos": 25.0, "monto": 25.0, "competencia": 20.0, "recurrencia": 15.0, "diversificacion": 15.0})
    columns = st.columns(5)
    labels = {
        "actos": "Número de actos",
        "monto": "Monto del mercado",
        "competencia": "Menor competencia",
        "recurrencia": "Recurrencia",
        "diversificacion": "Diversificación",
    }
    return {
        metric: float(columns[index].number_input(labels[metric], min_value=0.0, value=float(values[metric]), step=1.0, key=f"{key_prefix}_{metric}"))
        for index, metric in enumerate(labels)
    }


def _render_top_categories(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.subheader("Top de categorías")
    families = _family_market_summary(filters, repo)
    if families.empty:
        st.info("No hay categorías para los filtros seleccionados.")
        return
    order = st.selectbox(
        "Ordenar categorías por",
        ["Score configurable", "Monto total", "Número de actos", "Menor competencia", "Recurrencia"],
        key="pc_top_category_order",
    )
    if order == "Score configurable":
        with st.expander("Pesos del ranking", expanded=False):
            weights = _family_score_weights(key_prefix="pc_top_category_weight")
        ranked = score_family_opportunities(families.drop(columns=["score_oportunidad"], errors="ignore"), weights)
    else:
        sort_map = {
            "Monto total": ("monto_total", False),
            "Número de actos": ("actos", False),
            "Menor competencia": ("participantes_promedio", True),
            "Recurrencia": ("meses_activos", False),
        }
        column, ascending = sort_map[order]
        ranked = families.sort_values([column, "monto_total"], ascending=[ascending, False]).reset_index(drop=True)
    display = ranked.rename(columns={
        "familia": "Categoría", "score_oportunidad": "Score", "actos": "Actos",
        "monto_total": "Monto total", "ticket_promedio": "Ticket promedio",
        "participantes_promedio": "Participantes prom.", "entidades": "Entidades",
        "meses_activos": "Meses activos",
    })
    st.dataframe(display, use_container_width=True, hide_index=True, height=620, column_config=_number_config(display))
    metric = {
        "Score configurable": "Score",
        "Monto total": "Monto total",
        "Número de actos": "Actos",
        "Menor competencia": "Participantes prom.",
        "Recurrencia": "Meses activos",
    }[order]
    chart = alt.Chart(display.head(15)).mark_bar().encode(
        x=alt.X(f"{metric}:Q", title=metric),
        y=alt.Y("Categoría:N", sort="-x", title=""),
        tooltip=["Categoría", alt.Tooltip(f"{metric}:Q", format=",.1f"), "Actos", alt.Tooltip("Monto total:Q", format=",.2f")],
    ).properties(height=480)
    st.altair_chart(chart, use_container_width=True)


def _provider_weights(preset: str) -> dict[str, float]:
    presets = {
        "Dominio comercial": {"adjudicaciones": 30.0, "monto_ganado": 35.0, "tasa_exito": 15.0, "participaciones": 10.0, "diversificacion": 10.0},
        "Eficiencia": {"adjudicaciones": 20.0, "monto_ganado": 15.0, "tasa_exito": 50.0, "participaciones": 5.0, "diversificacion": 10.0},
        "Actividad": {"adjudicaciones": 15.0, "monto_ganado": 10.0, "tasa_exito": 5.0, "participaciones": 60.0, "diversificacion": 10.0},
        "Personalizado": {"adjudicaciones": 25.0, "monto_ganado": 25.0, "tasa_exito": 20.0, "participaciones": 15.0, "diversificacion": 15.0},
    }
    defaults = presets[preset]
    columns = st.columns(5)
    labels = {
        "adjudicaciones": "Actos ganados",
        "monto_ganado": "Monto ganado",
        "tasa_exito": "Tasa de éxito",
        "participaciones": "Participaciones",
        "diversificacion": "Diversificación",
    }
    return {
        metric: float(columns[index].number_input(
            labels[metric], min_value=0.0, value=float(defaults[metric]), step=1.0,
            key=f"pc_provider_weight_{preset}_{metric}",
        ))
        for index, metric in enumerate(labels)
    }


def _render_top_providers(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.subheader("Top de proveedores")
    controls = st.columns([2, 1, 1])
    mode = controls[0].selectbox(
        "Ranking",
        ["Score configurable", "Más actos ganados", "Mayor monto ganado", "Mayor tasa de éxito", "Más participaciones", "Emergentes"],
        key="pc_provider_top_mode",
    )
    minimum = int(controls[1].number_input("Participaciones mínimas", min_value=1, value=3, step=1, key="pc_provider_top_minimum"))
    top_n = int(controls[2].selectbox("Mostrar", [15, 25, 50, 100, 250], index=2, key="pc_provider_top_n"))
    ranking = _provider_top_ranking(filters, repo)
    if ranking.empty:
        st.info("No hay propuestas para el periodo seleccionado.")
        return
    ranking = ranking[pd.to_numeric(ranking["participaciones"], errors="coerce").fillna(0) >= minimum].copy()
    if ranking.empty:
        st.info("Ningún proveedor alcanza el mínimo de participaciones indicado.")
        return
    if mode == "Score configurable":
        preset = st.selectbox("Enfoque", ["Dominio comercial", "Eficiencia", "Actividad", "Personalizado"], key="pc_provider_preset")
        with st.expander("Pesos estrictos del ranking", expanded=preset == "Personalizado"):
            st.caption("Si una métrica recibe 100% del peso, el orden depende únicamente de esa métrica.")
            weights = _provider_weights(preset)
        ranking = score_provider_opportunities(ranking, weights)
    elif mode == "Emergentes":
        if not filters.start_date or not filters.end_date:
            st.warning("El ranking emergente necesita fechas de inicio y fin.")
            return
        period_days = max(1, (filters.end_date - filters.start_date).days + 1)
        previous_end = filters.start_date - timedelta(days=1)
        previous_filters = replace(filters, start_date=previous_end - timedelta(days=period_days - 1), end_date=previous_end)
        previous = _provider_top_ranking(previous_filters, repo)
        ranking = provider_growth_ranking(ranking, previous)
    else:
        sort_map = {
            "Más actos ganados": ["adjudicaciones", "monto_ganado"],
            "Mayor monto ganado": ["monto_ganado", "adjudicaciones"],
            "Mayor tasa de éxito": ["tasa_exito", "adjudicaciones"],
            "Más participaciones": ["participaciones", "adjudicaciones"],
        }
        ranking = ranking.sort_values(sort_map[mode], ascending=False).reset_index(drop=True)
    display = ranking.head(top_n).rename(columns={
        "proveedor": "Proveedor", "participaciones": "Participaciones", "adjudicaciones": "Actos ganados",
        "tasa_exito": "Tasa de éxito", "monto_ofertado": "Monto ofertado", "monto_ganado": "Monto ganado",
        "oferta_minima": "Oferta mínima", "oferta_promedio": "Oferta promedio", "oferta_maxima": "Oferta máxima",
        "familias": "Categorías", "entidades": "Entidades", "score_proveedor": "Score",
        "confianza_muestra": "Confianza muestra", "nivel_confianza": "Nivel confianza",
        "crecimiento_score": "Score crecimiento", "cambio_adjudicaciones": "Cambio ganados",
        "cambio_monto_ganado": "Cambio monto ganado", "cambio_participaciones": "Cambio participaciones",
    }).drop(columns=["proveedor_norm"], errors="ignore")
    st.dataframe(display, use_container_width=True, hide_index=True, height=720, column_config=_number_config(display))

    st.markdown("#### Relaciones proveedor–entidad")
    selected_provider = st.selectbox("Proveedor para ver entidades", display["Proveedor"].astype(str).tolist(), key="pc_provider_entity_selected")
    if selected_provider:
        relations = _provider_entity_ranking(filters, selected_provider, repo)
        relation_display = relations.rename(columns={
            "proveedor": "Proveedor", "entidad": "Entidad", "participaciones": "Participaciones",
            "adjudicaciones": "Actos ganados", "tasa_exito": "Tasa de éxito", "monto_ganado": "Monto ganado",
        }).drop(columns=["proveedor_norm"], errors="ignore")
        st.dataframe(relation_display, use_container_width=True, hide_index=True, height=360, column_config=_number_config(relation_display))


def _render_top_entities(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.subheader("Top de entidades compradoras")
    entities = _entity_market_ranking(filters, repo)
    if entities.empty:
        st.info("No hay entidades para los filtros seleccionados.")
        return
    with st.expander("Pesos del ranking", expanded=False):
        columns = st.columns(5)
        weights = {
            "actos": columns[0].number_input("Número de actos", min_value=0.0, value=25.0, key="pc_entity_weight_acts"),
            "monto": columns[1].number_input("Monto comprado", min_value=0.0, value=30.0, key="pc_entity_weight_amount"),
            "recurrencia": columns[2].number_input("Recurrencia", min_value=0.0, value=20.0, key="pc_entity_weight_recurrence"),
            "competencia": columns[3].number_input("Menor competencia", min_value=0.0, value=15.0, key="pc_entity_weight_competition"),
            "diversificacion": columns[4].number_input("Diversificación", min_value=0.0, value=10.0, key="pc_entity_weight_diversity"),
        }
    ranked = score_entity_opportunities(entities, weights)
    display = ranked.rename(columns={
        "entidad": "Entidad", "score_entidad": "Score", "actos": "Actos", "monto_total": "Monto total",
        "ticket_promedio": "Ticket promedio", "participantes_promedio": "Participantes prom.",
        "familias": "Categorías", "meses_activos": "Meses activos",
    })
    st.dataframe(display, use_container_width=True, hide_index=True, height=680, column_config=_number_config(display))


def _render_attackable_markets(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.subheader("Mercados atacables")
    st.caption("Combina volumen, monto, recurrencia y competencia; además permite excluir familias dominadas por un solo proveedor.")
    controls = st.columns(4)
    minimum_acts = int(controls[0].number_input("Actos mínimos", min_value=1, value=3, step=1, key="pc_attack_min_acts"))
    minimum_amount = float(controls[1].number_input("Monto mínimo", min_value=0.0, value=0.0, step=1000.0, key="pc_attack_min_amount"))
    maximum_participants = float(controls[2].number_input("Participantes prom. máximos", min_value=0.0, value=5.0, step=0.5, key="pc_attack_max_participants"))
    maximum_concentration = float(controls[3].number_input("Dominio máximo del líder (%)", min_value=0.0, max_value=100.0, value=70.0, step=5.0, key="pc_attack_max_concentration"))
    with st.expander("Pesos del ranking", expanded=False):
        weights = _family_score_weights(
            key_prefix="pc_attack_weight",
            defaults={"actos": 25.0, "monto": 30.0, "competencia": 25.0, "recurrencia": 10.0, "diversificacion": 10.0},
        )
    families = _family_market_summary(filters, repo)
    if families.empty:
        st.info("No hay categorías para los filtros seleccionados.")
        return
    concentration = family_market_concentration(_family_provider_ranking(filters, repo))
    ranked = score_family_opportunities(families.drop(columns=["score_oportunidad"], errors="ignore"), weights)
    if concentration.empty:
        ranked["proveedor_dominante"] = ""
        ranked["adjudicaciones_dominante"] = 0
        ranked["concentracion_top"] = 0.0
        ranked["proveedores_activos"] = 0
    else:
        ranked = ranked.merge(concentration, on="familia", how="left")
    ranked["concentracion_top"] = pd.to_numeric(ranked.get("concentracion_top", 0), errors="coerce").fillna(0)
    ranked = ranked[
        (ranked["actos"] >= minimum_acts)
        & (ranked["monto_total"] >= minimum_amount)
        & (ranked["participantes_promedio"] <= maximum_participants)
        & (ranked["concentracion_top"] <= maximum_concentration)
    ]
    display = ranked.rename(columns={
        "familia": "Categoría", "score_oportunidad": "Score", "actos": "Actos", "monto_total": "Monto total",
        "ticket_promedio": "Ticket promedio", "participantes_promedio": "Participantes prom.",
        "entidades": "Entidades", "meses_activos": "Meses activos", "proveedor_dominante": "Proveedor dominante",
        "concentracion_top": "Concentración del líder", "proveedores_activos": "Proveedores activos",
        "adjudicaciones_dominante": "Ganados por líder",
    })
    if display.empty:
        st.info("Ninguna categoría cumple simultáneamente los límites seleccionados.")
        return
    st.dataframe(display, use_container_width=True, hide_index=True, height=620, column_config=_number_config(display))


def _render_low_competition(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.subheader("Oportunidades con baja competencia")
    controls = st.columns(3)
    maximum = int(controls[0].number_input("Máximo de participantes", min_value=0, value=2, step=1, key="pc_low_max_participants"))
    minimum = float(controls[1].number_input("Monto mínimo", min_value=0.0, value=5000.0, step=1000.0, key="pc_low_min_amount"))
    limit = int(controls[2].selectbox("Mostrar", [25, 50, 100, 250, 500], index=2, key="pc_low_limit"))
    projects = _low_competition_projects(filters, maximum, minimum, limit, repo)
    if projects.empty:
        st.info("No hay proyectos que cumplan esos límites.")
        return
    display = projects.drop(columns=["acto_key"], errors="ignore").rename(columns={
        "fecha_analitica": "Fecha", "titulo": "Proyecto", "familia": "Categoría", "entidad": "Entidad",
        "estado": "Estado", "monto_referencia": "Monto de referencia", "num_participantes": "Participantes", "enlace": "Acto",
    })
    st.dataframe(
        display, use_container_width=True, hide_index=True, height=760,
        column_config=_number_config(display, {"Acto": st.column_config.LinkColumn("Acto", display_text="Abrir"), "Fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY")}),
    )


def _render_rs_intelligence(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.subheader("Inteligencia de RS Engineering")
    search = st.text_input("Empresa objetivo", value="RS Engineering", key="pc_rs_target")
    options = _company_options(search, repo) if len(search.strip()) >= 2 else []
    company = st.selectbox("Coincidencia registrada", options, key="pc_rs_option") if options else search
    if not clean_text(company):
        st.info("Indica una empresa.")
        return
    company_frame = _company_acts(company, filters, repo)
    _render_company_kpis(company_frame)
    if company_frame.empty:
        st.info("No hay participaciones para la empresa y filtros seleccionados.")
        return
    view = st.radio("Análisis", ["Resumen", "Casi ganados", "Competidores", "Comparables", "Entidades"], horizontal=True, key="pc_rs_view")
    if view == "Resumen":
        families = company_frame.groupby("familia", dropna=False).agg(
            participaciones=("acto_key", "nunique"), ganados=("ganado", "sum"),
            monto_participado=("monto_participacion", "sum"), monto_ganado=("monto_ganado", "sum"),
        ).reset_index().sort_values(["monto_ganado", "participaciones"], ascending=False)
        families["tasa_exito"] = families["ganados"] / families["participaciones"].clip(lower=1) * 100.0
        st.dataframe(families, use_container_width=True, hide_index=True, height=520, column_config=_number_config(families))
        _company_table(company_frame)
    elif view == "Casi ganados":
        keys = tuple(company_frame["acto_key"].dropna().astype(str).unique().tolist())
        proposals = _proposals_for_act_keys(keys, repo)
        near = near_miss_opportunities(company_frame, proposals, company)
        if near.empty:
            st.info("No se pudieron identificar derrotas con precios comparables en el periodo.")
        else:
            st.caption("Menor brecha porcentual primero. Verifica alcance y renglones antes de interpretar la diferencia como margen.")
            st.dataframe(near, use_container_width=True, hide_index=True, height=620, column_config=_number_config(near, {"enlace": st.column_config.LinkColumn("Acto", display_text="Abrir")}))
    elif view == "Competidores":
        competitors = competitor_summary(company_frame)
        st.dataframe(competitors, use_container_width=True, hide_index=True, height=620, column_config=_number_config(competitors))
    elif view == "Comparables":
        ranking = _provider_top_ranking(filters, repo)
        comparable = comparable_providers(ranking, company, limit=30).drop(columns=["proveedor_norm"], errors="ignore")
        if comparable.empty:
            st.info("La empresa no aparece con suficiente información en el ranking del periodo.")
        else:
            st.dataframe(comparable, use_container_width=True, hide_index=True, height=620, column_config=_number_config(comparable))
    else:
        relations = _provider_entity_ranking(filters, company, repo)
        st.dataframe(relations.drop(columns=["proveedor_norm"], errors="ignore"), use_container_width=True, hide_index=True, height=620, column_config=_number_config(relations))


def _render_tops(repo: InteligenciaPCRepository, filters: PCFilters) -> None:
    st.header("Tops estratégicos")
    st.caption("Todos los rankings respetan el periodo y filtros globales. Solo se consulta la vista elegida para conservar velocidad.")
    top_view = st.radio(
        "Vista de top",
        ["Categorías", "Proveedores", "Entidades", "Mercados atacables", "Baja competencia", "RS Engineering"],
        horizontal=True,
        key="pc_top_view",
    )
    if top_view == "Categorías":
        _render_top_categories(repo, filters)
    elif top_view == "Proveedores":
        _render_top_providers(repo, filters)
    elif top_view == "Entidades":
        _render_top_entities(repo, filters)
    elif top_view == "Mercados atacables":
        _render_attackable_markets(repo, filters)
    elif top_view == "Baja competencia":
        _render_low_competition(repo, filters)
    else:
        _render_rs_intelligence(repo, filters)


database_url = _config_value("SUPABASE_DB_URL") or _config_value("DATABASE_URL")
try:
    repo = _open_repository(database_url)
except PCAnalyticsUnavailable as exc:
    st.error(f"No fue posible abrir Inteligencia PC: {exc}")
    st.stop()

options = _options(repo)

st.title("Inteligencia PC")
st.caption(
    "Inteligencia de mercado no médico de Panamá Compra: proyectos, empresas, precios, competencia y tendencias. "
    "Los actos con ficha técnica o evidencia médica fuerte se excluyen de esta vista."
)
st.caption(f"Origen activo: `{repo.source_label}`")

with st.sidebar:
    st.header("Universo de análisis")
    current = date.today()
    start_default = date(max(2000, current.year - 2), 1, 1)
    start_date = st.date_input("Desde", value=start_default, max_value=current, key="pc_start")
    end_date = st.date_input("Hasta", value=current, min_value=start_date, max_value=current, key="pc_end")
    selected_states = st.multiselect("Estados", options.get("states", []), key="pc_states")
    selected_entities = st.multiselect("Entidades", options.get("entities", []), key="pc_entities")
    selected_families = st.multiselect("Familias de proyecto", options.get("families", []), key="pc_families")
    search_text = st.text_input("Buscar proyectos", placeholder="chiller, mantenimiento eléctrico", key="pc_search")
    search_mode = st.radio("Combinar búsqueda", ["OR", "AND"], horizontal=True, key="pc_search_mode")
    min_amount = st.number_input("Monto mínimo", min_value=0.0, value=0.0, step=1000.0, key="pc_min_amount")
    max_amount = st.number_input("Monto máximo (0 = sin límite)", min_value=0.0, value=0.0, step=1000.0, key="pc_max_amount")
    include_ambiguous = st.checkbox("Incluir casos ambiguos", value=False, help="Por defecto se excluyen para mantener limpio el universo no médico.")

search_groups = tuple(part.strip() for part in search_text.split(",") if part.strip())
filters = PCFilters(
    start_date=start_date,
    end_date=end_date,
    states=tuple(selected_states),
    entities=tuple(selected_entities),
    families=tuple(selected_families),
    search_groups=search_groups,
    search_mode=search_mode,
    min_amount=float(min_amount),
    max_amount=float(max_amount),
    include_ambiguous=include_ambiguous,
)

section = st.radio(
    "Sección",
    ["Panorama", "Tops", "Empresas", "Proyectos", "Competencia", "Tendencias", "Estudio profundo", "Seguimiento"],
    horizontal=True,
    label_visibility="collapsed",
    key="pc_section",
)

if section == "Tops":
    _render_tops(repo, filters)

elif section == "Panorama":
    with st.spinner("Construyendo el panorama no médico..."):
        families = _family_market_summary(filters, repo)
    total_acts = float(families["actos"].sum()) if not families.empty else 0.0
    total_amount = float(families["monto_total"].sum()) if not families.empty else 0.0
    active_months = int(families["meses_activos"].max()) if not families.empty else 0
    kpis = st.columns(5)
    kpis[0].metric("Actos", _number(total_acts))
    kpis[1].metric("Monto de referencia", _money(total_amount))
    kpis[2].metric("Promedio mensual", _number(total_acts / active_months if active_months else 0))
    kpis[3].metric("Ticket promedio", _money(total_amount / total_acts if total_acts else 0))
    kpis[4].metric("Familias", _number(len(families)))

    st.subheader("Mapa de oportunidades")
    st.caption("El score se calcula únicamente entre las familias visibles y respeta estrictamente los pesos indicados.")
    with st.expander("Pesos del score", expanded=False):
        weight_columns = st.columns(5)
        weights = {
            "actos": weight_columns[0].number_input("Número de actos", min_value=0.0, value=25.0, step=1.0),
            "monto": weight_columns[1].number_input("Monto de mercado", min_value=0.0, value=25.0, step=1.0),
            "competencia": weight_columns[2].number_input("Menor competencia", min_value=0.0, value=20.0, step=1.0),
            "recurrencia": weight_columns[3].number_input("Recurrencia", min_value=0.0, value=15.0, step=1.0),
            "diversificacion": weight_columns[4].number_input("Diversificación", min_value=0.0, value=15.0, step=1.0),
        }
    if not families.empty:
        families = score_family_opportunities(families.drop(columns=["score_oportunidad"], errors="ignore"), weights)
        display = families.rename(columns={
            "familia": "Familia", "score_oportunidad": "Score", "actos": "Actos",
            "monto_total": "Monto total", "ticket_promedio": "Ticket promedio",
            "ticket_mediano": "Ticket mediano", "participantes_promedio": "Participantes prom.",
            "entidades": "Entidades", "meses_activos": "Meses activos",
        })
        st.dataframe(display, use_container_width=True, hide_index=True, height=480, column_config=_number_config(display))
        chart = alt.Chart(display.head(12)).mark_bar().encode(
            x=alt.X("Score:Q", title="Score de oportunidad"),
            y=alt.Y("Familia:N", sort="-x", title=""),
            color=alt.Color("Monto total:Q", scale=alt.Scale(scheme="tealblues")),
            tooltip=["Familia", "Score", "Actos", alt.Tooltip("Monto total:Q", format=",.2f")],
        ).properties(height=420)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No hay actos no médicos para los filtros seleccionados.")

elif section == "Empresas":
    st.subheader("Consulta y ranking de proveedores")
    company = _company_selector(repo, key_prefix="pc_company")
    if company:
        with st.spinner(f"Analizando {company}..."):
            company_frame = _company_acts(company, filters, repo)
        _render_company_kpis(company_frame)
        _company_table(company_frame)
        trend = company_yearly_trend(company_frame)
        if not trend.empty:
            st.subheader("Evolución anual")
            melted = trend.melt("ano", value_vars=["participaciones", "ganados"], var_name="Métrica", value_name="Cantidad")
            st.altair_chart(alt.Chart(melted).mark_line(point=True).encode(x=alt.X("ano:O", title="Año"), y="Cantidad:Q", color="Métrica:N", tooltip=["ano", "Métrica", "Cantidad"]), use_container_width=True)
    else:
        st.info("Escribe una empresa para abrir su perfil. También puedes cargar el ranking general del periodo.")
        if st.button("Cargar top de proveedores", type="primary"):
            st.session_state["pc_load_provider_ranking"] = True
        if st.session_state.get("pc_load_provider_ranking"):
            with st.spinner("Calculando el ranking completo en la base de datos..."):
                ranking = _provider_market_ranking(filters, repo)
            if not ranking.empty:
                display = ranking.head(300).rename(columns={
                    "proveedor": "Proveedor", "participaciones": "Participaciones",
                    "adjudicaciones": "Adjudicaciones", "tasa_exito": "Tasa de éxito",
                    "monto_ofertado": "Monto ofertado", "monto_ganado": "Monto ganado",
                    "oferta_minima": "Oferta mínima", "oferta_promedio": "Oferta promedio",
                    "oferta_mediana": "Oferta mediana", "oferta_maxima": "Oferta máxima",
                    "familias": "Familias", "entidades": "Entidades",
                }).drop(columns=["proveedor_norm"], errors="ignore")
                st.dataframe(display, use_container_width=True, hide_index=True, height=700, column_config=_number_config(display))

elif section == "Proyectos":
    st.subheader("Explorador de proyectos")
    sort_map = {
        "Más recientes": ("fecha_analitica", False),
        "Mayor monto": ("monto_referencia", False),
        "Menor monto": ("monto_referencia", True),
        "Mayor competencia": ("num_participantes", False),
        "Menor competencia": ("num_participantes", True),
    }
    controls = st.columns([2, 1, 1])
    selected_sort = controls[0].selectbox("Orden", list(sort_map), key="pc_project_sort")
    page_size = controls[1].selectbox("Filas", [50, 100, 200, 500], index=1, key="pc_project_page_size")
    page = int(controls[2].number_input("Página", min_value=1, value=1, step=1, key="pc_project_page"))
    sort_column, ascending = sort_map[selected_sort]
    with st.spinner("Consultando la página solicitada..."):
        display, total_projects = _project_page(filters, sort_column, ascending, page_size, page, repo)
    if display.empty:
        st.info("No hay proyectos en esta página para los filtros seleccionados.")
    else:
        total_pages = max(1, (total_projects + page_size - 1) // page_size)
        st.caption(f"{_number(total_projects)} proyectos en el universo filtrado · página {page} de {total_pages}.")
        display = display.drop(columns=["acto_key"], errors="ignore")
        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            height=850,
            column_config=_number_config(display, {"enlace": st.column_config.LinkColumn("Acto", display_text="Abrir"), "fecha_analitica": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY")}),
        )

elif section == "Competencia":
    st.subheader("Inteligencia competitiva")
    company = _company_selector(repo, key_prefix="pc_competition")
    if company:
        company_frame = _company_acts(company, filters, repo)
        competitors = competitor_summary(company_frame)
        _render_company_kpis(company_frame)
        if competitors.empty:
            st.info("No se encontraron competidores compartidos en el periodo.")
        else:
            st.dataframe(competitors, use_container_width=True, hide_index=True, height=600, column_config=_number_config(competitors))
            chart = alt.Chart(competitors.head(20)).mark_bar().encode(
                x=alt.X("coincidencias:Q", title="Actos compartidos"),
                y=alt.Y("competidor:N", sort="-x", title=""),
                color=alt.Color("victorias_competidor:Q", title="Victorias"),
                tooltip=["competidor", "coincidencias", "victorias_competidor", alt.Tooltip("tasa_victoria_competidor:Q", format=".1f")],
            ).properties(height=520)
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Selecciona una empresa para identificar sus competidores recurrentes.")

elif section == "Tendencias":
    st.subheader("Tendencias del mercado")
    trend = _monthly_market_trend(filters, repo)
    if trend.empty:
        st.info("No hay datos temporales para los filtros seleccionados.")
    else:
        amount_chart = alt.Chart(trend).mark_area(opacity=0.5).encode(
            x=alt.X("periodo:T", title="Mes"), y=alt.Y("monto:Q", title="Monto de referencia"),
            tooltip=[alt.Tooltip("periodo:T", format="%Y-%m"), alt.Tooltip("monto:Q", format=",.2f"), "actos", "entidades"],
        ).properties(height=360)
        count_chart = alt.Chart(trend).mark_line(point=True, color="#21c77a").encode(
            x=alt.X("periodo:T", title="Mes"), y=alt.Y("actos:Q", title="Número de actos"), tooltip=[alt.Tooltip("periodo:T", format="%Y-%m"), "actos", "entidades"]
        ).properties(height=300)
        st.altair_chart(amount_chart, use_container_width=True)
        st.altair_chart(count_chart, use_container_width=True)
        st.dataframe(trend, use_container_width=True, hide_index=True, column_config=_number_config(trend))

elif section == "Estudio profundo":
    st.subheader("Estudio profundo empresarial")
    st.caption("Genera y conserva un informe reproducible con el mismo periodo y filtros visibles.")
    company = _company_selector(repo, key_prefix="pc_study")
    notes = st.text_area("Objetivo o notas", placeholder="Ej. identificar rubros adyacentes, competidores y bandas históricas de precio.")
    if company and st.button("Generar estudio", type="primary"):
        with st.spinner("Analizando historial, familias y competencia..."):
            company_frame = _company_acts(company, filters, repo)
            competitors = competitor_summary(company_frame)
            report = build_deep_report(target=company, acts=company_frame, competitors=competitors, filters=filters)
            if notes.strip():
                report += f"\n\n## Objetivo indicado\n{notes.strip()}"
            study_id = repo.save_study(
                study_type="empresa",
                target=company,
                report=report,
                payload={"filters": filters.__dict__, "notes": notes},
                username=current_username(),
            )
            st.session_state["pc_last_report"] = report
            st.success(f"Estudio guardado: {study_id}")
    report = st.session_state.get("pc_last_report", "")
    if report:
        st.markdown(report)
        st.download_button("Descargar estudio", report.encode("utf-8"), file_name="estudio_inteligencia_pc.md", mime="text/markdown")
    studies = repo.list_studies(limit=30)
    if not studies.empty:
        st.subheader("Estudios guardados")
        st.dataframe(studies.drop(columns=["report"], errors="ignore"), use_container_width=True, hide_index=True)

elif section == "Seguimiento":
    st.subheader("Radar y seguimiento")
    st.caption("Conserva empresas, familias o palabras clave para futuras alertas y revisiones rápidas.")
    watch_type = st.selectbox("Tipo", ["Empresa", "Familia", "Palabras clave"])
    if watch_type == "Familia":
        target = st.selectbox("Familia", [family for family, _ in FAMILY_RULES] + ["Otros rubros no medicos"])
    else:
        target = st.text_input("Objetivo", placeholder="RS Engineering o chiller, refrigeración")
    if st.button("Agregar a seguimiento", type="primary"):
        if not clean_text(target):
            st.warning("Indica un objetivo.")
        else:
            repo.add_watch(username=current_username(), watch_type=watch_type, target=target)
            st.success("Seguimiento guardado.")
            st.rerun()
    watches = repo.list_watches(username=current_username())
    if watches.empty:
        st.info("Todavía no hay seguimientos guardados.")
    else:
        st.dataframe(watches, use_container_width=True, hide_index=True)
        remove_id = st.selectbox("Quitar seguimiento", watches["watch_id"].tolist(), format_func=lambda value: watches.loc[watches["watch_id"] == value, "target"].iloc[0])
        if st.button("Quitar"):
            repo.remove_watch(username=current_username(), watch_id=remove_id)
            st.rerun()
