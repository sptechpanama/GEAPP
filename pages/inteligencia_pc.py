from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from core.config import APP_ROOT
from services.access_control import build_authenticator, current_username, require_page_access
from services.inteligencia_pc import (
    FAMILY_RULES,
    PCAnalyticsUnavailable,
    PCFilters,
    InteligenciaPCRepository,
    build_deep_report,
    clean_text,
    company_summary,
    company_yearly_trend,
    competitor_summary,
    score_family_opportunities,
)
from ui.theme import apply_global_theme


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
def _repository(database_url: str) -> InteligenciaPCRepository:
    return InteligenciaPCRepository.connect(database_url=database_url, local_candidates=LOCAL_DB_CANDIDATES)


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


database_url = _config_value("SUPABASE_DB_URL") or _config_value("DATABASE_URL")
try:
    repo = _repository(database_url)
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
    ["Panorama", "Empresas", "Proyectos", "Competencia", "Tendencias", "Estudio profundo", "Seguimiento"],
    horizontal=True,
    label_visibility="collapsed",
    key="pc_section",
)

if section == "Panorama":
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
