from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from googleapiclient.http import MediaIoBaseDownload

from core.config import APP_ROOT
from services.access_control import build_authenticator, current_username, require_page_access
from services.auth_drive import get_drive_delegated
from services.inteligencia_orquestador_v3 import (
    delete_saved_view,
    get_request_status,
    list_saved_views,
    queue_study,
    save_saved_view,
)
from services.inteligencia_proveedores_v3 import (
    AnalyticsFilters,
    AnalyticsRepository,
    AnalyticsUnavailable,
    DATE_COLUMNS,
    PROFILE_LABELS,
    SCORE_PRESETS,
    apply_master_filters,
    dataframe_to_csv_bytes,
    normalize_score_weights,
    preset_range,
    score_opportunities,
    sort_and_page,
    split_search_groups,
)
from ui.theme import apply_global_theme


PAGE_PATH = "pages/inteligencia_oportunidades_proveedores.py"
LOCAL_ANALYTICS_CANDIDATES = (
    APP_ROOT / "data" / "db" / "inteligencia_proveedores.db",
    APP_ROOT / "data" / "inteligencia_proveedores.db",
    APP_ROOT / "inteligencia_proveedores.db",
    Path.home() / "scrapers_repo" / "data" / "db" / "inteligencia_proveedores.db",
)


st.set_page_config(
    page_title="Inteligencia de oportunidades y proveedores",
    page_icon="🎯",
    layout="wide",
)
apply_global_theme()

authenticator = build_authenticator()
try:
    authenticator.login(" ", location="sidebar", key="auth_intel_v3_silent")
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
    env = str(os.getenv(key, "") or "").strip()
    if env:
        return env
    app = _app_secrets()
    value = str(app.get(key, "") or "").strip()
    if value:
        return value
    try:
        return str(st.secrets.get(key, default) or default).strip()
    except Exception:
        return default


def _database_url() -> str:
    return _config_value("SUPABASE_DB_URL") or _config_value("DATABASE_URL")


@st.cache_resource(show_spinner=False)
def _repository(database_url: str) -> AnalyticsRepository:
    return AnalyticsRepository.connect(database_url=database_url, local_candidates=LOCAL_ANALYTICS_CANDIDATES)


@st.cache_data(show_spinner=False, ttl=300)
def _master_data(filters: AnalyticsFilters, _repo: AnalyticsRepository) -> pd.DataFrame:
    return _repo.master_metrics(filters)


@st.cache_data(show_spinner=False, ttl=600)
def _filter_options(_repo: AnalyticsRepository) -> dict[str, list[str]]:
    return _repo.filter_options()


@st.cache_data(show_spinner=False, ttl=300)
def _monthly_data(filters: AnalyticsFilters, fichas: tuple[str, ...], _repo: AnalyticsRepository) -> pd.DataFrame:
    return _repo.monthly_trend(filters, fichas=fichas)


@st.cache_data(show_spinner=False, ttl=300)
def _acts_data(ficha: str, filters: AnalyticsFilters, _repo: AnalyticsRepository) -> pd.DataFrame:
    return _repo.acts_for_ficha(ficha, filters)


@st.cache_data(show_spinner=False, ttl=300)
def _provider_data(ficha: str, filters: AnalyticsFilters, _repo: AnalyticsRepository) -> pd.DataFrame:
    return _repo.providers_for_ficha(ficha, filters)


@st.cache_data(show_spinner=False, ttl=600)
def _catalog_data(ficha: str, _repo: AnalyticsRepository) -> pd.DataFrame:
    return _repo.catalog_for_ficha(ficha)


def _money(value: object) -> str:
    try:
        return f"${float(value or 0):,.2f}"
    except Exception:
        return "$0.00"


def _safe_int(value: object) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _normalize_ficha(value: object) -> str:
    raw = str(value if value is not None else "").strip()
    if re.fullmatch(r"\d+\.0+", raw):
        raw = raw.split(".", 1)[0]
    match = re.search(r"\d+", raw)
    return match.group(0) if match else ""


@st.cache_data(show_spinner=False, ttl=300)
def _drive_ficha_list(kind: str, configured_file_id: str) -> tuple[tuple[str, ...], str]:
    settings = {
        "favoritos": ("prospeccion_rir_favoritos.xlsx",),
        "foyomed": ("prospeccion_rir_presentes_catalogo_foyomed.xlsx",),
    }
    names = settings.get(kind, ())
    if not names:
        return (), ""
    drive = get_drive_delegated()
    if drive is None:
        raise RuntimeError("Google Drive no está disponible.")
    file_id = str(configured_file_id or "").strip()
    if not file_id:
        escaped = names[0].replace("'", "\\'")
        response = drive.files().list(
            q=f"trashed = false and name = '{escaped}'",
            pageSize=1,
            fields="files(id,name,modifiedTime)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        files = response.get("files", []) if isinstance(response, dict) else []
        if not files:
            return (), ""
        file_id = str(files[0].get("id", "") or "").strip()
    metadata = drive.files().get(
        fileId=file_id,
        fields="id,name,modifiedTime",
        supportsAllDrives=True,
    ).execute()
    stream = BytesIO()
    downloader = MediaIoBaseDownload(
        stream,
        drive.files().get_media(fileId=file_id, supportsAllDrives=True),
    )
    done = False
    while not done:
        _, done = downloader.next_chunk()
    stream.seek(0)
    frame = pd.read_excel(stream)
    ficha_column = next(
        (
            column
            for column in frame.columns
            if re.sub(r"[^a-z0-9]+", "", str(column).lower()) in {"ficha", "ficha#", "numeroficha"}
        ),
        None,
    )
    if ficha_column is None:
        return (), str(metadata.get("modifiedTime", "") or "")
    fichas = tuple(dict.fromkeys(filter(None, (_normalize_ficha(value) for value in frame[ficha_column]))))
    return fichas, str(metadata.get("modifiedTime", "") or "")


def _apply_pending_saved_view() -> None:
    payload = st.session_state.pop("intel_v3_pending_saved_view", None)
    if not isinstance(payload, dict) or not payload:
        return
    date_label_by_value = {
        "publicacion": "Fecha de publicación",
        "celebracion": "Fecha de celebración",
        "adjudicacion": "Fecha de adjudicación",
        "actualizacion": "Fecha de actualización",
    }
    preset_label_by_value = {
        "equilibrado": "Equilibrado",
        "volumen": "Priorizar volumen y dinero",
        "baja_competencia": "Priorizar baja competencia",
        "buscar_proveedor": "Priorizar búsqueda de proveedor",
        "baja_complejidad": "Priorizar baja complejidad",
        "personalizado": "Personalizado",
    }
    start_raw = str(payload.get("fecha_desde", "") or "").strip()
    end_raw = str(payload.get("fecha_hasta", "") or "").strip()
    if start_raw or end_raw:
        st.session_state["intel_v3_period"] = "Personalizado"
        try:
            st.session_state["intel_v3_start"] = date.fromisoformat(start_raw)
        except ValueError:
            pass
        try:
            st.session_state["intel_v3_end"] = date.fromisoformat(end_raw)
        except ValueError:
            pass
    st.session_state["intel_v3_date_basis"] = date_label_by_value.get(
        str(payload.get("tipo_fecha", "publicacion")), "Fecha de publicación"
    )
    profile = str(payload.get("perfil_deteccion", "moderado"))
    st.session_state["intel_v3_profile"] = PROFILE_LABELS.get(profile, PROFILE_LABELS["moderado"])
    assignments = {
        "intel_v3_states": list(payload.get("estados", []) or []),
        "intel_v3_entities": list(payload.get("entidades", []) or []),
        "intel_v3_areas": list(payload.get("areas", []) or []),
        "intel_v3_product_types": list(payload.get("tipos_producto", []) or []),
        "intel_v3_ct": str(payload.get("criterio_tecnico", "Todos") or "Todos"),
        "intel_v3_rs": str(payload.get("registro_sanitario", "Todos") or "Todos"),
        "intel_v3_search": ", ".join(str(value) for value in payload.get("busqueda", []) or []),
        "intel_v3_search_mode": str(payload.get("modo_busqueda", "OR") or "OR"),
        "intel_v3_min_ref": float(payload.get("monto_minimo", 0) or 0),
        "intel_v3_max_ref": float(payload.get("monto_maximo", 0) or 0),
        "intel_v3_min_award": float(payload.get("adjudicado_minimo", 0) or 0),
        "intel_v3_max_award": float(payload.get("adjudicado_maximo", 0) or 0),
        "intel_v3_min_acts": int(payload.get("actos_minimos", 0) or 0),
        "intel_v3_min_entities": int(payload.get("entidades_minimas", 0) or 0),
        "intel_v3_min_active_months": int(payload.get("meses_activos_minimos", 0) or 0),
        "intel_v3_max_participants": float(payload.get("participantes_promedio_maximo", 0) or 0),
        "intel_v3_catalog_only": bool(payload.get("solo_catalogo", False)),
        "intel_v3_availability": str(payload.get("disponibilidad", "Todas") or "Todas"),
        "intel_v3_min_score": float(payload.get("score_minimo_oportunidad", 0) or 0),
        "intel_v3_recommendations": list(payload.get("recomendaciones", []) or []),
    }
    for key, value in assignments.items():
        st.session_state[key] = value
    score_preset = str(payload.get("score_preset", "equilibrado") or "equilibrado")
    st.session_state["intel_v3_score_preset"] = preset_label_by_value.get(score_preset, "Equilibrado")
    for name, value in dict(payload.get("score_weights", {}) or {}).items():
        if name in SCORE_PRESETS["equilibrado"]:
            st.session_state[f"intel_v3_weight_{name}"] = float(value or 0)


def _render_saved_views(current_payload: dict[str, object]) -> None:
    with st.sidebar.expander("Vistas guardadas", expanded=False):
        username = current_username()
        sheet_id, _ = _sheet_ids()
        views = st.session_state.get("intel_v3_saved_views", [])
        if st.button("Cargar / actualizar vistas", key="intel_v3_load_views", width="stretch"):
            try:
                from sheets import get_client

                client, _ = get_client()
                views = list_saved_views(client, sheet_id=sheet_id, username=username)
                st.session_state["intel_v3_saved_views"] = views
                st.success(f"{len(views)} vista(s) disponible(s).")
            except Exception as exc:
                st.error(f"No se pudieron cargar las vistas: {exc}")
        views = st.session_state.get("intel_v3_saved_views", [])
        selected_id = ""
        if isinstance(views, list) and views:
            labels = {str(item.get("id", "")): str(item.get("name", "") or "Sin nombre") for item in views}
            selected_id = st.selectbox(
                "Vista",
                list(labels),
                format_func=lambda value: labels.get(value, value),
                key="intel_v3_saved_view_selected",
            )
            apply_col, delete_col = st.columns(2)
            if apply_col.button("Aplicar", key="intel_v3_apply_view", width="stretch"):
                selected = next((item for item in views if str(item.get("id", "")) == selected_id), {})
                st.session_state["intel_v3_pending_saved_view"] = dict(selected.get("payload", {}) or {})
                st.rerun()
            if delete_col.button("Eliminar", key="intel_v3_delete_view", width="stretch"):
                try:
                    from sheets import get_client

                    client, _ = get_client()
                    if delete_saved_view(client, sheet_id=sheet_id, username=username, view_id=selected_id):
                        st.session_state["intel_v3_saved_views"] = [
                            item for item in views if str(item.get("id", "")) != selected_id
                        ]
                        st.rerun()
                except Exception as exc:
                    st.error(f"No se pudo eliminar la vista: {exc}")
        view_name = st.text_input("Guardar configuración como", key="intel_v3_saved_view_name")
        if st.button("Guardar vista", key="intel_v3_save_view", width="stretch"):
            if not view_name.strip():
                st.warning("Escribe un nombre para la vista.")
            else:
                try:
                    from sheets import get_client

                    client, _ = get_client()
                    save_saved_view(
                        client,
                        sheet_id=sheet_id,
                        username=username,
                        name=view_name,
                        payload=current_payload,
                    )
                    st.session_state["intel_v3_saved_views"] = list_saved_views(
                        client, sheet_id=sheet_id, username=username
                    )
                    st.success("Vista guardada.")
                except Exception as exc:
                    st.error(f"No se pudo guardar la vista: {exc}")


def _period_inputs() -> tuple[date | None, date | None]:
    preset_labels = {
        "Año 2026": "2026",
        "Año 2025": "2025",
        "Últimos 6 meses": "ultimos_6_meses",
        "Últimos 12 meses": "ultimos_12_meses",
        "Últimos 24 meses": "ultimos_24_meses",
        "Histórico completo": "historico",
        "Personalizado": "personalizado",
    }
    selected_label = st.selectbox("Periodo de análisis", list(preset_labels), index=0, key="intel_v3_period")
    selected_key = preset_labels[selected_label]
    if selected_key != "personalizado":
        return preset_range(selected_key)
    default_start, default_end = preset_range("ultimos_12_meses")
    start = st.date_input("Desde", value=default_start, key="intel_v3_start")
    end = st.date_input("Hasta", value=default_end, key="intel_v3_end")
    if start > end:
        st.error("La fecha inicial no puede ser posterior a la final.")
        st.stop()
    return start, end


def _score_weights() -> tuple[str, dict[str, float]]:
    preset_labels = {
        "Equilibrado": "equilibrado",
        "Priorizar volumen y dinero": "volumen",
        "Priorizar baja competencia": "baja_competencia",
        "Priorizar búsqueda de proveedor": "buscar_proveedor",
        "Priorizar baja complejidad": "baja_complejidad",
        "Personalizado": "personalizado",
    }
    label = st.selectbox("Enfoque del ranking", list(preset_labels), index=0, key="intel_v3_score_preset")
    key = preset_labels[label]
    if key != "personalizado":
        return key, dict(SCORE_PRESETS[key])
    with st.expander("Pesos personalizados", expanded=True):
        columns = st.columns(3)
        raw: dict[str, float] = {}
        labels = {
            "demanda": "Demanda",
            "economia": "Potencial económico",
            "competencia": "Competencia favorable",
            "viabilidad": "Viabilidad/proveedores",
            "preparacion": "Preparación operativa",
            "confianza": "Confianza del dato",
        }
        labels.pop("preparacion", None)
        labels.pop("confianza", None)
        labels["complejidad"] = "Complejidad favorable"
        for index, (name, display) in enumerate(labels.items()):
            with columns[index % 3]:
                raw[name] = float(st.number_input(display, 0.0, 100.0, float(SCORE_PRESETS["equilibrado"][name]), 1.0, key=f"intel_v3_weight_{name}"))
    return key, normalize_score_weights(raw)


def _selected_ficha(frame: pd.DataFrame, key: str) -> str:
    if frame.empty:
        return ""
    labels = {
        str(row["ficha"]): f"{row['ficha']} | {str(row['nombre_ficha'])[:110]}"
        for _, row in frame.sort_values("score_oportunidad", ascending=False).iterrows()
    }
    codes = list(labels)
    selected = st.selectbox("Ficha para análisis detallado", codes, format_func=lambda value: labels[value], key=key)
    return str(selected)


def _sheet_ids() -> tuple[str, str]:
    fallback = _config_value("SHEET_ID")
    manual = _config_value("PC_MANUAL_SHEET_ID", fallback) or fallback
    config = _config_value("PC_CONFIG_SHEET_ID", manual) or manual
    return manual, config


def _render_data_status(repository: AnalyticsRepository) -> None:
    coverage = repository.coverage()
    metadata = repository.build_metadata()
    cols = st.columns([1.4, 1, 1, 1])
    cols[0].caption(f"Fuente: **{repository.source_label}**")
    cols[1].caption(f"Actos normalizados: **{_safe_int(coverage.get('acts')):,}**")
    cols[2].caption(f"Fichas: **{_safe_int(coverage.get('fichas')):,}**")
    built = str(metadata.get("built_at_utc", "") or "")[:19].replace("T", " ")
    cols[3].caption(f"Capa construida: **{built or 'sin dato'}**")


def _render_master_table(frame: pd.DataFrame) -> None:
    st.subheader("Mapa maestro de oportunidades")
    sort_options = {
        "Score de oportunidad": "score_oportunidad",
        "Monto de referencia": "monto_referencia",
        "Monto adjudicado confirmado": "monto_adjudicado",
        "Número de actos": "actos",
        "Actos de ficha única": "actos_ficha_unica",
        "Entidades distintas": "entidades",
        "Menor competencia promedio": "participantes_promedio",
        "Mayor crecimiento reciente": "tendencia_6m_pct",
        "Mayor confianza": "score_confianza",
        "Menor complejidad": "score_complejidad",
        "Mayor cobertura de datos": "cobertura_monto_referencia_pct",
        "Ficha": "ficha",
    }
    c1, c2, c3, c4 = st.columns([2.2, 1, 1, 1])
    with c1:
        sort_label = st.selectbox("Orden global", list(sort_options), index=0, key="intel_v3_sort")
    with c2:
        ascending = st.selectbox("Dirección", ["Mayor a menor", "Menor a mayor"], key="intel_v3_direction") == "Menor a mayor"
    with c3:
        page_size = int(st.selectbox("Filas por página", [25, 50, 100, 250], index=1, key="intel_v3_page_size"))
    max_pages = max(1, (len(frame) + page_size - 1) // page_size)
    with c4:
        page = int(st.number_input("Página", 1, max_pages, min(int(st.session_state.get("intel_v3_page", 1)), max_pages), key="intel_v3_page"))
    page_frame, pages, total = sort_and_page(frame, sort_by=sort_options[sort_label], ascending=ascending, page=page, page_size=page_size)
    st.caption(f"Orden aplicado sobre las **{total:,} fichas filtradas**. Página {page} de {pages}.")

    display_columns = [
        "ficha", "nombre_ficha", "recomendacion", "score_oportunidad", "score_demanda", "score_economia",
        "score_competencia", "score_viabilidad", "score_complejidad", "score_confianza", "actos", "actos_ficha_unica", "entidades",
        "monto_referencia", "monto_adjudicado", "cobertura_monto_referencia_pct", "cobertura_monto_adjudicado_pct",
        "cobertura_ganador_pct", "cobertura_participantes_pct", "ticket_promedio", "ticket_mediano",
        "participantes_promedio", "participantes_mediana", "proponentes_distintos", "top_1_ganador", "top_1_pct",
        "top_3_concentracion_pct", "concentracion_hhi",
        "proveedores_catalogo", "proveedores_contactables", "tiene_ct", "registro_sanitario", "tendencia_6m_pct",
        "ultima_fecha", "razones", "enlace_minsa",
    ]
    display = page_frame[[column for column in display_columns if column in page_frame.columns]].copy()
    st.dataframe(
        display,
        width="stretch",
        height=min(1_000, 90 + max(1, len(display)) * 35),
        hide_index=True,
        column_config={
            "ficha": "Ficha",
            "nombre_ficha": st.column_config.TextColumn("Nombre de ficha", width="large"),
            "recomendacion": st.column_config.TextColumn("Recomendación", width="medium"),
            "score_oportunidad": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.1f"),
            "score_demanda": st.column_config.NumberColumn("Demanda", format="%.1f"),
            "score_economia": st.column_config.NumberColumn("Economía", format="%.1f"),
            "score_competencia": st.column_config.NumberColumn("Competencia", format="%.1f"),
            "score_viabilidad": st.column_config.NumberColumn("Viabilidad", format="%.1f"),
            "score_complejidad": st.column_config.NumberColumn("Complejidad favorable", format="%.1f"),
            "score_confianza": st.column_config.NumberColumn("Confianza", format="%.1f"),
            "monto_referencia": st.column_config.NumberColumn("Monto referencia", format="$ %.2f"),
            "monto_adjudicado": st.column_config.NumberColumn("Adjudicado confirmado", format="$ %.2f"),
            "ticket_promedio": st.column_config.NumberColumn("Ticket promedio", format="$ %.2f"),
            "ticket_mediano": st.column_config.NumberColumn("Ticket mediano", format="$ %.2f"),
            "cobertura_monto_referencia_pct": st.column_config.NumberColumn("Cobertura referencia", format="%.1f%%"),
            "cobertura_monto_adjudicado_pct": st.column_config.NumberColumn("Cobertura adjudicado", format="%.1f%%"),
            "cobertura_ganador_pct": st.column_config.NumberColumn("Cobertura ganador", format="%.1f%%"),
            "cobertura_participantes_pct": st.column_config.NumberColumn("Cobertura participantes", format="%.1f%%"),
            "participantes_promedio": st.column_config.NumberColumn("Participantes prom.", format="%.2f"),
            "participantes_mediana": st.column_config.NumberColumn("Participantes mediana", format="%.2f"),
            "top_1_pct": st.column_config.NumberColumn("Top 1 %", format="%.1f%%"),
            "top_3_concentracion_pct": st.column_config.NumberColumn("Concentración Top 3", format="%.1f%%"),
            "tendencia_6m_pct": st.column_config.NumberColumn("Tendencia 6m", format="%.1f%%"),
            "enlace_minsa": st.column_config.LinkColumn("Ficha MINSA", display_text="Abrir"),
            "razones": st.column_config.TextColumn("Explicación", width="large"),
        },
    )
    st.download_button(
        "Descargar todas las fichas filtradas (CSV)",
        dataframe_to_csv_bytes(frame.sort_values(sort_options[sort_label], ascending=ascending, kind="stable")),
        file_name=f"inteligencia_oportunidades_{date.today():%Y%m%d}.csv",
        mime="text/csv",
        key="intel_v3_download_master",
    )


def _render_trends(frame: pd.DataFrame, filters: AnalyticsFilters, repository: AnalyticsRepository) -> None:
    st.subheader("Tendencias y estabilidad de la demanda")
    if frame.empty:
        st.info("No hay datos para el periodo seleccionado.")
        return
    top_codes = tuple(frame.nlargest(min(8, len(frame)), "score_oportunidad")["ficha"].astype(str).tolist())
    selected = st.multiselect("Fichas a comparar", frame["ficha"].astype(str).tolist(), default=list(top_codes[:5]), key="intel_v3_trend_fichas")
    if not selected:
        st.info("Selecciona al menos una ficha.")
        return
    monthly = _monthly_data(filters, tuple(selected), repository)
    if monthly.empty:
        st.warning("Las fichas seleccionadas no tienen meses con la dimensión temporal elegida.")
        return
    metric_label = st.radio("Métrica", ["Actos", "Monto de referencia", "Monto adjudicado"], horizontal=True, key="intel_v3_trend_metric")
    metric_map = {"Actos": "actos", "Monto de referencia": "monto_referencia", "Monto adjudicado": "monto_adjudicado"}
    pivot = monthly.pivot_table(index="mes", columns="ficha", values=metric_map[metric_label], aggfunc="sum", fill_value=0)
    st.line_chart(pivot, height=430)
    st.dataframe(monthly, width="stretch", hide_index=True, height=380)


def _render_competition(frame: pd.DataFrame) -> None:
    st.subheader("Competencia y concentración")
    if frame.empty:
        st.info("No hay datos.")
        return
    chart = frame[["ficha", "nombre_ficha", "participantes_promedio", "monto_referencia", "score_oportunidad"]].copy()
    chart = chart.nlargest(min(300, len(chart)), "monto_referencia")
    st.scatter_chart(chart, x="participantes_promedio", y="monto_referencia", color="score_oportunidad", size="score_oportunidad", height=500)
    st.caption("Arriba a la izquierda: mayor mercado con menos participantes. El color/tamaño representa el score integral.")
    detail = frame.sort_values(["score_competencia", "monto_referencia"], ascending=[False, False]).head(250)
    st.dataframe(
        detail[["ficha", "nombre_ficha", "participantes_promedio", "proporcion_unico_proponente", "proponentes_distintos", "concentracion_hhi", "top_1_ganador", "top_1_pct", "score_competencia"]],
        width="stretch",
        hide_index=True,
        height=650,
    )


def _render_provider_detail(frame: pd.DataFrame, filters: AnalyticsFilters, repository: AnalyticsRepository) -> None:
    st.subheader("Proveedores y evidencia por ficha")
    ficha = _selected_ficha(frame, "intel_v3_provider_ficha")
    if not ficha:
        st.info("No hay una ficha seleccionable.")
        return
    row = frame[frame["ficha"].astype(str).eq(ficha)].iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Actos", f"{_safe_int(row.get('actos')):,}")
    c2.metric("Mercado referencial", _money(row.get("monto_referencia")))
    c3.metric("Participantes promedio", f"{float(row.get('participantes_promedio', 0) or 0):,.2f}")
    c4.metric("Proveedores en catálogo", f"{_safe_int(row.get('proveedores_catalogo')):,}")

    providers = _provider_data(ficha, filters, repository)
    catalog = _catalog_data(ficha, repository)
    acts = _acts_data(ficha, filters, repository)
    tab1, tab2, tab3 = st.tabs(["Competidores observados", "Proveedores de catálogo", "Actos y evidencia"])
    with tab1:
        if providers.empty:
            st.info("No se encontraron proponentes estructurados para esta ficha y periodo.")
        else:
            st.dataframe(
                providers,
                width="stretch",
                hide_index=True,
                height=650,
                column_config={
                    "monto_ganado": st.column_config.NumberColumn("Monto ganado", format="$ %.2f"),
                    "oferta_promedio": st.column_config.NumberColumn("Oferta promedio", format="$ %.2f"),
                    "tasa_exito_pct": st.column_config.NumberColumn("Tasa de éxito", format="%.1f%%"),
                },
            )
    with tab2:
        if catalog.empty:
            st.info("No hay proveedores vinculados a esta ficha en el catálogo actual.")
        else:
            st.dataframe(catalog, width="stretch", hide_index=True, height=650)
    with tab3:
        if acts.empty:
            st.info("No hay actos para la ficha bajo los filtros actuales.")
        else:
            st.dataframe(
                acts,
                width="stretch",
                hide_index=True,
                height=760,
                column_config={
                    "enlace": st.column_config.LinkColumn("Acto", display_text="Abrir"),
                    "reference_amount": st.column_config.NumberColumn("Referencia", format="$ %.2f"),
                    "award_amount": st.column_config.NumberColumn("Adjudicado", format="$ %.2f"),
                    "detection_score": st.column_config.NumberColumn("Confianza", format="%.1f"),
                },
            )


def _render_deep_study(frame: pd.DataFrame, filters: AnalyticsFilters, score_preset: str) -> None:
    st.subheader("Estudio profundo con el orquestador")
    st.caption("El estudio recibe exactamente el mismo periodo, dimensión temporal, perfil de detección y filtros usados en este análisis.")
    ficha = _selected_ficha(frame, "intel_v3_study_ficha")
    if not ficha:
        st.info("No hay una ficha seleccionable.")
        return
    row = frame[frame["ficha"].astype(str).eq(ficha)].iloc[0]
    notes = st.text_area("Objetivo o notas para el estudio", key="intel_v3_study_notes", placeholder="Ej.: validar marcas, modelos, tiempos de entrega y proveedores alternativos.")
    max_queries = int(st.number_input("Máximo de consultas detalladas", 5, 500, 80, 5, key="intel_v3_max_queries"))
    manual_sheet_id, config_sheet_id = _sheet_ids()
    if not manual_sheet_id or not config_sheet_id:
        st.warning("Configura PC_MANUAL_SHEET_ID/PC_CONFIG_SHEET_ID (o SHEET_ID) para usar el orquestador.")
        return
    if st.button("Iniciar estudio profundo", type="primary", key="intel_v3_queue_study"):
        from sheets import get_client

        filter_payload = filters.as_payload()
        scope_raw = json.dumps({"ficha": ficha, "filters": filter_payload, "preset": score_preset}, ensure_ascii=False, sort_keys=True)
        payload = {
            "ficha": ficha,
            "nombre_ficha": str(row.get("nombre_ficha", "")),
            "db_path": r"C:\Users\rodri\scrapers_repo\data\db\panamacompra.db",
            "analytics_db_path": r"C:\Users\rodri\scrapers_repo\data\db\inteligencia_proveedores.db",
            "max_queries": max_queries,
            "notes": notes,
            "headless": False,
            "filters": filter_payload,
            "score_preset": score_preset,
            "scope_id": hashlib.sha256(scope_raw.encode("utf-8")).hexdigest()[:20],
            "requested_from": PAGE_PATH,
        }
        try:
            client, _ = get_client()
            request_id = queue_study(
                client,
                manual_sheet_id=manual_sheet_id,
                config_sheet_id=config_sheet_id,
                requested_by=current_username(),
                payload=payload,
                notes=notes,
            )
            st.session_state["intel_v3_request_id"] = request_id
            st.success(f"Estudio encolado correctamente. Solicitud: {request_id}")
        except Exception as exc:
            st.error(f"No fue posible encolar el estudio: {exc}")

    request_id = str(st.session_state.get("intel_v3_request_id", "") or "").strip()
    if request_id:
        st.caption(f"Solicitud activa: `{request_id}`")
        if st.button("Consultar estado", key="intel_v3_poll_study"):
            try:
                from sheets import get_client

                client, _ = get_client()
                status = get_request_status(client, manual_sheet_id=manual_sheet_id, request_id=request_id)
                st.session_state["intel_v3_request_status"] = status
            except Exception as exc:
                st.error(f"No se pudo consultar el estado: {exc}")
        status = st.session_state.get("intel_v3_request_status", {})
        if isinstance(status, dict) and status:
            state = str(status.get("status", "") or "")
            if state.lower() in {"done", "success", "completed", "completado"}:
                st.success(f"Estudio finalizado: {state}")
            elif state.lower() in {"error", "failed", "fallido"}:
                st.error(str(status.get("result_error", "") or "El estudio terminó con error."))
            else:
                st.info(f"Estado actual: {state or 'pendiente'}")
            result_url = str(status.get("result_file_url", "") or "").strip()
            if result_url:
                st.link_button("Abrir resultado", result_url)


_apply_pending_saved_view()

st.title("🎯 Inteligencia de oportunidades y proveedores")
st.caption(
    "Análisis temporal, económico y competitivo sobre fichas completas. "
    "La base filtra y agrega todos los registros; la interfaz recibe únicamente métricas resumidas."
)

try:
    repo = _repository(_database_url())
except AnalyticsUnavailable as exc:
    st.error(
        "No se encontró la capa analítica de Inteligencia. Ejecuta "
        "`C:\\Users\\rodri\\scrapers_repo\\db\\actualizar_base_corregida.bat` para construirla y publicarla. "
        f"Detalle: {exc}"
    )
    st.stop()

_render_data_status(repo)
options = _filter_options(repo)

with st.sidebar:
    st.header("Filtros del estudio")
    start_date, end_date = _period_inputs()
    date_labels = {
        "Fecha de publicación": "publicacion",
        "Fecha de celebración": "celebracion",
        "Fecha de adjudicación": "adjudicacion",
        "Fecha de actualización": "actualizacion",
    }
    date_basis_label = st.selectbox("Dimensión temporal", list(date_labels), index=0, key="intel_v3_date_basis")
    profile_labels_reverse = {label: key for key, label in PROFILE_LABELS.items()}
    profile_label = st.selectbox("Perfil de confianza", list(profile_labels_reverse), index=1, key="intel_v3_profile")
    with st.expander("Filtros de mercado", expanded=True):
        selected_states = tuple(st.multiselect("Estado del acto", options.get("states", []), key="intel_v3_states"))
        selected_entities = tuple(st.multiselect("Entidades", options.get("entities", []), key="intel_v3_entities"))
        selected_areas = tuple(st.multiselect("Areas", options.get("areas", []), key="intel_v3_areas"))
        selected_product_types = tuple(st.multiselect("Clase / tipo de producto", options.get("product_types", []), key="intel_v3_product_types"))
        ct_status = st.selectbox("Criterio técnico", ["Todos", "Si", "No"], key="intel_v3_ct")
        rs_status = st.selectbox("Registro sanitario", ["Todos", "Si", "No"], key="intel_v3_rs")
        search_raw = st.text_input("Buscar grupos o frases (separar por coma)", key="intel_v3_search", placeholder="chiller, refrigeración, aire acondicionado")
        search_mode = st.radio("Relación entre grupos", ["OR", "AND"], horizontal=True, key="intel_v3_search_mode")
        min_reference = float(st.number_input("Precio referencia mínimo", 0.0, value=0.0, step=100.0, key="intel_v3_min_ref"))
        max_reference = float(st.number_input("Precio referencia máximo (0 = sin límite)", 0.0, value=0.0, step=1_000.0, key="intel_v3_max_ref"))
        min_award = float(st.number_input("Monto adjudicado minimo", 0.0, value=0.0, step=100.0, key="intel_v3_min_award"))
        max_award = float(st.number_input("Monto adjudicado maximo (0 = sin limite)", 0.0, value=0.0, step=1_000.0, key="intel_v3_max_award"))
    with st.expander("Demanda, competencia y disponibilidad", expanded=False):
        min_acts = int(st.number_input("Actos minimos", 0, value=1, step=1, key="intel_v3_min_acts"))
        min_entities = int(st.number_input("Entidades minimas", 0, value=0, step=1, key="intel_v3_min_entities"))
        min_active_months = int(st.number_input("Meses activos minimos", 0, value=0, step=1, key="intel_v3_min_active_months"))
        max_participants = float(st.number_input("Participantes promedio max. (0 = libre)", 0.0, value=0.0, step=0.25, key="intel_v3_max_participants"))
        availability_mode = st.selectbox(
            "Disponibilidad comercial",
            ["Todas", "Favoritos", "Catálogo Foyomed", "Proveedor en catálogo", "Proveedor contactable"],
            key="intel_v3_availability",
        )
    score_preset, weights = _score_weights()

availability_fichas: tuple[str, ...] = ()
availability_modified = ""
if availability_mode in {"Favoritos", "Catálogo Foyomed"}:
    kind = "favoritos" if availability_mode == "Favoritos" else "foyomed"
    configured_id = (
        _config_value("DRIVE_PROSPECCION_RIR_FAVORITOS_FILE_ID")
        if kind == "favoritos"
        else _config_value("DRIVE_PROSPECCION_RIR_FOYOMED_FILE_ID")
    )
    try:
        availability_fichas, availability_modified = _drive_ficha_list(kind, configured_id)
    except Exception as exc:
        st.sidebar.error(f"No se pudo leer {availability_mode}: {exc}")
    if not availability_fichas:
        st.sidebar.warning(f"{availability_mode} no contiene fichas disponibles.")
        availability_fichas = ("__sin_fichas__",)
catalog_only = availability_mode == "Proveedor en catálogo"
contactable_only = availability_mode == "Proveedor contactable"

filters = AnalyticsFilters(
    start_date=start_date,
    end_date=end_date,
    date_basis=date_labels[date_basis_label],
    detection_profile=profile_labels_reverse[profile_label],
    states=selected_states,
    entities=selected_entities,
    areas=selected_areas,
    product_types=selected_product_types,
    fichas=availability_fichas,
    ct_status=ct_status,
    rs_status=rs_status,
    search_groups=split_search_groups(search_raw),
    search_mode=search_mode,
    min_reference_amount=min_reference,
    max_reference_amount=max_reference,
    min_award_amount=min_award,
    max_award_amount=max_award,
    min_acts=min_acts,
    min_entities=min_entities,
    min_active_months=min_active_months,
    max_average_participants=max_participants,
    catalog_only=catalog_only,
    contactable_only=contactable_only,
)

with st.spinner("Calculando métricas globales del periodo..."):
    master = score_opportunities(_master_data(filters, repo), weights)

with st.expander("Decisión final", expanded=False):
    c1, c2 = st.columns(2)
    min_score = float(c1.number_input("Score mínimo", 0.0, 100.0, 0.0, 1.0, key="intel_v3_min_score"))
    recommendation_options = sorted(master["recomendacion"].dropna().astype(str).unique().tolist()) if not master.empty else []
    selected_recommendations = c2.multiselect("Recomendaciones", recommendation_options, key="intel_v3_recommendations")

filtered_master = apply_master_filters(
    master,
    min_score=min_score,
    recommendations=selected_recommendations,
)

saved_view_payload: dict[str, object] = filters.as_payload()
saved_view_payload.update(
    {
        "score_preset": score_preset,
        "score_weights": dict(weights),
        "score_minimo_oportunidad": min_score,
        "recomendaciones": list(selected_recommendations),
        "disponibilidad": availability_mode,
        "disponibilidad_actualizada": availability_modified,
    }
)
_render_saved_views(saved_view_payload)

metric_cols = st.columns(5)
metric_cols[0].metric("Fichas evaluadas", f"{len(filtered_master):,}")
metric_cols[1].metric("Actos vinculados", f"{_safe_int(filtered_master.get('actos', pd.Series(dtype=float)).sum()):,}")
metric_cols[2].metric("Mercado referencial", _money(filtered_master.get("monto_referencia", pd.Series(dtype=float)).sum()))
metric_cols[3].metric("Adjudicado confirmado", _money(filtered_master.get("monto_adjudicado", pd.Series(dtype=float)).sum()))
metric_cols[4].metric("Score promedio", f"{float(filtered_master.get('score_oportunidad', pd.Series(dtype=float)).mean() or 0):,.1f}")

if filtered_master.empty:
    st.warning("Ninguna ficha cumple todos los filtros. Amplía el periodo o relaja las condiciones del ranking.")
    st.stop()

tab_master, tab_trends, tab_competition, tab_providers, tab_study = st.tabs(
    ["Oportunidades", "Tendencias", "Competencia", "Proveedores", "Estudio profundo"]
)
with tab_master:
    _render_master_table(filtered_master)
with tab_trends:
    _render_trends(filtered_master, filters, repo)
with tab_competition:
    _render_competition(filtered_master)
with tab_providers:
    _render_provider_detail(filtered_master, filters, repo)
with tab_study:
    _render_deep_study(filtered_master, filters, score_preset)
