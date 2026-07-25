"""Panel liviano para el seguimiento de Cotizaciones en Línea vencidas."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text


TABLE_NAME = "cl_cotizaciones"
STATE_LABELS = {
    "cerrada_sin_propuestas": "Sin propuestas (confirmado)",
    "cerrada_con_propuestas": "Con propuestas",
    "cerrada_pendiente_publicacion": "Pendiente de publicación",
    "cerrada_pendiente_verificacion": "Pendiente de verificación",
    "error_verificacion": "Verificación pendiente por error",
    "abierta": "Abierta",
    "continuada_a_otro_proceso": "Continuó a otro proceso",
    "desierta_oficial": "Desierta oficial",
}


def _load_rows(
    *,
    backend: str,
    db_url: str,
    db_path: str,
) -> tuple[pd.DataFrame, str]:
    columns = """
        numero_cl, enlace, titulo, entidad, unidad_solicitante,
        precio_referencia, fecha_publicacion, fecha_cierre,
        fichas_detectadas, estado_derivado, proposal_count,
        proponents_json, evidence_type, evidence_url, confidence,
        last_check_at, next_check_at, last_error, updated_at
    """
    query = f"""
        SELECT {columns}
        FROM {TABLE_NAME}
        WHERE estado_derivado <> 'abierta'
        ORDER BY COALESCE(fecha_cierre, updated_at) DESC
        LIMIT 5000
    """

    if backend == "postgres" and db_url:
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 15},
        )
        try:
            if TABLE_NAME not in inspect(engine).get_table_names():
                return pd.DataFrame(), "missing"
            with engine.connect() as connection:
                return pd.read_sql_query(text(query), connection), "ok"
        finally:
            engine.dispose()

    path = Path(db_path) if db_path else None
    if not path or not path.exists():
        return pd.DataFrame(), "unavailable"
    connection = sqlite3.connect(path, timeout=30)
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_NAME,),
        ).fetchone()
        if not table:
            return pd.DataFrame(), "missing"
        return pd.read_sql_query(query, connection), "ok"
    finally:
        connection.close()


def _prepare_rows(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data
    result = data.copy()
    result["Estado"] = result["estado_derivado"].map(STATE_LABELS).fillna(
        result["estado_derivado"]
    )
    result["Proponentes"] = pd.to_numeric(
        result["proposal_count"], errors="coerce"
    ).astype("Int64")
    result["Monto referencia"] = pd.to_numeric(
        result["precio_referencia"], errors="coerce"
    ).fillna(0.0)
    result["Cierre"] = pd.to_datetime(
        result["fecha_cierre"], errors="coerce", utc=True
    ).dt.tz_convert("America/Panama")
    result["Verificada"] = pd.to_datetime(
        result["last_check_at"], errors="coerce", utc=True
    ).dt.tz_convert("America/Panama")
    return result


def render_cl_opportunities_panel(
    *,
    backend: str,
    db_url: str = "",
    db_path: str = "",
    key_prefix: str = "pc_cl_opportunities",
) -> None:
    st.caption(
        "Archivo operativo de CL vencidas. Una CL solo aparece como “sin "
        "propuestas” cuando el cuadro oficial confirmó cero participantes."
    )
    enabled = st.toggle(
        "Cargar seguimiento de CL",
        value=False,
        key=f"{key_prefix}_enabled",
        help="Se mantiene apagado para no añadir tiempo de carga a Panamá Compra.",
    )
    if not enabled:
        return

    with st.spinner("Consultando cierres de Cotizaciones en Línea..."):
        try:
            raw, status = _load_rows(
                backend=backend,
                db_url=db_url,
                db_path=db_path,
            )
        except Exception as exc:
            st.error(f"No se pudo consultar el seguimiento de CL: {exc}")
            return

    if status == "missing":
        st.info(
            "La tabla de seguimiento se creará automáticamente en la próxima "
            "corrida de Cotizaciones Abiertas (clv)."
        )
        return
    if status == "unavailable":
        st.info("No hay una fuente de base de datos disponible para este panel.")
        return

    data = _prepare_rows(raw)
    if data.empty:
        st.info("Todavía no hay CL vencidas registradas.")
        return

    all_states = sorted(data["estado_derivado"].dropna().astype(str).unique())
    default_states = [
        state for state in ("cerrada_sin_propuestas",) if state in all_states
    ]
    selected_states = st.multiselect(
        "Estados",
        options=all_states,
        default=default_states or all_states,
        format_func=lambda value: STATE_LABELS.get(value, value),
        key=f"{key_prefix}_states",
    )
    search = st.text_input(
        "Buscar CL, entidad, título o ficha",
        key=f"{key_prefix}_search",
        placeholder="Ej. hospital, 43358, equipo...",
    ).strip()

    filtered = data[
        data["estado_derivado"].isin(selected_states)
        if selected_states
        else pd.Series(False, index=data.index)
    ].copy()
    if search:
        needle = search.casefold()
        searchable = (
            filtered[
                [
                    "numero_cl",
                    "titulo",
                    "entidad",
                    "unidad_solicitante",
                    "fichas_detectadas",
                ]
            ]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.casefold()
        )
        filtered = filtered[searchable.str.contains(needle, regex=False)]

    confirmed_zero = int(
        (data["estado_derivado"] == "cerrada_sin_propuestas").sum()
    )
    pending = int(data["estado_derivado"].isin(
        {
            "cerrada_pendiente_publicacion",
            "cerrada_pendiente_verificacion",
            "error_verificacion",
        }
    ).sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Sin propuestas confirmadas", f"{confirmed_zero:,}")
    c2.metric("Pendientes de verificación", f"{pending:,}")
    c3.metric("Resultados visibles", f"{len(filtered):,}")

    display_columns = [
        "numero_cl",
        "titulo",
        "entidad",
        "Monto referencia",
        "Cierre",
        "fichas_detectadas",
        "Proponentes",
        "Estado",
        "evidence_url",
        "enlace",
        "Verificada",
    ]
    display = filtered[display_columns].rename(
        columns={
            "numero_cl": "Número CL",
            "titulo": "Título",
            "entidad": "Entidad",
            "fichas_detectadas": "Fichas",
            "evidence_url": "Evidencia oficial",
            "enlace": "CL",
        }
    )
    st.dataframe(
        display,
        width="stretch",
        height=min(850, max(260, 42 + len(display) * 35)),
        hide_index=True,
        column_config={
            "Monto referencia": st.column_config.NumberColumn(
                "Monto referencia", format="$ %.2f"
            ),
            "Evidencia oficial": st.column_config.LinkColumn(
                "Evidencia oficial", display_text="Cuadro"
            ),
            "CL": st.column_config.LinkColumn("CL", display_text="Abrir"),
            "Cierre": st.column_config.DatetimeColumn(
                "Cierre", format="DD/MM/YYYY HH:mm"
            ),
            "Verificada": st.column_config.DatetimeColumn(
                "Verificada", format="DD/MM/YYYY HH:mm"
            ),
        },
        key=f"{key_prefix}_table",
    )
    st.download_button(
        "Descargar resultados CSV",
        data=display.to_csv(index=False).encode("utf-8-sig"),
        file_name="cl_seguimiento.csv",
        mime="text/csv",
        key=f"{key_prefix}_download",
    )
