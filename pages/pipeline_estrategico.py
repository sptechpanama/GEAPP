from __future__ import annotations

from datetime import datetime
import os
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from services.access_control import current_username, require_page_access
from services.auth_drive import get_drive_delegated
from services.pipeline_drive import PipelineDriveStorage
from services.pipeline_estrategico import (
    PipelineError,
    PipelineFilters,
    PipelineRepository,
    PipelineRuleError,
    ROUTES,
    clean_text,
)
from services.pipeline_sheets import PipelineSheetsMirror
from services.pipeline_trello_import import (
    TrelloImportError,
    import_trello_board,
    load_trello_export,
    preview_trello_export,
)
from sheets import get_client
from ui.theme import apply_global_theme


PAGE_PATH = "pages/pipeline_estrategico.py"
FLASH_KEY = "pipeline_flash"


st.set_page_config(
    page_title="Pipeline Estratégico",
    page_icon="🎯",
    layout="wide",
)
apply_global_theme()
username = require_page_access(PAGE_PATH)

st.markdown(
    """
<style>
.block-container { max-width: 1800px; padding-top: .8rem; }
.pipeline-title { margin-bottom: .15rem; }
.pipeline-subtitle { color: #9fb2c7; margin-bottom: .8rem; }
.pipeline-route-title { font-size: .92rem; font-weight: 800; line-height: 1.25; }
.pipeline-card-id { color: #7dd3fc; font-size: .76rem; font-weight: 800; letter-spacing: .02em; }
.pipeline-card-title { font-size: .9rem; font-weight: 750; line-height: 1.3; min-height: 2.2rem; }
.pipeline-card-meta { color: #aebdd0; font-size: .74rem; line-height: 1.35; }
.pipeline-card-description { color: #c9d4e4; font-size: .75rem; line-height: 1.35; }
[data-testid="stMetricValue"] { font-size: 1.75rem; }
</style>
""",
    unsafe_allow_html=True,
)


def _config_value(key: str, default: str = "") -> str:
    env_value = clean_text(os.getenv(key))
    if env_value:
        return env_value
    try:
        app_cfg = st.secrets.get("app", {})
        value = clean_text(app_cfg.get(key, ""))
        if value:
            return value
    except Exception:
        pass
    try:
        return clean_text(st.secrets.get(key, default))
    except Exception:
        return clean_text(default)


@st.cache_resource(show_spinner=False)
def _repository(database_url: str) -> PipelineRepository:
    return PipelineRepository.connect(database_url or None)


def _sheet_ids() -> list[str]:
    values = [_config_value("PIPELINE_SHEET_ID"), _config_value("SHEET_ID")]
    return list(dict.fromkeys(value for value in values if value))


def _sync_sheets(repo: PipelineRepository, *, limit: int = 300) -> dict[str, Any]:
    client, _ = get_client()
    mirror = PipelineSheetsMirror(
        client=client,
        sheet_ids=_sheet_ids(),
        repository=repo,
    )
    return mirror.sync_pending(limit=limit)


def _set_flash(level: str, message: str) -> None:
    st.session_state[FLASH_KEY] = (level, message)


def _show_flash() -> None:
    payload = st.session_state.pop(FLASH_KEY, None)
    if not payload:
        return
    level, message = payload
    getattr(st, level if level in {"success", "warning", "error", "info"} else "info")(
        message
    )


def _after_write(repo: PipelineRepository, message: str) -> None:
    try:
        result = _sync_sheets(repo)
        if result["errors"]:
            message += (
                " El cambio quedo seguro en Supabase; la replica a Sheets queda "
                "pendiente para reintento."
            )
            _set_flash("warning", message)
        else:
            _set_flash("success", message)
    except Exception:
        _set_flash(
            "warning",
            message
            + " El cambio quedo seguro en Supabase; la replica a Sheets queda pendiente para reintento.",
        )
    st.rerun()


def _display_date(value: Any) -> str:
    raw = clean_text(value)
    if not raw:
        return "Sin fecha"
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%d/%m/%Y")
    except ValueError:
        return raw


database_url = _config_value("SUPABASE_DB_URL") or _config_value("DATABASE_URL")
try:
    repo = _repository(database_url)
except Exception as exc:
    st.error(
        "No fue posible abrir la base del Pipeline Estratégico. Verifica SUPABASE_DB_URL "
        "y que el proyecto de Supabase esté activo."
    )
    with st.expander("Detalle tecnico"):
        st.code(str(exc))
    st.stop()


def _render_create_card(repository: PipelineRepository) -> None:
    with st.expander("＋ Nueva oportunidad", expanded=False):
        with st.form("pipeline_create_card", clear_on_submit=True):
            route_key = st.selectbox(
                "Categoría del pipeline",
                options=list(ROUTES),
                format_func=lambda value: ROUTES[value].label,
            )
            col1, col2, col3 = st.columns(3)
            with col1:
                ficha = st.text_input(
                    "Ficha técnica",
                    help="Si aún no existe, deja el campo vacío y usa un producto provisional claro.",
                )
                proveedor = st.text_input("Proveedor *")
            with col2:
                nombre_ficha = st.text_input("Nombre de ficha")
                marca = st.text_input("Marca *")
            with col3:
                producto = st.text_input("Producto / identificador provisional *")
                responsable = st.text_input("Responsable", value=current_username())
            descripcion = st.text_area("Descripción", height=80)
            col4, col5 = st.columns(2)
            with col4:
                prioridad = st.select_slider(
                    "Prioridad", options=[1, 2, 3, 4, 5], value=3,
                    help="1 es la prioridad mas alta.",
                )
            with col5:
                fecha_objetivo = st.text_input(
                    "Fecha objetivo (AAAA-MM-DD)", placeholder="2026-10-15"
                )

            st.caption("Contacto principal (opcional, disponible desde esta primera versión)")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                contacto_nombre = st.text_input("Nombre del contacto")
            with c2:
                contacto_email = st.text_input("Correo")
            with c3:
                contacto_telefono = st.text_input("Teléfono")
            with c4:
                contacto_chat = st.text_input("WhatsApp / WeChat")
            submitted = st.form_submit_button("Crear tarjeta", use_container_width=True)
        if submitted:
            try:
                card = repository.create_card(
                    ficha=ficha,
                    nombre_ficha=nombre_ficha,
                    producto=producto,
                    proveedor=proveedor,
                    marca=marca,
                    descripcion=descripcion,
                    route_key=route_key,
                    actor=username,
                    responsable=responsable,
                    prioridad=prioridad,
                    fecha_objetivo=fecha_objetivo,
                )
                if any(
                    clean_text(value)
                    for value in (
                        contacto_nombre,
                        contacto_email,
                        contacto_telefono,
                        contacto_chat,
                    )
                ):
                    repository.add_contact(
                        card["id"],
                        actor=username,
                        nombre=contacto_nombre,
                        email=contacto_email,
                        telefono=contacto_telefono,
                        whatsapp_wechat=contacto_chat,
                        es_principal=True,
                    )
                _after_write(repository, "Oportunidad creada correctamente.")
            except PipelineError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"No se pudo crear la oportunidad: {exc}")


def _render_filters(repository: PipelineRepository) -> PipelineFilters:
    options = repository.options()
    with st.expander("Filtros", expanded=False):
        c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
        with c1:
            routes = st.multiselect(
                "Categorías",
                options=list(ROUTES),
                format_func=lambda value: ROUTES[value].short_label,
            )
        with c2:
            providers = st.multiselect("Proveedores", options=options["providers"])
        with c3:
            fichas = st.multiselect("Fichas", options=options["fichas"])
        with c4:
            states = st.multiselect("Estados", options=options["states"])
        search = st.text_input(
            "Busqueda general",
            placeholder="Ficha, producto, marca, proveedor, responsable...",
        )
    return PipelineFilters(
        routes=tuple(routes),
        providers=tuple(providers),
        fichas=tuple(fichas),
        states=tuple(states),
        search=search,
    )


def _render_analytics(repository: PipelineRepository, filters: PipelineFilters) -> None:
    analytics = repository.analytics(filters)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Oportunidades", f"{analytics['total_cards']:,}")
    c2.metric("Avance global", f"{analytics['average_progress']:.1f}%")
    c3.metric("Listas para licitar", f"{analytics['ready_to_bid']:,}")
    c4.metric("Cerradas con éxito", f"{analytics['completed']:,}")
    route_frame = pd.DataFrame(analytics["routes"])
    funnel_frame = pd.DataFrame(analytics["funnel"])
    if route_frame.empty:
        st.info("Aún no hay oportunidades para los filtros seleccionados.")
        return
    left, right = st.columns(2)
    with left:
        route_chart = (
            alt.Chart(route_frame)
            .mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5)
            .encode(
                y=alt.Y("route:N", title=None, sort=None),
                x=alt.X("tarjetas:Q", title="Tarjetas"),
                color=alt.Color("avance_promedio:Q", title="Avance %", scale=alt.Scale(scheme="blues")),
                tooltip=["route", "tarjetas", "avance_promedio"],
            )
            .properties(height=190, title="Distribución por categoría")
        )
        st.altair_chart(route_chart, use_container_width=True)
    with right:
        route_filter = st.selectbox(
            "Embudo de controles",
            options=route_frame["route_key"].tolist(),
            format_func=lambda value: ROUTES[value].short_label,
            key="pipeline_funnel_route",
            label_visibility="collapsed",
        )
        selected = funnel_frame[funnel_frame["route_key"] == route_filter]
        funnel_chart = (
            alt.Chart(selected)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#22c55e")
            .encode(
                y=alt.Y("control:N", title=None, sort=alt.SortField("position")),
                x=alt.X("porcentaje:Q", title="Tarjetas que completaron el control (%)", scale=alt.Scale(domain=[0, 100])),
                tooltip=["control", "completadas", "total_tarjetas", "porcentaje"],
            )
            .properties(height=190)
        )
        st.altair_chart(funnel_chart, use_container_width=True)


def _render_card_summary(card: dict[str, Any], repository: PipelineRepository) -> None:
    ficha = clean_text(card.get("ficha")) or "POR CREAR"
    title = clean_text(card.get("producto")) or clean_text(card.get("nombre_ficha")) or "Sin producto"
    st.markdown(f"<div class='pipeline-card-id'>{ficha}</div>", unsafe_allow_html=True)
    st.markdown(f"**{title}**")
    st.caption(f"{clean_text(card.get('proveedor'))} · {clean_text(card.get('marca'))}")
    if clean_text(card.get("source")) == "duplicate":
        st.caption("Copia editable")
    st.progress(float(card.get("progress") or 0.0) / 100.0, text=f"{float(card.get('progress') or 0):.0f}%")
    meta = []
    if clean_text(card.get("responsable")):
        meta.append(f"Responsable: {clean_text(card.get('responsable'))}")
    if clean_text(card.get("fecha_objetivo")):
        meta.append(f"Meta: {_display_date(card.get('fecha_objetivo'))}")
    if meta:
        st.caption(" · ".join(meta))
    if st.button("Abrir", key=f"pipeline_open_{card['id']}", use_container_width=True):
        _card_dialog(card["id"], repository)


def _render_checklist(card: dict[str, Any], repository: PipelineRepository) -> None:
    checkpoints = repository.checkpoints(card["id"])
    completed = sum(1 for item in checkpoints if bool(item.get("completed")))
    st.progress(completed / len(checkpoints) if checkpoints else 0.0, text=f"{completed}/{len(checkpoints)} controles")
    pending = st.session_state.get("pipeline_pending_reset")
    if pending and pending.get("card_id") == card["id"]:
        st.warning("Al reabrir este control también se reiniciarán todos los posteriores.")
        yes, no = st.columns(2)
        if yes.button("Confirmar reinicio", key=f"confirm_reset_{card['id']}", use_container_width=True):
            repository.set_checkpoint(
                card_id=card["id"],
                checkpoint_key=pending["checkpoint_key"],
                completed=False,
                actor=username,
                reset_downstream=True,
            )
            st.session_state.pop("pipeline_pending_reset", None)
            _after_write(repository, "Lista de comprobacion actualizada.")
        if no.button("Cancelar", key=f"cancel_reset_{card['id']}", use_container_width=True):
            st.session_state.pop("pipeline_pending_reset", None)
            st.rerun()
    prior_complete = True
    for checkpoint in checkpoints:
        current = bool(checkpoint.get("completed"))
        widget_key = f"pipeline_cp_{checkpoint['id']}"
        value = st.checkbox(
            checkpoint["label"],
            value=current,
            disabled=(not current and not prior_complete),
            key=widget_key,
        )
        if value != current:
            try:
                repository.set_checkpoint(
                    card_id=card["id"],
                    checkpoint_key=checkpoint["checkpoint_key"],
                    completed=value,
                    actor=username,
                )
                st.session_state.pop(widget_key, None)
                _after_write(repository, "Lista de comprobacion actualizada.")
            except PipelineRuleError as exc:
                st.session_state.pop(widget_key, None)
                if exc.requires_confirmation:
                    st.session_state["pipeline_pending_reset"] = {
                        "card_id": card["id"],
                        "checkpoint_key": checkpoint["checkpoint_key"],
                    }
                    st.rerun()
                st.error(str(exc))
        prior_complete = prior_complete and current


def _render_card_data(card: dict[str, Any], repository: PipelineRepository) -> None:
    with st.form(f"pipeline_edit_{card['id']}"):
        c1, c2, c3 = st.columns(3)
        with c1:
            ficha = st.text_input("Ficha técnica", value=clean_text(card.get("ficha")))
            proveedor = st.text_input("Proveedor", value=clean_text(card.get("proveedor")))
            estado = st.selectbox(
                "Estado",
                ["activo", "en espera", "bloqueado", "cerrado"],
                index=["activo", "en espera", "bloqueado", "cerrado"].index(card.get("estado"))
                if card.get("estado") in ["activo", "en espera", "bloqueado", "cerrado"]
                else 0,
            )
        with c2:
            nombre_ficha = st.text_input("Nombre de ficha", value=clean_text(card.get("nombre_ficha")))
            marca = st.text_input("Marca", value=clean_text(card.get("marca")))
            responsable = st.text_input("Responsable", value=clean_text(card.get("responsable")))
        with c3:
            producto = st.text_input("Producto", value=clean_text(card.get("producto")))
            prioridad = st.select_slider(
                "Prioridad", [1, 2, 3, 4, 5], value=int(card.get("prioridad") or 3)
            )
            fecha_objetivo = st.text_input(
                "Fecha objetivo", value=clean_text(card.get("fecha_objetivo"))
            )
        descripcion = st.text_area("Descripción", value=clean_text(card.get("descripcion")), height=100)
        saved = st.form_submit_button("Guardar cambios", use_container_width=True)
    if saved:
        try:
            repository.update_card(
                card["id"],
                actor=username,
                expected_version=int(card.get("version") or 1),
                ficha=ficha,
                nombre_ficha=nombre_ficha,
                producto=producto,
                proveedor=proveedor,
                marca=marca,
                descripcion=descripcion,
                estado=estado,
                responsable=responsable,
                prioridad=prioridad,
                fecha_objetivo=fecha_objetivo,
            )
            _after_write(repository, "Datos de la oportunidad actualizados.")
        except PipelineError as exc:
            st.error(str(exc))

    st.divider()
    new_route = st.selectbox(
        "Mover a otra categoría",
        options=list(ROUTES),
        index=list(ROUTES).index(card["route_key"]),
        format_func=lambda value: ROUTES[value].label,
        key=f"pipeline_route_{card['id']}",
    )
    if new_route != card["route_key"]:
        confirm = st.checkbox(
            "Confirmo reiniciar la lista de comprobación si ya tiene avances.",
            key=f"pipeline_route_confirm_{card['id']}",
        )
        if st.button("Aplicar cambio de categoría", key=f"route_apply_{card['id']}"):
            try:
                repository.change_route(
                    card["id"],
                    route_key=new_route,
                    actor=username,
                    confirm_reset=confirm,
                )
                _after_write(repository, "Categoría actualizada.")
            except PipelineRuleError as exc:
                st.error(str(exc))
    st.divider()
    st.markdown("##### Acciones de la tarjeta")
    st.caption(
        "Duplicar conserva datos, avance, contactos y enlaces documentales; "
        "no crea copias físicas de los archivos de Drive."
    )
    if st.button(
        "Duplicar tarjeta",
        key=f"duplicate_card_{card['id']}",
        use_container_width=True,
    ):
        try:
            repository.duplicate_card(card["id"], actor=username)
            _after_write(repository, "Tarjeta duplicada correctamente.")
        except PipelineError as exc:
            st.error(str(exc))

    with st.expander("Eliminar tarjeta", expanded=False):
        st.warning(
            "La tarjeta desaparecerá del tablero, pero se conservarán su historial, "
            "contactos y documentos para recuperación y auditoría."
        )
        delete_confirmed = st.checkbox(
            "Confirmo que deseo eliminar esta tarjeta del tablero.",
            key=f"delete_card_confirm_{card['id']}",
        )
        if st.button(
            "Eliminar del tablero",
            key=f"delete_card_{card['id']}",
            disabled=not delete_confirmed,
            use_container_width=True,
        ):
            try:
                repository.archive_card(
                    card["id"],
                    actor=username,
                    expected_version=int(card.get("version") or 1),
                )
                _after_write(repository, "Tarjeta eliminada del tablero.")
            except PipelineError as exc:
                st.error(str(exc))


def _render_contacts(card: dict[str, Any], repository: PipelineRepository) -> None:
    contacts = repository.contacts(card["id"])
    if not contacts:
        st.caption("Aún no hay contactos registrados.")
    for contact in contacts:
        with st.container(border=True):
            c1, c2 = st.columns([5, 1])
            with c1:
                principal = " · Principal" if bool(contact.get("es_principal")) else ""
                st.markdown(f"**{clean_text(contact.get('nombre')) or 'Contacto'}**{principal}")
                details = [
                    clean_text(contact.get("cargo")),
                    clean_text(contact.get("email")),
                    clean_text(contact.get("telefono")),
                    clean_text(contact.get("whatsapp_wechat")),
                    clean_text(contact.get("pais")),
                ]
                st.caption(" · ".join(value for value in details if value))
            with c2:
                if st.button("Quitar", key=f"remove_contact_{contact['id']}"):
                    repository.archive_contact(contact["id"], actor=username)
                    _after_write(repository, "Contacto retirado de la tarjeta.")
    with st.form(f"pipeline_contact_{card['id']}", clear_on_submit=True):
        st.markdown("##### Agregar contacto")
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Nombre")
            email = st.text_input("Correo electrónico")
            whatsapp = st.text_input("WhatsApp / WeChat")
            country = st.text_input("País")
        with c2:
            role = st.text_input("Cargo")
            phone = st.text_input("Teléfono")
            channel = st.selectbox("Canal preferido", ["", "Correo", "WhatsApp", "WeChat", "Teléfono"])
            primary = st.checkbox("Contacto principal")
        notes = st.text_area("Notas del contacto", height=70)
        submitted = st.form_submit_button("Guardar contacto", use_container_width=True)
    if submitted:
        try:
            repository.add_contact(
                card["id"],
                actor=username,
                nombre=name,
                cargo=role,
                email=email,
                telefono=phone,
                whatsapp_wechat=whatsapp,
                pais=country,
                canal_preferido=channel,
                notas=notes,
                es_principal=primary,
            )
            _after_write(repository, "Contacto agregado.")
        except PipelineError as exc:
            st.error(str(exc))


def _drive_storage() -> PipelineDriveStorage:
    drive = get_drive_delegated()
    return PipelineDriveStorage.from_config(
        drive,
        pipeline_folder_id=_config_value("DRIVE_PIPELINE_FOLDER_ID"),
        parent_folder_id=_config_value("DRIVE_TOPS_FOLDER_ID"),
    )


def _render_documents(card: dict[str, Any], repository: PipelineRepository) -> None:
    documents = repository.documents(card["id"])
    if not documents:
        st.caption("Aún no hay documentos adjuntos.")
    for document in documents:
        with st.container(border=True):
            c1, c2 = st.columns([5, 1])
            with c1:
                name = clean_text(document.get("file_name")) or "Documento"
                url = clean_text(document.get("file_url"))
                if url:
                    st.link_button(name, url)
                else:
                    st.markdown(f"**{name}**")
                st.caption(
                    " · ".join(
                        value
                        for value in (
                            clean_text(document.get("document_type")),
                            clean_text(document.get("storage_provider")),
                            clean_text(document.get("uploaded_by")),
                        )
                        if value
                    )
                )
            with c2:
                if st.button("Quitar", key=f"remove_document_{document['id']}"):
                    repository.archive_document(document["id"], actor=username)
                    _after_write(
                        repository,
                        "Documento retirado de la tarjeta. El archivo permanece seguro en Drive.",
                    )
    st.markdown("##### Adjuntar a Google Drive")
    uploaded = st.file_uploader(
        "Documento",
        key=f"pipeline_upload_{card['id']}",
        help="El archivo se guarda en una carpeta propia de esta tarjeta.",
    )
    c1, c2 = st.columns(2)
    with c1:
        document_type = st.text_input(
            "Tipo de documento", key=f"pipeline_doc_type_{card['id']}"
        )
    with c2:
        document_description = st.text_input(
            "Descripción", key=f"pipeline_doc_desc_{card['id']}"
        )
    if st.button(
        "Subir documento",
        key=f"pipeline_doc_upload_button_{card['id']}",
        disabled=uploaded is None,
        use_container_width=True,
    ):
        try:
            storage = _drive_storage()
            result = storage.upload(
                card=card,
                file_name=uploaded.name,
                data=uploaded.getvalue(),
                mime_type=uploaded.type or "application/octet-stream",
            )
            repository.add_document(
                card["id"],
                actor=username,
                file_name=result.get("name", uploaded.name),
                file_url=result.get("webViewLink", ""),
                drive_file_id=result.get("id", ""),
                document_type=document_type,
                mime_type=result.get("mimeType", uploaded.type or ""),
                size_bytes=int(result.get("size") or len(uploaded.getvalue())),
                descripcion=document_description,
                storage_provider="drive",
            )
            _after_write(repository, "Documento guardado en Google Drive.")
        except Exception as exc:
            st.error(f"No se pudo adjuntar el documento: {exc}")


def _render_activity(card: dict[str, Any], repository: PipelineRepository) -> None:
    rows = repository.activities(card["id"], limit=150)
    if not rows:
        st.caption("Sin movimientos registrados.")
        return
    frame = pd.DataFrame(rows)
    frame = frame.rename(
        columns={
            "created_at": "Fecha",
            "actor": "Usuario",
            "action": "Acción",
            "field_name": "Campo",
            "old_value": "Antes",
            "new_value": "Después",
        }
    )
    st.dataframe(
        frame[["Fecha", "Usuario", "Acción", "Campo", "Antes", "Después"]],
        use_container_width=True,
        hide_index=True,
        height=420,
    )


@st.dialog("Detalle de la oportunidad", width="large")
def _card_dialog(card_id: str, repository: PipelineRepository) -> None:
    try:
        card = repository.get_card(card_id)
    except PipelineError as exc:
        st.error(str(exc))
        return
    route = ROUTES[card["route_key"]]
    st.caption(route.label)
    st.subheader(clean_text(card.get("producto")) or clean_text(card.get("nombre_ficha")) or "Oportunidad")
    st.caption(
        f"Ficha {clean_text(card.get('ficha')) or 'por crear'} · "
        f"{clean_text(card.get('proveedor'))} · {clean_text(card.get('marca'))}"
    )
    tab_check, tab_data, tab_contacts, tab_docs, tab_activity = st.tabs(
        ["Seguimiento", "Datos", "Contactos", "Documentos", "Actividad"]
    )
    with tab_check:
        _render_checklist(card, repository)
    with tab_data:
        _render_card_data(card, repository)
    with tab_contacts:
        _render_contacts(card, repository)
    with tab_docs:
        _render_documents(card, repository)
    with tab_activity:
        _render_activity(card, repository)


def _render_board(cards: list[dict[str, Any]], repository: PipelineRepository) -> None:
    columns = st.columns(5, gap="small")
    for column, (route_key, route) in zip(columns, ROUTES.items()):
        route_cards = [card for card in cards if card["route_key"] == route_key]
        with column:
            with st.container(border=True):
                st.markdown(f"**{route.short_label}** · {len(route_cards)}")
                st.caption(route.description)
            with st.container(height=690, border=False):
                if not route_cards:
                    st.caption("Sin tarjetas")
                for card in route_cards:
                    with st.container(border=True):
                        _render_card_summary(card, repository)


def _render_group_tables(cards: list[dict[str, Any]]) -> None:
    provider_tab, ficha_tab = st.tabs(["Vista por proveedor", "Vista por ficha"])
    frame = pd.DataFrame(cards)
    with provider_tab:
        if frame.empty:
            st.caption("Sin datos.")
        else:
            provider = (
                frame.groupby("proveedor", dropna=False)
                .agg(
                    tarjetas=("id", "count"),
                    fichas=("ficha", lambda values: len({value for value in values if clean_text(value)})),
                    marcas=("marca", lambda values: len({value for value in values if clean_text(value)})),
                    avance_promedio=("progress", "mean"),
                )
                .reset_index()
                .sort_values(["tarjetas", "avance_promedio"], ascending=[False, False])
            )
            provider["avance_promedio"] = provider["avance_promedio"].round(1)
            st.dataframe(provider, use_container_width=True, hide_index=True)
    with ficha_tab:
        if frame.empty:
            st.caption("Sin datos.")
        else:
            ficha = (
                frame.groupby(["ficha", "nombre_ficha"], dropna=False)
                .agg(
                    tarjetas=("id", "count"),
                    proveedores=("proveedor", lambda values: len({value for value in values if clean_text(value)})),
                    marcas=("marca", lambda values: len({value for value in values if clean_text(value)})),
                    avance_promedio=("progress", "mean"),
                )
                .reset_index()
                .sort_values(["tarjetas", "avance_promedio"], ascending=[False, False])
            )
            ficha["avance_promedio"] = ficha["avance_promedio"].round(1)
            st.dataframe(ficha, use_container_width=True, hide_index=True)


def _render_integrations(repository: PipelineRepository) -> None:
    with st.expander("Integraciones y migración", expanded=False):
        counts = repository.outbox_counts()
        st.caption(
            f"Réplica Sheets: {counts['pending']:,} pendientes · {counts['error']:,} con reintento · "
            f"{counts['synced']:,} sincronizados"
        )
        if st.button("Sincronizar ahora con Google Sheets", use_container_width=True):
            try:
                result = _sync_sheets(repository, limit=1000)
                if result["errors"]:
                    st.warning(
                        f"Se sincronizaron {result['synced']} movimientos; "
                        f"{result['errors']} quedan para reintento."
                    )
                else:
                    st.success(f"Sheets actualizado: {result['synced']} movimientos.")
            except Exception as exc:
                st.error(f"No fue posible sincronizar Sheets: {exc}")

        st.divider()
        st.markdown("##### Importación única desde Trello")
        st.caption(
            "Acepta el JSON real exportado por Trello. La importación es idempotente: "
            "volver a cargar el mismo archivo no duplica tarjetas."
        )
        uploaded = st.file_uploader(
            "Exportación JSON de Trello",
            type=["json"],
            key="pipeline_trello_json",
        )
        if uploaded is not None:
            try:
                board = load_trello_export(uploaded.getvalue())
                preview = preview_trello_export(board)
                st.info(
                    f"{preview.board_name}: {preview.eligible_cards} tarjetas con categoría, "
                    "proveedor y marca válidos, "
                    f"{preview.archived_cards} archivadas y {preview.skipped_cards} omitidas "
                    "por lista o datos incompletos."
                )
                route_preview = pd.DataFrame(
                    [
                        {
                            "Categoría": ROUTES[key].label,
                            "Tarjetas": preview.routes.get(key, 0),
                        }
                        for key in ROUTES
                    ]
                )
                st.dataframe(route_preview, use_container_width=True, hide_index=True)
                for warning in preview.warnings[:10]:
                    st.caption(f"Aviso: {warning}")
                if st.button("Importar tarjetas", type="primary", use_container_width=True):
                    result = import_trello_board(repository, board, actor=username)
                    _after_write(
                        repository,
                        f"Migración terminada: {result['created']} creadas, "
                        f"{result['existing']} ya existentes y {result['skipped']} omitidas.",
                    )
            except TrelloImportError as exc:
                st.error(str(exc))


st.markdown("<h1 class='pipeline-title'>🎯 Pipeline Estratégico</h1>", unsafe_allow_html=True)
st.markdown(
    "<div class='pipeline-subtitle'>Seguimiento comercial y regulatorio por ficha, proveedor y marca.</div>",
    unsafe_allow_html=True,
)
st.caption(f"Fuente principal: {repo.source_label} · Usuario: {username}")
_show_flash()
_render_create_card(repo)
filters = _render_filters(repo)
_render_analytics(repo, filters)
cards = repo.list_cards(filters)
pipeline_tab, grouped_tab = st.tabs(["Pipeline", "Análisis cruzado"])
with pipeline_tab:
    _render_board(cards, repo)
with grouped_tab:
    _render_group_tables(cards)
_render_integrations(repo)
