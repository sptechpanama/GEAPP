"""Lazy Streamlit view; network-heavy capture and rendering run in the existing queue."""
from __future__ import annotations

from datetime import datetime
import json
import uuid

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

from services.anestesia_docs import (BASE_KINDS, CATALOGS, COMPANY, EXCLUDED_KINDS, KINDS, PANAMA, PRODUCT_DOCS, REGISTRY_MAX_MONTHS, now_iso, parse_date,
                                    document_kind, select_documents,
                                    prepare_offer_config, review_errors, review_prompt, validate_package)
from services.anestesia_source import (delivery_destination, portal_delivery_term, route, source_is_closed,
                                     public_open_status, needs_live_open_status)
from services.anestesia_health import library_health, certificate_date_suggestions
from services.anestesia_storage import AnestesiaStorage, DRIVE_PARENT, SHEET_ID

ANESTESIA_UI_VERSION = 14
ACTIVE_STATES = {"En cola", "Procesando"}
CATALOG_LABELS = {"K": "Mascarilla 4 · Catálogo K", "C": "Mascarilla 5 · Catálogo C"}
TAX_LABELS = {"exento": "No aplica / exento", "adicional": "Se suma al precio", "incluido": "Ya incluido en el precio"}


@st.cache_data(ttl=20, max_entries=30, show_spinner=False)
def _records(sheet_id, table, _storage):
    return _storage.rows(table)


@st.cache_data(ttl=300, max_entries=8, show_spinner=False)
def _json(file_id, _storage):
    return _storage.json_file(file_id)


@st.cache_data(ttl=300, max_entries=6, show_spinner=False)
def _saved_certificate_dates(file_id, _storage):
    return certificate_date_suggestions(_storage.get_bytes(file_id), 'css')


@st.cache_data(ttl=45, max_entries=8, show_spinner=False)
def _open_status(url):
    return public_open_status(url)


@st.fragment(run_every='60s')
def _source_status(source):
    current = source
    if needs_live_open_status(source):
        try:
            current = {**source, 'official_status': _open_status(source['url'])}
        except Exception:
            # A failed request cannot extend a previously verified open state.
            current = {**source, 'official_status': {}}
    closed, reason = source_is_closed(current)
    if closed:
        st.warning(reason)
    elif reason:
        st.caption(reason)
    st.caption('Presentación publicada: ' + str(source.get('closing') or 'Pendiente de consultar'))


@st.cache_data(ttl=5, max_entries=64, show_spinner=False)
def _live_job(sheet_id, ident, _storage):
    # Separate from the library cache: completion must be visible promptly.
    job = _storage.job(ident)
    if job and job.get("state") in ACTIVE_STATES and job.get("queue_id"):
        request = _storage.queue_request(job["queue_id"])
        if request and request.get("status") in {"error", "failed", "cancelled", "canceled", "done"}:
            # The worker may have completed between the two reads.
            current = _storage.job(ident)
            if current and current.get("queue_id") == job["queue_id"] and current.get("state") in ACTIVE_STATES:
                return {**current, "state": "Error", "detail": request.get("result_error") or
                        "La tarea terminó sin actualizar el expediente. Revisa la ejecución y vuelve a capturar el acto."}
            return current
    return job


def _refresh():
    _records.clear()
    _live_job.clear()
    st.rerun()


@st.fragment(run_every="5s")
def _watch_job(storage, job):
    """Poll only while working; editable offer forms are outside this fragment."""
    try:
        current = _live_job(storage.sheet_id, job["id"], storage)
    except Exception:
        st.warning("No se pudo consultar el estado en este momento. Se reintentará automáticamente en 5 segundos; el expediente se conserva.")
        return
    if not current:
        st.warning("No se encontró el expediente en esta consulta. Se volverá a comprobar en 5 segundos.")
        return
    if current != job:
        _refresh()
    st.info("Actualización automática cada 5 segundos. Al terminar aparecerá el resultado y el siguiente paso. Puedes cambiar de página: la tarea continúa en el orquestador.")


def _enqueue(storage, job, action, actor, **payload):
    if job.get("state") in ACTIVE_STATES:
        raise ValueError("Ya hay una solicitud en curso para este expediente.")
    app = st.secrets.get("app", {})
    execution = storage.enqueue({"action": action, "request_id": job["id"], **payload}, actor=actor,
        python_path=app.get("ANESTESIA_PYTHON", r"C:\Users\rodri\scrapers_repo\.venv\Scripts\python.exe"),
        script_path=app.get("ANESTESIA_WORKER", r"C:\Users\rodri\scrapers_repo\orquestador\anestesia_docs_worker.py"))
    storage.save_job({"id": job["id"], "state": "En cola", "queued_at": now_iso(),
        "queue_id": execution, "detail": "Pendiente del orquestador de tu computadora-servidor."})
    _refresh()


def _table(rows):
    if not rows:
        return
    frame = pd.DataFrame(rows)
    st.dataframe(frame, hide_index=True, use_container_width=True,
        column_config={"enlace": st.column_config.LinkColumn("Documento", display_text="Abrir")})


def _close_library():
    st.session_state.pop("anes_document_dialog_open", None)


@st.dialog("Actualizar documento", width="large", on_dismiss=_close_library)
def _library(storage, actor):
    # Always read current metadata when opening/revisiting the replacement dialog.
    rows = storage.rows("ANESTESIA_DOCUMENTOS")
    catalog = st.session_state.get("anes_health_catalog", "K")
    health = library_health(rows, as_of=datetime.now(PANAMA).date(), catalog=catalog)
    statuses = {r["Documento"]: r["Estado"] for r in health}
    kinds = list(BASE_KINDS)
    priority = {"Vencido": 0, "Falta": 1, "Pendiente de verificar": 2, "Vence hoy": 3, "Vence pronto": 4}
    default = min(kinds, key=lambda k: priority.get(statuses.get(KINDS[k]), 5))
    kind = st.selectbox("Documento a actualizar", kinds, index=kinds.index(default),
        format_func=lambda k: f"{KINDS[k]} · {statuses.get(KINDS[k], 'Pendiente')}", key="anes_update_kind")
    previous = select_documents(rows, [{"kind": kind}], catalog, "").get(kind, {})
    if previous.get("url"):
        st.link_button("Abrir documento actual", previous["url"])
    pending = next((r['Qué falta / comprobación'] for r in health
        if r['Documento'] == KINDS[kind] and r['Estado'] == 'Pendiente de verificar'), '')
    if pending:
        st.warning(pending)
    revise = st.toggle("Solo corregir los datos del PDF actual", value=False,
        disabled=not bool(previous), key="anes_revise_" + kind)
    prefix = f"anes_doc_{kind}_{previous.get('id', 'missing')}_{revise}"
    uploaded = None if revise else st.file_uploader("Adjuntar PDF actualizado", type=["pdf"], key=prefix + "_pdf")
    # A different upload cannot inherit a prior certificate's dates or verification.
    upload_id = str(getattr(uploaded, "file_id", ""))
    initial = dict(previous) if revise else {}
    suggestions = {}
    if kind == 'css':
        try:
            if uploaded:
                suggestions = certificate_date_suggestions(uploaded.getvalue(), kind)
            elif revise and previous.get('file_id'):
                suggestions = _saved_certificate_dates(previous['file_id'], storage)
        except Exception:
            st.caption('No se pudieron leer las fechas automáticamente. Complétalas mirando el PDF.')
    initial.update({k: v for k, v in suggestions.items() if not initial.get(k)})
    if suggestions:
        st.caption('Fechas leídas del propio PDF (Generado / Válido hasta). Revisa los datos antes de guardar.')
    with st.form(prefix + upload_id):
        left, right = st.columns(2)
        issued = left.date_input("Fecha de emisión", value=parse_date(initial.get("issued")))
        expires = right.date_input("Fecha de vencimiento", value=parse_date(initial.get("expires")))
        if kind == "registro_publico":
            st.caption(f"Se calcula emisión + {REGISTRY_MAX_MONTHS} meses; completa vencimiento solo si el certificado indica uno anterior.")
        catalogs = initial.get("catalogs", "")
        fichas, models = initial.get("fichas", ""), initial.get("models", "")
        if kind in PRODUCT_DOCS:
            with st.expander("Cobertura del producto", expanded=True):
                catalogs = ",".join(st.multiselect("Catálogos expresamente cubiertos", ["C", "K"],
                    default=[c for c in catalogs.split(",") if c in CATALOGS], format_func=CATALOGS.get))
                fichas = st.text_input("Fichas expresamente cubiertas", value=fichas, placeholder="43358")
                models = st.text_input("Modelos expresamente cubiertos", value=models, placeholder="LB4330K, LB4330C")
        evidence = st.text_area("Comprobación del documento", value=initial.get("evidence", ""),
            placeholder="Indica la página donde constan el titular, las fechas y, si aplica, los modelos cubiertos.")
        with st.expander("Otros datos de verificación", expanded=False):
            no_expiry = st.checkbox("Sin vencimiento expreso verificado", value=bool(initial.get("no_expiry_confirmed")))
            notarized = st.checkbox("Autenticación notarial verificada", value=bool(initial.get("notarized")))
            apostilled = st.checkbox("Apostilla o legalización verificada", value=bool(initial.get("apostilled")))
            translated = st.checkbox("Español o traducción autorizada verificada", value=bool(initial.get("translation_verified")))
        verified = st.checkbox("Revisé el PDF, su titular y los datos indicados", key=prefix + upload_id + "_verified")
        save = st.form_submit_button("Guardar documento", type="primary")
    if save:
        if (not revise and not uploaded) or not evidence.strip():
            st.error("Adjunta el PDF actualizado e indica qué comprobaste en él.")
            return
        metadata = {"kind": kind, "label": KINDS[kind], "company": COMPANY,
            "issued": str(issued or ""), "expires": str(expires or ""),
            "catalogs": catalogs, "fichas": fichas.strip(), "models": models.strip(), "act": "",
            "evidence": evidence.strip(), "no_expiry_confirmed": no_expiry, "notarized": notarized,
            "apostilled": apostilled, "translation_verified": translated, "verified": verified}
        try:
            with st.spinner("Guardando documento en Drive..."):
                saved = (storage.revise_document(previous, metadata, actor=actor) if revise else
                    storage.upload_document(uploaded.name, uploaded.getvalue(), metadata, actor=actor))
            issues = saved.get("content_validation", {}).get("errors", [])
            st.session_state["anes_document_result"] = issues
            st.session_state["anes_document_saved"] = KINDS[kind]
        except Exception as exc:
            st.error(f"No se pudo guardar el documento: {exc}. El original anterior se conserva.")
            return
        _close_library()
        _refresh()
    with st.expander("Versiones anteriores de este documento", expanded=False):
        _table([{"Documento": r.get("name", KINDS[kind]), "Emisión": r.get("issued", ""),
            "Vence": r.get("expires", ""), "Guardado": r.get("created_at", ""), "enlace": r.get("url", "")}
            for r in reversed(rows) if document_kind(r.get("kind")) == kind])


def _configure(source, job, storage, actor):
    """One offer form, before and after asynchronous official capture."""
    cfg = prepare_offer_config(source, job.get("config") or {})
    today = datetime.now(PANAMA).date()
    new = job["id"] == "new"
    busy = job.get("state") in ACTIVE_STATES
    key = "anes_offer_" + job["id"]
    if source:
        st.write(f"**{source.get('number')} · {source.get('purchase_unit') or source.get('entity')}**")
        with st.expander("Acto oficial y anexos", expanded=False):
            st.link_button("Abrir acto oficial", source["url"])
            _table([{"Documento": a["name"], "enlace": a["url"]} for a in source.get("attachments", [])])
        _source_status(source)
    with st.form(key):
        url = st.text_input("Enlace del acto en PanamáCompra", value=job.get("url", ""),
            placeholder="https://www.panamacompra.gob.pa/Inicio/#/...", disabled=not new,
            key=key + "_url")
        c1, c2 = st.columns(2)
        catalog = c1.selectbox("Mascarilla / catálogo", ["K", "C"],
            index=["K", "C"].index(cfg["catalog"]) if cfg.get("catalog") in CATALOGS else None,
            placeholder="Selecciona mascarilla 4 o 5", format_func=CATALOG_LABELS.get, disabled=busy, key=key + "_catalog")
        price = c2.number_input("Precio UNITARIO de participación (USD)", min_value=0.0,
            value=float(cfg.get("price", 0)), step=0.01, format="%.4f", disabled=busy, key=key + "_price")
        modes = list(TAX_LABELS)
        mode = c1.selectbox("ITBMS", modes, index=modes.index(cfg.get("tax_mode", "exento")),
            format_func=TAX_LABELS.get, disabled=busy, key=key + "_tax_mode")
        tax_rate = c2.number_input("Tasa de ITBMS (%)", min_value=0.0, max_value=100.0,
            value=float(cfg.get("tax_rate", 7)), disabled=busy, key=key + "_tax_rate")
        st.caption("MFLAB · K: LB4330K (mascarilla 4) · C: LB4330C (mascarilla 5). Precio por kit.")
        portal_term = portal_delivery_term(source)
        st.markdown("**Término de entrega del portal:** " + (portal_term or ("No disponible" if source else "Se obtendrá al consultar el acto")))
        portal_reviewed = (cfg.get("delivery_use_portal") is True and bool(portal_term)
            and cfg.get("delivery_portal_value") == portal_term
            and cfg.get("delivery_portal_source_hash") == source.get("fingerprint"))
        use_portal = st.checkbox("Revisé los adjuntos y no indican una condición de entrega distinta; usar el plazo del portal.",
            value=portal_reviewed, disabled=busy or not bool(portal_term),
            key=f"anes_delivery_portal_{job['id']}_{source.get('fingerprint', '')}_{portal_term}")
        manual_default = cfg.get("delivery_manual", cfg.get("delivery", "") if not cfg.get("delivery_use_portal") else "")
        delivery = st.text_area("Calendario de entregas completo (manual)", value=manual_default,
            placeholder="Completa si no usas el plazo del portal, incluidas las entregas parciales.", disabled=busy, key=key + "_delivery")
        place, _ = delivery_destination(source)
        if place:
            st.markdown(f"**Lugar de entrega:** {place}")
        elif source:
            place = st.text_input("Lugar de entrega (solo si no se pudo extraer del anexo)",
                value=cfg.get("delivery_place", ""), disabled=busy, key=key + "_place")
        else:
            place = cfg.get("delivery_place", "")
        with st.expander("Fechas y condiciones de la oferta", expanded=False):
            st.markdown("**Garantía y esterilidad:** " + cfg["warranty"])
            st.caption(f"Registro Público: máximo {cfg['registry_max_months']} meses desde su emisión.")
            dates1, dates2 = st.columns(2)
            document_date = dates1.date_input("Fecha de los documentos",
                value=min(parse_date(cfg.get("document_date")) or today, today), max_value=today, disabled=busy, key=key + "_date")
            control_date = dates2.date_input("Vigencia exigible hasta (presentación u otra fecha exigida)",
                value=max(parse_date(cfg.get("control_date")) or today, today), min_value=today, disabled=busy, key=key + "_control")
            validity = st.number_input("Validez de la cotización (días)", min_value=1,
                value=int(cfg.get("proposal_validity_days", 30)), disabled=busy, key=key + "_validity")
            if cfg.get("extra_requirements"):
                st.caption("Requisitos adicionales ya guardados: " + "; ".join(cfg["extra_requirements"]))
        confirmed = st.checkbox("Revisé todos los requisitos y anexos, su calendario y posibles modificaciones",
            disabled=busy or not bool(source), key=key + "_confirmed_" + str(source.get("fingerprint", "")))
        buttons = st.columns(2)
        capture = buttons[0].form_submit_button("Consultar acto y anexos" if not source else "Actualizar acto y anexos", disabled=busy)
        submit = buttons[1].form_submit_button("Generar documentos", type="primary", disabled=busy or not bool(source))
    if not (capture or submit):
        return
    config = prepare_offer_config(source, {"catalog": catalog, "price": str(price), "tax_mode": mode, "tax_rate": tax_rate,
        "tax_evidence": cfg.get("tax_evidence", ""), "delivery_manual": delivery.strip(), "delivery_use_portal": use_portal,
        "delivery_portal_value": portal_term if use_portal else "",
        "delivery_portal_source_hash": source.get("fingerprint") if use_portal else None,
        "delivery_place": place.strip(), "document_date": str(document_date), "control_date": str(control_date),
        "proposal_validity_days": validity, "extra_requirements": cfg.get("extra_requirements", []), "source_confirmed": confirmed})
    if capture:
        _, _, number = route(url.strip())
        active = next((r for r in storage.rows("ANESTESIA_EXPEDIENTES")
            if r.get("number") == number and r.get("state") in ACTIVE_STATES), None)
        if active:
            st.session_state["anes_select_next"] = active["id"]
            _refresh()
            return
        if not catalog or price <= 0:
            st.error("Selecciona la mascarilla 4 o 5 e indica un precio unitario mayor que cero antes de consultar el acto.")
            return
        # Do not transfer approval to a new official snapshot.
        config.update(source_confirmed=False, delivery_use_portal=False,
            delivery_portal_value="", delivery_portal_source_hash=None)
        job = storage.save_job({"id": uuid.uuid4().hex if new else job["id"], "number": number,
            "url": url.strip(), "state": "Nuevo", "config": config,
            **({"created_by": actor} if new else {})})
        st.session_state["anes_select_next"] = job["id"]
        _enqueue(storage, job, "capture", actor, url=url.strip())
    elif submit:
        checks, _ = validate_package(source, config, storage.rows("ANESTESIA_DOCUMENTOS"))
        if any(c["estado"] != "Vigente documentalmente" for c in checks):
            storage.save_job({"id": job["id"], "config": config, "checks": checks,
                "state": "Bloqueado", "detail": "Actualiza los documentos o datos indicados. Se conservó tu configuración."})
            _refresh()
        else:
            storage.save_job({"id": job["id"], "config": config})
            _enqueue(storage, job, "generate", actor, config=config)


def _review(storage, job, actor):
    if not job.get("manifest_id") or job.get("state") not in {"Pendiente de revisión", "Listo para entregar", "Error"}:
        return
    manifest = _json(job["manifest_id"], storage)
    st.markdown("#### Revisión final con ChatGPT")
    st.caption("Abre ChatGPT con el modelo de revisión disponible en tu plan, adjunta o conecta la carpeta y pega este prompt. No hay un envío automático a un chat personal.")
    st.caption(f"Entrega prevista: {manifest.get('delivery_pdf_count', sum(f.get('mime') == 'application/pdf' for f in manifest.get('files', [])))} PDF. "
               "Tras aprobar y revalidar, se reemplazará el contenido de Entrega actual; los Word y archivos de revisión quedarán en el historial.")
    for note in manifest.get('delivery_notes', []):
        st.info(note)
    st.link_button("Abrir borradores y originales", manifest["folder_url"])
    st.download_button("Descargar prompt exacto de revisión", review_prompt(manifest), "prompt_revision_anestesia.md", mime="text/markdown")
    with st.expander("Ver prompt", expanded=False):
        st.code(review_prompt(manifest), language=None)
    upload = st.file_uploader("Adjuntar revision_anestesia.json devuelto por la revisión", type=["json"], key="anes_review_" + job["id"])
    if upload:
        if upload.size > 500000:
            st.error("La revisión supera 500 KB.")
            return
        try:
            review = json.loads(upload.getvalue().decode("utf-8-sig"))
            if not isinstance(review, dict):
                raise ValueError("La revisión debe ser un objeto JSON.")
            issues = review_errors(review, manifest)
            for issue in issues:
                st.warning(issue)
            confirm = st.checkbox("Leí la revisión y confirmo que corresponde a estos archivos y requisitos", key="anes_review_confirm_" + job["id"])
            if st.button("Revalidar y publicar expediente final", disabled=bool(issues) or not confirm or job.get("state") == "Listo para entregar", type="primary"):
                saved = storage.save_review(review, actor)
                _enqueue(storage, job, "finalize", actor, review_id=saved["id"], user_confirmed=True)
        except (ValueError, TypeError, KeyError) as exc:
            st.error(f"No se pudo aceptar la revisión: {exc}")


@st.fragment(run_every="10s")
def _delivery_links(storage, job):
    st.markdown("#### Documentos para presentar")
    if job.get('manifest_id') and job.get('draft_url') and job.get('state') == 'Pendiente de revisión':
        st.link_button('Abrir documentos para revisar en Drive', job['draft_url'], type='primary')
        manifest = _json(job['manifest_id'], storage)
        for file in manifest.get('files', []):
            if file.get('name') in {'01_Cotizacion.docx', '01_Cotizacion.pdf'} and file.get('url'):
                label = 'Abrir cotización membretada en Word' if file['name'].endswith('.docx') else 'Abrir cotización en PDF'
                st.link_button(label, file['url'])
        st.caption('Borradores generados para revisar. La carpeta final se habilita después de aprobar la revisión.')
    ready = (job.get('state') == 'Listo para entregar' and bool(job.get('manifest_hash'))
             and job.get('manifest_hash') == job.get('published_manifest'))
    target = None
    # This immutable snapshot contains only the PDFs approved for THIS case.
    # The shared final_url can be replaced by a later request for another act.
    if ready and job.get('delivery_folder_id') and job.get('archive_url'):
        target = job['archive_url']
    elif ready and job.get('delivery_folder_id') and job.get('final_url'):
        try:
            current = storage.delivery_status(job['delivery_folder_id'])
            if (current.get('state') == 'ready' and current.get('request') == job['id']
                    and current.get('manifest') == job.get('published_manifest')):
                target = job['final_url']
            elif current.get('state') == 'updating':
                st.warning("Se está reemplazando la entrega actual. Espera a que se complete la verificación de los PDF.")
            elif current.get('request') == job['id']:
                st.caption("Entrega actual conserva una versión anterior. Los nuevos documentos deben completar su revisión antes de reemplazarla.")
            else:
                st.caption("Entrega actual corresponde a otra solicitud. Los PDF revisados de este expediente siguen disponibles en su historial.")
        except Exception:
            st.warning("No se pudo verificar la entrega actual. Se reintentará en 10 segundos; conserva la referencia del expediente.")
    if target:
        st.link_button("Ver archivos en Drive", target, type='primary',
            help="Abre únicamente los PDF finales de este acto: cotización membretada y documentos de respaldo vigentes al validar la entrega.")
        count = job.get('delivery_pdf_count')
        st.caption(f"Acto {job.get('number', '')} · {str(count) + ' PDF' if count else 'PDF finales'}. "
                   "Cotización generada con los datos de esta oferta y sus documentos de respaldo. Referencias y Word se guardan aparte.")
        if job.get('zip_url'):
            st.link_button("Descargar todos los PDF (ZIP)", job['zip_url'])
    else:
        st.button("Ver archivos en Drive", disabled=True, key='anes_delivery_pending_' + job['id'])
        if job.get('state') == 'Pendiente de revisión':
            st.caption("Los documentos están generados y pendientes de revisión final. Completa la revisión de abajo para habilitar la entrega.")
        elif job.get('state') in ACTIVE_STATES:
            st.caption("El orquestador está trabajando. La carpeta de entrega se habilitará al completar la generación y validación.")
        elif not ready:
            st.caption("Aún no hay una entrega final para esta versión. Completa los datos y documentos indicados y genera el expediente.")
        else:
            st.caption("No hay una carpeta de PDF finales verificada para este expediente. Genera y revisa una nueva versión.")
        if job.get('archive_url'):
            with st.expander("Entrega anterior (no corresponde a la nueva versión)", expanded=False):
                st.caption("Se conserva una versión anterior; no es la entrega nueva pendiente.")
                st.link_button("Abrir PDF de la versión anterior", job['archive_url'])


def _reference_links(job):
    folder_url = job.get("folder_url") or (
        f"https://drive.google.com/drive/folders/{job['folder_id']}" if job.get("folder_id") else "")
    participation = job.get("participation") or {}
    if not folder_url and not participation:
        return
    with st.expander("Referencias, anexos e historial (no son la entrega)", expanded=False):
        if folder_url:
            st.link_button("Abrir referencias e historial", folder_url)
        if participation:
            st.write(f"Modelo ofrecido en la cotización anterior: **{participation.get('model', 'Pendiente')}** · "
                     f"Catálogo **{participation.get('catalog', 'Pendiente')}**.")
            st.caption("Antecedente documental: no sustituye la revisión de requisitos ni los datos de una nueva oferta.")
            if participation.get('quotation_url'):
                st.link_button("Abrir cotización de referencia", participation["quotation_url"])
            if participation.get('folder_url'):
                st.link_button("Ver adjuntos originales de la participación", participation["folder_url"])
            for note in participation.get("observations", []):
                st.info(note)


def render_anestesia_docs(creds, actor):
    st.subheader("Anestesia-Docs")
    st.caption("Ficha 43358 · documentos originales en Drive · vigencias por acto · generación desde tu orquestador.")
    app = st.secrets.get("app", {})
    storage = AnestesiaStorage(build("drive", "v3", credentials=creds, cache_discovery=False),
        build("sheets", "v4", credentials=creds, cache_discovery=False),
        sheet_id=app.get("PC_MANUAL_SHEET_ID", SHEET_ID), parent_id=app.get("DRIVE_COTIZACIONES_FOLDER_ID", DRIVE_PARENT))
    try:
        configuration = (storage.sheet_id, storage.parent_id)
        ready = st.session_state.get("anes_workbook_ready", {})
        if ready.get("configuration") == configuration:
            storage.sheet_id = ready["resolved_id"]
        else:
            storage.ensure_tables()
            st.session_state["anes_workbook_ready"] = {
                "configuration": configuration, "resolved_id": storage.sheet_id}
        _health_panel(storage, actor)
        if st.session_state.get("anes_document_dialog_open"):
            _library(storage, actor)
        st.markdown("#### Generar acto")
        # Stable IDs and labels preserve the selected case while the worker advances.
        jobs = list(reversed(_records(storage.sheet_id, "ANESTESIA_EXPEDIENTES", storage)))
        mapping = {r["id"]: r for r in jobs}
        next_selection = st.session_state.pop("anes_select_next", None)
        if next_selection in mapping:
            st.session_state["anes_selected"] = next_selection
        if st.session_state.get("anes_selected") not in ["new", *mapping]:
            st.session_state["anes_selected"] = next(iter(mapping), "new")
        left, right = st.columns([4, 1])
        selected = left.selectbox("Acto a preparar", ["new", *mapping], key="anes_selected",
            format_func=lambda k: "Nuevo acto" if k == "new" else f"{mapping[k].get('number', k)} · {k[:8]}")
        if right.button("Actualizar estado", key="anes_refresh"):
            _refresh()
        job = mapping.get(selected, {"id": "new", "state": "Nuevo"})
        if selected != "new":
            try:
                job = _live_job(storage.sheet_id, selected, storage) or job
            except Exception:
                st.warning("No se pudo actualizar el estado. Se conserva la última lectura; usa Actualizar estado para reintentar.")
            if job.get("state") == "Datos capturados":
                st.success("Acto y anexos listos. Revisa la entrega y pulsa Generar documentos.")
            elif job.get("state") == "Pendiente de revisión":
                st.success("Borradores listos para la revisión final.")
            elif job.get("state") == "Listo para entregar":
                st.success("Expediente listo para entregar.")
            elif job.get("state") == "Error":
                st.warning(job.get("detail") or "Revisa los datos y documentos para continuar.")
            elif job.get("state") == "Bloqueado":
                st.caption('Se conservaron los datos de tu oferta. Abajo se indican los pendientes actuales.')
            else:
                st.write(f"**{job['state']}** — {job.get('detail', '')}")
        source = _json(job["source_id"], storage) if job.get("source_id") else {}
        _configure(source, job, storage, actor)
        if selected == "new":
            return
        if job.get("state") in ACTIVE_STATES:
            _watch_job(storage, job)
        elif job.get("checks") or source.get("blocking_errors"):
            # Saved failures become obsolete when a certificate is replaced.
            checks, _ = validate_package(source, prepare_offer_config(source, job.get('config') or {}),
                _records(storage.sheet_id, 'ANESTESIA_DOCUMENTOS', storage))
            pending = [c for c in checks if c.get('estado') != 'Vigente documentalmente']
            if pending:
                st.markdown('**Pendientes antes de generar**')
                for check in pending:
                    if check.get('kind') == 'expediente':
                        for issue in check.get('issues') or [check.get('motivo', '')]:
                            st.warning(issue)
                    else:
                        st.warning(f"{check.get('documento')}: {check.get('motivo')}")
                st.caption('Son comprobaciones, no archivos. La cotización se creará al resolverlas y pulsar Generar documentos.')
            elif job.get('state') == 'Bloqueado':
                st.success('Los documentos y datos guardados ya pasan la comprobación. Pulsa Generar documentos.')
        _delivery_links(storage, job)
        if job.get("state") not in ACTIVE_STATES and job.get("manifest_id"):
            with st.expander("Revisión final y publicación", expanded=job.get("state") == "Pendiente de revisión"):
                _review(storage, job, actor)
        _reference_links(job)
    except Exception as exc:
        st.error(f"No fue posible completar la operación documental: {exc}")
        st.caption("Los originales y expedientes anteriores se conservan. Reintenta con Actualizar estado.")


@st.fragment(run_every="60s")
def _health_panel(storage, actor):
    st.markdown("#### Documentos actuales y vigencias")
    catalog = st.selectbox("Comprobar biblioteca para catálogo", ["K", "C"], format_func=CATALOGS.get, key="anes_health_catalog")
    try:
        rows = _records(storage.sheet_id, "ANESTESIA_DOCUMENTOS", storage)
    except Exception as exc:
        st.error(f"No se pudo comprobar la biblioteca en esta lectura: {exc}")
        st.caption("Los documentos en Drive se conservan. Reintenta con Actualizar estado; no se consideran faltantes por un fallo de conexión.")
        return
    today = datetime.now(PANAMA).date()
    _table([{key: value for key, value in row.items() if key != "Qué falta / comprobación"}
            for row in library_health(rows, as_of=today, catalog=catalog)])
    st.caption(f"Control al {today:%d/%m/%Y} (Panamá) · actualización automática cada 60 segundos. "
               "Al generar se verifican las vigencias para la fecha de presentación.")
    saved = st.session_state.pop("anes_document_saved", None)
    if saved:
        st.success(f"{saved}: documento guardado. La tabla muestra su estado de verificación.")
    for issue in st.session_state.pop("anes_document_result", []):
        st.warning("Documento guardado pendiente de verificación: " + issue)
    if st.button("Actualizar documento", key="anes_update_document"):
        st.session_state["anes_document_dialog_open"] = True
        st.rerun()
