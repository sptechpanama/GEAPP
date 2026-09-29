"""Lazy Streamlit view; network-heavy capture and rendering run in the existing queue."""
from __future__ import annotations

from datetime import datetime
import json
import uuid

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

from services.anestesia_docs import (CATALOGS, COMPANY, KINDS, PANAMA, now_iso, parse_date,
                                    review_errors, review_prompt, totals, validate_package)
from services.anestesia_source import route, source_is_closed
from services.anestesia_storage import AnestesiaStorage, DRIVE_PARENT, SHEET_ID

ANESTESIA_UI_VERSION = 2


@st.cache_data(ttl=20, max_entries=30, show_spinner=False)
def _records(sheet_id, table, _storage):
    return _storage.rows(table)


@st.cache_data(ttl=300, max_entries=8, show_spinner=False)
def _json(file_id, _storage):
    return _storage.json_file(file_id)


def _refresh():
    _records.clear()
    st.rerun()


def _enqueue(storage, job, action, actor, **payload):
    if job.get("state") in {"En cola", "Procesando"}:
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


def _library(storage, actor):
    st.caption("Cada carga conserva el original y su historial en Drive. La fecha de subida no renueva su vigencia.")
    rows = _records(storage.sheet_id, "ANESTESIA_DOCUMENTOS", storage)
    today = datetime.now(PANAMA).date()
    _table([{"Documento": r.get("label", KINDS.get(r.get("kind"), r.get("kind"))),
             "Catálogo": r.get("catalogs", ""), "Emisión": r.get("issued", ""), "Vence": r.get("expires", ""),
             "Estado": "Vencido" if parse_date(r.get("expires")) and parse_date(r["expires"]) < today
             else "Datos verificados" if r.get("verified") is True else "Pendiente de verificación",
             "Acto": r.get("act", ""), "enlace": r.get("url"), "Versión": r.get("created_at", "")} for r in rows])
    st.markdown("#### Cargar o actualizar un documento")
    entries = {r["id"]: r for r in rows}
    chosen = st.selectbox("Nuevo PDF o revisar uno ya guardado", [""] + list(entries),
        format_func=lambda k: "Cargar nuevo PDF" if not k else f"{entries[k].get('name', entries[k].get('kind'))} · {entries[k].get('created_at', '')}")
    previous = entries.get(chosen, {})
    if previous.get("url"):
        st.link_button("Abrir original que se verificará", previous["url"])
    original_kind = previous.get("kind", "dgi").split(":")[0]
    kind = st.selectbox("Documento", list(KINDS), index=list(KINDS).index(original_kind), format_func=KINDS.get, key="anes_library_kind_" + chosen)
    with st.form("anes_library_" + chosen + kind, clear_on_submit=True):
        uploaded = st.file_uploader("PDF original completo (máximo 30 MB)", type=["pdf"])
        extra = st.text_input("Nombre exacto del requisito adicional", value=previous.get("kind", "").partition(":")[2], help="Solo para Otro requisito del acto.") if kind == "otro" else ""
        left, right = st.columns(2)
        issued = left.date_input("Fecha de emisión (si consta)", value=parse_date(previous.get("issued")))
        expires = right.date_input("Fecha de vencimiento (si consta)", value=parse_date(previous.get("expires")))
        catalogs = left.multiselect("Catálogos expresamente cubiertos", ["C", "K"], default=[c for c in previous.get("catalogs", "").split(",") if c in CATALOGS], format_func=CATALOGS.get)
        fichas = right.text_input("Fichas expresamente cubiertas", value=previous.get("fichas", ""), placeholder="43358")
        models = st.text_input("Modelos exactos expresamente cubiertos (separados por coma)", value=previous.get("models", ""), placeholder="Ej.: LB4330K, LB4330C")
        st.caption("Un CT puede cubrir varios modelos. Registra todas sus páginas; la cotización ofrecerá únicamente el modelo seleccionado para el acto.")
        act = st.text_input("Número de acto específico, si aplica", value=previous.get("act", ""), help="Obligatorio para retorsión y calidad. Déjalo vacío en certificados generales.")
        evidence = st.text_area("Evidencia de verificación: páginas, fechas, titular, producto y alcance",
            value=previous.get("evidence", ""), placeholder="Ej.: página 1, vigencia impresa hasta..., emitido para RIR..., modelo LB4330K...")
        no_expiry = st.checkbox("Verifiqué que no tiene vencimiento expreso (cuando corresponda)", value=bool(previous.get("no_expiry_confirmed", False)))
        notarized = st.checkbox("Autenticación notarial presente y verificada")
        apostilled = st.checkbox("Apostilla o legalización presente y verificada")
        translated = st.checkbox("Original en español o traducción autorizada verificada")
        verified = st.checkbox("Revisé el PDF, el titular y los datos anteriores; no inferí fechas ni cobertura")
        save = st.form_submit_button("Guardar nueva versión y verificación", type="primary")
    if save:
        if (not uploaded and not previous) or not evidence.strip() or (kind == "otro" and not extra.strip()):
            st.error("Adjunta el PDF e indica la evidencia y, si aplica, el nombre del requisito adicional.")
        else:
            with st.spinner("Guardando original e índice..."):
                metadata = {
                    "kind": "otro:" + extra.strip() if kind == "otro" else kind, "label": extra.strip() if kind == "otro" else KINDS[kind],
                    "company": COMPANY, "issued": str(issued or ""), "expires": str(expires or ""),
                    "catalogs": ",".join(catalogs), "fichas": fichas.strip(), "models": models.strip(), "act": act.strip(),
                    "evidence": evidence.strip(), "no_expiry_confirmed": no_expiry, "notarized": notarized,
                    "apostilled": apostilled, "translation_verified": translated, "verified": verified}
                if uploaded:
                    storage.upload_document(uploaded.name, uploaded.getvalue(), metadata, actor=actor)
                else:
                    storage.revise_document(previous, metadata, actor=actor)
            _refresh()


def _configure(source, job, storage, actor):
    cfg = job.get("config") or {}
    today = datetime.now(PANAMA).date()
    st.link_button("Abrir acto oficial", source["url"])
    st.write(f"**{source.get('number')} · {source.get('purchase_unit') or source.get('entity')}**")
    st.caption("El calendario detallado de los anexos prevalece sobre el plazo resumido del portal. Revisa todas las entregas parciales.")
    _table([{"Documento": a["name"], "enlace": a["url"]} for a in source.get("attachments", [])])
    for message in source.get("blocking_errors", []):
        st.error(message)
    closed, reason = source_is_closed(source)
    if closed:
        st.warning(reason + " Se permite estudiar este expediente histórico, pero no publicarlo como oferta vigente.")
    registry = source.get("registry_max_months")
    st.info(f"Registro Público: antigüedad máxima detectada de {registry} meses. {source.get('registry_rule', '')}"
            if registry else "Registro Público: confirma la antigüedad máxima directamente en el requisito del acto.")
    with st.form("anes_config_" + job["id"]):
        c1, c2 = st.columns(2)
        catalog = c1.selectbox("Catálogo", ["K", "C"], index=0 if cfg.get("catalog", "K") == "K" else 1, format_func=CATALOGS.get)
        price = c2.number_input("Precio UNITARIO de participación (USD)", min_value=0.0, value=float(cfg.get("price", 0)), step=0.01, format="%.4f")
        modes = ["exento", "adicional", "incluido"]
        mode = c1.selectbox("ITBMS", modes, index=modes.index(cfg.get("tax_mode", "exento")),
            format_func=lambda x: {"exento": "No aplica / exento", "adicional": "Se suma al precio", "incluido": "Ya incluido en el precio"}[x])
        tax_rate = c2.number_input("Tasa de ITBMS (%)", min_value=0.0, max_value=100.0, value=float(cfg.get("tax_rate", 7)))
        tax_evidence = st.text_input("Respaldo del tratamiento tributario", value=cfg.get("tax_evidence", ""))
        brand = c1.text_input("Marca", value=cfg.get("catalog_brand", ""))
        model = c2.text_input("Modelo / referencia exacta", value=cfg.get("catalog_model", ""), placeholder="Verificar en el catálogo y CT")
        st.caption("MFLAB: K = LB4330K, mascarilla talla 4; C = LB4330C, mascarilla talla 5. El CT puede cubrir ambos; esta oferta debe indicar cuál se entrega.")
        delivery = st.text_area("Calendario de entregas completo", value=cfg.get("delivery", ""),
            placeholder="Ej.: 300 unidades a 30 días; 300 a 45 días; 300 a 60 días calendario desde...")
        place = st.text_input("Lugar exacto de entrega", value=cfg.get("delivery_place", ""))
        warranty = st.text_area("Garantía y vida útil / esterilidad exigida", value=cfg.get("warranty", ""))
        validity = st.number_input("Validez de la cotización (días)", min_value=1, value=int(cfg.get("proposal_validity_days", 30)))
        representative = c1.text_input("Representante de la entidad para el pacto", value=cfg.get("entity_representative", ""))
        entity_id = c2.text_input("Cédula del representante de la entidad", value=cfg.get("entity_id", ""))
        role = st.text_input("Cargo del representante de la entidad", value=cfg.get("entity_role", "Representante Legal"))
        document_date = c1.date_input("Fecha de los documentos", value=parse_date(cfg.get("document_date")) or today, max_value=today)
        control_date = c2.date_input("Vigencia exigible hasta (presentación u otra fecha exigida)",
            value=max(parse_date(cfg.get("control_date")) or today, today), min_value=today)
        max_age = st.number_input("Antigüedad máxima Registro Público (meses; 0 = no confirmada)", min_value=0, max_value=24,
            value=int(cfg.get("registry_max_months") or registry or 0))
        rule_evidence = st.text_input("Archivo/página que respalda esa antigüedad", value=cfg.get("registry_rule_evidence", source.get("registry_rule", "")))
        rs = st.checkbox("El acto exige además Registro Sanitario", value=bool(cfg.get("require_rs", False)))
        power = st.checkbox("Se presenta mediante apoderado: exige poder", value=bool(cfg.get("require_power", False)))
        extras = st.text_area("Otros documentos exigidos (uno por línea)", value="\n".join(cfg.get("extra_requirements", [])))
        confirmed = st.checkbox("Revisé todos los requisitos y anexos, su calendario y posibles modificaciones")
        signed = st.checkbox("Autorizo usar la firma de RIR en la cotización y el pacto de este expediente")
        submit = st.form_submit_button("Comprobar requisitos y preparar borradores", type="primary", disabled=job.get("state") in {"En cola", "Procesando"})
    if submit:
        config = {"catalog": catalog, "price": str(price), "tax_mode": mode, "tax_rate": tax_rate,
            "tax_evidence": tax_evidence.strip(), "catalog_brand": brand.strip(), "catalog_model": model.strip(),
            "delivery": delivery.strip(), "delivery_place": place.strip(), "warranty": warranty.strip(),
            "entity_representative": representative.strip(), "entity_id": entity_id.strip(), "entity_role": role.strip(),
            "document_date": str(document_date), "control_date": str(control_date), "proposal_validity_days": validity,
            "registry_max_months": max_age or None, "registry_rule_evidence": rule_evidence.strip(), "require_rs": rs,
            "require_power": power, "extra_requirements": [x.strip() for x in extras.splitlines() if x.strip()],
            "source_confirmed": confirmed, "signature_authorized": signed}
        checks, _ = validate_package(source, config, storage.rows("ANESTESIA_DOCUMENTOS"))
        if any(c["estado"] != "Vigente documentalmente" for c in checks):
            storage.save_job({"id": job["id"], "config": config, "checks": checks,
                "state": "Bloqueado", "detail": "Actualiza los documentos o datos indicados. Se conservó tu configuración."})
            _refresh()
        else:
            _enqueue(storage, job, "generate", actor, config=config)


def _review(storage, job, actor):
    if not job.get("manifest_id") or job.get("state") not in {"Pendiente de revisión", "Listo para entregar", "Error"}:
        return
    manifest = _json(job["manifest_id"], storage)
    st.markdown("#### Revisión final con ChatGPT")
    st.caption("Abre ChatGPT con el modelo de revisión disponible en tu plan, adjunta o conecta la carpeta y pega este prompt. No hay un envío automático a un chat personal.")
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
        left, right = st.columns([4, 1])
        section = left.radio("Vista", ["Expedientes", "Biblioteca y vigencias"], horizontal=True, label_visibility="collapsed")
        if right.button("Actualizar estado", key="anes_refresh"):
            _refresh()
        if section == "Biblioteca y vigencias":
            _library(storage, actor)
            return
        with st.expander("Nuevo expediente", expanded=True):
            with st.form("anes_new"):
                url = st.text_input("Enlace del acto en PanamáCompra", placeholder="https://www.panamacompra.gob.pa/Inicio/#/...")
                create = st.form_submit_button("Consultar acto y anexos", type="primary")
            if create:
                _, _, number = route(url.strip())
                existing = storage.rows("ANESTESIA_EXPEDIENTES")
                job = next((r for r in existing if r.get("number") == number and r.get("state") in {"En cola", "Procesando"}), None)
                if job:
                    st.info("Ese acto ya tiene una captura en curso. Puedes seleccionarlo abajo.")
                else:
                    job = storage.save_job({"id": uuid.uuid4().hex, "number": number, "url": url.strip(), "state": "Nuevo", "created_by": actor})
                    st.session_state["anes_selected"] = job["id"]
                    _enqueue(storage, job, "capture", actor, url=url.strip())
        jobs = sorted(_records(storage.sheet_id, "ANESTESIA_EXPEDIENTES", storage), key=lambda r: r.get("updated_at", ""), reverse=True)
        if not jobs:
            st.info("Consulta un acto y carga los certificados vigentes en Biblioteca y vigencias para comenzar.")
            return
        mapping = {r["id"]: r for r in jobs}
        if st.session_state.get("anes_selected") not in mapping:
            st.session_state["anes_selected"] = next(iter(mapping))
        selected = st.selectbox("Expediente", list(mapping), key="anes_selected",
            format_func=lambda k: f"{mapping[k].get('number', k)} · {mapping[k].get('state', '')} · {k[:8]}")
        job = mapping[selected]
        st.write(f"**{job['state']}** — {job.get('detail', '')}")
        st.caption("Último cambio: " + job.get("updated_at", ""))
        if job.get("folder_url"):
            st.link_button("Abrir carpeta e historial del expediente", job["folder_url"])
        participation = job.get("participation", {})
        if participation:
            with st.expander("Participación anterior de RIR y documentos recuperados", expanded=False):
                st.write(f"Modelo ofrecido en la cotización: **{participation.get('model', 'Pendiente')}** · "
                         f"Catálogo **{participation.get('catalog', 'Pendiente')}**.")
                st.caption("Antecedente documental: no sustituye la revisión de requisitos ni los datos de una nueva oferta.")
                st.link_button("Abrir cotización de referencia", participation["quotation_url"])
                st.link_button("Ver adjuntos originales de la participación", participation["folder_url"])
                for note in participation.get("observations", []):
                    st.info(note)
        if job.get("state") in {"En cola", "Procesando"}:
            st.info("Puedes cambiar de página: la tarea continúa en el orquestador. Usa Actualizar estado para consultar el resultado.")
            return
        if job.get("checks"):
            _table([{k: c.get(k, "") for k in ("documento", "estado", "motivo", "vence", "enlace")} for c in job["checks"]])
        if job.get("final_url"):
            st.link_button("Abrir expediente final en Drive", job["final_url"])
            st.link_button("Descargar ZIP", job["zip_url"])
            st.caption("Conserva el corte de la última revisión. Si cambian fechas, anexos o certificados, genera y revisa una nueva versión.")
        if job.get("source_id"):
            source = _json(job["source_id"], storage)
            _review(storage, job, actor)
            with st.expander("Datos y preparación del expediente", expanded=not bool(job.get("manifest_id"))):
                _configure(source, job, storage, actor)
        if job.get("url") and st.button("Volver a capturar acto y anexos", key="anes_recapture"):
            _enqueue(storage, job, "capture", actor, url=job["url"])
        with st.expander("Cómo funciona y qué debe verificarse", expanded=False):
            st.markdown("1. Captura oficial y revisión de anexos.\n2. Certificados originales vigentes y metadatos comprobados.\n"
                "3. Cotización y pacto en Word/PDF, con membrete y firma de RIR. Los certificados oficiales mantienen su formato y firma originales.\n"
                "4. Auditoría externa sobre todos los archivos y devolución del JSON de revisión.\n"
                "5. Revalidación del acto, vigencias y archivos antes de publicar en Drive.\n\n"
                "La firma de la entidad, notaría y apostillas deben obtenerse cuando el acto las exija; insertar la firma de RIR no las sustituye. "
                "La biblioteca conserva cada versión y su historial. El número de documentos depende del pliego, no de una cantidad fija.")
    except Exception as exc:
        st.error(f"No fue posible completar la operación documental: {exc}")
        st.caption("Los originales y expedientes anteriores se conservan. Reintenta con Actualizar estado.")
