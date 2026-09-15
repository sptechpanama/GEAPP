"""Standalone, paginated external opportunities. Scraping stays in the server."""
from __future__ import annotations

import importlib
import json
import logging
import math
import os
from datetime import date, timedelta
from collections.abc import Mapping

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

from services.access_control import require_page_access
from services import otras_fuentes as service
from services.external_requests import queue_external_refresh
from services import external_access
from ui.theme import apply_global_theme

if getattr(service, 'API_VERSION', 0) < 4:
    service = importlib.reload(service)

LOCAL_SOURCES = ('acp_sli', 'acp', 'ensa', 'ena', 'idaan', 'cruz_roja', 'ciudad_saber', 'ungm')
PORTALS = {
    'acp_sli': ('https://apps.pancanal.com/sli/', 'Licitaciones abiertas y enmendadas; registro SLI para ofertar.'),
    'acp': ('https://pancanal.com/solicitudes-de-informaciones/', 'Consultas previas a la contratación; no equivalen a una licitación.'),
    'ensa': ('https://ensa.com.pa/contratistas-proveedores/', 'Licitaciones, registro de contratistas y documentos de contratación.'),
    'ena': ('https://ena.com.pa/activas/', 'Avisos, solicitudes y anexos de ENA Corredores.'),
    'idaan': ('https://compras.idaan.gob.pa/home', 'Compras publicadas en el portal corporativo de IDAAN.'),
    'cruz_roja': ('https://cruzroja.org.pa/licitaciones-publicas/', 'Compras y contrataciones de Cruz Roja Panameña.'),
    'ciudad_saber': ('https://ciudaddelsaber.org/es/oportunidades/convocatorias/', 'Convocatorias de organizaciones de la comunidad.'),
    'ifrc': ('https://www.ifrc.org/our-work/supply-chain-management/business-opportunities', 'Compras humanitarias; verificar destino y requisitos de proveedor.'),
    'naturgy': (external_access.GUIDES['naturgy']['url'], external_access.GUIDES['naturgy']['access']),
    'aes': (external_access.GUIDES['aes']['url'], external_access.GUIDES['aes']['access']),
}
SORTS = {'Más recientemente publicadas': 'published_desc', 'Cierre más próximo': 'deadline_asc',
         'Detectadas recientemente': 'detected_desc', 'Mayor monto publicado': 'amount_desc'}
STATES = {'Vigentes o por confirmar': 'current', 'Para evaluar': 'relevant', 'Por revisar': 'review',
          'Histórico / vencidas': 'historical', 'Todas, sin ocultar anuncios': 'all'}


def config_value(key, default=''):
    try:
        app = st.secrets.get('app', {})
        value = app.get(key) if isinstance(app, Mapping) else None
        return value or st.secrets.get(key) or os.getenv(key) or default
    except Exception:
        return os.getenv(key) or default


@st.cache_resource(show_spinner=False, max_entries=1)
def database():
    dsn = config_value('SUPABASE_DB_URL') or config_value('DATABASE_URL')
    if not dsn:
        raise ValueError('Falta configurar la conexión a Supabase')
    if dsn.startswith('postgres://'):
        dsn = 'postgresql://' + dsn[len('postgres://'):]
    return create_engine(dsn, pool_pre_ping=True, pool_size=2, max_overflow=1,
                         pool_recycle=300, connect_args={'connect_timeout': 10, 'options': '-c statement_timeout=20000'})


@st.cache_data(ttl=90, max_entries=1, show_spinner=False)
def snapshot():
    return service.load_dashboard_snapshot(database())


@st.cache_data(ttl=90, max_entries=8, show_spinner=False)
def search(filters):
    return service.search_opportunities(database(), filters)


@st.cache_data(ttl=90, max_entries=1, show_spinner=False)
def health_data():
    return service.load_source_health(database())


@st.cache_data(ttl=60, max_entries=1, show_spinner=False)
def access_data():
    return external_access.load_access(database())


def render_access():
    st.subheader('Naturgy y AES · Registro de RS/SP')
    st.caption('Se verifica el portal público. Las compras privadas requieren evaluación e invitación; aún no están siendo extraídas.')
    company = st.selectbox('Empresa que se registrará', external_access.COMPANIES, key='external_access_company')
    try:
        saved = access_data()
        can_save = True
    except Exception:
        logging.getLogger(__name__).exception('Consulta de registros de proveedor fallida')
        st.warning('No se pudo consultar el seguimiento guardado. Los pasos oficiales siguen disponibles; intenta Actualizar vista.')
        saved, can_save = [], False
    view = pd.DataFrame(external_access.access_rows(saved, company))
    if not can_save:
        view['Estado'] = 'Sin consultar'
    st.dataframe(view, hide_index=True, use_container_width=True,
                 column_config={'Registro / pasos oficiales': st.column_config.LinkColumn(display_text='Abrir registro ↗'),
                                'Pendiente para acceder': st.column_config.TextColumn(width='large')})
    for source, guide in external_access.GUIDES.items():
        with st.expander(guide['name'] + ' · Pasos y seguimiento', expanded=False):
            st.write(guide['access'])
            for number, step in enumerate(guide['steps'], 1):
                st.write(f'{number}. {step}')
            st.link_button('Registro y pasos oficiales ↗', guide['registration_url'])
            st.link_button('Proceso oficial de proveedores ↗', guide['url'])
            row = next((r for r in saved if r['source'] == source and r['company'] == company), {})
            revision = row.get('updated_at') or 'new'
            with st.form(f'access_{source}_{company}_{revision}'):
                current = row.get('status', 'Pendiente')
                status = st.selectbox('Estado del trámite', external_access.STATUSES,
                                     index=external_access.STATUSES.index(current) if current in external_access.STATUSES else 0)
                notes = st.text_area('Notas de seguimiento', value=row.get('notes') or '', max_chars=1500)
                submitted = st.form_submit_button('Guardar seguimiento', disabled=not can_save)
            if submitted:
                try:
                    external_access.save_access(database(), source=source, company=company, status=status, notes=notes,
                        actor=st.session_state.get('username', 'usuario'), expected_updated_at=row.get('updated_at'))
                    access_data.clear()
                    st.session_state.external_access_saved = True
                    st.rerun()
                except ValueError as exc:
                    st.warning(str(exc)); access_data.clear()
                except Exception:
                    logging.getLogger(__name__).exception('Guardar seguimiento de proveedor falló')
                    st.error('No se pudo guardar. El seguimiento anterior se conserva; vuelve a intentarlo.')
            if row.get('updated_at'):
                st.caption('Guardado: ' + human_date(row['updated_at']) + ' · ' + row.get('updated_by', ''))
    if st.session_state.pop('external_access_saved', False):
        st.success('Seguimiento guardado en Supabase.')
    st.caption('Cambiar el estado registra tu avance; no crea cuentas, envía solicitudes ni conecta automáticamente portales privados.')


@st.cache_data(ttl=300, max_entries=4, show_spinner=False)
def detail_data(opportunity_id):
    return service.load_opportunity_detail(database(), opportunity_id), service.load_documents(database(), opportunity_id)


def parsed_json(value, default):
    try:
        return json.loads(value) if isinstance(value, str) else (value or default)
    except (TypeError, ValueError):
        return default


def human_date(value):
    if not value:
        return 'Sin confirmar'
    dt = pd.to_datetime(value, errors='coerce', utc=True)
    return dt.tz_convert('America/Panama').strftime('%d/%m/%Y %H:%M') if pd.notna(dt) else str(value)


def render_detail(row):
    data, documents = detail_data(row['id'])
    if not data:
        st.info('El registro no está disponible en esta consulta. Actualiza la vista.')
        return
    raw = parsed_json(data.get('raw_payload_json'), {})
    analysis = raw.get('document_analysis') or {}
    st.subheader(row['title'])
    st.caption(f"{service.SOURCE_LABELS.get(row['source'], row['source'])} · {row['external_id']} · {row.get('buyer', '')}")
    st.link_button('Abrir convocatoria oficial ↗', row['source_url'])
    st.write(row.get('review_reason', ''))
    keywords = parsed_json(data.get('matched_keywords_json'), [])
    if keywords:
        st.write('**Coincidencias:** ' + ', '.join(keywords))
    product_hits = raw.get('rir_product_matches') or []
    if product_hits:
        with st.expander('Productos RIR relacionados por palabras', expanded=False):
            for hit in product_hits:
                st.write(f"**{hit.get('ficha', '')} · {hit.get('name', '')}**")
                st.caption('Coincidencia en ' + hit.get('field', '') + ': ' + ', '.join(hit.get('terms', [])))
                st.write(hit.get('evidence', ''))
            st.caption('Son familias de productos por revisar. No confirma medidas, modelos ni cumplimiento de la ficha MINSA.')
    explicit = raw.get('explicit_fichas') or []
    st.caption('Fichas técnicas explícitas: ' + (', '.join(explicit) if explicit else 'No identificadas en el texto leído.'))
    st.caption('Una coincidencia de producto no confirma la equivalencia con una ficha MINSA ni la elegibilidad para ofertar.')
    description = data.get('description') or row['title']
    st.write(description)
    cols = st.columns(2)
    cols[0].write('**Registro:** ' + (data.get('registration_required') or 'Consultar las bases'))
    cols[1].write('**Presentación:** ' + (data.get('submission_channel') or 'Consultar las bases'))
    if data.get('eligibility'):
        st.write('**Requisitos de participación:** ' + data['eligibility'])
    st.caption('Última comprobación: ' + human_date(data.get('last_seen_at')))
    with st.expander('Documentos y evidencia', expanded=False):
        if row['source'] == 'acp_sli':
            st.caption('Para los documentos del Canal, abre primero la convocatoria oficial: SLI necesita la sesión pública del acto.')
        if not documents.empty:
            st.dataframe(documents[['title', 'url']].rename(columns={'title': 'Documento', 'url': 'Enlace'}),
                         hide_index=True, use_container_width=True,
                         column_config={'Enlace': st.column_config.LinkColumn('Enlace', display_text='Abrir ↗')})
        else:
            st.caption('Sin adjuntos recuperados. Consulta el enlace oficial para ver las bases.')
        status = analysis.get('status', 'listing')
        st.caption('Lectura del detalle: ' + {'ok': 'Completada', 'partial': 'Parcial', 'error': 'No disponible',
                   'pending': 'Pendiente', 'unreadable': 'Requiere revisión manual', 'listing': 'Datos del listado oficial'}.get(status, status))
        if analysis.get('attachments_total'):
            st.caption(f"Documentos interpretados: {analysis.get('attachments_read', 0)} de {analysis['attachments_total']}")
        if analysis.get('text'):
            st.text_area('Texto oficial recuperado', analysis['text'], height=280, disabled=True)


@st.dialog('Detalle de la oportunidad', width='large')
def detail_dialog(row):
    try:
        render_detail(row)
    except Exception:
        logging.getLogger(__name__).exception('Consulta de detalle externo fallida')
        st.error('No se pudo cargar el detalle. Puedes abrir el anuncio oficial desde la tabla.')


def render_sources(health):
    st.subheader('Fuentes y cobertura')
    st.caption('El monitor usa el orquestador existente. La caída de una fuente conserva su histórico y no detiene las demás.')
    for source in dict.fromkeys([*PORTALS, *health.get('source', pd.Series(dtype=str)).tolist()]):
        matches = health[health['source'].eq(source)] if not health.empty else pd.DataFrame()
        row = matches.iloc[0].to_dict() if not matches.empty else {}
        state = row.get('capture_status') or ('error' if row.get('last_error') else 'pending')
        marker = {'success': '🟢', 'partial': '🟠', 'error': '🔴', 'access_required': '🟠'}.get(state, '⚪')
        quantity = 'Requiere acceso' if source in external_access.GUIDES else f"{int(row.get('last_count') or 0):,} registros en última captura con datos"
        with st.expander(f"{marker} {service.SOURCE_LABELS.get(source, source)} · {quantity}", expanded=False):
            st.write({'success': 'Última captura completada', 'partial': 'Última captura parcial', 'error': 'Última captura fallida', 'access_required': 'Portal verificado · Acceso a licitaciones pendiente'}.get(state, 'Sin captura confirmada'))
            st.caption('Última comprobación: ' + human_date(row.get('updated_at')))
            st.caption('Último éxito: ' + human_date(row.get('last_success_at')))
            if row.get('coverage'):
                st.write(row['coverage'])
            if row.get('last_error'):
                # Source errors contain only public URLs, no database credentials.
                st.warning(str(row['last_error'])[:550])
            if source in PORTALS:
                url, explanation = PORTALS[source]
                st.write(explanation)
                st.link_button('Portal oficial ↗', url)
    st.info('Cobertura del listado público consultado. Los portales pueden exigir registro para presentar ofertas o descargar algunos documentos; eso se comprueba antes de participar.')


def main():
    st.set_page_config(page_title='Oportunidades externas', page_icon='🌐', layout='wide')
    apply_global_theme()
    require_page_access('pages/oportunidades_externas.py')
    st.title('Oportunidades externas')
    st.caption('Compras y convocatorias fuera de Panamá Compra, organizadas para RS/SP y RIR.')
    with st.spinner('Consultando oportunidades…'):
        try:
            _, last_run, overview, options = snapshot()
        except Exception:
            logging.getLogger(__name__).exception('Consulta de oportunidades externas fallida')
            st.error('No fue posible consultar Supabase. Los registros guardados se conservan. Vuelve a cargar esta página.')
            return
    metrics = st.columns(4)
    metrics[0].metric('Para evaluar', f"{overview.get('relevant', 0):,}")
    metrics[1].metric('Por revisar', f"{overview.get('review', 0):,}")
    metrics[2].metric('Histórico', f"{overview.get('historical', 0):,}")
    metrics[3].metric('Total sin duplicados', f"{sum(overview.get(k, 0) for k in ('relevant', 'review', 'historical', 'no_match')):,}")
    state = last_run.get('status')
    indicator = {'success': '🟢', 'partial': '🟠', 'error': '🔴', 'running': '🔵'}.get(state, '⚪')
    if state == 'running':
        st.caption(f"{indicator} Captura en curso desde {human_date(last_run.get('started_at'))}. Se mantienen disponibles los datos publicados.")
    else:
        st.caption(f"{indicator} Última corrida: {human_date(last_run.get('finished_at'))} · " +
                   {'success': 'Completada', 'partial': 'Completada con fuentes pendientes', 'error': 'Fallida'}.get(state, 'Pendiente'))
    if last_run.get('source_count'):
        st.caption(f"Fuentes consultadas en esa corrida: {last_run['source_count']}. Horario: todos los días a las 06:20, 12:20 y 18:20 (Panamá), con el servidor encendido.")
    actions = st.columns([1, 1, 3])
    if actions[0].button('Actualizar vista', use_container_width=True):
        snapshot.clear(); search.clear(); detail_data.clear(); health_data.clear(); access_data.clear()
        st.rerun()
    if actions[1].button('Solicitar captura', use_container_width=True):
        try:
            from sheets import get_client
            client, _ = get_client()
            request = queue_external_refresh(client, config_value('PC_MANUAL_SHEET_ID', '1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0'),
                                             str(st.session_state.get('username', 'usuario')))
            st.success('Solicitud registrada. El orquestador la ejecutará cuando esté disponible.')
        except Exception:
            logging.getLogger(__name__).exception('Solicitud manual de captura externa fallida')
            st.error('No se pudo registrar la solicitud en pc_manual. Las corridas programadas se mantienen.')
    area = st.radio('Explorar', ['Todas las oportunidades', 'RS/SP', 'RIR', 'Fuentes y cobertura', 'Registros y accesos'], horizontal=True)
    if area == 'Registros y accesos':
        render_access()
        return
    if area == 'Fuentes y cobertura':
        render_sources(health_data())
        return
    if area == 'RIR':
        st.caption('Busca por palabras y variantes de los productos de tus fichas en seguimiento, además de términos médicos generales. No exige el número de ficha en el anuncio. La ficha relacionada es una referencia para revisar el producto.')
    if area == 'RS/SP':
        with st.expander('Naturgy y AES · Acceso a licitaciones y pasos de registro', expanded=False):
            render_access()
    with st.expander('Filtros', expanded=False):
        with st.form('external_filters'):
            cols = st.columns(3)
            term = cols[0].text_input('Buscar producto, código o comprador')
            sources = cols[1].multiselect('Fuentes', list(service.SOURCE_LABELS), default=list(LOCAL_SOURCES),
                                         format_func=lambda value: service.SOURCE_LABELS[value])
            scope = cols[2].multiselect('Destino', ['Panamá', 'Región', 'Global'])
            cols = st.columns(3)
            dates = cols[0].checkbox('Filtrar fecha de publicación')
            start = cols[1].date_input('Desde', date.today() - timedelta(days=365))
            end = cols[2].date_input('Hasta', date.today())
            submitted = st.form_submit_button('Aplicar filtros')
        st.caption('Sin fecha oficial se usa la primera detección para el filtro. Los portales internacionales existentes están disponibles al cambiar Fuentes.')
    cols = st.columns([2, 2, 1])
    state_name = cols[0].selectbox('Mostrar', list(STATES))
    order_name = cols[1].selectbox('Ordenar por', list(SORTS))
    size = cols[2].selectbox('Filas por página', [25, 50, 100], index=1)
    if dates and start > end:
        st.warning('La fecha inicial debe ser anterior a la final.')
        return
    signature = (area, term, tuple(sources), tuple(scope), dates, str(start), str(end), state_name, order_name, size)
    if st.session_state.get('external_signature') != signature:
        st.session_state.external_page = 1
        st.session_state.external_signature = signature
    page = int(st.session_state.get('external_page', 1))
    filters = service.OpportunityFilters(search=term, sources=tuple(sources), scopes=tuple(scope),
        companies=() if area == 'Todas las oportunidades' else (area,),
        view='all' if area == 'Todas las oportunidades' and STATES[state_name] == 'current' else STATES[state_name],
        only_active=area == 'Todas las oportunidades' and STATES[state_name] == 'current',
        start_date=str(start) if dates else '', end_date=str(end) if dates else '',
        sort_by=SORTS[order_name], limit=size, offset=(page - 1) * size)
    try:
        frame = search(filters)
    except Exception:
        logging.getLogger(__name__).exception('Filtro de oportunidades externas fallido')
        st.error('No se pudo completar la consulta. Prueba otra vez con Actualizar vista.')
        return
    if frame.empty:
        if page > 1:
            st.session_state.external_page = 1
            st.rerun()
        st.info('Sin resultados con estos filtros. Consulta Por revisar, el histórico o amplía las fuentes.')
        return
    total = int(frame.iloc[0]['total_resultados'])
    pages = max(1, math.ceil(total / size))
    st.caption(f'{total:,} oportunidades · Página {page:,} de {pages:,}. El orden se aplica a todos los resultados.')
    if area == 'Todas las oportunidades':
        st.caption('Listado completo de las fuentes seleccionadas, incluyendo anuncios que no coinciden con RS/SP o RIR.')
    labels = {'source': 'Fuente', 'external_id': 'Código', 'title': 'Producto / oportunidad', 'buyer': 'Comprador',
              'publication_date': 'Publicación', 'deadline': 'Fecha límite', 'matched_company': 'Empresa',
              'estimated_value': 'Monto publicado', 'currency': 'Moneda', 'review_bucket': 'Situación',
              'source_url': 'Enlace oficial', 'country': 'País'}
    view = frame[list(labels)].rename(columns=labels)
    view['Fuente'] = frame.source.map(service.SOURCE_LABELS).fillna(frame.source)
    view['Situación'] = frame.review_bucket.map(service.VIEW_LABELS).fillna(frame.review_bucket)
    event = st.dataframe(view, hide_index=True, use_container_width=True, on_select='rerun', selection_mode='single-row',
        column_config={'Enlace oficial': st.column_config.LinkColumn(display_text='Abrir ↗'),
                       'Monto publicado': st.column_config.NumberColumn(format='localized'),
                       'Producto / oportunidad': st.column_config.TextColumn(width='large')}, key='external_table_' + str(hash(signature)) + '_' + str(page))
    st.caption('Selecciona una fila para abrir su detalle. Monto vacío significa que la fuente no publicó un presupuesto; no representa cero.')
    if event.selection.rows:
        detail_dialog(frame.iloc[event.selection.rows[0]].to_dict())
    nav = st.columns([1, 1, 3])
    if nav[0].button('← Anterior', disabled=page <= 1):
        st.session_state.external_page = page - 1; st.rerun()
    if nav[1].button('Siguiente →', disabled=page >= pages):
        st.session_state.external_page = page + 1; st.rerun()
    nav[2].download_button('Descargar esta página CSV', view.to_csv(index=False).encode('utf-8-sig'), 'oportunidades_externas.csv', 'text/csv')


main()
