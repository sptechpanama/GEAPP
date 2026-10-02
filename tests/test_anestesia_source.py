from unittest.mock import Mock

import pytest

from services.anestesia_source import explicit_ficha_evidence, public_open_status, source_is_closed


URL = 'https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-051598/0nM6ICc0JCL2AjN1UDMxojIpJye'
NUMBER = '2026-1-10-01-08-CL-051598'


def test_css_column_ocr_finds_explicit_ctni_without_using_description_or_classification():
    text = '''KIT CIRCUITO DE PACIENTE PARA MAQUINA ANESTESIA
43358
GLOBAL
CRÉDITO
TIEMPO DE ENTREGA:
PRESENTACION:
LUGAR DE ENTREGA:
VIGENCIA:
30 HABILES
UNIDAD
ALMACEN GENERAL
NO APLICA
CTNI:
FORMA DE ADJUDICACION:
FORMA DE PAGO:
'''
    assert {r['ficha'] for r in explicit_ficha_evidence(text)} == {'43358'}
    assert not explicit_ficha_evidence(text.replace('CTNI:', 'OTRO CAMPO:'))
    assert not explicit_ficha_evidence(text.replace('FORMA DE PAGO:', 'OTRA ETIQUETA:'))
    assert not explicit_ficha_evidence(text.replace('43358\nGLOBAL', '42272504\nGLOBAL'))
    assert not explicit_ficha_evidence('Clasificación 43358: Kit de anestesia')
    assert not explicit_ficha_evidence('CTNI: sin dato. Cantidad: 43358')
    assert {r['ficha'] for r in explicit_ficha_evidence('Ficha técnica: 43358\nCTNI #102625')} == {'43358', '102625'}


def current_source():
    return {'url': URL, 'number': NUMBER, 'flow': 1055606, 'process_type': 2,
            'closing': '28-09-2026 a 01-10-2026', 'info': {},
            'official_status': {'number': NUMBER, 'flow': 1055606, 'process_type': 2,
                'state_id': 8, 'state': 'Abierta', 'checked_at': '2026-10-01T19:30:00-05:00'}}


def test_today_without_hour_can_be_verified_by_fresh_exact_official_open_listing():
    assert not source_is_closed(current_source(), now='2026-10-01T19:32:00-05:00')[0]


@pytest.mark.parametrize('change', [
    {'checked_at': '2026-10-01T19:00:00-05:00'}, {'checked_at': '2026-10-01T20:00:00-05:00'},
    {'number': 'another-act'}, {'flow': 1055607}, {'process_type': 1}, {'state_id': None},
])
def test_today_without_hour_is_not_verified_using_stale_or_mismatched_state(change):
    source = current_source()
    source['official_status'].update(change)
    closed, reason = source_is_closed(source, now='2026-10-01T19:32:00-05:00')
    assert closed and 'Hay que confirmar' in reason
    assert 'Solo disponible para consulta histórica' not in reason


@pytest.mark.parametrize('change', [
    {'closing': '30-09-2026'}, {'closing': '01-10-2026 07:00 PM'},
    {'info': {'estado': 'Suspendido'}}, {'closing': ''},
])
def test_open_listing_never_overrides_explicit_closure_or_missing_deadline(change):
    source = {**current_source(), **change}
    assert source_is_closed(source, now='2026-10-01T19:32:00-05:00')[0]


@pytest.mark.parametrize('change,found', [({}, True), ({'idProcesosContratacionFlujos':1055607}, False),
    ({'numProceso':'other'}, False), ({'idEstado':15}, False), ({'idTipoProceso':1}, False)])
def test_official_listing_requires_exact_number_flow_type_and_active_state(change, found):
    row = {'numProceso': NUMBER, 'idProcesosContratacionFlujos':1055606, 'idTipoProceso':2,
           'idEstado':8, 'nombreRealizado':'Abierta', **change}
    client = Mock()
    client.post.return_value.json.return_value = {'status':1, 'result':{'registros':[row]}}
    result = public_open_status(URL, client=client)
    assert (result['state_id'] == 8) is found
    assert result['number'] == NUMBER
    assert client.post.call_args.kwargs['json']['filtro']['numProceso'] == NUMBER


@pytest.mark.parametrize('payload', [None, {'status':1,'result':None}, {'status':1,'result':{}}, {'status':0}])
def test_bad_listing_is_not_reported_as_closed_or_open(payload):
    client = Mock()
    client.post.return_value.json.return_value = payload
    with pytest.raises(ValueError, match='verificar el estado oficial'):
        public_open_status(URL, client=client)
