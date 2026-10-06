from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from services.anestesia_control import DOCUMENTS, create_control, expiry_status, initial_expiry, latest_documents
from services.anestesia_docs import file_hash
from test_anestesia_delivery import Request

TODAY = date(2026, 10, 5)


@pytest.mark.parametrize('warning', [10, 30])
@pytest.mark.parametrize('delta,result', [(-1, 'Vencido'), (0, 'Por vencer'), (1, 'Por vencer')])
def test_expiration_and_today_boundary(warning, delta, result):
    assert expiry_status(str(TODAY + timedelta(days=delta)), warning, today=TODAY) == result


@pytest.mark.parametrize('warning', [10, 30])
def test_exact_notice_boundary_and_next_day(warning):
    assert expiry_status(str(TODAY + timedelta(days=warning)), warning, today=TODAY) == 'Por vencer'
    assert expiry_status(str(TODAY + timedelta(days=warning+1)), warning, today=TODAY) == 'Vigente'


def test_unknown_date_is_not_treated_as_valid_but_dash_has_no_expiration():
    assert expiry_status('-', 10, today=TODAY) == 'Sin vencimiento'
    assert expiry_status('', 10, today=TODAY) == 'Pendiente'
    assert expiry_status('invalid', 10, today=TODAY) == 'Pendiente'


def test_initial_dates_use_original_expiry_and_registry_age_is_labelled_as_control():
    expiry, note = initial_expiry({'kind':'registro_publico','issued':'2026-09-16','no_expiry_confirmed':True})
    assert expiry == date(2027,9,16)
    assert 'No es un vencimiento impreso' in note and '051191' in note
    assert initial_expiry({'kind':'css','expires':'2026-10-31'})[0] == date(2026,10,31)
    assert initial_expiry({'kind':'aviso_operacion','no_expiry_confirmed':True})[0] is None
    with pytest.raises(ValueError): initial_expiry({'kind':'criterio_tecnico'})


def test_latest_aliases_and_missing_originals_do_not_silently_drop_document():
    rows = [{'kind':kind,'created_at':'2026-10-01'} for kind, *_ in DOCUMENTS]
    rows[-1]['kind'] = 'otro:Licencia de operaciones MINSA'
    rows[6]['kind'] = 'disposicion'
    selected = latest_documents(rows)
    assert len(selected) == 11 and 'licencia_minsa' in selected and 'metodo_destruccion' in selected
    with pytest.raises(ValueError, match='Faltan originales'): latest_documents(rows[:-1])


def test_existing_control_does_not_overwrite_manually_edited_dates():
    api = Mock()
    api.list.return_value = Request(lambda: {'files':[{'id':'control', 'mimeType':'application/vnd.google-apps.spreadsheet'}]})
    sheet = Mock()
    sheet.get.return_value = Request(lambda: {'sheets':[{'properties':{'sheetId':42, 'title':'Documentos'}}]})
    sheet.values.return_value.get.return_value = Request(lambda: {'values':[[],[],[],['Nombre del documento','Tipo','Fecha de vencimiento','Descargar'],['CSS','Otros documentos','31/10/2026','Descargar']]})
    storage = SimpleNamespace(drive=SimpleNamespace(files=lambda:api), sheets=SimpleNamespace(spreadsheets=lambda:sheet))
    assert create_control(storage, 'documents', {})['id'] == 'control'
    api.create.assert_not_called()
    sheet.batchUpdate.assert_not_called()


def test_existing_unrelated_sheet_structure_is_never_overwritten():
    api, sheet = Mock(), Mock()
    api.list.return_value = Request(lambda: {'files':[{'id':'control', 'mimeType':'application/vnd.google-apps.spreadsheet'}]})
    sheet.get.return_value = Request(lambda: {'sheets':[{'properties':{'sheetId':42, 'title':'Hoja de usuario'}}]})
    sheet.values.return_value.get.return_value = Request(lambda: {'values':[['Datos de usuario']]})
    storage = SimpleNamespace(drive=SimpleNamespace(files=lambda:api), sheets=SimpleNamespace(spreadsheets=lambda:sheet))
    with pytest.raises(ValueError, match='No se sobrescribieron'): create_control(storage, 'documents', {})
    api.create.assert_not_called()
    sheet.batchUpdate.assert_not_called()


@pytest.mark.parametrize('existing_blank', [False, True])
def test_native_control_has_exact_rows_download_links_date_formats_and_sort_safe_notice_days(existing_blank):
    api = Mock()
    api.list.return_value = Request(lambda: {'files':[{'id':'new-control', 'mimeType':'application/vnd.google-apps.spreadsheet'}] if existing_blank else []})
    api.get.side_effect = lambda fileId, **kw: Request(lambda: {
        'id':fileId, 'name':fileId, 'mimeType':'application/pdf', 'parents':['documents'],
        'webContentLink':'https://drive.google.com/download/'+fileId,
        'webViewLink':'https://drive.google.com/drive/folders/'+fileId})
    api.create.return_value = Request(lambda: {'id':'new-control','mimeType':'application/vnd.google-apps.spreadsheet'})
    sheet = Mock()
    sheet.get.return_value = Request(lambda: {'sheets':[{'properties':{'sheetId':42, 'title':'Hoja 1'}}]})
    sheet.values.return_value.get.return_value = Request(lambda: {'values':[]})
    written = []
    sheet.batchUpdate.side_effect = lambda **kw: Request(lambda: written.append(deepcopy(kw['body'])))
    data = b'original bytes'
    storage = SimpleNamespace(drive=SimpleNamespace(files=lambda:api),
        sheets=SimpleNamespace(spreadsheets=lambda:sheet), get_bytes=lambda ident:data)
    docs = {kind:{'kind':kind,'file_id':kind,'sha256':file_hash(data),'expires':'2026-10-31'} for kind,*_ in DOCUMENTS}
    docs['registro_publico'] = {**docs['registro_publico'], 'expires':'', 'issued':'2026-09-16'}
    docs['catalogo'] = {**docs['catalogo'], 'expires':'', 'no_expiry_confirmed':True}
    create_control(storage, 'documents', docs)
    if existing_blank:
        api.create.assert_not_called()
    requests = written[0]['requests']
    assert all(len(r) == 1 for r in requests)
    values = next(r['updateCells']['rows'] for r in requests if r.get('updateCells', {}).get('range', {}).get('startRowIndex') == 4)
    assert len(values) == 11 and all(len(r['values']) == 5 for r in values)
    assert values[2]['values'][2]['userEnteredValue'] == {'stringValue':'-'}
    assert all('HYPERLINK' in r['values'][3]['userEnteredValue']['formulaValue'] for r in values)
    assert values[8]['values'][4]['userEnteredValue']['numberValue'] == 10
    assert values[3]['values'][4]['userEnteredValue']['numberValue'] == 30
    assert next(r['setBasicFilter']['filter']['range'] for r in requests if 'setBasicFilter' in r)['endColumnIndex'] == 5
    rules = [r['addConditionalFormatRule']['rule']['booleanRule']['condition']['values'][0]['userEnteredValue'] for r in requests if 'addConditionalFormatRule' in r]
    assert len(rules) == 3 and any('$E5' in r for r in rules) and all('ISNUMBER' in r for r in rules)
    assert any(r.get('updateSpreadsheetProperties',{}).get('properties',{}).get('timeZone') == 'America/Panama' for r in requests)
