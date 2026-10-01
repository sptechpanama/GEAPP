from datetime import date
import fitz
import pytest

from services.anestesia_docs import document_status, file_hash
from services.anestesia_health import certificate_content_check, library_health

TODAY = date(2026, 9, 30)


def certificate(kind='css', **changes):
    return {'id': 'current', 'kind': kind, 'company': 'RIR MEDICAL ENGINEERING',
            'issued': '2026-09-23', 'expires': '2026-09-30', 'created_at': '2026-09-29',
            'verified': True, 'evidence': 'Pagina 1, fechas y titular impresos',
            'file_id': 'file', 'sha256': 'hash', 'catalogs': 'K,C', 'fichas': '43358',
            'act': '', **changes}


def pdf_text(text):
    with fitz.open() as pdf:
        pdf.new_page().insert_text((40, 40), text)
        return pdf.tobytes()


def row(library, name='Paz y salvo CSS / no cotizante', **kwargs):
    return next(r for r in library_health(library, as_of=TODAY, **kwargs) if r['Documento'] == name)


def test_opening_report_includes_missing_base_documents_without_an_existing_case():
    report = library_health([], as_of=TODAY)
    assert len(report) == 12
    assert {r['Estado'] for r in report} == {'Falta', 'Falta · por acto'}


@pytest.mark.parametrize('expiry,status', [('2026-09-29','Vencido'),('2026-09-30','Vence hoy'),
                                          ('2026-10-07','Vence pronto'),('2026-10-08','Vigente documentalmente')])
def test_expiration_uses_panama_control_date_including_today(expiry, status):
    assert row([certificate(expires=expiry)])['Estado'] == status


def test_newest_failed_replacement_does_not_show_old_valid_as_current():
    old = certificate(id='old', created_at='2026-09-01', expires='2027-01-01')
    new = certificate(verified=False, expires='2026-10-30')
    assert row([new, old])['Estado'] == 'Pendiente de verificar'


def test_registry_uses_researched_43358_acceptance_deadline_without_changing_original():
    doc = certificate('registro_publico', expires='', no_expiry_confirmed=True)
    result = row([doc], 'Certificado del Registro Público')
    assert result['Estado'] == 'Vigente documentalmente'
    assert result['Vence'] == '2027-09-23'
    assert doc['expires'] == ''
    assert document_status(doc, {'kind':'registro_publico'}, as_of=TODAY, catalog='K', act='')['estado'] == 'Vigente documentalmente'


def test_other_act_declaration_is_never_reported_reusable():
    doc = certificate('retorsion', act='ACTO-ANTERIOR', notarized=True)
    assert row([doc], 'Medidas de retorsión notarizadas')['Estado'] == 'Falta · por acto'


def test_catalog_c_does_not_reuse_k_only_document():
    doc = certificate('criterio_tecnico', catalogs='K', expires='2031-09-15')
    assert row([doc], 'Criterio técnico', catalog='C')['Estado'] == 'Falta'
    assert row([doc], 'Criterio técnico', catalog='K')['Estado'] == 'Vigente documentalmente'


def test_css_pdf_dates_and_company_are_verified_not_upload_date():
    data = pdf_text('CAJA DEL SEGURO SOCIAL\nNumero patronal: 1\nRIR MEDICAL ENGINEERING\nGenerado: 2026-09-23\nValido hasta: 2026-09-30')
    doc = certificate()
    assert not certificate_content_check(data, doc)['errors']
    doc['expires'] = '2027-09-30'
    assert certificate_content_check(data, doc)['errors']


@pytest.mark.parametrize('title,company', [('COTIZACION','RIR MEDICAL ENGINEERING'),
                                        ('CAJA DEL SEGURO SOCIAL\nNumero patronal: 1','OTRA EMPRESA')])
def test_wrong_pdf_cannot_be_approved_by_checkbox(title, company):
    data = pdf_text(f'{title}\n{company}\nGenerado: 2026-09-23\nValido hasta: 2026-09-30')
    assert certificate_content_check(data, certificate())['errors']


def test_dgi_disclaimer_does_not_make_it_a_css_certificate():
    data = pdf_text('DIRECCION GENERAL DE INGRESOS\nRIR MEDICAL ENGINEERING\n22/09/2026\n20/10/2026\nSIN INFORMACION DE LA CAJA DEL SEGURO SOCIAL')
    assert not certificate_content_check(data, certificate('dgi', issued='2026-09-22', expires='2026-10-20'))['errors']
    assert certificate_content_check(data, certificate('css', issued='2026-09-22', expires='2026-10-20'))['errors']


def test_editing_dates_after_validation_invalidates_fingerprint():
    data = pdf_text('CAJA DEL SEGURO SOCIAL\nNumero patronal: 1\nRIR MEDICAL ENGINEERING\n2026-09-23\nValido hasta: 2026-09-30')
    doc = certificate(sha256=file_hash(data))
    doc['content_validation'] = certificate_content_check(data, doc)
    assert document_status(doc, {'kind':'css'}, as_of=TODAY, catalog='K', act='')['estado'] == 'Vigente documentalmente'
    doc['expires'] = '2027-09-30'
    assert document_status(doc, {'kind':'css'}, as_of=TODAY, catalog='K', act='')['estado'] == 'Bloqueado'


@pytest.mark.parametrize('data', [b'', b'not a PDF'])
def test_unreadable_pdf_rejected_before_drive_upload(data):
    with pytest.raises(ValueError, match='PDF legible'):
        certificate_content_check(data, certificate())


def test_scanned_css_can_be_checked_with_ocr_without_modifying_original():
    data = pdf_text('')
    text = 'CAJA DEL SEGURO SOCIAL Numero patronal: 1 RIR MEDICAL ENGINEERING Generado: 2026-09-23 Valido hasta: 2026-09-30'
    assert certificate_content_check(data, certificate())['errors']
    checked = certificate_content_check(data, certificate(), ocr_text=text)
    assert not checked['errors'] and checked['sha256'] == file_hash(data)
    assert checked['unreadable_pages'] == [1]
