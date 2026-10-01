"""Expiry boundaries and current-tender precedence for the 43358 registry policy."""
from copy import deepcopy
from datetime import date

import pytest

from services.anestesia_docs import (COMPANY, document_status, document_expiry,
    prepare_offer_config, public_registry_age, registry_policy, base_requirements)
from services.anestesia_health import library_health


def registry(**changes):
    return {'id': 'registry', 'kind': 'registro_publico', 'company': COMPANY,
            'issued': '2025-09-30', 'expires': '', 'no_expiry_confirmed': True,
            'verified': True, 'evidence': 'Original, página 1, emisión comprobada',
            'file_id': 'original', 'sha256': 'hash', 'created_at': '2026-09-01', **changes}


@pytest.mark.parametrize('text,expected', [
    ('Registro Público: antigüedad máxima de seis meses.', 6),
    ('Registro Público: vigencia no mayor de un (I) año.', 12),
    ("Registro Público: vigencia no mayor de un ('1) año.", 12),
    ('Registro Público: fecha no mayor de (6) meses.', 6),
    ('Registro Público: certif¡cación expedida dentro de un año inmediatamente anterior a su presentación.', 12),
    ('Registro Público: fecha de expedición no mayor de tres (3) meses.', 3),
    ('Registro Público: vigencia no superior a 2 meses.', 2),
    ('Registro Público: antigüedad máxima de doce meses. Modificación: Registro Público, antigüedad máxima de seis meses.', 6),
    ('Registro Público vigente. Poder de representación con una antigüedad no mayor de 3 meses.', None),
    ('Registro Público vigente. Paz y salvo CSS: vigencia no mayor de 1 mes.', None),
    ('Registro Público: vigencia no mayor de un (1) año. Declaración jurada, antigüedad máxima de 3 meses.', 12),
    ('Registro Público vigente. El kit tiene vida útil mínima de 24 meses.', None),
])
def test_registry_parser_does_not_confuse_other_certificates(text, expected):
    assert public_registry_age(text)[0] == expected


@pytest.mark.parametrize('source,expected', [
    ({}, 12), ({'registry_max_months': 24}, 12), ({'registry_max_months': 6}, 6),
    ({'registry_max_months': '3'}, 3), ({'registry_max_months': 'invalid'}, 12),
    ({'registry_max_months': 12, 'attachments': [{'name': 'Adenda.pdf', 'text': 'Registro Público: antigüedad máxima de tres (3) meses.'}]}, 3),
])
def test_shorter_current_annex_precedes_defaults_and_cannot_be_overridden(source, expected):
    config = {'registry_max_months': 99, 'registry_rule_evidence': '', 'extra_requirements': ['Requisito previo']}
    old = deepcopy(config)
    result = prepare_offer_config(source, config)
    assert result['registry_max_months'] == registry_policy(source)[0] == expected
    assert result['registry_rule_evidence']
    assert next(r for r in base_requirements(source) if r['kind'] == 'registro_publico')['max_age_months'] == expected
    assert result['extra_requirements'] == ['Requisito previo'] and config == old


@pytest.mark.parametrize('issued,expiry', [('2024-02-29', '2025-02-28'), ('2025-09-30', '2026-09-30'),
                                         ('2026-09-16', '2027-09-16')])
def test_calendar_year_not_365_days_or_upload_date(issued, expiry):
    doc = registry(issued=issued, created_at='2099-01-01')
    assert str(document_expiry(doc, {'kind': 'registro_publico'})) == expiry
    assert doc['expires'] == ''


@pytest.mark.parametrize('as_of,status,health_status', [
    (date(2026, 9, 22), 'Vigente documentalmente', 'Vigente documentalmente'),
    (date(2026, 9, 23), 'Vigente documentalmente', 'Vence pronto'),
    (date(2026, 9, 30), 'Vigente documentalmente', 'Vence hoy'),
    (date(2026, 10, 1), 'Bloqueado', 'Vencido'),
])
def test_health_and_generation_share_the_same_expiration(as_of, status, health_status):
    doc = registry()
    check = document_status(doc, {'kind': 'registro_publico'}, as_of=as_of, catalog='K', act='')
    report = next(r for r in library_health([doc], as_of=as_of) if r['Documento'] == 'Certificado del Registro Público')
    assert check['estado'] == status and report['Estado'] == health_status
    assert check['vence'] == report['Vence'] == '2026-09-30'


def test_printed_expiration_precedes_age_limit_but_never_extends_it():
    assert document_expiry(registry(expires='2026-07-01'), {}) == date(2026, 7, 1)
    assert document_expiry(registry(expires='2028-01-01'), {}) == date(2026, 9, 30)
    assert document_expiry(registry(issued='2026-08-31'), {'max_age_months': 1}) == date(2026, 9, 30)


@pytest.mark.parametrize('changes', [{'issued': ''}, {'issued': '2027-01-01'}, {'verified': False}, {'evidence': ''}])
def test_automatic_expiry_does_not_approve_missing_or_unverified_data(changes):
    result = document_status(registry(**changes), {'kind': 'registro_publico'},
                             as_of=date(2026, 9, 30), catalog='K', act='')
    assert result['estado'] == 'Bloqueado'
