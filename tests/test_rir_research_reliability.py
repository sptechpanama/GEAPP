from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from services.rir_research_snapshots import ResearchSnapshotStore
from services.rir_supplier_research import (
    RIR_ACT_SHEETS, RIR_RESEARCH_SHEETS, RIR_TOP10_SHEET, RIR_RESEARCH_SHEET,
    assess_research_validity, build_research_opportunities, pack_frame,
    read_current_research_acts, read_research_safely, validate_publication,
    latest_top10_snapshot, evidence_date, followup_date,
)
from test_rir_live_opportunities import ROW, LIVE, NOW, READY

HEADERS = ['enlace', 'fecha', 'Fecha de Actualización', 'Fichas sin requisitos',
           'Tipo de adjudicación', 'Tipo de acto sin requisitos', 'Descartar',
           'Fichas con requisitos', 'Fichas por verificar']
DATA = [ROW['enlace_acto'], LIVE['fecha_cierre'], LIVE['verificado_en'], '41364',
        'Global', 'Solo fichas sin requisitos', '', '', '']


class Sources:
    def __init__(self, failed=(), temporary=0):
        self.failed = set(failed)
        self.temporary = temporary
        self.calls = []

    def values_batch_get(self, ranges):
        name = ranges[0].split("'")[1]
        self.calls.append(name)
        if name in self.failed:
            raise TimeoutError('private transport details')
        if self.temporary:
            self.temporary -= 1
            raise TimeoutError('first call failed')
        if len(ranges) == 1:
            return {'valueRanges': [{'values': [HEADERS]}]}
        return {'valueRanges': [{'values': [[value]]} for value in DATA]}


def test_one_source_failure_keeps_other_sources_and_records_real_coverage():
    reader = Sources(failed=[RIR_ACT_SHEETS[1]])
    result = read_current_research_acts(reader, sleeper=lambda _: None)
    assert len(result) == 2
    assert result.attrs['source_states'][RIR_ACT_SHEETS[1]] == 'error'
    assert len(result.attrs['source_errors']) == 1
    assert 'private' not in result.attrs['source_errors'][0]


def test_source_recovers_after_transient_failure_without_duplicates():
    result = read_current_research_acts(Sources(temporary=1), sleeper=lambda _: None)
    assert len(result) == 3 and not result.attrs['source_errors']


def test_outage_in_new_session_retains_dates_and_never_claims_backup_is_current(tmp_path):
    store = ResearchSnapshotStore('source', tmp_path / 'snapshots.db')
    read_current_research_acts(Sources(), store=store, sleeper=lambda _: None)
    new_session = ResearchSnapshotStore('source', tmp_path / 'snapshots.db')
    result = read_current_research_acts(Sources(failed=RIR_ACT_SHEETS), store=new_session, sleeper=lambda _: None)
    assert len(result) == 3 and result['lectura_respaldo'].all()
    assert set(result['verificado_en']) == {LIVE['verificado_en']}
    checked = assess_research_validity(pd.DataFrame([ROW]), result, now=NOW)
    assert checked.iloc[0].vigencia == 'Por verificar'


def test_remote_backup_survives_loss_of_local_cache_and_keeps_original_dates(tmp_path):
    remote = create_engine(f"sqlite:///{tmp_path / 'remote.db'}")
    first = ResearchSnapshotStore('source', tmp_path / 'first.db', engine=remote)
    payload = pack_frame(pd.DataFrame([LIVE]))
    first.save('acts', payload, checked_at='2026-09-17T12:00:00-05:00')
    first.save('acts', payload, checked_at='2026-09-18T12:00:00-05:00')
    second = ResearchSnapshotStore('source', tmp_path / 'new.db', engine=remote)
    saved, date = second.load('acts')
    assert saved == payload and date == '2026-09-17T12:00:00-05:00'
    other_spreadsheet = ResearchSnapshotStore('different', tmp_path / 'other.db', engine=remote)
    assert other_spreadsheet.load('acts') is None


def test_later_top_publication_cannot_renew_old_supplier_confirmations():
    detail = {**ROW, **READY, 'actualizado_en': '2026-09-14T10:00:00-05:00'}
    top = {**detail, 'ranking': 1, 'fecha_corte': '2026-09-17', 'actualizado_en': NOW,
           'numeros_preliminares': 'new apparent profit', 'resultado_cumplimiento': 'Cumple verificado'}
    accepted, _ = build_research_opportunities(pd.DataFrame([detail]), pd.DataFrame([top]), pd.DataFrame([LIVE]), now=NOW)
    assert accepted.iloc[0].situacion == 'Para cotizar o confirmar'
    assert accepted.iloc[0].fecha_estudio == detail['actualizado_en']
    assert 'numeros_preliminares' not in accepted


def test_newer_editorial_top_cannot_revive_a_withdrawn_line():
    top = {**ROW, 'ranking': 1, 'fecha_corte': '2026-09-17', 'actualizado_en': NOW}
    accepted, removed = build_research_opportunities(pd.DataFrame([{**ROW, 'estado_investigacion': 'No vigente — cierre confirmado'}]),
                                                     pd.DataFrame([top]), pd.DataFrame([LIVE]), now=NOW)
    assert accepted.empty and removed.iloc[0].vigencia == 'No vigente'


def test_partial_committed_top_keeps_previous_valid_publication_across_sessions(tmp_path):
    frames = {name: pd.DataFrame({'ficha': ['41364']}) for name in RIR_RESEARCH_SHEETS}
    store = ResearchSnapshotStore('source', tmp_path / 'reads.db')
    previous = read_research_safely(lambda: frames, store=store)
    incomplete = {RIR_TOP10_SHEET: pd.DataFrame([
        {'fecha_corte': '2026-09-17', 'ranking': 0, 'estado': 'publicacion=completa;filas_publicadas=2'},
        {'fecha_corte': '2026-09-17', 'ranking': 1, 'ficha': '41364'},
    ])}
    def reader():
        validate_publication(incomplete)
        return incomplete
    fallback = read_research_safely(reader, store=ResearchSnapshotStore('source', tmp_path / 'reads.db'))
    assert fallback.using_previous and fallback.checked_at == previous.checked_at
    assert fallback.frames[RIR_RESEARCH_SHEET].iloc[0].ficha == '41364'


def test_conflicting_same_date_same_line_prices_are_rejected():
    frames = {RIR_RESEARCH_SHEET: pd.DataFrame([{**ROW, 'precio_proveedor': '50'}, {**ROW, 'precio_proveedor': '500'}])}
    with pytest.raises(ValueError, match='contradictorias'):
        validate_publication(frames)


def test_complete_two_position_cut_and_explicit_zero_cut_are_valid():
    for count in (0, 2):
        frames = {RIR_TOP10_SHEET: pd.DataFrame([
            {'fecha_corte': '2026-09-17', 'ranking': 0, 'estado': f'publicacion=completa;filas_publicadas={count}'},
            *[{'fecha_corte': '2026-09-17', 'ranking': n} for n in range(1, count + 1)],
        ])}
        validate_publication(frames)


def test_same_day_shorter_rerun_does_not_resurrect_old_positions():
    frame = pd.DataFrame([
        *[{'fecha_corte': '2026-09-17', 'ranking': n, 'ficha': str(n),
           'actualizado_en': '2026-09-17T08:00:00-05:00'} for n in range(1, 11)],
        {'fecha_corte': '2026-09-17', 'ranking': 0, 'estado': 'publicacion=completa;filas_publicadas=2',
         'actualizado_en': NOW},
        *[{'fecha_corte': '2026-09-17', 'ranking': n, 'ficha': str(n), 'actualizado_en': NOW} for n in (1, 2)],
    ])
    validate_publication({RIR_TOP10_SHEET: frame})
    assert latest_top10_snapshot(frame).ranking.tolist() == [1, 2]


def test_explicit_commercial_review_and_followup_have_independent_clocks():
    row = {**ROW, 'observaciones': '[FECHAS_RIR_V2]\nfecha_revision_comercial=2026-09-14T10:00:00-05:00\n'
           'fecha_ultimo_seguimiento=2026-09-17T13:00:00-05:00\n[/FECHAS_RIR_V2]'}
    assert evidence_date(row).isoformat() == '2026-09-14T10:00:00-05:00'
    assert followup_date(row).isoformat() == '2026-09-17T13:00:00-05:00'


def test_missing_database_driver_is_reported_as_dependency_not_missing_schema(monkeypatch):
    from services import inteligencia_proveedores_v3 as module
    def missing(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'psycopg'")
    monkeypatch.setattr(module, 'create_engine', missing)
    with pytest.raises(module.AnalyticsUnavailable) as error:
        module.AnalyticsRepository.connect(database_url='postgresql+psycopg://example.invalid/db')
    assert error.value.reason == 'dependency'
