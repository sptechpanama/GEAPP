import pandas as pd
import pytest

from services.rir_supplier_research import (
    _deadline, assess_research_validity, build_research_opportunities, research_health,
)

NOW = "2026-09-17T14:00:00-05:00"
ACT = "2026-0-12-19-08-CL-041935"
ROW = dict(numero_acto=ACT, ficha="41364", renglon="1", nombre_ficha="Carro de emergencia",
           enlace_acto=f"https://www.panamacompra.gob.pa/Inicio/#/{ACT}",
           estado_investigacion="Parcial", actualizado_en="2026-09-17T10:00:00-05:00",
           proveedor_potencial="Fabricante", contacto_potencial="https://supplier.example/product",
           observaciones="Pendientes:\n• precio y stock\nPróxima acción: Solicitar cotización.\n[FECHAS_RIR_V1]\nfecha_cierre=2026-09-22\nfecha_cierre_hora=null\n[/FECHAS_RIR_V1]",
           fuentes="https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha=3368&idparam=0")
LIVE = dict(numero_acto=ACT, fecha_cierre="22-09-2026 - 07:00 AM a 02:00 PM",
            fichas_sin_requisitos="41364", tipo_acto="Solo fichas sin requisitos",
            tipo_adjudicacion="Global", verificado_en="2026-09-17 09:05:00")


def board(row=None, live=None, top=None, now=NOW):
    return build_research_opportunities(pd.DataFrame([{**ROW, **(row or {})}]),
                                       top, pd.DataFrame([{**LIVE, **(live or {})}]), now=now)


@pytest.mark.parametrize("value,expected", [
    ("17-09-2026 - 07:00 AM a 11:00 AM", "2026-09-17T11:00:00-05:00"),
    ("17-09-2026 hasta 02:00 PM", "2026-09-17T14:00:00-05:00"),
    ("17/09/2026 - 08:00 a.m. a 03:30 p.m.", "2026-09-17T15:30:00-05:00"),
    ("16-09-2026 08:00 AM a 17-09-2026 12:00 PM", "2026-09-17T12:00:00-05:00"),
    ("17-09-2026 hasta 12:00 AM", "2026-09-17T00:00:00-05:00"),
    ("17-09-2026 hasta 14:15:30", "2026-09-17T14:15:30-05:00"),
    ("2026-09-17T20:00:00Z", "2026-09-17T15:00:00-05:00"),
])
def test_official_close_formats(value, expected):
    closed, exact = _deadline(value)
    assert exact and closed.isoformat() == expected


@pytest.mark.parametrize("value", ["No Disponible", "", "31-02-2026", "17-09-2026 hasta 25:90 PM"])
def test_invalid_dates_do_not_invent_a_closing_hour(value):
    assert not _deadline(value)[1]


def test_latest_research_appears_without_a_published_top_or_quote():
    rows, removed = board()
    assert len(rows) == 1 and removed.empty
    row = rows.iloc[0]
    assert row.situacion == "Para cotizar o confirmar"
    assert "precio y stock" in row.que_falta
    assert row.accion_inmediata == "Solicitar cotización."
    assert row.cierre_verificado == "2026-09-22T14:00:00-05:00"
    assert "3368" in row.enlace_ficha_minsa


def test_expired_top_does_not_prevent_current_detailed_opportunities():
    top = pd.DataFrame([{**ROW, "numero_acto": "old", "fecha_corte": "2026-09-14", "ranking": 1,
                         "fecha_cierre": "2026-09-14"}])
    rows, removed = board(top=top)
    assert rows.numero_acto.tolist() == [ACT]
    assert len(removed) == 1


def test_new_research_replaces_old_price_and_compliance_for_same_line():
    top = pd.DataFrame([{**ROW, "fecha_corte": "2026-09-16", "ranking": 1,
                        "actualizado_en": "2026-09-16T12:00:00-05:00", "resultado_cumplimiento": "Cumple verificado",
                        "numeros_preliminares": "Margen viejo", "enlace_producto_recomendado": "https://old.example/product"}])
    rows, _ = board(top=top)
    row = rows.iloc[0]
    assert row.origen_evaluacion == "Investigación detallada"
    assert row.resultado_cumplimiento == "Pendiente de confirmar"
    assert row.enlace_producto_recomendado == ROW["contacto_potencial"]
    assert "numeros_preliminares" not in rows


def test_latest_exact_line_version_wins_and_withdrawal_does_not_revive():
    original = pd.DataFrame([ROW, {**ROW, "actualizado_en": "2026-09-17T11:00:00-05:00", "estado_investigacion": "No vigente"},
                              {**ROW, "renglon": "2"}])
    copy = original.copy(deep=True)
    rows, removed = build_research_opportunities(original, None, pd.DataFrame([LIVE]), now=NOW)
    assert rows.renglon.tolist() == ["2"]
    assert removed.renglon.tolist() == ["1"]
    pd.testing.assert_frame_equal(original, copy)


@pytest.mark.parametrize("changes", [
    {"fichas_sin_requisitos": "100000"},
    {"fichas_con_requisitos": "41364 (RS)", "tipo_adjudicacion": "Renglón"},
    {"fichas_por_verificar": "41364", "tipo_adjudicacion": "Parcial"},
    {"tipo_acto": "Acto mixto", "tipo_adjudicacion": "Global"},
    {"tipo_acto": "Acto mixto", "tipo_adjudicacion": "No identificado"},
    {"verificado_en": "2026-09-14 09:00:00"},
    {"verificado_en": "2026-09-18 09:00:00"},
    {"descartar": "TRUE"},
])
def test_flexibility_does_not_relax_regulatory_or_source_validity(changes):
    assert board(live=changes)[0].empty


def test_eligible_mixed_partial_act_remains_included():
    assert len(board(live={"tipo_acto": "Acto mixto", "tipo_adjudicacion": "Por renglón", "fichas_con_requisitos": "12345 (CT)"})[0]) == 1


@pytest.mark.parametrize("changes", [
    {"resultado_cumplimiento": "No cumple"},
    {"estado_investigacion": "Sin proveedor verificable"},
    {"observaciones": "El acto es global y contiene una camilla que no corresponde a la ficha."},
    {"observaciones": "La placa no es sustituto admisible de la punta."},
    {"observaciones": "Falso positivo de cilindro de motor."},
    {"observaciones": "Precio público 4000 antes de flete excede referencia."},
    {"renglon": ""},
    {"contacto_potencial": ""},
])
def test_proven_blockers_do_not_enter_the_candidate_selection(changes):
    assert board(row=changes)[0].empty


def test_optional_documents_missing_do_not_hide_a_candidate():
    rows, _ = board(row={"fuentes": "", "observaciones": "Falta manual español y garantía. Fuera del Top."},
                    live={"verificado_en": "2026-09-17 13:00:00"})
    assert len(rows) == 1 and "CTNI" in rows.iloc[0].que_falta


def test_multiple_ctni_links_are_not_guessed():
    rows, _ = board(row={"fuentes": ROW["fuentes"] + "; https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha=999&idparam=0"})
    assert rows.iloc[0].enlace_ficha_minsa == ""


def test_candidate_expires_automatically_without_another_write():
    assert not board(now="2026-09-22T13:59:00-05:00", live={"verificado_en": "2026-09-22 12:00:00"})[0].empty
    assert board(now="2026-09-22T14:00:00-05:00", live={"verificado_en": "2026-09-22 12:00:00"})[0].empty


def test_missing_live_source_does_not_claim_candidates_are_current():
    rows, removed = build_research_opportunities(pd.DataFrame([ROW]), None, None, now=NOW)
    assert rows.empty and len(removed) == 1


READY = {"situacion": "Lista para ofertar", "cumplimiento_confirmado": "si", "costo_puesto_confirmado": "si",
         "stock_confirmado": "si", "entrega_confirmada": "si", "economia_viable": "si", "que_falta": "Ninguno"}


def test_ready_requires_all_explicit_confirmations():
    rows, _ = board(row=READY)
    assert rows.iloc[0].situacion == "Lista para ofertar"
    for field in READY:
        if field != "situacion":
            assert board(row={**READY, field: "pendiente"})[0].iloc[0].situacion == "Para cotizar o confirmar"


def test_old_supplier_confirmations_are_downgraded_without_altering_evidence():
    rows, _ = board(row={**READY, "actualizado_en": "2026-09-14T10:00:00-05:00"})
    assert rows.iloc[0].situacion == "Para cotizar o confirmar"
    assert rows.iloc[0].actualizado_en == "2026-09-14T10:00:00-05:00"


def test_prompt_metadata_does_not_require_changing_the_existing_sheet_schema():
    block = "\n[EVALUACION_RIR_V2]\n" + "\n".join(f"{k}={v}" for k, v in READY.items()) + "\n[/EVALUACION_RIR_V2]"
    assert board(row={"observaciones": ROW["observaciones"] + block})[0].iloc[0].situacion == "Lista para ofertar"


def test_missing_date_on_withdrawn_research_stays_inactive():
    result = assess_research_validity(pd.DataFrame([{**ROW, "estado_investigacion": "No vigente", "observaciones": ""}]), now=NOW)
    assert result.iloc[0].vigencia == "No vigente"


def test_health_distinguishes_supplier_research_and_scraper_freshness():
    assert not research_health(pd.DataFrame([ROW]), pd.DataFrame([LIVE]), now=NOW)
    messages = research_health(pd.DataFrame([{**ROW, "actualizado_en": "2026-09-14"}]),
                               pd.DataFrame([{**LIVE, "verificado_en": "2026-09-14"}]), now=NOW)
    assert len(messages) == 2 and "ChatGPT" in messages[0] and "orquestador" in messages[1]


def test_empty_board_is_supported():
    eligible, removed = build_research_opportunities(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), now=NOW)
    assert eligible.empty and removed.empty


def test_same_cut_keeps_editorial_details_without_losing_pending_evidence():
    top = pd.DataFrame([{**ROW, "fecha_corte": "2026-09-17", "ranking": 2,
                         "correo_sugerido_proveedor": "Draft only", "producto_recomendado": "Model exact"}])
    rows, _ = board(top=top)
    assert rows.iloc[0].prioridad_publicada == 2
    assert rows.iloc[0].correo_sugerido_proveedor == "Draft only"
    assert rows.iloc[0].situacion == "Para cotizar o confirmar"


def test_structured_closing_hour_is_used_without_later_source_date():
    notes = "[FECHAS_RIR_V1]\nfecha_cierre=2026-09-17\nfecha_cierre_hora=13:00\nfuente_fecha_extraida=2026-09-18\n[/FECHAS_RIR_V1]"
    row = assess_research_validity(pd.DataFrame([{**ROW, "observaciones": notes}]), now=NOW).iloc[0]
    assert row.cierre_verificado == "2026-09-17T13:00:00-05:00"
    assert row.vigencia == "Vencida"


def test_one_incomplete_source_does_not_hide_other_confirmed_sources():
    from services.rir_supplier_research import read_current_research_acts
    headers = ['enlace', 'fecha', 'Fecha de Actualización', 'Fichas sin requisitos',
               'Tipo de adjudicación', 'Tipo de acto sin requisitos', 'Descartar',
               'Fichas con requisitos', 'Fichas por verificar']
    data = [ROW['enlace_acto'], LIVE['fecha_cierre'], LIVE['verificado_en'], '41364', 'Global', 'Solo fichas sin requisitos', '', '', '']
    class Reader:
        def values_batch_get(self, ranges):
            if len(ranges) == 3:
                return {'valueRanges': [{'values': [headers]}, {'values': [['enlace']]}, {'values': [headers]}]}
            return {'valueRanges': [{'values': [[value]]} for value in data * 2]}
    live = read_current_research_acts(Reader())
    assert len(live) == 2 and len(live.attrs['source_errors']) == 1
    assert len(build_research_opportunities(pd.DataFrame([ROW]), None, live, now=NOW)[0]) == 1
