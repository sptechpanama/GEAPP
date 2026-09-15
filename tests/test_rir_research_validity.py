import pandas as pd
import pytest
from services.rir_supplier_research import assess_research_validity, latest_top10_snapshot, read_current_research_acts

NOW = "2026-09-15T18:00:00-05:00"
ROW = {"numero_acto": "2026-1-10-01-08-CL-050000", "ficha": "108148", "oportunidad": "Renglón 2: urinales",
       "fecha_cierre": "2026-09-18", "actualizado_en": "2026-09-15T10:00:00-05:00"}
LIVE = {"numero_acto": ROW["numero_acto"], "fichas_sin_requisitos": "108148", "tipo_acto": "Solo fichas sin requisitos",
        "tipo_adjudicacion": "Global", "fecha_cierre": "16-09-2026 a 18-09-2026", "verificado_en": "2026-09-15 17:15:00"}


def review(row=None, live=None, research=None):
    return assess_research_validity(pd.DataFrame([{**ROW, **(row or {})}]),
                                   pd.DataFrame([{**LIVE, **(live or {})}]),
                                   research=research, now=NOW).iloc[0]


def test_future_live_opportunity_is_valid():
    assert review()["vigencia"] == "Vigente"


def test_screenshot_expired_rows_cannot_remain_in_top():
    row = review({"fecha_cierre": "2026-09-14"}, {"fecha_cierre": "2026-09-14"})
    assert row["vigencia"] == "Vencida"


@pytest.mark.parametrize("date,expected", [("2026-09-15", "Por verificar"),
    ("2026-09-15T18:00:00-05:00", "Vencida"), ("2026-09-15T23:01:00Z", "Vigente"),
    ("2026-09-15T17:59:00-05:00", "Vencida")])
def test_hour_timezone_and_exact_boundary(date, expected):
    assert review({"fecha_cierre": date}, {"fecha_cierre": date})["vigencia"] == expected


def test_extension_requires_newer_capture_and_preserves_original():
    assert review({"fecha_cierre": "2026-09-14"})["vigencia"] == "Vigente"
    result = review({"fecha_cierre": "2026-09-14"}, {"verificado_en": "2026-09-15 09:00:00"})
    assert result["vigencia"] == "Vencida"
    assert result["fecha_cierre"] == "2026-09-14"


@pytest.mark.parametrize("changes", [{"fichas_sin_requisitos": "100177"},
    {"tipo_acto": "Acto mixto", "tipo_adjudicacion": "Global"}, {"verificado_en": "2026-09-12 12:00:00"},
    {"descartar": "TRUE"}, {"verificado_en": ""}])
def test_unsafe_or_unverified_source_is_not_actionable(changes):
    assert review(live=changes)["vigencia"] == "Por verificar"


def test_mixed_by_line_remains_allowed():
    assert review(live={"tipo_acto": "Acto mixto", "tipo_adjudicacion": "Renglón"})["vigencia"] == "Vigente"


@pytest.mark.parametrize("changes", [{"fichas_con_requisitos": "101000 (CT)"},
    {"fichas_por_verificar": "108148", "tipo_adjudicacion": "Renglón"},
    {"fichas_con_requisitos": "108148 (RS)", "tipo_adjudicacion": "Renglón"}])
def test_contradictory_or_incomplete_eligibility_is_not_silently_accepted(changes):
    assert review(live=changes)["vigencia"] == "Por verificar"


def test_missing_source_does_not_claim_live_verification():
    assert assess_research_validity(pd.DataFrame([ROW]), now=NOW).iloc[0]["vigencia"] == "Por verificar"


def test_newer_research_withdrawal_matches_the_exact_line():
    research = pd.DataFrame([{**ROW, "renglon": "2", "estado_investigacion": "No vigente"}])
    assert review(research=research)["vigencia"] == "No vigente"
    research.loc[0, "renglon"] = "1"
    assert review(research=research)["vigencia"] == "Vigente"


def test_changed_research_requires_reassessment_of_the_top():
    research = pd.DataFrame([{**ROW, "renglon": "2", "actualizado_en": "2026-09-15T11:00:00-05:00"}])
    assert review(research=research)["vigencia"] == "Por verificar"


def test_notes_date_ignores_later_source_date_and_does_not_overwrite():
    frame = pd.DataFrame([{**ROW, "fecha_cierre": "", "observaciones": "cierre: 2026-09-18 (hora no registrada). Fuente 2026-09-15"}])
    original = frame.copy(deep=True)
    result = assess_research_validity(frame, now=NOW)
    assert result.iloc[0]["cierre_verificado"] == "2026-09-18"
    pd.testing.assert_frame_equal(frame, original)


def test_zero_candidate_cut_does_not_fall_back_to_yesterday():
    frame = pd.DataFrame([{**ROW, "fecha_corte": "2026-09-14", "ranking": 1},
                          {"fecha_corte": "2026-09-15", "ranking": 0, "estado": "Sin oportunidades vigentes"}])
    assert latest_top10_snapshot(frame).empty


def test_empty_assessment_has_stable_schema():
    assert "vigencia" in assess_research_validity(pd.DataFrame(), now=NOW)


def test_partial_sheet_read_raises_instead_of_false_empty():
    class Broken:
        def values_batch_get(self, _):
            return {"valueRanges": []}
    with pytest.raises(ValueError, match="incompleta"):
        read_current_research_acts(Broken())
