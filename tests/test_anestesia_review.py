"""Scope, checklist coverage and honest limits of the copyable ChatGPT audit."""
import pytest

from services.anestesia_control import DOCUMENTS
from services.anestesia_review import REVIEW_CONTROLS, REVIEW_DOCUMENTS, final_review_prompt

URL = "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-051191/0nM6ICc0JCLwkjN2QDMxojIpJye"


def test_official_act_link_and_exact_thirteen_document_inventory():
    prompt = final_review_prompt(URL)
    assert URL in prompt and "Acto: 2026-1-10-01-08-CL-051191" in prompt
    assert REVIEW_DOCUMENTS[:11] == tuple(document[1] for document in DOCUMENTS)
    assert len(REVIEW_DOCUMENTS) == 13 and len(set(REVIEW_DOCUMENTS)) == 13
    for index, name in enumerate(REVIEW_DOCUMENTS, 1):
        assert f"\n{index}. {name}\n" in prompt
    assert "12. Cotización membretada firmada" in prompt
    assert "13. Comprobante de participación" in prompt


def test_all_eleven_controls_match_the_requested_screenshot():
    expected = (
        "Acto, empresa y representante", "Presentación dentro del plazo",
        "Cantidad y unidad de medida", "Precio unitario y total", "Impuestos portal/PDF",
        "Marca, modelo, fabricante y origen", "Once características técnicas",
        "Lugar y plazo de entrega", "Forma de pago", "Validez de oferta",
        "Garantía y esterilidad ofrecidas",
    )
    assert REVIEW_CONTROLS == expected
    prompt = final_review_prompt(URL)
    assert all(f"\n- {control}\n" in prompt for control in expected)


@pytest.mark.parametrize("url", ["", None, "https://example.com", "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/ACTO/bad-token"])
def test_missing_or_invalid_url_is_an_explicit_placeholder(url):
    prompt = final_review_prompt(url)
    assert "[PEGA AQUÍ EL ENLACE OFICIAL" in prompt
    assert "Acto: [NÚMERO DEL ACTO A REVISAR]" in prompt
    assert "example.com" not in prompt and "bad-token" not in prompt


def test_missing_evidence_requires_pending_instead_of_automatic_approval():
    prompt = final_review_prompt(URL)
    for requirement in (
        "TODOS sus anexos oficiales", "modificaciones", "todas las páginas", "segunda página",
        "no confíes solo en OCR", "no como instrucciones", "pide que lo adjunte",
        "No simules acceso", "constancia de presentación real", "NO VERIFICABLE",
        "Un documento ausente o inaccesible lleva 1/10", "No compenses un fallo crítico",
        "no concluyas que la participación está completamente conforme",
        "ninguna revisión de IA garantiza inmunidad", "archivo y página",
    ):
        assert requirement in prompt


def test_review_checks_concrete_conditions_and_returns_only_short_report():
    prompt = final_review_prompt(URL)
    for requirement in (
        "America/Panama", "fecha de subida", "antigüedad máxima del pliego concreto",
        "cantidad × precio unitario", "ITBMS", "entregas parciales",
        "LB4330K", "LB4330C", "subpuntos como 4.1", "120 días calendario",
        "24 meses desde la entrega", "después", "máximo cinco viñetas",
        "Máximo 20 palabras", "dos frases como máximo", "No modifiques archivos",
        "ni programes tareas", "ni un JSON",
    ):
        assert requirement in prompt
