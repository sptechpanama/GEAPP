from __future__ import annotations

import re
import zipfile
from html import unescape
from io import BytesIO
from pathlib import Path

import pytest
from docx import Document

from services.lp_documents import SP_COMPANY_NAME, get_lp_company_profile, render_lp_document


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "assets" / "doc_gen_base"
ASSETS = ROOT / "assets" / "cotizacion_base"
SP_NOTICE = TEMPLATES / "aviso_operacion_sp.pdf"

RS_DOCS = [
    ("template_medidas_de_retorsion.docx", "medidas_de_retorsion.docx"),
    ("template_medidas_de_retorsion_sf.docx", "medidas_de_retorsion_sf.docx"),
    ("template_no_incapacidad_para_contratar.docx", "no_incapacidad_para_contratar.docx"),
    ("template_no_incapacidad_para_contratar_sf.docx", "no_incapacidad_para_contratar_sf.docx"),
    ("template_pacto_de_integridad.docx", "pacto_de_integridad.docx"),
    ("template_pacto_de_integridad_sf.docx", "pacto_de_integridad_sf.docx"),
    ("template_desglose_de_precios.docx", "desglose_de_precios.docx"),
    ("template_nota_adicional.docx", "nota_adicional.docx"),
    ("template_carta_de_adhesion.docx", "carta_de_adhesion.docx"),
    ("template_carta_de_adhesion_sf.docx", "carta_de_adhesion_sf.docx"),
]

RIR_DOCS = [
    ("template_medidas_de_retorsion_rir.docx", "medidas_de_retorsion.docx"),
    ("template_medidas_de_retorsion_sf_rir.docx", "medidas_de_retorsion_sf.docx"),
    ("template_no_incapacidad_para_contratar.docx", "no_incapacidad_para_contratar.docx"),
    ("template_no_incapacidad_para_contratar_sf.docx", "no_incapacidad_para_contratar_sf.docx"),
    ("template_pacto_de_integridad_rir.docx", "pacto_de_integridad.docx"),
    ("template_pacto_de_integridad_sf_rir.docx", "pacto_de_integridad_sf.docx"),
    ("template_desglose_de_precios_rir.docx", "desglose_de_precios.docx"),
    ("template_nota_adicional_rir.docx", "nota_adicional.docx"),
    ("template_carta_de_adhesion_rir.docx", "carta_de_adhesion.docx"),
    ("template_carta_de_adhesion_sf_rir.docx", "carta_de_adhesion_sf.docx"),
]

REPLACEMENTS = {
    "[Representante_legal_de_la_Entidad_Licitante]": "Ana María Pérez",
    "[entidad]": "Entidad Pública de Prueba",
    "[titulo]": "SUMINISTRO DE EQUIPOS PARA PRUEBA DOCUMENTAL",
    "[numero_de_acto]": "2026-0-00-00-00-LP-000001",
    "[cedula]": "8-888-888",
    "[lugar]": "Ciudad de Panamá",
    "[entrega]": "30 días calendario",
    "[fecha]": "27 de agosto de 2026",
    "[dia]": "27",
    "[mes]": "agosto",
    "[año]": "2026",
}


def _healthy_template(name: str) -> Path:
    candidate = TEMPLATES / name
    try:
        with zipfile.ZipFile(candidate) as archive:
            bad = archive.testzip()
        if bad is None:
            return candidate
    except Exception:
        pass
    if name == "template_no_incapacidad_para_contratar.docx":
        return TEMPLATES / "template_no_incapacidad_para_contratar_sf.docx"
    raise AssertionError(f"Plantilla inválida sin fallback: {name}")


def _all_word_xml(data: bytes) -> str:
    with zipfile.ZipFile(BytesIO(data)) as archive:
        return "\n".join(
            archive.read(name).decode("utf-8", errors="ignore")
            for name in archive.namelist()
            if name.startswith("word/") and name.endswith(".xml")
        )


def _all_word_text(data: bytes) -> str:
    xml = _all_word_xml(data)
    return "\n".join(
        unescape(value)
        for value in re.findall(r"<w:t(?:\s[^>]*)?>(.*?)</w:t>", xml, flags=re.DOTALL)
    )


def _document_text(data: bytes) -> str:
    with zipfile.ZipFile(BytesIO(data)) as archive:
        xml = archive.read("word/document.xml").decode("utf-8", errors="ignore")
    return "\n".join(
        unescape(value)
        for value in re.findall(r"<w:t(?:\s[^>]*)?>(.*?)</w:t>", xml, flags=re.DOTALL)
    )


def _normalized_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _body_paragraph_text(data: bytes) -> str:
    document = Document(BytesIO(data))
    return "\n".join(paragraph.text for paragraph in document.paragraphs)


@pytest.mark.parametrize(
    "company,template_name,output_name,expected_company",
    [
        (
            "RS Engineering",
            "template_medidas_de_retorsion.docx",
            "medidas_de_retorsion.docx",
            "RS ENGINEERING",
        ),
        (
            "RIR Medical",
            "template_no_incapacidad_para_contratar.docx",
            "no_incapacidad_para_contratar.docx",
            "RIR MEDICAL ENGINEERING",
        ),
        (
            SP_COMPANY_NAME,
            "template_pacto_de_integridad.docx",
            "pacto_de_integridad.docx",
            "SP TECH SOLUTIONS, S.A.",
        ),
    ],
)
def test_company_profiles_generate_healthy_legal_documents(
    company: str,
    template_name: str,
    output_name: str,
    expected_company: str,
) -> None:
    data = render_lp_document(
        _healthy_template(template_name),
        REPLACEMENTS,
        company_name=company,
        document_name=output_name,
        assets_dir=ASSETS,
    )
    with zipfile.ZipFile(BytesIO(data)) as archive:
        assert archive.testzip() is None
        document_xml = archive.read("word/document.xml").decode("utf-8")
    all_text = _all_word_text(data)
    assert expected_company in all_text
    assert not re.search(r"\[[^\]]+\]", all_text)
    assert re.search(r"<w:pgSz[^>]*w:w=\"12240\"", document_xml)
    assert re.search(r"<w:pgSz[^>]*w:h=\"20160\"", document_xml)
    assert len(data) < 5_000_000


def test_sp_profile_uses_official_identity_and_never_leaks_rs_identity() -> None:
    data = render_lp_document(
        _healthy_template("template_medidas_de_retorsion.docx"),
        REPLACEMENTS,
        company_name=SP_COMPANY_NAME,
        document_name="medidas_de_retorsion.docx",
        assets_dir=ASSETS,
    )
    text = _all_word_text(data)
    assert "Irvin Jesus Sanchez Prado" in text
    assert "9-749-1629" in text
    assert "155767402-2-2025" in text
    assert "RS ENGINEERING" not in text
    assert "Rodrigo Jesús Sánchez Prado" not in text
    assert "9-740-624" not in text


def test_sp_notice_and_assets_are_versioned() -> None:
    profile = get_lp_company_profile(SP_COMPANY_NAME)
    assert SP_NOTICE.exists() and SP_NOTICE.stat().st_size > 100_000
    assert (ASSETS / profile.header_filename).exists()
    assert (ASSETS / profile.signature_filename).exists()


def test_rir_header_uses_rir_identity_instead_of_sp_identity() -> None:
    data = render_lp_document(
        _healthy_template("template_pacto_de_integridad_rir.docx"),
        REPLACEMENTS,
        company_name="RIR Medical",
        document_name="pacto_de_integridad.docx",
        assets_dir=ASSETS,
    )
    text = _all_word_text(data)
    assert "155750585-2-2024" in text
    assert "155767402-2-2025" not in text


@pytest.mark.parametrize(
    "company,template_name",
    [
        ("RS Engineering", "template_pacto_de_integridad.docx"),
        ("RIR Medical", "template_pacto_de_integridad_rir.docx"),
        (SP_COMPANY_NAME, "template_pacto_de_integridad.docx"),
    ],
)
def test_pact_legal_clauses_are_not_rewritten(
    company: str,
    template_name: str,
) -> None:
    template = _healthy_template(template_name)
    original = _body_paragraph_text(template.read_bytes())
    for token, value in REPLACEMENTS.items():
        original = original.replace(token, value)

    generated = _body_paragraph_text(
        render_lp_document(
            template,
            REPLACEMENTS,
            company_name=company,
            document_name="pacto_de_integridad.docx",
            assets_dir=ASSETS,
        )
    )
    original_tail = original[original.index("PRIMERA") :]
    generated_tail = generated[generated.index("PRIMERA") :]
    assert _normalized_text(generated_tail) == _normalized_text(original_tail)


@pytest.mark.parametrize(
    "company,template_name,output_name,expected_company",
    [
        *(('RS Engineering', template, output, 'RS ENGINEERING') for template, output in RS_DOCS),
        *(('RIR Medical', template, output, 'RIR MEDICAL ENGINEERING') for template, output in RIR_DOCS),
    ],
)
def test_all_existing_company_documents_remain_complete(
    company: str,
    template_name: str,
    output_name: str,
    expected_company: str,
) -> None:
    data = render_lp_document(
        _healthy_template(template_name),
        REPLACEMENTS,
        company_name=company,
        document_name=output_name,
        assets_dir=ASSETS,
    )
    with zipfile.ZipFile(BytesIO(data)) as archive:
        assert archive.testzip() is None
    text = _all_word_text(data)
    assert get_lp_company_profile(company).representative in text
    if "carta_de_adhesion" not in output_name:
        assert expected_company in text
    assert not re.search(r"\[[^\]]+\]", text)
    assert len(data) < 5_000_000


@pytest.mark.parametrize(
    "template_name,output_name",
    [
        ("template_medidas_de_retorsion.docx", "medidas_de_retorsion.docx"),
        ("template_medidas_de_retorsion_sf.docx", "medidas_de_retorsion_sf.docx"),
        ("template_no_incapacidad_para_contratar.docx", "no_incapacidad_para_contratar.docx"),
        ("template_no_incapacidad_para_contratar_sf.docx", "no_incapacidad_para_contratar_sf.docx"),
        ("template_pacto_de_integridad.docx", "pacto_de_integridad.docx"),
        ("template_pacto_de_integridad_sf.docx", "pacto_de_integridad_sf.docx"),
        ("template_desglose_de_precios.docx", "desglose_de_precios.docx"),
        ("template_nota_adicional.docx", "nota_adicional.docx"),
        ("template_carta_de_adhesion.docx", "carta_de_adhesion.docx"),
        ("template_carta_de_adhesion_sf.docx", "carta_de_adhesion_sf.docx"),
    ],
)
def test_all_sp_lp_documents_are_dedicated_and_complete(
    template_name: str, output_name: str
) -> None:
    data = render_lp_document(
        _healthy_template(template_name),
        REPLACEMENTS,
        company_name=SP_COMPANY_NAME,
        document_name=output_name,
        assets_dir=ASSETS,
    )
    text = _all_word_text(data)
    assert "SP TECH SOLUTIONS, S.A." in text
    assert "RS ENGINEERING" not in text
    assert "Rodrigo Jesús Sánchez Prado" not in text
    assert not re.search(r"\[[^\]]+\]", text)
