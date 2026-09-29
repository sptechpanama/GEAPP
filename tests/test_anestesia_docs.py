from copy import deepcopy
from datetime import datetime, timedelta
from io import BytesIO
import json
from pathlib import Path
import re

from docx import Document
import fitz
import pytest

from services.anestesia_docs import (AUDIT_CONTROLS, BASE_KINDS, COMPANY, PANAMA, canonical_hash,
    document_status, file_hash, now_iso, public_registry_age, review_errors, totals, validate_package)
from services.anestesia_documents import pact_docx, quote_docx
from services.anestesia_source import route, source_is_closed
from services import anestesia_worker as worker

ROOT = Path(__file__).resolve().parents[1]
TODAY = datetime.now(PANAMA).date()
IDENT = "a" * 32
ACT = "2026-1-10-01-08-CL-051191"
URL = "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/" + ACT + "/0nM6ICc0JCLwkjN2QDMxojIpJye"


def fixture():
    source = {"url": URL, "number": ACT, "title": "Kit de circuito de paciente para máquina de anestesia",
        "entity": "Caja de Seguro Social", "purchase_unit": "Ciudad de la Salud", "info": {"forma de pago": "Crédito"},
        "publication": str(TODAY - timedelta(days=10)), "closing": str(TODAY + timedelta(days=2)),
        "items": [{"cantidad": 900, "unidad": "Unidad"}], "explicit_fichas": ["43358"],
        "blocking_errors": [], "registry_max_months": 12, "registry_rule": "Anexo p.2, antigüedad máxima de un año",
        "fingerprint": "oficial-123"}
    config = {"catalog": "K", "price": "20", "tax_mode": "exento", "tax_rate": 7,
        "tax_evidence": "Anexo de presupuesto, p.1, ITBMS 0", "catalog_brand": "MFLab", "catalog_model": "LB4330K",
        "source_confirmed": True, "signature_authorized": True, "delivery_place": "Almacén de Ciudad de la Salud",
        "delivery": "300 unidades a 30 días, 300 a 45 días y 300 a 60 días calendario desde orden de compra",
        "warranty": "Esterilidad mínima de 24 meses desde cada entrega", "entity_representative": "Persona de Prueba",
        "entity_id": "8-000-000", "entity_role": "Director", "document_date": str(TODAY),
        "control_date": str(TODAY + timedelta(days=2)), "proposal_validity_days": 30}
    docs = [{"id": kind, "kind": kind, "company": COMPANY, "issued": str(TODAY - timedelta(days=5)),
        "expires": str(TODAY + timedelta(days=100)), "verified": True, "evidence": "Original completo, página 1",
        "catalogs": "K", "models": "LB4330K", "fichas": "43358", "act": ACT if kind in {"retorsion", "calidad"} else "",
        "notarized": True, "apostilled": True, "translation_verified": True,
        "file_id": kind, "sha256": "hash", "created_at": now_iso()} for kind in BASE_KINDS]
    return source, config, docs


@pytest.mark.parametrize("catalog,model", [("K", "LB4330K"), ("C", "LB4330C")])
def test_full_certificate_covers_both_variants_but_quote_selects_only_one(catalog, model):
    source, config, docs = fixture()
    config.update(catalog=catalog, catalog_model=model)
    for doc in docs:
        doc.update(catalogs="K,C", models="LB4330K, LB4330C", evidence="CT páginas 1 y 2; catálogo página 4")
    checks, chosen = validate_package(source, config, docs)
    assert all(c["estado"] == "Vigente documentalmente" for c in checks), checks
    assert chosen["criterio_tecnico"]["models"] == "LB4330K, LB4330C"


@pytest.mark.parametrize("catalog,model", [("C", "LB4330K"), ("K", "LB4330C")])
def test_full_certificate_does_not_allow_wrong_mask_selection(catalog, model):
    source, config, docs = fixture()
    config.update(catalog=catalog, catalog_model=model)
    for doc in docs: doc.update(catalogs="K,C", models="LB4330K,LB4330C")
    checks, _ = validate_package(source, config, docs)
    assert any("corresponde al catálogo" in c["motivo"] for c in checks)


@pytest.mark.parametrize("mode,subtotal,tax,total", [("exento", "18000.00", "0.00", "18000.00"),
    ("adicional", "18000.00", "1260.00", "19260.00"), ("incluido", "16822.43", "1177.57", "18000.00")])
def test_tax_never_double_counts_unit_price(mode, subtotal, tax, total):
    result = totals(900, "20", mode)
    assert (result["subtotal"], result["itbms"], result["total"]) == (subtotal, tax, total)


@pytest.mark.parametrize("price", [0, -5, "NaN", "inf", None, "1,000"])
def test_invalid_price_blocks_generation(price):
    with pytest.raises(ValueError): totals(900, price, "exento")


@pytest.mark.parametrize("text,months", [("Registro Público. Una vigencia no mayor de seis (6) meses", 6),
    ("Registro Público; vigencia no mayor de un (1) año", 12),
    ("Registro Público; vigencia no mayor de un (l) año", 12),
    ("Registro Público; antigüedad no superior a 3 meses", 3),
    ("Registro Público. El kit tiene vida útil de 24 meses", None)])
def test_registry_age_comes_from_this_tender(text, months):
    assert public_registry_age(text)[0] == months


def test_complete_evidence_passes_and_all_missing_documents_are_listed():
    source, config, docs = fixture()
    checks, _ = validate_package(source, config, docs)
    assert {r["estado"] for r in checks} == {"Vigente documentalmente"}
    checks, _ = validate_package(source, config, [])
    assert len(checks) == 12 and all(c["estado"] == "Falta" for c in checks)


@pytest.mark.parametrize("kind,changes,reason", [
    ("css", {"expires": str(TODAY - timedelta(days=1)), "created_at": now_iso()}, "Venció"),
    ("dgi", {"expires": ""}, "vencimiento"),
    ("criterio_tecnico", {"catalogs": "C"}, "biblioteca"),
    ("criterio_tecnico", {"fichas": "102625"}, "43358"),
    ("oferente", {"company": "Otra empresa"}, "RIR"),
    ("retorsion", {"issued": str(TODAY - timedelta(days=30))}, "publicación"),
    ("calidad", {"notarized": False}, "notarial"),
    ("disposicion", {"apostilled": False}, "apostilla"),
    ("disposicion", {"translation_verified": False}, "idioma"),
    ("cedula", {"verified": "false"}, "verificar"),
    ("catalogo", {"file_id": ""}, "Drive"),
    ("registro_publico", {"issued": str(TODAY - timedelta(days=400))}, "meses"),
])
def test_certificates_fail_closed_with_specific_action(kind, changes, reason):
    source, config, docs = fixture()
    next(d for d in docs if d["kind"] == kind).update(changes)
    checks, _ = validate_package(source, config, docs)
    check = next(c for c in checks if c["kind"] == kind)
    assert check["estado"] != "Vigente documentalmente" and reason in check["motivo"]


def test_cannot_validate_only_today_when_certificate_expires_before_tender_close():
    source, config, docs = fixture()
    config["control_date"] = str(TODAY)
    docs[0]["expires"] = str(TODAY + timedelta(days=1))
    checks, _ = validate_package(source, config, docs)
    assert any("presentación" in c["motivo"] for c in checks)


def test_unknown_registry_rule_and_unreadable_annex_block_without_guessing():
    source, config, docs = fixture()
    source.update(registry_max_months=None, blocking_errors=["Anexo no interpretado"])
    checks, _ = validate_package(source, config, docs)
    assert any("Anexo no interpretado" in c["motivo"] for c in checks)
    assert any("antigüedad máxima" in c["motivo"] for c in checks)


def test_requirements_not_limited_to_fixed_twelve_and_no_automatic_ct_for_c():
    source, config, docs = fixture()
    config.update(require_rs=True, require_power=True, extra_requirements=["Certificado adicional"])
    checks, _ = validate_package(source, config, docs)
    assert len(checks) == 15 and sum(c["estado"] == "Falta" for c in checks) == 3
    config["catalog"] = "C"
    assert any(c["kind"] == "criterio_tecnico" and c["estado"] == "Falta" for c in validate_package(source, config, docs)[0])


def test_latest_invalid_version_cannot_silently_fall_back_to_an_older_valid_one():
    source, config, docs = fixture()
    docs.append({**docs[0], "id": "updated", "verified": False, "created_at": "2099-01-01"})
    checks, selected = validate_package(source, config, docs)
    assert selected["dgi"]["id"] == "updated"
    assert next(c for c in checks if c["kind"] == "dgi")["estado"] == "Bloqueado"


def test_url_and_closing_reject_wrong_host_corrupt_tokens_and_past_or_cancelled_acts():
    assert route(URL) == (1046690, 2, ACT)
    for url in (URL.replace("panamacompra.gob.pa", "evil.example"), URL + "bad", "http://localhost/test"):
        with pytest.raises(ValueError): route(url)
    source, _, _ = fixture()
    assert not source_is_closed(source)[0]
    assert source_is_closed({**source, "closing": ""})[0]
    assert source_is_closed({**source, "closing": "2020-01-01"})[0]
    assert source_is_closed({**source, "info": {"estado": "Suspendido"}})[0]


def text_docx(data):
    doc = Document(BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs) + "\n" + "\n".join(c.text for t in doc.tables for r in t.rows for c in r.cells)


def test_authored_documents_have_correct_amounts_delivery_identity_and_position():
    source, config, _ = fixture()
    quote = quote_docx(source, config, ROOT / "assets/cotizacion_base")
    text = text_docx(quote)
    assert "18,000.00" in text and "22,500.00" not in text
    for fragment in ("900", "LB4330K", "300 a 45", "Ciudad de la Salud", "24 meses", "S.EP."):
        assert fragment in text
    doc = Document(BytesIO(quote))
    assert len(doc.inline_shapes) >= 1
    assert "PH Bonanza" in "\n".join(p.text for s in doc.sections for p in s.header.paragraphs) or "PH Bonanza" in str(doc.sections[0].header._element.xml)
    pact = text_docx(pact_docx(source, config, ROOT))
    assert "en su calidad de Director" in pact and "Director" in pact.splitlines()
    assert "persona natural" not in pact and "sociedad de emprendimiento" in pact
    assert "155750585-2-2024-2024-574365876" in pact and "Bella Vista" in pact
    assert not re.search(r"\[[A-Za-z_]+\]", pact)


def test_quote_preserves_unit_price_precision_used_to_calculate_total():
    source, config, _ = fixture()
    config["price"] = "20.1234"
    quote = text_docx(quote_docx(source, config, ROOT / "assets/cotizacion_base"))
    assert "20.1234" in quote and "18,111.06" in quote


def approval(manifest):
    return {"request_id": manifest["request_id"], "manifest_hash": manifest["manifest_hash"],
        "decision": "aprobado", "pendientes": [], "revisor": "Revisor de prueba", "modelo": "simulacion",
        "reviewed_at": now_iso(), "archivos_revisados_sha256": [f["sha256"] for f in manifest["files"] if f.get("deliverable")],
        "controles": {key: {"resultado": "cumple", "evidencia": "Simulación automatizada; no constituye revisión de una oferta real"} for key in AUDIT_CONTROLS}}


@pytest.mark.parametrize("change", [{"manifest_hash": "wrong"}, {"decision": "observado"}, {"pendientes": ["Falta CT"]},
    {"controles": {}}, {"archivos_revisados_sha256": []}, {"archivos_revisados_sha256": [{}]}, {"reviewed_at": "2099-01-01"}])
def test_incomplete_or_other_version_review_cannot_approve(change):
    manifest = {"request_id": IDENT, "manifest_hash": "h", "created_at": now_iso(), "files": [{"deliverable": True, "sha256": "f"}]}
    assert not review_errors(approval(manifest), manifest)
    assert review_errors({**approval(manifest), **change}, manifest)


class FakeStorage:
    def __init__(self, source, docs):
        self.objects = {"source": json.dumps(source).encode()}
        self.saved = {"id": IDENT, "number": ACT, "state": "Datos capturados", "source_id": "source"}
        self.tables = {"ANESTESIA_DOCUMENTOS": deepcopy(docs), "ANESTESIA_REVISIONES": []}
        for item in self.tables["ANESTESIA_DOCUMENTOS"]:
            data = ("original " + item["kind"]).encode()
            item["sha256"] = file_hash(data); self.objects[item["file_id"]] = data
    def job(self, ident): return dict(self.saved)
    def save_job(self, changes): self.saved.update(changes); return dict(self.saved)
    def root(self): return "root"
    def folder(self, name, parent): return name
    def rows(self, name): return deepcopy(self.tables[name])
    def json_file(self, ident): return json.loads(self.objects[ident])
    def get_bytes(self, ident): return self.objects[ident]
    def put(self, parent, name, data, mime):
        ident = str(len(self.objects)); self.objects[ident] = data
        return {"file_id": ident, "name": name, "url": "https://drive.example/" + ident, "sha256": file_hash(data), "mime": mime}
    def convert_document(self, data, name, parent):
        pdf = fitz.open(); page = pdf.new_page(); page.insert_text((40, 40), ACT)
        return pdf.tobytes()


def generated(monkeypatch):
    source, config, docs = fixture()
    storage = FakeStorage(source, docs)
    monkeypatch.setattr(worker, "capture", lambda *args, **kw: source)
    result = worker.run_request(storage, {"action": "generate", "request_id": IDENT, "config": config}, execution_id="first", root=ROOT)
    assert result["state"] == "Pendiente de revisión"
    manifest = storage.json_file(result["manifest_id"])
    review = {**approval(manifest), "id": "review"}
    storage.tables["ANESTESIA_REVISIONES"].append(review)
    return source, storage, manifest


def test_worker_generates_reviewable_bundle_and_only_publishes_exact_approved_files(monkeypatch):
    _, storage, manifest = generated(monkeypatch)
    assert len(manifest["files"]) == 16  # two authored Word/PDF pairs plus 12 originals
    result = worker.run_request(storage, {"action": "finalize", "request_id": IDENT, "review_id": "review", "user_confirmed": True}, execution_id="second", root=ROOT)
    assert result["state"] == "Listo para entregar" and result["zip_url"]
    size = len(storage.objects)
    again = worker.run_request(storage, {"action": "finalize", "request_id": IDENT}, execution_id="retry", root=ROOT)
    assert again["published_manifest"] == manifest["manifest_hash"] and len(storage.objects) == size


@pytest.mark.parametrize("tamper", ["pdf", "manifest", "source", "library", "expired", "closed", "approval"])
def test_publication_blocks_every_material_change_after_review(monkeypatch, tamper):
    source, storage, manifest = generated(monkeypatch)
    if tamper == "pdf": storage.objects[manifest["files"][0]["file_id"]] += b"modified"
    elif tamper == "manifest": storage.objects[storage.saved["manifest_id"]] = json.dumps({**manifest, "amounts": {"total": "1"}}).encode()
    elif tamper == "source": monkeypatch.setattr(worker, "capture", lambda *a, **k: {**source, "fingerprint": "changed"})
    elif tamper == "library": storage.tables["ANESTESIA_DOCUMENTOS"][0]["id"] = "changed"
    elif tamper == "expired": storage.tables["ANESTESIA_DOCUMENTOS"][0]["expires"] = "2020-01-01"
    elif tamper == "closed": monkeypatch.setattr(worker, "capture", lambda *a, **k: {**source, "closing": "2020-01-01"})
    elif tamper == "approval": storage.tables["ANESTESIA_REVISIONES"][0]["decision"] = "observado"
    try:
        result = worker.run_request(storage, {"action": "finalize", "request_id": IDENT, "review_id": "review", "user_confirmed": True}, execution_id="changed", root=ROOT)
        assert result["state"] == "Bloqueado"
    except ValueError:
        pass
    assert storage.saved["state"] != "Listo para entregar" and not storage.saved.get("zip_url")


def test_capture_error_preserves_previous_source_and_library(monkeypatch):
    source, _, docs = fixture()
    storage = FakeStorage(source, docs)
    before = deepcopy(storage.objects)
    def fail(*a, **k): raise TimeoutError("Fuente temporalmente caída")
    monkeypatch.setattr(worker, "capture", fail)
    with pytest.raises(TimeoutError):
        worker.run_request(storage, {"action": "capture", "request_id": IDENT, "url": URL}, execution_id="failed", root=ROOT)
    assert storage.objects == before and storage.saved["source_id"] == "source"
