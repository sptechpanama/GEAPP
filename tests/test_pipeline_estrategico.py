from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from services.pipeline_estrategico import (
    PipelineError,
    PipelineFilters,
    PipelineRepository,
    PipelineRuleError,
    ROUTES,
)
from services.pipeline_drive import PipelineDriveStorage
from services.pipeline_sheets import PipelineSheetsMirror, WorksheetNotFound
from services.pipeline_trello_import import (
    TrelloImportError,
    import_trello_board,
    load_trello_export,
    preview_trello_export,
)


@pytest.fixture()
def repo(tmp_path: Path) -> PipelineRepository:
    repository = PipelineRepository(
        create_engine(f"sqlite:///{(tmp_path / 'pipeline.db').as_posix()}")
    )
    yield repository
    repository.close()


def _create_card(repo: PipelineRepository, **overrides):
    values = {
        "ficha": "43358",
        "nombre_ficha": "Circuito de paciente",
        "producto": "Circuito de paciente pediatrico",
        "proveedor": "Foyomed",
        "marca": "MFLab",
        "descripcion": "Oportunidad de anestesia",
        "route_key": "fichas_viejas",
        "actor": "rsanchez",
    }
    values.update(overrides)
    return repo.create_card(**values)


def test_dynamic_templates_have_expected_lengths_and_initial_steps() -> None:
    assert len(ROUTES["fichas_viejas"].checklist) == 10
    assert len(ROUTES["fichas_recien_creadas"].checklist) == 10
    assert len(ROUTES["homologaciones_anunciadas"].checklist) == 13
    assert len(ROUTES["solicitudes_creacion"].checklist) == 13
    assert len(ROUTES["creacion_desde_cero"].checklist) == 13
    assert ROUTES["homologaciones_anunciadas"].checklist[0][0] == "citas_homologacion_agregadas"
    assert ROUTES["solicitudes_creacion"].checklist[0][0] == "solicitudes_creacion_agregadas"
    assert ROUTES["creacion_desde_cero"].checklist[0][0] == "fichas_a_crear_agregadas"
    for route in ROUTES.values():
        assert route.checklist[-1][0] == "entrega_recibido_conforme"


def test_explicit_local_path_overrides_database_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://invalid.invalid/example")
    repository = PipelineRepository.connect(local_path=tmp_path / "forced-local.db")
    try:
        assert repository.dialect == "sqlite"
        assert repository.source_label.startswith("SQLite local")
    finally:
        repository.close()


def test_card_identity_is_ficha_provider_brand_and_is_unique(repo: PipelineRepository) -> None:
    first = _create_card(repo)
    assert first["identity_key"] == "43358|foyomed|mflab"
    with pytest.raises(PipelineError, match="Ya existe"):
        _create_card(repo, producto="Otro texto que no cambia la identidad")


def test_from_scratch_uses_product_as_provisional_identity(repo: PipelineRepository) -> None:
    card = _create_card(
        repo,
        ficha="",
        producto="Nuevo sensor neonatal",
        route_key="creacion_desde_cero",
    )
    assert card["identity_key"].startswith("POR-CREAR:nuevo sensor neonatal|")
    assert len(repo.checkpoints(card["id"])) == 13


def test_checkpoints_are_strictly_sequential(repo: PipelineRepository) -> None:
    card = _create_card(repo)
    checkpoints = repo.checkpoints(card["id"])
    with pytest.raises(PipelineRuleError, match="Completa primero"):
        repo.set_checkpoint(
            card_id=card["id"],
            checkpoint_key=checkpoints[1]["checkpoint_key"],
            completed=True,
            actor="rsanchez",
        )
    repo.set_checkpoint(
        card_id=card["id"],
        checkpoint_key=checkpoints[0]["checkpoint_key"],
        completed=True,
        actor="rsanchez",
    )
    result = repo.set_checkpoint(
        card_id=card["id"],
        checkpoint_key=checkpoints[1]["checkpoint_key"],
        completed=True,
        actor="rsanchez",
    )
    assert [bool(row["completed"]) for row in result[:3]] == [True, True, False]


def test_reopening_checkpoint_requires_confirmation_and_resets_downstream(
    repo: PipelineRepository,
) -> None:
    card = _create_card(repo)
    checkpoints = repo.checkpoints(card["id"])
    for checkpoint in checkpoints[:3]:
        repo.set_checkpoint(
            card_id=card["id"],
            checkpoint_key=checkpoint["checkpoint_key"],
            completed=True,
            actor="rsanchez",
        )
    with pytest.raises(PipelineRuleError) as error:
        repo.set_checkpoint(
            card_id=card["id"],
            checkpoint_key=checkpoints[0]["checkpoint_key"],
            completed=False,
            actor="rsanchez",
        )
    assert error.value.requires_confirmation
    result = repo.set_checkpoint(
        card_id=card["id"],
        checkpoint_key=checkpoints[0]["checkpoint_key"],
        completed=False,
        actor="rsanchez",
        reset_downstream=True,
    )
    assert not any(bool(row["completed"]) for row in result)


def test_route_change_resets_only_with_confirmation(repo: PipelineRepository) -> None:
    card = _create_card(repo)
    first = repo.checkpoints(card["id"])[0]
    repo.set_checkpoint(
        card_id=card["id"], checkpoint_key=first["checkpoint_key"], completed=True, actor="rsanchez"
    )
    with pytest.raises(PipelineRuleError) as error:
        repo.change_route(card["id"], route_key="homologaciones_anunciadas", actor="rsanchez")
    assert error.value.requires_confirmation
    updated = repo.change_route(
        card["id"],
        route_key="homologaciones_anunciadas",
        actor="rsanchez",
        confirm_reset=True,
    )
    assert updated["route_key"] == "homologaciones_anunciadas"
    assert len(repo.checkpoints(card["id"])) == 13
    assert not any(bool(row["completed"]) for row in repo.checkpoints(card["id"]))


def test_filters_progress_analytics_contacts_documents_and_audit(repo: PipelineRepository) -> None:
    first = _create_card(repo)
    second = _create_card(
        repo,
        ficha="100523",
        producto="Bolsa para esterilizacion",
        proveedor="OEM Medical",
        marca="CleanPack",
        route_key="fichas_recien_creadas",
    )
    checkpoint = repo.checkpoints(first["id"])[0]
    repo.set_checkpoint(
        card_id=first["id"], checkpoint_key=checkpoint["checkpoint_key"], completed=True, actor="rsanchez"
    )
    cards = repo.list_cards(PipelineFilters(providers=("Foyomed",)))
    assert [card["id"] for card in cards] == [first["id"]]
    assert cards[0]["progress"] == pytest.approx(10.0)
    analytics = repo.analytics()
    assert analytics["total_cards"] == 2
    assert analytics["average_progress"] == pytest.approx(5.0)
    contact = repo.add_contact(
        first["id"], actor="rsanchez", nombre="Ana", email="ana@example.com", es_principal=True
    )
    assert repo.contacts(first["id"])[0]["id"] == contact["id"]
    document = repo.add_document(
        first["id"],
        actor="rsanchez",
        file_name="ficha.pdf",
        file_url="https://drive.google.com/file/1",
        drive_file_id="1",
        mime_type="application/pdf",
    )
    assert repo.documents(first["id"])[0]["id"] == document["id"]
    actions = {row["action"] for row in repo.activities(first["id"])}
    assert {"card_created", "checkpoint_completed", "contact_added", "document_added"} <= actions
    repo.archive_card(second["id"], actor="rsanchez")
    assert len(repo.list_cards()) == 1


def test_card_update_rejects_a_stale_version(repo: PipelineRepository) -> None:
    card = _create_card(repo)
    updated = repo.update_card(
        card["id"],
        actor="usuario_a",
        expected_version=card["version"],
        descripcion="Cambio del usuario A",
    )
    assert updated["version"] == card["version"] + 1
    with pytest.raises(PipelineError, match="otro usuario"):
        repo.update_card(
            card["id"],
            actor="usuario_b",
            expected_version=card["version"],
            descripcion="Cambio obsoleto del usuario B",
        )


def test_duplicate_card_copies_progress_contacts_and_document_references(
    repo: PipelineRepository,
) -> None:
    original = _create_card(repo)
    for checkpoint in repo.checkpoints(original["id"])[:3]:
        repo.set_checkpoint(
            card_id=original["id"],
            checkpoint_key=checkpoint["checkpoint_key"],
            completed=True,
            actor="rsanchez",
        )
    original_contact = repo.add_contact(
        original["id"],
        actor="rsanchez",
        nombre="Ana",
        email="ana@example.com",
        es_principal=True,
    )
    original_document = repo.add_document(
        original["id"],
        actor="rsanchez",
        file_name="ficha.pdf",
        file_url="https://drive.google.com/file/1",
        drive_file_id="1",
        mime_type="application/pdf",
    )

    duplicated = repo.duplicate_card(original["id"], actor="jsilva")

    assert duplicated["id"] != original["id"]
    assert duplicated["identity_key"].startswith(original["identity_key"] + "|COPIA:")
    for field in (
        "ficha",
        "nombre_ficha",
        "producto",
        "proveedor",
        "marca",
        "descripcion",
        "route_key",
        "estado",
        "responsable",
        "prioridad",
        "fecha_objetivo",
    ):
        assert duplicated[field] == original[field]
    assert duplicated["source"] == "duplicate"
    duplicate_view = next(
        card for card in repo.list_cards() if card["id"] == duplicated["id"]
    )
    assert duplicate_view["progress"] == pytest.approx(30.0)
    assert [bool(row["completed"]) for row in repo.checkpoints(duplicated["id"])[:4]] == [
        True,
        True,
        True,
        False,
    ]
    duplicate_contact = repo.contacts(duplicated["id"])[0]
    assert duplicate_contact["id"] != original_contact["id"]
    assert duplicate_contact["email"] == original_contact["email"]
    duplicate_document = repo.documents(duplicated["id"])[0]
    assert duplicate_document["id"] != original_document["id"]
    assert duplicate_document["drive_file_id"] == original_document["drive_file_id"]
    assert duplicate_document["file_url"] == original_document["file_url"]
    assert {row["action"] for row in repo.activities(duplicated["id"])} >= {
        "card_duplicated_from"
    }
    assert {row["action"] for row in repo.activities(original["id"])} >= {
        "card_duplicated_to"
    }

    # Se pueden crear varias copias y editar una sin chocar con la identidad
    # funcional de la tarjeta original.
    second_duplicate = repo.duplicate_card(original["id"], actor="jsilva")
    assert second_duplicate["identity_key"] != duplicated["identity_key"]
    updated = repo.update_card(
        duplicated["id"],
        actor="jsilva",
        expected_version=duplicated["version"],
        descripcion="Copia ajustada",
    )
    assert updated["descripcion"] == "Copia ajustada"
    assert "|COPIA:" in updated["identity_key"]
    assert len(repo.list_cards()) == 3


def test_delete_card_is_audited_recoverable_and_concurrency_safe(
    repo: PipelineRepository,
) -> None:
    card = _create_card(repo)
    changed = repo.update_card(
        card["id"],
        actor="usuario_a",
        expected_version=card["version"],
        descripcion="Actualizada por otro usuario",
    )
    with pytest.raises(PipelineError, match="modificada por otro usuario"):
        repo.archive_card(
            card["id"],
            actor="usuario_b",
            expected_version=card["version"],
        )

    repo.archive_card(
        card["id"],
        actor="usuario_b",
        expected_version=changed["version"],
    )
    assert repo.list_cards() == []
    archived = repo.list_cards(PipelineFilters(include_archived=True))
    assert len(archived) == 1
    assert bool(archived[0]["archived"])
    assert "card_archived" in {row["action"] for row in repo.activities(card["id"])}

    repo.archive_card(card["id"], actor="usuario_b", archived=False)
    assert len(repo.list_cards()) == 1
    assert "card_restored" in {row["action"] for row in repo.activities(card["id"])}


def test_changing_primary_contact_is_mirrored_for_both_contacts(
    repo: PipelineRepository,
) -> None:
    card = _create_card(repo)
    first = repo.add_contact(
        card["id"], actor="rsanchez", nombre="Ana", es_principal=True
    )
    repo.add_contact(card["id"], actor="rsanchez", nombre="Luis", es_principal=True)
    contacts = repo.contacts(card["id"])
    assert [contact["nombre"] for contact in contacts] == ["Luis", "Ana"]
    assert [bool(contact["es_principal"]) for contact in contacts] == [True, False]
    pending = repo.list_outbox(limit=500)
    first_contact_events = [
        event
        for event in pending
        if event["entity_type"] == "contact" and event["entity_id"] == first["id"]
    ]
    assert len(first_contact_events) == 2
    assert first_contact_events[-1]["payload_data"]["es_principal"] == 0


class _FakeWorksheet:
    def __init__(self, title: str, rows: int = 1000, cols: int = 30):
        self.title = title
        self.values: list[list[str]] = []

    def get_all_values(self):
        return [list(row) for row in self.values]

    def update(self, _range: str, values):
        self.values = [[str(value) for value in row] for row in values]

    def clear(self):
        self.values = []


class _FakeSpreadsheet:
    def __init__(self):
        self.worksheets: dict[str, _FakeWorksheet] = {}

    def worksheet(self, title: str):
        if title not in self.worksheets:
            raise WorksheetNotFound(title)
        return self.worksheets[title]

    def add_worksheet(self, title: str, rows: int, cols: int):
        worksheet = _FakeWorksheet(title, rows, cols)
        self.worksheets[title] = worksheet
        return worksheet


class _FakeClient:
    def __init__(self):
        self.sheet = _FakeSpreadsheet()

    def open_by_key(self, _sheet_id: str):
        return self.sheet


def test_sheets_outbox_is_idempotent(repo: PipelineRepository) -> None:
    card = _create_card(repo)
    client = _FakeClient()
    mirror = PipelineSheetsMirror(client=client, sheet_ids="sheet", repository=repo)
    first = mirror.sync_pending(limit=200)
    assert first["errors"] == 0
    assert first["synced"] == 12  # tarjeta + 10 controles + actividad
    assert mirror.sync_pending(limit=200)["synced"] == 0
    card_sheet = client.sheet.worksheets["pipeline_cards"].values
    assert len(card_sheet) == 2
    assert card["id"] in card_sheet[1]


def _trello_board() -> dict:
    standard_items = [
        {"name": label, "state": "complete" if index < 2 else "incomplete"}
        for index, (_, label) in enumerate(ROUTES["fichas_viejas"].checklist)
    ]
    return {
        "name": "RIR Medical - Pipeline Estrategico",
        "lists": [{"id": "l1", "name": "Etapa 1: Fichas Viejas"}],
        "customFields": [
            {"id": "f1", "name": "Ficha Tecnica", "type": "text"},
            {"id": "f2", "name": "Producto", "type": "text"},
            {"id": "f3", "name": "Proveedor", "type": "text"},
            {"id": "f4", "name": "Proveedor/marca", "type": "text"},
            {"id": "f5", "name": "Correo Electronico", "type": "text"},
        ],
        "checklists": [{"id": "cl1", "name": "Checklist", "checkItems": standard_items}],
        "cards": [
            {
                "id": "trello-card-1",
                "idList": "l1",
                "name": "Circuito pediatrico",
                "desc": "Descripcion",
                "closed": False,
                "idChecklists": ["cl1"],
                "customFieldItems": [
                    {"idCustomField": "f1", "value": {"text": "43358"}},
                    {"idCustomField": "f2", "value": {"text": "Circuito pediatrico"}},
                    {"idCustomField": "f3", "value": {"text": "Foyomed"}},
                    {"idCustomField": "f4", "value": {"text": "Foyomed / MFLab"}},
                    {"idCustomField": "f5", "value": {"text": "ventas@example.com"}},
                ],
                "attachments": [{"name": "Ficha", "url": "https://trello/attachment/1"}],
            }
        ],
    }


def test_trello_import_rejects_html_and_imports_real_json_idempotently(
    repo: PipelineRepository,
) -> None:
    with pytest.raises(TrelloImportError, match="página HTML"):
        load_trello_export(b"<!doctype html><html></html>")
    board = load_trello_export(json.dumps(_trello_board()))
    preview = preview_trello_export(board)
    assert preview.eligible_cards == 1
    result = import_trello_board(repo, board, actor="rsanchez")
    assert result["created"] == 1
    card = repo.list_cards()[0]
    assert [bool(row["completed"]) for row in repo.checkpoints(card["id"])[:3]] == [True, True, False]
    assert repo.contacts(card["id"])[0]["email"] == "ventas@example.com"
    assert repo.documents(card["id"])[0]["storage_provider"] == "trello"
    repeated = import_trello_board(repo, board, actor="rsanchez")
    assert repeated["existing"] == 1
    assert len(repo.list_cards()) == 1
    assert len(repo.contacts(card["id"])) == 1
    assert len(repo.documents(card["id"])) == 1


def test_trello_duplicate_identity_merges_progress_and_combined_provider_brand(
    repo: PipelineRepository,
) -> None:
    board = _trello_board()
    base = dict(board["cards"][0])
    base["id"] = "trello-card-2"
    base["name"] = "Misma oportunidad con mas avance"
    board["cards"].append(base)
    board["checklists"].append(
        {
            "id": "cl2",
            "name": "Checklist avanzado",
            "checkItems": [
                {"name": label, "state": "complete" if index < 4 else "incomplete"}
                for index, (_, label) in enumerate(ROUTES["fichas_viejas"].checklist)
            ],
        }
    )
    board["cards"][1]["idChecklists"] = ["cl2"]
    # El campo simple queda viejo; la pareja combinada debe prevalecer.
    for item in board["cards"][0]["customFieldItems"]:
        if item["idCustomField"] == "f3":
            item["value"] = {"text": "Proveedor viejo"}
    preview = preview_trello_export(board)
    assert preview.eligible_cards == 2
    result = import_trello_board(repo, board, actor="rsanchez")
    assert result["created"] == 1
    assert result["existing"] == 1
    cards = repo.list_cards()
    assert len(cards) == 1
    assert cards[0]["proveedor"] == "Foyomed"
    assert cards[0]["marca"] == "MFLab"
    assert cards[0]["progress"] == pytest.approx(40.0)


class _FakeDriveRequest:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        return self.payload


class _FakeDriveFiles:
    def __init__(self):
        self.created: list[dict] = []

    def list(self, **_kwargs):
        return _FakeDriveRequest({"files": []})

    def create(self, *, body, fields, supportsAllDrives, media_body=None):
        assert supportsAllDrives
        file_id = f"id-{len(self.created) + 1}"
        payload = {
            "id": file_id,
            "name": body["name"],
            "mimeType": "application/pdf" if media_body is not None else body.get("mimeType", ""),
            "size": "4",
            "webViewLink": f"https://drive/{file_id}",
        }
        self.created.append({"body": body, "fields": fields, "media": media_body})
        return _FakeDriveRequest(payload)


class _FakeDrive:
    def __init__(self):
        self.api = _FakeDriveFiles()

    def files(self):
        return self.api


def test_drive_upload_creates_pipeline_and_card_folders() -> None:
    drive = _FakeDrive()
    storage = PipelineDriveStorage.from_config(drive, parent_folder_id="root")
    result = storage.upload(
        card={"id": "card", "ficha": "43358", "proveedor": "Foyomed", "marca": "MFLab"},
        file_name="ficha.pdf",
        data=b"test",
        mime_type="application/pdf",
    )
    assert result["webViewLink"].startswith("https://drive/")
    assert [item["body"]["name"] for item in drive.api.created] == [
        "Pipeline Estrategico",
        "43358 - Foyomed - MFLab",
        "ficha.pdf",
    ]
