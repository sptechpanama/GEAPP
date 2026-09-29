from copy import deepcopy
from unittest.mock import MagicMock
import pytest

from services.anestesia_docs import file_hash
from services.anestesia_storage import AnestesiaStorage, TABLES, SHEET_ID
from googleapiclient.errors import HttpError
from httplib2 import Response


def workbook_metadata(*names):
    return {"sheets": [{"properties": {"title": n}} for n in names]}


def google_error(status, message):
    return HttpError(Response({"status": str(status)}),
        ('{"error":{"message":"' + message + '"}}').encode())


def test_office_id_recovers_existing_native_book_without_converting_or_creating_queue():
    sheets, drive = MagicMock(), MagicMock()
    sheets.spreadsheets.return_value.get.return_value.execute.side_effect = [
        google_error(400, "This operation is not supported for this document. The document must not be an Office file."),
        workbook_metadata("pc_config", "pc_manual", *TABLES)]
    storage = AnestesiaStorage(drive, sheets, sheet_id="office-xlsx")
    storage.ensure_tables()
    assert storage.sheet_id == SHEET_ID
    assert [c.kwargs["spreadsheetId"] for c in sheets.spreadsheets.return_value.get.call_args_list] == ["office-xlsx", SHEET_ID]
    sheets.spreadsheets.return_value.batchUpdate.assert_not_called()
    sheets.spreadsheets.return_value.values.assert_not_called()
    drive.assert_not_called()
    assert not drive.mock_calls


@pytest.mark.parametrize("status", [400, 403, 404, 429, 503])
def test_access_or_transient_errors_never_silently_redirect_the_library(status):
    sheets = MagicMock()
    sheets.spreadsheets.return_value.get.return_value.execute.side_effect = google_error(status, "Other failure")
    storage = AnestesiaStorage(None, sheets, sheet_id="configured-native")
    with pytest.raises(HttpError): storage.ensure_tables()
    assert storage.sheet_id == "configured-native"
    assert sheets.spreadsheets.return_value.get.call_count == 1
    sheets.spreadsheets.return_value.batchUpdate.assert_not_called()


def test_office_fallback_without_orchestrator_is_rejected_before_any_mutation():
    sheets = MagicMock()
    sheets.spreadsheets.return_value.get.return_value.execute.side_effect = [
        google_error(400, "The document must not be an Office file."), workbook_metadata("Sheet1")]
    storage = AnestesiaStorage(None, sheets, sheet_id="office")
    with pytest.raises(ValueError, match="cola del orquestador"): storage.ensure_tables()
    assert storage.sheet_id == "office"
    sheets.spreadsheets.return_value.batchUpdate.assert_not_called()


def test_valid_configured_native_workbook_is_preserved():
    sheets = MagicMock()
    sheets.spreadsheets.return_value.get.return_value.execute.return_value = workbook_metadata(*TABLES)
    storage = AnestesiaStorage(None, sheets, sheet_id="custom-native")
    storage.ensure_tables()
    assert storage.sheet_id == "custom-native"
    sheets.spreadsheets.return_value.batchUpdate.assert_not_called()


def test_metadata_revision_keeps_pdf_and_previous_record_immutable(monkeypatch):
    storage = AnestesiaStorage(None, None)
    data = b"original certificate"
    original = {"id": "previous", "file_id": "drive-original", "sha256": file_hash(data), "kind": "css",
        "verified": False, "expires": "2026-04-30", "url": "https://drive.example/original", "_row": 2}
    copy = deepcopy(original)
    writes = []
    monkeypatch.setattr(storage, "get_bytes", lambda file_id: data)
    monkeypatch.setattr(storage, "_write", lambda *args, **kwargs: writes.append((args, kwargs)))
    new = storage.revise_document(original, {"verified": True, "evidence": "Página 1"}, actor="usuario")
    assert original == copy and new["id"] != original["id"]
    assert new["file_id"] == "drive-original" and new["expires"] == "2026-04-30"
    assert new["previous_id"] == "previous" and "_row" not in new
    assert len(writes) == 1 and "row" not in writes[0][1]


def test_mutated_original_cannot_be_marked_verified(monkeypatch):
    storage = AnestesiaStorage(None, None)
    monkeypatch.setattr(storage, "get_bytes", lambda ident: b"changed")
    with pytest.raises(ValueError, match="cambió"):
        storage.revise_document({"file_id": "id", "sha256": file_hash(b"original")}, {"verified": True}, actor="usuario")


def test_reordered_sheet_headers_block_write_without_overwriting_cells():
    sheets = MagicMock()
    api = sheets.spreadsheets.return_value.values.return_value
    api.get.return_value.execute.return_value = {"values": [list(reversed(TABLES["ANESTESIA_DOCUMENTOS"]))]}
    storage = AnestesiaStorage(None, sheets)
    with pytest.raises(ValueError, match="estructura"):
        storage._write("ANESTESIA_DOCUMENTOS", {}, {"id": "new"})
    api.append.assert_not_called()
    api.update.assert_not_called()


def test_existing_worker_registration_does_not_duplicate_configuration():
    sheets = MagicMock()
    api = sheets.spreadsheets.return_value.values.return_value
    api.batchGet.return_value.execute.return_value = {"valueRanges": [
        {"values": [["name", "python", "script", "days", "times", "enabled"], ["anestesia_docs", "python", "script", "", "", "si"]]},
        {"values": [["id", "job", "requested_by", "requested_at", "status", "notes", "payload", "result_file_id", "result_file_url", "result_file_name", "result_error"]]}]}
    AnestesiaStorage(None, sheets).register_worker(python_path="python", script_path="script")
    api.append.assert_not_called()
