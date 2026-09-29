from copy import deepcopy
from unittest.mock import MagicMock
import pytest

from services.anestesia_docs import file_hash
from services.anestesia_storage import AnestesiaStorage, TABLES


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
