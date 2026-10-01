"""Real publication logic with a deterministic Drive transport and injected failures."""
from copy import deepcopy
import json
import re
from types import SimpleNamespace

import fitz
import pytest

from services.anestesia_docs import file_hash, BASE_KINDS
from services.anestesia_delivery import DeliveryPublisher, original_pdf_set, CURRENT_NAME, UPDATING_NAME, ROLE, FOLDER_MIME


def pdf_bytes(text, pages=1, signature=False):
    with fitz.open() as pdf:
        for i in range(pages):
            page = pdf.new_page()
            page.insert_text((50, 50), f'{text} pagina {i+1}')
        if signature:
            widget = fitz.Widget()
            widget.field_name = 'Signature'; widget.field_type = fitz.PDF_WIDGET_TYPE_SIGNATURE
            widget.rect = fitz.Rect(50, 100, 200, 140)
            page.add_widget(widget)
        return pdf.tobytes()


def test_twelve_pdfs_keep_each_example_attachment_separate_and_byte_identical():
    originals = {k: pdf_bytes(k, 2 if k in {'oferente', 'inscripcion_producto'} else 1) for k in BASE_KINDS}
    before = deepcopy(originals)
    selected = {k: {'id': k} for k in originals}
    result, notes = original_pdf_set(originals, selected)
    assert len(result) == 11  # plus quotation = 12
    assert not notes and originals == before
    assert {ident for f in result for ident in f['library_ids']} == set(BASE_KINDS)
    for file in result:
        assert file['data'] == originals[file['kind']]
        assert file['library_ids'] == [file['kind']]
        assert file['original_hashes'] == {file['kind']: file_hash(originals[file['kind']])}
    assert any(f['kind'] == 'oferente' for f in result)
    assert any(f['kind'] == 'inscripcion_producto' for f in result)


@pytest.mark.parametrize('signed', ['oferente', 'inscripcion_producto'])
def test_signature_fields_preserve_original_bytes_and_separate_files(signed):
    originals = {k: pdf_bytes(k, signature=k == signed) for k in BASE_KINDS}
    result, notes = original_pdf_set(originals, {k: {'id': k} for k in originals})
    assert len(result) == 11 and not notes  # signatures stay intact without changing the file count
    assert all(f['data'] == originals[f['kind']] for f in result)


def test_additional_requirements_are_never_dropped_to_force_file_count():
    originals = {k: pdf_bytes(k) for k in (*BASE_KINDS, 'otro:Certificado adicional')}
    result, _ = original_pdf_set(originals, {k: {'id': k} for k in originals})
    assert len(result) == 12 and any(f['kind'].startswith('otro:') for f in result)


class Request:
    def __init__(self, fn): self.fn = fn
    def execute(self): return self.fn()


class MemoryDrive:
    def __init__(self):
        self.files_data = {}; self.content = {}; self.counter = 0
        self.copy_count = 0; self.fail_copy = None; self.crash_copy = None; self.corrupt_copy = None
    def create(self, body, **kwargs):
        def run():
            self.counter += 1; ident = str(self.counter)
            self.files_data[ident] = {'id': ident, **deepcopy(body)}
            return deepcopy(self.files_data[ident])
        return Request(run)
    def list(self, q, **kwargs):
        def run():
            parent = re.search(r"'([^']+)' in parents", q)[1]
            values = [deepcopy(f) for f in self.files_data.values() if not f.get('trashed') and parent in f.get('parents', [])]
            if 'appProperties has' in q: values = [f for f in values if f.get('appProperties', {}).get('role') == ROLE]
            return {'files': values}
        return Request(run)
    def update(self, fileId, body=None, addParents=None, removeParents=None, **kwargs):
        def run():
            f = self.files_data[fileId]
            if removeParents: f['parents'].remove(removeParents)
            if addParents: f['parents'].append(addParents)
            for k, v in (body or {}).items():
                if k == 'appProperties':
                    props = f.setdefault(k, {})
                    for key, value in v.items():
                        if value is None: props.pop(key, None)
                        else: props[key] = value
                else: f[k] = v
            return deepcopy(f)
        return Request(run)
    def copy(self, fileId, body, **kwargs):
        def run():
            self.copy_count += 1
            if self.fail_copy == self.copy_count: raise TimeoutError('network failure')
            if self.crash_copy == self.copy_count: raise KeyboardInterrupt('simulated worker termination')
            result = self.create({**deepcopy(self.files_data[fileId]), **body}).execute()
            # Source id is not a writable field in Drive's copy body.
            ident = str(self.counter); result['id'] = ident; self.files_data[ident]['id'] = ident
            self.content[ident] = self.content[fileId] + (b'changed' if self.corrupt_copy == self.copy_count else b'')
            return result
        return Request(run)


class Storage:
    def __init__(self):
        self.api = MemoryDrive(); self.drive = SimpleNamespace(files=lambda: self.api)
    def root(self): return 'root'
    def folder(self, name, parent):
        matches = [f for f in self.api.files_data.values() if f['name'] == name and parent in f.get('parents', [])]
        return matches[0]['id'] if matches else self.api.create({'name': name, 'parents': [parent], 'mimeType': FOLDER_MIME}).execute()['id']
    def get_bytes(self, ident): return self.api.content[ident]
    def json_file(self, ident): return json.loads(self.get_bytes(ident))
    def put(self, parent, name, data, mime):
        f = self.api.create({'name': name, 'parents': [parent], 'mimeType': mime,
            'appProperties': {'sha256': file_hash(data), 'module': 'anestesia_docs'}}).execute()
        self.api.content[f['id']] = data
        return {'file_id': f['id'], 'name': name, 'sha256': file_hash(data), 'mime': mime}


def package(storage, act, count=12):
    return [storage.put('immutable-' + act, f'{i:02d}_documento.pdf', pdf_bytes(f'{act}-{i}'), 'application/pdf') for i in range(count)]


def publish(storage, files, act):
    return DeliveryPublisher(storage).publish(files, request_id='request-' + act, number=act, manifest_hash='hash-' + act)


def contents(storage, folder):
    files = DeliveryPublisher(storage)._children(folder)
    return {f['name']: storage.get_bytes(f['id']) for f in files}


def test_new_request_replaces_the_whole_set_in_same_folder_and_archives_previous():
    storage = Storage(); first = package(storage, 'A'); second = package(storage, 'B')
    a = publish(storage, first, 'A'); old = contents(storage, a['folder_id'])
    b = publish(storage, second, 'B')
    assert a['folder_id'] == b['folder_id'] and b['count'] == 12
    assert contents(storage, b['folder_id']) == {f['name']: storage.get_bytes(f['file_id']) for f in second}
    assert all(data in storage.api.content.values() for data in old.values())
    assert storage.api.files_data[b['folder_id']]['appProperties']['act'] == 'B'
    before = storage.api.copy_count
    assert publish(storage, second, 'B') == b and storage.api.copy_count == before


@pytest.mark.parametrize('failure', ['network', 'wrong_bytes'])
def test_failed_replacement_restores_old_delivery_without_mixed_acts(failure):
    storage = Storage(); a = publish(storage, package(storage, 'A'), 'A')
    old = contents(storage, a['folder_id']); files = package(storage, 'B')
    setattr(storage.api, 'fail_copy' if failure == 'network' else 'corrupt_copy', storage.api.copy_count + 5)
    with pytest.raises(RuntimeError, match='restauró'):
        publish(storage, files, 'B')
    assert contents(storage, a['folder_id']) == old
    meta = storage.api.files_data[a['folder_id']]
    assert meta['name'] == CURRENT_NAME and meta['appProperties']['act'] == 'A'
    assert publish(storage, files, 'B')['folder_id'] == a['folder_id']


def test_hard_worker_interruption_is_recovered_on_next_attempt():
    storage = Storage(); a = publish(storage, package(storage, 'A'), 'A')
    files = package(storage, 'B'); storage.api.crash_copy = storage.api.copy_count + 4
    with pytest.raises(KeyboardInterrupt): publish(storage, files, 'B')
    meta = storage.api.files_data[a['folder_id']]
    assert meta['name'] == UPDATING_NAME and meta['appProperties']['state'] == 'updating'
    result = publish(storage, files, 'B')
    assert result['folder_id'] == a['folder_id'] and result['count'] == 12
    assert contents(storage, result['folder_id']) == {f['name']: storage.get_bytes(f['file_id']) for f in files}


def test_foreign_file_is_never_removed_to_force_pdf_count():
    storage = Storage(); a = publish(storage, package(storage, 'A'), 'A')
    storage.put(a['folder_id'], 'manual.txt', b'keep', 'text/plain'); old = contents(storage, a['folder_id'])
    with pytest.raises(ValueError, match='ajenos'): publish(storage, package(storage, 'B'), 'B')
    assert contents(storage, a['folder_id']) == old


def test_changed_approved_pdf_cannot_replace_current_delivery():
    storage = Storage(); a = publish(storage, package(storage, 'A'), 'A'); old = contents(storage, a['folder_id'])
    files = package(storage, 'B'); storage.api.content[files[0]['file_id']] += b'changed'
    with pytest.raises(ValueError, match='Cambió un PDF'): publish(storage, files, 'B')
    assert contents(storage, a['folder_id']) == old


def test_shorter_new_set_leaves_no_old_extra_pdf():
    storage = Storage(); a = publish(storage, package(storage, 'A', 13), 'A')
    publish(storage, package(storage, 'B', 12), 'B')
    assert len(contents(storage, a['folder_id'])) == 12
