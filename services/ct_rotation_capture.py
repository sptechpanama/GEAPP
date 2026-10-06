from __future__ import annotations

import hashlib
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

from services import ct_rotation as rotation
from services.anestesia_source import explicit_ficha_evidence
from services.inteligencia_proveedores_v3 import normalize_text


TARGET = re.compile(r"(?<!\d)43358(?!\d)")


def document_ficha_codes(text: str) -> set[str]:
    codes = {proof["ficha"] for proof in explicit_ficha_evidence(text)}
    normalized = normalize_text(text)
    pattern = r"\b(?:ficha\s+tecnica|c\s*t\s*n\s*i|f\s*t)\s*(?:numero|nro|no|n)?\s*(\d{4,7})\b"
    codes.update(match[1] for match in re.finditer(pattern, normalized))
    return codes


def selected_kit_lines(items: list[dict], *, confirmed: bool) -> list[dict]:
    exact = [item for item in items if TARGET.search(str(item.get("descripcion", ""))) and not (document_ficha_codes(str(item.get("descripcion", ""))) - {rotation.FICHA})]
    if exact:
        return exact
    if not confirmed:
        return []
    selected = []
    for item in items:
        description = normalize_text(item.get("descripcion", ""))
        other = document_ficha_codes(description) - {rotation.FICHA}
        if not other and "kit" in description and "circuit" in description and ("anest" in description or "pacient" in description):
            selected.append(item)
    return selected if len(selected) == 1 else []


def seed_from_audit(analysis: dict, relaunch_groups: list[dict]) -> list[dict]:
    hospitals = {row["unit"]: row["hospital"] for row in analysis["hospitals"]}
    repetitions = {code: group for group in relaunch_groups for code in group["codes"]}
    output = []
    for row in analysis["cycles"]:
        repetition = repetitions.get(row["cycle"])
        output.append({
            "ficha": rotation.FICHA, "convocatoria": row["cycle"], "publicacion": row["date"],
            "entidad": row["entity"], "unidad_compra": row["unit"], "hospital": hospitals[row["unit"]],
            "kits": row["kits"], "estado": row["state"], "actos_relacionados": row["codes"],
            "demanda_id": repetition["codes"][0] if repetition else row["cycle"],
            "requisicion": re.search(r"\d[\d-]*", repetition["reference"])[0] if repetition else "", "enlace": row["url"],
            "verificado_en": analysis["summary"]["checked_at"], "actualizado_en": analysis["summary"]["checked_at"],
        })
    return rotation.merge_history([], output)


class OfficialCapture:
    def __init__(self, scraper_root: Path, cache: Path, credentials: Path):
        sys.path.insert(0, str(scraper_root))
        from db import db_api_updater

        self.api = db_api_updater
        self.cache = cache
        self.cache.mkdir(parents=True, exist_ok=True)
        self.credentials = credentials
        self.errors = []

    def _listing(self, kind: int, term: str, start: datetime, end: datetime, depth: int = 0) -> list[dict]:
        body = self.api.listing_payload(0, start, end)
        body["filtro"].update(idTipoProceso=kind, titulo=term)
        result = self.api.request_json("POST", self.api.LIST_ENDPOINT, payload=body)
        if result.get("status") != 1 or not isinstance(result.get("result", {}).get("registros"), list):
            raise ValueError("PanamáCompra no devolvió una lista oficial válida.")
        rows = result["result"]["registros"]
        if len(rows) < self.api.API_PAGE_SIZE:
            return rows
        if depth >= 30 or end - start <= timedelta(seconds=1):
            raise ValueError("La ventana oficial está saturada; no se acepta una captura truncada.")
        middle = start + (end - start) / 2
        return self._listing(kind, term, start, middle, depth + 1) + self._listing(kind, term, middle + timedelta(milliseconds=1), end, depth + 1)

    def _detail(self, record: dict) -> dict:
        flow = int(record["idProcesosContratacionFlujos"])
        path = self.cache / f"{flow}.json"
        try:
            saved = json.loads(path.read_text("utf-8")) if path.exists() else None
        except (OSError, json.JSONDecodeError):
            saved = None
        signature = hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        publication = self.api.parse_source_date(record.get("fechaPublicacion"))
        recent = publication is None or (rotation.now().date() - publication).days <= 90
        if saved and saved.get("signature") == signature:
            age = rotation.now() - datetime.fromisoformat(saved["checked_at"])
            if age < (timedelta(hours=12) if recent else timedelta(days=30)):
                return {**saved, "record": record}
        kind = int(record["idTipoProceso"])
        try:
            payload = self.api.request_json("GET", self.api.DETAIL_ENDPOINT.format(tipo=kind, flujo=flow))
        except Exception:
            payload = self.api.request_json("POST", self.api.API_ROOT + "/ps/documentos-proceso/pliego-general/publico/get-page", payload={"idTipoProceso": kind, "idProcesosContratacionFlujos": flow})
        if payload.get("status") != 1 or not isinstance(payload.get("result", {}).get("pageComponentes"), list):
            raise ValueError(f"Detalle incompleto: {record.get('numProceso', flow)}")
        components = payload["result"]["pageComponentes"]
        detail = {
            "record": record, "signature": signature, "checked_at": rotation.now().isoformat(),
            "labels": self.api._component_labels(components),
            "items": [item for component in components if component.get("tipo") in {"componentItems", "componentItemsPliego"} for item in self.api._component_rows(component)],
            "files": [item for component in components if component.get("tipo") == "componentFiles" for item in self.api._component_rows(component)],
            "relations": [item for component in components if component.get("tipo") == "componentInfoGeneracion" for item in (component.get("value") or []) if isinstance(item, dict)],
        }
        path.write_text(json.dumps(detail, ensure_ascii=False), encoding="utf-8")
        return detail

    def _documents(self, detail: dict) -> tuple[bool, str]:
        import fitz

        document_key = hashlib.sha256(json.dumps(detail["files"], sort_keys=True).encode()).hexdigest()
        path = self.cache / f"docs_{document_key}.json"
        if path.exists():
            try:
                saved = json.loads(path.read_text("utf-8"))
                return saved["confirmed"], saved["requisition"]
            except (OSError, json.JSONDecodeError, KeyError):
                pass
        confirmed, requisition = False, ""
        for attachment in detail["files"]:
            route = str(attachment.get("rutaCompleta", ""))
            if not route.startswith("/procesos-contratacion-archivos/") or ".." in route:
                raise ValueError("Adjunto oficial sin ruta pública válida.")
            with self.api._session().get(self.api.API_ROOT + route, timeout=(10, 60), stream=True) as response:
                response.raise_for_status()
                data = bytearray()
                for chunk in response.iter_content(128 * 1024):
                    data.extend(chunk)
                    if len(data) > 20 * 1024 * 1024:
                        raise ValueError("Anexo demasiado grande para la lectura de rotación.")
            if not data.startswith(b"%PDF"):
                raise ValueError("Anexo no PDF pendiente de interpretar.")
            with fitz.open(stream=bytes(data), filetype="pdf") as document:
                if len(document) > 100:
                    raise ValueError("Anexo de más de 100 páginas pendiente de interpretar.")
                texts = []
                for page in document:
                    text = page.get_text()
                    if len(text.strip()) < 60:
                        from google.cloud import vision
                        from google.oauth2.service_account import Credentials

                        credentials = Credentials.from_service_account_file(str(self.credentials))
                        with vision.ImageAnnotatorClient(credentials=credentials) as client:
                            image = page.get_pixmap(matrix=fitz.Matrix(2, 2)).tobytes("png")
                            result = client.document_text_detection(image=vision.Image(content=image), timeout=60)
                            if result.error.message:
                                raise ValueError("No fue posible interpretar el anexo mediante OCR.")
                            text += "\n" + result.full_text_annotation.text
                    texts.append(text)
                combined = "\n".join(texts)
                confirmed |= rotation.FICHA in document_ficha_codes(combined)
                match = re.search(r"\b(?:requisici[oó]n|req\.)\s*(?:n[uú]mero|no\.?|#|:)?\s*(\d[\d-]{2,})", combined, re.I)
                if match:
                    requisition = match[1]
        path.write_text(json.dumps({"confirmed": confirmed, "requisition": requisition}), encoding="utf-8")
        return confirmed, requisition

    def capture(self, existing: list[dict]) -> list[dict]:
        records = {}
        start = self.api.date_to_api_start(date(2000, 1, 1))
        end = self.api.date_to_api_start(date(2100, 1, 1))
        for kind in (2, -1):
            for term in ("anestesia", "circuito", rotation.FICHA):
                for record in self._listing(kind, term, start, end):
                    records[str(record["idProcesosContratacionFlujos"])] = record
        known = {code: row for row in existing for code in rotation._codes(row)}
        details = {}
        def fetch(record):
            try:
                return self._detail(record)
            except Exception:
                self.errors.append(f"Detalle pendiente: {record.get('numProceso', '')}")
                return None
        with ThreadPoolExecutor(max_workers=4) as pool:
            for detail in pool.map(fetch, records.values()):
                if detail:
                    details[str(detail["record"]["idProcesosContratacionFlujos"])] = detail
        candidates = []
        for detail in details.values():
            fields = " ".join([str(detail["labels"].get("titulo", "")), str(detail["labels"].get("descripcion", "")), *(str(item.get("descripcion", "")) for item in detail["items"])])
            normalized = normalize_text(fields)
            if detail["record"]["numProceso"] in known or TARGET.search(fields) or ("circuit" in normalized and "anest" in normalized):
                candidates.append(detail)
        position = 0
        while position < len(candidates):
            detail = candidates[position]
            position += 1
            for relation in detail["relations"]:
                for record in relation.get("generacionProcesos", []):
                    flow = str(record.get("idProcesosContratacionFlujos", ""))
                    if not flow or flow in details:
                        continue
                    related = fetch(record)
                    if related:
                        details[flow] = related
                        candidates.append(related)
                    if len(candidates) > 2000:
                        raise ValueError("Demasiadas relaciones oficiales; se conserva el histórico.")
        confirmed = set()
        requisitions = {}
        for detail in candidates:
            record = detail["record"]
            flow = str(record["idProcesosContratacionFlujos"])
            fields = " ".join([str(detail["labels"].get("titulo", "")), str(detail["labels"].get("descripcion", "")), *(str(item.get("descripcion", "")) for item in detail["items"])])
            if record["numProceso"] in known or TARGET.search(fields):
                confirmed.add(flow)
                if record["numProceso"] not in known and int(record["idTipoProceso"]) == 2:
                    try:
                        _, requisition = self._documents(detail)
                        requisitions[flow] = requisition
                    except Exception:
                        self.errors.append(f"Requisición pendiente: {record['numProceso']}")
                continue
            if not selected_kit_lines(detail["items"], confirmed=True):
                continue
            try:
                matches, requisition = self._documents(detail)
                if matches:
                    confirmed.add(flow)
                    requisitions[flow] = requisition
            except Exception:
                self.errors.append(f"Anexo pendiente: {record['numProceso']}")
        parent = {flow: flow for flow in details}
        def root(flow):
            while parent[flow] != flow:
                parent[flow] = parent[parent[flow]]
                flow = parent[flow]
            return flow
        def join(left, right):
            if left in parent and right in parent:
                parent[root(right)] = root(left)
        numbers = {}
        for flow, detail in details.items():
            code = detail["record"]["numProceso"]
            if code in numbers:
                join(flow, numbers[code])
            numbers[code] = flow
            for relation in detail["relations"]:
                for record in relation.get("generacionProcesos", []):
                    join(flow, str(record.get("idProcesosContratacionFlujos", "")))
        groups = {}
        for flow, detail in details.items():
            groups.setdefault(root(flow), []).append(detail)
        output = []
        stamp = rotation.now().isoformat()
        for group in groups.values():
            if not any(str(detail["record"]["idProcesosContratacionFlujos"]) in confirmed for detail in group):
                continue
            usable = [detail for detail in group if selected_kit_lines(detail["items"], confirmed=True)]
            if not usable:
                continue
            originals = [detail for detail in usable if int(detail["record"]["idTipoProceso"]) == 2]
            original = min(originals or usable, key=lambda detail: (self.api.parse_source_date(detail["labels"].get("fecha de publicacion") or detail["record"].get("fechaPublicacion")) or date.max, int(detail["record"]["idProcesosContratacionFlujos"])))
            record, labels = original["record"], original["labels"]
            publication = self.api.parse_source_date(labels.get("fecha de publicacion") or record.get("fechaPublicacion"))
            if publication is None or publication > rotation.now().date():
                continue
            latest = max(usable, key=lambda detail: int(detail["record"]["idProcesosContratacionFlujos"]))
            previous = known.get(record["numProceso"], {})
            lines = selected_kit_lines(original["items"], confirmed=True)
            if any(normalize_text(item.get("unidad", "")) not in {"kit", "unidad"} for item in lines):
                self.errors.append(f"Unidad de cantidad pendiente: {record['numProceso']}")
                continue
            try:
                quantity = sum(rotation._number(item.get("cantidad")) for item in lines)
            except (TypeError, ValueError):
                self.errors.append(f"Cantidad pendiente: {record['numProceso']}")
                continue
            if not quantity:
                self.errors.append(f"Cantidad pendiente: {record['numProceso']}")
                continue
            unit = labels.get("unidad de compra") or record.get("nombreUnidadCompra", "")
            row = {
                "ficha": rotation.FICHA, "convocatoria": record["numProceso"], "publicacion": publication.isoformat(),
                "entidad": labels.get("entidad") or record.get("nombreEntidad", ""), "unidad_compra": unit,
                "hospital": previous.get("hospital") or unit, "kits": quantity,
                "estado": latest["labels"].get("estado") or latest["record"].get("nombreRealizado") or previous.get("estado", ""),
                "actos_relacionados": ",".join(sorted({detail["record"]["numProceso"] for detail in usable})),
                "demanda_id": previous.get("demanda_id") or record["numProceso"],
                "requisicion": previous.get("requisicion") or requisitions.get(str(record["idProcesosContratacionFlujos"]), ""),
                "enlace": self.api.process_link({**record, "prefijo": "CL" if int(record["idTipoProceso"]) == 2 else record.get("prefijo", "")}), "verificado_en": stamp, "actualizado_en": stamp,
            }
            output.append(row)
        chronological = sorted(output, key=lambda row: (row["publicacion"], row["convocatoria"]))
        current = {row["convocatoria"]: row for row in existing}
        for row in chronological:
            if row["requisicion"] and row["demanda_id"] == row["convocatoria"]:
                for older in sorted(current.values(), key=lambda item: (item["publicacion"], item["convocatoria"])):
                    distance = (date.fromisoformat(row["publicacion"]) - date.fromisoformat(older["publicacion"])).days
                    if (older["convocatoria"] != row["convocatoria"] and older["requisicion"] == row["requisicion"] and older["unidad_compra"] == row["unidad_compra"] and float(older["kits"]) == float(row["kits"]) and 0 <= distance <= 45 and "adjudic" not in normalize_text(older["estado"])):
                        row["demanda_id"] = older["demanda_id"] or older["convocatoria"]
                        break
            current[row["convocatoria"]] = row
        return rotation.merge_history(existing, chronological)
