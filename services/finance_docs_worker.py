from __future__ import annotations

import base64
import hashlib
import io
import json
import mimetypes
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import gspread
import pandas as pd
import requests
from flask import Flask, jsonify, request
from google.oauth2.service_account import Credentials
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/cloud-platform",
]

WS_DOCS_FINANCIEROS = "FinanzasDocs"
DEFAULT_MODEL = "gpt-4o-mini"

EMPRESAS = ["RIR", "RS-SP"]

ING_CATEGORY_OPTIONS = [
    "Proyectos",
    "Oficina",
    "Otros ingresos operativos",
    "Ingreso financiero",
    "Ingreso no operativo",
    "Aporte de socio / capital",
    "Financiamiento recibido",
    "Miscelaneos",
]
GAS_CATEGORY_OPTIONS = [
    "Proyectos",
    "Gastos fijos",
    "Gastos operativos",
    "Oficina",
    "Inversiones",
    "Miscelaneos",
    "Comisiones",
    "Gasto financiero",
    "Impuestos",
]
GAS_BALANCE_OPTIONS = [
    "Gasto del periodo",
    "Activo fijo",
    "Inventario",
    "Anticipo / prepago",
    "Inversion / participacion en otra empresa",
    "Cuenta por cobrar / prestamo otorgado",
    "Cancelacion de pasivo / deuda",
]

DOC_COL_ROWID = "RowID"
DOC_COL_FECHA_CARGA = "Fecha carga"
DOC_COL_USUARIO = "Usuario"
DOC_COL_EMPRESA = "Empresa"
DOC_COL_ARCHIVO = "Archivo"
DOC_COL_HASH = "Hash archivo"
DOC_COL_MIME = "Mime type"
DOC_COL_DRIVE_ID = "Drive file id"
DOC_COL_DRIVE_URL = "Drive url"
DOC_COL_ORIGEN = "Origen"
DOC_COL_NOTA = "Nota usuario"
DOC_COL_TEXTO_USUARIO = "Texto usuario"
DOC_COL_TIPO = "Tipo documento"
DOC_COL_ESTADO = "Estado borrador"
DOC_COL_MENSAJE = "Mensaje"
DOC_COL_API_USADA = "API usada"
DOC_COL_MODELO = "Modelo IA"
DOC_COL_JSON = "Extraccion JSON"
DOC_COL_DESTINO = "Destino sugerido"
DOC_COL_ACCION = "Accion sugerida"
DOC_COL_CATEGORIA = "Categoria sugerida"
DOC_COL_TRATAMIENTO = "Tratamiento sugerido"
DOC_COL_ESTADO_MOV = "Estado movimiento"
DOC_COL_FECHA_HECHO = "Fecha hecho sugerida"
DOC_COL_FECHA_ESPERADA = "Fecha esperada sugerida"
DOC_COL_FECHA_REAL = "Fecha real sugerida"
DOC_COL_MONTO = "Monto sugerido"
DOC_COL_CONTRAPARTE = "Contraparte sugerida"
DOC_COL_DETALLE = "Detalle sugerido"
DOC_COL_DESCRIPCION = "Descripcion sugerida"
DOC_COL_CONFIANZA = "Confianza"
DOC_COL_DUPLICADO = "Posible duplicado"
DOC_COL_INSTRUMENTO = "Instrumento sugerido"
DOC_COL_PAGO_CONSUMOS = "Pago consumos sugerido"
DOC_COL_INTERES = "Interes sugerido"
DOC_COL_OTROS_CARGOS = "Otros cargos sugeridos"
DOC_COL_OCR_USADO = "OCR usado"
DOC_COL_WS_REG = "Worksheet registrado"
DOC_COL_ROWID_REG = "RowID registrado"
DOC_COL_APROBADO_POR = "Aprobado por"
DOC_COL_FECHA_APROBADO = "Fecha aprobado"

DOC_BASE_COLUMNS = [
    DOC_COL_ROWID,
    DOC_COL_FECHA_CARGA,
    DOC_COL_USUARIO,
    DOC_COL_EMPRESA,
    DOC_COL_ARCHIVO,
    DOC_COL_HASH,
    DOC_COL_MIME,
    DOC_COL_DRIVE_ID,
    DOC_COL_DRIVE_URL,
    DOC_COL_ORIGEN,
    DOC_COL_NOTA,
    DOC_COL_TEXTO_USUARIO,
    DOC_COL_TIPO,
    DOC_COL_ESTADO,
    DOC_COL_MENSAJE,
    DOC_COL_API_USADA,
    DOC_COL_MODELO,
    DOC_COL_JSON,
    DOC_COL_DESTINO,
    DOC_COL_ACCION,
    DOC_COL_CATEGORIA,
    DOC_COL_TRATAMIENTO,
    DOC_COL_ESTADO_MOV,
    DOC_COL_FECHA_HECHO,
    DOC_COL_FECHA_ESPERADA,
    DOC_COL_FECHA_REAL,
    DOC_COL_MONTO,
    DOC_COL_CONTRAPARTE,
    DOC_COL_DETALLE,
    DOC_COL_DESCRIPCION,
    DOC_COL_CONFIANZA,
    DOC_COL_DUPLICADO,
    DOC_COL_INSTRUMENTO,
    DOC_COL_PAGO_CONSUMOS,
    DOC_COL_INTERES,
    DOC_COL_OTROS_CARGOS,
    DOC_COL_OCR_USADO,
    DOC_COL_WS_REG,
    DOC_COL_ROWID_REG,
    DOC_COL_APROBADO_POR,
    DOC_COL_FECHA_APROBADO,
]


def _load_local_secrets() -> dict[str, Any]:
    secrets_path = Path(__file__).resolve().parents[1] / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return {}
    try:
        import tomllib

        with secrets_path.open("rb") as fh:
            return tomllib.load(fh)
    except Exception:
        return {}


LOCAL_SECRETS = _load_local_secrets()


def _secret(path: str, default: str = "") -> str:
    env_key = path.upper().replace(".", "_")
    raw = os.environ.get(env_key)
    if raw:
        return raw.strip()
    cur: Any = LOCAL_SECRETS
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
    return str(cur or default).strip()


def _service_account_info_from_env() -> dict[str, Any] | None:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_INFO")
    if raw:
        try:
            info = json.loads(raw)
            if isinstance(info, dict):
                return info
        except Exception:
            pass
    info = LOCAL_SECRETS.get("google_service_account")
    return dict(info) if isinstance(info, dict) else None


def _google_credentials():
    info = _service_account_info_from_env()
    if info:
        pk = str(info.get("private_key", "") or "")
        if "\\n" in pk and "\n" not in pk:
            info["private_key"] = pk.replace("\\n", "\n")
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    creds, _ = google.auth.default(scopes=SCOPES)
    return creds


def _gspread_client(creds):
    return gspread.authorize(creds)


def _drive_client(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _sheet_id() -> str:
    return _secret("app.SHEET_ID") or os.environ.get("SHEET_ID", "")


def _worksheet_name() -> str:
    return os.environ.get("FINANCE_DOCS_WORKSHEET", WS_DOCS_FINANCIEROS)


def _folder_id_for_empresa(empresa: str) -> str:
    suffix = str(empresa or "").upper().replace("-", "_").replace(" ", "_")
    return (
        os.environ.get(f"DRIVE_FINANCE_DOCS_FOLDER_ID_{suffix}")
        or _secret(f"app.DRIVE_FINANCE_DOCS_FOLDER_ID_{suffix}")
        or ""
    ).strip()


def _openai_api_key() -> str:
    return os.environ.get("OPENAI_API_KEY", "").strip() or _secret("app.OPENAI_API_KEY")


def _openai_model() -> str:
    return (
        os.environ.get("OPENAI_FINANCE_DOC_MODEL")
        or os.environ.get("OPENAI_MODEL")
        or _secret("app.OPENAI_FINANCE_DOC_MODEL")
        or DEFAULT_MODEL
    ).strip()


def _today_iso() -> str:
    return date.today().isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_bytes(raw: bytes | None) -> str:
    return hashlib.sha256(raw or b"").hexdigest()


def _ensure_worksheet(gc, sheet_id: str):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(_worksheet_name())
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=_worksheet_name(), rows=1000, cols=len(DOC_BASE_COLUMNS) + 5)
        ws.update([DOC_BASE_COLUMNS], "A1")
        return ws

    headers = ws.row_values(1)
    if not headers:
        ws.update([DOC_BASE_COLUMNS], "A1")
    else:
        missing = [col for col in DOC_BASE_COLUMNS if col not in headers]
        if missing:
            ws.update([headers + missing], "A1")
    return ws


def _load_docs_df(ws) -> pd.DataFrame:
    values = ws.get_all_records()
    df = pd.DataFrame(values)
    for col in DOC_BASE_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0 if col in {DOC_COL_MONTO, DOC_COL_CONFIANZA, DOC_COL_PAGO_CONSUMOS, DOC_COL_INTERES, DOC_COL_OTROS_CARGOS} else ""
    return df


def _append_doc_rows(ws, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    values = [[row.get(col, "") for col in DOC_BASE_COLUMNS] for row in rows]
    ws.append_rows(values, value_input_option="USER_ENTERED")


def _download_drive_file(drive, file_id: str) -> tuple[bytes, str, str, str]:
    meta = drive.files().get(
        fileId=file_id,
        fields="id,name,mimeType,webViewLink",
        supportsAllDrives=True,
    ).execute()
    request_obj = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return (
        fh.getvalue(),
        str(meta.get("name", "")),
        str(meta.get("mimeType", "") or mimetypes.guess_type(str(meta.get("name", "")))[0] or ""),
        str(meta.get("webViewLink", "")),
    )


def _list_drive_files(drive, folder_id: str, limit: int) -> list[dict[str, Any]]:
    resp = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,webViewLink,modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=int(limit),
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return list(resp.get("files", []) or [])


def _extract_pdf_text_local(content: bytes, max_chars: int) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        return ""
    try:
        reader = PdfReader(io.BytesIO(content or b""))
        chunks: list[str] = []
        for page in reader.pages[: int(os.environ.get("FINANCE_DOCS_MAX_PDF_PAGES", "5"))]:
            chunks.append(page.extract_text() or "")
            if sum(len(x) for x in chunks) >= max_chars:
                break
        return "\n".join(chunks).strip()[:max_chars]
    except Exception:
        return ""


def _vision_image_text(content: bytes, creds, max_chars: int) -> str:
    try:
        from google.cloud import vision
    except Exception:
        return ""
    try:
        client = vision.ImageAnnotatorClient(credentials=creds)
        response = client.document_text_detection(image=vision.Image(content=content))
        err_msg = getattr(getattr(response, "error", None), "message", "")
        if err_msg:
            return ""
        return str(getattr(response.full_text_annotation, "text", "") or "").strip()[:max_chars]
    except Exception:
        return ""


def _render_pdf_pages_with_pymupdf(content: bytes, max_pages: int) -> list[bytes]:
    try:
        import fitz
    except Exception:
        return []
    try:
        doc = fitz.open(stream=content or b"", filetype="pdf")
        images: list[bytes] = []
        for idx in range(min(len(doc), int(max_pages))):
            page = doc.load_page(idx)
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            images.append(pix.tobytes("png"))
        return images
    except Exception:
        return []


def _ocr_text(content: bytes, mime_type: str, creds) -> tuple[str, str]:
    max_chars = int(os.environ.get("FINANCE_DOCS_MAX_TEXT_CHARS", "8000"))
    mime = str(mime_type or "").lower()
    if "pdf" in mime:
        text = _extract_pdf_text_local(content, max_chars)
        if text:
            return text, "PDF texto local"
        chunks: list[str] = []
        max_pages = int(os.environ.get("FINANCE_DOCS_MAX_PDF_PAGES", "5"))
        for page_image in _render_pdf_pages_with_pymupdf(content, max_pages):
            page_text = _vision_image_text(page_image, creds, max_chars)
            if page_text:
                chunks.append(page_text)
            if sum(len(x) for x in chunks) >= max_chars:
                break
        text = "\n".join(chunks).strip()[:max_chars]
        return text, "Google Vision PDF render" if text else ""
    if mime.startswith("image/"):
        text = _vision_image_text(content, creds, max_chars)
        return text, "Google Vision" if text else ""
    return "", ""


def _parse_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.S)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _call_openai(file_name: str, mime_type: str, text: str, empresa: str) -> tuple[dict[str, Any], str]:
    api_key = _openai_api_key()
    if not api_key:
        return {}, "No"
    model = _openai_model()
    prompt = (
        "Extrae datos de un documento financiero para crear un BORRADOR en Finanzas 1. "
        "No es registro final; un humano aprueba. Devuelve SOLO JSON valido con claves: "
        "tipo_documento, destino_sugerido, accion_sugerida, categoria_operativa, tratamiento_balance, "
        "estado_movimiento, fecha_hecho, fecha_esperada, fecha_real, monto, contraparte, detalle, descripcion, "
        "instrumento_financiero, pago_consumos, interes_pagado, otros_cargos, confianza, explicacion.\n"
        "Destinos permitidos: Ingreso, Gasto, Tarjeta de credito, Linea de credito, Factoring, Gestion de cobro, Revisar manualmente.\n"
        "Para tarjeta, usa accion_sugerida 'Registrar consumo' o 'Registrar pago / cargo' y separa pago_consumos/interes_pagado/otros_cargos.\n"
        "Para inventario/activo fijo/prepago, usa el tratamiento correcto y baja confianza si falta dato.\n"
        f"Categorias ingreso: {', '.join(ING_CATEGORY_OPTIONS)}.\n"
        f"Categorias gasto: {', '.join(GAS_CATEGORY_OPTIONS)}.\n"
        f"Tratamientos gasto: {', '.join(GAS_BALANCE_OPTIONS)}.\n"
        "Fechas YYYY-MM-DD. Monto decimal.\n\n"
        f"Empresa: {empresa}\nArchivo: {file_name}\nMIME: {mime_type}\nTexto OCR/PDF:\n{text[:8000]}"
    )
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "Eres un extractor financiero conservador. Devuelve JSON valido; si falta informacion, baja confianza."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
            "max_tokens": int(os.environ.get("FINANCE_DOCS_OPENAI_MAX_TOKENS", "800")),
        },
        timeout=90,
    )
    response.raise_for_status()
    raw = str((response.json().get("choices") or [{}])[0].get("message", {}).get("content", "") or "")
    return _parse_json_object(raw), "Si"


def _float_value(value: Any) -> float:
    try:
        return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])
    except Exception:
        return 0.0


def _date_or_blank(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.date().isoformat()


def _proposal_from_ai(payload: dict[str, Any], file_name: str, text: str, empresa: str) -> dict[str, Any]:
    lower = " ".join([file_name, text]).lower()
    tipo = str(payload.get("tipo_documento", "") or "").strip()
    destino = str(payload.get("destino_sugerido", "") or "").strip()
    if not destino:
        destino = "Tarjeta de credito" if any(k in lower for k in ["visa", "mastercard", "tarjeta"]) else ("Ingreso" if "cliente" in lower else "Gasto")
    accion = str(payload.get("accion_sugerida", "") or "").strip() or "Registrar"
    monto = _float_value(payload.get("monto"))
    categoria = str(payload.get("categoria_operativa", "") or "").strip()
    tratamiento = str(payload.get("tratamiento_balance", "") or "").strip()
    if destino == "Ingreso":
        categoria = categoria if categoria in ING_CATEGORY_OPTIONS else "Proyectos"
    elif destino in {"Gasto", "Tarjeta de credito"}:
        categoria = categoria if categoria in GAS_CATEGORY_OPTIONS else "Gastos operativos"
        tratamiento = tratamiento if tratamiento in GAS_BALANCE_OPTIONS else "Gasto del periodo"
    return {
        DOC_COL_TIPO: tipo or "Documento financiero",
        DOC_COL_DESTINO: destino,
        DOC_COL_ACCION: accion,
        DOC_COL_CATEGORIA: categoria,
        DOC_COL_TRATAMIENTO: tratamiento,
        DOC_COL_ESTADO_MOV: str(payload.get("estado_movimiento", "") or "Pendiente").strip(),
        DOC_COL_FECHA_HECHO: _date_or_blank(payload.get("fecha_hecho")) or _today_iso(),
        DOC_COL_FECHA_ESPERADA: _date_or_blank(payload.get("fecha_esperada")),
        DOC_COL_FECHA_REAL: _date_or_blank(payload.get("fecha_real")),
        DOC_COL_MONTO: monto,
        DOC_COL_CONTRAPARTE: str(payload.get("contraparte", "") or "").strip(),
        DOC_COL_DETALLE: str(payload.get("detalle", "") or "").strip() or "Otros",
        DOC_COL_DESCRIPCION: str(payload.get("descripcion", "") or f"Documento financiero - {file_name}").strip()[:240],
        DOC_COL_CONFIANZA: max(0.0, min(1.0, _float_value(payload.get("confianza")))),
        DOC_COL_JSON: json.dumps(payload, ensure_ascii=False),
        DOC_COL_INSTRUMENTO: str(payload.get("instrumento_financiero", "") or "").strip(),
        DOC_COL_PAGO_CONSUMOS: max(0.0, _float_value(payload.get("pago_consumos"))),
        DOC_COL_INTERES: max(0.0, _float_value(payload.get("interes_pagado"))),
        DOC_COL_OTROS_CARGOS: max(0.0, _float_value(payload.get("otros_cargos"))),
        DOC_COL_MENSAJE: str(payload.get("explicacion", "") or "Borrador generado por worker.").strip()[:500],
    }


def _possible_duplicate(row: dict[str, Any], docs_df: pd.DataFrame) -> str:
    file_hash = str(row.get(DOC_COL_HASH, "") or "").strip()
    drive_id = str(row.get(DOC_COL_DRIVE_ID, "") or "").strip()
    if not docs_df.empty:
        if drive_id and DOC_COL_DRIVE_ID in docs_df and docs_df[DOC_COL_DRIVE_ID].astype(str).str.strip().eq(drive_id).any():
            return "Posible duplicado: mismo archivo Drive ya existe."
        if file_hash and DOC_COL_HASH in docs_df and docs_df[DOC_COL_HASH].astype(str).str.strip().eq(file_hash).any():
            return "Posible duplicado: mismo hash de archivo ya existe."
    return ""


def _new_doc_row(*, empresa: str, file_id: str, file_name: str, mime_type: str, drive_url: str, file_hash: str, text: str, ocr_used: str, payload: dict[str, Any], api_used: str) -> dict[str, Any]:
    proposal = _proposal_from_ai(payload, file_name, text, empresa)
    row = {col: "" for col in DOC_BASE_COLUMNS}
    row.update(proposal)
    row.update(
        {
            DOC_COL_ROWID: hashlib.sha1(f"{file_id}:{file_hash}".encode("utf-8")).hexdigest(),
            DOC_COL_FECHA_CARGA: _today_iso(),
            DOC_COL_USUARIO: "finance-docs-worker",
            DOC_COL_EMPRESA: empresa,
            DOC_COL_ARCHIVO: file_name,
            DOC_COL_HASH: file_hash,
            DOC_COL_MIME: mime_type,
            DOC_COL_DRIVE_ID: file_id,
            DOC_COL_DRIVE_URL: drive_url,
            DOC_COL_ORIGEN: "Drive worker",
            DOC_COL_NOTA: f"Procesado automaticamente para {empresa} a las {_now_iso()}",
            DOC_COL_TEXTO_USUARIO: text[:8000],
            DOC_COL_ESTADO: "Procesado" if api_used == "Si" else "Borrador",
            DOC_COL_API_USADA: api_used,
            DOC_COL_MODELO: _openai_model() if api_used == "Si" else "",
            DOC_COL_OCR_USADO: ocr_used,
        }
    )
    return row


def run_scan() -> dict[str, Any]:
    sheet_id = _sheet_id()
    if not sheet_id:
        raise RuntimeError("Falta SHEET_ID.")
    creds = _google_credentials()
    gc = _gspread_client(creds)
    drive = _drive_client(creds)
    ws = _ensure_worksheet(gc, sheet_id)
    docs_df = _load_docs_df(ws)
    existing_drive_ids = set(docs_df.get(DOC_COL_DRIVE_ID, pd.Series(dtype=str)).astype(str).str.strip())
    existing_hashes = set(docs_df.get(DOC_COL_HASH, pd.Series(dtype=str)).astype(str).str.strip())
    limit = int(os.environ.get("FINANCE_DOCS_SCAN_LIMIT", "50"))
    rows_to_append: list[dict[str, Any]] = []
    result: dict[str, Any] = {"created": 0, "skipped": 0, "errors": [], "companies": {}}

    for empresa in EMPRESAS:
        folder_id = _folder_id_for_empresa(empresa)
        company_stats = {"created": 0, "skipped": 0, "errors": []}
        result["companies"][empresa] = company_stats
        if not folder_id:
            company_stats["errors"].append("Falta carpeta Drive.")
            continue
        try:
            files = _list_drive_files(drive, folder_id, limit)
        except Exception as exc:
            company_stats["errors"].append(f"No se pudo listar Drive: {str(exc)[:180]}")
            continue
        for item in files:
            file_id = str(item.get("id", "") or "").strip()
            if not file_id:
                continue
            if file_id in existing_drive_ids:
                company_stats["skipped"] += 1
                result["skipped"] += 1
                continue
            try:
                content, file_name, mime_type, drive_url = _download_drive_file(drive, file_id)
                file_hash = _hash_bytes(content)
                if file_hash in existing_hashes:
                    company_stats["skipped"] += 1
                    result["skipped"] += 1
                    continue
                text, ocr_used = _ocr_text(content, mime_type, creds)
                payload, api_used = _call_openai(file_name, mime_type, text, empresa) if text else ({}, "No")
                row = _new_doc_row(
                    empresa=empresa,
                    file_id=file_id,
                    file_name=file_name,
                    mime_type=mime_type,
                    drive_url=drive_url,
                    file_hash=file_hash,
                    text=text,
                    ocr_used=ocr_used,
                    payload=payload,
                    api_used=api_used,
                )
                row[DOC_COL_DUPLICADO] = _possible_duplicate(row, docs_df)
                rows_to_append.append(row)
                existing_drive_ids.add(file_id)
                existing_hashes.add(file_hash)
                company_stats["created"] += 1
                result["created"] += 1
            except Exception as exc:
                company_stats["errors"].append(f"{str(item.get('name', '') or file_id)}: {str(exc)[:180]}")
        result["errors"].extend(company_stats["errors"])

    _append_doc_rows(ws, rows_to_append)
    return result


app = Flask(__name__)


def _authorized() -> bool:
    expected = os.environ.get("FINANCE_DOCS_WORKER_TOKEN", "").strip()
    if not expected:
        return True
    auth_header = request.headers.get("Authorization", "")
    token_header = request.headers.get("X-Worker-Token", "")
    return auth_header == f"Bearer {expected}" or token_header == expected


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.post("/run")
def run_endpoint():
    if not _authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        return jsonify({"ok": True, **run_scan()})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:500]}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
