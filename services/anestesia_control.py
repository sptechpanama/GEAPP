"""User-editable control of the eleven original supporting PDFs in native Sheets."""
from __future__ import annotations

from datetime import date

from services.anestesia_docs import add_months, document_kind, file_hash, parse_date
from services.anestesia_storage import escape

CONTROL_NAME = "Control de documentos - Anestesia"
DOCUMENTS_FOLDER = "Documentos y vigencias"
OTHER = "Otros documentos"
DOCUMENTS = (
    ("aviso_operacion", "Aviso de Operación de RIR Medical Engineering, S.EP.", "Copia simple de aviso de operación", 30),
    ("cedula", "Copia de cédula del representante legal", "Copia simple de cédula o pasaporte del Rep. Legal", 30),
    ("catalogo", "Especificaciones técnicas y catálogo del producto", "Ficha técnica / Especificaciones técnicas", 30),
    ("criterio_tecnico", "Certificado de Criterio Técnico", OTHER, 30),
    ("oferente", "Certificado de Registro Nacional de Oferente", OTHER, 30),
    ("inscripcion_producto", "Constancia de inscripción del producto en el Registro Nacional de Oferente", OTHER, 30),
    ("metodo_destruccion", "Método o procedimiento de destrucción o disposición final del fabricante", OTHER, 30),
    ("registro_publico", "Certificado del Registro Público", OTHER, 10),
    ("css", "Paz y salvo de la Caja de Seguro Social", OTHER, 10),
    ("dgi", "Paz y salvo de la Dirección General de Ingresos", OTHER, 10),
    ("licencia_minsa", "Licencia de Operaciones MINSA para la comercialización de dispositivos médicos", OTHER, 30),
)


def latest_documents(rows):
    selected = {}
    for row in rows:
        kind = document_kind(row.get("kind"))
        if kind not in {d[0] for d in DOCUMENTS}:
            continue
        if kind not in selected or row.get("created_at", "") > selected[kind].get("created_at", ""):
            selected[kind] = row
    missing = [name for kind, name, _, _ in DOCUMENTS if kind not in selected]
    if missing:
        raise ValueError("Faltan originales para organizar la carpeta: " + ", ".join(missing))
    return selected


def initial_expiry(document):
    expiry = parse_date(document.get("expires"))
    note = "Fecha de vencimiento del documento original; editable manualmente."
    if document_kind(document.get("kind")) == "registro_publico" and not expiry:
        issued = parse_date(document.get("issued"))
        if not issued:
            raise ValueError("El Registro Público necesita una emisión verificada para su control de antigüedad.")
        expiry = add_months(issued, 12)
        note = ("Control de antigüedad: emisión + 12 meses, según el pliego de la ficha 43358 "
            "revisado en el acto 2026-1-10-01-08-CL-051191. No es un vencimiento impreso del certificado. "
            "Editar si el acto exige una antigüedad menor.")
    if not expiry and not document.get("no_expiry_confirmed"):
        raise ValueError("No se ha verificado vencimiento o ausencia de vencimiento de " + document.get("kind", "documento"))
    return expiry, note if expiry else "El original no indica vencimiento; no se inventa una fecha."


def expiry_status(value, warning_days, *, today):
    expiry = parse_date(value)
    if str(value or "").strip() == "-":
        return "Sin vencimiento"
    if not expiry:
        return "Pendiente"
    days = (expiry - today).days
    return "Vencido" if days < 0 else "Por vencer" if days <= warning_days else "Vigente"


def create_control(storage, folder, selected):
    """Provision once. Subsequent calls preserve all manually edited dates."""
    files = storage.drive.files().list(q=f"trashed=false and '{escape(folder)}' in parents and name='{escape(CONTROL_NAME)}'",
        fields="files(id,name,mimeType,webViewLink)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
    if files:
        if len(files) != 1 or files[0]["mimeType"] != "application/vnd.google-apps.spreadsheet":
            raise ValueError("El control existente no es un único Google Sheets nativo. No se modificó.")
        created = files[0]
        sheets = storage.sheets.spreadsheets()
        metadata = sheets.get(spreadsheetId=created["id"], fields="sheets.properties").execute()
        title = metadata["sheets"][0]["properties"]["title"].replace("'", "''")
        existing = sheets.values().get(spreadsheetId=created["id"], range=f"'{title}'!A1:E15").execute().get("values", [])
        if any(any(row) for row in existing):
            if len(existing) < 4 or existing[3][:4] != ["Nombre del documento", "Tipo", "Fecha de vencimiento", "Descargar"]:
                raise ValueError("El control tiene una estructura distinta. No se sobrescribieron sus datos.")
            return created
        # An interrupted first creation can leave a blank native spreadsheet.
        # Complete that same file; never create another one or reset user dates.
    else:
        created = None
    data = []
    for kind, name, type_, days in DOCUMENTS:
        doc = selected[kind]
        content = storage.get_bytes(doc["file_id"])
        if file_hash(content) != doc["sha256"]:
            raise ValueError("Cambió el original de " + name)
        expiry, note = initial_expiry(doc)
        meta = storage.drive.files().get(fileId=doc["file_id"], fields="id,name,mimeType,parents,webContentLink,webViewLink,trashed",
            supportsAllDrives=True).execute()
        if meta.get("trashed") or meta.get("mimeType") != "application/pdf" or folder not in meta.get("parents", []):
            raise ValueError("El original no está disponible en la carpeta de documentos: " + name)
        data.append((name, type_, expiry, note, meta.get("webContentLink") or meta["webViewLink"], days))
    if created is None:
        created = storage.drive.files().create(body={"name": CONTROL_NAME,
            "mimeType": "application/vnd.google-apps.spreadsheet", "parents": [folder],
            "appProperties": {"module": "anestesia_docs", "role": "document_control"}},
            fields="id,name,mimeType,webViewLink", supportsAllDrives=True).execute()
    ident = created["id"]
    sheets = storage.sheets.spreadsheets()
    meta = sheets.get(spreadsheetId=ident, fields="sheets.properties").execute()
    sheet_id = meta["sheets"][0]["properties"]["sheetId"]
    root_meta = storage.drive.files().get(fileId=folder, fields="webViewLink", supportsAllDrives=True).execute()
    grid = {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": 15, "startColumnIndex": 0, "endColumnIndex": 5}
    def cell(text): return {"userEnteredValue": {"stringValue": text}}
    rows = []
    for name, type_, expiry, note, url, days in data:
        expiry_cell = {"userEnteredValue": {"numberValue": (expiry - date(1899, 12, 30)).days} if expiry else {"stringValue": "-"}, "note": note}
        rows.append({"values": [cell(name), cell(type_), expiry_cell,
            {"userEnteredValue": {"formulaValue": '=HYPERLINK("' + url.replace('"', '""') + '","Descargar")'}},
            {"userEnteredValue": {"numberValue": days}}]})
    requests = [
        {"updateSpreadsheetProperties": {"properties": {"title": CONTROL_NAME, "locale": "es_MX",
            "timeZone": "America/Panama", "autoRecalc": "HOUR"}, "fields": "title,locale,timeZone,autoRecalc"}},
        {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "title": "Documentos",
            "gridProperties": {"rowCount": 24, "columnCount": 5, "frozenRowCount": 4}}, "fields": "title,gridProperties"}},
        {"mergeCells": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 4}, "mergeType": "MERGE_ALL"}},
        {"mergeCells": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 4}, "mergeType": "MERGE_ALL"}},
        {"updateCells": {"start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0}, "rows": [{"values": [{"userEnteredValue": {
            "formulaValue": '=HYPERLINK("' + root_meta["webViewLink"] + '","ABRIR CARPETA DE LOS 11 DOCUMENTOS")'}}]}], "fields": "userEnteredValue"}},
        {"updateCells": {"start": {"sheetId": sheet_id, "rowIndex": 1, "columnIndex": 0}, "rows": [{"values": [cell(
            "Vencimientos editables. Verde: vigente · Amarillo: hasta 10 días (CSS/DGI/Registro Público) o 30 días (resto) · Rojo: vencido · -: sin vencimiento.")]}], "fields": "userEnteredValue"}},
        {"updateCells": {"start": {"sheetId": sheet_id, "rowIndex": 3, "columnIndex": 0}, "rows": [{"values": [cell(v) for v in [
            "Nombre del documento", "Tipo", "Fecha de vencimiento", "Descargar", "Aviso (días)"]]}], "fields": "userEnteredValue"}},
        {"updateCells": {"range": grid, "rows": rows, "fields": "userEnteredValue,note"}},
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 15, "startColumnIndex": 0, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Arial", "fontSize": 10}, "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.07, "green": 0.21, "blue": 0.32},
                "textFormat": {"fontFamily": "Arial", "fontSize": 11, "bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment"}},
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.88, "green": 0.93, "blue": 0.97}, "textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.bold"}},
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": 15, "startColumnIndex": 2, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment"}},
        {"setBasicFilter": {"filter": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 15, "startColumnIndex": 0, "endColumnIndex": 5}}}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 5}, "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 4, "endIndex": 15}, "properties": {"pixelSize": 48}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 44}, "fields": "pixelSize"}},
    ]
    for col, width in enumerate([460, 285, 180, 110]):
        requests.append({"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col, "endIndex": col+1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"}})
    for index, (formula, color) in enumerate([
        ('=AND(ISNUMBER($C5),$C5<TODAY())', {"red": 0.96, "green": 0.70, "blue": 0.70}),
        ('=AND(ISNUMBER($C5),$C5>=TODAY(),$C5<=TODAY()+$E5)', {"red": 1, "green": 0.91, "blue": 0.59}),
        ('=AND(ISNUMBER($C5),$C5>TODAY()+$E5)', {"red": 0.72, "green": 0.88, "blue": 0.76}),
    ]):
        requests.append({"addConditionalFormatRule": {"index": index, "rule": {
            "ranges": [{**grid, "startColumnIndex": 2, "endColumnIndex": 3}],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                "format": {"backgroundColor": color}}}}})
    sheets.batchUpdate(spreadsheetId=ident, body={"requests": requests}).execute()
    return created
