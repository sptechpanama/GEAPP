"""Use the existing pc_manual queue and shared Sheets credentials."""
from datetime import datetime
from zoneinfo import ZoneInfo
from uuid import uuid4


def queue_external_refresh(client, spreadsheet_id: str, username: str) -> str:
    # Explicit worksheet only; never fall back to an unrelated first tab.
    ws = client.open_by_key(spreadsheet_id).worksheet('pc_manual')
    headers = [str(v).strip().lower() for v in ws.row_values(1)]
    aliases = {'id': 'request_id', 'job': 'job_name', 'requested_at': 'timestamp',
               'created_at': 'timestamp', 'notes': 'note'}
    normalized = [aliases.get(h, h) for h in headers]
    if not {'request_id', 'job_name'}.issubset(normalized):
        raise ValueError('La hoja pc_manual no tiene las columnas esperadas')
    request_id = uuid4().hex
    values = {'request_id': request_id, 'timestamp': datetime.now(ZoneInfo('America/Panama')).strftime('%Y-%m-%d %H:%M:%S'),
              'job_name': 'otras_fuentes', 'job_label': 'Oportunidades externas', 'requested_by': username,
              'note': 'Actualización solicitada desde Oportunidades externas', 'status': 'pending'}
    ws.append_row([values.get(h, '') for h in normalized], value_input_option='RAW')
    return request_id
