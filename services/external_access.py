"""Small supplier-access directory with shared, optimistic PostgreSQL updates."""
from datetime import datetime, timezone

from sqlalchemy import text


COMPANIES = ('RS Engineering', 'SPTech')
STATUSES = ('Pendiente', 'Solicitud enviada', 'En evaluación', 'Registrado; esperando invitación', 'Invitación recibida')
GUIDES = {
    'naturgy': {
        'name': 'Naturgy Panamá',
        'url': 'https://www.naturgy.com.pa/proveedores/',
        'registration_url': 'https://www.naturgy.com.pa/proveedores/',
        'pending': 'Solicitar invitación a Compras y completar evaluación inicial',
        'access': 'La Unidad de Compras decide la incorporación. El alta no garantiza invitaciones a todas las compras.',
        'steps': (
            'Enviar una presentación de la empresa a compraseinstalaciones@naturgy.com, el contacto publicado en el portal oficial. Solicitar evaluación para los servicios que realmente ofrecen.',
            'Esperar la invitación de Compras y completar el cuestionario de evaluación inicial. Naturgy considera capacidad financiera, recursos técnicos y humanos, aspectos legales y reputación.',
            'Revisar los compromisos de ética, derechos humanos y seguridad laboral. Consultar si el servicio requiere homologaciones adicionales.',
            'Confirmar con Compras el canal para recibir y presentar ofertas. Mantener actualizado el registro; el portal indica evaluación periódica anual.',
        ),
    },
    'aes': {
        'name': 'AES Panamá',
        'url': 'https://www.aespanama.com/es/proveedores',
        'registration_url': 'https://www.aespanama.com/es/proveedores-potenciales',
        'pending': 'Registro en Ariba, precalificación e invitación de AES',
        'access': 'La cuenta Ariba por sí sola no da acceso a todas las licitaciones. AES invita a sus procesos y define la precalificación.',
        'steps': (
            'Abrir «Proveedores potenciales» de AES y seguir su botón «Regístrate» hacia Ariba. Usar o crear la cuenta de la empresa; el enlace oficial genera su propia sesión.',
            'Completar el perfil de productos y servicios y revisar las políticas publicadas. AES evalúa capacidad, estabilidad, soporte, seguridad y cumplimiento.',
            'Obtener la invitación de AES al proceso correspondiente. En Ariba se revisa la solicitud de información, propuesta o cotización y la precalificación local.',
            'Si se obtiene un contrato, completar el registro contractual indicado por AES. El formulario posterior puede pedir datos bancarios y certificaciones; entregarlos únicamente por el canal oficial.',
        ),
    },
}


def load_access(engine):
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(text(
            'SELECT source,company,status,notes,updated_by,updated_at FROM external_source_access'
        )).mappings()]


def save_access(engine, *, source, company, status, notes, actor, expected_updated_at=None):
    if source not in GUIDES or company not in COMPANIES or status not in STATUSES:
        raise ValueError('Fuente, empresa o estado inválido')
    notes = str(notes or '').strip()
    if len(notes) > 1500:
        raise ValueError('Las notas deben tener un máximo de 1,500 caracteres')
    values = dict(source=source, company=company, status=status, notes=notes,
                  actor=str(actor or 'usuario')[:150], now=datetime.now(timezone.utc).isoformat(),
                  previous=expected_updated_at)
    with engine.begin() as connection:
        if expected_updated_at is None:
            result = connection.execute(text('''INSERT INTO external_source_access
                (source,company,status,notes,updated_by,updated_at)
                VALUES (:source,:company,:status,:notes,:actor,:now)
                ON CONFLICT(source,company) DO NOTHING'''), values)
        else:
            result = connection.execute(text('''UPDATE external_source_access SET
                status=:status,notes=:notes,updated_by=:actor,updated_at=:now
                WHERE source=:source AND company=:company AND updated_at=:previous'''), values)
        if result.rowcount != 1:
            raise ValueError('Otro usuario actualizó este registro. Actualiza la vista antes de guardar.')
    return values['now']


def access_rows(saved, company):
    existing = {(r['source'], r['company']): r for r in saved}
    next_step = {'Solicitud enviada': 'Dar seguimiento a la respuesta de Compras',
                 'En evaluación': 'Completar la información o precalificación solicitada',
                 'Registrado; esperando invitación': 'Solicitar invitaciones para los rubros de la empresa',
                 'Invitación recibida': 'Revisar requisitos y fecha límite de la invitación; captura privada aún sin conectar'}
    return [dict(Fuente=guide['name'], Empresa=company,
                 Estado=existing.get((source, company), {}).get('status', 'Pendiente'),
                 **{'Pendiente para acceder': next_step.get(existing.get((source, company), {}).get('status'), guide['pending']),
                    'Registro / pasos oficiales': guide['registration_url']})
            for source, guide in GUIDES.items()]
