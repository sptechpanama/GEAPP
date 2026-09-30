"""Current library health and checks for replacement certificates (no network)."""
from __future__ import annotations

from datetime import date, timedelta
import re

from services.anestesia_docs import (
    BASE_KINDS, KINDS, canonical_hash, document_status,
    file_hash, normalized, now_iso, parse_date, select_documents,
)


def metadata_hash(metadata):
    fields = ('kind', 'company', 'issued', 'expires', 'catalogs', 'fichas', 'models',
              'act', 'evidence', 'no_expiry_confirmed', 'notarized', 'apostilled',
              'translation_verified')
    return canonical_hash({k: metadata.get(k) for k in fields})


def certificate_content_check(data, metadata, *, ocr_text=''):
    """Check the actual PDF; DGI/CSS dates and issuer must match readable content.

    Other document types retain the recorded human review of their scope and
    formalities. This never represents online authentication by an issuer.
    """
    import fitz
    if len(data) > 30 * 1024 * 1024:
        raise ValueError('El PDF supera 30 MB.')
    try:
        with fitz.open(stream=data, filetype='pdf') as pdf:
            if pdf.is_encrypted or not len(pdf) or len(pdf) > 100:
                raise ValueError('PDF vacío, cifrado o de más de 100 páginas')
            texts = [page.get_text() for page in pdf]
    except Exception as exc:
        raise ValueError('Adjunta un PDF legible, sin contraseña, con todas sus páginas (máximo 100).') from exc
    text = normalized(' '.join(texts) + '\n' + ocr_text)
    kind = metadata.get('kind')
    errors = []
    if kind in {'dgi', 'css'}:
        # DGI also mentions CSS in a disclaimer: do not accept that as CSS's issuer.
        is_dgi = 'direccion general de ingresos' in text or 'dgi.mef.gob.pa' in text
        is_css = bool(re.search(r'caja (?:del? )?seguro social', text)) and 'numero patronal' in text and not is_dgi
        if not (is_dgi if kind == 'dgi' else is_css):
            errors.append('No se pudo comprobar que el PDF corresponde al paz y salvo ' + kind.upper() + '. Usa un PDF con texto legible del emisor.')
        if 'rir medical engineering' not in text and not re.search(r'155750585\s*-\s*2\s*-\s*2024', text):
            errors.append('No se pudo comprobar en el PDF el titular o RUC de RIR.')
        dates = set()
        for token in re.findall(r'\b(?:\d{4}-\d{2}-\d{2}|\d{2}[/-]\d{2}[/-]\d{4})\b', text):
            if parse_date(token):
                dates.add(parse_date(token))
        for field, label in [('issued', 'emisión'), ('expires', 'vencimiento')]:
            day = parse_date(metadata.get(field))
            if not day or day not in dates:
                errors.append(f'La fecha de {label} registrada no está comprobada en el PDF.')
        # CSS has a labelled expiration. A historical period is not the expiry.
        if kind == 'css':
            match = re.search(r'valido hasta\s*:?\s*(\d{4}-\d{2}-\d{2}|\d{2}[/-]\d{2}[/-]\d{4})', text)
            if not match or parse_date(match[1]) != parse_date(metadata.get('expires')):
                errors.append('El vencimiento debe coincidir con «Válido hasta» del certificado CSS.')
        if kind == 'dgi' and dates and parse_date(metadata.get('expires')) != max(dates):
            errors.append('Revisa la fecha de validez DGI; no uses la fecha de emisión como vencimiento.')
    return {'version': 1, 'sha256': file_hash(data), 'metadata_hash': metadata_hash(metadata),
            'checked_at': now_iso(), 'pages': len(texts), 'errors': errors,
            'unreadable_pages': [i+1 for i, text in enumerate(texts) if len(text.strip()) < 60],
            'ocr_text': ocr_text[:18000],
            'scope': 'PDF, emisor, titular y fechas impresas' if kind in {'dgi', 'css'}
                     else 'Integridad del PDF; alcance y formalidades mediante revisión registrada',
            'issuer_online_verified': False}


def library_health(library, *, as_of: date, catalog='K'):
    """Show only the latest applicable original, with missing base requirements.

    Does not invent a uniform legal validity period for Registro Público or
    present old act-specific declarations as reusable for a new tender.
    """
    kinds = list(BASE_KINDS)
    kinds.extend(sorted({d.get('kind') for d in library if d.get('kind') and not d.get('act')} - set(kinds)))
    requirements = [{'kind': kind, 'library_only': True} for kind in kinds]
    selected = select_documents(library, requirements, catalog, '')
    result = []
    for rule in requirements:
        kind = rule['kind']; doc = selected.get(kind)
        check = document_status(doc, rule, as_of=as_of, catalog=catalog, act='')
        expiry = parse_date((doc or {}).get('expires'))
        if not doc:
            status = 'Falta · por acto' if kind in {'retorsion', 'calidad'} else 'Falta'
            detail = 'Se debe preparar y verificar para cada acto.' if kind in {'retorsion', 'calidad'} else check['motivo']
        elif expiry and expiry < as_of:
            status, detail = 'Vencido', check['motivo']
        elif check['estado'] != 'Vigente documentalmente':
            status, detail = 'Pendiente de verificar', check['motivo']
        elif kind == 'registro_publico':
            status, detail = 'Según pliego', 'Emisión comprobada. La antigüedad permitida se valida contra cada acto.'
        elif expiry and expiry == as_of:
            status, detail = 'Vence hoy', 'Válido para hoy; requiere renovación para una presentación posterior.'
        elif expiry and expiry <= as_of + timedelta(days=7):
            status, detail = 'Vence pronto', 'Revisar si cubre la fecha de presentación antes de generar.'
        else:
            status, detail = 'Vigente documentalmente', 'Según fechas, alcance y revisión registrados; se revalida para cada acto.'
        result.append({'Documento': KINDS.get(kind) or (doc or {}).get('label') or kind,
                       'Estado': status, 'Emisión': (doc or {}).get('issued', ''),
                       'Vence': str(expiry or ('Sin vencimiento expreso' if (doc or {}).get('no_expiry_confirmed') else '')),
                       'Qué falta / comprobación': detail, 'enlace': (doc or {}).get('url', '')})
    return result
