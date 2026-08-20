from __future__ import annotations

import re
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Callable, Iterable, Sequence

DEFAULT_PANAMACOMPRA_KEYWORDS = ("chiller", "york", "daikin")
KEYWORD_REGISTRY_HEADERS = ("Palabra clave", "Actualizado por", "Actualizado")
_REGISTRY_LOCK = threading.RLock()


class KeywordRegistryError(RuntimeError):
    """La lista persistente no pudo leerse o verificarse de forma segura."""


class KeywordRegistryConflictError(KeywordRegistryError):
    """La lista cambio remotamente entre la lectura y la escritura."""


@dataclass(frozen=True)
class KeywordRegistrySnapshot:
    terms: tuple[str, ...]
    remote_ok: bool
    source: str
    warning: str = ""


def _normalize_search_text(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )
    text = re.sub(r"[^0-9a-z]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_keyword_term(value: object) -> str:
    raw = str(value or "").strip()
    root_match = raw.endswith("*")
    normalized = _normalize_search_text(raw[:-1] if root_match else raw)
    if normalized and root_match:
        return f"{normalized}*"
    return normalized


def normalize_keyword_terms(values: Iterable[object]) -> list[str]:
    """Normaliza y elimina duplicados conservando el orden configurado."""

    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        term = normalize_keyword_term(value)
        if not term or term in seen:
            continue
        seen.add(term)
        output.append(term)
    return output


def parse_keyword_registry_values(values: Sequence[Sequence[object]]) -> list[str]:
    """Extrae las palabras de una hoja, con o sin encabezado reconocido."""

    rows = list(values or [])
    if not rows:
        return []
    first = normalize_keyword_term(rows[0][0] if rows[0] else "")
    header = normalize_keyword_term(KEYWORD_REGISTRY_HEADERS[0])
    start = 1 if first in {header, "palabra", "keyword", "termino"} else 0
    return normalize_keyword_terms(
        row[0] for row in rows[start:] if row and str(row[0] or "").strip()
    )


def keyword_registry_sheet_values(
    terms: Iterable[object],
    *,
    updated_by: str,
    updated_at: str,
) -> list[list[str]]:
    rows = [list(KEYWORD_REGISTRY_HEADERS)]
    rows.extend(
        [term, str(updated_by or "sistema"), str(updated_at or "")]
        for term in normalize_keyword_terms(terms)
    )
    return rows


def apply_keyword_changes(
    current: Iterable[object],
    *,
    add: Iterable[object] = (),
    remove: Iterable[object] = (),
) -> list[str]:
    """Aplica altas/bajas exactas sin alterar las demas palabras."""

    existing = normalize_keyword_terms(current)
    remove_set = set(normalize_keyword_terms(remove))
    kept = [term for term in existing if term not in remove_set]
    kept_set = set(kept)
    kept.extend(term for term in normalize_keyword_terms(add) if term not in kept_set)
    return normalize_keyword_terms(kept)


class KeywordRegistryStore:
    """Registro resiliente en Google Sheets para las palabras de Actos RS/SP.

    La hoja remota es la fuente canonica. Nunca se ejecuta ``clear()`` antes de
    guardar: se escribe primero, se limpia solo el sobrante y luego se vuelve a
    leer para verificar que la lista completa quedo persistida.
    """

    def __init__(
        self,
        client_provider: Callable[[], object],
        *,
        sheet_id: str,
        worksheet_name: str,
        defaults: Iterable[object] = DEFAULT_PANAMACOMPRA_KEYWORDS,
        attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client_provider = client_provider
        self.sheet_id = str(sheet_id or "").strip()
        self.worksheet_name = str(worksheet_name or "").strip()
        self.defaults = tuple(normalize_keyword_terms(defaults))
        self.attempts = max(1, int(attempts))
        self.sleeper = sleeper

    def _retry(self, action: Callable[[], object]) -> object:
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            try:
                return action()
            except KeywordRegistryConflictError:
                raise
            except Exception as exc:  # pragma: no cover - tipo depende de API
                last_error = exc
                if attempt + 1 < self.attempts:
                    self.sleeper(0.4 * (2**attempt))
        raise KeywordRegistryError(str(last_error or "Operacion remota no ejecutada."))

    @staticmethod
    def _worksheet_missing(exc: Exception) -> bool:
        return type(exc).__name__ == "WorksheetNotFound"

    def _open_worksheet(self) -> tuple[object, bool]:
        if not self.sheet_id or not self.worksheet_name:
            raise KeywordRegistryError("Falta identificar la hoja persistente.")
        spreadsheet = self.client_provider().open_by_key(self.sheet_id)
        try:
            return spreadsheet.worksheet(self.worksheet_name), False
        except Exception as exc:
            if not self._worksheet_missing(exc):
                raise
            worksheet = spreadsheet.add_worksheet(
                title=self.worksheet_name,
                rows=2000,
                cols=len(KEYWORD_REGISTRY_HEADERS),
            )
            return worksheet, True

    @staticmethod
    def _ensure_columns(worksheet: object) -> None:
        current = int(getattr(worksheet, "col_count", 0) or 0)
        required = len(KEYWORD_REGISTRY_HEADERS)
        if current and current < required:
            worksheet.add_cols(required - current)

    def _write_rows(
        self,
        worksheet: object,
        terms: Iterable[object],
        *,
        updated_by: str,
        previous_values: Sequence[Sequence[object]],
    ) -> list[str]:
        rows = keyword_registry_sheet_values(
            terms,
            updated_by=updated_by,
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        self._ensure_columns(worksheet)
        worksheet.update(f"A1:C{len(rows)}", rows)
        if len(previous_values) > len(rows):
            worksheet.batch_clear([f"A{len(rows) + 1}:C{len(previous_values)}"])
        verified = parse_keyword_registry_values(worksheet.get_all_values())
        expected = normalize_keyword_terms(terms)
        if verified != expected:
            raise KeywordRegistryError(
                "La verificacion posterior no coincide con la lista solicitada."
            )
        return verified

    def load(self, *, last_good: Iterable[object] = ()) -> KeywordRegistrySnapshot:
        fallback = normalize_keyword_terms(last_good) or list(self.defaults)

        def operation() -> list[str]:
            worksheet, created = self._open_worksheet()
            values = worksheet.get_all_values()
            if created:
                return self._write_rows(
                    worksheet,
                    self.defaults,
                    updated_by="sistema",
                    previous_values=values,
                )
            if not values:
                # Una lista configurada como vacia conserva el encabezado. Una
                # hoja totalmente vacia indica creacion incompleta/corrupcion;
                # restauramos la ultima copia valida (o los defaults iniciales).
                return self._write_rows(
                    worksheet,
                    fallback,
                    updated_by="recuperacion automatica",
                    previous_values=values,
                )
            return parse_keyword_registry_values(values)

        try:
            with _REGISTRY_LOCK:
                terms = self._retry(operation)
            return KeywordRegistrySnapshot(tuple(terms), True, "Google Sheets")
        except Exception as exc:
            return KeywordRegistrySnapshot(
                tuple(fallback),
                False,
                "ultima lectura valida" if list(last_good) else "respaldo inicial",
                str(exc),
            )

    def save(
        self,
        terms: Iterable[object],
        *,
        updated_by: str,
        expected_current: Iterable[object] | None = None,
    ) -> list[str]:
        requested = normalize_keyword_terms(terms)
        expected = (
            None
            if expected_current is None
            else normalize_keyword_terms(expected_current)
        )

        def operation() -> list[str]:
            worksheet, _ = self._open_worksheet()
            previous_values = worksheet.get_all_values()
            remote_terms = parse_keyword_registry_values(previous_values)
            if expected is not None and remote_terms != expected:
                # Si el primer intento alcanzo a escribir pero fallo al leer la
                # verificacion, el reintento encuentra exactamente el resultado
                # pedido. Se considera exito idempotente, no conflicto.
                if remote_terms == requested:
                    return remote_terms
                raise KeywordRegistryConflictError(
                    "La lista cambio en otra sesion. Recarga antes de volver a guardar."
                )
            return self._write_rows(
                worksheet,
                requested,
                updated_by=updated_by,
                previous_values=previous_values,
            )

        with _REGISTRY_LOCK:
            return list(self._retry(operation))


@lru_cache(maxsize=512)
def _keyword_pattern(normalized_term: str) -> re.Pattern[str] | None:
    normalized_term = normalize_keyword_term(normalized_term)
    if not normalized_term:
        return None
    root_match = normalized_term.endswith("*")
    term_body = normalized_term[:-1].strip() if root_match else normalized_term
    tokens = [re.escape(token) for token in term_body.split() if token]
    if not tokens:
        return None
    token_pattern = r"\s+".join(tokens)
    if root_match:
        token_pattern += r"[0-9a-z]*"
    pattern = rf"(?<![0-9a-z]){token_pattern}(?![0-9a-z])"
    return re.compile(pattern)


def match_keywords_in_text(text: object, keywords: Iterable[object]) -> list[str]:
    normalized_text = _normalize_search_text(text)
    if not normalized_text:
        return []

    matches: list[str] = []
    seen: set[str] = set()
    for raw_keyword in keywords:
        keyword = normalize_keyword_term(raw_keyword)
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        pattern = _keyword_pattern(keyword)
        if pattern and pattern.search(normalized_text):
            matches.append(keyword)
    return matches
