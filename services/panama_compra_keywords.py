from __future__ import annotations

import re
import math
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Callable, Iterable, Sequence

HVAC_OVER_15K_KEYWORDS = (
    "aire acondicion*>15k",
    "aires acondicion*>15k",
    "sistema de aire acondicionado>15k",
    "aire acondicionado central>15k",
    "split>15k",
    "mini split>15k",
    "minisplit>15k",
    "multisplit>15k",
    "aire acondicionado inverter>15k",
    "expansion directa>15k",
    "sistema dx>15k",
    "vrf>15k",
    "vrv>15k",
    "flujo de refrigerante variable>15k",
    "volumen de refrigerante variable>15k",
    "unidad manejadora de aire>15k",
    "unidad manejador de aire>15k",
    "manejadora de aire>15k",
    "manejador de aire>15k",
    "uma>15k",
    "unidad tipo paquete>15k",
    "unidad paquete>15k",
    "rooftop>15k",
    "roof top>15k",
    "fan coil>15k",
    "fancoil>15k",
    "agua helada>15k",
    "enfriador de agua>15k",
    "torre de enfriamiento>15k",
    "chiller>15k",
    "chiler>15k",
    "shiller>15k",
    "unidad condensadora>15k",
    "unidad evaporadora>15k",
    "cassette de aire acondicionado>15k",
    "bomba de calor>15k",
    "climatizacion*>15k",
)
KEYWORD_RULES_VERSION = 3
DEFAULT_PANAMACOMPRA_KEYWORDS = (
    "chiller",
    "york",
    "daikin",
    *HVAC_OVER_15K_KEYWORDS,
)
DEFAULT_PANAMACOMPRA_NEGATIVE_KEYWORDS = (
    "automotriz",
    "habitacion de hotel",
    "bloqueador solar",
    "protector solar",
    "oracle solaris",
    "correa del serpentin",
)
# Variantes gramaticales evidentes que representan exactamente el mismo
# contexto negativo. La lista visible y persistente conserva un solo nombre
# canonico por regla para mantener la administracion sencilla.
_NEGATIVE_KEYWORD_ALIASES = {
    "habitacion de hotel": (
        "habitacion de hotel",
        "habitaciones de hotel",
        "habitacion hotel",
        "habitaciones hotel",
    ),
    "correa del serpentin": (
        "correa del serpentin",
        "correas del serpentin",
        "correa de serpentin",
        "correas de serpentin",
    ),
}
KEYWORD_REGISTRY_HEADERS = ("Palabra clave", "Actualizado por", "Actualizado")
_REGISTRY_LOCK = threading.RLock()
_AMOUNT_SUFFIX_RE = re.compile(
    r"^(?P<term>.*?)\s*>\s*(?:usd|us\$|b/?\.?|\$)?\s*"
    r"(?P<amount>[0-9][0-9.,\s]*)\s*(?P<unit>[km]?)\s*$",
    re.IGNORECASE,
)


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


@dataclass(frozen=True)
class KeywordRule:
    """Regla normalizada de texto con un umbral monetario opcional."""

    term: str
    minimum_amount: float | None = None

    @property
    def is_root(self) -> bool:
        return self.term.endswith("*")

    @property
    def canonical(self) -> str:
        if self.minimum_amount is None:
            return self.term
        return f"{self.term}>{_format_rule_amount(self.minimum_amount)}"


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


@lru_cache(maxsize=1)
def _legacy_hvac_rule_aliases() -> dict[str, str]:
    """Reconoce solamente las 37 reglas HVAC dañadas por el UI anterior.

    Una versión antigua de Streamlit eliminaba ``>`` y el asterisco antes de
    volver a guardar la hoja; por ejemplo, ``aire acondicion*>15k`` terminaba
    como ``aire acondicion 15k``. La lista cerrada evita interpretar como
    umbral cualquier frase legítima que casualmente termine en ``15k``.
    """

    return {
        _normalize_search_text(rule.replace("*", "").replace(">", " ")): rule
        for rule in HVAC_OVER_15K_KEYWORDS
    }


def _format_rule_amount(value: float) -> str:
    amount = float(value)
    if amount >= 1_000_000 and math.isclose(amount % 1_000_000, 0.0, abs_tol=1e-6):
        return f"{amount / 1_000_000:g}m"
    if amount >= 1_000 and math.isclose(amount % 1_000, 0.0, abs_tol=1e-6):
        return f"{amount / 1_000:g}k"
    return f"{amount:g}"


def parse_reference_amount(value: object) -> float | None:
    """Convierte montos de Sheets sin confundir miles con decimales."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        amount = float(value)
        return amount if math.isfinite(amount) else None

    raw = str(value or "").strip()
    if not raw:
        return None
    negative = raw.startswith("(") and raw.endswith(")")
    raw = re.sub(r"(?i)(B/\.?|USD|US\$|PAB|\$)", "", raw)
    raw = re.sub(r"[^0-9,\.\-]", "", raw)
    if not raw or raw in {"-", ".", ","}:
        return None

    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        parts = raw.split(",")
        if len(parts) > 2:
            raw = "".join(parts[:-1]) + (f".{parts[-1]}" if len(parts[-1]) <= 2 else parts[-1])
        elif len(parts[-1]) <= 2:
            raw = ".".join(parts)
        else:
            raw = "".join(parts)
    elif "." in raw:
        parts = raw.split(".")
        if len(parts) > 2:
            raw = "".join(parts[:-1]) + (f".{parts[-1]}" if len(parts[-1]) <= 2 else parts[-1])
        elif len(parts[-1]) == 3:
            raw = "".join(parts)

    try:
        amount = float(raw)
    except (TypeError, ValueError):
        return None
    if negative:
        amount = -abs(amount)
    return amount if math.isfinite(amount) else None


def parse_keyword_rule(value: object) -> KeywordRule | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    if ">" not in raw:
        raw = _legacy_hvac_rule_aliases().get(_normalize_search_text(raw), raw)

    minimum_amount: float | None = None
    amount_match = _AMOUNT_SUFFIX_RE.fullmatch(raw)
    if amount_match:
        raw = amount_match.group("term").strip()
        amount_text = amount_match.group("amount")
        minimum_amount = parse_reference_amount(amount_text)
        if minimum_amount is None:
            return None
        unit = amount_match.group("unit").lower()
        if unit == "k":
            minimum_amount *= 1_000
        elif unit == "m":
            minimum_amount *= 1_000_000
    elif ">" in raw:
        return None

    root_match = raw.endswith("*")
    normalized = _normalize_search_text(raw[:-1] if root_match else raw)
    if not normalized:
        return None
    term = f"{normalized}*" if root_match else normalized
    return KeywordRule(term=term, minimum_amount=minimum_amount)


def normalize_keyword_term(value: object) -> str:
    rule = parse_keyword_rule(value)
    return rule.canonical if rule else ""


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


def parse_keyword_input(value: object) -> list[str]:
    """Interpreta el contenido de los cuadros Agregar/Quitar."""

    raw = str(value or "").strip()
    if not raw:
        return []
    return normalize_keyword_terms(re.split(r"[,;\n\r]+", raw))


def keyword_table_column_order(columns: Iterable[object]) -> list[object]:
    """Coloca el contexto de detección entre Descripción y los Item_n."""

    original = list(columns)
    desired_keys = (
        "palabras clave detectadas",
        "campos con coincidencia",
        "tipo convocatoria",
        "pestana origen",
    )
    by_key: dict[str, object] = {}
    for column in original:
        normalized = _normalize_search_text(column)
        if normalized in desired_keys and normalized not in by_key:
            by_key[normalized] = column

    context_columns = [by_key[key] for key in desired_keys if key in by_key]
    if not context_columns:
        return original

    context_set = set(context_columns)
    remaining = [column for column in original if column not in context_set]
    normalized_remaining = [_normalize_search_text(column) for column in remaining]

    description_indexes = [
        index
        for index, normalized in enumerate(normalized_remaining)
        if normalized == "descripcion"
    ]
    item_indexes = [
        index
        for index, normalized in enumerate(normalized_remaining)
        if normalized.startswith("item")
    ]
    if description_indexes and (
        not item_indexes or description_indexes[-1] < item_indexes[0]
    ):
        insert_at = description_indexes[-1] + 1
    elif item_indexes:
        insert_at = item_indexes[0]
    else:
        insert_at = len(remaining)

    return remaining[:insert_at] + context_columns + remaining[insert_at:]


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


def _raw_keyword_registry_terms(values: Sequence[Sequence[object]]) -> list[str]:
    """Devuelve la primera columna tal como está guardada, sin repararla."""

    rows = list(values or [])
    if not rows:
        return []
    first = _normalize_search_text(rows[0][0] if rows[0] else "")
    header = _normalize_search_text(KEYWORD_REGISTRY_HEADERS[0])
    start = 1 if first in {header, "palabra", "keyword", "termino"} else 0
    return [
        str(row[0] or "").strip()
        for row in rows[start:]
        if row and str(row[0] or "").strip()
    ]


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
        # gspread 6.x recibe primero ``values`` y luego ``range_name``. Usamos
        # argumentos nombrados para evitar que una actualizacion de la libreria
        # vuelva a invertir silenciosamente ambos valores. RAW conserva el
        # asterisco final de terminos por raiz como ``fotovolta*``.
        worksheet.update(
            values=rows,
            range_name=f"A1:C{len(rows)}",
            value_input_option="RAW",
        )
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
            parsed = parse_keyword_registry_values(values)
            if _raw_keyword_registry_terms(values) != parsed:
                return self._write_rows(
                    worksheet,
                    parsed,
                    updated_by="normalizacion automatica",
                    previous_values=values,
                )
            return parsed

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
            needs_canonical_write = (
                _raw_keyword_registry_terms(previous_values) != remote_terms
            )
            if expected is not None and remote_terms != expected:
                # Si el primer intento alcanzo a escribir pero fallo al leer la
                # verificacion, el reintento encuentra exactamente el resultado
                # pedido. Se considera exito idempotente, no conflicto.
                if remote_terms == requested:
                    if not needs_canonical_write:
                        return remote_terms
                    return self._write_rows(
                        worksheet,
                        requested,
                        updated_by="normalizacion automatica",
                        previous_values=previous_values,
                    )
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

    def mutate(
        self,
        *,
        add: Iterable[object] = (),
        remove: Iterable[object] = (),
        updated_by: str,
    ) -> tuple[list[str], bool]:
        """Aplica altas y bajas sobre la lista remota en una sola operación.

        La interfaz anterior releía la hoja antes de llamar a ``save`` y
        ``save`` volvía a abrirla y leerla. Además de lento, ese recorrido
        aumentaba la ventana de conflicto entre sesiones. Esta operación lee
        la fuente canónica una sola vez, calcula el cambio sobre esa versión,
        escribe y verifica. Si el término ya estaba en el estado solicitado,
        evita una escritura innecesaria.
        """

        additions = normalize_keyword_terms(add)
        removals = normalize_keyword_terms(remove)

        def operation() -> tuple[list[str], bool]:
            worksheet, _ = self._open_worksheet()
            previous_values = worksheet.get_all_values()
            remote_terms = parse_keyword_registry_values(previous_values)
            needs_canonical_write = (
                _raw_keyword_registry_terms(previous_values) != remote_terms
            )
            updated = apply_keyword_changes(
                remote_terms,
                add=additions,
                remove=removals,
            )
            if updated == remote_terms and not needs_canonical_write:
                return remote_terms, False
            verified = self._write_rows(
                worksheet,
                updated,
                updated_by=updated_by,
                previous_values=previous_values,
            )
            return verified, True

        with _REGISTRY_LOCK:
            result = self._retry(operation)
        verified, changed = result
        return list(verified), bool(changed)


@lru_cache(maxsize=512)
def _keyword_pattern(normalized_term: str) -> re.Pattern[str] | None:
    rule = parse_keyword_rule(normalized_term)
    if rule is None:
        return None
    normalized_term = rule.term
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


def match_keywords_in_text(
    text: object,
    keywords: Iterable[object],
    *,
    reference_amount: object = None,
) -> list[str]:
    normalized_text = _normalize_search_text(text)
    if not normalized_text:
        return []

    parsed_amount = parse_reference_amount(reference_amount)
    matches: list[str] = []
    match_index: dict[str, int] = {}
    matched_rules: dict[str, KeywordRule] = {}
    for raw_keyword in keywords:
        rule = parse_keyword_rule(raw_keyword)
        if rule is None:
            continue
        if rule.minimum_amount is not None and (
            parsed_amount is None or parsed_amount <= rule.minimum_amount
        ):
            continue
        pattern = _keyword_pattern(rule.term)
        if not pattern or not pattern.search(normalized_text):
            continue

        # Si coexisten ``chiller`` y ``chiller>15k``, en actos grandes se
        # muestra la regla mas especifica; en actos menores permanece la regla
        # historica sin umbral.
        previous = matched_rules.get(rule.term)
        if previous is None:
            match_index[rule.term] = len(matches)
            matched_rules[rule.term] = rule
            matches.append(rule.canonical)
            continue
        previous_minimum = previous.minimum_amount or -math.inf
        current_minimum = rule.minimum_amount or -math.inf
        if current_minimum > previous_minimum:
            matched_rules[rule.term] = rule
            matches[match_index[rule.term]] = rule.canonical
    return matches


def match_negative_keywords_in_text(
    text: object,
    negative_keywords: Iterable[object],
) -> list[str]:
    """Devuelve reglas negativas presentes, incluyendo variantes seguras."""

    normalized_text = _normalize_search_text(text)
    if not normalized_text:
        return []

    matches: list[str] = []
    seen: set[str] = set()
    for raw_keyword in negative_keywords:
        rule = parse_keyword_rule(raw_keyword)
        if rule is None:
            continue
        canonical = rule.canonical
        variants = _NEGATIVE_KEYWORD_ALIASES.get(rule.term, (rule.term,))
        if not any(
            (pattern := _keyword_pattern(variant)) is not None
            and pattern.search(normalized_text)
            for variant in variants
        ):
            continue
        if canonical not in seen:
            seen.add(canonical)
            matches.append(canonical)
    return matches


def negative_keywords_in_matching_context(
    *,
    title: object,
    matched_field_values: Iterable[object],
    negative_keywords: Iterable[object],
) -> list[str]:
    """Busca negativos solo en titulo y campos que activaron una alerta.

    Este alcance evita descartar un acto valido porque una palabra negativa
    aparezca en un renglon ajeno a la coincidencia positiva.
    """

    configured = normalize_keyword_terms(negative_keywords)
    if not configured:
        return []
    matches: list[str] = []
    seen: set[str] = set()
    for context in (title, *tuple(matched_field_values)):
        for term in match_negative_keywords_in_text(context, configured):
            if term not in seen:
                seen.add(term)
                matches.append(term)
    return matches
