"""Deteccion defensiva de fichas vigiladas dentro de tablas PanamaCompra.

La capa principal sigue siendo la clasificacion de los scrapers. Esta ayuda
permite recuperar en la interfaz filas historicas etiquetadas como
``No Detectada`` cuando el nombre especifico de una ficha vigilada si aparece
en titulo, descripcion o items.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping


_STOPWORDS = {
    "a", "al", "con", "de", "del", "el", "en", "la", "las", "lo",
    "los", "o", "para", "por", "su", "sus", "un", "una", "y",
}


def normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = "".join(
        char
        for char in unicodedata.normalize("NFD", text.lower())
        if unicodedata.category(char) != "Mn"
    )
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()


def _variants(token: str) -> frozenset[str]:
    output = {token} if token else set()
    if len(token) >= 5 and token.endswith("s"):
        output.add(token[:-1])
    if len(token) >= 6 and token.endswith("es"):
        output.add(token[:-2])
    if len(token) >= 6 and token.endswith("ces"):
        output.add(token[:-3] + "z")
    return frozenset(item for item in output if len(item) >= 3)


def _equivalent(left: str, right: str) -> bool:
    return left == right or bool(_variants(left).intersection(_variants(right)))


def _contains_specific_name(text: str, name: str) -> bool:
    text_tokens = [token for token in normalize_text(text).split() if token not in _STOPWORDS]
    name_tokens = [token for token in normalize_text(name).split() if token not in _STOPWORDS]
    if len(name_tokens) < 4 or sum(map(len, name_tokens)) < 18:
        return False
    width = len(name_tokens)
    for start in range(0, len(text_tokens) - width + 1):
        window = text_tokens[start : start + width]
        if all(_equivalent(actual, expected) for actual, expected in zip(window, name_tokens)):
            return True
    return False


def detect_watched_fichas(
    fields: Mapping[str, object],
    watched_names: Mapping[str, str],
) -> tuple[str, ...]:
    """Detecta nombres largos vigilados, admitiendo singular/plural editorial."""
    combined = " ".join(str(value or "") for value in fields.values())
    matches = [
        str(code)
        for code, name in watched_names.items()
        if code and name and _contains_specific_name(combined, name)
    ]
    return tuple(sorted(set(matches), key=lambda value: int(value)))

