from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Iterable

DEFAULT_PANAMACOMPRA_KEYWORDS = ("chiller", "york", "daikin")


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
    return _normalize_search_text(value)


@lru_cache(maxsize=512)
def _keyword_pattern(normalized_term: str) -> re.Pattern[str] | None:
    normalized_term = normalize_keyword_term(normalized_term)
    if not normalized_term:
        return None
    tokens = [re.escape(token) for token in normalized_term.split() if token]
    if not tokens:
        return None
    pattern = rf"(?<![0-9a-z]){r'\s+'.join(tokens)}(?![0-9a-z])"
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
