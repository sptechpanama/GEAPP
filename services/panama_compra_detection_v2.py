from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
import math
from pathlib import Path
import re
import unicodedata
from typing import Iterable, Sequence

import pandas as pd


LOCAL_MASTER_PATH = Path(r"C:\Users\rodri\fichas\fichas-y-nombre.xlsx")

GENERIC_STOPWORDS = {
    "a",
    "al",
    "ante",
    "bajo",
    "base",
    "con",
    "contra",
    "de",
    "del",
    "desde",
    "e",
    "el",
    "en",
    "entre",
    "la",
    "las",
    "los",
    "o",
    "para",
    "por",
    "segun",
    "sin",
    "sobre",
    "su",
    "sus",
    "u",
    "un",
    "una",
    "uno",
    "y",
}

LOW_SIGNAL_TOKENS = {
    "accesorio",
    "adquisicion",
    "aparato",
    "articulo",
    "componente",
    "compra",
    "consumible",
    "dispositivo",
    "equipo",
    "herramienta",
    "implemento",
    "insumo",
    "instrumento",
    "juego",
    "kit",
    "material",
    "materiales",
    "modulo",
    "paquete",
    "parte",
    "pieza",
    "piezas",
    "producto",
    "productos",
    "reactivo",
    "reactivos",
    "repuesto",
    "repuestos",
    "servicio",
    "sistema",
    "suministro",
    "unidad",
    "uso",
}

NEGATIVE_NUMERIC_CONTEXT = {
    "amp",
    "cm",
    "fr",
    "g",
    "gauge",
    "gr",
    "grs",
    "hp",
    "hz",
    "item",
    "kg",
    "kw",
    "lote",
    "marca",
    "medida",
    "mg",
    "ml",
    "modelo",
    "parte",
    "pieza",
    "psi",
    "ref",
    "referencia",
    "repuesto",
    "serie",
    "size",
    "volt",
    "v",
    "w",
}

POSITIVE_NUMERIC_CONTEXT = {
    "criterio",
    "ctni",
    "ficha",
    "idficha",
    "minsa",
    "tecnica",
    "tecnico",
}


@dataclass(frozen=True)
class AliasEntry:
    text: str
    tokens: tuple[str, ...]
    weight_sum: float


@dataclass(frozen=True)
class CatalogEntry:
    code: str
    name: str
    aliases: tuple[AliasEntry, ...]
    tiene_ct: str
    tiene_rs: str
    enlace_minsa: str
    clase: str


@dataclass(frozen=True)
class DetectionCandidate:
    code: str
    score: float
    evidence: tuple[str, ...]
    meta: CatalogEntry


@dataclass(frozen=True)
class DetectionResult:
    detected_code: str
    possible_code: str
    score: float
    confidence: str
    evidence: str
    reason: str
    abstained: bool
    meta: CatalogEntry | None


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_text(value: object) -> str:
    text = _clean_text(value).lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_code(value: object) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return ""
    normalized = digits.lstrip("0")
    return normalized or "0"


def _normalize_link(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    url_match = re.search(r"https?://[^\s\"'<>]+", text, flags=re.IGNORECASE)
    if url_match:
        return url_match.group(0).rstrip(".,);")
    path_match = re.search(
        r"/Utilities/LoadFicha/\?idficha=\d+[^\s\"'<>]*",
        text,
        flags=re.IGNORECASE,
    )
    if path_match:
        return f"https://ctni.minsa.gob.pa{path_match.group(0)}"
    id_match = re.search(r"idficha\s*=\s*(\d+)", text, flags=re.IGNORECASE)
    if id_match:
        return f"https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha={id_match.group(1)}&idparam=0"
    return ""


def _coerce_yes_no(value: object) -> str:
    norm = _normalize_text(value)
    if norm in {"si", "s", "true", "1", "x", "ct", "con ct", "con registro sanitario"}:
        return "Si"
    if norm.startswith("si "):
        return "Si"
    return "No"


def _tokenize_significant(value: object) -> list[str]:
    norm = _normalize_text(value)
    if not norm:
        return []
    out: list[str] = []
    for token in norm.split():
        if token in GENERIC_STOPWORDS or token in LOW_SIGNAL_TOKENS:
            continue
        if len(token) <= 2:
            continue
        if token.isdigit():
            continue
        out.append(token)
    return out


def _resolve_column(df: pd.DataFrame, aliases: Sequence[str]) -> str:
    normalized = {}
    for col in df.columns:
        key = _normalize_text(col)
        if key:
            normalized[key] = col
    for alias in aliases:
        key = _normalize_text(alias)
        if key in normalized:
            return normalized[key]
    for alias in aliases:
        key = _normalize_text(alias)
        for norm_col, original in normalized.items():
            if key and key in norm_col:
                return original
    return ""


def _load_local_master_df() -> pd.DataFrame:
    if not LOCAL_MASTER_PATH.exists():
        return pd.DataFrame(columns=["ficha", "nombre"])
    try:
        df = pd.read_excel(LOCAL_MASTER_PATH, header=None)
    except Exception:
        return pd.DataFrame(columns=["ficha", "nombre"])
    if df.empty:
        return pd.DataFrame(columns=["ficha", "nombre"])
    width = max(2, min(len(df.columns), 8))
    df = df.iloc[:, :width].copy()
    rename_map = {df.columns[0]: "ficha", df.columns[1]: "nombre"}
    df = df.rename(columns=rename_map)
    return df


def _metadata_records_from_df(df: pd.DataFrame | None) -> tuple[tuple[str, str, str, str, str, str], ...]:
    if df is None or df.empty:
        return tuple()

    ficha_col = _resolve_column(df, ["ficha", "numero ficha", "ficha_tecnica", "codigo ficha", "id ficha"])
    if not ficha_col:
        return tuple()
    nombre_col = _resolve_column(df, ["nombre generico", "nombre ficha", "nombre"])
    ct_col = _resolve_column(df, ["tiene ct", "ct", "criterio tecnico", "criterio"])
    rs_col = _resolve_column(df, ["registro sanitario", "reg sanitario", "registro_sanitario"])
    link_col = _resolve_column(
        df,
        [
            "enlace_ficha_tecnica",
            "enlace ficha tecnica",
            "enlace ficha minsa",
            "enlace minsa",
            "link minsa",
            "url minsa",
            "enlace",
            "url",
            "link",
        ],
    )
    clase_col = _resolve_column(df, ["clase", "categoria", "clasificacion", "tipo"])

    records: list[tuple[str, str, str, str, str, str]] = []
    for _, row in df.iterrows():
        code = _normalize_code(row.get(ficha_col))
        if not code:
            continue
        records.append(
            (
                code,
                _clean_text(row.get(nombre_col)) if nombre_col else "",
                _clean_text(row.get(ct_col)) if ct_col else "",
                _clean_text(row.get(rs_col)) if rs_col else "",
                _clean_text(row.get(link_col)) if link_col else "",
                _clean_text(row.get(clase_col)) if clase_col else "",
            )
        )
    return tuple(records)


def _build_catalog_records(metadata_records: tuple[tuple[str, str, str, str, str, str], ...]) -> list[dict[str, object]]:
    local_df = _load_local_master_df()
    records_by_code: dict[str, dict[str, object]] = {}

    for _, row in local_df.iterrows():
        code = _normalize_code(row.get("ficha"))
        if not code:
            continue
        name = _clean_text(row.get("nombre"))
        entry = records_by_code.setdefault(
            code,
            {"code": code, "names": set(), "ct": "No", "rs": "No", "link": "", "clase": ""},
        )
        if name:
            entry["names"].add(name)

    for code, name, ct, rs, link, clase in metadata_records:
        entry = records_by_code.setdefault(
            code,
            {"code": code, "names": set(), "ct": "No", "rs": "No", "link": "", "clase": ""},
        )
        if name:
            entry["names"].add(name)
        if _coerce_yes_no(ct) == "Si":
            entry["ct"] = "Si"
        if _coerce_yes_no(rs) == "Si":
            entry["rs"] = "Si"
        norm_link = _normalize_link(link)
        if norm_link and not entry["link"]:
            entry["link"] = norm_link
        if clase and not entry["clase"]:
            entry["clase"] = clase

    return list(records_by_code.values())


@lru_cache(maxsize=8)
def _build_catalog(metadata_records: tuple[tuple[str, str, str, str, str, str], ...]) -> tuple[
    dict[str, CatalogEntry],
    dict[str, set[str]],
    dict[str, float],
]:
    raw_records = _build_catalog_records(metadata_records)
    token_document_freq: Counter[str] = Counter()
    aliases_by_code: dict[str, list[tuple[str, tuple[str, ...]]]] = defaultdict(list)

    for raw in raw_records:
        code = str(raw["code"])
        names = sorted({name for name in raw["names"] if _clean_text(name)})
        for raw_name in names:
            norm_name = _normalize_text(raw_name)
            if not norm_name:
                continue
            sig_tokens = tuple(dict.fromkeys(_tokenize_significant(norm_name)))
            aliases_by_code[code].append((norm_name, sig_tokens))
            token_document_freq.update(set(sig_tokens))

    token_weight: dict[str, float] = {}
    for token, freq in token_document_freq.items():
        # Favorece tokens raros pero evita pesos extremos.
        token_weight[token] = max(1.0, min(6.0, 6.5 - math.log2(freq + 1)))

    token_index: dict[str, set[str]] = defaultdict(set)
    catalog: dict[str, CatalogEntry] = {}

    for raw in raw_records:
        code = str(raw["code"])
        alias_entries: list[AliasEntry] = []
        for norm_name, sig_tokens in aliases_by_code.get(code, []):
            if not sig_tokens:
                continue
            weight_sum = sum(token_weight.get(token, 1.0) for token in sig_tokens)
            alias_entries.append(AliasEntry(text=norm_name, tokens=sig_tokens, weight_sum=weight_sum))
            for token in set(sig_tokens):
                token_index[token].add(code)

        if not alias_entries:
            continue
        alias_entries = sorted(alias_entries, key=lambda item: (-len(item.tokens), item.text))
        catalog[code] = CatalogEntry(
            code=code,
            name=next((name for name in sorted(raw["names"]) if _clean_text(name)), f"Ficha {code}"),
            aliases=tuple(alias_entries),
            tiene_ct=str(raw.get("ct", "No") or "No"),
            tiene_rs=str(raw.get("rs", "No") or "No"),
            enlace_minsa=str(raw.get("link", "") or ""),
            clase=str(raw.get("clase", "") or ""),
        )

    return catalog, token_index, token_weight


def _context_score_for_numeric(raw_text: str, code: str) -> tuple[float, list[str]]:
    evidence: list[str] = []
    score = 0.0
    pattern = re.compile(rf"(?<!\d){re.escape(code)}(?!\d)")
    for match in pattern.finditer(raw_text):
        left = raw_text[max(0, match.start() - 36) : match.start()].lower()
        right = raw_text[match.end() : min(len(raw_text), match.end() + 36)].lower()
        ctx = f"{left} {right}"
        has_positive = any(re.search(rf"\b{re.escape(token)}\b", ctx) for token in POSITIVE_NUMERIC_CONTEXT)
        has_negative = any(re.search(rf"\b{re.escape(token)}\b", ctx) for token in NEGATIVE_NUMERIC_CONTEXT)
        if has_positive and not has_negative:
            score = max(score, 125.0)
            evidence.append(f"codigo_contextual:{code}")
        elif not has_negative:
            score = max(score, 58.0)
            evidence.append(f"codigo_aislado:{code}")
        else:
            score = max(score, 18.0)
            evidence.append(f"codigo_debil:{code}")
    return score, evidence


def _ordered_window_match(alias_tokens: Sequence[str], field_sequence: Sequence[str]) -> bool:
    if not alias_tokens or not field_sequence:
        return False
    positions: list[int] = []
    target_idx = 0
    for pos, token in enumerate(field_sequence):
        if target_idx >= len(alias_tokens):
            break
        if token == alias_tokens[target_idx]:
            positions.append(pos)
            target_idx += 1
    if target_idx != len(alias_tokens):
        return False
    return (positions[-1] - positions[0]) <= max(len(alias_tokens) + 5, 8)


def _score_alias_in_field(
    alias: AliasEntry,
    field_text: str,
    field_tokens: set[str],
    field_sequence: list[str],
    field_name: str,
    token_weight: dict[str, float],
) -> tuple[float, list[str]]:
    if not alias.tokens:
        return 0.0, []

    weight_total = alias.weight_sum or 1.0
    matched_tokens = [token for token in alias.tokens if token in field_tokens]
    matched_weight = sum(token_weight.get(token, 1.0) for token in matched_tokens)
    coverage = matched_weight / weight_total if weight_total else 0.0
    rare_matches = sum(1 for token in matched_tokens if token_weight.get(token, 1.0) >= 3.0)

    field_bias = {
        "titulo": (96.0, 84.0, 60.0),
        "items": (92.0, 80.0, 55.0),
        "descripcion": (80.0, 68.0, 44.0),
    }.get(field_name, (74.0, 60.0, 38.0))

    evidence: list[str] = []
    score = 0.0
    if alias.text and alias.text in field_text:
        score = field_bias[0] + min(12.0, rare_matches * 3.5)
        evidence.append(f"frase_exacta:{field_name}")
    elif len(matched_tokens) >= max(2, math.ceil(len(alias.tokens) * 0.6)):
        if coverage >= 0.9 and _ordered_window_match(alias.tokens, field_sequence):
            score = field_bias[1] + min(10.0, rare_matches * 2.5)
            evidence.append(f"subsecuencia_fuerte:{field_name}")
        elif coverage >= 0.78 and rare_matches >= 1:
            score = field_bias[2] + (coverage * 12.0)
            evidence.append(f"cobertura_alta:{field_name}")
        elif coverage >= 0.65 and rare_matches >= 2 and len(alias.tokens) >= 3:
            score = (field_bias[2] - 8.0) + (coverage * 10.0)
            evidence.append(f"cobertura_media:{field_name}")
    return score, evidence


def _detect_single_record(
    *,
    title: object,
    description: object,
    items: Iterable[object],
    metadata_records: tuple[tuple[str, str, str, str, str, str], ...],
) -> DetectionResult:
    catalog, token_index, token_weight = _build_catalog(metadata_records)
    if not catalog:
        return DetectionResult(
            detected_code="No Detectada",
            possible_code="",
            score=0.0,
            confidence="Nula",
            evidence="Catalogo vacio",
            reason="catalogo_vacio",
            abstained=True,
            meta=None,
        )

    title_raw = _clean_text(title)
    description_raw = _clean_text(description)
    items_raw = " ".join(_clean_text(item) for item in items if _clean_text(item))
    raw_text = " || ".join(part for part in (title_raw, description_raw, items_raw) if part)

    field_texts = {
        "titulo": _normalize_text(title_raw),
        "descripcion": _normalize_text(description_raw),
        "items": _normalize_text(items_raw),
    }
    field_sequences = {name: _tokenize_significant(text) for name, text in field_texts.items()}
    field_token_sets = {name: set(tokens) for name, tokens in field_sequences.items()}

    candidate_scores: defaultdict[str, float] = defaultdict(float)
    candidate_evidence: defaultdict[str, list[str]] = defaultdict(list)

    # 1) Numericos explicitos, pero con contexto.
    for match in re.finditer(r"(?<!\d)(\d{3,6})(?!\d)", raw_text):
        code = _normalize_code(match.group(1))
        if code not in catalog:
            continue
        numeric_score, numeric_evidence = _context_score_for_numeric(raw_text, code)
        if numeric_score > candidate_scores[code]:
            candidate_scores[code] = max(candidate_scores[code], numeric_score)
        candidate_evidence[code].extend(numeric_evidence)

    # 2) Candidatos por tokens raros / significativos.
    candidates_from_tokens: set[str] = set()
    for field_name, field_tokens in field_token_sets.items():
        if not field_tokens:
            continue
        for token in field_tokens:
            if token not in token_index:
                continue
            # Evita explosiones por tokens demasiado genericos.
            if len(token_index[token]) > 220:
                continue
            candidates_from_tokens.update(token_index[token])

    for code in candidates_from_tokens:
        entry = catalog.get(code)
        if entry is None:
            continue
        best_alias_score = 0.0
        best_alias_evidence: list[str] = []
        for alias in entry.aliases:
            alias_best_for_candidate = 0.0
            alias_evidence_for_candidate: list[str] = []
            for field_name in ("titulo", "items", "descripcion"):
                field_score, field_evidence = _score_alias_in_field(
                    alias,
                    field_text=field_texts[field_name],
                    field_tokens=field_token_sets[field_name],
                    field_sequence=field_sequences[field_name],
                    field_name=field_name,
                    token_weight=token_weight,
                )
                if field_score > alias_best_for_candidate:
                    alias_best_for_candidate = field_score
                    alias_evidence_for_candidate = field_evidence
            if alias_best_for_candidate > best_alias_score:
                best_alias_score = alias_best_for_candidate
                best_alias_evidence = alias_evidence_for_candidate
        if best_alias_score:
            candidate_scores[code] += best_alias_score
            candidate_evidence[code].extend(best_alias_evidence)

    if not candidate_scores:
        return DetectionResult(
            detected_code="No Detectada",
            possible_code="",
            score=0.0,
            confidence="Nula",
            evidence="Sin evidencia suficiente",
            reason="sin_evidencia",
            abstained=True,
            meta=None,
        )

    ranked: list[DetectionCandidate] = []
    for code, score in candidate_scores.items():
        meta = catalog.get(code)
        if meta is None:
            continue
        evidence = tuple(dict.fromkeys(candidate_evidence.get(code, [])))
        ranked.append(DetectionCandidate(code=code, score=score, evidence=evidence, meta=meta))
    ranked.sort(key=lambda item: (-item.score, item.code))
    if not ranked:
        return DetectionResult(
            detected_code="No Detectada",
            possible_code="",
            score=0.0,
            confidence="Nula",
            evidence="Sin candidatos",
            reason="sin_candidatos",
            abstained=True,
            meta=None,
        )

    top = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None
    margin = top.score - (second.score if second else 0.0)
    hard_numeric = any(ev.startswith("codigo_contextual") for ev in top.evidence)
    exact_phrase = any(ev.startswith("frase_exacta") for ev in top.evidence)
    strong_sequence = any(ev.startswith("subsecuencia_fuerte") for ev in top.evidence)

    detected = False
    confidence = "Baja"
    reason = "abstencion"
    if hard_numeric and top.score >= 110 and margin >= 6:
        detected = True
        confidence = "Alta"
        reason = "codigo_contextual"
    elif exact_phrase and top.score >= 92 and margin >= 12:
        detected = True
        confidence = "Alta"
        reason = "frase_exacta"
    elif strong_sequence and top.score >= 84 and margin >= 14:
        detected = True
        confidence = "Media-Alta"
        reason = "subsecuencia_fuerte"
    elif top.score >= 120 and margin >= 4:
        detected = True
        confidence = "Alta"
        reason = "score_muy_alto"
    elif top.score >= 98 and margin >= 18:
        detected = True
        confidence = "Media-Alta"
        reason = "score_alto"

    if detected:
        return DetectionResult(
            detected_code=top.code,
            possible_code=second.code if second and second.score >= 60 else "",
            score=top.score,
            confidence=confidence,
            evidence="; ".join(top.evidence[:4]) or "deteccion_confirmada",
            reason=reason,
            abstained=False,
            meta=top.meta,
        )

    possible_codes = [cand.code for cand in ranked[:2] if cand.score >= 55]
    return DetectionResult(
        detected_code="No Detectada",
        possible_code=", ".join(possible_codes),
        score=top.score,
        confidence="Baja" if top.score >= 55 else "Nula",
        evidence="; ".join(top.evidence[:4]) or "evidencia_insuficiente",
        reason="abstencion",
        abstained=True,
        meta=top.meta if possible_codes else None,
    )


def apply_detection_v2_to_dataframe(
    df: pd.DataFrame,
    *,
    metadata_df: pd.DataFrame | None = None,
    title_aliases: Sequence[str] = ("titulo", "título"),
    description_aliases: Sequence[str] = ("descripcion", "descripción"),
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    title_col = _resolve_column(df, title_aliases)
    desc_col = _resolve_column(df, description_aliases)
    item_cols = [
        col
        for col in df.columns
        if _normalize_text(col).startswith("item ")
        or _normalize_text(col).startswith("item_")
        or re.fullmatch(r"item\s*\d+", _normalize_text(col))
    ]
    if not title_col and not desc_col and not item_cols:
        return df

    metadata_records = _metadata_records_from_df(metadata_df)
    out = df.copy()
    original_col = _resolve_column(out, ["ficha_detectada", "ficha detectada", "ficha", "numero ficha"])
    if original_col and "ficha_detectada_original" not in out.columns:
        out["ficha_detectada_original"] = out[original_col].fillna("").astype(str)

    detected_values: list[str] = []
    possible_values: list[str] = []
    score_values: list[float] = []
    confidence_values: list[str] = []
    evidence_values: list[str] = []
    reason_values: list[str] = []
    ct_values: list[str] = []
    rs_values: list[str] = []
    link_values: list[str] = []
    clase_values: list[str] = []

    for _, row in out.iterrows():
        result = _detect_single_record(
            title=row.get(title_col, "") if title_col else "",
            description=row.get(desc_col, "") if desc_col else "",
            items=[row.get(col, "") for col in item_cols],
            metadata_records=metadata_records,
        )
        detected_values.append(result.detected_code)
        possible_values.append(result.possible_code)
        score_values.append(round(float(result.score or 0.0), 2))
        confidence_values.append(result.confidence)
        evidence_values.append(result.evidence)
        reason_values.append(result.reason)
        meta = result.meta
        ct_values.append(meta.tiene_ct if meta else "No")
        rs_values.append(meta.tiene_rs if meta else "No")
        link_values.append(meta.enlace_minsa if meta else "")
        clase_values.append(meta.clase if meta else "")

    target_col = original_col or "ficha_detectada"
    out[target_col] = detected_values
    out["posible_ficha"] = possible_values
    out["score_ficha_v2"] = score_values
    out["confianza_ficha_v2"] = confidence_values
    out["evidencia_ficha_v2"] = evidence_values
    out["motivo_ficha_v2"] = reason_values
    out["tiene_ct_v2"] = ct_values
    out["registro_sanitario_v2"] = rs_values
    out["enlace_ficha_minsa_v2"] = link_values
    out["clase_ficha_v2"] = clase_values
    return out


@dataclass(frozen=True)
class FlexibleDetectionProfile:
    slug: str
    label: str
    possible_score_min: float
    numeric_score_min: float
    numeric_margin_min: float
    exact_score_min: float
    exact_margin_min: float
    sequence_score_min: float
    sequence_margin_min: float
    fuzzy_high_score_min: float
    fuzzy_high_margin_min: float
    fuzzy_medium_score_min: float
    fuzzy_medium_margin_min: float
    coverage_high_score_min: float
    coverage_high_margin_min: float
    coverage_medium_score_min: float
    coverage_medium_margin_min: float
    absolute_score_min: float
    absolute_margin_min: float


FLEXIBLE_DETECTION_PROFILES: dict[str, FlexibleDetectionProfile] = {
    "estricto": FlexibleDetectionProfile(
        slug="estricto",
        label="Estricto",
        possible_score_min=55.0,
        numeric_score_min=110.0,
        numeric_margin_min=6.0,
        exact_score_min=92.0,
        exact_margin_min=12.0,
        sequence_score_min=84.0,
        sequence_margin_min=14.0,
        fuzzy_high_score_min=999.0,
        fuzzy_high_margin_min=999.0,
        fuzzy_medium_score_min=999.0,
        fuzzy_medium_margin_min=999.0,
        coverage_high_score_min=999.0,
        coverage_high_margin_min=999.0,
        coverage_medium_score_min=999.0,
        coverage_medium_margin_min=999.0,
        absolute_score_min=98.0,
        absolute_margin_min=18.0,
    ),
    "moderado": FlexibleDetectionProfile(
        slug="moderado",
        label="Moderado",
        possible_score_min=48.0,
        numeric_score_min=94.0,
        numeric_margin_min=4.0,
        exact_score_min=84.0,
        exact_margin_min=8.0,
        sequence_score_min=76.0,
        sequence_margin_min=8.0,
        fuzzy_high_score_min=82.0,
        fuzzy_high_margin_min=6.0,
        fuzzy_medium_score_min=74.0,
        fuzzy_medium_margin_min=6.0,
        coverage_high_score_min=74.0,
        coverage_high_margin_min=7.0,
        coverage_medium_score_min=999.0,
        coverage_medium_margin_min=999.0,
        absolute_score_min=82.0,
        absolute_margin_min=10.0,
    ),
    "muy_flexible": FlexibleDetectionProfile(
        slug="muy_flexible",
        label="Muy Flexible",
        possible_score_min=40.0,
        numeric_score_min=78.0,
        numeric_margin_min=2.0,
        exact_score_min=74.0,
        exact_margin_min=4.0,
        sequence_score_min=68.0,
        sequence_margin_min=4.0,
        fuzzy_high_score_min=72.0,
        fuzzy_high_margin_min=3.0,
        fuzzy_medium_score_min=66.0,
        fuzzy_medium_margin_min=2.0,
        coverage_high_score_min=64.0,
        coverage_high_margin_min=2.0,
        coverage_medium_score_min=58.0,
        coverage_medium_margin_min=0.0,
        absolute_score_min=70.0,
        absolute_margin_min=6.0,
    ),
}


def list_detection_profiles() -> tuple[str, ...]:
    return tuple(FLEXIBLE_DETECTION_PROFILES.keys())


def get_detection_profile_labels() -> dict[str, str]:
    return {slug: profile.label for slug, profile in FLEXIBLE_DETECTION_PROFILES.items()}


def normalize_detection_profile_key(value: object) -> str:
    key = _normalize_text(value).replace(" ", "_")
    mapping = {
        "strict": "estricto",
        "estricta": "estricto",
        "estricto": "estricto",
        "moderado": "moderado",
        "moderate": "moderado",
        "medium": "moderado",
        "flexible": "muy_flexible",
        "muy_flexible": "muy_flexible",
        "muy__flexible": "muy_flexible",
        "muyflexible": "muy_flexible",
        "very_flexible": "muy_flexible",
        "very__flexible": "muy_flexible",
    }
    return mapping.get(key, "estricto")


def flexible_output_col(base_name: str, profile_key: str) -> str:
    return f"{base_name}_{normalize_detection_profile_key(profile_key)}"


def _relax_token(token: str) -> str:
    value = _normalize_text(token)
    if not value:
        return ""
    if len(value) > 6 and value.endswith("es"):
        value = value[:-2]
    elif len(value) > 5 and value.endswith("s") and not value.endswith(("us", "is")):
        value = value[:-1]
    return value


def _ordered_window_match_relaxed(alias_tokens: Sequence[str], field_sequence: Sequence[str]) -> bool:
    alias_relaxed = [_relax_token(token) for token in alias_tokens if _relax_token(token)]
    field_relaxed = [_relax_token(token) for token in field_sequence if _relax_token(token)]
    if not alias_relaxed or not field_relaxed:
        return False
    positions: list[int] = []
    target_idx = 0
    for pos, token in enumerate(field_relaxed):
        if target_idx >= len(alias_relaxed):
            break
        if token == alias_relaxed[target_idx]:
            positions.append(pos)
            target_idx += 1
    if target_idx != len(alias_relaxed):
        return False
    return (positions[-1] - positions[0]) <= max(len(alias_relaxed) + 6, 10)


def _extract_candidate_window(
    alias_tokens: Sequence[str],
    field_sequence: Sequence[str],
) -> str:
    alias_exact = {token for token in alias_tokens if token}
    alias_relaxed = {_relax_token(token) for token in alias_tokens if _relax_token(token)}
    positions: list[int] = []
    for pos, token in enumerate(field_sequence):
        relaxed = _relax_token(token)
        if token in alias_exact or relaxed in alias_relaxed:
            positions.append(pos)
    if not positions:
        return ""
    start = max(0, min(positions) - 2)
    end = min(len(field_sequence), max(positions) + 3)
    return " ".join(field_sequence[start:end]).strip()


def _score_alias_in_field_flexible(
    alias: AliasEntry,
    field_text: str,
    field_tokens: set[str],
    field_sequence: list[str],
    field_name: str,
    token_weight: dict[str, float],
) -> tuple[float, list[str]]:
    if not alias.tokens:
        return 0.0, []

    field_relaxed_tokens = {_relax_token(token) for token in field_tokens if _relax_token(token)}
    matched_tokens: list[str] = []
    matched_relaxed: set[str] = set()
    for token in alias.tokens:
        relaxed_token = _relax_token(token)
        if token in field_tokens or relaxed_token in field_relaxed_tokens:
            matched_tokens.append(token)
            matched_relaxed.add(relaxed_token)

    weight_total = alias.weight_sum or 1.0
    matched_weight = sum(token_weight.get(token, 1.0) for token in matched_tokens)
    coverage = matched_weight / weight_total if weight_total else 0.0
    rare_matches = sum(1 for token in matched_tokens if token_weight.get(token, 1.0) >= 3.0)
    evidence: list[str] = []
    score = 0.0

    field_bias = {
        "titulo": (96.0, 84.0, 60.0),
        "items": (92.0, 80.0, 55.0),
        "descripcion": (80.0, 68.0, 44.0),
    }.get(field_name, (74.0, 60.0, 38.0))

    if alias.text and alias.text in field_text:
        score = field_bias[0] + min(12.0, rare_matches * 3.5)
        evidence.append(f"frase_exacta:{field_name}")
    elif len(matched_tokens) >= max(2, math.ceil(len(alias.tokens) * 0.6)):
        if coverage >= 0.9 and _ordered_window_match_relaxed(alias.tokens, field_sequence):
            score = field_bias[1] + min(10.0, rare_matches * 2.5)
            evidence.append(f"subsecuencia_fuerte:{field_name}")
        elif coverage >= 0.78 and rare_matches >= 1:
            score = field_bias[2] + (coverage * 12.0)
            evidence.append(f"cobertura_alta:{field_name}")
        elif coverage >= 0.60 and len(matched_relaxed) >= 2 and len(alias.tokens) >= 2:
            score = (field_bias[2] - 10.0) + (coverage * 12.0)
            evidence.append(f"cobertura_media:{field_name}")

    if len(matched_tokens) >= 2 and coverage >= 0.45:
        window_text = _extract_candidate_window(alias.tokens, field_sequence)
        if window_text:
            ratio = SequenceMatcher(None, alias.text, window_text).ratio()
            if ratio >= 0.92:
                fuzzy_score = field_bias[1] + 8.0 + min(8.0, rare_matches * 2.0)
                if fuzzy_score > score:
                    score = fuzzy_score
                    evidence = [f"fuzzy_alta:{field_name}"]
            elif ratio >= 0.84 and coverage >= 0.50:
                fuzzy_score = field_bias[2] + 2.0 + (ratio * 10.0)
                if fuzzy_score > score:
                    score = fuzzy_score
                    evidence = [f"fuzzy_media:{field_name}"]

    return score, evidence


@lru_cache(maxsize=8)
def _build_flexible_catalog(metadata_records: tuple[tuple[str, str, str, str, str, str], ...]) -> tuple[
    dict[str, CatalogEntry],
    dict[str, set[str]],
    dict[str, set[str]],
    dict[str, float],
]:
    catalog, token_index, token_weight = _build_catalog(metadata_records)
    relaxed_index: dict[str, set[str]] = defaultdict(set)
    for code, entry in catalog.items():
        for alias in entry.aliases:
            for token in alias.tokens:
                relaxed = _relax_token(token)
                if relaxed:
                    relaxed_index[relaxed].add(code)
    return catalog, token_index, relaxed_index, token_weight


def _rank_candidates_flexible(
    *,
    title: object,
    description: object,
    items: Iterable[object],
    metadata_records: tuple[tuple[str, str, str, str, str, str], ...],
) -> list[DetectionCandidate]:
    catalog, token_index, relaxed_index, token_weight = _build_flexible_catalog(metadata_records)
    if not catalog:
        return []

    title_raw = _clean_text(title)
    description_raw = _clean_text(description)
    items_raw = " ".join(_clean_text(item) for item in items if _clean_text(item))
    raw_text = " || ".join(part for part in (title_raw, description_raw, items_raw) if part)

    field_texts = {
        "titulo": _normalize_text(title_raw),
        "descripcion": _normalize_text(description_raw),
        "items": _normalize_text(items_raw),
    }
    field_sequences = {name: _tokenize_significant(text) for name, text in field_texts.items()}
    field_token_sets = {name: set(tokens) for name, tokens in field_sequences.items()}
    field_relaxed_sets = {
        name: {_relax_token(token) for token in tokens if _relax_token(token)}
        for name, tokens in field_token_sets.items()
    }

    candidate_scores: defaultdict[str, float] = defaultdict(float)
    candidate_evidence: defaultdict[str, list[str]] = defaultdict(list)

    for match in re.finditer(r"(?<!\d)(\d{3,6})(?!\d)", raw_text):
        code = _normalize_code(match.group(1))
        if code not in catalog:
            continue
        numeric_score, numeric_evidence = _context_score_for_numeric(raw_text, code)
        if numeric_score > candidate_scores[code]:
            candidate_scores[code] = max(candidate_scores[code], numeric_score)
        candidate_evidence[code].extend(numeric_evidence)

    candidates_from_tokens: set[str] = set()
    for field_name in ("titulo", "items", "descripcion"):
        for token in field_token_sets.get(field_name, set()):
            codes = token_index.get(token)
            if not codes or len(codes) > 220:
                continue
            candidates_from_tokens.update(codes)
        for token in field_relaxed_sets.get(field_name, set()):
            codes = relaxed_index.get(token)
            if not codes or len(codes) > 260:
                continue
            candidates_from_tokens.update(codes)

    for code in candidates_from_tokens:
        entry = catalog.get(code)
        if entry is None:
            continue
        best_alias_score = 0.0
        best_alias_evidence: list[str] = []
        for alias in entry.aliases:
            alias_best_score = 0.0
            alias_best_evidence: list[str] = []
            for field_name in ("titulo", "items", "descripcion"):
                field_score, field_evidence = _score_alias_in_field_flexible(
                    alias,
                    field_text=field_texts[field_name],
                    field_tokens=field_token_sets[field_name],
                    field_sequence=field_sequences[field_name],
                    field_name=field_name,
                    token_weight=token_weight,
                )
                if field_score > alias_best_score:
                    alias_best_score = field_score
                    alias_best_evidence = field_evidence
            if alias_best_score > best_alias_score:
                best_alias_score = alias_best_score
                best_alias_evidence = alias_best_evidence
        if best_alias_score:
            candidate_scores[code] += best_alias_score
            candidate_evidence[code].extend(best_alias_evidence)

    ranked: list[DetectionCandidate] = []
    for code, score in candidate_scores.items():
        meta = catalog.get(code)
        if meta is None:
            continue
        evidence = tuple(dict.fromkeys(candidate_evidence.get(code, [])))
        ranked.append(DetectionCandidate(code=code, score=score, evidence=evidence, meta=meta))

    ranked.sort(key=lambda item: (-item.score, item.code))
    return ranked


def _resolve_flexible_detection(
    ranked: Sequence[DetectionCandidate],
    profile_key: str,
) -> DetectionResult:
    profile = FLEXIBLE_DETECTION_PROFILES[normalize_detection_profile_key(profile_key)]
    if not ranked:
        return DetectionResult(
            detected_code="No Detectada",
            possible_code="",
            score=0.0,
            confidence="Nula",
            evidence="Sin evidencia suficiente",
            reason="sin_evidencia",
            abstained=True,
            meta=None,
        )

    top = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None
    margin = float(top.score) - float(second.score if second else 0.0)
    evidence_set = set(top.evidence)
    hard_numeric = any(ev.startswith("codigo_contextual") for ev in evidence_set)
    exact_phrase = any(ev.startswith("frase_exacta") for ev in evidence_set)
    strong_sequence = any(ev.startswith("subsecuencia_fuerte") for ev in evidence_set)
    high_coverage = any(ev.startswith("cobertura_alta") for ev in evidence_set)
    medium_coverage = any(ev.startswith("cobertura_media") for ev in evidence_set)
    fuzzy_high = any(ev.startswith("fuzzy_alta") for ev in evidence_set)
    fuzzy_medium = any(ev.startswith("fuzzy_media") for ev in evidence_set)

    detected = False
    confidence = "Baja"
    reason = "abstencion"

    if hard_numeric and top.score >= profile.numeric_score_min and margin >= profile.numeric_margin_min:
        detected = True
        confidence = "Alta"
        reason = "codigo_contextual"
    elif exact_phrase and top.score >= profile.exact_score_min and margin >= profile.exact_margin_min:
        detected = True
        confidence = "Alta" if profile.slug == "estricto" else "Media-Alta"
        reason = "frase_exacta"
    elif strong_sequence and top.score >= profile.sequence_score_min and margin >= profile.sequence_margin_min:
        detected = True
        confidence = "Media-Alta" if profile.slug != "muy_flexible" else "Media"
        reason = "subsecuencia_fuerte"
    elif fuzzy_high and top.score >= profile.fuzzy_high_score_min and margin >= profile.fuzzy_high_margin_min:
        detected = True
        confidence = "Media-Alta" if profile.slug != "muy_flexible" else "Media"
        reason = "fuzzy_alta"
    elif fuzzy_medium and top.score >= profile.fuzzy_medium_score_min and margin >= profile.fuzzy_medium_margin_min:
        detected = True
        confidence = "Media"
        reason = "fuzzy_media"
    elif high_coverage and top.score >= profile.coverage_high_score_min and margin >= profile.coverage_high_margin_min:
        detected = True
        confidence = "Media"
        reason = "cobertura_alta"
    elif medium_coverage and top.score >= profile.coverage_medium_score_min and margin >= profile.coverage_medium_margin_min:
        detected = True
        confidence = "Baja"
        reason = "cobertura_media"
    elif top.score >= profile.absolute_score_min and margin >= profile.absolute_margin_min:
        detected = True
        confidence = "Alta" if profile.slug == "estricto" else "Media"
        reason = "score_absoluto"

    if detected:
        return DetectionResult(
            detected_code=top.code,
            possible_code=second.code if second and second.score >= profile.possible_score_min else "",
            score=top.score,
            confidence=confidence,
            evidence="; ".join(tuple(dict.fromkeys(top.evidence))[:5]) or "deteccion_confirmada",
            reason=reason,
            abstained=False,
            meta=top.meta,
        )

    possible_codes = [cand.code for cand in ranked[:3] if cand.score >= profile.possible_score_min]
    return DetectionResult(
        detected_code="No Detectada",
        possible_code=", ".join(possible_codes),
        score=top.score,
        confidence="Baja" if top.score >= profile.possible_score_min else "Nula",
        evidence="; ".join(tuple(dict.fromkeys(top.evidence))[:5]) or "evidencia_insuficiente",
        reason="abstencion",
        abstained=True,
        meta=top.meta if possible_codes else None,
    )


def apply_detection_profiles_to_dataframe(
    df: pd.DataFrame,
    *,
    metadata_df: pd.DataFrame | None = None,
    profiles: Sequence[str] = ("estricto", "moderado", "muy_flexible"),
    title_aliases: Sequence[str] = ("titulo", "título"),
    description_aliases: Sequence[str] = ("descripcion", "descripción"),
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    title_col = _resolve_column(df, title_aliases)
    desc_col = _resolve_column(df, description_aliases)
    item_cols = [
        col
        for col in df.columns
        if _normalize_text(col).startswith("item ")
        or _normalize_text(col).startswith("item_")
        or re.fullmatch(r"item\s*\d+", _normalize_text(col))
    ]
    if not title_col and not desc_col and not item_cols:
        return df

    profile_keys = tuple(dict.fromkeys(normalize_detection_profile_key(profile) for profile in profiles if str(profile or "").strip()))
    if not profile_keys:
        profile_keys = ("estricto", "moderado", "muy_flexible")

    metadata_records = _metadata_records_from_df(metadata_df)
    out = df.copy()
    original_col = _resolve_column(out, ["ficha_detectada", "ficha detectada", "ficha", "numero ficha"])
    if original_col and "ficha_detectada_original" not in out.columns:
        out["ficha_detectada_original"] = out[original_col].fillna("").astype(str)

    column_positions = {col: idx for idx, col in enumerate(out.columns)}
    title_idx = column_positions.get(title_col) if title_col else None
    desc_idx = column_positions.get(desc_col) if desc_col else None
    item_indices = [column_positions[col] for col in item_cols if col in column_positions]

    buffers: dict[str, dict[str, list[object]]] = {
        profile: {
            "ficha_detectada": [],
            "nombre_ficha": [],
            "posible_ficha": [],
            "score_ficha": [],
            "confianza_ficha": [],
            "evidencia_ficha": [],
            "motivo_ficha": [],
            "tiene_ct": [],
            "registro_sanitario": [],
            "enlace_ficha_minsa": [],
            "clase_ficha": [],
        }
        for profile in profile_keys
    }

    for row in out.itertuples(index=False, name=None):
        ranked = _rank_candidates_flexible(
            title=row[title_idx] if title_idx is not None else "",
            description=row[desc_idx] if desc_idx is not None else "",
            items=[row[idx] for idx in item_indices],
            metadata_records=metadata_records,
        )
        for profile in profile_keys:
            result = _resolve_flexible_detection(ranked, profile)
            meta = result.meta
            profile_buffer = buffers[profile]
            profile_buffer["ficha_detectada"].append(result.detected_code)
            profile_buffer["nombre_ficha"].append(meta.name if meta else "")
            profile_buffer["posible_ficha"].append(result.possible_code)
            profile_buffer["score_ficha"].append(round(float(result.score or 0.0), 2))
            profile_buffer["confianza_ficha"].append(result.confidence)
            profile_buffer["evidencia_ficha"].append(result.evidence)
            profile_buffer["motivo_ficha"].append(result.reason)
            profile_buffer["tiene_ct"].append(meta.tiene_ct if meta else "No")
            profile_buffer["registro_sanitario"].append(meta.tiene_rs if meta else "No")
            profile_buffer["enlace_ficha_minsa"].append(meta.enlace_minsa if meta else "")
            profile_buffer["clase_ficha"].append(meta.clase if meta else "")

    for profile in profile_keys:
        payload = buffers[profile]
        for base_name, values in payload.items():
            out[flexible_output_col(base_name, profile)] = values

    return out
