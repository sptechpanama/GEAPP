from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
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

