from __future__ import annotations

import re
from collections import Counter
from datetime import datetime

import pandas as pd

from services.panama_compra_detection_v2 import flexible_output_col, normalize_detection_profile_key


FICHA_TOKEN_RE = re.compile(r"\b\d{3,8}\*?\b")


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    return text


def _parse_number(value: object) -> float:
    text = _clean_text(value)
    if not text:
        return 0.0
    text = text.replace("$", "").replace("USD", "").replace("us$", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_any_date(value: object) -> pd.Timestamp:
    text = _clean_text(value)
    if not text:
        return pd.NaT
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, errors="coerce")
    return parsed


def _extract_ficha_codes(value: object) -> list[str]:
    tokens = FICHA_TOKEN_RE.findall(str(value or ""))
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        code = re.sub(r"\D", "", token or "")
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _detected_mask(series: pd.Series) -> pd.Series:
    clean = series.fillna("").astype(str).str.strip()
    return clean.ne("") & clean.str.lower().ne("no detectada")


def _dominant_value(series: pd.Series) -> str:
    counter = Counter(_clean_text(value) for value in series.tolist() if _clean_text(value))
    if not counter:
        return ""
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]


def build_profile_comparison(df: pd.DataFrame, profiles: list[str] | tuple[str, ...]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for raw_profile in profiles:
        profile = normalize_detection_profile_key(raw_profile)
        det_col = flexible_output_col("ficha_detectada", profile)
        score_col = flexible_output_col("score_ficha", profile)
        conf_col = flexible_output_col("confianza_ficha", profile)
        if det_col not in df.columns:
            continue
        mask = _detected_mask(df[det_col])
        subset = df.loc[mask].copy()
        unique_codes: set[str] = set()
        if not subset.empty:
            for value in subset[det_col].tolist():
                unique_codes.update(_extract_ficha_codes(value))
        rows.append(
            {
                "Perfil": profile,
                "Actos detectados": int(mask.sum()),
                "Fichas únicas": int(len(unique_codes)),
                "Score promedio": round(pd.to_numeric(subset.get(score_col), errors="coerce").fillna(0.0).mean(), 2) if not subset.empty else 0.0,
                "Confianza dominante": _dominant_value(subset.get(conf_col, pd.Series(dtype=str))),
            }
        )
    return pd.DataFrame(rows)


def build_rescued_acts_view(df: pd.DataFrame, profile_key: str, *, previous_col: str = "ficha_detectada") -> pd.DataFrame:
    profile = normalize_detection_profile_key(profile_key)
    det_col = flexible_output_col("ficha_detectada", profile)
    if det_col not in df.columns:
        return pd.DataFrame()
    previous_mask = _detected_mask(df.get(previous_col, pd.Series("", index=df.index)))
    rescued_mask = ~previous_mask & _detected_mask(df[det_col])
    return df.loc[rescued_mask].copy()


def build_detected_fichas_summary(df: pd.DataFrame, profile_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    profile = normalize_detection_profile_key(profile_key)
    det_col = flexible_output_col("ficha_detectada", profile)
    name_col = flexible_output_col("nombre_ficha", profile)
    score_col = flexible_output_col("score_ficha", profile)
    conf_col = flexible_output_col("confianza_ficha", profile)
    ct_col = flexible_output_col("tiene_ct", profile)
    rs_col = flexible_output_col("registro_sanitario", profile)
    class_col = flexible_output_col("clase_ficha", profile)
    link_col = flexible_output_col("enlace_ficha_minsa", profile)

    if det_col not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    work = df.copy()
    work["__ficha_code__"] = work[det_col].map(lambda value: _extract_ficha_codes(value)[:1])
    work = work[work["__ficha_code__"].map(len) > 0].copy()
    if work.empty:
        return pd.DataFrame(), pd.DataFrame()

    work["__ficha_code__"] = work["__ficha_code__"].map(lambda values: values[0])
    work["__precio__"] = work.get("precio_referencia", 0).map(_parse_number)
    date_candidates = [col for col in ("fecha", "publicacion", "fecha_actualizacion") if col in work.columns]
    if date_candidates:
        work["__fecha_ref__"] = pd.NaT
        for col in date_candidates:
            parsed = work[col].map(_parse_any_date)
            work["__fecha_ref__"] = work["__fecha_ref__"].fillna(parsed)
    else:
        work["__fecha_ref__"] = pd.NaT

    grouped = work.groupby("__ficha_code__", dropna=False)
    summary = pd.DataFrame(
        {
            "Ficha #": grouped["__ficha_code__"].first(),
            "Nombre ficha": grouped[name_col].apply(_dominant_value) if name_col in work.columns else "",
            "Actos detectados": grouped["id"].nunique() if "id" in work.columns else grouped.size(),
            "Monto referencial total": grouped["__precio__"].sum().round(2),
            "Entidades distintas": grouped["entidad"].nunique() if "entidad" in work.columns else 0,
            "Score promedio": grouped[score_col].mean().round(2) if score_col in work.columns else 0.0,
            "Confianza dominante": grouped[conf_col].apply(_dominant_value) if conf_col in work.columns else "",
            "Tiene CT": grouped[ct_col].apply(_dominant_value) if ct_col in work.columns else "",
            "Registro sanitario": grouped[rs_col].apply(_dominant_value) if rs_col in work.columns else "",
            "Clase ficha": grouped[class_col].apply(_dominant_value) if class_col in work.columns else "",
            "Enlace ficha MINSA": grouped[link_col].apply(_dominant_value) if link_col in work.columns else "",
            "Última fecha ref.": grouped["__fecha_ref__"].max(),
        }
    ).reset_index(drop=True)

    if "Nombre ficha" in summary.columns:
        summary["Nombre ficha"] = summary["Nombre ficha"].fillna("").astype(str).str.strip()
        summary.loc[summary["Nombre ficha"].eq(""), "Nombre ficha"] = summary["Ficha #"].map(lambda code: f"Ficha {code}")

    summary = summary.sort_values(
        by=["Actos detectados", "Monto referencial total", "Score promedio"],
        ascending=[False, False, False],
        kind="stable",
    ).reset_index(drop=True)
    return summary, work


def build_difference_view(df: pd.DataFrame, profiles: list[str] | tuple[str, ...]) -> pd.DataFrame:
    profile_keys = [normalize_detection_profile_key(profile) for profile in profiles]
    det_cols = [flexible_output_col("ficha_detectada", profile) for profile in profile_keys if flexible_output_col("ficha_detectada", profile) in df.columns]
    if len(det_cols) < 2:
        return pd.DataFrame()
    work = df.copy()
    comparable = work[det_cols].fillna("").astype(str).apply(lambda row: tuple(val.strip() for val in row), axis=1)
    diff_mask = comparable.map(lambda values: len({value for value in values}) > 1)
    return work.loc[diff_mask].copy()
