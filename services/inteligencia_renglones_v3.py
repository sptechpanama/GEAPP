from __future__ import annotations

"""Preparación auditable de resultados del estudio profundo por renglón."""

from typing import Any

import pandas as pd


NUMERIC_COLUMNS = (
    "match_score",
    "cantidad",
    "precio_referencia_unitario",
    "precio_referencia_total",
    "precio_participacion_unitario",
    "precio_participacion_total",
    "binding_score",
)


def prepare_line_results(
    frame: pd.DataFrame,
    *,
    request_id: str = "",
    ficha: str = "",
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    output = frame.copy()
    output.columns = [str(column).strip() for column in output.columns]
    if request_id and "request_id" in output:
        exact = output["request_id"].fillna("").astype(str).str.strip().eq(str(request_id).strip())
        # Nunca mostrar el estudio anterior de la misma ficha mientras una
        # solicitud nueva todavía no ha publicado su resultado.
        output = output.loc[exact].copy()
    if ficha and "ficha" in output:
        output = output[
            output["ficha"]
            .fillna("")
            .astype(str)
            .str.replace(r"\D", "", regex=True)
            .eq(str(ficha).strip())
        ].copy()
    for column in NUMERIC_COLUMNS:
        if column in output:
            output[column] = (
                pd.to_numeric(output[column], errors="coerce")
                .fillna(0.0)
                .astype(float)
            )
    for column in ("match_requires_review",):
        if column in output:
            output[column] = (
                output[column]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
                .isin({"1", "true", "si", "sí", "yes"})
            )
    sort_columns = [
        column
        for column in ("acto_id", "renglon_numero", "proveedor", "line_detail_id")
        if column in output
    ]
    if sort_columns:
        output = output.sort_values(sort_columns, kind="stable")
    return output.reset_index(drop=True)


def summarize_line_results(frame: pd.DataFrame) -> dict[str, Any]:
    if frame is None or frame.empty:
        return {
            "actos": 0,
            "renglones": 0,
            "ofertas": 0,
            "referencia_atribuible": 0.0,
            "participacion_atribuible": 0.0,
            "pendientes_revision": 0,
        }
    valid = frame.copy()
    if "renglon_numero" in valid:
        valid = valid[valid["renglon_numero"].fillna("").astype(str).str.strip().ne("")]
    unique_lines = valid
    line_keys = [
        column for column in ("acto_id", "renglon_id") if column in valid
    ]
    if line_keys:
        unique_lines = valid.drop_duplicates(line_keys, keep="first")
    offers = valid
    if "precio_participacion_unitario" in offers:
        offers = offers[
            pd.to_numeric(
                offers["precio_participacion_unitario"], errors="coerce"
            ).fillna(0.0)
            > 0
        ]
    review = (
        int(frame["match_requires_review"].fillna(False).astype(bool).sum())
        if "match_requires_review" in frame
        else 0
    )
    return {
        "actos": (
            int(valid["acto_id"].fillna("").astype(str).nunique())
            if "acto_id" in valid
            else 0
        ),
        "renglones": len(unique_lines),
        "ofertas": len(offers),
        "referencia_atribuible": (
            float(
                pd.to_numeric(
                    unique_lines.get(
                        "precio_referencia_total",
                        pd.Series(dtype=float),
                    ),
                    errors="coerce",
                )
                .fillna(0.0)
                .sum()
            )
        ),
        "participacion_atribuible": (
            float(
                pd.to_numeric(
                    offers.get(
                        "precio_participacion_total",
                        pd.Series(dtype=float),
                    ),
                    errors="coerce",
                )
                .fillna(0.0)
                .sum()
            )
        ),
        "pendientes_revision": review,
    }


def display_line_results(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    rename = {
        "acto_url": "Acto",
        "acto_nombre": "Nombre del acto",
        "renglon_numero": "Renglón",
        "renglon_texto": "Descripción del renglón",
        "match_method": "Cómo se identificó",
        "match_score": "Confianza ficha–renglón",
        "match_evidence": "Evidencia",
        "cantidad": "Cantidad",
        "unidad_medida": "Unidad",
        "precio_referencia_unitario": "Referencia unitaria",
        "precio_referencia_total": "Referencia del renglón",
        "proveedor": "Proveedor",
        "precio_participacion_unitario": "Oferta unitaria",
        "precio_participacion_total": "Oferta del renglón",
        "binding_method": "Cómo se vinculó la oferta",
        "binding_score": "Confianza oferta–renglón",
        "match_requires_review": "Revisar",
    }
    columns = [column for column in rename if column in frame]
    return frame[columns].rename(columns=rename).copy()
