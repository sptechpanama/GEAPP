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
    "precio_total_acto",
    "binding_score",
)


def enrich_line_results_context(
    frame: pd.DataFrame,
    *,
    acts: pd.DataFrame | None = None,
    minsa_url: str = "",
) -> pd.DataFrame:
    """Agrega contexto visible sin mezclarlo con los montos atribuibles."""

    if frame is None or frame.empty:
        return pd.DataFrame() if frame is None else frame.copy()
    output = frame.copy()
    if "precio_total_acto" not in output:
        output["precio_total_acto"] = 0.0
    output["precio_total_acto"] = pd.to_numeric(
        output["precio_total_acto"], errors="coerce"
    ).fillna(0.0)

    if isinstance(acts, pd.DataFrame) and not acts.empty:
        amount_column = next(
            (
                column
                for column in (
                    "reference_amount_context",
                    "reference_amount",
                    "precio_referencia",
                )
                if column in acts.columns
            ),
            "",
        )
        if amount_column:
            amounts = pd.to_numeric(acts[amount_column], errors="coerce").fillna(0.0)
            id_map: dict[str, float] = {}
            for id_column in ("acto_key", "id"):
                if id_column in acts:
                    id_map.update(
                        {
                            str(key or "").strip(): float(amount)
                            for key, amount in zip(acts[id_column], amounts)
                            if str(key or "").strip()
                        }
                    )
            link_column = "enlace" if "enlace" in acts else ("acto_url" if "acto_url" in acts else "")
            url_map = (
                {
                    str(url or "").strip(): float(amount)
                    for url, amount in zip(acts[link_column], amounts)
                    if str(url or "").strip()
                }
                if link_column
                else {}
            )
            for index in output.index:
                if float(output.at[index, "precio_total_acto"] or 0.0) > 0:
                    continue
                acto_id = str(output.at[index, "acto_id"] or "").strip() if "acto_id" in output else ""
                acto_url = str(output.at[index, "acto_url"] or "").strip() if "acto_url" in output else ""
                output.at[index, "precio_total_acto"] = id_map.get(
                    acto_id,
                    url_map.get(acto_url, 0.0),
                )

    if "enlace_ficha_minsa" not in output:
        output["enlace_ficha_minsa"] = ""
    clean_minsa_url = str(minsa_url or "").strip()
    if clean_minsa_url:
        missing = output["enlace_ficha_minsa"].fillna("").astype(str).str.strip().eq("")
        output.loc[missing, "enlace_ficha_minsa"] = clean_minsa_url
    return output


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
        "enlace_ficha_minsa": "Ficha MINSA",
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
        "precio_total_acto": "Precio total del acto",
        "precio_participacion_total": "Oferta del renglón",
        "binding_method": "Cómo se vinculó la oferta",
        "binding_score": "Confianza oferta–renglón",
        "match_requires_review": "Revisar",
    }
    columns = [column for column in rename if column in frame]
    return frame[columns].rename(columns=rename).copy()
