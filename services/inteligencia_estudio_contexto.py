from __future__ import annotations

"""Contexto visible de resultados del estudio profundo.

El precio total del acto es informativo: nunca reemplaza ni altera el precio
unitario de participacion ni los montos atribuibles a la ficha.
"""

import re

import pandas as pd


def _identifier(value: object) -> str:
    text = str(value or "").strip()
    return re.sub(r"\.0$", "", text)


def enrich_study_details(
    frame: pd.DataFrame,
    *,
    acts: pd.DataFrame | None = None,
    minsa_url: str = "",
) -> pd.DataFrame:
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
            for id_column in ("acto_key", "id", "acto_id"):
                if id_column in acts.columns:
                    id_map.update(
                        {
                            _identifier(key): float(amount)
                            for key, amount in zip(acts[id_column], amounts)
                            if _identifier(key)
                        }
                    )
            url_column = next(
                (column for column in ("enlace", "acto_url") if column in acts.columns),
                "",
            )
            url_map = (
                {
                    str(url or "").strip(): float(amount)
                    for url, amount in zip(acts[url_column], amounts)
                    if str(url or "").strip()
                }
                if url_column
                else {}
            )
            for index in output.index:
                if float(output.at[index, "precio_total_acto"] or 0.0) > 0:
                    continue
                acto_id = _identifier(output.at[index, "acto_id"]) if "acto_id" in output else ""
                acto_url = (
                    str(output.at[index, "acto_url"] or "").strip()
                    if "acto_url" in output
                    else ""
                )
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
