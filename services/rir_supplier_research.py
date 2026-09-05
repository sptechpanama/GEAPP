"""Helpers for the RIR supplier-research executive snapshot."""

from __future__ import annotations

import pandas as pd


RIR_TOP5_SHEET = "RIR_TOP5_DIARIO"
RIR_TOP5_SERVICE_VERSION = 1


def latest_top5_snapshot(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Return the newest complete daily Top-5 snapshot, ordered by ranking.

    Daily reruns may temporarily leave more than one row for the same ranking.
    The newest ``actualizado_en`` wins, so Streamlit remains deterministic while
    the writer replaces that day's rows.
    """

    if frame is None or frame.empty:
        return pd.DataFrame()
    if "fecha_corte" not in frame.columns or "ranking" not in frame.columns:
        return pd.DataFrame()

    result = frame.copy()
    result["__fecha_corte__"] = pd.to_datetime(
        result["fecha_corte"], errors="coerce", format="mixed", utc=True
    )
    result["__ranking__"] = pd.to_numeric(result["ranking"], errors="coerce")
    result = result[
        result["__fecha_corte__"].notna()
        & result["__ranking__"].between(1, 5, inclusive="both")
    ].copy()
    if result.empty:
        return pd.DataFrame()

    if "actualizado_en" in result.columns:
        result["__actualizado_en__"] = pd.to_datetime(
            result["actualizado_en"], errors="coerce", format="mixed", utc=True
        )
        result = result.sort_values(
            ["__fecha_corte__", "__ranking__", "__actualizado_en__"],
            ascending=[False, True, True],
            kind="stable",
            na_position="first",
        )
    else:
        result = result.sort_values(
            ["__fecha_corte__", "__ranking__"],
            ascending=[False, True],
            kind="stable",
        )

    result["__day__"] = result["__fecha_corte__"].dt.normalize()
    daily_frames: list[pd.DataFrame] = []
    for _day, daily in result.groupby("__day__", sort=False):
        daily = daily.drop_duplicates("__ranking__", keep="last")
        daily_frames.append(daily)
        if set(daily["__ranking__"].astype(int)) == {1, 2, 3, 4, 5}:
            result = daily
            break
    else:
        # En la primera corrida puede no haber todavía cinco candidatas válidas.
        # En ese único caso se muestra el corte más reciente disponible.
        result = daily_frames[0]

    result = result.sort_values("__ranking__", kind="stable").head(5)
    result["ranking"] = result["__ranking__"].astype(int)
    return result.drop(
        columns=["__fecha_corte__", "__ranking__", "__actualizado_en__", "__day__"],
        errors="ignore",
    ).reset_index(drop=True)


def top5_general_recommendation(frame: pd.DataFrame | None) -> str:
    """Return the first non-empty executive recommendation in a snapshot."""

    if frame is None or frame.empty or "recomendacion_general" not in frame.columns:
        return ""
    values = frame["recomendacion_general"].fillna("").astype(str).str.strip()
    values = values[values.ne("")]
    return values.iloc[0] if not values.empty else ""
