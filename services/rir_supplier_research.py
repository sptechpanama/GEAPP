"""Helpers for the RIR supplier-research executive snapshot."""

from __future__ import annotations

import pandas as pd


RIR_TOP10_SHEET = "RIR_TOP10_DIARIO"
RIR_TOP_LIMIT = 10
RIR_TOP_SERVICE_VERSION = 3
RIR_TOP_LINK_COLUMNS = (
    "enlace_acto",
    "enlace_ficha_minsa",
    "enlace_producto_recomendado",
)

# Compatibilidad temporal con despliegues/cachés que todavía importan los
# nombres anteriores. El módulo nuevo utiliza los símbolos RIR_TOP10_*.
RIR_TOP5_SHEET = RIR_TOP10_SHEET
RIR_TOP5_SERVICE_VERSION = RIR_TOP_SERVICE_VERSION
RIR_TOP5_LINK_COLUMNS = RIR_TOP_LINK_COLUMNS


def top_link_coverage(frame: pd.DataFrame | None) -> dict[str, int]:
    """Count valid HTTP(S) links for each executive-snapshot link field."""

    coverage = {column: 0 for column in RIR_TOP_LINK_COLUMNS}
    if frame is None or frame.empty:
        return coverage
    for column in RIR_TOP_LINK_COLUMNS:
        if column not in frame.columns:
            continue
        values = frame[column].fillna("").astype(str).str.strip().str.lower()
        coverage[column] = int(values.str.match(r"^https?://").sum())
    return coverage


def latest_top_snapshot(
    frame: pd.DataFrame | None,
    *,
    rank_limit: int = RIR_TOP_LIMIT,
) -> pd.DataFrame:
    """Return the newest complete daily snapshot, ordered by ranking.

    Daily reruns may temporarily leave more than one row for the same ranking.
    The newest ``actualizado_en`` wins, so Streamlit remains deterministic while
    the writer replaces that day's rows.
    """

    rank_limit = max(1, int(rank_limit))
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
        & result["__ranking__"].between(1, rank_limit, inclusive="both")
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
        if set(daily["__ranking__"].astype(int)) == set(
            range(1, rank_limit + 1)
        ):
            result = daily
            break
    else:
        # En la primera corrida puede no haber todavía suficientes candidatas válidas.
        # En ese único caso se muestra el corte más reciente disponible.
        result = daily_frames[0]

    result = result.sort_values("__ranking__", kind="stable").head(rank_limit)
    result["ranking"] = result["__ranking__"].astype(int)
    return result.drop(
        columns=["__fecha_corte__", "__ranking__", "__actualizado_en__", "__day__"],
        errors="ignore",
    ).reset_index(drop=True)


def latest_top10_snapshot(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Return the latest complete Top-10 cut, with partial-cut fallback."""

    return latest_top_snapshot(frame, rank_limit=RIR_TOP_LIMIT)


def latest_top5_snapshot(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Compatibility helper for consumers that explicitly still need Top 5."""

    return latest_top_snapshot(frame, rank_limit=5)


def top_general_recommendation(frame: pd.DataFrame | None) -> str:
    """Return the first non-empty executive recommendation in a snapshot."""

    if frame is None or frame.empty or "recomendacion_general" not in frame.columns:
        return ""
    values = frame["recomendacion_general"].fillna("").astype(str).str.strip()
    values = values[values.ne("")]
    return values.iloc[0] if not values.empty else ""


def top5_link_coverage(frame: pd.DataFrame | None) -> dict[str, int]:
    """Backward-compatible alias for :func:`top_link_coverage`."""

    return top_link_coverage(frame)


def top5_general_recommendation(frame: pd.DataFrame | None) -> str:
    """Backward-compatible alias for :func:`top_general_recommendation`."""

    return top_general_recommendation(frame)
