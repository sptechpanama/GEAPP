"""Helpers for the RIR supplier-research executive snapshot."""

from __future__ import annotations

import pandas as pd
import re
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo


RIR_TOP10_SHEET = "RIR_TOP10_DIARIO"
RIR_TOP_LIMIT = 10
RIR_TOP_SERVICE_VERSION = 4
RIR_RESEARCH_SHEET = "RIR_INVESTIGACION_PROVEEDORES"
RIR_PRICES_SHEET = "RIR_PRECIOS_HISTORICOS"
RIR_RESEARCH_SHEETS = (RIR_TOP10_SHEET, RIR_RESEARCH_SHEET, RIR_PRICES_SHEET)
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
    prefer_complete: bool = True,
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
        & result["__ranking__"].mod(1).eq(0)
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
        if not prefer_complete or set(daily["__ranking__"].astype(int)) == set(
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
    """Show the newest published cut, even when fewer than ten qualify."""

    return latest_top_snapshot(frame, rank_limit=RIR_TOP_LIMIT, prefer_complete=False)


def research_column_key(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[\s_]+", "_", text)


def research_column(frame: pd.DataFrame, name: str) -> str | None:
    return next((col for col in frame if research_column_key(col) == name), None)


def research_updated_at(frame: pd.DataFrame | None) -> pd.Timestamp | None:
    if frame is None or frame.empty:
        return None
    for field in ("actualizado_en", "fecha_investigacion", "fecha_corte"):
        column = research_column(frame, field)
        if column:
            parsed = pd.to_datetime(frame[column], errors="coerce", format="mixed", utc=True)
            if parsed.notna().any():
                return parsed.max().tz_convert("America/Panama")
    return None


def prepare_research_table(frame: pd.DataFrame, *, include_inactive: bool = False) -> pd.DataFrame:
    """Keep stored evidence intact; present the latest updates first."""
    work = frame.copy()
    status = research_column(work, "estado_investigacion")
    if status and not include_inactive:
        labels = work[status].fillna("").map(research_column_key)
        work = work.loc[~labels.isin({"no_vigente", "vencido", "vencida", "cancelado", "cancelada", "archivado", "archivada"})].copy()
    updated = research_column(work, "actualizado_en")
    created = research_column(work, "fecha_investigacion")
    if updated or created:
        dates = pd.to_datetime(work[updated or created], errors="coerce", format="mixed", utc=True)
        if updated and created:
            dates = dates.fillna(pd.to_datetime(work[created], errors="coerce", format="mixed", utc=True))
        work = work.assign(__research_sort=dates).sort_values("__research_sort", ascending=False, kind="stable", na_position="last").drop(columns="__research_sort")
    return work.reset_index(drop=True)


def research_frames_from_values(response: Mapping) -> dict[str, pd.DataFrame]:
    """Validate one batch read without treating connection errors as empty data."""
    ranges = response.get("valueRanges", [])
    if len(ranges) != len(RIR_RESEARCH_SHEETS):
        raise ValueError("La lectura de las hojas RIR quedó incompleta.")
    required = ({"fecha_corte", "ranking"}, {"ficha", "numero_acto", "estado_investigacion"}, {"ficha", "actualizado_en"})
    frames = {}
    for name, data, fields in zip(RIR_RESEARCH_SHEETS, ranges, required):
        values = data.get("values", [])
        headers = [research_column_key(col) for col in values[0]] if values else []
        if not fields.issubset(headers) or len(headers) != len(set(headers)):
            raise ValueError(f"La hoja {name} tiene encabezados incompletos o repetidos.")
        rows = [list(row[:len(headers)]) + [""] * max(0, len(headers) - len(row)) for row in values[1:]]
        frames[name] = pd.DataFrame(rows, columns=headers).replace("", pd.NA).dropna(how="all")
    return frames


@dataclass
class ResearchRead:
    frames: dict[str, pd.DataFrame]
    checked_at: datetime
    error: str = ""
    using_previous: bool = False


def read_research_safely(reader: Callable, previous: ResearchRead | None = None) -> ResearchRead:
    """Retain the last successful session read when Sheets is unavailable."""
    now = datetime.now(ZoneInfo("America/Panama"))
    try:
        frames = reader()
        if previous and any(frames[name].empty and not previous.frames.get(name, pd.DataFrame()).empty for name in RIR_RESEARCH_SHEETS):
            raise ValueError("Una hoja respondió vacía después de tener registros; vuelve a consultar al terminar la publicación.")
        return ResearchRead(frames, now)
    except Exception as exc:
        return ResearchRead(previous.frames if previous else {}, previous.checked_at if previous else now,
                            f"No se pudieron leer los resultados de Google Sheets ({type(exc).__name__}).", bool(previous and previous.frames))


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
