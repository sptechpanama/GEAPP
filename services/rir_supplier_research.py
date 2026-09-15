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
RIR_TOP_SERVICE_VERSION = 6
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
    # A writer may publish a dated rank-0 row to explicitly report an empty Top.
    # Do not revive yesterday's opportunities when today's cut contains none.
    if frame is not None and not frame.empty and "fecha_corte" in frame:
        dates = pd.to_datetime(frame["fecha_corte"], errors="coerce", format="mixed", utc=True)
        frame = frame.loc[dates.dt.normalize().eq(dates.max().normalize())] if dates.notna().any() else frame
    return latest_top_snapshot(frame, rank_limit=RIR_TOP_LIMIT, prefer_complete=False)


PANAMA = ZoneInfo("America/Panama")
RIR_ACT_SHEETS = ("cl_abiertas_rir_sin_requisitos", "cl_prog_sin_requisitos", "ap_sin_requisitos")
INACTIVE_LABELS = {"no_vigente", "vencido", "vencida", "cancelado", "cancelada", "archivado", "archivada", "descartado", "descartada", "suspendido", "adjudicado", "desierto"}


def _text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def _local_timestamp(value: object) -> pd.Timestamp | None:
    raw = _text(value)
    if not raw:
        return None
    try:
        parsed = pd.to_datetime(raw, dayfirst=not bool(re.match(r"^\d{4}-", raw)), errors="raise")
        return parsed.tz_localize(PANAMA) if parsed.tzinfo is None else parsed.tz_convert(PANAMA)
    except (ValueError, TypeError, OverflowError):
        return None


def _deadline(value: object) -> tuple[pd.Timestamp | None, bool]:
    raw = _text(value)
    # Published CL dates commonly contain an opening-to-closing date range.
    tokens = re.findall(r"(?:\d{4}-\d{2}-\d{2}|\d{2}[-/]\d{2}[-/]\d{4})(?:[T ]\d{1,2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?)?", raw)
    token = tokens[-1] if tokens else raw
    return _local_timestamp(token), bool(re.search(r"\d{1,2}:\d{2}", token))


def _line(row: Mapping) -> str:
    value = _text(row.get("renglon"))
    if value:
        return value.removesuffix(".0")
    found = re.search(r"renglon\s*(\d+)", research_column_key(row.get("oportunidad", "")).replace("_", " "))
    return found.group(1) if found else ""


def _key(row: Mapping) -> tuple[str, str, str]:
    return (_text(row.get("numero_acto")), _text(row.get("ficha")).removesuffix(".0"), _line(row))


def assess_research_validity(frame: pd.DataFrame, current_acts: pd.DataFrame | None = None,
                             *, research: pd.DataFrame | None = None, now=None) -> pd.DataFrame:
    """Evaluate saved evidence against the clock and latest scraper publication.

    Never writes research dates, supplier prices or rankings. Unknown dates,
    unknown closing times today, stale sources and mixed global acts cannot be
    presented as actionable opportunities.
    """
    clock = _local_timestamp(now) if now is not None else pd.Timestamp.now(tz=PANAMA)
    if clock is None:
        raise ValueError("Fecha de verificación inválida")
    acts = {}
    if current_acts is not None and not current_acts.empty:
        for row in current_acts.to_dict("records"):
            key = _text(row.get("numero_acto"))
            checked = _local_timestamp(row.get("verificado_en"))
            old = acts.get(key)
            if key and (old is None or (checked is not None and (old[0] is None or checked > old[0]))):
                acts[key] = (checked, row)
    investigations = {}
    if research is not None and not research.empty:
        # Latest version wins per exact act/ficha/line; never mix distinct lines.
        for row in reversed(prepare_research_table(research, include_inactive=True).to_dict("records")):
            investigations[_key(row)] = row
    output = []
    for row in frame.to_dict("records"):
        item = dict(row)
        raw = _text(row.get("fecha_cierre"))
        if not raw:
            match = re.search(r"\bcierre\s*:\s*((?:\d{4}-\d{2}-\d{2}|\d{2}[-/]\d{2}[-/]\d{4})(?:[T ]\d{1,2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?)?)", _text(row.get("observaciones")), flags=re.I)
            raw = match.group(1) if match else ""
        closed, exact = _deadline(raw)
        reason = ""
        status = research_column_key(_text(row.get("estado_investigacion")) or _text(row.get("estado")))
        if status in INACTIVE_LABELS:
            reason = "Retirada en la investigación publicada"
        newer = investigations.get(_key(row))
        if newer:
            if research_column_key(_text(newer.get("estado_investigacion"))) in INACTIVE_LABELS:
                reason = "Retirada en la investigación más reciente"
            updated = _local_timestamp(newer.get("actualizado_en"))
            previous = _local_timestamp(row.get("actualizado_en"))
            if not reason and updated is not None and previous is not None and updated > previous:
                reason = "El análisis cambió después de publicar el ranking"
        checked, live = acts.get(_text(row.get("numero_acto")), (None, None))
        source_note = "Sin verificación reciente del scraper"
        if live is not None:
            source_note = "Verificado con la última captura del scraper"
            live_close, live_exact = _deadline(live.get("fecha_cierre"))
            published = _local_timestamp(row.get("actualizado_en"))
            if live_close is not None and checked is not None and (published is None or checked >= published):
                closed, exact = live_close, live_exact
            codes = set(re.findall(r"\b\d{4,7}\b", _text(live.get("fichas_sin_requisitos"))))
            restricted = set(re.findall(r"\b\d{4,7}\b", _text(live.get("fichas_con_requisitos"))))
            unverified = set(re.findall(r"\b\d{4,7}\b", _text(live.get("fichas_por_verificar"))))
            if _key(row)[1] not in codes:
                reason = reason or "Ficha no confirmada sin requisitos en la captura actual"
            if _key(row)[1] in restricted | unverified:
                reason = reason or "La ficha tiene requisitos o una clasificación pendiente en la captura"
            scope = research_column_key(_text(live.get("tipo_acto")))
            award = research_column_key(_text(live.get("tipo_adjudicacion")))
            if ("mixto" in scope or restricted or unverified) and not any(word in award for word in ("renglon", "parcial", "linea", "item")):
                reason = reason or "Acto mixto sin adjudicación por renglón"
            if research_column_key(_text(live.get("descartar"))) in {"true", "si", "1", "x"}:
                reason = reason or "Descartada en la vista de actos"
            if checked is None or clock - checked > pd.Timedelta(hours=36):
                reason = reason or "Captura del scraper sin verificar en las últimas 36 horas"
        else:
            reason = reason or source_note
        if closed is None:
            validity, reason = "Por verificar", reason or "Sin fecha de cierre verificable"
        elif (exact and closed <= clock) or (not exact and closed.date() < clock.date()):
            validity, reason = "Vencida", "La fecha de cierre registrada ya pasó"
        elif not exact and closed.date() == clock.date():
            validity, reason = "Por verificar", "Cierra hoy, pero falta la hora exacta"
        elif reason:
            validity = "No vigente" if "Retirada" in reason else "Por verificar"
        else:
            validity, reason = "Vigente", source_note
        item.update(vigencia=validity, motivo_vigencia=reason,
                    cierre_verificado=closed.isoformat() if closed is not None and exact else (closed.strftime("%Y-%m-%d") if closed is not None else ""),
                    verificado_en=checked.isoformat() if checked is not None else "")
        output.append(item)
    return pd.DataFrame(output, columns=list(frame.columns) + [c for c in ("vigencia", "motivo_vigencia", "cierre_verificado", "verificado_en") if c not in frame.columns])


def read_current_research_acts(spreadsheet) -> pd.DataFrame:
    """Read only identity, date and eligibility columns, never the item payloads."""
    fields = {"enlace": "enlace_acto", "fecha": "fecha_cierre", "fecha_de_actualizacion": "verificado_en",
              "fichas_sin_requisitos": "fichas_sin_requisitos", "tipo_de_adjudicacion": "tipo_adjudicacion",
              "tipo_de_acto_sin_requisitos": "tipo_acto", "descartar": "descartar",
              "fichas_con_requisitos": "fichas_con_requisitos", "fichas_por_verificar": "fichas_por_verificar"}
    heads = spreadsheet.values_batch_get([f"'{name}'!A1:AZ1" for name in RIR_ACT_SHEETS]).get("valueRanges", [])
    if len(heads) != len(RIR_ACT_SHEETS):
        raise ValueError("Lectura incompleta de las fuentes de actos RIR")
    ranges, targets = [], []
    for name, data in zip(RIR_ACT_SHEETS, heads):
        headers = data.get("values", [[]])[0]
        found = set()
        for position, label in enumerate(headers, 1):
            key = research_column_key(label)
            if key in fields:
                found.add(key)
                col, number = "", position
                while number:
                    number, remainder = divmod(number - 1, 26)
                    col = chr(65 + remainder) + col
                ranges.append(f"'{name}'!{col}2:{col}15000")
                targets.append((name, fields[key]))
        if found != set(fields):
            raise ValueError(f"Faltan columnas de verificación en {name}")
    values = spreadsheet.values_batch_get(ranges).get("valueRanges", [])
    if len(values) != len(targets):
        raise ValueError("Lectura incompleta de fechas de cierre")
    tables = {name: {} for name in RIR_ACT_SHEETS}
    for (name, column), data in zip(targets, values):
        tables[name][column] = pd.Series([row[0] if row else "" for row in data.get("values", [])], dtype=object)
    result = pd.concat([pd.DataFrame(table) for table in tables.values()], ignore_index=True).fillna("")
    if not result.empty:
        result["numero_acto"] = result["enlace_acto"].str.extract(r"(\d{4}-\d+(?:-\d+)+-[A-Z]+-\d+)", expand=False)
    return result


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
