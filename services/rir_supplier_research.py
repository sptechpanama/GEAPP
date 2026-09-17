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
RIR_TOP_SERVICE_VERSION = 7
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
    # Preserve timezone-bearing ISO dates, including midnight and exact boundaries.
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?", raw):
        return _local_timestamp(raw), True
    dates = list(re.finditer(r"\d{4}-\d{2}-\d{2}|\d{2}[-/]\d{2}[-/]\d{4}", raw))
    if not dates:
        return None, False
    date = _local_timestamp(dates[-1].group())
    if date is None:
        return None, False
    # CL: '17-09-2026 - 07:00 AM a 11:00 AM'; AP: '17-09-2026 hasta 02:00 PM'.
    times = list(re.finditer(r"(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([ap]\.?\s*m\.?)?",
                             raw[dates[-1].end():], re.I))
    if not times:
        return date, False
    hour, minute, second, period = times[-1].groups()
    hour, minute, second = int(hour), int(minute), int(second or 0)
    if minute > 59 or second > 59 or hour > (12 if period else 23) or (period and hour == 0):
        return date, False
    if period:
        hour = hour % 12 + (12 if period.lower().startswith("p") else 0)
    return date.replace(hour=hour, minute=minute, second=second), True


def _research_deadline(row: Mapping) -> str:
    raw = _text(row.get("fecha_cierre"))
    if raw:
        return raw
    notes = _text(row.get("observaciones"))
    date = re.search(r"\b(?:fecha_cierre|cierre)\s*[:=]\s*((?:\d{4}-\d{2}-\d{2}|\d{2}[-/]\d{2}[-/]\d{4})(?:[T ]\d{1,2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?)?)", notes, re.I)
    if not date:
        return ""
    raw = date.group(1)
    hour = re.search(r"\bfecha_cierre_hora\s*=\s*(\d{1,2}:\d{2}(?::\d{2})?\s*(?:[ap]m)?)", notes, re.I)
    return raw + " " + hour.group(1) if hour and ":" not in raw else raw


def _line(row: Mapping) -> str:
    value = _text(row.get("renglon"))
    if value:
        return value.removesuffix(".0")
    found = re.search(r"renglon\s*(\d+)", research_column_key(_text(row.get("oportunidad"))).replace("_", " "))
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
        raw = _research_deadline(row)
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
            elif live_close is not None and closed is not None and live_exact and not exact and live_close.date() == closed.date():
                # Adding the official hour for the same date is not a date extension.
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
            elif checked - clock > pd.Timedelta(minutes=5):
                reason = reason or "La fecha de captura está en el futuro; verificar el reloj del scraper"
        else:
            reason = reason or source_note
        if reason.startswith("Retirada"):
            validity = "No vigente"
        elif closed is None:
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
    ranges, targets, source_errors = [], [], []
    for name, data in zip(RIR_ACT_SHEETS, heads):
        headers = (data.get("values") or [[]])[0]
        found = set()
        source_ranges, source_targets = [], []
        for position, label in enumerate(headers, 1):
            key = research_column_key(label)
            if key in fields:
                found.add(key)
                col, number = "", position
                while number:
                    number, remainder = divmod(number - 1, 26)
                    col = chr(65 + remainder) + col
                source_ranges.append(f"'{name}'!{col}2:{col}15000")
                source_targets.append((name, fields[key]))
        if found != set(fields):
            source_errors.append(f"Faltan columnas de verificación en {name}; esa fuente queda pendiente")
            continue
        ranges.extend(source_ranges)
        targets.extend(source_targets)
    if not ranges:
        raise ValueError("Ninguna fuente tiene columnas de verificación completas")
    values = spreadsheet.values_batch_get(ranges).get("valueRanges", [])
    if len(values) != len(targets):
        raise ValueError("Lectura incompleta de fechas de cierre")
    tables = {name: {} for name in RIR_ACT_SHEETS}
    for (name, column), data in zip(targets, values):
        tables[name][column] = pd.Series([row[0] if row else "" for row in data.get("values", [])], dtype=object)
    result = pd.concat([pd.DataFrame(table) for table in tables.values()], ignore_index=True).fillna("")
    if not result.empty:
        result["numero_acto"] = result["enlace_acto"].str.extract(r"(\d{4}-\d+(?:-\d+)+-[A-Z]+-\d+)", expand=False)
    result.attrs["source_errors"] = source_errors
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


def _note_section(notes: str, heading: str) -> str:
    match = re.search(rf"(?:^|\n){heading}\s*:\s*(.*?)(?=\n(?:[A-ZÁÉÍÓÚ][^\n:]*:|\[FECHAS|\[EVALUACION)|\Z)", notes, re.S | re.I)
    return match.group(1).strip() if match else ""


def _evaluation(row: Mapping) -> dict[str, str]:
    """Optional explicit decisions; never infer technical approval from prose."""
    result = {name: _text(row.get(name)) for name in (
        "situacion", "que_falta", "accion_inmediata", "bloqueo_material",
        "cumplimiento_confirmado", "costo_puesto_confirmado", "stock_confirmado",
        "entrega_confirmada", "economia_viable",
    )}
    block = re.search(r"\[EVALUACION_RIR_V2\](.*?)\[/EVALUACION_RIR_V2\]",
                      _text(row.get("observaciones")), re.S)
    if block:
        for line in block.group(1).splitlines():
            name, sep, value = line.partition("=")
            if sep and name.strip() in result and not result[name.strip()]:
                result[name.strip()] = value.strip()
    return result


def _candidate_blocker(row: Mapping) -> str:
    explicit = _evaluation(row)["bloqueo_material"]
    if explicit and research_column_key(explicit) not in {"no", "ninguno", "ninguna", "false", "0"}:
        return explicit
    if research_column_key(_text(row.get("resultado_cumplimiento"))) == "no_cumple":
        return "Incompatibilidad técnica documentada"
    # Compatibility with existing investigations. 'Fuera del Top' by itself is
    # NOT a blocker: it often only means a quote or supporting document is pending.
    notes = research_column_key(_text(row.get("observaciones"))).replace("_", " ")
    if re.search(r"\b(?:falso positivo|ficha mal asignada|no es sustituto admisible)\b", notes):
        return "Correspondencia del producto descartada en la investigación"
    if "acto" in notes and "global" in notes and re.search(r"no corresponde|sin correspondencia", notes):
        return "El estudio detectó un acto global con renglones no cubiertos"
    if re.search(r"(?:numero de acto no existe|acto no existe)", notes):
        return "La investigación reportó un conflicto con el acto oficial"
    if re.search(r"precio publico.{0,100}excede (?:la )?referencia", notes):
        return "El proveedor localizado supera la referencia; buscar una alternativa"
    return ""


def _http(value: object) -> str:
    value = _text(value)
    return value if re.match(r"^https?://[^\s]+$", value, re.I) else ""


def _research_candidate(row: Mapping) -> dict:
    """Adapt saved research to the executive view without fabricating a ranking."""
    item = dict(row)
    notes = _text(row.get("observaciones"))
    narrative = re.sub(r"\[(?:FECHAS_RIR_V1|EVALUACION_RIR_V2)\].*?\[/(?:FECHAS_RIR_V1|EVALUACION_RIR_V2)\]", "", notes, flags=re.S).strip()
    links = re.findall(r"https?://[^\s<>]+", _text(row.get("fuentes")))
    ctni = list(dict.fromkeys(url.rstrip(";,") for url in links if re.match(r"https?://ctni\.minsa\.gob\.pa/", url, re.I)))
    product = _http(row.get("enlace_producto_recomendado")) or _http(row.get("contacto_proveedor")) or _http(row.get("contacto_potencial"))
    decision = _evaluation(row)
    item.update(
        renglon=_line(row),
        oportunidad=_text(row.get("oportunidad")) or f"Renglón {_line(row) or '?'}: {_text(row.get('descripcion_renglon')) or _text(row.get('nombre_ficha'))}",
        proveedor_objetivo=_text(row.get("proveedor_objetivo")) or _text(row.get("proveedor_con_precio")) or _text(row.get("proveedor_potencial")),
        enlace_ficha_minsa=_http(row.get("enlace_ficha_minsa")) or (ctni[0] if len(ctni) == 1 else ""),
        enlace_producto_recomendado=product,
        pais_origen=_text(row.get("pais_origen")) or _text(row.get("pais")) or _text(row.get("pais_potencial")),
        analisis_cumplimiento_ficha=_text(row.get("analisis_cumplimiento_ficha")) or narrative,
        resultado_cumplimiento=_text(row.get("resultado_cumplimiento")) or "Pendiente de confirmar",
        viabilidad_economica=_text(row.get("viabilidad_economica")) or "Pendiente de confirmar costo puesto en Panamá y rentabilidad",
        que_falta=decision["que_falta"] or _note_section(notes, "Pendientes") or narrative or "Confirmar cumplimiento, cotización, disponibilidad y entrega",
        accion_inmediata=decision["accion_inmediata"] or _note_section(notes, "Próxima acción") or "Confirmar requisitos pendientes y solicitar cotización, stock y entrega en Panamá",
    )
    return item


def build_research_opportunities(research: pd.DataFrame | None, published: pd.DataFrame | None,
                                 current_acts: pd.DataFrame | None, *, now=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rebuild the current work queue from evidence, never from an old Top alone.

    New detailed research supersedes older executive claims for the SAME line.
    Price/stock/document gaps stay visible as pending. This does not generate
    supplier research, renew quotes, or write to the external researcher's sheets.
    """
    clock = _local_timestamp(now) if now is not None else pd.Timestamp.now(tz=PANAMA)
    if clock is None:
        raise ValueError("Fecha de evaluación inválida")
    top = latest_top10_snapshot(published)
    detailed = prepare_research_table(research, include_inactive=True) if research is not None else pd.DataFrame()
    latest = {}
    for row in reversed(detailed.to_dict("records")):
        latest[_key(row)] = row
    candidates = {}
    for row in top.to_dict("records"):
        candidates[_key(row)] = dict(row, origen_evaluacion="Top publicado")
    for key, row in latest.items():
        old = candidates.get(key)
        old_date = _local_timestamp(old.get("actualizado_en")) if old else None
        date = _local_timestamp(row.get("actualizado_en"))
        if old and date is not None and date == old_date:
            candidates[key] = {**old, **row, "origen_evaluacion": "Investigación y Top del mismo corte"}
        elif not old or old_date is None or date is None or date > old_date:
            # Do not inherit a stale margin, model, compliance or recommendation.
            candidates[key] = dict(row, origen_evaluacion="Investigación detallada")
    adapted = pd.DataFrame([_research_candidate(row) for row in candidates.values()])
    reviewed = assess_research_validity(adapted, current_acts, now=clock)
    if reviewed.empty:
        return reviewed, reviewed.copy()
    accepted, excluded = [], []
    for row in reviewed.to_dict("records"):
        blocker = _candidate_blocker(row)
        key = _key(row)
        if not all(key):
            blocker = blocker or "Falta identificar acto, ficha y renglón exactos"
        if research_column_key(_text(row.get("estado_investigacion"))) == "sin_proveedor_verificable":
            blocker = blocker or "Falta localizar un proveedor/producto concreto"
        if not _http(row.get("enlace_producto_recomendado")):
            blocker = blocker or "Falta un enlace al producto o proveedor localizado"
        if not _http(row.get("enlace_acto")):
            blocker = blocker or "Falta el enlace oficial al acto"
        if row["vigencia"] != "Vigente" or blocker:
            row["motivo_exclusion"] = row["motivo_vigencia"] if row["vigencia"] != "Vigente" else blocker
            excluded.append(row)
            continue
        decision = _evaluation(row)
        evidence = _local_timestamp(row.get("actualizado_en"))
        flags = ("cumplimiento_confirmado", "costo_puesto_confirmado", "stock_confirmado", "entrega_confirmada", "economia_viable")
        ready = (research_column_key(decision["situacion"] or _text(row.get("estado"))) == "lista_para_ofertar"
                 and all(research_column_key(decision[name]) in {"si", "true", "1"} for name in flags)
                 and research_column_key(decision["que_falta"]) in {"ninguno", "ninguna", "sin_pendientes"}
                 and bool(_http(row.get("enlace_ficha_minsa")))
                 and evidence is not None and pd.Timedelta(0) <= clock - evidence <= pd.Timedelta(hours=36))
        row["situacion"] = "Lista para ofertar" if ready else "Para cotizar o confirmar"
        if not _http(row.get("enlace_ficha_minsa")):
            row["que_falta"] = "Verificar enlace y ficha CTNI oficial. " + row["que_falta"]
        row["prioridad_publicada"] = row.get("ranking", 999) if row["origen_evaluacion"] != "Investigación detallada" else 999
        accepted.append(row)
    eligible = pd.DataFrame(accepted, columns=list(dict.fromkeys([*reviewed.columns, "situacion", "prioridad_publicada"])))
    if not eligible.empty:
        eligible["__ready"] = eligible["situacion"].eq("Lista para ofertar")
        eligible["__close"] = pd.to_datetime(eligible["cierre_verificado"], format="mixed", utc=True, errors="coerce")
        eligible["prioridad_publicada"] = pd.to_numeric(eligible["prioridad_publicada"], errors="coerce").fillna(999)
        eligible = eligible.sort_values(["__ready", "prioridad_publicada", "__close", "numero_acto", "ficha", "renglon"],
                                         ascending=[False, True, True, True, True, True], kind="stable")
        eligible = eligible.drop(columns=["__ready", "__close"]).reset_index(drop=True)
        eligible["ranking"] = range(1, len(eligible) + 1)
    return eligible, pd.DataFrame(excluded)


def research_health(research: pd.DataFrame, current_acts: pd.DataFrame | None, *, now=None) -> list[str]:
    """Small, actionable health messages. Refreshing the UI never renews evidence."""
    clock = _local_timestamp(now) if now is not None else pd.Timestamp.now(tz=PANAMA)
    issues = []
    updated = research_updated_at(research)
    if updated is None or clock - updated > pd.Timedelta(hours=36):
        issues.append("La investigación de proveedores lleva más de 36 horas sin actualizarse; revisar la corrida de ChatGPT. La vista no renueva cotizaciones.")
    if current_acts is None:
        issues.append("No se pudo comprobar la captura de actos; reintentar la lectura.")
    else:
        issues.extend(current_acts.attrs.get("source_errors", []))
        dates = current_acts.get("verificado_en", pd.Series(dtype=object)).map(_local_timestamp)
        stale = sum(value is None or clock - value > pd.Timedelta(hours=36) for value in dates)
        if stale:
            issues.append(f"{stale} actos tienen una captura de más de 36 horas o sin fecha. Revisar la última ejecución de CL abiertas, programadas y licitaciones en el orquestador.")
    return issues


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
