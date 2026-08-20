from __future__ import annotations

"""Genera un Top 20 estratégico de oportunidades médicas desde la capa analítica.

El informe usa la misma base analítica que consume Streamlit, recalcula la
unicidad de ficha por acto con el perfil moderado (score >= 90), cruza los
metadatos oficiales de MINSA/CTNI y produce exactamente cuatro hojas Excel.
"""

import argparse
import json
import math
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.worksheet.table import Table, TableStyleInfo


DETECTION_THRESHOLD = 90.0
EXCLUDED_FICHAS = {
    "107135",
    "101792",
    "104747",
    "107260",
    "22998",
    "108528",
    "22287",
    "103152",
    "107861",
}
SHEET_NAMES = (
    "1_Historicas",
    "2_Nuevas_Potencial",
    "3_Barrera_Cero",
    "4_Actos_Desiertos",
)

MEDICAL_MARKERS = (
    "medico quirurgico",
    "dispositivo medico",
    "laboratorio",
    "odontolog",
    "imagenolog",
    "radiolog",
    "instrumental medico",
    "especialidades medicas",
)
PHARMA_MARKERS = (
    "medicamento",
    "medicamentosos",
    "farmaceut",
    "nutricion",
    "nutricional",
)


@dataclass(frozen=True)
class InputPaths:
    analytics_db: Path
    ctni_db: Path


def clean_text(value: object) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return re.sub(r"\s+", " ", text)


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", clean_text(value).lower())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()


def normalize_ficha(value: object) -> str:
    match = re.search(r"\d+", clean_text(value))
    return (match.group(0).lstrip("0") or "0") if match else ""


def parse_iso_date(value: object) -> pd.Timestamp:
    return pd.to_datetime(clean_text(value), errors="coerce")


def _read_ctni_catalog(ctni_db: Path) -> pd.DataFrame:
    """Consolida el histórico CTNI por ficha y conserva el nombre oficial completo."""
    rows: list[dict[str, object]] = []
    with sqlite3.connect(ctni_db) as connection:
        payloads = connection.execute(
            "SELECT payload_json FROM records WHERE categoria = 'fichas'"
        ).fetchall()
    for (payload_raw,) in payloads:
        try:
            payload = json.loads(payload_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        ficha = normalize_ficha(payload.get("numero_ficha"))
        event_date = parse_iso_date(payload.get("fecha"))
        if not ficha or pd.isna(event_date):
            continue
        rows.append(
            {
                "ficha": ficha,
                "ctni_fecha": event_date,
                "ctni_accion": clean_text(payload.get("accion")),
                "ctni_producto": clean_text(payload.get("producto")),
                "ctni_subcomite": clean_text(payload.get("subcomite")),
                "ctni_enlace": clean_text(payload.get("enlace_oficial")),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "ficha",
                "ctni_primera_fecha",
                "ctni_ultima_fecha",
                "ctni_ultima_accion",
                "ctni_producto_oficial",
                "ctni_subcomite",
                "ctni_enlace",
            ]
        )
    events = pd.DataFrame(rows).sort_values(["ficha", "ctni_fecha"])
    first_dates = events.groupby("ficha", as_index=False)["ctni_fecha"].min().rename(
        columns={"ctni_fecha": "ctni_primera_fecha"}
    )
    latest = events.groupby("ficha", as_index=False).tail(1).rename(
        columns={
            "ctni_fecha": "ctni_ultima_fecha",
            "ctni_accion": "ctni_ultima_accion",
            "ctni_producto": "ctni_producto_oficial",
        }
    )
    latest = latest[
        [
            "ficha",
            "ctni_ultima_fecha",
            "ctni_ultima_accion",
            "ctni_producto_oficial",
            "ctni_subcomite",
            "ctni_enlace",
        ]
    ]
    return first_dates.merge(latest, on="ficha", how="left")


def _load_source(paths: InputPaths) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
    with sqlite3.connect(paths.analytics_db) as connection:
        facts = pd.read_sql_query(
            """
            SELECT *
            FROM intel_actos_fichas
            WHERE detection_score >= ?
            """,
            connection,
            params=(DETECTION_THRESHOLD,),
        )
        metadata = pd.read_sql_query("SELECT * FROM intel_ficha_metadata", connection)
        build_metadata = dict(
            connection.execute("SELECT key, value FROM intel_build_metadata").fetchall()
        )
    ctni = _read_ctni_catalog(paths.ctni_db)
    for frame in (facts, metadata, ctni):
        if "ficha" in frame.columns:
            frame["ficha"] = frame["ficha"].map(normalize_ficha)
    metadata = metadata.drop_duplicates("ficha", keep="last")
    metadata = metadata.merge(ctni, on="ficha", how="outer")
    return facts, metadata, build_metadata


def _derive_medical_scope(metadata: pd.DataFrame) -> pd.DataFrame:
    result = metadata.copy()
    for column in (
        "nombre_ficha",
        "descripcion",
        "area",
        "tipo_producto",
        "especialidad",
        "ctni_producto_oficial",
        "ctni_subcomite",
    ):
        if column not in result.columns:
            result[column] = ""
        result[column] = result[column].map(clean_text)

    result["descripcion_oficial"] = result.apply(
        lambda row: clean_text(row.get("ctni_producto_oficial"))
        or clean_text(row.get("nombre_ficha"))
        or clean_text(row.get("descripcion"))
        or f"Ficha {row.get('ficha', '')}",
        axis=1,
    )
    result["clasificacion_oficial"] = result.apply(
        lambda row: clean_text(row.get("ctni_subcomite"))
        or clean_text(row.get("area"))
        or clean_text(row.get("tipo_producto")),
        axis=1,
    )
    taxonomy = result.apply(
        lambda row: normalize_text(
            " ".join(
                clean_text(row.get(column))
                for column in (
                    "ctni_subcomite",
                    "area",
                    "tipo_producto",
                    "especialidad",
                    "ctni_producto_oficial",
                    "nombre_ficha",
                )
            )
        ),
        axis=1,
    )
    is_pharma = taxonomy.map(lambda text: any(marker in text for marker in PHARMA_MARKERS))
    is_medical = taxonomy.map(lambda text: any(marker in text for marker in MEDICAL_MARKERS))
    result["es_universo_medico"] = is_medical & ~is_pharma
    result["motivo_clasificacion"] = ""
    result.loc[is_medical & ~is_pharma, "motivo_clasificacion"] = "Taxonomía oficial médica"
    result.loc[is_pharma, "motivo_clasificacion"] = "Excluida: medicamento/nutrición"
    result.loc[~is_medical & ~is_pharma, "motivo_clasificacion"] = "Excluida: clasificación no confirmada"
    return result


def _prepare_facts(facts: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    result = facts.copy()
    result["ficha"] = result["ficha"].map(normalize_ficha)
    result["profile_ficha_count"] = result.groupby("acto_key")["ficha"].transform("nunique")
    result["es_ficha_unica"] = result["profile_ficha_count"].eq(1)
    for column in (
        "reference_amount_context",
        "award_amount_context",
        "participant_count",
        "detection_score",
    ):
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)
    for column in ("publication_date", "celebration_date", "award_date", "update_date"):
        result[column] = pd.to_datetime(result[column], errors="coerce")
    result["fecha_analisis"] = (
        result["publication_date"]
        .fillna(result["celebration_date"])
        .fillna(result["award_date"])
        .fillna(result["update_date"])
    )
    result["estado_norm"] = result["estado"].map(normalize_text)
    result["es_desierto"] = result["estado_norm"].str.contains("desiert", na=False)
    result["es_adjudicado"] = result["estado_norm"].str.contains("adjudic", na=False)
    result = result.merge(
        metadata[
            [
                "ficha",
                "descripcion_oficial",
                "clasificacion_oficial",
                "es_universo_medico",
                "tiene_ct",
                "registro_sanitario",
                "clase_riesgo",
                "enlace_minsa",
                "ctni_enlace",
                "ctni_primera_fecha",
                "ctni_ultima_fecha",
                "ctni_ultima_accion",
            ]
        ],
        on="ficha",
        how="left",
        validate="many_to_one",
    )
    return result[
        result["es_universo_medico"].fillna(False)
        & ~result["ficha"].isin(EXCLUDED_FICHAS)
    ].copy()


def _percentile(series: pd.Series, *, higher_is_better: bool = True) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if values.empty:
        return values
    if values.nunique(dropna=False) <= 1:
        return pd.Series(50.0, index=values.index)
    ranked = values.rank(method="average", pct=True) * 100.0
    return ranked if higher_is_better else 100.0 - ranked + (100.0 / len(values))


def _competition_label(median_participants: float, avg_participants: float) -> str:
    reference = median_participants if median_participants > 0 else avg_participants
    if reference <= 0:
        return "Sin datos de proponentes"
    if reference <= 1.25:
        level = "Baja"
    elif reference <= 2.5:
        level = "Media"
    else:
        level = "Alta"
    return f"{level} (mediana {median_participants:.1f}; promedio {avg_participants:.1f})"


def _requirements(ct_value: object, rs_value: object, risk_class: object) -> str:
    ct = clean_text(ct_value) or "Sin dato"
    rs = clean_text(rs_value) or "Sin dato"
    risk = clean_text(risk_class) or "Sin clase"
    return f"CT: {ct} | RS: {rs} | Clase: {risk}"


def _aggregate(facts: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ficha, group in facts.groupby("ficha", sort=False):
        positive_participants = group.loc[group["participant_count"] > 0, "participant_count"]
        acts = int(group["acto_key"].nunique())
        unique_group = group[group["es_ficha_unica"]]
        adjudicated_acts = int(group.loc[group["es_adjudicado"], "acto_key"].nunique())
        deserted_acts = int(group.loc[group["es_desierto"], "acto_key"].nunique())
        first = group.iloc[0]
        total_amount = float(group["reference_amount_context"].clip(lower=0).sum())
        unique_amount = float(unique_group["reference_amount_context"].clip(lower=0).sum())
        award_amount = float(group["award_amount_context"].clip(lower=0).sum())
        avg_participants = float(positive_participants.mean()) if not positive_participants.empty else 0.0
        median_participants = float(positive_participants.median()) if not positive_participants.empty else 0.0
        rows.append(
            {
                "ficha": ficha,
                "descripcion_oficial": clean_text(first.get("descripcion_oficial")),
                "clasificacion_oficial": clean_text(first.get("clasificacion_oficial")),
                "actos": acts,
                "actos_ficha_unica": int(unique_group["acto_key"].nunique()),
                "monto_total": total_amount,
                "monto_ficha_unica": unique_amount,
                "monto_adjudicado": award_amount,
                "promedio_por_acto": total_amount / acts if acts else 0.0,
                "meses_activos": int(group["fecha_analisis"].dt.to_period("M").nunique()),
                "primera_fecha": group["fecha_analisis"].min(),
                "ultima_fecha": group["fecha_analisis"].max(),
                "actos_adjudicados": adjudicated_acts,
                "actos_desiertos": deserted_acts,
                "pct_adjudicado": adjudicated_acts / acts if acts else 0.0,
                "pct_desierto": deserted_acts / acts if acts else 0.0,
                "participantes_promedio": avg_participants,
                "participantes_mediana": median_participants,
                "competencia": _competition_label(median_participants, avg_participants),
                "estatus_adjudicacion": (
                    f"Adjudicados {adjudicated_acts}/{acts} ({adjudicated_acts / acts:.1%}); "
                    f"desiertos {deserted_acts}/{acts} ({deserted_acts / acts:.1%})"
                    if acts
                    else "Sin actos"
                ),
                "tiene_ct": clean_text(first.get("tiene_ct")),
                "registro_sanitario": clean_text(first.get("registro_sanitario")),
                "clase_riesgo": clean_text(first.get("clase_riesgo")),
                "requisitos": _requirements(
                    first.get("tiene_ct"), first.get("registro_sanitario"), first.get("clase_riesgo")
                ),
                "enlace_minsa": clean_text(first.get("enlace_minsa"))
                or clean_text(first.get("ctni_enlace")),
                "ctni_primera_fecha": first.get("ctni_primera_fecha"),
                "ctni_ultima_fecha": first.get("ctni_ultima_fecha"),
                "ctni_ultima_accion": clean_text(first.get("ctni_ultima_accion")),
                "confianza_deteccion": float(group["detection_score"].mean()),
            }
        )
    return pd.DataFrame(rows)


def _score_historical(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["score"] = (
        _percentile(result["actos"]) * 0.30
        + _percentile(result["actos_ficha_unica"]) * 0.20
        + _percentile(result["monto_ficha_unica"]) * 0.25
        + _percentile(result["monto_total"]) * 0.15
        + _percentile(result["meses_activos"]) * 0.10
    )
    return result.sort_values(["score", "monto_ficha_unica", "actos"], ascending=False)


def _score_new(facts: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    # Una ficha nueva no puede heredar actos anteriores a su creación. Las
    # coincidencias semánticas históricas son útiles para otros análisis, pero
    # aquí inflarían artificialmente la adopción real del código recién creado.
    eligible_facts = facts[
        facts["ctni_primera_fecha"].notna()
        & (facts["ctni_primera_fecha"] >= cutoff)
        & facts["fecha_analisis"].notna()
        & (facts["fecha_analisis"] >= facts["ctni_primera_fecha"])
    ].copy()
    result = _aggregate(eligible_facts)
    result = result[
        (result["actos"] >= 3)
        & (result["actos_ficha_unica"] >= 1)
        & (result["monto_ficha_unica"] > 0)
    ].copy()
    age_months = ((pd.Timestamp.today().normalize() - result["ctni_primera_fecha"]).dt.days / 30.44).clip(lower=1)
    result["dinamismo_mensual"] = result["actos"] / age_months
    result["score"] = (
        _percentile(result["actos"]) * 0.25
        + _percentile(result["monto_ficha_unica"]) * 0.25
        + _percentile(result["monto_total"]) * 0.15
        + _percentile(result["participantes_mediana"], higher_is_better=False) * 0.20
        + _percentile(result["dinamismo_mensual"]) * 0.15
    )
    return result.sort_values(["score", "monto_ficha_unica", "actos"], ascending=False)


def _score_barrier_zero(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame[
        frame["tiene_ct"].map(normalize_text).eq("no")
        & frame["registro_sanitario"].map(normalize_text).eq("no")
    ].copy()
    result["score"] = (
        _percentile(result["actos"]) * 0.30
        + _percentile(result["actos_ficha_unica"]) * 0.20
        + _percentile(result["monto_ficha_unica"]) * 0.30
        + _percentile(result["monto_total"]) * 0.10
        + _percentile(result["participantes_mediana"], higher_is_better=False) * 0.10
    )
    return result.sort_values(["score", "monto_ficha_unica", "actos"], ascending=False)


def _score_deserted(facts: pd.DataFrame) -> pd.DataFrame:
    deserted = facts[facts["es_desierto"]].copy()
    result = _aggregate(deserted)
    result = result[result["actos"] >= 2].copy()
    result["score"] = (
        _percentile(result["actos"]) * 0.45
        + _percentile(result["monto_total"]) * 0.35
        + _percentile(result["actos_ficha_unica"]) * 0.10
        + _percentile(result["meses_activos"]) * 0.10
    )
    return result.sort_values(["score", "monto_total", "actos"], ascending=False)


def _reason(row: pd.Series, category: str, rank: int) -> str:
    prefix = "TOP 5: " if rank <= 5 else ""
    if category == "historical":
        return (
            f"{prefix}{int(row['actos'])} actos en {int(row['meses_activos'])} meses activos; "
            f"USD {row['monto_ficha_unica']:,.2f} en actos de ficha única y "
            f"USD {row['monto_total']:,.2f} de mercado relacionado."
        )
    if category == "new":
        event_date = pd.to_datetime(row.get("ctni_primera_fecha"), errors="coerce")
        event_label = event_date.strftime("%Y-%m-%d") if pd.notna(event_date) else "sin fecha"
        return (
            f"{prefix}Ficha reciente desde {event_label}; {int(row['actos'])} actos, "
            f"USD {row['monto_ficha_unica']:,.2f} de ficha única y competencia {row['competencia'].lower()}."
        )
    if category == "barrier":
        return (
            f"{prefix}Entrada regulatoria directa (CT No y RS No), {int(row['actos'])} actos y "
            f"USD {row['monto_ficha_unica']:,.2f} en demanda de ficha única."
        )
    return (
        f"{prefix}{int(row['actos'])} actos desiertos por USD {row['monto_total']:,.2f}; "
        f"señal de demanda no satisfecha recurrente en {int(row['meses_activos'])} meses."
    )


def _to_export(frame: pd.DataFrame, category: str, top_n: int = 20) -> pd.DataFrame:
    top = frame.head(top_n).copy().reset_index(drop=True)
    top.insert(0, "Ranking", range(1, len(top) + 1))
    top["Prioridad"] = top["Ranking"].map(lambda value: "⭐ TOP 5" if value <= 5 else "Top 20")
    top["Por qué destaca"] = [
        _reason(row, category, int(row["Ranking"])) for _, row in top.iterrows()
    ]
    if category == "new":
        top["Fecha publicación/creación"] = pd.to_datetime(
            top["ctni_primera_fecha"], errors="coerce"
        ).dt.date
        top["Acción CTNI más reciente"] = top["ctni_ultima_accion"]
    columns = [
        "Ranking",
        "Prioridad",
        "ficha",
        "descripcion_oficial",
    ]
    if category == "new":
        columns.extend(["Fecha publicación/creación", "Acción CTNI más reciente"])
    columns.extend(
        [
            "actos",
            "actos_ficha_unica",
            "monto_total",
            "monto_ficha_unica",
            "promedio_por_acto",
            "competencia",
            "participantes_promedio",
            "participantes_mediana",
            "estatus_adjudicacion",
            "requisitos",
            "clasificacion_oficial",
            "clase_riesgo",
            "score",
            "Por qué destaca",
            "enlace_minsa",
        ]
    )
    exported = top[columns].rename(
        columns={
            "ficha": "Código de Ficha",
            "descripcion_oficial": "Descripción Oficial",
            "actos": "Cantidad de Actos",
            "actos_ficha_unica": "Actos de Ficha Única",
            "monto_total": "Monto Total Acumulado (USD)",
            "monto_ficha_unica": "Monto Ficha Única (USD)",
            "promedio_por_acto": "Promedio por Acto (USD)",
            "competencia": "Nivel de Competencia",
            "participantes_promedio": "Proponentes Promedio",
            "participantes_mediana": "Proponentes Mediana",
            "estatus_adjudicacion": "Estatus de Adjudicación",
            "requisitos": "Requisitos Exigidos",
            "clasificacion_oficial": "Clasificación Oficial",
            "clase_riesgo": "Clase de Riesgo",
            "score": "Score Estratégico",
            "enlace_minsa": "Enlace MINSA",
        }
    )
    return exported


def _validate_rankings(
    exports: dict[str, pd.DataFrame],
    new_cutoff: pd.Timestamp,
) -> list[str]:
    checks: list[str] = []
    for sheet_name, frame in exports.items():
        assert len(frame) == 20, f"{sheet_name}: se esperaban 20 filas y hay {len(frame)}"
        assert frame["Código de Ficha"].nunique() == 20, f"{sheet_name}: fichas duplicadas"
        assert not set(frame["Código de Ficha"]) & EXCLUDED_FICHAS, f"{sheet_name}: exclusión fallida"
        assert frame["Score Estratégico"].is_monotonic_decreasing, f"{sheet_name}: score desordenado"
        assert (frame["Monto Total Acumulado (USD)"] >= 0).all(), f"{sheet_name}: monto negativo"
        checks.append(f"{sheet_name}: 20 fichas únicas, orden y exclusiones correctos")
    barrier = exports["3_Barrera_Cero"]
    assert barrier["Requisitos Exigidos"].str.contains(r"CT: No \| RS: No", regex=True).all()
    checks.append("3_Barrera_Cero: CT=No y RS=No confirmado en las 20 filas")
    new = exports["2_Nuevas_Potencial"]
    new_dates = pd.to_datetime(new["Fecha publicación/creación"], errors="coerce")
    assert new_dates.notna().all() and (new_dates >= new_cutoff).all()
    checks.append(f"2_Nuevas_Potencial: las 20 fichas son posteriores a {new_cutoff.date()}")
    deserted = exports["4_Actos_Desiertos"]
    assert deserted["Estatus de Adjudicación"].str.contains("desiertos", case=False).all()
    checks.append("4_Actos_Desiertos: universo construido exclusivamente con actos desiertos")
    return checks


def _style_excel(path: Path, built_at: str) -> None:
    workbook = load_workbook(path)
    header_fill = PatternFill("solid", fgColor="17365D")
    header_font = Font(color="FFFFFF", bold=True)
    top_fill = PatternFill("solid", fgColor="FFF2CC")
    top_font = Font(color="7F6000", bold=True)
    money_headers = {
        "Monto Total Acumulado (USD)",
        "Monto Ficha Única (USD)",
        "Promedio por Acto (USD)",
    }
    comments = {
        "Monto Total Acumulado (USD)": (
            "Suma del precio de referencia completo de cada acto donde aparece la ficha. "
            "Es una medida del tamaño del mercado relacionado y puede incluir otros renglones/fichas."
        ),
        "Monto Ficha Única (USD)": (
            "Suma del precio de referencia únicamente en actos que contienen una sola ficha técnica distinta. "
            "Es la medida monetaria más confiable para atribuir demanda a la ficha."
        ),
        "Score Estratégico": (
            "Percentil ponderado específico de cada categoría. No es una garantía de adjudicación ni utilidad."
        ),
    }
    for index, sheet_name in enumerate(SHEET_NAMES, start=1):
        worksheet = workbook[sheet_name]
        worksheet.freeze_panes = "A2"
        worksheet.sheet_view.showGridLines = False
        worksheet.auto_filter.ref = worksheet.dimensions
        for cell in worksheet[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            if cell.value in comments:
                cell.comment = Comment(comments[cell.value], "Codex")
        worksheet.row_dimensions[1].height = 40
        if worksheet.max_row >= 2:
            table = Table(displayName=f"TopOportunidades{index}", ref=worksheet.dimensions)
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False,
            )
            worksheet.add_table(table)
        headers = {cell.value: cell.column for cell in worksheet[1]}
        for row in range(2, worksheet.max_row + 1):
            rank = worksheet.cell(row, headers["Ranking"]).value
            if isinstance(rank, int) and rank <= 5:
                for cell in worksheet[row]:
                    cell.fill = top_fill
                worksheet.cell(row, headers["Prioridad"]).font = top_font
            worksheet.row_dimensions[row].height = 48
            for cell in worksheet[row]:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
        for header in money_headers:
            if header in headers:
                for row in range(2, worksheet.max_row + 1):
                    worksheet.cell(row, headers[header]).number_format = '$#,##0.00'
        for header in ("Cantidad de Actos", "Actos de Ficha Única"):
            if header in headers:
                for row in range(2, worksheet.max_row + 1):
                    worksheet.cell(row, headers[header]).number_format = '#,##0'
        if "Score Estratégico" in headers:
            for row in range(2, worksheet.max_row + 1):
                worksheet.cell(row, headers["Score Estratégico"]).number_format = '0.0'
        if "Enlace MINSA" in headers:
            column = headers["Enlace MINSA"]
            for row in range(2, worksheet.max_row + 1):
                cell = worksheet.cell(row, column)
                url = clean_text(cell.value)
                if url.startswith("http"):
                    cell.hyperlink = url
                    cell.value = "Abrir ficha oficial"
                    cell.style = "Hyperlink"
        for column_cells in worksheet.columns:
            header = clean_text(column_cells[0].value)
            values = [clean_text(cell.value) for cell in column_cells[: min(21, len(column_cells))]]
            width = min(max(max((len(value) for value in values), default=0) + 2, 11), 58)
            if header in {"Descripción Oficial", "Por qué destaca", "Estatus de Adjudicación"}:
                width = 52
            elif header in {"Nivel de Competencia", "Requisitos Exigidos", "Clasificación Oficial"}:
                width = 30
            worksheet.column_dimensions[column_cells[0].column_letter].width = width
        worksheet.oddFooter.center.text = f"Fuente analítica construida: {built_at} | Perfil de detección ≥ {DETECTION_THRESHOLD:.0f}"
        worksheet.oddFooter.right.text = "Página &P de &N"
        worksheet.sheet_properties.pageSetUpPr.fitToPage = True
        worksheet.page_setup.fitToWidth = 1
        worksheet.page_setup.fitToHeight = 0
        worksheet.sheet_view.zoomScale = 80
    workbook.properties.title = "Top de oportunidades médicas en Panamá Compra"
    workbook.properties.subject = "Minería estratégica de fichas técnicas médicas"
    workbook.properties.creator = "GEAPP / Codex"
    workbook.save(path)


def generate_report(paths: InputPaths, output_path: Path) -> tuple[dict[str, pd.DataFrame], list[str]]:
    facts, metadata_raw, build_metadata = _load_source(paths)
    metadata = _derive_medical_scope(metadata_raw)
    scoped_facts = _prepare_facts(facts, metadata)
    aggregate = _aggregate(scoped_facts)
    cutoff = pd.Timestamp(date.today() - timedelta(days=730))

    rankings = {
        "1_Historicas": _score_historical(aggregate),
        "2_Nuevas_Potencial": _score_new(scoped_facts, cutoff),
        "3_Barrera_Cero": _score_barrier_zero(aggregate),
        "4_Actos_Desiertos": _score_deserted(scoped_facts),
    }
    categories = {
        "1_Historicas": "historical",
        "2_Nuevas_Potencial": "new",
        "3_Barrera_Cero": "barrier",
        "4_Actos_Desiertos": "deserted",
    }
    exports = {
        sheet: _to_export(rankings[sheet], categories[sheet]) for sheet in SHEET_NAMES
    }
    checks = _validate_rankings(exports, cutoff)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name in SHEET_NAMES:
            exports[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
    _style_excel(output_path, build_metadata.get("built_at_utc", "sin dato"))

    workbook = load_workbook(output_path, read_only=False, data_only=False)
    assert workbook.sheetnames == list(SHEET_NAMES), "El libro debe contener exactamente cuatro hojas"
    for sheet_name in SHEET_NAMES:
        worksheet = workbook[sheet_name]
        assert worksheet.max_row == 21, f"{sheet_name}: filas Excel inesperadas"
        assert len(worksheet.tables) == 1, f"{sheet_name}: falta la tabla con filtros"
        assert worksheet.freeze_panes == "A2", f"{sheet_name}: panel no congelado"
    workbook.close()
    checks.append("Excel reabierto: 4 hojas, 20 filas por hoja, filtros y paneles congelados verificados")
    checks.append(
        f"Cobertura fuente: {len(facts):,} relaciones moderadas; {len(scoped_facts):,} médicas elegibles; "
        f"corte analítico {build_metadata.get('built_at_utc', 'sin dato')}"
    )
    return exports, checks


def parse_args() -> argparse.Namespace:
    home = Path.home()
    default_output = home / "Downloads" / f"Top_Oportunidades_Medicas_{date.today().isoformat()}.xlsx"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--analytics-db",
        type=Path,
        default=home / "scrapers_repo" / "data" / "db" / "inteligencia_proveedores.db",
    )
    parser.add_argument(
        "--ctni-db",
        type=Path,
        default=home / "scrapers_repo" / "data" / "ctni" / "ctni_monitor.db",
    )
    parser.add_argument("--output", type=Path, default=default_output)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = InputPaths(args.analytics_db.resolve(), args.ctni_db.resolve())
    for path in (paths.analytics_db, paths.ctni_db):
        if not path.exists():
            raise FileNotFoundError(f"No existe la fuente requerida: {path}")
    exports, checks = generate_report(paths, args.output.resolve())
    print(f"Excel generado: {args.output.resolve()}")
    for check in checks:
        print(f"[OK] {check}")
    for sheet_name, frame in exports.items():
        preview = ", ".join(
            f"{row['Ranking']}. {row['Código de Ficha']} ({row['Score Estratégico']:.1f})"
            for _, row in frame.head(5).iterrows()
        )
        print(f"[TOP5] {sheet_name}: {preview}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
