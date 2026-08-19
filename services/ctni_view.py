"""Transformaciones puras para las tres vistas CTNI de Panamá Compra."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from typing import Iterable
from urllib.parse import urljoin, urlparse

import pandas as pd


CTNI_BASE_URL = "https://ctni.minsa.gob.pa"
_SPANISH_MONTHS = {
    "enero": "01",
    "febrero": "02",
    "marzo": "03",
    "abril": "04",
    "mayo": "05",
    "junio": "06",
    "julio": "07",
    "agosto": "08",
    "septiembre": "09",
    "setiembre": "09",
    "octubre": "10",
    "noviembre": "11",
    "diciembre": "12",
}


@dataclass(frozen=True)
class CtniViewSpec:
    title: str
    sheet: str
    columns: tuple[tuple[str, str], ...]
    link_columns: tuple[str, ...] = ()


CTNI_VIEWS = {
    "Solicitudes de fichas": CtniViewSpec(
        title="Solicitudes de fichas",
        sheet="ctni_solicitudes",
        columns=(
            ("fecha", "Fecha de solicitud"),
            ("producto", "Producto"),
            ("numero_formulario", "N.º formulario"),
            ("tipo", "Tipo"),
            ("numero_ficha", "Ficha"),
            ("subcomite", "Subcomité"),
            ("institucion", "Institución"),
            ("estado", "Estado"),
            ("enlace_oficial", "Enlace oficial"),
            ("primera_deteccion", "Primera detección"),
            ("condicion", "Condición"),
        ),
        link_columns=("Enlace oficial",),
    ),
    "Homologaciones": CtniViewSpec(
        title="Homologaciones",
        sheet="ctni_homologaciones",
        columns=(
            ("fecha", "Fecha de homologación"),
            ("hora", "Hora"),
            ("producto", "Producto / aviso"),
            ("numero_formulario", "N.º formulario"),
            ("numero_ficha", "Ficha"),
            ("subcomite", "Subcomité"),
            ("tipo_evento", "Tipo de evento"),
            ("estado", "Estado"),
            ("enlace_adjunto", "Documento"),
            ("enlace_oficial", "Enlace oficial"),
            ("primera_deteccion", "Primera detección"),
            ("condicion", "Condición"),
        ),
        link_columns=("Documento", "Enlace oficial"),
    ),
    "Fichas nuevas": CtniViewSpec(
        title="Fichas nuevas",
        sheet="ctni_fichas",
        columns=(
            ("fecha", "Fecha de creación/modificación"),
            ("producto", "Producto"),
            ("numero_ficha", "Ficha"),
            ("clase_riesgo", "Clase de riesgo"),
            ("accion", "Acción"),
            ("acta", "Acta"),
            ("subcomite", "Subcomité"),
            ("estado", "Estado"),
            ("confirmacion_publicada", "Comprobación secundaria"),
            ("enlace_oficial", "Enlace oficial"),
            ("primera_deteccion", "Primera detección"),
            ("condicion", "Condición"),
        ),
        link_columns=("Enlace oficial",),
    ),
}


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _plain_text(value: object) -> str:
    raw = re.sub(r"\s+", " ", _safe_text(value)).strip().lower()
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", raw)
        if not unicodedata.combining(char)
    )


def normalize_ficha_number(value: object) -> str:
    """Normaliza un número de ficha sin alterar el resto del registro."""
    digits = re.sub(r"\D+", "", _safe_text(value))
    if not digits:
        return ""
    return digits.lstrip("0") or "0"


def normalize_risk_class(value: object) -> str:
    """Devuelve exclusivamente la clase de riesgo oficial A, B, C o D.

    No se reutilizan columnas de clase comercial, grupo o categoría. Esto evita
    presentar como clase de riesgo una clasificación distinta a la publicada en
    el detalle oficial de la ficha técnica de MINSA.
    """
    normalized = _plain_text(value).upper()
    if normalized in {"A", "B", "C", "D"}:
        return normalized
    for pattern in (
        r"\bCLASE\s+DE\s+RIESGO\s*[:\-]?\s*([ABCD])\b",
        r"\bCLASE\s+RIESGO\s*[:\-]?\s*([ABCD])\b",
        r"\bCLASE\s*[:\-]?\s*([ABCD])\b",
    ):
        match = re.search(pattern, normalized)
        if match:
            return match.group(1)
    return ""


def is_medication_record(row: pd.Series | dict[str, object]) -> bool:
    """Identifica medicamentos con la taxonomía CTNI, no por texto ambiguo."""
    values = [
        row.get(column, "")
        for column in (
            "es_medicamento",
            "area",
            "grupo",
            "subgrupo",
            "especialidad",
            "categoria",
            "producto",
        )
    ]
    normalized = " ".join(_plain_text(value) for value in values if _safe_text(value))
    if not normalized:
        return False
    if _plain_text(row.get("es_medicamento", "")) in {"si", "true", "1", "x"}:
        return True
    return any(token in normalized for token in ("medicamento", "farmaceut"))


def enrich_new_fichas(
    frame: pd.DataFrame,
    metadata_by_ficha: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Completa clase y taxonomía CTNI sin modificar los datos publicados.

    CTNI puede publicar una ficha trabajada antes de que tenga metadata oficial
    completa. En ese caso la fila se conserva como ``Sin clase asignada``: no se
    infiere una clase y el usuario puede verla o filtrarla explícitamente.
    """
    if frame.empty:
        return frame.copy()

    metadata_by_ficha = metadata_by_ficha or {}
    output = frame.copy()
    metadata_columns = ("area", "grupo", "subgrupo", "especialidad", "categoria")
    classes: list[str] = []
    medication_flags: list[str] = []
    enriched_metadata: dict[str, list[str]] = {column: [] for column in metadata_columns}

    for _, row in output.iterrows():
        ficha = normalize_ficha_number(row.get("numero_ficha"))
        metadata = metadata_by_ficha.get(ficha, {}) if ficha else {}
        existing_class = normalize_risk_class(row.get("clase_riesgo"))
        if not existing_class:
            existing_class = normalize_risk_class(row.get("clase"))
        metadata_class = normalize_risk_class(metadata.get("clase_riesgo"))
        if not metadata_class:
            metadata_class = normalize_risk_class(metadata.get("clase"))
        resolved_class = existing_class or metadata_class
        classes.append(resolved_class or "Sin clase asignada")

        row_data = dict(row)
        for column in metadata_columns:
            value = _safe_text(row.get(column)) or _safe_text(metadata.get(column))
            enriched_metadata[column].append(value)
            row_data[column] = value
        row_data["es_medicamento"] = _safe_text(row.get("es_medicamento")) or _safe_text(
            metadata.get("es_medicamento")
        )
        medication_flags.append("Si" if is_medication_record(row_data) else "No")

    output["clase_riesgo"] = classes
    # Alias transitorio para sesiones de Streamlit que todavía tengan el
    # esquema anterior en memoria. La vista oficial utiliza clase_riesgo.
    output["clase"] = classes
    output["es_medicamento"] = medication_flags
    for column, values in enriched_metadata.items():
        output[column] = values
    return output


def ctni_date_series(frame: pd.DataFrame, column: str = "fecha") -> pd.Series:
    """Convierte fechas CTNI, incluyendo meses escritos en español."""
    if column not in frame.columns:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")

    raw = frame[column].fillna("").astype(str).str.strip()
    parsed = pd.to_datetime(raw, errors="coerce", format="mixed", dayfirst=True)
    missing = parsed.isna() & raw.ne("")
    if missing.any():
        translated = raw.loc[missing].map(_plain_text)
        for month, number in _SPANISH_MONTHS.items():
            translated = translated.str.replace(
                rf"\b{re.escape(month)}\b",
                number,
                regex=True,
            )
        parsed.loc[missing] = pd.to_datetime(
            translated,
            errors="coerce",
            dayfirst=True,
        )
    return parsed


def latest_recent_ficha_events(
    frame: pd.DataFrame,
    *,
    as_of: date | None = None,
    years: int = 2,
) -> pd.DataFrame:
    """Consolida la novedad CTNI mas reciente de cada ficha.

    La hoja ``ctni_fichas`` conserva el historial completo (elaboraciones,
    actualizaciones, correcciones y cambios de habilitacion). Para el radar de
    demanda necesitamos una sola fila vigente por ficha y una fecha inequívoca
    desde la cual contar actos publicos posteriores.
    """
    if frame.empty or "numero_ficha" not in frame.columns:
        return frame.iloc[0:0].copy()

    reference_date = pd.Timestamp(as_of or date.today()).normalize()
    cutoff = reference_date - pd.DateOffset(years=max(1, int(years)))
    output = frame.copy()
    output["numero_ficha"] = output["numero_ficha"].map(normalize_ficha_number)
    output["fecha_ctni"] = ctni_date_series(output)
    output = output[
        output["numero_ficha"].ne("")
        & output["fecha_ctni"].notna()
        & output["fecha_ctni"].between(cutoff, reference_date, inclusive="both")
    ].copy()
    if output.empty:
        return output.reset_index(drop=True)

    tie_breakers = ["fecha_ctni"]
    if "primera_deteccion" in output.columns:
        output["__primera_deteccion"] = pd.to_datetime(
            output["primera_deteccion"], errors="coerce", format="mixed"
        )
        tie_breakers.append("__primera_deteccion")
    output = output.sort_values(
        tie_breakers,
        ascending=[False] * len(tie_breakers),
        na_position="last",
        kind="stable",
    ).drop_duplicates("numero_ficha", keep="first")
    output["fecha_ctni_iso"] = output["fecha_ctni"].dt.date.astype(str)
    return output.drop(columns=["__primera_deteccion"], errors="ignore").reset_index(drop=True)


def merge_recent_ficha_demand(
    ficha_events: pd.DataFrame,
    act_rows: pd.DataFrame,
) -> pd.DataFrame:
    """Agrega actos y montos posteriores a la fecha CTNI de cada ficha.

    Cada acto se cuenta una sola vez por ficha aunque la ficha aparezca en
    varios renglones. El monto utilizado es el precio de referencia completo
    del acto (contexto), tambien sumado una sola vez.
    """
    output = ficha_events.copy()
    if output.empty:
        output["actos_asociados"] = pd.Series(dtype="int64")
        output["monto_asociado"] = pd.Series(dtype="float64")
        return output

    output["numero_ficha"] = output["numero_ficha"].map(normalize_ficha_number)
    output["fecha_ctni"] = pd.to_datetime(output.get("fecha_ctni"), errors="coerce")
    if act_rows.empty:
        output["actos_asociados"] = 0
        output["monto_asociado"] = 0.0
        return output

    acts = act_rows.copy()
    acts["ficha"] = acts.get("ficha", "").map(normalize_ficha_number)
    acts["acto_key"] = acts.get("acto_key", "").fillna("").astype(str).str.strip()
    acts["fecha_acto"] = pd.to_datetime(acts.get("fecha_acto"), errors="coerce")
    acts["monto_contexto"] = pd.to_numeric(
        acts.get("monto_contexto", 0.0), errors="coerce"
    ).fillna(0.0)

    event_dates = output.set_index("numero_ficha")["fecha_ctni"].to_dict()
    acts["fecha_ctni"] = acts["ficha"].map(event_dates)
    acts = acts[
        acts["ficha"].ne("")
        & acts["acto_key"].ne("")
        & acts["fecha_acto"].notna()
        & acts["fecha_ctni"].notna()
        & (acts["fecha_acto"] >= acts["fecha_ctni"])
    ].copy()
    if acts.empty:
        output["actos_asociados"] = 0
        output["monto_asociado"] = 0.0
        return output

    # Cuando una fuente repite el mismo acto/renglon conservamos el mayor
    # contexto monetario informado y nunca duplicamos el acto.
    acts = acts.sort_values("monto_contexto", ascending=False, kind="stable")
    acts = acts.drop_duplicates(["ficha", "acto_key"], keep="first")
    totals = (
        acts.groupby("ficha", as_index=False)
        .agg(
            actos_asociados=("acto_key", "nunique"),
            monto_asociado=("monto_contexto", "sum"),
        )
        .rename(columns={"ficha": "numero_ficha"})
    )
    output = output.merge(totals, on="numero_ficha", how="left")
    output["actos_asociados"] = output["actos_asociados"].fillna(0).astype(int)
    output["monto_asociado"] = output["monto_asociado"].fillna(0.0).astype(float)
    return output


def _official_ctni_url(value: object) -> str:
    """Devuelve solo enlaces HTTP(S) oficiales y corrige valores relativos/formula."""
    raw = _safe_text(value)
    if not raw or raw.lower() in {"nan", "none", "<na>"}:
        return ""
    formula = re.match(r'^=HYPERLINK\(\s*"([^"]+)"', raw, flags=re.IGNORECASE)
    if formula:
        raw = formula.group(1)
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/"):
        raw = urljoin(f"{CTNI_BASE_URL}/", raw.lstrip("/"))
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        return ""
    if parsed.hostname and parsed.hostname.lower() != "ctni.minsa.gob.pa":
        return ""
    return raw


def _official_id(value: object) -> str:
    match = re.search(r"\d+", _safe_text(value))
    return match.group(0) if match else ""


def _request_type_id(value: object) -> str:
    normalized = _plain_text(value)
    if "actualizacion" in normalized:
        return "1"
    if "medicamento" in normalized:
        return "3"
    if "elaboracion" in normalized:
        return "2"
    return ""


def _repair_official_links(frame: pd.DataFrame, view_name: str) -> pd.DataFrame:
    """Transforma endpoints JSON/genéricos en destinos oficiales navegables."""
    output = frame.copy()
    if "enlace_oficial" not in output.columns:
        output["enlace_oficial"] = ""

    repaired: list[str] = []
    for _index, row in output.iterrows():
        current = _official_ctni_url(row.get("enlace_oficial"))
        official_id = _official_id(row.get("id_oficial"))
        if view_name == "Solicitudes de fichas":
            form_type = _request_type_id(row.get("tipo"))
            if official_id and form_type:
                current = (
                    f"{CTNI_BASE_URL}/Utilities/GenerateFormulario"
                    f"?IdFormulario={official_id}&IdTipoFormulario={form_type}"
                )
            elif not current or "/FormularioInfo" in current:
                current = f"{CTNI_BASE_URL}/Formularios/Estado"
        elif view_name == "Fichas nuevas":
            if official_id:
                current = (
                    f"{CTNI_BASE_URL}/Utilities/LoadFicha/"
                    f"?idficha={official_id}&idparam=0"
                )
            elif not current:
                current = f"{CTNI_BASE_URL}/Home/ConsultarFichas"
        elif not current:
            current = CTNI_BASE_URL
        repaired.append(current)
    output["enlace_oficial"] = repaired

    if "enlace_adjunto" in output.columns:
        output["enlace_adjunto"] = output["enlace_adjunto"].map(_official_ctni_url)
    return output


def available_values(frame: pd.DataFrame, column: str) -> list[str]:
    if column not in frame.columns:
        return []
    values = {
        str(value).strip()
        for value in frame[column].fillna("").tolist()
        if str(value).strip()
    }
    return sorted(values, key=_plain_text)


def filter_ctni_records(
    frame: pd.DataFrame,
    *,
    search: str = "",
    states: Iterable[str] = (),
    subcommittees: Iterable[str] = (),
    conditions: Iterable[str] = (),
    actions: Iterable[str] = (),
    classes: Iterable[str] = (),
    exclude_medications: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    output = frame.copy()

    terms = [_plain_text(term) for term in str(search or "").split(",") if _plain_text(term)]
    if terms:
        searchable = output.fillna("").astype(str).agg(" ".join, axis=1).map(_plain_text)
        output = output[searchable.map(lambda text: any(term in text for term in terms))]

    for column, selected in (
        ("estado", list(states)),
        ("subcomite", list(subcommittees)),
        ("condicion", list(conditions)),
        ("accion", list(actions)),
        ("clase_riesgo", list(classes or ())),
    ):
        if selected and column in output.columns:
            output = output[output[column].fillna("").astype(str).isin(selected)]

    if exclude_medications and "es_medicamento" in output.columns:
        medication_labels = output["es_medicamento"].map(_plain_text)
        output = output[~medication_labels.isin({"si", "true", "1", "x"})]

    if "fecha" in output.columns and (start_date or end_date):
        parsed = ctni_date_series(output)
        if start_date:
            output = output[parsed.dt.date >= start_date]
            parsed = parsed.loc[output.index]
        if end_date:
            output = output[parsed.dt.date <= end_date]

    if "fecha" in output.columns:
        output["__ctni_date"] = ctni_date_series(output)
        output = output.sort_values(
            ["__ctni_date", "primera_deteccion"]
            if "primera_deteccion" in output.columns
            else ["__ctni_date"],
            ascending=False,
            na_position="last",
            kind="stable",
        ).drop(columns=["__ctni_date"])
    return output.reset_index(drop=True)


def display_ctni_records(frame: pd.DataFrame, view_name: str) -> pd.DataFrame:
    spec = CTNI_VIEWS[view_name]
    repaired = _repair_official_links(frame, view_name)
    available = [(source, label) for source, label in spec.columns if source in repaired.columns]
    if not available:
        return pd.DataFrame()
    output = repaired[[source for source, _label in available]].copy()
    return output.rename(columns=dict(available))
