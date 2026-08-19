"""Transformaciones puras para las tres vistas CTNI de Panamá Compra."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd


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
            ("fecha", "Fecha"),
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
            ("fecha", "Fecha"),
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
            ("fecha", "Fecha"),
            ("producto", "Producto"),
            ("numero_ficha", "Ficha"),
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


def _plain_text(value: object) -> str:
    raw = re.sub(r"\s+", " ", str(value or "")).strip().lower()
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", raw)
        if not unicodedata.combining(char)
    )


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
    ):
        if selected and column in output.columns:
            output = output[output[column].fillna("").astype(str).isin(selected)]

    if "fecha" in output.columns and (start_date or end_date):
        parsed = pd.to_datetime(output["fecha"], errors="coerce", format="mixed", dayfirst=True)
        if start_date:
            output = output[parsed.dt.date >= start_date]
            parsed = parsed.loc[output.index]
        if end_date:
            output = output[parsed.dt.date <= end_date]

    if "fecha" in output.columns:
        output["__ctni_date"] = pd.to_datetime(
            output["fecha"], errors="coerce", format="mixed", dayfirst=True
        )
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
    available = [(source, label) for source, label in spec.columns if source in frame.columns]
    if not available:
        return pd.DataFrame()
    output = frame[[source for source, _label in available]].copy()
    return output.rename(columns=dict(available))
