"""Genera archivos Excel con los tops de PanamaCompra para GEAPP."""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config import APP_ROOT, DB_PATH  # noqa: E402
from core.panamacompra_tops import (  # noqa: E402
    SUPPLIER_TOP_CONFIG,
    TOPS_EXCEL_PATH,
    TOPS_METADATA_SHEET,
    sheet_name_for_top,
)
from services.panamacompra_drive import upload_tops_excel_to_drive  # noqa: E402


DEFAULT_FICHAS = APP_ROOT / "fichas_ctni.xlsx"
DEFAULT_CRITERIOS = APP_ROOT / "criterios_tecnicos.xlsx"
DEFAULT_OFERENTES = APP_ROOT / "oferentes_catalogos.xlsx"
DEFAULT_OUTPUT = TOPS_EXCEL_PATH


def _normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return normalized.strip()


def _normalize_supplier_key(value: Optional[str]) -> str:
    base = _normalize_text(value).upper()
    return re.sub(r"[^A-Z0-9]+", "", base)


def _select_supplier_name(row: pd.Series) -> str:
    for col in ("nombre_comercial", "razon_social"):
        value = str(row.get(col) or "").strip()
        if value:
            return value
    unidad = str(row.get("unidad_solic", "")).strip()
    return unidad or "Proveedor sin nombre"


def _detect_ct_flag(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = _normalize_text(text).lower()
    if not normalized or "no detect" in normalized or normalized in {"no", "sin ficha", "sin dato"}:
        return False
    return bool(re.search(r"\d", text))


def _extract_ficha_label(value: Optional[str]) -> str:
    if not _detect_ct_flag(value):
        return "Sin ficha detectada"
    text = str(value or "").strip()
    text = text.replace("*", "")
    text = re.sub(r"\s+", " ", text)
    text = text.replace(", ,", ",").strip(",; ")
    return text or "Ficha detectada"


def _normalize_ct_code(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"\d+(\.0+)?", text):
        try:
            text = str(int(float(text)))
        except Exception:
            text = text.split(".", 1)[0]
    return text


def _normalize_ct_label(value: Optional[str]) -> str:
    if not value:
        return ""
    text = str(value).upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("*", "")
    text = re.sub(r"[^A-Z0-9/.-]", "", text)
    return text.strip()


def _extract_ct_candidates(value: Optional[str]) -> list[str]:
    tokens = re.findall(r"[A-Z0-9/.-]+", str(value or "").upper())
    candidates: list[str] = []
    for token in tokens:
        normalized = _normalize_ct_label(token)
        if normalized:
            candidates.append(normalized)
    return candidates


def _match_known_ct_code(label: str, known_codes: set[str]) -> str:
    candidates = _extract_ct_candidates(label)
    for candidate in candidates:
        if candidate in known_codes:
            return candidate
    return candidates[0] if candidates else ""


def _last_non_empty(values: Iterable[str]) -> str:
    for raw in reversed(list(values)):
        text = str(raw or "").strip()
        if text:
            return text
    return ""


def _yes_no(value: bool | str | int) -> str:
    return "Sí" if bool(value) else "No"


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def load_supplier_awards_df(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"No se encontró la base {db_path}")

    query = """
        SELECT
            razon_social,
            nombre_comercial,
            precio_referencia,
            fecha_adjudicacion,
            publicacion,
            fecha_actualizacion,
            ficha_detectada,
            num_participantes,
            estado
        FROM actos_publicos
        WHERE estado = 'Adjudicado'
    """
    with _connect_sqlite(db_path) as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        return df

    for col in ("fecha_adjudicacion", "publicacion", "fecha_actualizacion"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["fecha_referencia"] = (
        df["fecha_adjudicacion"]
        .combine_first(df["publicacion"])
        .combine_first(df["fecha_actualizacion"])
    )
    df = df[df["fecha_referencia"].notna()].copy()
    df["fecha_referencia"] = df["fecha_referencia"].dt.tz_localize(None)
    df["precio_referencia"] = pd.to_numeric(df["precio_referencia"], errors="coerce").fillna(0.0)
    df["num_participantes"] = (
        pd.to_numeric(df["num_participantes"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["supplier_name"] = df.apply(_select_supplier_name, axis=1)
    df["supplier_name"] = df["supplier_name"].astype(str).str.strip()
    df = df[df["supplier_name"].astype(bool)]
    df["supplier_key"] = df["supplier_name"].map(_normalize_supplier_key)
    df = df[df["supplier_key"].astype(bool)].copy()
    df["tiene_ct"] = df["ficha_detectada"].map(_detect_ct_flag)
    df["ct_label"] = df["ficha_detectada"].map(_extract_ficha_label)
    return df.reset_index(drop=True)


def load_oferente_metadata(
    file_path: Optional[Path],
) -> tuple[dict[str, dict[str, bool]], dict[str, int], dict[str, str]]:
    if not file_path or not file_path.exists():
        return {}, {}, {}
    df = pd.read_excel(file_path)
    if df.empty:
        return {}, {}, {}

    normalized_cols = {
        col: _normalize_text(col).lower()
        for col in df.columns
    }
    name_col = next(
        (col for col, norm in normalized_cols.items() if "oferente" in norm or "proveedor" in norm),
        None,
    )
    reg_col = next(
        (col for col, norm in normalized_cols.items() if "reg" in norm and "san" in norm),
        None,
    )
    ficha_col = next(
        (col for col, norm in normalized_cols.items() if "ficha" in norm and "ctni" in norm),
        None,
    )
    crit_col = next(
        (col for col, norm in normalized_cols.items() if "criterio" in norm),
        None,
    )
    ct_name_col = next(
        (col for col, norm in normalized_cols.items() if "nombre" in norm and "gener" in norm),
        None,
    )
    if not name_col:
        return {}, {}, {}

    metadata: dict[str, dict[str, bool]] = {}
    ct_suppliers: dict[str, set[str]] = defaultdict(set)
    ct_name_lookup: dict[str, str] = {}
    for _, row in df.iterrows():
        supplier = str(row.get(name_col) or "").strip()
        if not supplier:
            continue
        key = _normalize_supplier_key(supplier)
        if not key:
            continue
        meta = metadata.setdefault(key, {"has_registro": False, "has_ct": False})
        if reg_col:
            reg_value = str(row.get(reg_col) or "").strip()
            if reg_value:
                meta["has_registro"] = True
        norm_label = ""
        if ficha_col:
            ct_value = _normalize_ct_code(row.get(ficha_col))
            norm_label = _normalize_ct_label(ct_value)
        if not norm_label and crit_col:
            crit_value = str(row.get(crit_col) or "").strip()
            if crit_value:
                norm_label = _normalize_ct_label(_extract_ficha_label(crit_value))
        if norm_label:
            meta["has_ct"] = True
            meta.setdefault("ct_labels", set()).add(norm_label)
            ct_suppliers[norm_label].add(key)
            if ct_name_col:
                label_name = str(row.get(ct_name_col) or "").strip()
                if label_name:
                    ct_name_lookup.setdefault(norm_label, label_name)

    for meta in metadata.values():
        if "ct_labels" in meta:
            meta["ct_labels"] = tuple(sorted(meta["ct_labels"]))
    ct_stats = {label: len(keys) for label, keys in ct_suppliers.items()}
    return metadata, ct_stats, ct_name_lookup


def load_ct_name_map(file_path: Optional[Path]) -> dict[str, str]:
    if not file_path or not file_path.exists():
        return {}
    df = pd.read_excel(file_path)
    if df.empty:
        return {}

    normalized_cols = {
        col: _normalize_text(col).lower()
        for col in df.columns
    }
    def _find_column(patterns: list[tuple[str, ...]]) -> Optional[str]:
        for tokens in patterns:
            for col, norm in normalized_cols.items():
                if all(token in norm for token in tokens):
                    return col
        return None

    ficha_col = _find_column(
        [
            ("ficha", "ctni"),
            ("numero", "ficha"),
            ("codigo", "ficha"),
            ("ficha",),
        ]
    )
    nombre_col = _find_column(
        [
            ("nombre", "gener"),
            ("descripcion", "gener"),
            ("nombre",),
        ]
    )
    if not ficha_col or not nombre_col:
        return {}

    name_map: dict[str, str] = {}
    for _, row in df.iterrows():
        criterio = _normalize_ct_code(row.get(ficha_col))
        nombre = str(row.get(nombre_col) or "").strip()
        norm = _normalize_ct_label(criterio)
        if norm and nombre:
            name_map.setdefault(norm, nombre)
    return name_map


def _resolve_ct_display_name(label: str, ct_names: dict[str, str]) -> str:
    norm_label = _normalize_ct_label(label)
    if not norm_label:
        return ""
    if norm_label in ct_names:
        return ct_names[norm_label]
    seen: set[str] = set()
    resolved: list[str] = []
    for candidate in _extract_ct_candidates(label):
        norm = _normalize_ct_label(candidate)
        if norm in seen:
            continue
        seen.add(norm)
        name = ct_names.get(norm)
        if name:
            resolved.append(name)
    return ", ".join(resolved)


def _compute_supplier_ranking(
    df: pd.DataFrame,
    *,
    require_ct: bool,
    require_registro: Optional[bool],
    metric: str,
    metadata: dict[str, dict[str, bool]],
    ct_stats: dict[str, int],
) -> pd.DataFrame:
    subset = df[df["tiene_ct"] == require_ct]
    if subset.empty:
        return pd.DataFrame()

    if require_registro is not None:
        subset = subset[
            subset["supplier_key"].map(
                lambda key: metadata.get(key, {}).get("has_registro", False)
            )
            == require_registro
        ]
    if subset.empty:
        return pd.DataFrame()

    grouped = (
        subset.sort_values("fecha_referencia")
        .groupby(["supplier_key", "supplier_name"], as_index=False)
        .agg(
            actos=("supplier_key", "size"),
            monto=("precio_referencia", "sum"),
            participantes_prom=("num_participantes", "mean"),
            participantes_max=("num_participantes", "max"),
            ultima_ficha=("ct_label", _last_non_empty),
        )
    )
    grouped["Monto adjudicado"] = grouped["monto"].round(2)
    grouped["Actos ganados"] = grouped["actos"]
    grouped["Participantes promedio"] = grouped["participantes_prom"].round(2)
    grouped["Participantes máx."] = grouped["participantes_max"].fillna(0).astype(int)
    grouped["Ficha / Criterio más reciente"] = grouped["ultima_ficha"].replace("", "Sin ficha registrada")
    grouped["_has_registro"] = grouped["supplier_key"].map(
        lambda key: metadata.get(key, {}).get("has_registro", False)
    )
    grouped["Precio promedio acto"] = (
        grouped["Monto adjudicado"] / grouped["Actos ganados"].replace(0, pd.NA)
    ).fillna(0).round(2)
    if require_registro is not None:
        grouped = grouped[grouped["_has_registro"] == require_registro]
    if grouped.empty:
        return pd.DataFrame()

    grouped["Tiene CT"] = grouped["supplier_key"].map(lambda _: require_ct)
    grouped["Tiene Registro Sanitario"] = grouped["_has_registro"]
    known_ct_codes = set(ct_stats.keys())
    grouped["_ct_code"] = grouped["Ficha / Criterio más reciente"].map(
        lambda label: _match_known_ct_code(label, known_ct_codes)
    )
    grouped["Oferentes con esta ficha"] = grouped["_ct_code"].map(lambda code: ct_stats.get(code, 0))

    if metric == "amount":
        grouped = grouped.sort_values(
            ["Monto adjudicado", "Actos ganados"],
            ascending=[False, False],
        )
    else:
        grouped = grouped.sort_values(
            ["Actos ganados", "Monto adjudicado"],
            ascending=[False, False],
        )

    grouped = grouped.copy()
    grouped["Proveedor"] = grouped["supplier_name"]
    grouped["Tiene CT"] = grouped["Tiene CT"].map(_yes_no)
    grouped["Tiene Registro Sanitario"] = grouped["Tiene Registro Sanitario"].map(_yes_no)
    grouped = grouped.drop(columns=["_has_registro", "_ct_code"])
    display_cols = [
        "Proveedor",
        "Actos ganados",
        "Monto adjudicado",
        "Participantes promedio",
        "Participantes máx.",
        "Ficha / Criterio más reciente",
        "Tiene CT",
        "Tiene Registro Sanitario",
    ]
    if require_ct:
        display_cols.insert(3, "Precio promedio acto")
        display_cols.append("Oferentes con esta ficha")
    return grouped[display_cols]


def _compute_ct_ranking(
    df: pd.DataFrame,
    *,
    require_registro: Optional[bool],
    metric: str,
    metadata: dict[str, dict[str, bool]],
    ct_stats: dict[str, int],
    ct_names: dict[str, str],
) -> pd.DataFrame:
    subset = df[df["tiene_ct"]]
    if subset.empty:
        return pd.DataFrame()

    if require_registro is not None:
        subset = subset[
            subset["supplier_key"].map(
                lambda key: metadata.get(key, {}).get("has_registro", False)
            )
            == require_registro
        ]
    if subset.empty:
        return pd.DataFrame()

    subset["norm_label"] = subset["ct_label"].map(_normalize_ct_label)
    subset = subset[subset["norm_label"].astype(bool)]
    if subset.empty:
        return pd.DataFrame()

    rows = []
    for norm_label, group in subset.groupby("norm_label"):
        display_label = group["ct_label"].iloc[-1]
        total_actos = len(group.index)
        total_monto = group["precio_referencia"].sum()
        avg_price = total_monto / total_actos if total_actos else 0.0
        participantes_prom = group["num_participantes"].mean()
        participantes_max = group["num_participantes"].max()
        supplier_breakdown = (
            group.groupby("supplier_name", as_index=False)
            .agg(
                actos=("supplier_key", "size"),
                monto=("precio_referencia", "sum"),
            )
            .sort_values(["monto", "actos"], ascending=[False, False])
        )
        top_amount = supplier_breakdown.nlargest(3, ["monto", "actos"])
        top_amount_str = ", ".join(
            f"{row.supplier_name} (${row.monto:,.0f})" for _, row in top_amount.iterrows()
        )
        top_actos = supplier_breakdown.nlargest(3, ["actos", "monto"])
        top_actos_str = ", ".join(
            f"{row.supplier_name} ({int(row.actos)} actos)" for _, row in top_actos.iterrows()
        )
        rows.append(
            {
                "Ficha / Criterio": display_label,
                "Nombre de la ficha": _resolve_ct_display_name(display_label, ct_names),
                "Actos ganados": total_actos,
                "Monto adjudicado": round(total_monto, 2),
                "Precio promedio acto": round(avg_price, 2),
                "Participantes promedio": round(participantes_prom or 0, 2),
                "Participantes máx.": int(participantes_max or 0),
                "Oferentes en catálogo": ct_stats.get(norm_label, 0),
                "Top 3 por monto": top_amount_str or "Sin datos",
                "Top 3 por actos": top_actos_str or "Sin datos",
            }
        )

    ranking_df = pd.DataFrame(rows)
    if ranking_df.empty:
        return ranking_df

    if metric == "amount":
        ranking_df = ranking_df.sort_values(
            ["Monto adjudicado", "Actos ganados"], ascending=[False, False]
        )
    else:
        ranking_df = ranking_df.sort_values(
            ["Actos ganados", "Monto adjudicado"], ascending=[False, False]
        )
    return ranking_df


def generate_top_tables(
    *,
    db_path: Path,
    fichas_path: Optional[Path],
    criterios_path: Optional[Path],
    oferentes_path: Optional[Path],
) -> tuple[dict[str, pd.DataFrame], dict[str, str], pd.DataFrame]:
    awards_df = load_supplier_awards_df(db_path)
    if awards_df.empty:
        raise RuntimeError("La base panamacompra.db no contiene adjudicaciones para procesar.")

    metadata, ct_stats, ct_names_oferentes = load_oferente_metadata(oferentes_path)
    ct_names = ct_names_oferentes.copy()
    ct_names.update(load_ct_name_map(fichas_path))
    ct_names.update(load_ct_name_map(criterios_path))

    top_tables: dict[str, pd.DataFrame] = {}
    for cfg in SUPPLIER_TOP_CONFIG:
        if cfg["mode"] == "ct":
            df = _compute_ct_ranking(
                awards_df,
                require_registro=cfg.get("require_registro"),
                metric=cfg["metric"],
                metadata=metadata,
                ct_stats=ct_stats,
                ct_names=ct_names,
            )
        else:
            df = _compute_supplier_ranking(
                awards_df,
                require_ct=cfg["require_ct"],
                require_registro=cfg.get("require_registro"),
                metric=cfg["metric"],
                metadata=metadata,
                ct_stats=ct_stats,
            )
        top_tables[cfg["key"]] = df

    meta_info = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "total_adjudicaciones": str(len(awards_df)),
        "fecha_min": awards_df["fecha_referencia"].min().isoformat() if not awards_df.empty else "",
        "fecha_max": awards_df["fecha_referencia"].max().isoformat() if not awards_df.empty else "",
        "fichas_path": str(fichas_path) if fichas_path else "",
        "criterios_path": str(criterios_path) if criterios_path else "",
        "oferentes_path": str(oferentes_path) if oferentes_path else "",
    }
    return top_tables, meta_info, awards_df


def export_to_excel(
    tables: dict[str, pd.DataFrame],
    metadata: dict[str, str],
    *,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for key, df in tables.items():
            sheet_name = sheet_name_for_top(key)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        meta_df = pd.DataFrame(
            [{"clave": key, "valor": value} for key, value in metadata.items()]
        )
        meta_df.to_excel(writer, sheet_name=TOPS_METADATA_SHEET, index=False)
    return output_path


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera los tops de PanamaCompra en un Excel listo para GEAPP."
    )
    parser.add_argument(
        "--db-path",
        default=str(DB_PATH),
        help="Ruta a panamacompra.db (por defecto el configurado en la app).",
    )
    parser.add_argument(
        "--fichas",
        default=str(DEFAULT_FICHAS),
        help="Excel con el listado de fichas CTNI.",
    )
    parser.add_argument(
        "--criterios",
        default=str(DEFAULT_CRITERIOS),
        help="Excel con criterios técnicos (opcional).",
    )
    parser.add_argument(
        "--oferentes",
        default=str(DEFAULT_OFERENTES),
        help="Excel de oferentes y catálogos.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Ruta del archivo Excel destino.",
    )
    parser.add_argument(
        "--upload-to-drive",
        action="store_true",
        help="Sube el Excel generado a la carpeta de Drive configurada.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    db_path = Path(args.db_path).expanduser()
    fichas_path = Path(args.fichas).expanduser() if args.fichas else None
    criterios_path = Path(args.criterios).expanduser() if args.criterios else None
    oferentes_path = Path(args.oferentes).expanduser() if args.oferentes else None
    output_path = Path(args.output).expanduser()

    try:
        tables, meta_info, awards = generate_top_tables(
            db_path=db_path,
            fichas_path=fichas_path if fichas_path and fichas_path.exists() else None,
            criterios_path=criterios_path if criterios_path and criterios_path.exists() else None,
            oferentes_path=oferentes_path if oferentes_path and oferentes_path.exists() else None,
        )
    except Exception as exc:
        print(f"[ERROR] No se pudieron generar los tops: {exc}")
        return 1

    try:
        export_to_excel(tables, meta_info, output_path=output_path)
    except Exception as exc:
        print(f"[ERROR] No se pudo escribir el archivo de salida: {exc}")
        return 1

    if args.upload_to_drive:
        if upload_tops_excel_to_drive(output_path):
            print(f"[DRIVE] Archivo actualizado en la carpeta configurada ({output_path.name}).")
        else:
            print("[WARN] No se pudo subir el archivo a Drive.")

    print(f"[OK] Tops guardados en {output_path}")
    print(f"[LOG] Total adjudicaciones procesadas: {len(awards)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
