from __future__ import annotations

import re
import sqlite3
from datetime import date
import io
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import bcrypt
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build

from core.config import APP_ROOT, DB_PATH
from services.auth_drive import get_drive_delegated
from ui.theme import apply_global_theme


st.set_page_config(
    page_title="Inteligencia CT y Proveedores",
    page_icon="🧠",
    layout="wide",
)
apply_global_theme()


# Guard de autenticacion (mismo patron que otras paginas)
USERS = {
    "rsanchez": ("Rodrigo Sánchez", "Sptech-71"),
    "isanchez": ("Irvin Sánchez", "Sptech-71"),
    "igsanchez": ("Iris Grisel Sánchez", "Sptech-71"),
}


def _hash(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


credentials = {
    "usernames": {u: {"name": n, "password": _hash(p)} for u, (n, p) in USERS.items()}
}
COOKIE_NAME = "finapp_auth"
COOKIE_KEY = "finapp_key_123"
authenticator = stauth.Authenticate(credentials, COOKIE_NAME, COOKIE_KEY, 30)

try:
    authenticator.login(" ", location="sidebar", key="auth_intel_ct_silent")
    st.sidebar.empty()
except Exception:
    pass

if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")

authenticator.logout("Cerrar sesión", location="sidebar")




FICHA_TOKEN_RE = re.compile(r"\b\d{3,8}\*?\b")
FALLBACK_DB_PATH = Path(r"C:\Users\rodri\OneDrive\cl\panamacompra.db")


def _normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _parse_number(value: object) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace("$", "").replace("USD", "").replace("us$", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _candidate_db_paths() -> list[Path]:
    raw_candidates = [
        Path(DB_PATH),
        APP_ROOT / "panamacompra.db",
        APP_ROOT / "data" / "panamacompra.db",
        APP_ROOT / "data" / "db" / "panamacompra_drive.db",
        Path.cwd() / "panamacompra.db",
        Path.cwd() / "data" / "panamacompra.db",
        Path.cwd() / "data" / "db" / "panamacompra_drive.db",
        FALLBACK_DB_PATH,
    ]
    unique: list[Path] = []
    for path in raw_candidates:
        try:
            normalized = path.expanduser().resolve()
        except Exception:
            normalized = path.expanduser()
        if normalized not in unique:
            unique.append(normalized)
    return unique


def _panamacompra_drive_file_id() -> str:
    try:
        app_cfg = st.secrets.get("app", {})
    except Exception:
        app_cfg = {}
    for key in (
        "DRIVE_PANAMACOMPRA_FILE_ID",
        "DRIVE_PANAMACOMPRA_DB_FILE_ID",
        "DRIVE_DB_PANAMACOMPRA_FILE_ID",
    ):
        value = app_cfg.get(key) if isinstance(app_cfg, dict) else None
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    for key in (
        "DRIVE_PANAMACOMPRA_FILE_ID",
        "DRIVE_PANAMACOMPRA_DB_FILE_ID",
        "DRIVE_DB_PANAMACOMPRA_FILE_ID",
    ):
        value = os.environ.get(key)
        if value and str(value).strip():
            return _normalize_drive_file_id(str(value).strip())
    return ""


def _normalize_drive_file_id(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        return value

    parsed = urlparse(value)
    qs = parse_qs(parsed.query)
    if "id" in qs and qs["id"]:
        return qs["id"][0]

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", parsed.path)
    if match:
        return match.group(1)
    return value


def _get_drive_client() -> tuple[object | None, str]:
    try:
        drive = get_drive_delegated()
        if drive is not None:
            return drive, "delegated"
    except Exception:
        pass

    # fallback: direct service account (without domain delegation)
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    json_path = os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    try:
        if json_path:
            creds = service_account.Credentials.from_service_account_file(json_path, scopes=scopes)
        else:
            info = dict(st.secrets["google_service_account"])
            private_key = info.get("private_key", "")
            if "\\n" in private_key and "\n" not in private_key:
                info["private_key"] = private_key.replace("\\n", "\n")
            creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        drive = build("drive", "v3", credentials=creds)
        return drive, "service_account"
    except Exception as exc:
        return None, f"auth_error:{exc}"


def _download_panamacompra_db_from_drive(file_id: str) -> tuple[bytes | None, str]:
    try:
        drive, mode = _get_drive_client()
        if drive is None:
            return None, mode
        request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        stream = io.BytesIO()
        downloader = MediaIoBaseDownload(stream, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return stream.getvalue(), mode
    except Exception as exc:
        return None, f"download_error:{exc}"


def _resolve_db_path() -> Path | None:
    candidates = _candidate_db_paths()
    for candidate in candidates:
        path = candidate.expanduser()
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            return path

    file_id = _panamacompra_drive_file_id()
    if file_id:
        raw, mode = _download_panamacompra_db_from_drive(file_id)
        if raw:
            runtime_path = APP_ROOT / "data" / "db" / "panamacompra_drive.db"
            try:
                runtime_path.parent.mkdir(parents=True, exist_ok=True)
                runtime_path.write_bytes(raw)
                st.session_state["intel_db_status"] = (
                    f"DB descargada desde Drive ({mode}) -> {runtime_path}"
                )
                return runtime_path
            except Exception:
                pass
        else:
            st.session_state["intel_db_status"] = (
                f"No se pudo descargar DB de Drive. file_id={file_id} ({mode})"
            )
    return None


@st.cache_data(show_spinner=False, ttl=300)
def _load_actos_db_df() -> tuple[pd.DataFrame, str]:
    db_path = _resolve_db_path()
    if db_path is None:
        st.session_state["intel_db_status"] = "No se encontro panamacompra.db local ni se pudo descargar de Drive."
        return pd.DataFrame(), ""
    try:
        with sqlite3.connect(db_path) as conn:
            tables_df = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'",
                conn,
            )
            tables = set(tables_df["name"].astype(str).tolist())
            actos_table = ""
            for candidate in ("actos_publicos", "actos", "panamacompra_actos"):
                if candidate in tables:
                    actos_table = candidate
                    break
            if not actos_table:
                st.session_state["intel_db_status"] = (
                    f"Se encontro DB en `{db_path}` pero no existe tabla de actos "
                    "(esperadas: actos_publicos, actos, panamacompra_actos)."
                )
                return pd.DataFrame(), str(db_path)
            df = pd.read_sql_query(f"SELECT * FROM {actos_table}", conn)
        st.session_state["intel_db_status"] = f"DB OK: {db_path}"
        return df, str(db_path)
    except Exception as exc:
        st.session_state["intel_db_status"] = f"Error leyendo DB `{db_path}`: {exc}"
        return pd.DataFrame(), str(db_path)


def _extract_ficha_tokens(raw_value: object) -> list[str]:
    tokens = FICHA_TOKEN_RE.findall(str(raw_value or ""))
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token not in seen:
            seen.add(token)
            unique.append(token)
    return unique


def _winner_price_from_row(row: pd.Series) -> float:
    winner = _normalize_text(row.get("razon_social", "")) or _normalize_text(row.get("nombre_comercial", ""))
    if winner:
        for idx in range(1, 15):
            proponente = _normalize_text(row.get(f"Proponente {idx}", ""))
            if proponente and proponente == winner:
                winner_price = _parse_number(row.get(f"Precio Proponente {idx}", ""))
                if winner_price > 0:
                    return winner_price
    return _parse_number(row.get("precio_referencia", 0))


@st.cache_data(show_spinner=False, ttl=300)
def _build_ficha_universe() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    base_df, db_path = _load_actos_db_df()
    if base_df.empty:
        return pd.DataFrame(), pd.DataFrame(), db_path

    work = base_df.copy()
    if "id" not in work.columns:
        work["id"] = range(1, len(work) + 1)
    work["ficha_detectada"] = work.get("ficha_detectada", "").fillna("").astype(str)
    work["ficha_tokens"] = work["ficha_detectada"].map(_extract_ficha_tokens)
    # Fallback: if ficha_detectada is missing/empty in source, try extraction from key text fields.
    if work["ficha_tokens"].map(len).sum() == 0:
        fallback_cols = [
            col
            for col in ("ficha", "titulo", "descripcion", "item_1", "item_2", "observaciones")
            if col in work.columns
        ]
        if fallback_cols:
            merged_text = work[fallback_cols].fillna("").astype(str).agg(" ".join, axis=1)
            work["ficha_tokens"] = merged_text.map(_extract_ficha_tokens)
    work = work[work["ficha_tokens"].map(len) > 0].copy()
    if work.empty:
        st.session_state["intel_db_status"] = (
            f"DB leida ({db_path}) pero no se detectaron fichas en columnas de referencia."
        )
        return pd.DataFrame(), pd.DataFrame(), db_path

    work["monto_estimado"] = work.apply(_winner_price_from_row, axis=1)
    work["num_participantes_num"] = work.get("num_participantes", 0).map(_parse_number)
    work["entidad"] = work.get("entidad", "").fillna("").astype(str).str.strip()
    work["ganador"] = work.get("razon_social", "").fillna("").astype(str).str.strip()

    exploded = work.explode("ficha_tokens").rename(columns={"ficha_tokens": "ficha_token"})
    exploded["ficha_token"] = exploded["ficha_token"].astype(str).str.strip()
    exploded = exploded[exploded["ficha_token"] != ""].copy()
    exploded["ficha"] = exploded["ficha_token"].str.replace(r"\D", "", regex=True)
    exploded = exploded[exploded["ficha"] != ""].copy()
    exploded = exploded.drop_duplicates(subset=["id", "ficha"]).reset_index(drop=True)

    grouped = exploded.groupby("ficha", dropna=False)
    ficha_metrics = pd.DataFrame(
        {
            "ficha": grouped["ficha"].first(),
            "actos": grouped["id"].nunique(),
            "monto_historico": grouped["monto_estimado"].sum(),
            "entidades_distintas": grouped["entidad"].nunique(),
            "ganadores_distintos": grouped["ganador"].apply(lambda s: s[s.str.strip() != ""].nunique()),
            "competencia_promedio": grouped["num_participantes_num"].mean(),
        }
    ).reset_index(drop=True)
    if "fecha_adjudicacion" in exploded.columns:
        ficha_metrics["ultima_fecha"] = grouped["fecha_adjudicacion"].max().reset_index(drop=True)
    else:
        ficha_metrics["ultima_fecha"] = ""

    ficha_metrics["afinidad_negocio"] = 0.5
    ficha_metrics["barreras_regulatorias"] = 0.5

    return ficha_metrics, exploded, db_path


def _minmax(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    smin = float(numeric.min())
    smax = float(numeric.max())
    if smax <= smin:
        return pd.Series([0.0] * len(numeric), index=numeric.index)
    return (numeric - smin) / (smax - smin)


def _classify_score(score: float) -> str:
    if score >= 75:
        return "atacar ya"
    if score >= 55:
        return "prometedor"
    if score >= 35:
        return "observacion"
    return "baja prioridad"


def _default_weights() -> dict[str, float]:
    return {
        "actos": 20.0,
        "monto": 20.0,
        "entidades": 15.0,
        "ganadores": 10.0,
        "competencia": 10.0,
        "afinidad": 15.0,
        "barreras": 10.0,
    }


def _score_fichas(ficha_df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    if ficha_df.empty:
        return ficha_df
    df = ficha_df.copy()
    df["f_actos"] = _minmax(df["actos"])
    df["f_monto"] = _minmax(df["monto_historico"])
    df["f_entidades"] = _minmax(df["entidades_distintas"])
    df["f_ganadores"] = _minmax(df["ganadores_distintos"])
    df["f_competencia"] = _minmax(df["competencia_promedio"])
    df["f_afinidad"] = _minmax(df["afinidad_negocio"])
    df["f_barreras"] = _minmax(df["barreras_regulatorias"])

    barreras_component = 1.0 - df["f_barreras"]
    total_weight = sum(weights.values()) or 1.0
    weighted = (
        weights["actos"] * df["f_actos"]
        + weights["monto"] * df["f_monto"]
        + weights["entidades"] * df["f_entidades"]
        + weights["ganadores"] * df["f_ganadores"]
        + weights["competencia"] * df["f_competencia"]
        + weights["afinidad"] * df["f_afinidad"]
        + weights["barreras"] * barreras_component
    )
    df["score_total"] = (100.0 * weighted / total_weight).round(2)
    df["clasificacion"] = df["score_total"].map(_classify_score)
    return df.sort_values(["score_total", "actos", "monto_historico"], ascending=[False, False, False]).reset_index(drop=True)


def _ensure_study_state() -> list[dict]:
    if "intel_fichas_estudio" not in st.session_state:
        st.session_state["intel_fichas_estudio"] = []
    return st.session_state["intel_fichas_estudio"]


def _add_ficha_to_study(row: pd.Series) -> bool:
    current = _ensure_study_state()
    ficha = str(row.get("ficha", "")).strip()
    if not ficha:
        return False
    if any(str(item.get("ficha", "")).strip() == ficha for item in current):
        return False
    current.append(
        {
            "ficha": ficha,
            "score_inicial": float(row.get("score_total", 0.0)),
            "clasificacion": str(row.get("clasificacion", "")),
            "actos": int(row.get("actos", 0)),
            "monto_historico": float(row.get("monto_historico", 0.0)),
            "estado": "pendiente de estudio profundo",
            "fecha_ingreso": date.today().isoformat(),
            "notas": "",
        }
    )
    st.session_state["intel_fichas_estudio"] = current
    return True

def _empty_table(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _placeholder_block(title: str, text: str, columns: list[str] | None = None) -> None:
    st.markdown(f"#### {title}")
    st.info(text)
    if columns:
        st.caption("Formato esperado de tabla (Fase 1, sin datos):")
        st.dataframe(_empty_table(columns), use_container_width=True, hide_index=True)


def _render_sidebar() -> None:
    st.sidebar.markdown("### 🎛️ Filtros globales")
    st.sidebar.selectbox("Ficha", ["Todas"], index=0)
    st.sidebar.selectbox("Estado ficha", ["Todos"], index=0)
    st.sidebar.selectbox("Prioridad", ["Todas"], index=0)
    st.sidebar.selectbox("Proveedor", ["Todos"], index=0)
    st.sidebar.selectbox("País", ["Todos"], index=0)
    st.sidebar.selectbox("Clasificación de contacto", ["Todas"], index=0)
    st.sidebar.checkbox("Solo con contacto encontrado", value=False)
    st.sidebar.checkbox("Solo con seguimiento vencido", value=False)
    st.sidebar.checkbox("Solo viable: prov. en conv.", value=False)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚡ Acciones rápidas")
    st.sidebar.button("Recalcular scoring", disabled=True)
    st.sidebar.button("Actualizar tablero", disabled=True)
    st.sidebar.button("Exportar resumen", disabled=True)
    st.sidebar.button("Limpiar filtros", disabled=True)
    st.sidebar.caption("Fase 1: botones visuales (sin ejecución).")


def _render_kpis(ranked_df: pd.DataFrame) -> None:
    st.markdown("### Centro de control")
    total_fichas = int(len(ranked_df))
    top_ataque = int((ranked_df.get("clasificacion", pd.Series(dtype=str)) == "atacar ya").sum()) if not ranked_df.empty else 0
    prometedoras = int((ranked_df.get("clasificacion", pd.Series(dtype=str)) == "prometedor").sum()) if not ranked_df.empty else 0
    fichas_estudio = _ensure_study_state()
    total_en_seguimiento = len(fichas_estudio)
    total_en_estudio = sum(1 for x in fichas_estudio if str(x.get("estado", "")).strip().lower() == "en estudio")

    cols = st.columns(5)
    cols[0].metric("Fichas det. con actos", f"{total_fichas:,}")
    cols[1].metric("Fichas en seg.", f"{total_en_seguimiento:,}")
    cols[2].metric("Fichas en est.", f"{total_en_estudio:,}")
    cols[3].metric("Seg. vencidos", "0")
    cols[4].metric("Correos por env.", "0")

    cols2 = st.columns(5)
    cols2[0].metric("Viable: prov. en conv.", "0")
    cols2[1].metric("Estudio: pend. contacto", "0")
    cols2[2].metric("Estudio: sin proveedor", "0")
    cols2[3].metric("Contactada no rentable", "0")
    cols2[4].metric("Justif. no rent. pend.", "0")

    st.caption(
        f"Captacion actual (DB): {top_ataque} fichas en 'atacar ya' y {prometedoras} en 'prometedor'. "
        "Estados de seguimiento/contacto se habilitan en la siguiente fase."
    )

def _render_tab_dashboard(ranked_df: pd.DataFrame, db_path: str) -> None:
    st.markdown("### Dashboard Ejecutivo")
    db_status = str(st.session_state.get("intel_db_status", "")).strip()
    if db_status:
        st.caption(f"Estado fuente: {db_status}")
    if ranked_df.empty:
        st.warning("No hay fichas detectadas en la base para construir el dashboard.")
        return

    st.info(
        f"Base utilizada: `{db_path}`. Captacion inicial por ficha detectada "
        "(incluye fichas con y sin asterisco, normalizadas a numero base)."
    )

    _placeholder_block(
        "Alertas y tareas del dia",
        "Aqui se mostraran alertas (vencimientos, fichas sin avance, contactos pendientes) y tareas recomendadas.",
        ["tipo_alerta", "ficha", "proveedor", "prioridad", "fecha_limite", "accion_sugerida"],
    )

    st.markdown("#### Top fichas por score (captacion)")
    st.dataframe(
        ranked_df[
            [
                "ficha",
                "score_total",
                "clasificacion",
                "actos",
                "monto_historico",
                "entidades_distintas",
                "ganadores_distintos",
                "competencia_promedio",
            ]
        ].head(15),
        use_container_width=True,
        hide_index=True,
    )

def _render_tab_deteccion_ct(ficha_metrics_df: pd.DataFrame, ficha_acts_df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("### Deteccion automatica de fichas")
    sub1, sub2, sub3 = st.tabs(["Scoring", "Resultados", "Detalle ficha"])
    default_weights = _default_weights()

    if "intel_weights" not in st.session_state:
        st.session_state["intel_weights"] = default_weights.copy()
    weights = st.session_state["intel_weights"]

    if ficha_metrics_df.empty:
        with sub1:
            st.warning("No hay actos con ficha detectada en la base actual.")
        with sub2:
            st.info("Sin datos para ranking.")
        with sub3:
            st.info("Sin datos para detalle.")
        return pd.DataFrame()

    with sub1:
        st.markdown("#### Ajuste de pesos del score")
        c1, c2, c3 = st.columns(3)
        weights["actos"] = c1.slider("Peso frecuencia", 0.0, 100.0, float(weights["actos"]), 1.0)
        weights["monto"] = c1.slider("Peso monto historico", 0.0, 100.0, float(weights["monto"]), 1.0)
        weights["entidades"] = c2.slider("Peso entidades", 0.0, 100.0, float(weights["entidades"]), 1.0)
        weights["ganadores"] = c2.slider("Peso ganadores distintos", 0.0, 100.0, float(weights["ganadores"]), 1.0)
        weights["competencia"] = c3.slider("Peso competencia", 0.0, 100.0, float(weights["competencia"]), 1.0)
        weights["afinidad"] = c3.slider("Peso afinidad negocio", 0.0, 100.0, float(weights["afinidad"]), 1.0)
        weights["barreras"] = st.slider("Peso barreras regulatorias/tecnicas", 0.0, 100.0, float(weights["barreras"]), 1.0)
        st.session_state["intel_weights"] = weights

        total_weights = sum(weights.values())
        st.caption(f"Suma de pesos: {total_weights:.1f}")
        if total_weights <= 0:
            st.error("La suma de pesos debe ser mayor a 0 para calcular score.")

        b1, b2, b3, b4 = st.columns(4)
        if b1.button("Recalcular"):
            st.success("Scoring recalculado con los pesos actuales.")
        if b2.button("Restaurar default"):
            st.session_state["intel_weights"] = default_weights.copy()
            st.rerun()
        b3.button("Guardar configuracion", disabled=True)
        b4.button("Cargar configuracion", disabled=True)

    ranked_df = _score_fichas(ficha_metrics_df, weights)

    with sub2:
        st.caption("Ranking de fichas detectadas en actos: normaliza 43358, 43358* y 43358.")
        order_mode = st.selectbox(
            "Orden",
            ["Ficha (asc)", "Score (desc)"],
            index=0,
            key="intel_order_mode",
        )
        max_rows = st.slider("Max. fichas a mostrar", 10, 300, 60, 10)
        ranking_cols = [
            "ficha",
            "score_total",
            "clasificacion",
            "actos",
            "monto_historico",
            "entidades_distintas",
            "ganadores_distintos",
            "competencia_promedio",
        ]
        if order_mode == "Ficha (asc)":
            view_df = ranked_df.sort_values(
                "ficha",
                ascending=True,
                kind="stable",
                key=lambda s: pd.to_numeric(s, errors="coerce"),
            ).reset_index(drop=True)
        else:
            view_df = ranked_df.sort_values("score_total", ascending=False, kind="stable").reset_index(drop=True)

        shown_df = view_df[ranking_cols].head(max_rows).copy()
        st.dataframe(shown_df, use_container_width=True, hide_index=True)

        st.markdown("#### Acciones por ficha")
        for _, row in shown_df.iterrows():
            ficha_val = str(row["ficha"])
            c0, c1, c2, c3, c4 = st.columns([1.3, 0.9, 1.0, 1.0, 1.5])
            c0.markdown(f"**Ficha {ficha_val}**")
            c1.write(f"Score: {float(row['score_total']):.1f}")
            c2.write(f"Actos: {int(row['actos'])}")
            c3.write(str(row["clasificacion"]))
            if c4.button("Ver actos", key=f"intel_view_{ficha_val}"):
                st.session_state["intel_selected_ficha"] = ficha_val
                st.rerun()
            if c4.button("Pasar a estudio", key=f"intel_study_{ficha_val}"):
                full_row = ranked_df[ranked_df["ficha"].astype(str) == ficha_val]
                if not full_row.empty and _add_ficha_to_study(full_row.iloc[0]):
                    st.success(f"Ficha {ficha_val} enviada a 'Fichas en seg.'")
                else:
                    st.info(f"Ficha {ficha_val} ya estaba en seguimiento.")

    with sub3:
        selected = st.session_state.get("intel_selected_ficha")
        if not selected:
            st.info("Selecciona una ficha en Resultados para ver su detalle.")
            return ranked_df

        row = ranked_df[ranked_df["ficha"].astype(str) == str(selected)]
        if row.empty:
            st.info("No hay detalle para la ficha seleccionada.")
            return ranked_df

        row = row.iloc[0]
        detail_score = pd.DataFrame(
            [
                ["frecuencia", row["f_actos"], weights["actos"]],
                ["monto_historico", row["f_monto"], weights["monto"]],
                ["entidades", row["f_entidades"], weights["entidades"]],
                ["ganadores", row["f_ganadores"], weights["ganadores"]],
                ["competencia", row["f_competencia"], weights["competencia"]],
                ["afinidad", row["f_afinidad"], weights["afinidad"]],
                ["barreras (inverso)", 1.0 - row["f_barreras"], weights["barreras"]],
            ],
            columns=["factor", "valor_norm", "peso"],
        )
        detail_score["contribucion"] = detail_score["valor_norm"] * detail_score["peso"]
        st.markdown(f"#### Score de ficha {selected}: {row['score_total']:.2f} ({row['clasificacion']})")
        st.dataframe(detail_score, use_container_width=True, hide_index=True)

        st.markdown("#### Actos asociados")
        acts = ficha_acts_df[ficha_acts_df["ficha"].astype(str) == str(selected)].copy()
        acts = acts.rename(columns={"ganador": "proveedor_ganador", "num_participantes": "participantes"})
        show_cols = [
            "id",
            "ficha_token",
            "titulo",
            "entidad",
            "fecha",
            "fecha_adjudicacion",
            "proveedor_ganador",
            "participantes",
            "monto_estimado",
            "enlace",
        ]
        proponent_cols = [c for c in acts.columns if c.startswith("Proponente ")]
        price_cols = [c for c in acts.columns if c.startswith("Precio Proponente ")]
        show_cols.extend(proponent_cols + price_cols)
        present_cols = [c for c in show_cols if c in acts.columns]
        st.dataframe(acts[present_cols].head(500), use_container_width=True, hide_index=True)

    return ranked_df

def _render_tab_seguimiento_ct() -> None:
    st.markdown("### Fichas en seg.")
    fichas_estudio = _ensure_study_state()
    if not fichas_estudio:
        st.info("Todavia no has enviado fichas a seguimiento desde 'Detecc. fichas'.")
        return

    df_seg = pd.DataFrame(fichas_estudio)
    show_cols = [
        "ficha",
        "score_inicial",
        "clasificacion",
        "actos",
        "monto_historico",
        "estado",
        "fecha_ingreso",
        "notas",
    ]
    cols = [c for c in show_cols if c in df_seg.columns]
    st.dataframe(df_seg[cols], use_container_width=True, hide_index=True)

    c1, c2, c3 = st.columns([1.4, 1.8, 1.0])
    target = c1.selectbox("Ficha a gestionar", df_seg["ficha"].astype(str).tolist(), key="intel_seg_target")
    new_state = c2.selectbox(
        "Nuevo estado",
        [
            "pendiente de estudio profundo",
            "en estudio",
            "listo para busqueda de proveedores",
            "pausado",
            "descartado",
        ],
        key="intel_seg_state",
    )
    if c3.button("Actualizar estado"):
        for item in fichas_estudio:
            if str(item.get("ficha", "")) == str(target):
                item["estado"] = new_state
        st.session_state["intel_fichas_estudio"] = fichas_estudio
        st.success(f"Estado actualizado para ficha {target}.")

    if st.button("Quitar ficha seleccionada"):
        st.session_state["intel_fichas_estudio"] = [
            x for x in fichas_estudio if str(x.get("ficha", "")) != str(target)
        ]
        st.success(f"Ficha {target} removida de seguimiento.")
        st.rerun()


def _render_tab_estudio_profundo() -> None:
    st.markdown("### Estudio profundo por ficha")
    st.selectbox("Selecciona ficha para estudio", ["(sin datos en Fase 1)"], index=0)
    _placeholder_block(
        "Acto por acto",
        "Aquí se mostrará el detalle completo de actos asociados a la ficha seleccionada.",
        [
            "fecha_publicacion",
            "fecha_adjudicacion",
            "dias_pub_a_adj",
            "tiempo_entrega",
            "entidad",
            "proveedor_participante",
            "proveedor_ganador",
            "marca",
            "modelo",
            "pais_origen",
            "precio_unitario_ofertado",
            "precio_unitario_ganador",
            "cantidad",
            "monto_total",
        ],
    )
    _placeholder_block(
        "KPIs consolidados y variación de precios",
        "Aquí se mostrarán promedio/min/max de adjudicación, marcas/paises frecuentes y variación % de precios.",
    )
    _placeholder_block(
        "Gráficos de análisis",
        "Aquí se mostrarán barras de precios por proveedor, frecuencia de victorias, marcas y países.",
    )


def _render_tab_proveedores_historicos_ia() -> None:
    st.markdown("### Proveedores históricos + IA")
    _placeholder_block(
        "Tabla histórica por proveedor",
        "Aquí se mostrarán solo proveedores con al menos una adjudicación en la ficha seleccionada.",
        [
            "proveedor",
            "participaciones",
            "victorias",
            "%_victorias",
            "precio_min",
            "precio_prom",
            "precio_max",
            "marcas",
            "modelos",
            "paises_origen",
            "entidades_donde_mas_gana",
        ],
    )
    _placeholder_block(
        "Bloque IA (interpretación ejecutiva)",
        "Aquí se mostrará el análisis IA: dominante, agresivo en precio, premium, concentración, posibilidad de entrada.",
    )


def _render_tab_contacto_correos() -> None:
    st.markdown("### Contacto y correos")
    _placeholder_block(
        "Generador de correo inicial",
        "Aquí se generará el correo inicial usando variables del proveedor, ficha y producto.",
        ["proveedor", "ficha", "asunto", "cuerpo_correo", "canal_sugerido"],
    )
    st.caption("Acciones visuales previstas: copiar correo, abrir mailto, abrir WhatsApp, marcar enviado.")


def _render_tab_seguimiento_contacto() -> None:
    st.markdown("### Seguimiento de contacto (CRM)")
    _placeholder_block(
        "Matriz de seguimiento",
        "Aquí se mostrará estado por proveedor/contacto/canal y días desde último contacto.",
        [
            "ficha",
            "proveedor",
            "canal_usado",
            "fecha_primer_contacto",
            "correo_enviado",
            "whatsapp_enviado",
            "contacto_exitoso_correo",
            "contacto_exitoso_whatsapp",
            "respuesta_recibida",
            "estado_actual",
            "dias_desde_ultimo_contacto",
        ],
    )
    _placeholder_block(
        "Automatización de follow-up",
        "Aquí se verán reglas automáticas de 2do y 3er contacto, con próximos pasos sugeridos.",
    )


def _render_tab_resultado_final() -> None:
    st.markdown("### Resultado final por ficha")
    row1_col1, row1_col2 = st.columns(2)
    with row1_col1:
        _placeholder_block(
            "Ficha viable: prov. en conv.",
            "Aquí se listarán fichas viables con contacto activo y proveedor útil.",
            ["ficha", "proveedor", "marca", "modelo", "pais_origen", "email", "whatsapp", "estado_contacto", "precio", "observaciones"],
        )
    with row1_col2:
        _placeholder_block(
            "Ficha en est.: pend. contacto",
            "Aquí se listarán fichas que requieren primer contacto o siguiente acción inmediata.",
            ["ficha", "prioridad", "proveedor_objetivo", "canal_recomendado", "observaciones"],
        )

    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        _placeholder_block(
            "Ficha en est.: sin proveedor",
            "Aquí se listarán fichas con intentos agotados sin proveedor confirmado.",
            ["ficha", "intentos_realizados", "canales_usados", "motivo_actual", "proximo_paso"],
        )
    with row2_col2:
        _placeholder_block(
            "Ficha contactada no rentable",
            "Aquí se mostrará justificación económica con proveedores contactados y razones.",
            [
                "ficha",
                "proveedores_contactados",
                "precio_obtenido",
                "rango_objetivo",
                "diferencia_%",
                "razones_no_rentable",
            ],
        )


def _render_architecture_notes() -> None:
    with st.expander("🧱 Arquitectura funcional propuesta (Fase 1 - diseño)", expanded=False):
        st.markdown(
            """
            - Esta página está montada como **blueprint visual** para validar flujo y UX.
            - En Fase 2 se conectará con tablas de datos (determinístico + IA).
            - Diseño preparado para módulos:
              - `core/ct_scoring.py`
              - `core/ct_analytics.py`
              - `services/ct_repository.py`
              - `services/ct_automation.py`
              - `services/ct_ai_insights.py`
            """
        )


st.markdown("# 🧠 Inteligencia de Prospección CT y Proveedores")
st.caption("Fase 1.1: captacion operativa desde DB + arquitectura del embudo.")

if "intel_weights" not in st.session_state:
    st.session_state["intel_weights"] = _default_weights().copy()

ficha_metrics_df, ficha_acts_df, db_path = _build_ficha_universe()
ranked_df = _score_fichas(ficha_metrics_df, st.session_state["intel_weights"])

_render_sidebar()
_render_kpis(ranked_df)
_render_architecture_notes()

tabs = st.tabs(
    [
        "Dashboard",
        "Detecc. fichas",
        "Fichas en seg.",
        "Estudio ficha",
        "Prov. hist. + IA",
        "Contacto y correos",
        "Seg. contacto",
        "Resultado ficha",
    ]
)

with tabs[0]:
    _render_tab_dashboard(ranked_df, db_path)
with tabs[1]:
    _render_tab_deteccion_ct(ficha_metrics_df, ficha_acts_df)
with tabs[2]:
    _render_tab_seguimiento_ct()
with tabs[3]:
    _render_tab_estudio_profundo()
with tabs[4]:
    _render_tab_proveedores_historicos_ia()
with tabs[5]:
    _render_tab_contacto_correos()
with tabs[6]:
    _render_tab_seguimiento_contacto()
with tabs[7]:
    _render_tab_resultado_final()
