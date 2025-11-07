"""Vista PanamáCompra para GE FinApp."""

# pages/visualizador.py
import re
import sqlite3
import time
from pathlib import Path
import streamlit as st
import pandas as pd
import uuid
from datetime import date, timedelta, datetime, timezone

from core.config import DB_PATH
from sheets import get_client, read_worksheet


ROW_ID_COL = "__row__"
CHECKBOX_FLAG_NAMES = {
    "prioritario",
    "prioritarios",
    "descartar",
    "descarte",
}
TRUE_VALUES = {"true", "1", "si", "sí", "yes", "y", "t", "x", "on"}


def _require_authentication() -> None:
    status = st.session_state.get("authentication_status")
    if status is True:
        st.session_state.setdefault("username", st.session_state.get("username"))
        return
    if status is False:
        st.error("Credenciales inválidas. Vuelve a la portada para iniciar sesión.")
    else:
        st.warning("Debes iniciar sesión para entrar.")

    # Redirige al home, igual que otras páginas protegidas del multipage.
    try:
        st.switch_page("Inicio.py")
    except Exception:
        st.stop()
    st.stop()

PC_STATE_WORKSHEET = "pc_state"
PC_CONFIG_WORKSHEET = "pc_config"
PC_MANUAL_SHEET_ID = "1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
PC_MANUAL_WORKSHEET = "pc_manual"
JOB_NAME_LABELS = {
    "clrir": "Cotizaciones Programadas",
    "clv": "Cotizaciones Abiertas",
    "rir1": "Licitaciones",
}
JOB_NAME_ORDER = ["clrir", "clv", "rir1"]
STATUS_BADGES = {
    "success": ("🟢", "Éxito"),
    "running": ("🟡", "En curso"),
    "failed": ("🔴", "Error"),
    "error": ("🔴", "Error"),
}

HEADER_ALIASES = {
    "request_id": {"request_id", "id", "uuid", "solicitud_id"},
    "timestamp": {"timestamp", "requested_at", "fecha", "fecha_solicitud", "created_at"},
    "job_name": {"job_name", "job", "bot", "proceso"},
    "job_label": {"job_label", "job_desc", "descripcion", "descripción"},
    "requested_by": {"requested_by", "user", "usuario", "solicitado_por"},
    "note": {"note", "nota", "comentario", "observacion", "observación"},
    "status": {"status", "estado"},
}

FALLBACK_DB_PATH = Path(r"C:\Users\rodri\GEAPP\panamacompra.db")


def _candidate_db_paths() -> list[Path]:
    candidates: list[Path] = []
    for raw in (DB_PATH, FALLBACK_DB_PATH):
        if not raw:
            continue
        try:
            candidate = Path(raw).expanduser()
        except Exception:
            continue
        if candidate in candidates:
            continue
        candidates.append(candidate)
    return candidates


def _preferred_db_path() -> Path | None:
    candidates = _candidate_db_paths()
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else None


def _quote_identifier(identifier: str) -> str:
    return f"\"{identifier.replace('\"', '\"\"')}\""


def _connect_sqlite(db_path: str):
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


@st.cache_data(ttl=300)
def list_sqlite_tables(db_path: str) -> list[str]:
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]


@st.cache_data(ttl=300)
def count_sqlite_rows(db_path: str, table_name: str) -> int:
    identifier = _quote_identifier(table_name)
    with _connect_sqlite(db_path) as conn:
        cur = conn.execute(f"SELECT COUNT(1) FROM {identifier}")
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


@st.cache_data(ttl=300)
def load_sqlite_preview(db_path: str, table_name: str, limit: int) -> pd.DataFrame:
    identifier = _quote_identifier(table_name)
    limit = max(1, int(limit))
    query = f"SELECT * FROM {identifier} LIMIT {limit}"
    with _connect_sqlite(db_path) as conn:
        return pd.read_sql_query(query, conn)


PC_CONFIG_OVERRIDE_TTL_SECONDS = 180
PC_CONFIG_NAME_ALIASES = ("name", "bot", "job", "proceso")
PC_CONFIG_DAYS_ALIASES = ("days", "dias", "días")
PC_CONFIG_TIMES_ALIASES = ("times", "horas", "hora", "hours")


def _normalize_job_key(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _resolve_config_column(df: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    if df is None or df.empty:
        return None
    col_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in aliases:
        key = str(candidate).strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _pc_config_overrides() -> dict[str, dict[str, str]]:
    overrides = st.session_state.setdefault("pc_config_overrides", {})
    now = time.time()
    stale_keys = [
        key
        for key, payload in list(overrides.items())
        if now - float(payload.get("ts", now)) > PC_CONFIG_OVERRIDE_TTL_SECONDS
    ]
    for key in stale_keys:
        overrides.pop(key, None)
    return overrides


def _sanitize_config_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _canonicalize_schedule_text(value: str) -> str:
    """
    Normaliza listas separadas por comas/semicolons a un formato consistente.
    Mantiene el orden de entrada y elimina espacios duplicados.
    """
    text = _sanitize_config_value(value)
    if not text:
        return ""
    parts = [p.strip() for p in re.split(r"[;,]+", text) if p.strip()]
    if not parts:
        return ""
    return ", ".join(parts)


def _apply_pc_config_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    name_col = _resolve_config_column(df, PC_CONFIG_NAME_ALIASES)
    if not name_col:
        return df

    overrides = _pc_config_overrides()
    if not overrides:
        return df

    work_df = df.copy()
    name_keys = work_df[name_col].astype(str).str.strip().str.lower()
    days_column = _resolve_config_column(work_df, PC_CONFIG_DAYS_ALIASES)
    times_column = _resolve_config_column(work_df, PC_CONFIG_TIMES_ALIASES)
    resolved_keys: list[str] = []

    for key, payload in list(overrides.items()):
        job_key = _normalize_job_key(key)
        if not job_key:
            resolved_keys.append(key)
            continue

        mask = name_keys == job_key
        if not mask.any():
            resolved_keys.append(key)
            continue

        row_index = mask[mask].index[0]

        if days_column and "days" in payload:
            desired_value = _canonicalize_schedule_text(payload["days"])
            work_df.at[row_index, days_column] = desired_value
            payload["days"] = desired_value

        if times_column and "times" in payload:
            desired_value = _canonicalize_schedule_text(payload["times"])
            work_df.at[row_index, times_column] = desired_value
            payload["times"] = desired_value

    for key in resolved_keys:
        overrides.pop(key, None)

    # Conserva metadatos como los encabezados originales.
    work_df.attrs = dict(df.attrs)
    return work_df


def _manual_sheet_id() -> str | None:
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    manual_id = app_cfg.get("PC_MANUAL_SHEET_ID") if isinstance(app_cfg, dict) else None
    manual_id = manual_id or PC_MANUAL_SHEET_ID
    return manual_id or None


def _pc_config_sheet_id() -> str | None:
    """Obtiene el Sheet ID donde vive pc_config, con fallbacks sensatos."""
    try:
        app_cfg = st.secrets["app"]
    except Exception:
        app_cfg = {}

    sheet_id = None
    if isinstance(app_cfg, dict):
        sheet_id = (
            app_cfg.get("PC_CONFIG_SHEET_ID")
            or app_cfg.get("PC_MANUAL_SHEET_ID")
            or app_cfg.get("SHEET_ID")
        )

    return sheet_id or _manual_sheet_id()


def _current_user() -> str:
    for key in ("username", "user", "email", "correo", "name", "nombre"):
        value = st.session_state.get(key)
        if value:
            return str(value)
    return "desconocido"


def append_manual_request(job_name: str, job_label: str, note: str) -> bool:
    sheet_id = _manual_sheet_id()
    if not sheet_id:
        st.error("No hay hoja configurada para registrar ejecuciones manuales.")
        return False

    client = get_gc()
    try:
        sh = client.open_by_key(sheet_id)
    except Exception as exc:
        st.error(f"No se pudo abrir la hoja manual: {exc}")
        return False

    ws = None
    try:
        ws = sh.worksheet(PC_MANUAL_WORKSHEET)
    except Exception:
        try:
            ws = sh.sheet1
        except Exception:
            st.error("No encontramos la pestaña pc_manual en la hoja configurada.")
            return False

    headers = ws.row_values(1)
    cleaned_headers = [h.strip() for h in headers if h.strip()]
    if not cleaned_headers:
        cleaned_headers = ["request_id", "timestamp", "job_name", "job_label", "requested_by", "note"]
        ws.update("A1", [cleaned_headers])

    payload_map = {
        "request_id": uuid.uuid4().hex,
        "timestamp": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "job_name": job_name,
        "job_label": job_label,
        "requested_by": _current_user(),
        "note": note.strip(),
        "status": "pending",
    }

    def _value_for_header(header: str) -> str:
        normalized = header.strip().lower()
        for key, aliases in HEADER_ALIASES.items():
            if normalized == key or normalized in aliases:
                return payload_map.get(key, "")
        return payload_map.get(normalized, "")

    row = [_value_for_header(header) for header in cleaned_headers]
    status_idx = None
    for idx, header in enumerate(cleaned_headers, start=1):
        header_norm = header.strip().lower()
        if header_norm in {"status", "estado"}:
            status_idx = idx
            break

    try:
        append_result = ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as exc:
        st.error(f"No se pudo registrar la ejecución manual: {exc}")
        return False

    if status_idx:
        updated_range = ""
        try:
            updated_range = (
                append_result.get("updates", {}).get("updatedRange", "")
                if isinstance(append_result, dict)
                else ""
            )
        except AttributeError:
            updated_range = ""

        row_number = None
        if updated_range:
            try:
                range_bounds = updated_range.split("!")[-1]
                start_cell = range_bounds.split(":")[0]
                row_number = int("".join(filter(str.isdigit, start_cell)))
            except Exception:
                row_number = None

        if not row_number:
            try:
                row_number = len(ws.col_values(1))
            except Exception:
                row_number = None

        if row_number:
            try:
                ws.update_cell(row_number, status_idx, payload_map.get("status", "pending"))
            except Exception as exc:
                st.warning(f"No se pudo actualizar el estado 'pending' en la fila {row_number}: {exc}")

    return True


@st.cache_data(ttl=180)
def load_pc_state() -> pd.DataFrame:
    """Carga el estado de los jobs PanamáCompra desde Finanzas Operativas."""
    try:
        sheet_id = st.secrets["app"]["SHEET_ID"]
    except Exception:
        return pd.DataFrame()

    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_STATE_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    keep_cols = [
        col
        for col in (
            "job_name",
            "status",
            "started_at",
            "finished_at",
            "duration_display",
            "duration_seconds",
        )
        if col in df.columns
    ]

    if not keep_cols:
        return pd.DataFrame()

    data = df[keep_cols].copy()
    if "job_name" not in data.columns:
        return pd.DataFrame()

    data["job_name"] = data["job_name"].astype(str).str.strip()

    if "status" in data.columns:
        data["status"] = data["status"].astype(str).str.strip()
    else:
        data["status"] = ""
    if "started_at" in data.columns:
        data["__started_ts"] = pd.to_datetime(data["started_at"], errors="coerce")
    else:
        data["__started_ts"] = pd.NaT

    # Conserva el último registro por job.
    data = data.sort_values("__started_ts", ascending=False)
    data = data.drop_duplicates(subset=["job_name"], keep="first")

    order_map = {name: idx for idx, name in enumerate(JOB_NAME_ORDER)}
    data["__order"] = data["job_name"].str.lower().map(order_map).fillna(len(order_map))
    data = data.sort_values(["__order", "__started_ts"], ascending=[True, False]).reset_index(drop=True)
    return data.drop(columns=["__started_ts", "__order"], errors="ignore")


@st.cache_data(ttl=180)
def load_pc_config() -> pd.DataFrame:
    """Obtiene la configuración de programación (días/horas) desde la hoja pc_config."""
    sheet_id = _pc_config_sheet_id()
    if not sheet_id:
        return pd.DataFrame()

    try:
        df = read_worksheet(get_gc(), sheet_id, PC_CONFIG_WORKSHEET)
    except Exception:
        return pd.DataFrame()

    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    original_columns = list(df.columns)
    df = df.rename(columns=lambda col: str(col).strip().lower())
    df.attrs["__original_columns__"] = original_columns
    return _apply_pc_config_overrides(df)


def _format_pc_datetime(value) -> str:
    if value is None or value == "":
        return "—"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "—"
    return ts.strftime("%d/%m %H:%M")


def _format_pc_duration(row: pd.Series) -> str:
    display = str(row.get("duration_display", "")).strip()
    if display:
        return display
    seconds = row.get("duration_seconds")
    try:
        return f"{float(seconds):.0f} s"
    except Exception:
        return "—"


def render_pc_state_cards(
    pc_state_df: pd.DataFrame | None,
    pc_config_df: pd.DataFrame | None,
    suffix: str | None = None,
) -> None:
    """Renderiza tarjetas discretas con el estado de los bots de PanamáCompra."""
    if pc_state_df is None or pc_state_df.empty:
        return

    st.markdown(
        """
<div style="margin-top:14px;margin-bottom:6px;padding:8px 16px;background:rgba(34,52,82,0.55);border-radius:8px;border:1px solid rgba(120,170,255,0.15);font-size:1.12rem;font-weight:600;color:#f4f6fb;letter-spacing:0.01em;">
  🔧 Resumen de últimas actualizaciones y controles
</div>
""",
        unsafe_allow_html=True,
    )
    rows = pc_state_df.drop(columns=[col for col in pc_state_df.columns if col.startswith("__")]).copy()

    config_map = {}
    name_col = None
    days_col = None
    times_col = None
    if pc_config_df is not None and not pc_config_df.empty:
        name_col = _resolve_config_column(pc_config_df, PC_CONFIG_NAME_ALIASES)
        if name_col:
            tmp = pc_config_df.copy()
            tmp["__pc_key__"] = tmp[name_col].astype(str).str.strip().str.lower()
            days_col = _resolve_config_column(tmp, PC_CONFIG_DAYS_ALIASES)
            times_col = _resolve_config_column(tmp, PC_CONFIG_TIMES_ALIASES)
            for _, cfg_row in tmp.iterrows():
                config_map[cfg_row["__pc_key__"]] = cfg_row

    buffer = st.session_state.setdefault("pc_config_buffer", {})
    missing_config_jobs: set[str] = set()

    for start in range(0, len(rows), 3):
        chunk = rows.iloc[start : start + 3]
        cols = st.columns(len(chunk))
        for col_widget, (_, row) in zip(cols, chunk.iterrows()):
            job_raw = str(row.get("job_name", "")).strip()
            job_label = JOB_NAME_LABELS.get(job_raw.lower(), job_raw or "Job sin nombre")
            status_key = str(row.get("status", "")).strip().lower()
            icon, status_label = STATUS_BADGES.get(status_key, ("⚪", status_key.capitalize() or "Sin dato"))
            started_text = _format_pc_datetime(row.get("started_at"))
            duration_text = _format_pc_duration(row)

            job_key = job_raw.strip().lower()
            cfg = config_map.get(job_key) if config_map else None
            if cfg is not None:
                cfg_key = _sanitize_config_value(cfg.get("__pc_key__", job_key))
                job_key = cfg_key or job_key
            cfg_name_value = (
                _sanitize_config_value(cfg.get(name_col, job_raw))
                if (cfg is not None and name_col)
                else job_raw
            )
            key_suffix = f"_{suffix}" if suffix else ""
            days_key = f"pc_days_{job_raw.lower()}{key_suffix}"
            times_key = f"pc_times_{job_raw.lower()}{key_suffix}"
            days_value = (
                _canonicalize_schedule_text(cfg.get(days_col, ""))
                if cfg is not None and days_col
                else ""
            )
            times_value = (
                _canonicalize_schedule_text(cfg.get(times_col, ""))
                if cfg is not None and times_col
                else ""
            )

            card = f"""
<div style="border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:12px 14px;margin-top:8px;background-color:rgba(17,20,24,0.35);">
  <div style="font-weight:600;font-size:0.95rem;">{job_label}</div>
  <div style="font-size:0.8rem;color:#9aa0a6;margin-top:4px;">{icon} {status_label}</div>
  <div style="font-size:0.76rem;color:#d6d8dc;margin-top:10px;line-height:1.45;">
    <div>Fecha: {started_text}</div>
    <div>Duración: {duration_text}</div>
  </div>
</div>
"""
            col_widget.markdown(card, unsafe_allow_html=True)

            manual_col = col_widget.container()
            if manual_col.button(
                "▶ Actualización manual",
                key=f"pc_manual_btn_{job_raw.lower()}{key_suffix}",
                use_container_width=True,
            ):
                if append_manual_request(job_raw, job_label, ""):
                    st.session_state["pc_manual_feedback"] = job_label

            fields_col = col_widget.container()
            with fields_col:
                days_input = st.text_input(
                    f"Días programados ({job_label})",
                    value=days_value,
                    key=days_key,
                    placeholder="Días separados por comas",
                    label_visibility="collapsed",
                    help="Días separados por comas",
                )
                times_input = st.text_input(
                    f"Horas programadas ({job_label})",
                    value=times_value,
                    key=times_key,
                    placeholder="Horas separadas por comas",
                    label_visibility="collapsed",
                    help="Horas separadas por comas",
                )


            if cfg is None:
                missing_config_jobs.add(job_label)
                continue

            cleaned_days = _canonicalize_schedule_text(days_input) if days_col else ""
            cleaned_times = _canonicalize_schedule_text(times_input) if times_col else ""

            diff_days = bool(days_col and cleaned_days != days_value)
            diff_times = bool(times_col and cleaned_times != times_value)

            entry = buffer.get(job_key)

            if diff_days or diff_times:
                entry = buffer.setdefault(
                    job_key,
                    {"name": cfg_name_value, "label": job_label},
                )
                if diff_days:
                    entry["days"] = cleaned_days
                elif "days" in entry:
                    entry.pop("days", None)

                if diff_times:
                    entry["times"] = cleaned_times
                elif "times" in entry:
                    entry.pop("times", None)

                entry["ts"] = time.time()
            else:
                if entry:
                    entry.pop("days", None)
                    entry.pop("times", None)
                    if not any(k in entry for k in ("days", "times")):
                        buffer.pop(job_key, None)

            if entry and any(k in entry for k in ("days", "times")):
                col_widget.caption("⬆ Cambios pendientes")
            else:
                col_widget.caption("")

    if missing_config_jobs:
        st.warning(
            "No encontramos filas de configuración para: "
            + ", ".join(sorted(missing_config_jobs))
            + ". Revisa la hoja pc_config."
        )

    feedback_key = "pc_manual_feedback"
    if feedback_key in st.session_state:
        job_label = st.session_state.pop(feedback_key)
        st.success(f"Ejecución manual iniciada, scraping en curso para {job_label} (status: pending).")

    unsaved_jobs = [
        info.get("label") or key
        for key, info in buffer.items()
        if any(k in info for k in ("days", "times"))
    ]

    if unsaved_jobs:
        st.info(
            "Cambios pendientes: "
            + ", ".join(sorted(set(unsaved_jobs)))
        )

    sync_key = f"pc_sync_btn_{suffix or 'main'}"
    if st.button(
        "Sincronizar cambios",
        key=sync_key,
        type="primary",
        disabled=not unsaved_jobs,
        use_container_width=True,
    ):
        st.session_state["pc_config_sync_trigger"] = True

    if st.session_state.pop("pc_config_sync_trigger", False):
        success = sync_pc_config_updates(pc_config_df)
        if success:
            st.success("Se sincronizaron los cambios en pc_config.")
        else:
            st.warning("No se detectaron cambios para sincronizar.")


def sync_pc_config_updates(pc_config_df: pd.DataFrame | None) -> bool:
    """Sincroniza con Google Sheets los cambios almacenados en pc_config_buffer."""

    buffer = st.session_state.get("pc_config_buffer", {}) or {}
    pending: list[dict[str, str]] = []
    for job_key, info in buffer.items():
        payload = {"key": job_key}
        if info.get("name"):
            payload["name"] = info["name"]
        if "days" in info:
            payload["days"] = info["days"]
        if "times" in info:
            payload["times"] = info["times"]
        if len(payload) > 1:
            payload["label"] = info.get("label", job_key)
            pending.append(payload)

    if not pending:
        return False

    sheet_id = _pc_config_sheet_id()
    if not sheet_id:
        st.warning("No hay SHEET_ID configurado para sincronizar pc_config.")
        return False

    client = get_gc()
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(PC_CONFIG_WORKSHEET)
    except Exception as exc:
        st.error(f"No se pudo acceder a la hoja pc_config: {exc}")
        return False

    try:
        df = read_worksheet(client, sheet_id, PC_CONFIG_WORKSHEET)
    except Exception as exc:
        st.error(f"No se pudo leer pc_config: {exc}")
        return False

    if df is None or df.empty:
        st.warning("pc_config está vacía; no se aplicaron cambios.")
        return False

    headers = [str(c) for c in df.columns]
    work_df = df.copy()
    work_df.columns = [str(c).strip().lower() for c in work_df.columns]

    name_col = _resolve_config_column(work_df, PC_CONFIG_NAME_ALIASES)
    if not name_col:
        st.warning("No se encontró ninguna columna equivalente a 'name' en pc_config; no se aplicaron cambios.")
        return False

    days_col = _resolve_config_column(work_df, PC_CONFIG_DAYS_ALIASES)
    times_col = _resolve_config_column(work_df, PC_CONFIG_TIMES_ALIASES)

    def _column_index(lower_name: str | None) -> int | None:
        if not lower_name:
            return None
        target = lower_name.strip().lower()
        for idx, header in enumerate(headers, start=1):
            if str(header).strip().lower() == target:
                return idx
        return None

    col_idx_days = _column_index(days_col)
    col_idx_times = _column_index(times_col)

    overrides = _pc_config_overrides()
    missing_jobs: list[str] = []
    updated_any = False
    now = time.time()

    name_series = work_df[name_col].astype(str).str.strip().str.lower()

    for entry in pending:
        job_key = _normalize_job_key(entry.get("key") or entry.get("name"))
        if not job_key:
            continue

        mask = name_series == job_key
        if not mask.any():
            missing_jobs.append(entry.get("label", job_key))
            continue

        pos = mask[mask].index[0]
        row_number = int(pos) + 2  # +1 header row, +1 porque DataFrame inicia en 0

        override_entry = overrides.setdefault(job_key, {})
        override_entry["name"] = _sanitize_config_value(work_df.at[pos, name_col])

        if col_idx_days and "days" in entry and entry["days"] is not None:
            value = _canonicalize_schedule_text(entry["days"])
            try:
                ws.update_cell(row_number, col_idx_days, value)
            except Exception as exc:
                st.error(f"No se pudo actualizar los días para '{job_key}': {exc}")
                return
            work_df.at[pos, days_col] = value
            override_entry["days"] = value
            updated_any = True

        if col_idx_times and "times" in entry and entry["times"] is not None:
            value = _canonicalize_schedule_text(entry["times"])
            try:
                ws.update_cell(row_number, col_idx_times, value)
            except Exception as exc:
                st.error(f"No se pudo actualizar las horas para '{job_key}': {exc}")
                return
            work_df.at[pos, times_col] = value
            override_entry["times"] = value
            updated_any = True

        override_entry["pending"] = False
        override_entry["ts"] = now

    if missing_jobs:
        st.warning(
            "No se encontraron filas en pc_config para: "
            + ", ".join(sorted(missing_jobs))
        )

    if not updated_any:
        return False

    load_pc_config.clear()
    st.session_state["pc_config_buffer"] = {
        key: info
        for key, info in buffer.items()
        if key not in {entry["key"] for entry in pending}
    }
    return True


def _make_unique(headers):
    out, seen = [], {}
    for i, h in enumerate(headers):
        h = (h or "").strip() or f"col_{i+1}"
        if h in seen:
            seen[h] += 1
            h = f"{h}_{seen[h]}"
        else:
            seen[h] = 0
        out.append(h)
    return out


def _coerce_to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return value != 0
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in TRUE_VALUES:
            return True
        if norm in {"false", "0", "no", "n"}:
            return False
    return False


def _is_checkbox_target(col_name: str) -> bool:
    return col_name.strip().lower() in CHECKBOX_FLAG_NAMES


def _parse_sheet_date_column(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series([], index=series.index, dtype="datetime64[ns]", name=series.name)

    if pd.api.types.is_datetime64_any_dtype(series):
        tz_info = getattr(series.dt, "tz", None)
        return series.dt.tz_convert(None) if tz_info is not None else series

    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.replace({
        "": pd.NA,
        "none": pd.NA,
        "null": pd.NA,
        "nan": pd.NA,
        "nat": pd.NA,
        "n/a": pd.NA,
    }, regex=False)

    parsed = pd.to_datetime(cleaned, errors="coerce", dayfirst=True)

    mask_serial = parsed.isna() & cleaned.str.fullmatch(r"\d+(\.0+)?", na=False)
    if mask_serial.any():
        serial = cleaned[mask_serial].astype(float)
        parsed.loc[mask_serial] = pd.to_datetime(
            serial,
            errors="coerce",
            origin="1899-12-30",
            unit="D",
        )

    mask_pattern = parsed.isna() & cleaned.notna()
    if mask_pattern.any():
        date_pattern = re.compile(r"(\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4})")

        def _last_match(text: str):
            matches = date_pattern.findall(text)
            if matches:
                return matches[-1]
            return text

        extracted = cleaned[mask_pattern].map(_last_match)
        parsed.loc[mask_pattern] = pd.to_datetime(extracted, errors="coerce", dayfirst=True)

    tz_info = getattr(parsed.dt, "tz", None)
    if tz_info is not None:
        parsed = parsed.dt.tz_convert(None)

    return parsed

st.set_page_config(page_title="Visualizador de Actos", layout="wide")
_require_authentication()
st.title("📋 Visualizador de Actos Panamá Compra")

# ---- Config ----
SHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"


DEFAULT_SHEET = "cl_abiertas_rir_sin_requisitos"

SHEET_LABELS = {
    "cl_abiertas_rir_sin_requisitos": "CL abiertas RIR sin requisitos",
    "cl_abiertas": "CL abiertas",
    "cl_abiertas_rir_con_ct": "CL abiertas RIR con CT",
    "cl_prog_sin_ficha": "CL programadas sin ficha",
    "cl_prog_sin_requisitos": "CL programadas sin requisitos",
    "cl_prog_con_ct": "CL programadas con CT",
    "cl_prioritarios": "CL prioritarios",
    "ap_con_ct": "AP con CT",
    "ap_sin_ficha": "AP sin ficha",
    "ap_sin_requisitos": "AP sin requisitos",
}

SHEET_GROUPS = {
    "Cotizaciones Abiertas": [
        "cl_abiertas_rir_sin_requisitos",
        "cl_abiertas",
        "cl_abiertas_rir_con_ct",
    ],
    "Cotizaciones Programadas": [
        "cl_prog_sin_ficha",
        "cl_prog_sin_requisitos",
        "cl_prog_con_ct",
    ],
    "Licitaciones": [
        "ap_con_ct",
        "ap_sin_ficha",
        "ap_sin_requisitos",
    ],
    "Prioritarias": [
        "cl_prioritarios",
    ],
}

CATEGORY_ORDER = [
    "Cotizaciones Abiertas",
    "Cotizaciones Programadas",
    "Licitaciones",
    "Prioritarias",
]


def _sheet_label(name: str) -> str:
    return SHEET_LABELS.get(name, name.replace("_", " ").title())

def get_gc():
    # Reutiliza las credenciales centralizadas en sheets.get_client()
    client, _ = get_client()
    return client


def apply_checkbox_updates(sheet_name: str, updates):
    if not updates:
        return

    ws = get_gc().open_by_key(SHEET_ID).worksheet(sheet_name)
    headers = _make_unique(ws.row_values(1))

    for row_number, column_name, value in updates:
        try:
            col_idx = headers.index(column_name) + 1
        except ValueError:
            continue
        ws.update_cell(int(row_number), col_idx, "TRUE" if value else "FALSE")

# --- reemplaza tu load_df y añade el helper _make_unique ---

@st.cache_data(ttl=300)
def load_df(sheet_name: str) -> pd.DataFrame:
    sh = get_gc().open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    raw_headers = ws.row_values(1)
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    if not any(c.strip() for c in raw_headers):
        header_row_idx = None
        for r in range(min(10, len(values))):
            non_empty = sum(1 for c in values[r] if c.strip())
            if non_empty >= 3:
                header_row_idx = r
                break

        if header_row_idx is None:
            return pd.DataFrame()

        headers = _make_unique(values[header_row_idx])
        width = len(headers)

        data_rows, row_numbers = [], []
        for idx, row in enumerate(values[header_row_idx + 1 :], start=header_row_idx + 2):
            trimmed = row[:width] + [""] * (width - len(row))
            data_rows.append(trimmed)
            row_numbers.append(idx)

        if not data_rows:
            df = pd.DataFrame(columns=headers)
            df[ROW_ID_COL] = pd.Series(dtype=int)
            return df

        df = pd.DataFrame(data_rows, columns=headers)
        df[ROW_ID_COL] = pd.Series(row_numbers, dtype=int)
    else:
        headers = _make_unique(raw_headers)
        width = len(headers)
        data_rows = values[1:] if len(values) > 1 else []
        padded_rows = [row[:width] + [""] * (width - len(row)) for row in data_rows]
        df = pd.DataFrame(padded_rows, columns=headers)
        if df.empty:
            df[ROW_ID_COL] = pd.Series(dtype=int)
        else:
            df[ROW_ID_COL] = pd.Series(range(2, len(df) + 2), dtype=int)

    df = df.replace("", pd.NA)
    data_cols = [c for c in df.columns if c != ROW_ID_COL]
    if data_cols:
        df = df.dropna(how="all", subset=data_cols)
    df = df.reset_index(drop=True)
    return df


def render_df(
    df: pd.DataFrame,
    sheet_name: str,
    pc_state_df: pd.DataFrame | None = None,
    pc_config_df: pd.DataFrame | None = None,
    suffix: str | None = None,
):
    keyp = f"{sheet_name}_"

    notice_key = keyp + "update_notice"
    if notice_key in st.session_state:
        count = st.session_state.pop(notice_key)
        if count:
            st.success(f"Se guardaron {count} cambio(s) en la hoja.")

    df = df.copy()
    displayable_columns = [c for c in df.columns if c != ROW_ID_COL]

    def _normalize_label(value: str) -> str:
        value = (value or "").strip().lower()
        for src, tgt in (
            ("á", "a"),
            ("é", "e"),
            ("í", "i"),
            ("ó", "o"),
            ("ú", "u"),
            ("ü", "u"),
        ):
            value = value.replace(src, tgt)
        value = re.sub(r"\s+", " ", value)
        return value

    def _is_date_header(name: str) -> bool:
        normalized = _normalize_label(name)
        if not normalized:
            return False
        if normalized.startswith("fecha"):
            return True
        if "publicacion" in normalized:
            return True
        return False

    date_columns = [c for c in df.columns if _is_date_header(c)]

    filter_date_col = None
    for col in date_columns:
        norm = _normalize_label(col)
        if "publicacion" in norm:
            filter_date_col = col
            break

    if filter_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha sola":
                filter_date_col = col
                break

    if filter_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha":
                filter_date_col = col
                break

    if filter_date_col is None and date_columns:
        filter_date_col = date_columns[0]

    event_date_col = None
    for col in date_columns:
        if _normalize_label(col) == "fecha sola":
            event_date_col = col
            break

    if event_date_col is None:
        for col in date_columns:
            norm = _normalize_label(col)
            if any(token in norm for token in ("acto", "celebr")):
                event_date_col = col
                break

    if event_date_col is None:
        for col in date_columns:
            if _normalize_label(col) == "fecha":
                event_date_col = col
                break

    parsed_date_series = {}
    for col in date_columns:
        parsed = _parse_sheet_date_column(df[col])
        parsed_date_series[col] = parsed
        normalized_name = _normalize_label(col)
        keep_original = (col == filter_date_col) or (normalized_name == "fecha")
        if not keep_original:
            df[col] = parsed

    # Detectar columnas de monto/precio
    money_cols = [c for c in df.columns if any(x in c.lower() for x in ["precio", "monto", "estimado", "referencia"])]

    today = date.today()
    today_ts = pd.Timestamp(today)

    with st.expander("🔎 Filtros", expanded=True):
        if "Entidad" in df.columns:
            opciones = sorted([e for e in df["Entidad"].dropna().unique()])
            sel = st.multiselect("Entidad", opciones, key=keyp+"ent")
            if sel:
                df = df[df["Entidad"].isin(sel)]

        if "Estado" in df.columns:
            opciones = sorted([e for e in df["Estado"].dropna().unique()])
            sel = st.multiselect("Estado", opciones, key=keyp+"estado")
            if sel:
                df = df[df["Estado"].isin(sel)]

        date_filter_series = None
        if filter_date_col:
            date_filter_series = parsed_date_series.get(filter_date_col)
            if date_filter_series is not None and date_filter_series.notna().any():
                normalized_dates = date_filter_series.dt.normalize()
                valid_dates = normalized_dates.dropna()
                mind = valid_dates.min() if not valid_dates.empty else pd.Timestamp(today)
                maxd = valid_dates.max() if not valid_dates.empty else pd.Timestamp(today)
                default_fin = today
                default_ini = today - timedelta(days=30)
                if pd.notna(mind) and mind.date() > default_fin:
                    default_ini = mind.date()
                    default_fin = mind.date()
                if default_ini > default_fin:
                    default_ini = default_fin
                r = st.date_input(
                    "Rango de fechas",
                    value=(default_ini, default_fin),
                    key=keyp+"fecha",
                )
                if isinstance(r, tuple) and len(r) == 2:
                    ini = pd.Timestamp(r[0]).normalize()
                    fin = pd.Timestamp(r[1]).normalize()
                    normalized = date_filter_series.dt.normalize()
                    mask_valid = (normalized >= ini) & (normalized <= fin)
                    mask = normalized.isna() | mask_valid
                    df = df[mask]
            else:
                st.info("No encontramos fechas válidas en esa columna todavía.")

        if money_cols:
            colm = money_cols[0]
            v = pd.to_numeric(df[colm], errors="coerce")
            if v.notna().any():
                price_min, price_max = 1000.0, 2000000.0
                if price_min < price_max:
                    r = st.slider(
                        f"Rango de {colm}",
                        min_value=price_min,
                        max_value=price_max,
                        value=(price_min, price_max),
                        step=1000.0,
                        key=keyp+"monto",
                    )
                    df = df[(pd.to_numeric(df[colm], errors="coerce") >= r[0]) &
                            (pd.to_numeric(df[colm], errors="coerce") <= r[1])]

        q = st.text_input("Búsqueda rápida (todas las columnas)", key=keyp+"q",
                          placeholder="Palabra clave, CT, entidad, título…")
        if q:
            mask = df.astype(str).apply(lambda s: s.str.contains(q, case=False, na=False)).any(axis=1)
            df = df[mask]
        def _is_item_column(col_name: str) -> bool:
            normalized = col_name.strip().lower().replace("í", "i")
            return normalized.startswith("item")

        item_cols_sorted = [c for c in displayable_columns if _is_item_column(c)]

        cols_key = keyp + "cols"
        state_applied_key = keyp + "items_all_applied"

        if item_cols_sorted:
            non_item_cols = [c for c in displayable_columns if c not in item_cols_sorted]
            base_items = item_cols_sorted[:2] if len(item_cols_sorted) >= 2 else item_cols_sorted
            base_default = non_item_cols + base_items
            show_all = st.toggle("➡️ Mostrar todos los Item_n", key=keyp+"toggle_items")

            if cols_key not in st.session_state:
                st.session_state[cols_key] = base_default
                st.session_state[state_applied_key] = False
            st.session_state.setdefault(state_applied_key, False)

            if show_all and not st.session_state.get(state_applied_key, False):
                st.session_state[cols_key] = non_item_cols + item_cols_sorted
                st.session_state[state_applied_key] = True
            elif not show_all and st.session_state.get(state_applied_key, False):
                st.session_state[cols_key] = base_default
                st.session_state[state_applied_key] = False
        else:
            if cols_key not in st.session_state:
                st.session_state[cols_key] = displayable_columns

        selected_cols = st.multiselect(
            "Columnas a mostrar",
            options=displayable_columns,
            key=cols_key,
        )
        if selected_cols:
            cols = [c for c in displayable_columns if c in selected_cols]
        else:
            cols = displayable_columns

    parsed_filtered = {col: series.loc[df.index] for col, series in parsed_date_series.items()}

    df_base = df.copy()

    metric_state_key = keyp + "metric_filter"
    active_metric = st.session_state.get(metric_state_key)
    top_modal_key = keyp + "show_top_unidades"

    metrics_defs = []
    metrics_defs.append({
        "key": "total",
        "label": "Total de actos públicos",
        "count": int(len(df_base)),
        "filter": None,
    })

    public_col = next((c for c in df_base.columns if "public" in c.lower()), None)
    public_series = None
    public_series_normalized = None

    if public_col:
        public_series = _parse_sheet_date_column(df_base[public_col])
        count_public_today = 0
        if public_series.notna().any():
            public_series_normalized = public_series.dt.normalize()
            count_public_today = int((public_series_normalized == today_ts).sum())
        metrics_defs.append({
            "key": "publicados_hoy",
            "label": "Actos publicados hoy",
            "count": count_public_today,
            "filter": "publicados_hoy",
        })

    event_series = None
    event_series_normalized = None
    count_date_today = 0

    if event_date_col:
        event_series = parsed_filtered.get(event_date_col)
        if event_series is not None:
            event_series_normalized = event_series.dt.normalize()
            if event_series_normalized.notna().any():
                count_date_today = int((event_series_normalized == today_ts).sum())

    if event_date_col:
        metrics_defs.append({
            "key": "fecha_hoy",
            "label": "Actos a celebrarse hoy",
            "count": count_date_today,
            "filter": "fecha_hoy",
        })

    metrics_defs.append({
        "key": "top_unidades",
        "label": "Top unidades solicitantes (próximamente)",
        "count": None,
        "filter": None,
        "placeholder": True,
    })

    cols_metrics = st.columns(len(metrics_defs))
    for metric_col, metric in zip(cols_metrics, metrics_defs):
        with metric_col:
            label = metric["label"]
            if metric.get("count") is not None:
                label = f"{label}\n{metric['count']}"

            prefix = "✅ " if active_metric == metric.get("filter") else ""
            if metric["key"] == "total" and active_metric is None:
                prefix = "✅ "

            clicked = st.button(
                prefix + label,
                key=keyp + f"metric_btn_{metric['key']}",
                use_container_width=True,
            )

            if clicked:
                if metric["key"] == "total":
                    st.session_state[metric_state_key] = None
                elif metric.get("placeholder"):
                    st.session_state[top_modal_key] = not st.session_state.get(top_modal_key, False)
                else:
                    current = st.session_state.get(metric_state_key)
                    st.session_state[metric_state_key] = None if current == metric["filter"] else metric["filter"]

    active_metric = st.session_state.get(metric_state_key)

    df = df_base
    if active_metric == "fecha_hoy" and event_series_normalized is not None:
        mask = event_series_normalized == today_ts
        df = df_base.loc[mask.fillna(False)]
    elif active_metric == "publicados_hoy" and public_series_normalized is not None:
        mask = public_series_normalized == today_ts
        df = df_base.loc[mask.fillna(False)]

    if st.session_state.get(top_modal_key):
        st.info("Pronto mostraremos el top de unidades solicitantes con su suma de precio de referencia (últimos 7 días).")

    displayable_columns = [c for c in df.columns if c != ROW_ID_COL]

    table_columns = cols if cols else displayable_columns
    display_df = df[table_columns].copy()

    editable_cols = [c for c in display_df.columns if _is_checkbox_target(c)]
    for col in editable_cols:
        display_df[col] = display_df[col].map(_coerce_to_bool)

    col_cfg = {}
    for date_field in date_columns:
        if date_field in display_df.columns and pd.api.types.is_datetime64_any_dtype(display_df[date_field]):
            col_cfg[date_field] = st.column_config.DateColumn(date_field, help="Fecha")

    for c in money_cols:
        if c in display_df.columns:
            col_cfg[c] = st.column_config.NumberColumn(c, format="B/. %,.2f")

    link_col = next((c for c in display_df.columns if c.strip().lower() in {"enlace", "link", "url"}), None)
    if link_col:
        col_cfg[link_col] = st.column_config.LinkColumn(
            label="🔗",
            display_text="🔗",
            help="Abrir acto en PanamáCompra",
        )

    for col in editable_cols:
        col_cfg[col] = st.column_config.CheckboxColumn(col, help="Sincroniza al marcar", default=False)

    table_height = 620
    disabled_columns = [c for c in display_df.columns if c not in editable_cols]
    disabled_config = {"columns": disabled_columns} if disabled_columns else False

    editor_key = keyp + "editor"

    if editable_cols:
        original_display = display_df.copy()
        edited_df = st.data_editor(
            display_df,
            hide_index=True,
            width="stretch",
            height=table_height,
            column_config=col_cfg,
            disabled=disabled_config,
            key=editor_key,
        )

        changes = []
        for col in editable_cols:
            orig_series = original_display[col].fillna(False).astype(bool)
            new_series = edited_df[col].fillna(False).astype(bool)
            diff_mask = orig_series != new_series
            if diff_mask.any():
                for idx in edited_df.index[diff_mask]:
                    row_number = df.loc[idx, ROW_ID_COL]
                    if pd.isna(row_number):
                        continue
                    changes.append((int(row_number), col, bool(new_series.loc[idx])))

        if changes:
            apply_checkbox_updates(sheet_name, changes)
            st.session_state[notice_key] = len(changes)
            load_df.clear()
            st.rerun()

        df_view = edited_df
    else:
        df_view = display_df
        st.data_editor(
            df_view,
            hide_index=True,
            width="stretch",
            height=table_height,
            column_config=col_cfg,
            disabled=True,
            key=editor_key,
        )

    st.caption(f"Mostrando {len(df)} filas")

    render_pc_state_cards(pc_state_df, pc_config_df, suffix=suffix)


def render_panamacompra_db_panel() -> None:
    """Muestra una vista de las tablas disponibles en la base local panamacompra.db."""
    st.divider()
    st.subheader("Base panamacompra.db")

    db_path = _preferred_db_path()
    if db_path is None:
        st.info("No hay rutas configuradas para la base panamacompra.db.")
        return

    st.caption(f"Origen configurado: `{db_path}`")
    if not db_path.exists():
        st.warning(
            "No pudimos abrir el archivo. Verifica que la ruta "
            "`C:\\Users\\rodri\\GEAPP\\panamacompra.db` existe o define `FINAPP_DB_PATH` "
            "apuntando a una copia local."
        )
        return

    db_path_str = str(db_path)
    try:
        db_tables = list_sqlite_tables(db_path_str)
    except sqlite3.OperationalError as exc:
        st.error(f"No fue posible conectar a la base: {exc}")
        return
    except Exception as exc:
        st.error(f"No fue posible listar las tablas: {exc}")
        return

    if not db_tables:
        st.info("La base panamacompra.db no contiene tablas visibles.")
        return

    selected_table = st.selectbox(
        "Tabla disponible en la base",
        db_tables,
        key="pc_db_table_selector",
    )

    limit = st.slider(
        "L��mite de filas a mostrar",
        min_value=100,
        max_value=5000,
        value=1000,
        step=100,
        help="Ampl��a el l��mite si necesitas revisar m��s registros.",
    )

    try:
        preview_df = load_sqlite_preview(db_path_str, selected_table, limit)
    except sqlite3.OperationalError as exc:
        st.error(f"No se pudo leer la tabla {selected_table}: {exc}")
        return
    except Exception as exc:
        st.error(f"Error al consultar {selected_table}: {exc}")
        return

    total_rows: int | None = None
    try:
        total_rows = count_sqlite_rows(db_path_str, selected_table)
    except sqlite3.OperationalError:
        pass
    except Exception:
        pass

    if preview_df.empty:
        st.info("La consulta no devolvi�� filas para la tabla seleccionada.")
    else:
        st.dataframe(preview_df, use_container_width=True, height=520)

    caption = f"Mostrando hasta {limit} filas."
    if total_rows is not None:
        caption += f" Total en `{selected_table}`: {total_rows:,}."
    st.caption(caption)

# ---- UI: pestañas de categorías + desplegable de hojas ----
pc_state_df = load_pc_state()
pc_config_df = load_pc_config()
ordered_categories = [c for c in CATEGORY_ORDER if c in SHEET_GROUPS]
category_tabs = st.tabs(ordered_categories)

for tab, category_name in zip(category_tabs, ordered_categories):
    with tab:
        st.subheader(category_name)
        sheets = SHEET_GROUPS.get(category_name, [])
        if not sheets:
            st.info("Sin hojas configuradas para esta categoría.")
            continue

        selector_slug = re.sub(r"[^0-9a-z]+", "_", category_name.lower())
        selector_key = f"sheet_selector_{selector_slug.strip('_')}"
        tab_suffix = selector_slug.strip("_") or None

        if len(sheets) == 1:
            sheet_name = sheets[0]
            st.caption(_sheet_label(sheet_name))
        else:
            default_idx = sheets.index(DEFAULT_SHEET) if DEFAULT_SHEET in sheets else 0
            sheet_name = st.selectbox(
                "Seleccione la hoja",
                sheets,
                index=default_idx,
                key=selector_key,
                format_func=_sheet_label,
                label_visibility="collapsed",
            )

        df = load_df(sheet_name)
        if df.empty:
            st.info("Sin datos en esta pestaña.")
        else:
            render_df(df, sheet_name, pc_state_df, pc_config_df, suffix=tab_suffix)

render_panamacompra_db_panel()
