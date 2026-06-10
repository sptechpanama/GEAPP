from __future__ import annotations

import json
import unicodedata
import uuid
from calendar import monthrange
from datetime import date

import pandas as pd


COL_FECHA = "Fecha"
COL_DESC = "Descripcion"
COL_CONC = "Concepto"
COL_MONTO = "Monto"
COL_CAT = "Categoria"
COL_ESC = "Escenario"
COL_PROY = "Proyecto"
COL_CLI_ID = "ClienteID"
COL_CLI_NOM = "ClienteNombre"
COL_EMP = "Empresa"
COL_FPAGO = "Fecha esperada de pago"
COL_FPAGO_REAL = "Fecha real de pago"
COL_REC = "Recurrente"
COL_REC_PER = "Periodo recurrencia"
COL_REC_REG = "Regla fecha recurrencia"
COL_REC_DUR = "Duracion recurrencia"
COL_REC_HASTA = "Recurrencia hasta fecha"
COL_REC_CANT = "Recurrencia cantidad periodos"
COL_ROWID = "RowID"
COL_REF_RID = "Ref RowID Ingreso"
COL_POR_PAG = "Por_pagar"
COL_PROV = "Proveedor"
COL_USER = "Usuario"
COL_GAS_SUB = "Subclasificacion gerencial"
COL_GAS_DET = "Detalle gasto"
COL_TRAT_BAL_GAS = "Tratamiento balance gasto"
COL_PAGO_REAL_MONTO = "Monto real pagado"
COL_GAS_PARTIALS = "Detalle pagos parciales"

AUTO_PAYMENT_NOTE = "Pago automático por gasto recurrente"
SUPPORTED_BALANCE_TREATMENTS = {"", "Gasto del periodo"}


def _ts(value) -> pd.Timestamp | pd.NaT:
    ts = pd.to_datetime(value, errors="coerce")
    return ts if not pd.isna(ts) else pd.NaT


def _norm_text(value) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _yes_no_norm(value) -> str:
    raw = _norm_text(value)
    return "Si" if raw in {"si", "sí", "s", "yes", "true", "1"} else "No"


def _num(value, default: float = 0.0) -> float:
    series = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(default)
    return float(series.iloc[0])


def _parse_partial_events(raw_value) -> list[dict]:
    try:
        data = json.loads(str(raw_value or "[]"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    rows: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        fecha = _ts(item.get("fecha"))
        monto = _num(item.get("monto", 0.0))
        nota = str(item.get("nota", "") or "").strip()
        if pd.isna(fecha) or monto <= 0:
            continue
        rows.append({"fecha": fecha, "monto": monto, "nota": nota})
    rows.sort(key=lambda x: x["fecha"])
    return rows


def _serialize_partial_events(entries: list[dict]) -> str:
    payload: list[dict[str, object]] = []
    for entry in entries:
        fecha = _ts(entry.get("fecha"))
        monto = _num(entry.get("monto", 0.0))
        nota = str(entry.get("nota", "") or "").strip()
        if pd.isna(fecha) or monto <= 0:
            continue
        payload.append({"fecha": fecha.date().isoformat(), "monto": monto, "nota": nota})
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return "[]"


def _event_allowed(row: pd.Series, event_index: int, event_date: pd.Timestamp) -> bool:
    duration = _norm_text(row.get(COL_REC_DUR, ""))
    until_date = _ts(row.get(COL_REC_HASTA))
    count_limit = int(_num(row.get(COL_REC_CANT, 0), default=0.0))
    if "hasta fecha" in duration and pd.notna(until_date):
        return event_date.normalize() <= until_date.normalize()
    if "cantidad" in duration and count_limit > 0:
        return event_index <= count_limit
    return True


def _recurrence_interval_months(raw_value) -> int:
    key = _norm_text(raw_value)
    if "semestr" in key:
        return 6
    return 1


def _due_date_for_period(period: pd.Period, base_date: pd.Timestamp, rule_raw) -> pd.Timestamp:
    rule = _norm_text(rule_raw)
    max_day = monthrange(int(period.year), int(period.month))[1]
    if "inicio" in rule:
        day = 1
    elif "15" in rule:
        day = 15
    else:
        day = min(max(1, int(base_date.day)), max_day)
    return pd.Timestamp(year=int(period.year), month=int(period.month), day=day)


def _next_due_date(current_due: pd.Timestamp, row: pd.Series) -> pd.Timestamp | pd.NaT:
    due = _ts(current_due)
    if pd.isna(due):
        return pd.NaT
    period_key = _norm_text(row.get(COL_REC_PER, "Mensual"))
    if "quinc" in period_key or "15nal" in period_key:
        if int(due.day) < 15:
            max_day = monthrange(int(due.year), int(due.month))[1]
            return pd.Timestamp(year=int(due.year), month=int(due.month), day=min(15, max_day))
        next_month = due.to_period("M") + 1
        return pd.Timestamp(year=int(next_month.year), month=int(next_month.month), day=1)
    next_period = due.to_period("M") + _recurrence_interval_months(row.get(COL_REC_PER, "Mensual"))
    return _due_date_for_period(next_period, due, row.get(COL_REC_REG, "Inicio de cada mes"))


def _base_due_date(row: pd.Series) -> pd.Timestamp | pd.NaT:
    due = _ts(row.get(COL_FPAGO))
    if pd.isna(due):
        due = _ts(row.get(COL_FECHA))
    return due.normalize() if not pd.isna(due) else pd.NaT


def _is_supported_recurring_source(row: pd.Series) -> bool:
    if _yes_no_norm(row.get(COL_REC, "No")) != "Si":
        return False
    if _yes_no_norm(row.get(COL_POR_PAG, "Sí")) == "No":
        return False
    if _num(row.get(COL_MONTO, 0.0)) <= 0:
        return False
    if _num(row.get(COL_PAGO_REAL_MONTO, 0.0)) > 0:
        return False
    if pd.notna(_ts(row.get(COL_FPAGO_REAL))):
        return False
    if _parse_partial_events(row.get(COL_GAS_PARTIALS)):
        return False
    treatment = str(row.get(COL_TRAT_BAL_GAS, "") or "").strip()
    if treatment not in SUPPORTED_BALANCE_TREATMENTS:
        return False
    return True


def _build_paid_row(source_row: pd.Series, due_date: pd.Timestamp, *, current_user: str = "") -> dict:
    monto = _num(source_row.get(COL_MONTO, 0.0))
    desc = str(source_row.get(COL_DESC, "") or "").strip()
    conc = str(source_row.get(COL_CONC, "") or desc).strip() or desc
    note = AUTO_PAYMENT_NOTE
    return {
        **source_row.to_dict(),
        COL_ROWID: uuid.uuid4().hex,
        COL_FECHA: due_date,
        COL_DESC: desc,
        COL_CONC: conc,
        COL_ESC: "Real",
        COL_REF_RID: str(source_row.get(COL_ROWID, "") or "").strip(),
        COL_POR_PAG: "No",
        COL_REC: "No",
        COL_REC_PER: "",
        COL_REC_REG: "",
        COL_REC_DUR: "",
        COL_REC_HASTA: pd.NaT,
        COL_REC_CANT: 0,
        COL_FPAGO: due_date,
        COL_FPAGO_REAL: due_date,
        COL_PAGO_REAL_MONTO: monto,
        COL_GAS_PARTIALS: _serialize_partial_events([{"fecha": due_date, "monto": monto, "nota": note}]),
        COL_USER: str(current_user or source_row.get(COL_USER, "") or "").strip(),
    }


def materialize_due_recurring_gastos(
    df_gas: pd.DataFrame,
    *,
    today: date | pd.Timestamp | None = None,
    current_user: str = "",
) -> tuple[pd.DataFrame, list[dict]]:
    if not isinstance(df_gas, pd.DataFrame) or df_gas.empty:
        return (df_gas.copy() if isinstance(df_gas, pd.DataFrame) else pd.DataFrame()), []

    work = df_gas.copy()
    for col, default in (
        (COL_ROWID, ""),
        (COL_REF_RID, ""),
        (COL_REC, "No"),
        (COL_POR_PAG, "Sí"),
        (COL_PAGO_REAL_MONTO, 0.0),
        (COL_GAS_PARTIALS, ""),
        (COL_TRAT_BAL_GAS, ""),
        (COL_FPAGO, pd.NaT),
        (COL_FPAGO_REAL, pd.NaT),
        (COL_REC_PER, ""),
        (COL_REC_REG, ""),
        (COL_REC_DUR, ""),
        (COL_REC_HASTA, pd.NaT),
        (COL_REC_CANT, 0),
    ):
        if col not in work.columns:
            work[col] = default

    today_norm = _ts(today or pd.Timestamp.today())
    if pd.isna(today_norm):
        today_norm = pd.Timestamp.today()
    today_norm = today_norm.normalize()

    generated_rows: list[dict] = []
    rows_to_drop: list[int] = []
    summary: list[dict] = []

    for idx, source_row in work.copy().iterrows():
        if not _is_supported_recurring_source(source_row):
            continue
        source_rowid = str(source_row.get(COL_ROWID, "") or "").strip()
        if not source_rowid:
            continue
        due_date = _base_due_date(source_row)
        if pd.isna(due_date) or due_date > today_norm:
            continue

        linked_mask = work[COL_REF_RID].astype(str).str.strip().eq(source_rowid)
        existing_linked = work.loc[linked_mask].copy()
        existing_dates = set(
            pd.to_datetime(existing_linked.get(COL_FPAGO_REAL), errors="coerce").dropna().dt.normalize().tolist()
        )
        materialized_count = len(existing_dates)
        generated_for_source = 0
        last_due = pd.NaT

        while pd.notna(due_date) and due_date <= today_norm:
            event_index = materialized_count + 1
            if not _event_allowed(source_row, event_index, due_date):
                due_date = pd.NaT
                break
            if due_date not in existing_dates:
                generated_rows.append(_build_paid_row(source_row, due_date, current_user=current_user))
                existing_dates.add(due_date)
                generated_for_source += 1
            materialized_count += 1
            last_due = due_date
            next_due = _next_due_date(due_date, source_row)
            if pd.isna(next_due):
                due_date = pd.NaT
                break
            next_index = materialized_count + 1
            if not _event_allowed(source_row, next_index, next_due):
                due_date = pd.NaT
                break
            due_date = next_due.normalize()

        if generated_for_source <= 0 and pd.isna(last_due):
            continue

        if pd.notna(due_date):
            work.at[idx, COL_FECHA] = due_date
            work.at[idx, COL_FPAGO] = due_date
            work.at[idx, COL_POR_PAG] = "Sí"
            work.at[idx, COL_FPAGO_REAL] = pd.NaT
            work.at[idx, COL_PAGO_REAL_MONTO] = 0.0
            work.at[idx, COL_GAS_PARTIALS] = ""
        else:
            rows_to_drop.append(idx)

        summary.append(
            {
                "rowid": source_rowid,
                "descripcion": str(source_row.get(COL_CONC, "") or source_row.get(COL_DESC, "") or "").strip(),
                "materializados": generated_for_source,
                "ultimo_pago": last_due,
                "proximo_pago": due_date if pd.notna(due_date) else pd.NaT,
            }
        )

    if not generated_rows and not rows_to_drop:
        return work, []

    if rows_to_drop:
        work = work.drop(index=rows_to_drop).reset_index(drop=True)
    if generated_rows:
        work = pd.concat([work, pd.DataFrame(generated_rows)], ignore_index=True, sort=False)
    return work, summary
