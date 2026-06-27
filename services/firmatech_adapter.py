from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests
import streamlit as st


class FirmatechError(RuntimeError):
    """Base error for Firmatech integration helpers."""


class FirmatechConfigurationError(FirmatechError):
    """Raised when the provider is not configured yet."""


@dataclass(slots=True)
class FirmatechSettings:
    provider_name: str = "Firmatech"
    base_url: str = ""
    issue_path: str = ""
    status_path: str = ""
    cancel_path: str = ""
    auth_header: str = "Authorization"
    auth_scheme: str = "Bearer"
    api_key: str = ""
    api_token: str = ""
    timeout_seconds: int = 30
    mode: str = "stub"
    extra_headers: dict[str, str] = field(default_factory=dict)

    @property
    def auth_value(self) -> str:
        token = str(self.api_token or self.api_key or "").strip()
        if not token:
            return ""
        prefix = str(self.auth_scheme or "").strip()
        return f"{prefix} {token}".strip() if prefix else token

    @property
    def is_configured(self) -> bool:
        return bool(str(self.base_url).strip() and self.auth_value)


def load_firmatech_settings() -> FirmatechSettings:
    raw = dict(st.secrets.get("firmatech", {}))
    extra_headers = raw.get("extra_headers", {}) or {}
    if not isinstance(extra_headers, dict):
        extra_headers = {}
    return FirmatechSettings(
        provider_name=str(raw.get("provider_name") or "Firmatech").strip() or "Firmatech",
        base_url=str(raw.get("base_url") or "").strip(),
        issue_path=str(raw.get("issue_path") or "").strip(),
        status_path=str(raw.get("status_path") or "").strip(),
        cancel_path=str(raw.get("cancel_path") or "").strip(),
        auth_header=str(raw.get("auth_header") or "Authorization").strip() or "Authorization",
        auth_scheme=str(raw.get("auth_scheme") or "Bearer").strip(),
        api_key=str(raw.get("api_key") or "").strip(),
        api_token=str(raw.get("api_token") or "").strip(),
        timeout_seconds=int(raw.get("timeout_seconds") or 30),
        mode=str(raw.get("mode") or "stub").strip().lower() or "stub",
        extra_headers={str(k): str(v) for k, v in extra_headers.items()},
    )


def provider_health(settings: FirmatechSettings | None = None) -> dict[str, Any]:
    cfg = settings or load_firmatech_settings()
    missing: list[str] = []
    if not cfg.base_url:
        missing.append("base_url")
    if not (cfg.api_token or cfg.api_key):
        missing.append("api_token/api_key")
    if not cfg.issue_path:
        missing.append("issue_path")
    return {
        "provider": cfg.provider_name,
        "mode": cfg.mode,
        "configured": cfg.is_configured and not missing and cfg.mode != "stub",
        "can_prepare": True,
        "can_emit": cfg.is_configured and bool(cfg.issue_path) and cfg.mode != "stub",
        "missing": missing,
        "base_url": cfg.base_url,
        "issue_path": cfg.issue_path,
        "status_path": cfg.status_path,
        "cancel_path": cfg.cancel_path,
    }


def validate_invoice_payload(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    document = payload.get("document", {}) or {}
    customer = payload.get("customer", {}) or {}
    totals = payload.get("totals", {}) or {}
    lines = payload.get("lines", []) or []

    if not str(document.get("internal_id") or "").strip():
        errors.append("Falta document.internal_id.")
    if not str(document.get("issue_date") or "").strip():
        errors.append("Falta document.issue_date.")
    if not str(document.get("currency") or "").strip():
        errors.append("Falta document.currency.")
    if not str(customer.get("name") or "").strip():
        errors.append("Falta customer.name.")
    if not str(customer.get("ruc") or "").strip():
        errors.append("Falta customer.ruc.")
    if not isinstance(lines, list) or not lines:
        errors.append("Debe existir al menos una linea.")
    for idx, line in enumerate(lines, start=1):
        if not str(line.get("description") or "").strip():
            errors.append(f"Linea {idx}: falta description.")
        try:
            qty = float(line.get("quantity") or 0)
            unit_price = float(line.get("unit_price") or 0)
        except Exception:
            qty = 0
            unit_price = 0
        if qty <= 0:
            errors.append(f"Linea {idx}: quantity debe ser mayor que 0.")
        if unit_price < 0:
            errors.append(f"Linea {idx}: unit_price no puede ser negativo.")
    try:
        total = float(totals.get("total") or 0)
    except Exception:
        total = 0
    if total <= 0:
        errors.append("totals.total debe ser mayor que 0.")
    return errors


def build_invoice_payload_from_draft(draft: dict[str, Any]) -> dict[str, Any]:
    lines_in = draft.get("lines", []) or []
    lines_out: list[dict[str, Any]] = []
    subtotal = 0.0
    tax_total = 0.0

    for row in lines_in:
        try:
            quantity = float(row.get("quantity") or 0)
        except Exception:
            quantity = 0.0
        try:
            unit_price = float(row.get("unit_price") or 0)
        except Exception:
            unit_price = 0.0
        try:
            tax_rate = float(row.get("tax_rate") or 0)
        except Exception:
            tax_rate = 0.0
        line_subtotal = round(quantity * unit_price, 2)
        line_tax = round(line_subtotal * (tax_rate / 100.0), 2)
        line_total = round(line_subtotal + line_tax, 2)
        subtotal += line_subtotal
        tax_total += line_tax
        lines_out.append(
            {
                "sku": str(row.get("sku") or "").strip(),
                "description": str(row.get("description") or "").strip(),
                "quantity": quantity,
                "unit_price": round(unit_price, 2),
                "tax_rate": round(tax_rate, 2),
                "line_subtotal": line_subtotal,
                "line_tax": line_tax,
                "line_total": line_total,
            }
        )

    subtotal = round(subtotal, 2)
    tax_total = round(tax_total, 2)
    total = round(subtotal + tax_total, 2)

    return {
        "document": {
            "type": "invoice",
            "internal_id": str(draft.get("invoice_id") or "").strip(),
            "series": str(draft.get("series") or "").strip(),
            "issue_date": str(draft.get("issue_date") or "").strip(),
            "due_date": str(draft.get("due_date") or "").strip(),
            "payment_terms": str(draft.get("payment_terms") or "").strip(),
            "currency": str(draft.get("currency") or "USD").strip() or "USD",
            "notes": str(draft.get("notes") or "").strip(),
        },
        "issuer": {
            "company": str(draft.get("company") or "").strip(),
        },
        "customer": {
            "client_id": str(draft.get("client_id") or "").strip(),
            "name": str(draft.get("client_name") or "").strip(),
            "ruc": str(draft.get("client_ruc") or "").strip(),
            "dv": str(draft.get("client_dv") or "").strip(),
            "email": str(draft.get("client_email") or "").strip(),
            "address": str(draft.get("client_address") or "").strip(),
        },
        "project": {
            "project_id": str(draft.get("project_id") or "").strip(),
            "project_name": str(draft.get("project_name") or "").strip(),
        },
        "origin": {
            "source": str(draft.get("source") or "").strip(),
            "source_ref": str(draft.get("source_ref") or "").strip(),
        },
        "totals": {
            "subtotal": subtotal,
            "tax": tax_total,
            "total": total,
        },
        "lines": lines_out,
        "metadata": {
            "status_local": str(draft.get("status_local") or "").strip(),
            "created_by": str(draft.get("created_by") or "").strip(),
        },
    }


class FirmatechClient:
    def __init__(self, settings: FirmatechSettings | None = None) -> None:
        self.settings = settings or load_firmatech_settings()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.settings.auth_value:
            headers[self.settings.auth_header] = self.settings.auth_value
        headers.update(self.settings.extra_headers)
        return headers

    def _assert_issue_ready(self) -> None:
        if self.settings.mode == "stub":
            raise FirmatechConfigurationError(
                "La integracion esta en modo stub. Falta conectar el endpoint real del API."
            )
        if not self.settings.base_url:
            raise FirmatechConfigurationError("Falta firmatech.base_url en secrets.")
        if not self.settings.issue_path:
            raise FirmatechConfigurationError("Falta firmatech.issue_path en secrets.")
        if not self.settings.auth_value:
            raise FirmatechConfigurationError("Falta token o api key de Firmatech.")

    def issue_invoice(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._assert_issue_ready()
        errors = validate_invoice_payload(payload)
        if errors:
            raise FirmatechError("Payload invalido: " + " | ".join(errors))
        url = f"{self.settings.base_url.rstrip('/')}/{self.settings.issue_path.lstrip('/')}"
        response = requests.post(
            url,
            json=payload,
            headers=self._headers(),
            timeout=self.settings.timeout_seconds,
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            return {"raw_text": response.text, "status_code": response.status_code}
