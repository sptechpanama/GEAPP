"""Last verified RIR reads, shared between sessions and persisted in Supabase.

The backup preserves original source/evidence dates. Reading it never renews
supplier confirmations or certifies an act as currently open.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text

PANAMA = ZoneInfo("America/Panama")
TABLE = "rir_app_read_snapshots"
DDL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
    spreadsheet_id TEXT NOT NULL, component TEXT NOT NULL,
    payload TEXT NOT NULL, fingerprint TEXT NOT NULL, checked_at TEXT NOT NULL,
    PRIMARY KEY (spreadsheet_id, component))"""
UPSERT = f"""INSERT INTO {TABLE} (spreadsheet_id, component, payload, fingerprint, checked_at)
    VALUES (:spreadsheet_id, :component, :payload, :fingerprint, :checked_at)
    ON CONFLICT (spreadsheet_id, component) DO UPDATE SET
        payload=excluded.payload, fingerprint=excluded.fingerprint, checked_at=excluded.checked_at
    WHERE {TABLE}.fingerprint <> excluded.fingerprint
      AND {TABLE}.checked_at <= excluded.checked_at"""
SELECT = f"SELECT payload, checked_at FROM {TABLE} WHERE spreadsheet_id=:spreadsheet_id AND component=:component"


class ResearchSnapshotStore:
    def __init__(self, spreadsheet_id: str, local_path: Path, *, engine=None):
        self.spreadsheet_id = spreadsheet_id
        self.local_path = Path(local_path)
        self.engine = engine
        self._remote_ready = False
        self.local_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.local_path) as connection:
            connection.execute(DDL)

    def _prepare_remote(self):
        if self.engine is not None and not self._remote_ready:
            with self.engine.begin() as connection:
                connection.execute(text(DDL))
                if self.engine.dialect.name == "postgresql":
                    connection.execute(text(f"ALTER TABLE {TABLE} ENABLE ROW LEVEL SECURITY"))
                    public_roles = connection.execute(text("SELECT rolname FROM pg_roles WHERE rolname IN ('anon', 'authenticated')")).scalars().all()
                    for role in public_roles:
                        connection.execute(text(f"REVOKE ALL ON {TABLE} FROM {role}"))
            self._remote_ready = True

    def save(self, component: str, payload: dict, *, checked_at=None) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > 12_000_000:
            raise ValueError("El respaldo RIR supera el tamaño permitido")
        at = checked_at or datetime.now(PANAMA).isoformat()
        params = dict(spreadsheet_id=self.spreadsheet_id, component=component,
                      payload=encoded, fingerprint=hashlib.sha256(encoded.encode()).hexdigest(),
                      checked_at=str(at))
        with sqlite3.connect(self.local_path) as connection:
            connection.execute(UPSERT, params)
        if self.engine is not None:
            try:
                self._prepare_remote()
                with self.engine.begin() as connection:
                    connection.execute(text(UPSERT), params)
            except Exception as exc:
                logging.warning("Respaldo RIR remoto pendiente (%s); copia local conservada", type(exc).__name__)

    def load(self, component: str) -> tuple[dict, str] | None:
        params = dict(spreadsheet_id=self.spreadsheet_id, component=component)
        with sqlite3.connect(self.local_path) as connection:
            local = connection.execute(SELECT, params).fetchone()
        if local is not None:
            try:
                return json.loads(local[0]), local[1]
            except (ValueError, TypeError):
                logging.warning("Se descartó un respaldo RIR local ilegible")
        if self.engine is not None:
            try:
                self._prepare_remote()
                with self.engine.connect() as connection:
                    remote = connection.execute(text(SELECT), params).first()
                if remote is not None:
                    payload = json.loads(remote[0])
                    self.save(component, payload, checked_at=remote[1])
                    return payload, remote[1]
            except Exception as exc:
                logging.warning("Respaldo RIR remoto no disponible (%s)", type(exc).__name__)
        return None
