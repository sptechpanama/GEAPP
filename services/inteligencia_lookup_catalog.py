from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore, Lock
from time import monotonic

import pandas as pd


COLUMNS = ("ficha", "nombre_ficha", "clase_riesgo", "enlace_minsa")
POLICY = "no_rs_detection_70_v1"
_WORKERS = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ficha-catalog")
_SLOTS = BoundedSemaphore(1)


def _validated_frame(records: list) -> pd.DataFrame:
    if not isinstance(records, list) or not records or len(records) > 50000:
        raise ValueError("Catálogo vacío o fuera de límites")
    frame = pd.DataFrame(records)
    if not set(COLUMNS).issubset(frame.columns):
        raise ValueError("Faltan columnas del catálogo")
    frame = frame.loc[:, list(COLUMNS)].fillna("").astype(str)
    frame["ficha"] = frame["ficha"].str.strip()
    if not frame["ficha"].str.fullmatch(r"\d{3,8}").all() or frame["ficha"].duplicated().any():
        raise ValueError("Códigos inválidos o duplicados")
    if frame.map(len).max().max() > 3000:
        raise ValueError("Valor del catálogo fuera de límites")
    return frame.sort_values("ficha").reset_index(drop=True)


class DirectFichaLookup:
    def __init__(self, repository_factory, *, source_key: str, seed_path: Path, cache_dir: Path | None = None):
        self._factory = repository_factory
        self._repository = None
        self._lock = Lock()
        self._future = None
        self._retry_at = 0.0
        self._failed = False
        self._frame = pd.DataFrame(columns=COLUMNS)
        self._checked_at = ""
        self._origin = "unavailable"
        digest = hashlib.sha256(source_key.encode()).hexdigest()
        self._source_digest = digest
        directory = cache_dir or Path(tempfile.gettempdir()) / "geapp_lookup_catalog"
        self._cache_path = Path(directory) / f"{digest}.json"
        self._load(seed_path, seed=True)
        self._load(self._cache_path, seed=False)

    def _load(self, path: Path, *, seed: bool) -> None:
        try:
            if not path.exists() or path.stat().st_size > 10_000_000:
                return
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("policy") != POLICY:
                return
            if not seed and payload.get("source") != self._source_digest:
                return
            stamp = datetime.fromisoformat(payload["checked_at"])
            if stamp.tzinfo is None:
                return
            if self._checked_at and stamp < datetime.fromisoformat(self._checked_at):
                return
            frame = _validated_frame(payload["records"])
            self._frame = frame
            self._checked_at = stamp.astimezone(timezone.utc).isoformat()
            self._origin = "backup"
        except (OSError, ValueError, KeyError, TypeError):
            logging.warning("Se omitió un respaldo de catálogo ilegible")

    def _get_repository(self):
        with self._lock:
            repository = self._repository
        if repository is not None:
            return repository
        created = self._factory()
        with self._lock:
            if self._repository is None:
                self._repository = created
            repository = self._repository
        if created is not repository:
            created.close()
        return repository

    def _save(self, payload: dict) -> None:
        temporary = None
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self._cache_path.parent, delete=False) as handle:
                temporary = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False)
            os.replace(temporary, self._cache_path)
        except OSError:
            logging.warning("No se pudo persistir el catálogo; copia en memoria conservada")
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    logging.warning("No se pudo retirar el archivo temporal del catálogo")

    def _refresh(self) -> None:
        try:
            frame = _validated_frame(self._get_repository().ficha_search_options().to_dict("records"))
            checked_at = datetime.now(timezone.utc).isoformat()
            self._save({"policy": POLICY, "source": self._source_digest, "checked_at": checked_at, "records": frame.to_dict("records")})
            with self._lock:
                self._frame = frame
                self._checked_at = checked_at
                self._origin = "database"
                self._failed = False
                self._retry_at = monotonic() + 3600
        except Exception as exc:
            logging.warning("Catálogo remoto no disponible (%s); respaldo conservado", type(exc).__name__)
            with self._lock:
                self._failed = True
                self._retry_at = monotonic() + 60
        finally:
            _SLOTS.release()

    def catalog_snapshot(self) -> pd.DataFrame:
        with self._lock:
            pending = self._future is not None and not self._future.done()
            if not pending and monotonic() >= self._retry_at and _SLOTS.acquire(blocking=False):
                try:
                    self._future = _WORKERS.submit(self._refresh)
                    pending = True
                except Exception:
                    _SLOTS.release()
                    self._failed = True
                    self._retry_at = monotonic() + 60
            result = self._frame.copy()
            result.attrs.update(catalog_origin=self._origin, catalog_checked_at=self._checked_at, catalog_failed=self._failed, catalog_pending=pending)
            return result

    def retry_catalog(self) -> None:
        with self._lock:
            self._retry_at = 0

    def all_acts_for_ficha(self, ficha: str) -> pd.DataFrame:
        return self._get_repository().all_acts_for_ficha(ficha)

    def price_benchmarks_for_fichas(self, fichas: tuple[str, ...]) -> pd.DataFrame:
        return self._get_repository().price_benchmarks_for_fichas(fichas)
