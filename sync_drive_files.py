"""Utility to refresh Drive-backed datasets for the Panamá Compra dashboard."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from core.config import APP_ROOT
from services.panamacompra_drive import (
    ensure_drive_criterios_tecnicos,
    ensure_drive_fichas_ctni,
    ensure_drive_oferentes_catalogos,
    ensure_local_panamacompra_db,
)


DriveFetcher = Callable[..., Optional[Path]]

TARGETS: list[tuple[str, str, DriveFetcher]] = [
    ("panamacompra_db", "panamacompra.db", ensure_local_panamacompra_db),
    ("fichas_ctni", "fichas_ctni.xlsx", ensure_drive_fichas_ctni),
    ("criterios_tecnicos", "criterios_tecnicos.xlsx", ensure_drive_criterios_tecnicos),
    ("oferentes_catalogos", "oferentes_catalogos.xlsx", ensure_drive_oferentes_catalogos),
]

STATUS_FILE = APP_ROOT / "data" / "drive_sync_status.json"


def sync_files(force: bool = False) -> dict[str, str]:
    results: dict[str, str] = {}
    for key, label, fetcher in TARGETS:
        path = None
        try:
            path = fetcher(force=force)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"[ERROR] {label}: {exc}")

        if path and Path(path).exists():
            results[key] = str(path)
            print(f"[OK] {label}: {path}")
        else:
            results[key] = ""
            print(f"[WARN] {label}: no se pudo descargar el archivo.")
    return results


def write_status(data: dict[str, str]) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "files": data,
    }
    STATUS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[LOG] Estado guardado en {STATUS_FILE}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sincroniza archivos de Panamá Compra desde Drive.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Descarga aunque el archivo local parezca estar actualizado.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    files = sync_files(force=args.force)
    write_status(files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
