from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from services import ct_rotation as rotation
from services.ct_rotation_capture import seed_from_audit
from services.ct_rotation_sources import read_existing_history


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Publica rotación con datos ya extraídos; no ejecuta scrapers.")
    parser.add_argument("--scrapers-root", type=Path, default=Path.home() / "scrapers_repo")
    parser.add_argument("--sheet-id", default=os.environ.get("CT_ROTACION_SHEET_ID", rotation.SHEET_ID))
    parser.add_argument("--seed-audit", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    import gspread
    from google.oauth2.service_account import Credentials

    credentials_file = Path(os.environ.get("ORQUESTADOR_GOOGLE_SERVICE_ACCOUNT", args.scrapers_root / "credentials/service-account.json"))
    credentials = Credentials.from_service_account_file(str(credentials_file), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(credentials)
    snapshot = rotation.load_snapshot(client, sheet_id=args.sheet_id)
    records = snapshot["records"]
    try:
        if args.seed_audit:
            analysis = json.loads((args.seed_audit / "analysis.json").read_text("utf-8"))
            statistics = json.loads((args.seed_audit / "statistics_extra.json").read_text("utf-8"))
            records = rotation.merge_history(records, seed_from_audit(analysis, statistics["summary"]["exact_documented_relaunch_groups"]))
        refreshed, pending = read_existing_history(args.scrapers_root, records, today=rotation.now().date())
        published = refreshed if args.dry_run else rotation.publish_history(client, refreshed, sheet_id=args.sheet_id)
        if not args.dry_run:
            errors = f"{len(pending)} actos extraídos sin cantidad o unidad verificable; se conserva el histórico confirmado." if pending else ""
            rotation.save_refresh_status(client, errors, sheet_id=args.sheet_id)
        summary = rotation.rotation_summary(published, today=rotation.now().date())
        print(json.dumps({
            "fuente": "bases existentes de las corridas normales", "scraping_adicional": False,
            "simulacion": args.dry_run, "convocatorias": len(published),
            "actos_depurados_desde_2025": summary["acts"], "kits_depurados_desde_2025": summary["kits"],
            "actos_pendientes_de_cantidad": pending,
        }, ensure_ascii=False), flush=True)
        return 0
    except Exception:
        if not args.dry_run:
            rotation.save_refresh_status(client, "No se pudo publicar la información ya extraída. Se conserva el histórico anterior.", sheet_id=args.sheet_id)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
