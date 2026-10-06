from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from services import ct_rotation as rotation
from services.ct_rotation_capture import OfficialCapture, seed_from_audit


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scrapers-root", type=Path, default=Path.home() / "scrapers_repo")
    parser.add_argument("--sheet-id", default=os.environ.get("CT_ROTACION_SHEET_ID", rotation.SHEET_ID))
    parser.add_argument("--seed-audit", type=Path)
    args = parser.parse_args(argv)
    import gspread
    from google.oauth2.service_account import Credentials

    credentials_file = Path(os.environ.get("ORQUESTADOR_GOOGLE_SERVICE_ACCOUNT", args.scrapers_root / "credentials/service-account.json"))
    credentials = Credentials.from_service_account_file(str(credentials_file), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(credentials)
    snapshot = rotation.load_snapshot(client, sheet_id=args.sheet_id)
    records = snapshot["records"]
    if args.seed_audit:
        analysis = json.loads((args.seed_audit / "analysis.json").read_text("utf-8"))
        statistics = json.loads((args.seed_audit / "statistics_extra.json").read_text("utf-8"))
        records = rotation.merge_history(records, seed_from_audit(analysis, statistics["summary"]["exact_documented_relaunch_groups"]))
    cache = args.scrapers_root / "data/ct_rotacion_43358"
    capture = OfficialCapture(args.scrapers_root, cache, credentials_file)
    if args.seed_audit:
        source_records = json.loads((args.seed_audit / "official_search_records.json").read_text("utf-8"))
        import hashlib

        for record in source_records:
            flow = str(record["idProcesosContratacionFlujos"])
            original = args.seed_audit / "details" / f"{flow}.json"
            target = cache / f"{flow}.json"
            if not original.exists() or target.exists():
                continue
            components = json.loads(original.read_text("utf-8"))["result"]["pageComponentes"]
            values = {
                "record": record, "signature": hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
                "checked_at": analysis["summary"]["checked_at"], "labels": capture.api._component_labels(components),
                "items": [item for component in components if component.get("tipo") in {"componentItems", "componentItemsPliego"} for item in capture.api._component_rows(component)],
                "files": [item for component in components if component.get("tipo") == "componentFiles" for item in capture.api._component_rows(component)],
                "relations": [item for component in components if component.get("tipo") == "componentInfoGeneracion" for item in (component.get("value") or []) if isinstance(item, dict)],
            }
            target.write_text(json.dumps(values, ensure_ascii=False), encoding="utf-8")
        if (args.seed_audit / "document_evidence.json").exists():
            proofs = json.loads((args.seed_audit / "document_evidence.json").read_text("utf-8"))
            for proof in proofs:
                source = cache / f"{proof['flow']}.json"
                if not source.exists():
                    continue
                detail = json.loads(source.read_text("utf-8"))
                key = hashlib.sha256(json.dumps(detail["files"], sort_keys=True).encode()).hexdigest()
                target = cache / f"docs_{key}.json"
                if not target.exists():
                    target.write_text(json.dumps({"confirmed": bool(proof.get("documents") or proof.get("prior_verified")), "requisition": ""}), encoding="utf-8")
    try:
        refreshed = capture.capture(records)
        published = rotation.publish_history(client, refreshed, sheet_id=args.sheet_id)
        unique_errors = sorted(set(capture.errors))
        errors = "; ".join(unique_errors[:10])
        if len(unique_errors) > 10:
            errors += f"; y {len(unique_errors) - 10} incidencias adicionales."
        (cache / "last_capture.json").write_text(json.dumps({"checked_at": rotation.now().isoformat(), "errors": capture.errors, "convocations": len(published)}, ensure_ascii=False), encoding="utf-8")
        rotation.save_refresh_status(client, errors, sheet_id=args.sheet_id)
        summary = rotation.rotation_summary(published, today=rotation.now().date())
        print(json.dumps({"convocatorias": len(published), "actos_depurados_desde_2025": summary["acts"], "kits_depurados_desde_2025": summary["kits"], "incidencias": len(capture.errors)}, ensure_ascii=False), flush=True)
        return 1 if capture.errors else 0
    except Exception:
        rotation.save_refresh_status(client, "No se pudo completar la captura o publicación. Se conserva el histórico anterior.", sheet_id=args.sheet_id)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
