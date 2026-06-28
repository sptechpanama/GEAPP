from __future__ import annotations

import runpy

from core.config import APP_ROOT


runpy.run_path(
    str(APP_ROOT / "pages" / "inteligencia_ct_proveedores.py"),
    init_globals={"GEAPP_INTEL_PAGE_VARIANT": "flexible"},
    run_name="__main__",
)
