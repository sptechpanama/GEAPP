from __future__ import annotations

import ast
from pathlib import Path


PAGE = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"
NEW_SHEETS = {
    "cl_abiertas_419_sfd",
    "cl_prog_419_sfd",
    "ap_419_sfd",
}


def _literal_assignment(tree: ast.Module, name: str):
    node = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == name
    )
    return ast.literal_eval(node.value)


def test_law_419_category_is_visible_after_licitaciones() -> None:
    source = PAGE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    groups = _literal_assignment(tree, "SHEET_GROUPS")
    order = _literal_assignment(tree, "CATEGORY_ORDER")

    assert set(groups["Ley 419 sin ficha detectada"]) == NEW_SHEETS
    assert order.index("Ley 419 sin ficha detectada") == order.index("Licitaciones") + 1


def test_new_sheets_feed_rs_sp_but_not_ct_rir() -> None:
    tree = ast.parse(PAGE.read_text(encoding="utf-8"))
    groups = _literal_assignment(tree, "SHEET_GROUPS")
    ct_rir = set(_literal_assignment(tree, "CT_RIR_SCAN_SHEETS"))

    assert NEW_SHEETS.issubset(groups["Actos RS/SP"])
    assert NEW_SHEETS.isdisjoint(ct_rir)


def test_law_419_view_unifies_and_deduplicates_three_sources() -> None:
    source = PAGE.read_text(encoding="utf-8")
    assert 'if category_name == "Ley 419 sin ficha detectada":' in source
    assert "_deduplicate_keyword_hits(" in source
    assert '"ley_419_sfd_unificado"' in source
    assert "Los casos con ley no identificada permanecen" in source


def test_database_preview_uses_human_process_law_label() -> None:
    source = PAGE.read_text(encoding="utf-8")
    panel_start = source.index("def render_panamacompra_db_panel")
    panel_end = source.index("@st.cache_data", panel_start)
    panel = source[panel_start:panel_end]
    assert '"ley_proceso": "Ley del proceso"' in panel
