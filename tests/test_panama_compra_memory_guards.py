from pathlib import Path


PAGE = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"
INTELLIGENCE_PAGES = (
    Path(__file__).resolve().parents[1]
    / "pages"
    / "inteligencia_oportunidades_proveedores.py",
    Path(__file__).resolve().parents[1] / "pages" / "inteligencia_pc.py",
)


def test_heavy_categories_are_rendered_selectively() -> None:
    source = PAGE.read_text(encoding="utf-8")

    assert "category_tabs = st.tabs(ordered_categories)" not in source
    assert 'key="pc_category_selector"' in source
    assert "for category_name in (selected_category,):" in source


def test_large_sheet_and_drive_caches_are_bounded() -> None:
    source = PAGE.read_text(encoding="utf-8")

    assert "@st.cache_data(ttl=300, max_entries=2, show_spinner=False)\ndef load_df" in source
    assert "@st.cache_data(ttl=600, max_entries=2, show_spinner=False)\ndef load_drive_excel" in source


def test_heavy_reference_panels_require_explicit_loading() -> None:
    source = PAGE.read_text(encoding="utf-8")

    assert 'key="pc_load_database_panel"' in source
    assert 'key="pc_load_fichas_panel"' in source
    assert 'key="pc_load_catalogos_panel"' in source


def test_large_reference_tables_start_paginated() -> None:
    source = PAGE.read_text(encoding="utf-8")

    assert source.count("show_all_default = False") >= 2


def test_sheet_update_check_reads_only_the_date_column() -> None:
    source = PAGE.read_text(encoding="utf-8")
    function_source = source.split("def _latest_sheet_update_by_job", 1)[1].split(
        "\ndef ", 1
    )[0]

    assert "load_df(" not in function_source
    assert 'worksheet.get("1:10")' in function_source
    assert "worksheet.col_values" in function_source


def test_intelligence_dataframe_caches_are_bounded() -> None:
    for page in (*INTELLIGENCE_PAGES, PAGE):
        cache_decorators = [
            line.strip()
            for line in page.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("@st.cache_data")
        ]
        assert cache_decorators
        assert all("max_entries=" in line for line in cache_decorators)
