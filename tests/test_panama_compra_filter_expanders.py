from pathlib import Path


PAGE = Path(__file__).resolve().parents[1] / "pages" / "panama_compra.py"


def test_panama_compra_filter_expanders_start_collapsed() -> None:
    source = PAGE.read_text(encoding="utf-8")

    assert 'with st.expander("🔎 Filtros", expanded=False):' in source
    assert 'with st.expander("Filtros y orden", expanded=False):' in source
    assert 'with st.expander("🔎 Filtros", expanded=True):' not in source
    assert 'with st.expander("Filtros y orden", expanded=True):' not in source
