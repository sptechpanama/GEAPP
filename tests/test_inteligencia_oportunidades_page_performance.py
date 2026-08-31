from pathlib import Path


PAGE = Path(__file__).resolve().parents[1] / "pages" / "inteligencia_oportunidades_proveedores.py"


def test_initial_page_does_not_run_full_coverage_or_master_query() -> None:
    source = PAGE.read_text(encoding="utf-8")
    status_start = source.index("def _render_data_status")
    status_end = source.index("def _render_master_table", status_start)
    status_source = source[status_start:status_end]
    assert "repository.coverage()" not in status_source
    assert "repository.build_metadata()" in status_source

    ready_guard = source.index('if not st.session_state.get("intel_v3_analysis_ready", False):')
    master_query = source.index("raw_master = _master_data(")
    assert ready_guard < master_query


def test_page_renders_only_the_selected_intelligence_view() -> None:
    source = PAGE.read_text(encoding="utf-8")
    runtime_start = source.rindex("\n_apply_pending_saved_view()\n")
    runtime_source = source[runtime_start:]
    assert "selected_view = st.radio(" in runtime_source
    assert "direct_views = {" in runtime_source
    assert "st.tabs(" not in runtime_source
    assert 'if selected_view == "Oportunidades":' in runtime_source


def test_expensive_filter_catalog_is_opt_in_and_filters_are_batched() -> None:
    source = PAGE.read_text(encoding="utf-8")
    runtime_start = source.rindex("\n_apply_pending_saved_view()\n")
    runtime_source = source[runtime_start:]
    toggle = runtime_source.index('"Cargar filtros avanzados"')
    guarded_load = runtime_source.index("if advanced_filters:", toggle)
    options_query = runtime_source.index("options = _filter_options(repo)", guarded_load)
    form = runtime_source.index('with st.sidebar.form("intel_v3_filters_form"', options_query)
    submit = runtime_source.index('"Aplicar filtros y cargar análisis"', form)
    assert toggle < guarded_load < options_query < form < submit
