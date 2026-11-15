from pathlib import Path


PAGE = Path(__file__).resolve().parents[1] / "pages" / "inteligencia_oportunidades_proveedores.py"


def test_initial_page_does_not_run_full_coverage_or_master_query() -> None:
    source = PAGE.read_text(encoding="utf-8")
    status_start = source.index("def _render_data_status")
    status_end = source.index("def _render_master_table", status_start)
    status_source = source[status_start:status_end]
    assert "repository.coverage()" not in status_source
    assert "_build_metadata_cached(" in status_source

    ready_guard = source.index("if not analysis_ready:")
    master_query = source.index("raw_master = _master_data(")
    assert ready_guard < master_query


def test_initial_page_defers_repository_connection_until_requested() -> None:
    source = PAGE.read_text(encoding="utf-8")
    runtime_start = source.rindex("\n_apply_pending_saved_view()\n")
    runtime_source = source[runtime_start:]
    advanced_guard = runtime_source.index("if advanced_filters:")
    first_connection = runtime_source.index("repo = _require_repository()", advanced_guard)
    direct_or_ready = runtime_source.index("if selected_view in direct_views or analysis_ready:")
    assert advanced_guard < first_connection < direct_or_ready


def test_page_renders_only_the_selected_intelligence_view() -> None:
    source = PAGE.read_text(encoding="utf-8")
    runtime_start = source.rindex("\n_apply_pending_saved_view()\n")
    runtime_source = source[runtime_start:]
    assert "selected_view = st.radio(" in runtime_source
    assert "direct_views = {" in runtime_source
    assert "st.tabs(" not in runtime_source
    assert 'if selected_view == "Oportunidades":' in runtime_source


def test_deep_study_history_does_not_depend_on_master_ranking() -> None:
    source = PAGE.read_text(encoding="utf-8")
    runtime_start = source.rindex("\n_apply_pending_saved_view()\n")
    runtime_source = source[runtime_start:]
    deep_catalog = runtime_source.index(
        "study_catalog, catalog_warnings = _deep_study_catalog_data(repo)"
    )
    master_query = runtime_source.index("raw_master = _master_data(")
    assert deep_catalog < master_query
    assert 'elif selected_view == "Estudio profundo":' in runtime_source
    assert "_render_deep_study(study_catalog" in runtime_source


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


def test_direct_lookup_skips_master_sidebar_and_reruns_only_its_fragment() -> None:
    source = PAGE.read_text(encoding="utf-8")
    runtime = source[source.rindex("\n_apply_pending_saved_view()\n"):]
    direct = runtime.index('if selected_view == "Consulta por ficha":')
    sidebar = runtime.index('st.header("Filtros del estudio")')
    assert direct < sidebar
    assert "st.stop()" in runtime[direct:sidebar]
    assert "@st.fragment\ndef _render_direct_ficha_lookup" in source
    assert 'page_acts = acts.iloc[offset:offset + page_size]' in source
    assert '_all_acts_data.clear(ficha, ANALYTICS_REPOSITORY_API_VERSION, repository)' in source
