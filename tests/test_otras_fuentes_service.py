from __future__ import annotations

from services.otras_fuentes import OpportunityFilters, build_search_query


def test_search_query_applies_every_filter_without_interpolating_values():
    filters = OpportunityFilters(
        search="solar' OR 1=1 --",
        sources=("acp", "ensa"),
        companies=("RS/SP",),
        statuses=("Activa",),
        priorities=("Alta",),
        start_date="2026-01-01",
        end_date="2026-08-28",
        only_active=True,
        sort_by="published_desc",
        limit=99999,
        offset=150,
    )
    query, params = build_search_query(filters)
    assert "solar' OR 1=1 --" not in query
    assert params["search"] == "%solar' or 1=1 --%"
    assert params["limit"] == 500
    assert params["offset"] == 150
    assert "o.is_active = 1" in query
    assert "o.publication_date" in query
    assert "LIMIT :limit OFFSET :offset" in query
    assert params["source_0"] == "acp"
    assert params["source_1"] == "ensa"


def test_empty_filters_keep_query_valid():
    query, params = build_search_query(OpportunityFilters())
    assert "WHERE 1=1" in query
    assert params == {"limit": 100, "offset": 0}


def test_sort_order_is_selected_from_safe_allowlist():
    recent_query, _ = build_search_query(
        OpportunityFilters(sort_by="published_desc")
    )
    amount_query, _ = build_search_query(
        OpportunityFilters(sort_by="amount_desc")
    )
    invalid_query, _ = build_search_query(
        OpportunityFilters(sort_by="o.title; DROP TABLE external_opportunities")
    )

    assert "o.publication_date" in recent_query
    assert "o.estimated_value DESC NULLS LAST" in amount_query
    assert "DROP TABLE" not in invalid_query
    assert "o.publication_date" in invalid_query
