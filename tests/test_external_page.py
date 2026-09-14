import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from services.external_requests import queue_external_refresh
from services.otras_fuentes import OpportunityFilters, build_search_query


def test_new_page_is_separate_and_panama_compra_loses_only_external_navigation():
    root = Path(__file__).resolve().parents[1]
    pc = (root/'pages/panama_compra.py').read_text(encoding='utf8')
    page = (root/'pages/oportunidades_externas.py').read_text(encoding='utf8')
    assert '_render_otras_fuentes_module' not in pc
    assert '"Otras fuentes": [' not in pc
    assert '"Actos RS/SP": [' in pc and '"CTNI": [' in pc
    assert 'from pages.panama_compra' not in page
    assert 'require_page_access' in page and 'st.tabs(' not in page
    ast.parse(page)


def test_manual_refresh_uses_same_job_and_exact_queue():
    rows=[]
    ws=SimpleNamespace(row_values=lambda n:['request_id','timestamp','job_name','status','requested_by'],
                       append_row=lambda row, **kwargs:rows.append(row))
    def worksheet(name):
        assert name=='pc_manual'
        return ws
    client=SimpleNamespace(open_by_key=lambda key:SimpleNamespace(worksheet=worksheet))
    request_id=queue_external_refresh(client,'sheet-id','user')
    assert rows[0][0]==request_id
    assert rows[0][2:]==['otras_fuentes','pending','user']


def test_invalid_manual_queue_is_not_modified():
    ws=SimpleNamespace(row_values=lambda n:['unrelated'],append_row=lambda *a,**kw:pytest.fail('Should not write'))
    client=SimpleNamespace(open_by_key=lambda key:SimpleNamespace(worksheet=lambda name:ws))
    with pytest.raises(ValueError): queue_external_refresh(client,'sheet-id','user')


def test_manual_refresh_matches_deployed_sheet_headers():
    rows=[]
    ws=SimpleNamespace(row_values=lambda n:['id','job','requested_by','requested_at','status','notes','payload','result_file_id'],
                       append_row=lambda row, **kwargs:rows.append(row))
    client=SimpleNamespace(open_by_key=lambda key:SimpleNamespace(worksheet=lambda name:ws))
    queue_external_refresh(client,'sheet-id','user')
    assert rows[0][1]=='otras_fuentes' and rows[0][4]=='pending'
    assert rows[0][3] and rows[0][5]
    assert rows[0][6:]==['','']


def test_current_view_and_detail_query_are_bounded():
    sql,params=build_search_query(OpportunityFilters(view='current',companies=('RS/SP',),limit=50))
    assert "o.review_bucket IN ('relevant','review')" in sql
    assert params['limit']==50
    assert 'superseded_by' in sql
