import json
from datetime import datetime, timezone
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from services.otras_fuentes import OpportunityFilters, build_search_query, load_review_counts

NOW = datetime(2026,9,14,16,tzinfo=timezone.utc)

@pytest.fixture
def db():
    engine = create_engine('sqlite://')
    columns = '''id source external_id title source_type buyer country publication_date deadline
        status currency matched_company priority source_url first_seen_at last_seen_at cross_source_key
        canonical_url description raw_payload_json'''.split()
    with engine.begin() as c:
        c.execute(text('CREATE TABLE external_opportunities (' + ','.join(k+' TEXT' for k in columns) + ',is_active INTEGER,fit_score REAL,estimated_value REAL)'))
        records = [
            ('open','relevant','2099-09-20','RS/SP','https://a/1','Panamá','2099-09-13',''),
            ('dup','relevant','2099-09-20','RS/SP','https://a/1','Panamá','2099-09-12',''),
            ('expired','relevant','2026-09-13','RS/SP','https://a/2','Panamá','2099-09-13',''),
            ('unknown','review','','RIR','https://a/3','Región','2099-09-13',''),
            ('noise','no_match','2099-09-20','','https://a/4','Global','2099-09-13',''),
            ('both','relevant','2099-09-20','RS/SP + RIR','https://a/5','Panamá','2099-09-13',''),
            ('stale','relevant','2099-09-20','RS/SP','https://a/6','Panamá','2026-09-01',''),
            ('today_elapsed','relevant','2026-09-14','RS/SP','https://a/7','Panamá','2099-09-13','2026-09-14T15:00:00+00:00'),
        ]
        for id,bucket,deadline,company,url,scope,seen,at in records:
            q = {'bucket':bucket,'reason':'evidencia','deadline_date':deadline,'deadline_at':at,'scope':scope,'closed':False}
            data = {k:'' for k in columns}
            data.update(id=id,title=id,external_id=id,source='ungm',source_url=url,canonical_url=url,
                        matched_company=company,raw_payload_json=json.dumps({'qualification':q}),
                        last_seen_at=seen,publication_date='2026-09-12',deadline=deadline,status='Activa',
                        is_active=1,fit_score=42,estimated_value=20000)
            c.execute(text('INSERT INTO external_opportunities ('+','.join(data)+') VALUES ('+','.join(':'+k for k in data)+')'),data)
    yield engine
    engine.dispose()


def search(db, **kwargs):
    query, params = build_search_query(OpportunityFilters(**kwargs),dialect='sqlite',now=NOW)
    return pd.read_sql_query(text(query),db,params=params)


def test_default_view_hides_expired_and_duplicates_but_preserves_history(db):
    assert set(search(db).id) == {'open','both'}
    assert set(search(db,view='historical').id) == {'expired','today_elapsed'}
    assert set(search(db,view='review').id) == {'unknown','stale'}
    assert set(search(db,view='no_match').id) == {'noise'}
    assert len(search(db,view='all')) == 7
    assert len(search(db,view='all',deduplicate=False)) == 8


def test_company_filter_includes_opportunity_for_both_companies(db):
    assert set(search(db,companies=('RIR',)).id) == {'both'}
    assert set(search(db,companies=('RS/SP',)).id) == {'open','both'}


def test_scope_and_search_cannot_inject_sql(db):
    assert set(search(db,view='all',scopes=('Región',)).id) == {'unknown'}
    assert search(db,search="' OR 1=1 --").empty
    assert search(db,view='all',only_active=True).review_bucket.ne('historical').all()


def test_pagination_counts_after_deduplication(db):
    frame = search(db,view='all',limit=2,offset=2)
    assert len(frame) == 2
    assert set(frame.total_resultados) == {7}


def test_legacy_without_metadata_is_reviewed_instead_of_crashing(db):
    with db.begin() as c:
        c.execute(text("UPDATE external_opportunities SET raw_payload_json='{}' WHERE id='open'"))
    assert 'open' in set(search(db,view='review').id)
    assert 'Clasificación pendiente' in search(db,view='review').set_index('id').loc['open','review_reason']


def test_review_counts_are_deduplicated(db):
    counts = load_review_counts(db)
    assert sum(counts.values()) == 7
