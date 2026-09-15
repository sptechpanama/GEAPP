from sqlalchemy import create_engine, text
import pytest
from services.external_access import save_access, load_access, access_rows, GUIDES


@pytest.fixture
def engine(tmp_path):
    engine=create_engine('sqlite:///'+str(tmp_path/'access.db'))
    with engine.begin() as c:
        c.execute(text('''CREATE TABLE external_source_access (source TEXT, company TEXT,
            status TEXT,notes TEXT,updated_by TEXT,updated_at TEXT,PRIMARY KEY(source,company))'''))
    yield engine
    engine.dispose()


def test_supplier_status_persists_isolated_by_company_and_source(engine):
    revision=save_access(engine,source='aes',company='RS Engineering',status='Solicitud enviada',notes='Contacto pendiente',actor='rodri')
    rows=load_access(engine)
    assert rows[0]['updated_at']==revision
    assert access_rows(rows,'RS Engineering')[1]['Estado']=='Solicitud enviada'
    assert all(r['Estado']=='Pendiente' for r in access_rows(rows,'SPTech'))
    save_access(engine,source='aes',company='RS Engineering',status='En evaluación',notes='Revisar categorías',actor='otro',expected_updated_at=revision)
    assert load_access(engine)[0]['status']=='En evaluación'


def test_stale_edit_cannot_overwrite_new_status(engine):
    args=dict(source='naturgy',company='SPTech',status='Solicitud enviada',notes='Primera',actor='rodri')
    first=save_access(engine,**args)
    save_access(engine,**{**args,'status':'En evaluación'},expected_updated_at=first)
    with pytest.raises(ValueError,match='Otro usuario'):
        save_access(engine,**args,expected_updated_at=first)
    with pytest.raises(ValueError,match='Otro usuario'):
        save_access(engine,**args)
    assert load_access(engine)[0]['status']=='En evaluación'


@pytest.mark.parametrize('overrides',[{'source':'bad'},{'company':'unknown'},{'status':'success'},{'notes':'x'*1501}])
def test_access_rejects_invalid_updates_without_side_effects(engine,overrides):
    args=dict(source='naturgy',company='SPTech',status='Pendiente',notes='',actor='rodri')
    with pytest.raises(ValueError): save_access(engine,**{**args,**overrides})
    assert load_access(engine)==[]


def test_official_registration_links_and_invitations_are_explicit():
    assert GUIDES['aes']['registration_url'].endswith('/proveedores-potenciales')
    assert 'invitaciones' in GUIDES['naturgy']['access']
    assert 'Ariba por sí sola no da acceso' in GUIDES['aes']['access']
    assert all(len(g['steps'])==4 for g in GUIDES.values())
