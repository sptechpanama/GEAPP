from sqlalchemy import create_engine, text
from services.otras_fuentes import load_email_health


def test_missing_delivery_migration_does_not_break_the_page():
    engine=create_engine('sqlite://')
    assert load_email_health(engine)=={'available':False}


def test_email_status_distinguishes_real_events_probes_and_recipient_delivery():
    engine=create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE external_email_deliveries (event_id TEXT, status TEXT, updated_at TEXT)'))
        c.execute(text("INSERT INTO external_email_deliveries VALUES "
            "('one','sent','2026-09-15'),('one','sent','2026-09-15'),"
            "('two','pending','2026-09-14'),('verification:1','sent','2026-09-16')"))
        c.execute(text('CREATE TABLE external_email_state (key TEXT,value TEXT)'))
        c.execute(text('INSERT INTO external_email_state VALUES (:key,:value)'),
                  {'key':'smtp_health','value':'{"status":"configuration_missing"}'})
    result=load_email_health(engine)
    statuses={r['status']:r for r in result['statuses']}
    assert result['available']
    assert statuses['sent']['events']==1 and statuses['sent']['deliveries']==2
    assert statuses['pending']['events']==1
    assert result['last_probe_accepted']=='2026-09-16'
    assert result['check']['status']=='configuration_missing'
