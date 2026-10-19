from copy import deepcopy
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import event_sha256
from test_trace_capacity import case, ingest, next_event  # noqa: F401

from aggregator import trace_payloads, trace_retention


@pytest.mark.parametrize("separated", [False, True])
def test_test_payload_expiry_precedes_older_normal_and_keeps_identity(
    case,  # noqa: F811 - imported fixture
    monkeypatch,
    separated,
):  # noqa: F811
    db, normal = case
    ingest(db, normal)
    test = deepcopy(next_event(normal))
    test["event"]["test_run_id"] = str(uuid4())
    test["event_sha256"] = event_sha256(test["event"])
    accepted = ingest(db, test)
    with db:
        db.execute("UPDATE trace_raw_events SET received_at_ms=ingest_seq")
    if separated:
        trace_payloads.prepare(db)
        while not trace_payloads.migrate_batch(db):
            pass
    monkeypatch.setattr(trace_payloads, "BATCH_ROWS", 1)
    monkeypatch.setattr(trace_retention, "SCAN_ROWS", 1)
    before = db.execute("SELECT count(*) FROM trace_ingest_positions").fetchone()[0]
    result = trace_retention.expire_payloads(
        db, now_ms=trace_retention.RETENTION_MS + 3
    )
    assert result["expired_payloads"] == 1
    assert db.execute(
        "SELECT ingest_seq FROM trace_raw_events WHERE payload_expired_at_ms IS NOT NULL"
    ).fetchall() == [(2,)]
    assert db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 2
    assert (
        db.execute("SELECT count(*) FROM trace_ingest_positions").fetchone()[0]
        == before
    )
    assert ingest(db, test) == accepted
