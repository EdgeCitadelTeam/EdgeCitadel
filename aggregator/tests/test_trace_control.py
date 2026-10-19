import json
import sqlite3
from uuid import uuid4

import aggregator.trace_control as control
import pytest
from aggregator.trace_store import initialize


@pytest.fixture
def case():
    db = sqlite3.connect(":memory:")
    initialize(db)
    request = {
        "schema_version": 2,
        "request_id": str(uuid4()),
        "node_id": "edge-a",
        "source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
        "after_export_seq": 0,
        "collector_epoch": None,
    }
    yield db, request, control.SettlementResponder(db)
    db.close()


@pytest.mark.parametrize(
    "data",
    [b"x" * 1025, b"[", b'{"request_id":1}', b'{"request_id":"x","request_id":"y"}'],
)
def test_untrusted_request_is_dropped_without_database_work(case, data):
    db, _, responder = case
    statements = []
    db.set_trace_callback(statements.append)
    assert responder.reply(data) is None
    assert not statements


def test_version_validation_and_rate_limit_have_correlated_errors(case, monkeypatch):
    _, req, responder = case
    assert (
        responder.reply(json.dumps({**req, "schema_version": 1}).encode())["code"]
        == "unsupported_version"
    )
    assert (
        responder.reply(json.dumps({**req, "after_export_seq": -1}).encode())["code"]
        == "invalid_request"
    )
    monkeypatch.setattr(control.time, "monotonic", lambda: responder.updated)
    data = json.dumps(req).encode()
    for _ in range(20):
        assert responder.reply(data)["code"] == "unknown_source"
    reply = responder.reply(data)
    assert reply["code"] == "rate_limited" and reply["request_id"] == req["request_id"]


def test_dense_pages_make_progress_and_query_interrupt_does_not_leave_transaction(
    case, monkeypatch
):
    db, req, responder = case
    with db:
        db.executemany(
            "INSERT INTO trace_ingest_positions VALUES(?,?,?,?,?,?,'accepted',0,?)",
            [
                (
                    req["node_id"],
                    req["source_epoch"],
                    req["export_generation"],
                    i,
                    str(uuid4()),
                    "0" * 64,
                    i,
                )
                for i in range(1, 1501)
            ],
        )
    monkeypatch.setattr(control, "QUERY_STEPS", 0)
    assert (
        responder.reply(json.dumps(req).encode())["code"] == "temporarily_unavailable"
    )
    assert not db.in_transaction
    monkeypatch.setattr(control, "QUERY_STEPS", 100_000)
    positions = []
    while True:
        reply = responder.reply(json.dumps(req).encode())
        assert reply["status"] == "ok"
        page = reply["page"]
        positions.append(page["settled_export_seq"])
        if not page["more"]:
            break
        req.update(
            after_export_seq=page["settled_export_seq"],
            collector_epoch=page["collector_epoch"],
            request_id=str(uuid4()),
        )
    assert positions == [512, 1024, 1500]
    assert not db.in_transaction
