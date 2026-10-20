import hashlib
import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import TraceContractError, canonical_bytes

from aggregator import trace_store
from aggregator.trace_ingest import ingest_wire


@pytest.fixture
def record():
    path = (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/events.v1.json"
    )
    event = json.loads(path.read_text())["fixtures"][0]["event"]
    return {
        "schema_version": 1,
        "node_id": event["node_id"],
        "source_epoch": event["source_epoch"],
        "export_generation": str(uuid4()),
        "export_seq": 1,
        "event_sha256": digest(event),
        "event": event,
    }


def digest(event):
    return hashlib.sha256(canonical_bytes(event)).hexdigest()


def encode(record):
    record["event_sha256"] = digest(record["event"])
    return json.dumps(record).encode()


@pytest.fixture
def connection(tmp_path):
    conn = sqlite3.connect(tmp_path / "core.db")
    trace_store.initialize(conn)
    try:
        yield conn
    finally:
        conn.close()


def deliver(connection, payload, subject="edgecitadel.telemetry.v1.edge-a"):
    return ingest_wire(connection, subject, payload, received_at_ms=1000)


def count(conn, table):
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


@pytest.mark.parametrize(
    "payload",
    [
        b"private-secret-not-json",
        b"\xff",
        b"[]",
        b'{"a":1,"a":2}',
        b"[" * 1500 + b"]" * 1500,
        b"x" * (18 * 1024 + 1),
        b'{"schema_version":NaN}',
    ],
)
def test_wire_poison_is_durable_without_origin_or_payload(connection, record, payload):
    result = deliver(connection, payload)
    assert result.outcome == "quarantined"
    assert result.ingest_seq == 0
    assert count(connection, "trace_ingest_positions") == 0
    assert count(connection, "trace_rejected_positions") == 0
    assert count(connection, "trace_poison_counts") == 1
    assert "private-secret" not in "\n".join(connection.iterdump())
    assert deliver(connection, encode(record)).outcome == "accepted"


@pytest.mark.parametrize("kind", ["unsupported_wrapper", "hash", "subject", "origin"])
def test_untrusted_wrapper_never_gets_source_receipt(connection, record, kind):
    subject = "edgecitadel.telemetry.v1.edge-a"
    if kind == "unsupported_wrapper":
        record["schema_version"] = 2
    if kind == "subject":
        subject = "edgecitadel.telemetry.v1.other"
    if kind == "origin":
        record["event"]["node_id"] = "other"
    payload = encode(record)
    if kind == "hash":
        record["event_sha256"] = "0" * 64
        payload = json.dumps(record).encode()
    assert deliver(connection, payload, subject).outcome == "quarantined"
    assert count(connection, "trace_rejected_positions") == 0
    assert count(connection, "trace_ingest_positions") == 0
    assert (
        connection.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0] == 0
    )


def test_known_wrapper_payload_rejection_survives_restart_without_raw_content(
    tmp_path, record
):
    path = tmp_path / "core.db"
    record["event"]["schema_version"] = 2
    record["event"]["private-secret-field"] = "private-secret-value"
    payload = encode(record)
    with sqlite3.connect(path) as conn:
        trace_store.initialize(conn)
        first = deliver(conn, payload)
        assert first.outcome == "rejected"
        assert count(conn, "trace_raw_events") == 0
        assert "private-secret" not in "\n".join(conn.iterdump())
    with sqlite3.connect(path) as conn:
        trace_store.initialize(conn)
        assert deliver(conn, payload) == first
        assert count(conn, "trace_rejected_positions") == 1


def test_rejection_cap_preserves_receipts_and_next_valid_event(
    connection, record, monkeypatch
):
    monkeypatch.setattr(trace_store, "MAX_REJECTED_POSITIONS", 3)
    invalid = deepcopy(record)
    invalid["event"]["schema_version"] = 2
    for seq in range(1, 4):
        invalid["export_seq"] = seq
        assert deliver(connection, encode(invalid)).outcome == "rejected"
    previous = list(connection.iterdump())
    invalid["export_seq"] = 4
    with pytest.raises(TraceContractError, match="rejection_capacity_exceeded"):
        deliver(connection, encode(invalid))
    assert list(connection.iterdump()) == previous
    invalid["export_seq"] = 1
    assert deliver(connection, encode(invalid)).outcome == "rejected"
    record["export_seq"] = 5
    record["event"]["event_id"] = str(uuid4())
    assert deliver(connection, encode(record)).outcome == "accepted"
    assert count(connection, "trace_rejected_positions") == 3


@pytest.mark.parametrize("first_valid", [True, False])
def test_rejected_and_accepted_position_collisions_preserve_first_receipt(
    connection, record, first_valid
):
    invalid = deepcopy(record)
    invalid["event"]["schema_version"] = 2
    first, second = (record, invalid) if first_valid else (invalid, record)
    original = deliver(connection, encode(first))
    collision = deliver(connection, encode(second))
    assert collision.outcome == "conflict"
    assert deliver(connection, encode(second)) == collision
    assert deliver(connection, encode(first)) == original
    assert count(connection, "trace_ingest_conflicts") == 1
    assert count(connection, "trace_rejected_positions") == int(not first_valid)
    assert count(connection, "trace_ingest_positions") == int(first_valid)


def test_different_rejected_contents_cannot_reuse_position(connection, record):
    record["event"]["schema_version"] = 2
    first = deliver(connection, encode(record))
    record["event"]["schema_version"] = 3
    collision = deliver(connection, encode(record))
    assert first.outcome == "rejected" and collision.outcome == "conflict"
    assert deliver(connection, encode(record)) == collision
    assert count(connection, "trace_rejected_positions") == 1


@pytest.mark.parametrize(
    "table", ["trace_rejected_positions", "trace_poison_counts", "trace_collector"]
)
def test_failed_disposition_never_reports_success(connection, record, table):
    record["event"]["schema_version"] = 2
    payload = b"broken" if table == "trace_poison_counts" else encode(record)
    before = list(connection.iterdump())
    operation = "UPDATE" if table == "trace_collector" else "INSERT"
    connection.execute(
        f"CREATE TEMP TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        deliver(connection, payload)
    assert list(connection.iterdump()) == before


def test_invalid_input_cardinality_does_not_grow_quarantine(connection):
    for value in range(1000):
        deliver(
            connection,
            f"private-secret-{value}".encode(),
            subject=f"private-subject-{value}",
        )
    assert count(connection, "trace_poison_counts") == 1
    assert (
        connection.execute("SELECT observations FROM trace_poison_counts").fetchone()[0]
        == 1000
    )
    assert count(connection, "trace_rejected_positions") == 0
    assert "private-" not in "\n".join(connection.iterdump())


def test_counter_saturates_and_does_not_regress_time(connection):
    deliver(connection, b"broken")
    with connection:
        connection.execute(
            "UPDATE trace_poison_counts SET observations=9007199254740991,last_received_at_ms=2000"
        )
    deliver(connection, b"broken-again")
    assert connection.execute(
        "SELECT observations,last_received_at_ms FROM trace_poison_counts"
    ).fetchone() == (9007199254740991, 2000)


def test_rejected_event_identity_cannot_be_corrected_in_new_generation(
    connection, record
):
    invalid = deepcopy(record)
    invalid["event"]["schema_version"] = 2
    assert deliver(connection, encode(invalid)).outcome == "rejected"
    record["export_generation"] = str(uuid4())
    assert deliver(connection, encode(record)).outcome == "conflict"
    assert count(connection, "trace_raw_events") == 0
    record["export_seq"] = 2
    record["event"]["event_id"] = str(uuid4())
    assert deliver(connection, encode(record)).outcome == "accepted"


def test_malformed_variant_cannot_poison_previously_accepted_identity(
    connection, record
):
    assert deliver(connection, encode(record)).outcome == "accepted"
    invalid = deepcopy(record)
    invalid["export_generation"] = str(uuid4())
    invalid["event"]["schema_version"] = 2
    assert deliver(connection, encode(invalid)).outcome == "rejected"
    assert count(connection, "trace_rejected_identities") == 0
    record["export_generation"] = str(uuid4())
    assert deliver(connection, encode(record)).outcome == "duplicate"
