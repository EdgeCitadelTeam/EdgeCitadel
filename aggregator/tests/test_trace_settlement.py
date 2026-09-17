import hashlib
import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from aggregator.trace_ingest import ingest_wire
from aggregator.trace_settlement import settlement_page_reply, settlement_reply
from aggregator.trace_store import initialize

from edgecitadel_agentd.trace_contract import canonical_bytes

FIXTURES = json.loads(
    (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/events.v1.json"
    ).read_text()
)["fixtures"]


@pytest.fixture
def connection(tmp_path):
    conn = sqlite3.connect(tmp_path / "core.db")
    initialize(conn)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def request_data():
    return {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "node_id": "edge-a",
        "source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
    }


def put(conn, scope, seq, *, rejected=False, event=None):
    value = deepcopy(event or FIXTURES[0]["event"])
    value.update(
        node_id=scope["node_id"],
        source_epoch=scope["source_epoch"],
        source_seq=seq,
        event_id=str(uuid4()),
    )
    if rejected:
        value["schema_version"] = 2
    wrapper = {
        key: scope[key] for key in ("node_id", "source_epoch", "export_generation")
    }
    wrapper.update(
        schema_version=1,
        export_seq=seq,
        event=value,
        event_sha256=hashlib.sha256(canonical_bytes(value)).hexdigest(),
    )
    return ingest_wire(
        conn,
        f"edgecitadel.telemetry.v1.{scope['node_id']}",
        canonical_bytes(wrapper),
        received_at_ms=1000,
    )


def loss(conn, target, ranges, *, through=1000, writer=None, seq=1):
    writer = writer or {
        **target,
        "source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
    }
    event = deepcopy(next(x["event"] for x in FIXTURES if x["name"] == "coverage"))
    event["attributes"] = {
        "export_generation": target["export_generation"],
        "affected_source_epoch": target["source_epoch"],
        "through_export_seq": through,
        "lost_ranges": [{"first": a, "last": b} for a, b in ranges],
    }
    put(conn, writer, seq, event=event)
    return writer


def test_unknown_has_no_checkpoint_and_sparse_positions_do_not_fill_holes(
    connection, request_data
):
    reply = settlement_reply(connection, request_data)
    assert reply["code"] == "unknown_source" and "checkpoint" not in reply
    put(connection, request_data, 3)
    assert (
        settlement_reply(connection, request_data)["checkpoint"]["settled_export_seq"]
        == 0
    )
    put(connection, request_data, 1)
    assert (
        settlement_reply(connection, request_data)["checkpoint"]["settled_export_seq"]
        == 1
    )
    put(connection, request_data, 2)
    assert (
        settlement_reply(connection, request_data)["checkpoint"]["settled_export_seq"]
        == 3
    )


def test_rejected_ranges_are_exact_and_survive_restart(connection, request_data):
    put(connection, request_data, 1)
    put(connection, request_data, 2, rejected=True)
    put(connection, request_data, 3, rejected=True)
    put(connection, request_data, 4)
    expected = settlement_reply(connection, request_data)
    assert expected["checkpoint"]["settled_export_seq"] == 4
    assert expected["checkpoint"]["rejected_ranges"] == [{"first": 2, "last": 3}]
    path = connection.execute("PRAGMA database_list").fetchone()[2]
    with sqlite3.connect(path) as reopened:
        initialize(reopened)
        assert settlement_reply(reopened, request_data) == expected
    with sqlite3.connect(":memory:") as fresh:
        initialize(fresh)
        assert settlement_reply(fresh, request_data)["code"] == "unknown_source"
        assert (
            fresh.execute("SELECT collector_epoch FROM trace_collector").fetchone()[0]
            != expected["checkpoint"]["collector_epoch"]
        )


def test_loss_applies_to_affected_epoch_and_preserves_received_holes(
    connection, request_data
):
    put(connection, request_data, 2)
    put(connection, request_data, 4, rejected=True)
    writer = loss(connection, request_data, [(1, 5)])
    checkpoint = settlement_reply(connection, request_data)["checkpoint"]
    assert checkpoint["settled_export_seq"] == 5
    assert checkpoint["lost_ranges"] == [
        {"first": 1, "last": 1},
        {"first": 3, "last": 3},
        {"first": 5, "last": 5},
    ]
    assert checkpoint["rejected_ranges"] == [{"first": 4, "last": 4}]
    own = settlement_reply(connection, writer)["checkpoint"]
    assert own["settled_export_seq"] == 1 and own["lost_ranges"] == []


def test_delayed_fragment_does_not_inherit_shared_high_watermark(
    connection, request_data
):
    loss(connection, request_data, [(1, 2)], through=10)
    assert (
        settlement_reply(connection, request_data)["checkpoint"]["settled_export_seq"]
        == 2
    )
    loss(connection, request_data, [(5, 10)], through=10)
    assert (
        settlement_reply(connection, request_data)["checkpoint"]["settled_export_seq"]
        == 2
    )
    loss(connection, request_data, [(3, 4)], through=10)
    checkpoint = settlement_reply(connection, request_data)["checkpoint"]
    assert checkpoint["settled_export_seq"] == 10
    assert checkpoint["lost_ranges"] == [{"first": 1, "last": 10}]


def test_three_epochs_keep_marker_loss_separate_from_original_loss(
    connection, request_data
):
    second = loss(connection, request_data, [(1, 3)])
    put(connection, second, 2)
    third = loss(connection, second, [(1, 3)])
    assert settlement_reply(connection, request_data)["checkpoint"]["lost_ranges"] == [
        {"first": 1, "last": 3}
    ]
    assert settlement_reply(connection, second)["checkpoint"]["lost_ranges"] == [
        {"first": 3, "last": 3}
    ]
    assert settlement_reply(connection, third)["checkpoint"]["lost_ranges"] == []


def test_huge_explicit_range_is_not_expanded_per_position(connection, request_data):
    maximum = 9007199254740991
    loss(connection, request_data, [(1, maximum)], through=maximum)
    checkpoint = settlement_reply(connection, request_data)["checkpoint"]
    assert checkpoint["settled_export_seq"] == maximum
    assert checkpoint["lost_ranges"] == [{"first": 1, "last": maximum}]


def test_range_limit_never_silently_omits_rejections(connection, request_data):
    for seq in range(1, 259):
        put(connection, request_data, seq, rejected=bool(seq % 2))
    reply = settlement_reply(connection, request_data)
    assert reply["code"] == "temporarily_unavailable"
    assert "checkpoint" not in reply


def test_loss_index_and_raw_event_share_rollback(connection, request_data):
    before = list(connection.iterdump())
    connection.execute(
        "CREATE TEMP TRIGGER fail BEFORE INSERT ON trace_loss_ranges BEGIN SELECT RAISE(ABORT, 'injected'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        loss(connection, request_data, [(1, 4)])
    assert list(connection.iterdump()) == before
    assert settlement_reply(connection, request_data)["code"] == "unknown_source"


def test_settlement_cannot_observe_callers_uncommitted_evidence(
    connection, request_data
):
    connection.execute("BEGIN IMMEDIATE")
    with pytest.raises(ValueError, match="idle_connection"):
        settlement_reply(connection, request_data)
    assert connection.in_transaction
    connection.rollback()


def test_pages_reconcile_more_than_128_ranges_without_omission(
    connection, request_data
):
    for seq in range(1, 601):
        put(connection, request_data, seq, rejected=bool(seq % 2))
    assert (
        settlement_reply(connection, request_data)["code"] == "temporarily_unavailable"
    )
    request = {
        **request_data,
        "schema_version": 2,
        "after_export_seq": 0,
        "collector_epoch": None,
    }
    rejected, bases = [], []
    while True:
        page = settlement_page_reply(connection, request)["page"]
        assert page["after_export_seq"] == request["after_export_seq"]
        assert len(page["rejected_ranges"]) <= 128
        rejected.extend(page["rejected_ranges"])
        bases.append((page["after_export_seq"], page["settled_export_seq"]))
        if not page["more"]:
            break
        request.update(
            request_id=str(uuid4()),
            collector_epoch=page["collector_epoch"],
            after_export_seq=page["settled_export_seq"],
        )
    assert bases == [(0, 256), (256, 512), (512, 600)]
    assert rejected == [{"first": seq, "last": seq} for seq in range(1, 601, 2)]


def test_page_stops_at_hole_and_clips_loss_at_confirmed_base(connection, request_data):
    loss(connection, request_data, [(1, 10), (12, 15)], through=15)
    epoch = connection.execute(
        "SELECT collector_epoch FROM trace_collector"
    ).fetchone()[0]
    request = {
        **request_data,
        "schema_version": 2,
        "after_export_seq": 5,
        "collector_epoch": epoch,
    }
    page = settlement_page_reply(connection, request)["page"]
    assert page["settled_export_seq"] == 10 and page["more"] is False
    assert page["lost_ranges"] == [{"first": 6, "last": 10}]
    request["after_export_seq"] = 10
    page = settlement_page_reply(connection, request)["page"]
    assert page["settled_export_seq"] == 10 and page["lost_ranges"] == []
    put(connection, request_data, 11)
    page = settlement_page_reply(connection, request)["page"]
    assert page["settled_export_seq"] == 15
    assert page["lost_ranges"] == [{"first": 12, "last": 15}]


def test_page_continuation_cannot_cross_collector_reset(connection, request_data):
    put(connection, request_data, 1)
    request = {
        **request_data,
        "schema_version": 2,
        "after_export_seq": 0,
        "collector_epoch": None,
    }
    page = settlement_page_reply(connection, request)["page"]
    request.update(after_export_seq=1, collector_epoch=page["collector_epoch"])
    with sqlite3.connect(":memory:") as restored:
        initialize(restored)
        reply = settlement_page_reply(restored, request)
        assert reply["code"] == "collector_changed" and "page" not in reply
        request.update(after_export_seq=0, collector_epoch=None)
        assert settlement_page_reply(restored, request)["code"] == "unknown_source"
