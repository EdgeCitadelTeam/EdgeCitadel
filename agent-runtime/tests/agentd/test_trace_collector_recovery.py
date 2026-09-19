import json
import sqlite3
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_collector_recovery import begin_recovery, recover_batch
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request


def response(request, epoch, through=130):
    page = {
        key: request[key]
        for key in (
            "schema_version",
            "node_id",
            "source_epoch",
            "export_generation",
            "after_export_seq",
        )
    }
    page.update(
        collector_epoch=epoch,
        settled_export_seq=through,
        more=False,
        rejected_ranges=[],
        lost_ranges=[],
    )
    return {
        "schema_version": 2,
        "request_id": request["request_id"],
        "status": "ok",
        "page": page,
    }


@pytest.fixture
def settled(tmp_path):
    store = AgentdStore(tmp_path / "state.db")
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(store._connection)
        epoch, generation = journal.initialize("edge-a")
        for _ in range(130):
            event["event_id"] = str(uuid4())
            journal.record("edge-a", event, selected=True)
    scope = ("edge-a", epoch, generation)
    collector = str(uuid4())
    request = page_request(store, scope)
    apply_page(store, request, response(request, collector))
    try:
        yield store, scope, collector
    finally:
        store.close()


def expire(store, position, *, sparse=False):
    with store._connection:
        event_id = store._connection.execute(
            "SELECT journal_event_id FROM trace_spool WHERE export_seq=?", (position,)
        ).fetchone()[0]
        store._connection.execute(
            "UPDATE trace_spool SET journal_event_id=NULL WHERE export_seq=?",
            (position,),
        )
        store._connection.execute(
            "DELETE FROM trace_journal WHERE event_id=?", (event_id,)
        )
        if sparse:
            store._connection.execute(
                "DELETE FROM trace_spool WHERE export_seq=?", (position,)
            )


def test_bounded_recovery_restart_replays_payload_and_declares_exact_absence(settled):
    store, scope, old = settled
    expire(store, 2)
    expire(store, 4, sparse=True)
    begin_recovery(store, scope, expected_epoch=old)
    begin_recovery(store, scope, expected_epoch=old)
    with pytest.raises(TraceContractError, match="recovery_in_progress"):
        page_request(store, scope)
    assert recover_batch(store, scope) is False
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_spool WHERE state='pending' AND export_seq<=130"
        ).fetchone()[0]
        == 63
    )
    reopened = AgentdStore(store.path)
    try:
        assert recover_batch(reopened, scope) is False
        assert recover_batch(reopened, scope) is True
        assert recover_batch(reopened, scope) is True
        request = page_request(reopened, scope)
        assert request["after_export_seq"] == 0 and request["collector_epoch"] is None
        markers = [
            json.loads(row[0])
            for row in reopened._connection.execute(
                "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            )
        ]
        assert len(markers) == 1
        assert markers[0]["attributes"]["lost_ranges"] == [
            {"first": 2, "last": 2},
            {"first": 4, "last": 4},
        ]
        assert markers[0]["attributes"]["affected_source_epoch"] == scope[1]
        assert markers[0]["attributes"]["through_export_seq"] == 130
        with pytest.raises(TraceContractError, match="retired_collector_epoch"):
            apply_page(reopened, request, response(request, old))
        new = str(uuid4())
        assert apply_page(reopened, request, response(request, new)) == "applied"
        assert page_request(reopened, scope)["collector_epoch"] == new
        assert (
            reopened._connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
            == 0
        )
    finally:
        reopened.close()


def test_marker_failure_rolls_back_replay_and_scan_progress(settled):
    store, scope, old = settled
    expire(store, 2)
    begin_recovery(store, scope, expected_epoch=old)
    before = list(store._connection.iterdump())
    store._connection.execute(
        "CREATE TEMP TRIGGER fail BEFORE INSERT ON trace_journal BEGIN SELECT RAISE(ABORT,'marker fault'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="marker fault"):
        recover_batch(store, scope)
    assert list(store._connection.iterdump()) == before


def test_all_prior_collector_epochs_stay_fenced_after_multiple_resets(settled):
    store, scope, first = settled
    epochs = [first]
    for _ in range(2):
        begin_recovery(store, scope, expected_epoch=epochs[-1])
        while not recover_batch(store, scope):
            pass
        request = page_request(store, scope)
        for old in epochs:
            with pytest.raises(TraceContractError, match="retired_collector_epoch"):
                apply_page(store, request, response(request, old))
        epochs.append(str(uuid4()))
        apply_page(store, request, response(request, epochs[-1]))
    assert (
        json.loads(
            store._connection.execute(
                "SELECT blocked_epochs_json FROM trace_collector_recovery"
            ).fetchone()[0]
        )
        == epochs[:-1]
    )


def test_loss_marker_uses_current_writer_for_retired_target(settled):
    store, scope, old = settled
    expire(store, 2)
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        store._connection.execute("UPDATE trace_sources SET active=0")
        store._connection.execute("UPDATE trace_export_generations SET active=0")
        new_epoch, _ = TraceJournal(store._connection).initialize(scope[0])
    begin_recovery(store, scope, expected_epoch=old)
    recover_batch(store, scope)
    marker = json.loads(
        store._connection.execute(
            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchone()[0]
    )
    assert marker["source_epoch"] == new_epoch
    assert marker["attributes"]["affected_source_epoch"] == scope[1]
    assert marker["attributes"]["export_generation"] == scope[2]


def test_wrong_expected_epoch_and_metadata_capacity_fail_without_changes(
    settled, monkeypatch
):
    store, scope, old = settled
    before = list(store._connection.iterdump())
    with pytest.raises(TraceContractError, match="epoch_mismatch"):
        begin_recovery(store, scope, expected_epoch=str(uuid4()))
    monkeypatch.setattr(
        "edgecitadel_agentd.trace_collector_recovery.MAX_BLOCKED_EPOCHS", 0
    )
    with pytest.raises(TraceContractError, match="epoch_capacity_exceeded"):
        begin_recovery(store, scope, expected_epoch=old)
    assert list(store._connection.iterdump()) == before


def test_schema_19_upgrade_failure_preserves_existing_settlement(settled):
    store, scope, old = settled
    with store._connection:
        store._connection.execute("DROP TABLE trace_collector_recovery")
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=19")
    before = list(store._connection.iterdump())
    captured = []

    class FailingStore(AgentdStore):
        def _execute_migration_sql(self, source_sql):
            super()._execute_migration_sql(source_sql)
            if "trace_collector_recovery" in source_sql:
                captured.append(self._connection)
                raise RuntimeError("migration fault")

    try:
        with pytest.raises(RuntimeError, match="migration fault"):
            FailingStore(store.path)
    finally:
        for connection in captured:
            connection.close()
    assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 19
    assert list(store._connection.iterdump()) == before
    reopened = AgentdStore(store.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 29
        assert page_request(reopened, scope)["collector_epoch"] == old
        assert page_request(reopened, scope)["after_export_seq"] == 130
    finally:
        reopened.close()
