import json
import sqlite3

import pytest

from edgecitadel_agentd import trace_reservations as reservations
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.storage_sqlite import configure_scratch
from edgecitadel_agentd.trace_contract import TraceContractError


@pytest.fixture
def db(tmp_path):
    store = AgentdStore(tmp_path / "trace.sqlite3")
    store.close()
    # Exercise the slot primitive with the real counter schema, without the
    # store-level canonical-record guards used by higher-level writer tests.
    connection = sqlite3.connect(tmp_path / "trace.sqlite3")
    configure_scratch(connection)
    try:
        yield connection
    finally:
        connection.close()


def test_obligation_and_full_width_record_commit_and_rollback_together(db):
    obligation = reservations.Obligation("run", "owned-run", "terminal")
    with pytest.raises(RuntimeError), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.reserve(db, obligation)
        raise RuntimeError("admission did not commit")
    assert not db.execute("SELECT 1 FROM trace_completion_slots").fetchall()
    with db:
        db.execute("BEGIN IMMEDIATE")
        slot = reservations.reserve(db, obligation)
        assert reservations.reserve(db, obligation) == slot
    assert reservations.read(db, slot) is None
    record = {"event": {"phase": "completed"}, "receipt": "x" * 16000}
    with pytest.raises(RuntimeError), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.fill(db, obligation, record)
        raise RuntimeError("completion did not commit")
    assert reservations.read(db, slot) is None
    with db:
        db.execute("BEGIN IMMEDIATE")
        reservations.fill(db, obligation, record)
    assert reservations.read(db, slot) == record
    assert (
        db.execute("SELECT length(record) FROM trace_completion_slots").fetchone()[0]
        == reservations.SLOT_BYTES
    )
    with pytest.raises(TraceContractError, match="already_filled"), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.fill(db, obligation, {"event": "different"})
    with pytest.raises(TraceContractError, match="materialization"), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.release_unused(db, obligation)
    assert reservations.read(db, slot) == record


def test_completion_rewrites_all_payload_pages_without_free_pages(db):
    obligations = [
        reservations.Obligation("operation", str(n).zfill(128), "x" * 32)
        for n in range(32)
    ]
    with db:
        db.execute("BEGIN IMMEDIATE")
        for obligation in obligations:
            reservations.reserve(db, obligation)
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    assert db.execute("PRAGMA freelist_count").fetchone()[0] == 0
    assert db.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] == pages
    record = {"payload": "x" * (reservations.SLOT_BYTES - 14)}
    assert (
        len(json.dumps(record, separators=(",", ":")).encode())
        == reservations.SLOT_BYTES
    )
    with db:
        db.execute("BEGIN IMMEDIATE")
        for obligation in reversed(obligations):
            reservations.fill(db, obligation, record)
    assert db.execute("PRAGMA page_count").fetchone()[0] == pages
    assert db.execute(
        "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
    ).fetchone()[0] == len(obligations)
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_unused_slot_can_be_reassigned_but_old_owner_cannot_fill_it(db):
    old = reservations.Obligation("task", "old", "terminal")
    new = reservations.Obligation("task", "new", "terminal")
    with db:
        db.execute("BEGIN IMMEDIATE")
        slot = reservations.reserve(db, old)
        reservations.release_unused(db, old)
        assert reservations.reserve(db, new) == slot
    with pytest.raises(TraceContractError, match="missing"), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.fill(db, old, {"wrong": True})
    assert reservations.read(db, slot) is None


def test_pool_limit_and_oversized_record_leave_obligation_unchanged(db, monkeypatch):
    monkeypatch.setattr(reservations, "MAX_SLOTS", 1)
    obligation = reservations.Obligation("run", "one", "terminal")
    with db:
        db.execute("BEGIN IMMEDIATE")
        slot = reservations.reserve(db, obligation)
    with pytest.raises(TraceContractError, match="quota_exceeded"), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.reserve(db, reservations.Obligation("run", "two", "terminal"))
    with pytest.raises(TraceContractError, match="oversize_record"), db:
        db.execute("BEGIN IMMEDIATE")
        reservations.fill(db, obligation, {"oversized": "x" * reservations.SLOT_BYTES})
    assert reservations.read(db, slot) is None
    assert db.execute("SELECT count(*) FROM trace_completion_slots").fetchone()[0] == 1
