"""Real pager behavior behind the source's bounded-journal prerequisite."""

import hashlib
import struct
import sqlite3

import pytest

from edgecitadel_agentd.store import AgentdStore


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def exercise(store, *, spill=False):
    """Rewrite more than the cache, including a rollback and a repeated write."""
    db = store._connection
    with db:
        db.execute("CREATE TABLE journal_probe (id INTEGER PRIMARY KEY, value BLOB)")
        db.executemany(
            "INSERT INTO journal_probe VALUES (?,?)",
            ((number, b"a" * 16384) for number in range(256)),
        )
    original = digest(store.path)
    pages = db.execute("PRAGMA main.page_count").fetchone()[0]
    page_size = db.execute("PRAGMA main.page_size").fetchone()[0]
    db.execute("PRAGMA main.cache_size=8")
    db.execute("PRAGMA task_state.cache_size=8")
    if spill:
        db.execute("PRAGMA cache_spill=ON")
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE journal_probe SET value=?", (b"b" * 16384,))
        db.execute("SAVEPOINT repeat_write")
        db.execute("UPDATE journal_probe SET value=?", (b"c" * 16384,))
        db.execute("ROLLBACK TO repeat_write")
        db.execute("RELEASE repeat_write")
        assert (
            db.execute("SELECT DISTINCT value FROM journal_probe").fetchall()[0][0]
            == b"b" * 16384
        )
        db.execute("UPDATE journal_probe SET value=?", (b"d" * 16384,))
        journal = store.path.with_name(store.path.name + "-journal").read_bytes()
        changed_before_commit = digest(store.path) != original
        if not spill:
            assert not changed_before_commit
            sector_size, stored_page_size = struct.unpack(">II", journal[20:28])
            assert stored_page_size == page_size
            # Before commit there is no super-journal trailer. Each original page
            # can occur once in the one-header rollback journal, even after a
            # savepoint rollback and subsequent modification of the same rows.
            assert len(journal) <= sector_size + pages * (page_size + 8)
            records = (len(journal) - sector_size) // (page_size + 8)
            numbers = [
                struct.unpack_from(">I", journal, sector_size + n * (page_size + 8))[0]
                for n in range(records)
            ]
            assert len(numbers) > 8
            assert len(set(numbers)) == len(numbers)
            assert all(1 <= number <= pages for number in numbers)
        return {
            "pages": pages,
            "page_size": page_size,
            "journal_bytes_before_commit": len(journal),
            "database_changed_before_commit": changed_before_commit,
        }
    finally:
        db.rollback()
        assert digest(store.path) == original
        assert (
            db.execute("SELECT DISTINCT value FROM journal_probe").fetchall()[0][0]
            == b"a" * 16384
        )
        with db:
            db.execute("DROP TABLE journal_probe")


@pytest.mark.parametrize("spill", [False, True])
def test_savepoint_and_rewrites_do_not_spill_with_production_policy(tmp_path, spill):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        report = exercise(store, spill=spill)
        # The deliberate old-policy control proves the workload exceeds cache
        # and would dirty the database before commit without the policy.
        assert report["database_changed_before_commit"] is spill
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        store.close()


def test_every_reopened_pair_uses_memory_scratch_and_no_spilling(tmp_path):
    path = tmp_path / "agentd.sqlite3"
    for _ in range(2):
        store = AgentdStore(path)
        try:
            db = store._connection
            assert db.execute("PRAGMA temp_store").fetchone()[0] == 2
            for schema in ("main", "task_state"):
                assert db.execute(f"PRAGMA {schema}.cache_spill").fetchone()[0] == 0
            task = store.create_task(
                sender_id="sender", recipient_id="worker", payload={"body": "paired"}
            )
            assert store.get_task(task["task_id"])["payload"] == {"body": "paired"}
        finally:
            store.close()


@pytest.mark.parametrize("options", [[], [("TEMP_STORE=0",)]])
def test_unverifiable_or_disk_only_sqlite_refuses_before_mutation(options):
    from edgecitadel_agentd.storage_sqlite import configure_scratch

    class UnsupportedConnection:
        def execute(self, sql):
            assert sql == "PRAGMA compile_options"
            return options

    with pytest.raises(sqlite3.NotSupportedError, match="memory temporary storage"):
        configure_scratch(UnsupportedConnection())
