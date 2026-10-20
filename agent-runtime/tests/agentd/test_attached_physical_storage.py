import sqlite3
from contextlib import closing
from pathlib import Path

from edgecitadel_agentd.trace_capacity import physical_storage


def test_attached_pages_and_active_rollback_journal_are_counted(tmp_path):
    main = tmp_path / "main.db"
    attached = tmp_path / "trace.db"
    with closing(sqlite3.connect(main)) as db:
        db.execute('ATTACH DATABASE ? AS "trace odd"', (str(attached),))
        db.execute('PRAGMA "trace odd".journal_mode=PERSIST')
        db.execute('CREATE TABLE "trace odd".records(body BLOB)')
        db.execute('INSERT INTO "trace odd".records VALUES(?)', (b"a" * 262144,))
        db.commit()
        before = physical_storage(db)
        db.execute("BEGIN IMMEDIATE")
        db.execute('UPDATE "trace odd".records SET body=?', (b"b" * 262144,))
        measured = physical_storage(db)
        page_bytes = sum(
            db.execute(f'PRAGMA "{schema}".page_count').fetchone()[0]
            * db.execute(f'PRAGMA "{schema}".page_size').fetchone()[0]
            for schema in ("main", "trace odd")
        )
        assert measured["allocated_page_bytes"] == page_bytes
        journal = Path(str(attached) + "-journal").stat()
        assert measured["rollback_journal_file_bytes"] >= journal.st_size > 262144
        assert measured["pressure_bytes"] > before["pressure_bytes"]
        assert measured["pressure_bytes"] >= page_bytes + journal.st_size
        db.rollback()


def test_allocated_blocks_are_not_replaced_by_smaller_logical_size(
    tmp_path, monkeypatch
):
    path = tmp_path / "main.db"
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE records(body BLOB)")
        db.commit()
        original = Path.stat

        def allocated(file, *args, **kwargs):
            info = original(file, *args, **kwargs)
            if file == path:

                class Oversized:
                    st_size = info.st_size
                    st_blocks = info.st_blocks + 2048

                return Oversized()
            return info

        monkeypatch.setattr(Path, "stat", allocated)
        measured = physical_storage(db)
        assert measured["pressure_bytes"] >= original(path).st_blocks * 512 + 1048576
        assert measured["filesystem_allocated_bytes"] >= 1048576


def test_attached_wal_growth_closes_optional_admission(tmp_path, monkeypatch):
    import pytest

    from edgecitadel_agentd import trace_capacity
    from edgecitadel_agentd.trace_contract import TraceContractError

    main = tmp_path / "main.db"
    attached = tmp_path / "trace.db"
    db = sqlite3.connect(main)
    reader = sqlite3.connect(attached)
    try:
        db.execute(
            "CREATE TABLE trace_storage_usage(singleton INTEGER PRIMARY KEY,event_bytes INTEGER,event_count INTEGER)"
        )
        db.execute("INSERT INTO trace_storage_usage VALUES(1,0,0)")
        db.execute(
            "CREATE VIEW trace_storage_usage_all AS SELECT * FROM trace_storage_usage"
        )
        db.commit()
        db.execute("ATTACH DATABASE ? AS telemetry", (str(attached),))
        assert db.execute("PRAGMA telemetry.journal_mode=WAL").fetchone()[0] == "wal"
        db.execute("CREATE TABLE telemetry.records(body BLOB)")
        db.execute("INSERT INTO telemetry.records VALUES(?)", (b"a" * 16384,))
        db.commit()
        db.execute("PRAGMA telemetry.wal_checkpoint(TRUNCATE)")
        baseline = physical_storage(db)["pressure_bytes"]
        reader.execute("BEGIN")
        reader.execute("SELECT body FROM records").fetchone()
        for i in range(8):
            with db:
                db.execute("UPDATE telemetry.records SET body=?", (bytes([i]) * 16384,))
        measured = physical_storage(db)
        assert (
            measured["wal_file_bytes"]
            == Path(str(attached) + "-wal").stat().st_size
            > 0
        )
        assert (
            measured["shm_file_bytes"]
            == Path(str(attached) + "-shm").stat().st_size
            > 0
        )
        assert measured["pressure_bytes"] > baseline + 16384
        monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", baseline + 16384)
        with db:
            db.execute("BEGIN IMMEDIATE")
            with pytest.raises(TraceContractError, match="quota_exceeded"):
                trace_capacity.admit_event(db, event_bytes=100, kind="tool")
        assert (
            db.execute("SELECT event_count FROM trace_storage_usage").fetchone()[0] == 0
        )
    finally:
        reader.close()
        db.close()
