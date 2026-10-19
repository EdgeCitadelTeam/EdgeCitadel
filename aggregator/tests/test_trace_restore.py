import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from aggregator import trace_restore, trace_store
from aggregator.trace_settlement import settlement_page_reply
from test_trace_settlement import put


def test_restore_preserves_wal_evidence_rotates_epoch_and_fences_old_pages(tmp_path):
    backup = tmp_path / "backup.db"
    output = tmp_path / "restored.db"
    scope = {
        "node_id": "edge-a",
        "source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
    }
    with sqlite3.connect(backup) as source:
        source.execute("PRAGMA journal_mode=WAL")
        source.execute("PRAGMA wal_autocheckpoint=0")
        trace_store.initialize(source)
        accepted = put(source, scope, 1)
        put(source, scope, 2, rejected=True)
        source.execute("CREATE TABLE command_evidence(value TEXT)")
        source.execute("INSERT INTO command_evidence VALUES('retained')")
        source.commit()
        before = list(source.iterdump())
        epochs = trace_restore.prepare_restore(backup, output)
        assert list(source.iterdump()) == before
    assert epochs["previous_collector_epoch"] == accepted.collector_epoch
    assert epochs["collector_epoch"] != accepted.collector_epoch
    assert output.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(output) as restored:
        trace_store.initialize(restored)
        assert (
            restored.execute("SELECT collector_epoch FROM trace_collector").fetchone()[
                0
            ]
            == epochs["collector_epoch"]
        )
        assert restored.execute("SELECT * FROM command_evidence").fetchall() == [
            ("retained",)
        ]
        for table in trace_restore_preserved_tables():
            with sqlite3.connect(backup) as source:
                assert (
                    restored.execute(f"SELECT * FROM {table}").fetchall()
                    == source.execute(f"SELECT * FROM {table}").fetchall()
                )
        request = dict(
            schema_version=2,
            request_id=str(uuid4()),
            **scope,
            collector_epoch=accepted.collector_epoch,
            after_export_seq=0,
        )
        assert settlement_page_reply(restored, request)["code"] == "collector_changed"
        request["collector_epoch"] = None
        reply = settlement_page_reply(restored, request)
        assert reply["page"]["collector_epoch"] == epochs["collector_epoch"]
        assert reply["page"]["settled_export_seq"] == 2
    assert not list(tmp_path.glob(".core-restore-*"))


def trace_restore_preserved_tables():
    return (
        *trace_store.trace_capacity.TABLES,
        "trace_poison_counts",
        "trace_capacity_usage",
    )


@pytest.mark.parametrize("kind", ["file", "symlink"])
def test_destination_never_overwritten(tmp_path, kind):
    source = tmp_path / "source.db"
    source.write_bytes(b"not even opened")
    destination = tmp_path / "output.db"
    if kind == "file":
        destination.write_bytes(b"existing")
    else:
        destination.symlink_to(tmp_path / "missing")
    with pytest.raises(FileExistsError):
        trace_restore.prepare_restore(source, destination)
    assert (
        destination.is_symlink()
        if kind == "symlink"
        else destination.read_bytes() == b"existing"
    )


@pytest.mark.parametrize(
    "failure", ["invalid_database", "missing_collector", "epoch_write", "publish"]
)
def test_failed_preparation_leaves_snapshot_intact_and_no_output(
    tmp_path, monkeypatch, failure
):
    source = tmp_path / "source.db"
    output = tmp_path / "output.db"
    if failure == "invalid_database":
        source.write_bytes(b"invalid")
    else:
        with sqlite3.connect(source) as connection:
            if failure != "missing_collector":
                trace_store.initialize(connection)
            if failure == "epoch_write":
                connection.execute(
                    "CREATE TRIGGER deny_epoch BEFORE UPDATE ON trace_collector BEGIN SELECT RAISE(ABORT, 'injected'); END"
                )
    before = source.read_bytes()
    if failure == "publish":

        def fail(*args):
            raise OSError("injected")

        monkeypatch.setattr(trace_restore.os, "link", fail)
    with pytest.raises((sqlite3.DatabaseError, OSError)):
        trace_restore.prepare_restore(source, output)
    assert source.read_bytes() == before
    assert not output.exists()
    assert not list(tmp_path.glob(".core-restore-*"))


def test_repeated_preparations_are_distinct_and_cli_output_is_usable(tmp_path):
    import json
    import os
    import subprocess
    import sys

    source = tmp_path / "backup.db"
    with sqlite3.connect(source) as conn:
        trace_store.initialize(conn)
    first = trace_restore.prepare_restore(source, tmp_path / "first.db")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "aggregator.trace_restore",
            str(source),
            str(tmp_path / "second.db"),
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[2]),
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    second = json.loads(result.stdout)
    assert first["previous_collector_epoch"] == second["previous_collector_epoch"]
    assert first["collector_epoch"] != second["collector_epoch"]
