"""Acknowledged SIGKILL boundaries for owned restore/activation processes."""

import json
import os
import select
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from test_restore import rows, seed
from test_restore_activation import activate, staged
from test_writer_lock import start, stop

from edgecitadel_agentd.restore import RESTORE_BARRIER, require_startable
from storage_test_support import stage_restore
from edgecitadel_agentd.restore_activation import review_inventory
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.writer_lock import exclusive_writer

STAGE_POINTS = (
    "destination_barred",
    "holds_mid_transaction",
    "before_retirement",
    "retired",
)
ACTIVATION_POINTS = ("before_commit", "committed", "unbarred")


def killed(config, directory):
    config_path = directory / "owned-restore-crash.json"
    config_path.write_text(json.dumps(config))
    config_path.chmod(0o600)
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), str(config_path)],
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).parents[2] / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        readable, _, _ = select.select([process.stdout], [], [], 15)
        assert readable, "owned child did not acknowledge restore boundary"
        assert process.stdout.readline().strip() == "ready", process.stderr.read()
        process.kill()
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()
        config_path.unlink()


def exercise_stage(directory, point):
    directory = directory.resolve()
    old, new = directory / "old", directory / "new"
    epoch = seed(old)
    before = rows(old)
    killed(
        {
            "operation": "stage",
            "point": point,
            "old": str(old),
            "new": str(new),
            "epoch": epoch,
        },
        directory,
    )
    assert rows(old) == before
    assert (new / RESTORE_BARRIER).exists()
    with exclusive_writer(old), exclusive_writer(new):
        if point == "retired":
            assert (old / RESTORE_BARRIER).exists()
        else:
            require_startable(old)
    if point == "destination_barred":
        assert not (new / "agentd.sqlite3").exists()
    elif point == "holds_mid_transaction":
        store = AgentdStore(new / "agentd.sqlite3")
        try:
            assert (
                store._connection.execute(
                    "SELECT COUNT(*) FROM restore_holds"
                ).fetchone()[0]
                == 0
            )
            assert rows(new) == before
        finally:
            store.close()
    if point != "retired":
        # Incomplete staging is never reused. A fresh destination may be staged
        # while the untouched old directory is still available.
        recovery = directory / "recovered"
        marker = stage_restore(
            snapshot_dir=old,
            previous_state_dir=old,
            destination_dir=recovery,
            node_id="edge-a",
            expected_source_epoch=epoch,
        )
        assert (new / RESTORE_BARRIER).exists()
    else:
        recovery = new
        store = AgentdStore(recovery / "agentd.sqlite3")
        try:
            marker = json.loads(
                store._connection.execute(
                    "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.phase')='restored'"
                ).fetchone()[0]
            )
        finally:
            store.close()
    store = AgentdStore(recovery / "agentd.sqlite3")
    try:
        review = review_inventory(store)
    finally:
        store.close()
    event = activate(old, recovery, marker, review)
    assert event["phase"] == "unknown"
    return {
        "point": point,
        "signal": "SIGKILL",
        "old_execution_rows_unchanged": True,
        "incomplete_destination_barred": True,
        "recovered_source_epoch": marker["source_epoch"],
        "coverage_event_id": event["event_id"],
    }


def exercise_activation(directory, point):
    directory = directory.resolve()
    old, new, marker, review = staged(directory)
    killed(
        {
            "operation": "activate",
            "point": point,
            "old": str(old),
            "new": str(new),
            "marker": marker,
            "review": review,
        },
        directory,
    )
    assert (new / RESTORE_BARRIER).exists() == (point != "unbarred")
    store = AgentdStore(new / "agentd.sqlite3")
    try:
        count_before = store._connection.execute(
            "SELECT COUNT(*) FROM restore_activations"
        ).fetchone()[0]
        coverage_before = store._connection.execute(
            "SELECT COUNT(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchone()[0]
        assert count_before == coverage_before == (0 if point == "before_commit" else 1)
    finally:
        store.close()
    event = activate(old, new, marker, review)
    assert activate(old, new, marker, review) == event
    process = start(new)
    stop(process)
    store = AgentdStore(new / "agentd.sqlite3")
    try:
        assert store.pending_transport() == []
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM restore_activations"
            ).fetchone()[0]
            == 1
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            ).fetchone()[0]
            == 1
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM trace_spool WHERE event_id=?",
                (event["event_id"],),
            ).fetchone()[0]
            == 1
        )
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()
    return {
        "point": point,
        "signal": "SIGKILL",
        "receipts_before_retry": count_before,
        "coverage_event_id": event["event_id"],
        "receipts_after_retry": 1,
        "new_daemon_started": True,
        "held_commands_not_pending": True,
    }


@pytest.mark.parametrize("point", STAGE_POINTS)
def test_stage_sigkill_recovery(tmp_path, point):
    exercise_stage(tmp_path, point)


@pytest.mark.parametrize("point", ACTIVATION_POINTS)
def test_activation_sigkill_recovery(tmp_path, point):
    exercise_activation(tmp_path, point)


def child(config):
    from edgecitadel_agentd import restore, restore_activation

    old, new = Path(config["old"]), Path(config["new"])
    point = config["point"]

    def pause():
        print("ready", flush=True)
        signal.pause()
        raise AssertionError("owned child unexpectedly resumed")

    if config["operation"] == "stage":
        original_barrier = restore._barrier

        def barrier(directory, value):
            if directory == old and point == "before_retirement":
                pause()
            original_barrier(directory, value)
            if (directory == new and point == "destination_barred") or (
                directory == old and point == "retired"
            ):
                pause()

        restore._barrier = barrier
    else:
        original_remove = restore_activation._remove_barrier

        def remove(directory):
            if point == "committed":
                pause()
            original_remove(directory)
            if point == "unbarred":
                pause()

        restore_activation._remove_barrier = remove
    original_store = AgentdStore

    class FaultStore(original_store):
        def __init__(self, path, **kwargs):
            super().__init__(path, **kwargs)
            receipt_inserted = False

            def trace(statement):
                nonlocal receipt_inserted
                if "INSERT INTO restore_activations" in statement:
                    receipt_inserted = True
                if (
                    point == "before_commit"
                    and receipt_inserted
                    and statement == "COMMIT"
                ) or (
                    point == "holds_mid_transaction"
                    and statement.startswith("UPDATE sessions SET closed_at_ms=")
                ):
                    pause()

            self._connection.set_trace_callback(trace)

    from edgecitadel_agentd import storage_layout

    storage_layout.AgentdStore = FaultStore
    if config["operation"] == "stage":
        stage_restore(
            snapshot_dir=old,
            previous_state_dir=old,
            destination_dir=new,
            node_id="edge-a",
            expected_source_epoch=config["epoch"],
        )
    else:
        activate(old, new, config["marker"], config["review"])
    raise AssertionError("owned boundary was not reached")


if __name__ == "__main__":
    child(json.loads(Path(sys.argv[1]).read_text()))
