"""Two connections to one ordinary store (agentd's and its telemetry thread's).

A provisioned store's completion workspace is the cross-connection writer lock;
an ordinary store (component tests, experiment nodes) gets the same exclusion
from a per-file lock. Without it, two write transactions that each read the
other's member of the task/trace pair deadlock at commit: each waits for the
lock the other holds until the busy timeout, then both fail.
"""

from __future__ import annotations

import sqlite3
import threading

from edgecitadel_agentd.store import AgentdStore


def test_writes_of_two_connections_to_one_store_do_not_deadlock(tmp_path):
    path = tmp_path / "agentd.sqlite3"
    first, second = AgentdStore(path), AgentdStore(path)
    # Scratch tables in each member (opened stores do not check inventory again).
    with first._lock, first._connection:
        first._connection.execute("CREATE TABLE main.trace_side(x)")
        first._connection.execute("CREATE TABLE task_state.task_side(x)")
    for store in (first, second):
        store._connection.execute("PRAGMA busy_timeout=1000")
    second_read = threading.Event()
    errors: list[BaseException] = []

    def task_writer() -> None:
        # Writes the task member, reads the trace member, then commits.
        try:
            with first._lock, first._connection:
                first._connection.execute("INSERT INTO task_state.task_side VALUES (1)")
                first._connection.execute(
                    "SELECT count(*) FROM main.trace_side"
                ).fetchall()
                started.set()
                second_read.wait(1.5)
        except BaseException as error:  # noqa: BLE001 - reported below
            errors.append(error)

    def trace_writer() -> None:
        started.wait(5)
        try:
            with second._lock, second._connection:
                second._connection.execute("INSERT INTO main.trace_side VALUES (1)")
                second._connection.execute(
                    "SELECT count(*) FROM task_state.task_side"
                ).fetchall()
                second_read.set()
        except BaseException as error:  # noqa: BLE001 - reported below
            errors.append(error)

    started = threading.Event()
    threads = [
        threading.Thread(target=task_writer),
        threading.Thread(target=trace_writer),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(15)
    try:
        assert not [t for t in threads if t.is_alive()]
        assert errors == [], errors
        for store in (first, second):
            counts = [
                store._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("main.trace_side", "task_state.task_side")
            ]
            assert counts == [1, 1]
    finally:
        first.close()
        second.close()


def test_a_held_writer_turns_a_second_writer_away_after_its_wait(tmp_path, monkeypatch):
    from edgecitadel_agentd import storage_workspace

    monkeypatch.setattr(storage_workspace, "WRITER_WAIT_SECONDS", 0.2)
    path = tmp_path / "agentd.sqlite3"
    first, second = AgentdStore(path), AgentdStore(path)
    try:
        with first._lock, first._connection:
            first._connection.execute("CREATE TABLE main.trace_side(x)")
        with first._lock, first._connection:
            first._connection.execute("INSERT INTO main.trace_side VALUES (0)")
            try:
                with second._lock, second._connection:
                    second._connection.execute("INSERT INTO main.trace_side VALUES (1)")
            except sqlite3.OperationalError as error:
                assert str(error) == "store writer is busy"
            else:
                raise AssertionError("a second writer entered a held store")
    finally:
        first.close()
        second.close()
