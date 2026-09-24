"""Owned fixture maintenance must not erase production or receipt identities."""

import importlib.util
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from test_trace_graph_pages import project_all
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

core = core_fixture
spec = importlib.util.spec_from_file_location(
    "trace_cleanup", Path(__file__).parents[2] / "e2e/helpers/trace_cleanup.py"
)
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


def test_fixture_cleanup_preserves_real_trace_and_receipts(core):
    fixture, real = uuid4().hex, uuid4().hex
    generation = str(uuid4())
    for seq, trace in enumerate((fixture, real), 1):
        put(core, event(seq=seq, trace_id=trace), generation, seq)
    project_all(core)
    core.executescript("""
        CREATE TABLE agents(agent_id TEXT PRIMARY KEY);
        CREATE TABLE messages(sender_id TEXT,recipient_id TEXT,task_id TEXT);
        INSERT INTO agents VALUES('fixture'),('real');
        INSERT INTO messages VALUES('fixture','real',NULL),('real','real',NULL);
    """)
    identities = core.execute(
        "SELECT node_id,source_epoch,event_id,event_sha256 FROM trace_raw_events ORDER BY ingest_seq"
    ).fetchall()
    positions = core.execute(
        "SELECT * FROM trace_ingest_positions ORDER BY export_seq"
    ).fetchall()
    result = cleanup.purge_core(core, [fixture], ["fixture"])
    assert result == {"agents": 0, "messages": 0, "payloads": 1}
    assert (
        core.execute(
            "SELECT node_id,source_epoch,event_id,event_sha256 FROM trace_raw_events ORDER BY ingest_seq"
        ).fetchall()
        == identities
    )
    assert (
        core.execute(
            "SELECT * FROM trace_ingest_positions ORDER BY export_seq"
        ).fetchall()
        == positions
    )
    assert core.execute("SELECT trace_id FROM trace_projection_runs").fetchall() == [
        (real,)
    ]
    assert (
        core.execute(
            "SELECT count(*) FROM trace_projection_history_rows WHERE json_extract(row_json,'$.trace_id')=?",
            (fixture,),
        ).fetchone()[0]
        == 0
    )
    assert (
        core.execute(
            "SELECT count(*) FROM trace_projection_history_rows WHERE json_extract(row_json,'$.trace_id')=?",
            (real,),
        ).fetchone()[0]
        > 0
    )
    assert core.execute("SELECT agent_id FROM agents").fetchall() == [
        ("fixture",),
        ("real",),
    ]
    assert core.execute(
        "SELECT kind FROM trace_projection_changes ORDER BY cursor DESC LIMIT 1"
    ).fetchone() == ("trace_expired",)

    cursor = core.execute(
        "SELECT change_cursor FROM trace_projection_state"
    ).fetchone()[0]
    assert cleanup.purge_core(core, [fixture], ["fixture"]) == {
        "agents": 0,
        "messages": 0,
        "payloads": 0,
    }
    assert (
        core.execute("SELECT change_cursor FROM trace_projection_state").fetchone()[0]
        == cursor
    )


@pytest.mark.parametrize("boundary", ["active_task", "open_binding", "unsettled"])
def test_source_cleanup_refuses_protected_trace(boundary):
    db = sqlite3.connect(":memory:")
    db.execute("ATTACH DATABASE ':memory:' AS task_state")
    db.executescript("""
        CREATE TABLE task_state.tasks(task_id,trace_id,state);
        CREATE TABLE trace_bindings(binding_id,trace_id,closed_at_ms);
        CREATE TABLE trace_journal(node_id,source_epoch,event_id,trace_id);
        CREATE TABLE trace_spool(node_id,source_epoch,journal_event_id,state,export_generation,collector_epoch,export_seq);
        CREATE TABLE trace_source_settlements(node_id,source_epoch,export_generation,collector_epoch,applied_through);
        INSERT INTO trace_journal VALUES('node','epoch','event','owned');
    """)
    if boundary == "active_task":
        db.execute("INSERT INTO task_state.tasks VALUES('task','owned','running')")
    elif boundary == "open_binding":
        db.execute("INSERT INTO trace_bindings VALUES('binding','owned',NULL)")
    else:
        db.execute(
            "INSERT INTO trace_spool VALUES('node','epoch','event','broker_acked','generation','collector',1)"
        )
    db.commit()
    with pytest.raises(ValueError, match="closed_settled"):
        cleanup.validate_source(db, ["owned"])
    assert db.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 1
    db.close()


def test_cleanup_rejects_protected_run_before_mutation(core):
    with pytest.raises(ValueError, match="protected_trace"):
        cleanup.purge_core(core, ["protected"], [], protected=["protected"])
    with pytest.raises(ValueError, match="protected_trace"):
        cleanup.purge_source(core, ["protected"], [], protected=["protected"])


def test_cleanup_deletes_only_messages_of_owned_tasks_for_shared_agent(core):
    removed, kept = uuid4().hex, uuid4().hex
    removed_task, kept_task = str(uuid4()), str(uuid4())
    generation = str(uuid4())
    for seq, (trace, task) in enumerate(
        ((removed, removed_task), (kept, kept_task)), 1
    ):
        put(core, event(seq=seq, trace_id=trace, task_id=task), generation, seq)
    project_all(core)
    core.executescript(
        "CREATE TABLE agents(agent_id TEXT PRIMARY KEY); CREATE TABLE messages(sender_id TEXT,recipient_id TEXT,task_id TEXT);"
    )
    core.execute("INSERT INTO agents VALUES('codex')")
    core.executemany(
        "INSERT INTO messages VALUES('codex','hermes',?)",
        [(removed_task,), (kept_task,), (None,)],
    )
    core.commit()
    cleanup.purge_core(core, [removed], ["codex"], protected=[kept])
    assert core.execute("SELECT task_id FROM messages").fetchall() == [
        (kept_task,),
        (None,),
    ]
    assert core.execute("SELECT agent_id FROM agents").fetchall() == [("codex",)]


def test_inventory_opens_consistent_read_transaction(core):
    core.execute("CREATE TABLE messages(task_id TEXT)")
    core.commit()
    assert cleanup.core_inventory(core) == {
        "run_ids": [],
        "history_rows": 0,
        "messages": 0,
        "retained_payloads": 0,
    }
    assert not core.in_transaction
