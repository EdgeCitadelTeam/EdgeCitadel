"""Offline maintenance for explicitly owned, settled jim-eq E2E fixtures.

Keep source/export identities, receipts and executable task state. Remove owned
trace payloads, derived graph history, messages and revoked agent registrations.
The runner stops writers and validates all sources before calling these helpers.
"""

import json
import re
import sqlite3
import time

FIXTURE = re.compile(
    r"(?:trace-(?:e2e|denial|s1|ui-fixture|large-fixture|latency-pilot)-[0-9a-f]{8,12}"
    r"|trace-baseline-[0-9a-f]{8,12}-\d+|jim-eq-s1-[0-9a-f]{8}-[abc])\Z"
)
TERMINAL = ("completed", "failed", "rejected", "cancelled", "expired", "undeliverable")


def scope_table(db, name, values):
    if name not in ("owned_agents", "owned_traces", "owned_events"):
        raise ValueError("invalid_scope")
    db.execute(f"CREATE TEMP TABLE IF NOT EXISTS {name}(id TEXT PRIMARY KEY)")
    db.execute(f"DELETE FROM {name}")
    db.executemany(f"INSERT INTO {name} VALUES(?)", [(value,) for value in values])
    db.commit()


def source_plan(db):
    """Task DB must be attached as task_state; inspect without modifying it."""
    agents = []
    for agent, connector, revoked in db.execute(
        "SELECT agent_id,connector_id,revoked_at_ms FROM task_state.connectors"
    ):
        if not FIXTURE.fullmatch(agent) or revoked is None:
            continue
        if db.execute(
            "SELECT 1 FROM task_state.sessions WHERE connector_id=? AND closed_at_ms IS NULL",
            (connector,),
        ).fetchone():
            continue
        agents.append(agent)
    scope_table(db, "owned_agents", agents)
    traces = {
        row[0]
        for row in db.execute(
            "SELECT DISTINCT trace_id FROM trace_journal WHERE agent_id IN (SELECT id FROM owned_agents) AND trace_id IS NOT NULL "
            "UNION SELECT trace_id FROM trace_bindings WHERE agent_id IN (SELECT id FROM owned_agents)"
        )
    }
    # Archive fixtures have no registered connector. The grant and receipt are
    # the ownership proof; arbitrary user-provided trace labels are not enough.
    for source, receipt in db.execute(
        "SELECT g.import_source_id,r.receipt_json FROM trace_import_records r "
        "JOIN trace_import_grants g USING(namespace_id)"
    ):
        if re.fullmatch(r"archive-e2e-[0-9a-f]{32}", source):
            identity = json.loads(receipt)
            row = db.execute(
                "SELECT trace_id FROM trace_journal WHERE source_epoch=? AND event_id=?",
                (identity["source_epoch"], identity["event_id"]),
            ).fetchone()
            if row and row[0]:
                traces.add(row[0])
    return {"agents": sorted(agents), "traces": sorted(traces)}


def validate_source(db, traces):
    scope_table(db, "owned_traces", traces)
    active = db.execute(
        "SELECT task_id FROM task_state.tasks WHERE trace_id IN (SELECT id FROM owned_traces) "
        "AND state NOT IN (?,?,?,?,?,?) LIMIT 1",
        TERMINAL,
    ).fetchone()
    bindings = db.execute(
        "SELECT binding_id FROM trace_bindings WHERE trace_id IN (SELECT id FROM owned_traces) AND closed_at_ms IS NULL LIMIT 1"
    ).fetchone()
    unsettled = db.execute(
        "SELECT 1 FROM trace_journal j JOIN trace_spool p ON p.node_id=j.node_id "
        "AND p.source_epoch=j.source_epoch AND p.journal_event_id=j.event_id "
        "WHERE j.trace_id IN (SELECT id FROM owned_traces) AND (p.state<>'core_settled' OR NOT EXISTS ("
        "SELECT 1 FROM trace_source_settlements c WHERE c.node_id=p.node_id AND c.source_epoch=p.source_epoch "
        "AND c.export_generation=p.export_generation AND c.collector_epoch=p.collector_epoch AND c.applied_through>=p.export_seq)) LIMIT 1"
    ).fetchone()
    if active or bindings or unsettled:
        raise ValueError("fixture_cleanup_requires_closed_settled_traces")


def purge_source(db, traces, agents):
    validate_source(db, traces)
    scope_table(db, "owned_agents", agents)
    deleted = 0
    # Keep rollback journals comfortably within the source's enforced quota.
    while True:
        with db:
            db.execute("BEGIN IMMEDIATE")
            events = db.execute(
                "SELECT node_id,source_epoch,event_id FROM trace_journal "
                "WHERE trace_id IN (SELECT id FROM owned_traces) LIMIT 128"
            ).fetchall()
            if not events:
                break
            db.executemany(
                "UPDATE trace_spool SET journal_event_id=NULL WHERE node_id=? AND source_epoch=? AND journal_event_id=?",
                events,
            )
            db.executemany(
                "DELETE FROM trace_journal WHERE node_id=? AND source_epoch=? AND event_id=?",
                events,
            )
            deleted += len(events)
    with db:
        db.execute("PRAGMA defer_foreign_keys=ON")
        db.execute(
            "DELETE FROM trace_operations WHERE binding_id IN (SELECT binding_id FROM trace_bindings WHERE trace_id IN (SELECT id FROM owned_traces))"
        )
        db.execute(
            "DELETE FROM trace_requests WHERE binding_id IN (SELECT binding_id FROM trace_bindings WHERE trace_id IN (SELECT id FROM owned_traces))"
        )
    for table in ("events", "spans"):
        with db:
            db.execute(
                f"DELETE FROM {table} WHERE trace_id IN (SELECT id FROM owned_traces) OR agent_id IN (SELECT id FROM owned_agents)"
            )
    with db:
        db.execute(
            "DELETE FROM presence_history WHERE agent_id IN (SELECT id FROM owned_agents)"
        )
        db.execute(
            "UPDATE trace_import_grants SET enabled=0 WHERE import_source_id GLOB 'archive-e2e-*'"
        )
    return deleted


def purge_core(db, traces, agents):
    from aggregator import trace_payloads
    from aggregator import trace_projection_history as history
    from aggregator.trace_projection_retention import TRACE_TABLES, _finish
    from aggregator.trace_projection_store import _state
    from aggregator.trace_projection_tables import select_tables

    if trace_payloads.is_prepared(db):
        trace_payloads.open_layout(db)
    scope_table(db, "owned_traces", traces)
    scope_table(db, "owned_agents", agents)
    now = time.time_ns() // 1_000_000
    with db:
        db.execute("BEGIN IMMEDIATE")
        tables = select_tables(db)
        state = _state(tables)
        high = db.execute(
            "SELECT ingest_seq FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        if state.ingest_cursor != high:
            raise ValueError("fixture_cleanup_requires_caught_up_projection")
        if db.execute(
            "SELECT 1 FROM trace_projection_generations WHERE status='building'"
        ).fetchone():
            raise ValueError("fixture_cleanup_during_rebuild")
    db.execute("CREATE TEMP TABLE owned_tasks(task_id TEXT PRIMARY KEY)")
    with db:
        tables.execute(
            "INSERT INTO owned_tasks SELECT DISTINCT task_id FROM {trace_projected_tasks} WHERE trace_id IN (SELECT id FROM owned_traces)"
        )
    payloads = trace_payloads.is_prepared(db)
    db.execute("CREATE TEMP TABLE owned_payloads(ingest_seq INTEGER PRIMARY KEY)")
    with db:
        db.execute(
            "INSERT INTO owned_payloads SELECT ingest_seq FROM "
            + ("trace_payloads" if payloads else "trace_raw_events")
            + " WHERE event_json<>'' AND json_extract(event_json,'$.trace_id') IN (SELECT id FROM owned_traces)"
        )
    count = db.execute("SELECT count(*) FROM owned_payloads").fetchone()[0]
    # Each trace retirement publishes a durable change before erasing its history.
    for trace in traces:
        with db:
            db.execute("BEGIN IMMEDIATE")
            tables = select_tables(db)
            state = _state(tables)
            history.start_change(
                tables, state.change_cursor + 1, state.ingest_cursor, received_at_ms=now
            )
            for name in (*TRACE_TABLES, "trace_projection_runs"):
                tables.execute("DELETE FROM {" + name + "} WHERE trace_id=?", (trace,))
            _finish(
                tables,
                state,
                trace,
                "trace_expired",
                {"trace_id": trace, "reason": "retention_expired"},
            )
    with db:
        tables.execute(
            "DELETE FROM {trace_projection_history_rows} WHERE json_extract(row_json,'$.trace_id') IN (SELECT id FROM owned_traces)"
        )
        tables.execute(
            "DELETE FROM {trace_projection_changes} WHERE trace_id IN (SELECT id FROM owned_traces) AND kind<>'trace_expired'"
        )
        if payloads:
            db.execute(
                "DELETE FROM trace_payloads WHERE ingest_seq IN (SELECT ingest_seq FROM owned_payloads)"
            )
        db.execute(
            "UPDATE trace_raw_events SET event_json='',payload_expired_at_ms=? WHERE ingest_seq IN (SELECT ingest_seq FROM owned_payloads)",
            (now,),
        )
        messages = db.execute(
            "DELETE FROM messages WHERE task_id IN (SELECT task_id FROM owned_tasks) OR sender_id IN (SELECT id FROM owned_agents) OR recipient_id IN (SELECT id FROM owned_agents)"
        ).rowcount
        cards = db.execute(
            "DELETE FROM agents WHERE agent_id IN (SELECT id FROM owned_agents)"
        ).rowcount
    return {"payloads": count, "messages": messages, "agents": cards}


if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Only invoked inside the stopped Core's disposable maintenance container.
    manifest = json.loads(Path(sys.argv[1]).read_text())
    connection = sqlite3.connect("/data/openclaw.db", timeout=30)
    print(json.dumps(purge_core(connection, manifest["traces"], manifest["agents"])))
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.execute("VACUUM")
    connection.close()
