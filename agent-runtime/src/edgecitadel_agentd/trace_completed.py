"""Authoritative completed-slot projections and transactional materialization."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from .trace_contract import TraceContractError, canonical_bytes, validate_rpc_reply
from .trace_reservations import SLOT_BYTES, Obligation, encode_record, fill

JOURNAL_COLUMNS = (
    "node_id",
    "source_epoch",
    "event_id",
    "source_seq",
    "trace_id",
    "agent_id",
    "task_id",
    "event_sha256",
    "event_json",
    "event_bytes",
    "received_at_ms",
)
SPOOL_COLUMNS = (
    "node_id",
    "source_epoch",
    "export_generation",
    "export_seq",
    "event_id",
    "journal_event_id",
    "event_sha256",
    "state",
    "collector_epoch",
    "core_outcome",
)
RECEIPT_COLUMNS = (
    "connector_id",
    "operation",
    "scope",
    "request_id",
    "request_sha256",
    "binding_id",
    "result_json",
)


def _check_settlement_room(record: dict[str, Any]) -> None:
    # A valid completion must remain writable when the null collector/outcome
    # become their longest legal values. Reserve this encoded space before the
    # completing transaction commits, including after adding its retry receipt.
    encode_record(
        {
            **record,
            "state": "lost_with_marker",
            "collector_epoch": "f" * 36,
            "core_outcome": "accepted",
        }
    )


def fill_completed(
    db: sqlite3.Connection, obligation: Obligation, record: dict[str, Any]
) -> int:
    _check_settlement_room(record)
    return fill(db, obligation, record)


def _references():
    return (
        (
            "task",
            "tasks",
            ("task_id", "trace_id"),
            ("event.task_id", "event.trace_id"),
            "json_extract(CAST({record} AS TEXT),'$.legacy_event') IS NOT NULL",
        ),
        (
            "source",
            "trace_sources",
            ("node_id", "source_epoch"),
            ("event.node_id", "event.source_epoch"),
            "1",
        ),
        (
            "generation",
            "trace_export_generations",
            ("node_id", "source_epoch", "export_generation"),
            ("event.node_id", "event.source_epoch", "export_generation"),
            "json_extract(CAST({record} AS TEXT),'$.export_seq') IS NOT NULL",
        ),
        (
            "receipt",
            "trace_bindings",
            ("binding_id", "connector_id"),
            ("receipt.binding_id", "receipt.connector_id"),
            "json_extract(CAST({record} AS TEXT),'$.receipt') IS NOT NULL",
        ),
        (
            "binding",
            "trace_bindings",
            ("binding_id", "trace_id", "execution_attempt_id"),
            ("binding.binding_id", "event.trace_id", "event.execution_attempt_id"),
            "json_extract(CAST({record} AS TEXT),'$.binding') IS NOT NULL",
        ),
        (
            "operation",
            "trace_operations",
            ("span_id", "binding_id", "kind", "name"),
            (
                "operation.span_id",
                "operation.binding_id",
                "event.kind",
                "event.attributes.name",
            ),
            "json_extract(CAST({record} AS TEXT),'$.operation') IS NOT NULL",
        ),
    )


def verify_references(db: sqlite3.Connection) -> None:
    for _, parent, keys, paths, enabled in _references():
        match = " AND ".join(
            f"p.{key}=json_extract(CAST(s.record AS TEXT),'$.{path}')"
            for key, path in zip(keys, paths, strict=True)
        )
        if db.execute(
            f"SELECT 1 FROM trace_completion_slots s WHERE s.filled=1 AND ({enabled.format(record='s.record')}) AND NOT EXISTS (SELECT 1 FROM {parent} p WHERE {match}) LIMIT 1"
        ).fetchone():
            raise sqlite3.IntegrityError("completed record reference check failed")


def install_reference_guards(db: sqlite3.Connection) -> None:
    """JSON-held facts retain the same parent protection as indexed evidence."""
    for name, parent, keys, paths, enabled in _references():
        new_match = " AND ".join(
            f"p.{key}=json_extract(CAST(NEW.record AS TEXT),'$.{path}')"
            for key, path in zip(keys, paths, strict=True)
        )
        for operation in ("INSERT", "UPDATE"):
            db.execute(f"""CREATE TEMP TRIGGER completed_{name}_{operation} BEFORE {operation} ON trace_completion_slots
                WHEN NEW.filled=1 AND ({enabled.format(record="NEW.record")})
                AND NOT EXISTS (SELECT 1 FROM {parent} p WHERE {new_match})
                BEGIN SELECT RAISE(ABORT,'completed record reference constraint failed'); END""")
        old_match = " AND ".join(
            f"OLD.{key}=json_extract(CAST(s.record AS TEXT),'$.{path}')"
            for key, path in zip(keys, paths, strict=True)
        )
        for operation in ("DELETE", "UPDATE"):
            changed = (
                "1"
                if operation == "DELETE"
                else " OR ".join(f"NEW.{key} IS NOT OLD.{key}" for key in keys)
            )
            db.execute(f"""CREATE TEMP TRIGGER completed_{name}_{operation}_parent BEFORE {operation} ON {parent}
                WHEN ({changed}) AND EXISTS (SELECT 1 FROM trace_completion_slots s WHERE s.filled=1
                    AND ({enabled.format(record="s.record")}) AND {old_match})
                BEGIN SELECT RAISE(ABORT,'completed record reference constraint failed'); END""")


def install_views(db: sqlite3.Connection) -> None:
    """Persistent projections also serve read-only operator connections."""
    event_columns = {
        "node_id",
        "source_epoch",
        "event_id",
        "source_seq",
        "trace_id",
        "agent_id",
        "task_id",
    }
    journal = []
    for name in JOURNAL_COLUMNS:
        path = (
            "event"
            if name == "event_json"
            else f"event.{name}"
            if name in event_columns
            else name
        )
        journal.append(f"json_extract(CAST(record AS TEXT),'$.{path}') AS {name}")
    spool = []
    for name in SPOOL_COLUMNS:
        path = (
            "event.event_id"
            if name == "journal_event_id"
            else f"event.{name}"
            if name in event_columns
            else name
        )
        spool.append(f"json_extract(CAST(record AS TEXT),'$.{path}') AS {name}")
    receipt = [
        f"json_extract(CAST(record AS TEXT),'$.receipt.{name}') AS {name}"
        for name in RECEIPT_COLUMNS
    ]
    for name, columns, projection, condition in (
        ("journal", JOURNAL_COLUMNS, journal, ""),
        (
            "spool",
            SPOOL_COLUMNS,
            spool,
            " AND json_extract(CAST(record AS TEXT),'$.export_seq') IS NOT NULL",
        ),
        (
            "requests",
            RECEIPT_COLUMNS,
            receipt,
            " AND json_extract(CAST(record AS TEXT),'$.receipt') IS NOT NULL",
        ),
    ):
        indexed_slot = ",NULL AS _slot_id" if name == "spool" else ""
        reserved_slot = ",slot_id AS _slot_id" if name == "spool" else ""
        db.execute(
            f"CREATE VIEW IF NOT EXISTS trace_{name}_all AS SELECT {','.join(columns)}{indexed_slot} FROM trace_{name} "
            f"UNION ALL SELECT {','.join(projection)}{reserved_slot} FROM trace_completion_slots WHERE filled=1{condition}"
        )
    # Join indexed storage before unioning slots. Joining the two union views
    # makes SQLite materialize the entire retained journal for a small page.
    payload = ("event_json", "event_bytes", "received_at_ms")
    db.execute(
        "CREATE VIEW IF NOT EXISTS trace_completed_export AS SELECT "
        + ",".join(spool + [journal[JOURNAL_COLUMNS.index(name)] for name in payload])
        + " FROM trace_completion_slots WHERE filled=1 AND json_extract(CAST(record AS TEXT),'$.export_seq') IS NOT NULL"
    )
    db.execute(
        "CREATE VIEW IF NOT EXISTS trace_export_records AS SELECT "
        + ",".join(f"s.{name}" for name in SPOOL_COLUMNS)
        + ","
        + ",".join(f"j.{name}" for name in payload)
        + " FROM trace_spool s LEFT JOIN trace_journal j ON j.node_id=s.node_id "
        "AND j.source_epoch=s.source_epoch AND j.event_id=s.journal_event_id "
        "UNION ALL SELECT "
        + ",".join(spool + [journal[JOURNAL_COLUMNS.index(name)] for name in payload])
        + " FROM trace_completion_slots WHERE filled=1 AND json_extract(CAST(record AS TEXT),'$.export_seq') IS NOT NULL"
    )
    # State updates must preserve the immutable event, position and reference.
    # SQLite printf's ordinary %s width counts UTF-8 bytes, matching the blob
    # length CHECK; an oversized update aborts instead of growing the slot.
    immutable = (
        "node_id",
        "source_epoch",
        "export_generation",
        "export_seq",
        "event_id",
        "journal_event_id",
        "event_sha256",
    )
    scope = " AND ".join(
        f"{name}=OLD.{name}"
        for name in ("node_id", "source_epoch", "export_generation", "export_seq")
    )
    unchanged = " AND ".join(f"NEW.{name} IS OLD.{name}" for name in immutable)
    db.execute(f"""CREATE TRIGGER IF NOT EXISTS trace_completed_spool_update
        INSTEAD OF UPDATE ON trace_spool_all BEGIN
        SELECT CASE WHEN NEW._slot_id IS NOT OLD._slot_id OR
            (OLD._slot_id IS NOT NULL AND NOT ({unchanged}))
            THEN RAISE(ABORT,'completed reference requires materialization') END;
        SELECT CASE WHEN NEW.state IS NULL OR NEW.state NOT IN ('pending','broker_acked','core_settled','lost_with_marker')
            OR (NEW.core_outcome IS NOT NULL AND NEW.core_outcome NOT IN ('accepted','rejected','lost'))
            OR (NEW.collector_epoch IS NOT NULL AND (typeof(NEW.collector_epoch)<>'text' OR length(NEW.collector_epoch)>36))
            THEN RAISE(ABORT,'invalid completed settlement') END;
        UPDATE trace_spool SET {",".join(f"{name}=NEW.{name}" for name in SPOOL_COLUMNS)} WHERE OLD._slot_id IS NULL AND {scope};
        UPDATE trace_completion_slots SET record=CAST(printf('%-{SLOT_BYTES}s',json_set(CAST(record AS TEXT),
            '$.state',NEW.state,'$.collector_epoch',NEW.collector_epoch,'$.core_outcome',NEW.core_outcome)) AS BLOB)
        WHERE filled=1 AND slot_id=OLD._slot_id;
        END""")
    db.execute("""CREATE VIEW IF NOT EXISTS trace_storage_usage_all AS
        SELECT singleton,event_bytes+(SELECT COALESCE(SUM(json_extract(CAST(record AS TEXT),'$.event_bytes')),0)
            FROM trace_completion_slots WHERE filled=1) AS event_bytes,
            event_count+(SELECT count(*) FROM trace_completion_slots WHERE filled=1) AS event_count
        FROM trace_storage_usage""")


def export_page(
    db: sqlite3.Connection,
    scope: tuple[str, str, str],
    *,
    after: int,
    limit: int,
    through: int | None = None,
    states: tuple[str, ...] = (),
    payload_required: bool = False,
) -> list[sqlite3.Row]:
    """Bound each storage branch before merging by export position.

    An outer LIMIT on a UNION view does not reliably stop the indexed branch.
    Explicit branch limits keep retained history from turning one page into a
    scope-wide payload scan. The other branch contains at most 512 slots.
    """
    condition = (
        "s.node_id=? AND s.source_epoch=? AND s.export_generation=? AND s.export_seq>?"
    )
    parameters: list[Any] = [*scope, after]
    if through is not None:
        condition += " AND s.export_seq<=?"
        parameters.append(through)
    if states:
        condition += " AND s.state IN (" + ",".join("?" for _ in states) + ")"
        parameters.extend(states)
    normal = (
        ",".join(f"s.{name}" for name in SPOOL_COLUMNS)
        + ",j.event_json,j.event_bytes,j.received_at_ms"
    )
    sql = (
        f"SELECT * FROM (SELECT {normal} FROM trace_spool s LEFT JOIN trace_journal j "
        "ON j.node_id=s.node_id AND j.source_epoch=s.source_epoch AND j.event_id=s.journal_event_id "
        f"WHERE {condition}"
        + (" AND j.event_json IS NOT NULL" if payload_required else "")
        + " ORDER BY s.export_seq LIMIT ?) UNION ALL SELECT * FROM (SELECT * FROM trace_completed_export s "
        f"WHERE {condition} ORDER BY s.export_seq LIMIT ?) ORDER BY export_seq LIMIT ?"
    )
    return db.execute(sql, (*parameters, limit, *parameters, limit, limit)).fetchall()


def attach_receipt(
    db: sqlite3.Connection, obligation: Obligation, receipt: dict[str, Any]
) -> None:
    """Caller already authorized the request and owns its completion transaction."""
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if set(receipt) != set(RECEIPT_COLUMNS):
        raise TraceContractError("invalid_completion_receipt")
    reply = json.loads(receipt["result_json"])
    validate_rpc_reply(
        reply, operation=receipt["operation"], request_id=receipt["request_id"]
    )
    if receipt["result_json"] != canonical_bytes(reply).decode():
        raise TraceContractError("invalid_completion_receipt")
    if (
        db.execute(
            "SELECT 1 FROM trace_bindings WHERE binding_id=? AND connector_id=?",
            (receipt["binding_id"], receipt["connector_id"]),
        ).fetchone()
        is None
    ):
        raise TraceContractError("invalid_completion_receipt")
    if db.execute(
        "SELECT 1 FROM trace_requests_all WHERE connector_id=? AND operation=? AND scope=? AND request_id=?",
        tuple(receipt[name] for name in RECEIPT_COLUMNS[:4]),
    ).fetchone():
        raise TraceContractError("completion_receipt_already_present")
    set_completed_metadata(db, obligation, "receipt", receipt)


def set_completed_metadata(
    db: sqlite3.Connection, obligation: Obligation, name: str, value: dict[str, Any]
) -> None:
    """Attach owned metadata once, before the completing transaction commits."""
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if name not in {"receipt", "binding", "operation", "legacy_event"}:
        raise TraceContractError("invalid_completion_metadata")
    row = db.execute(
        "SELECT slot_id,record FROM trace_completion_slots WHERE owner_kind=? AND owner_id=? AND purpose=? AND filled=1",
        obligation.key,
    ).fetchone()
    if row is None:
        raise TraceContractError("completion_reservation_missing")
    record = json.loads(row[1])
    if name in record:
        raise TraceContractError("completion_metadata_already_present")
    if name == "binding" and (
        obligation.kind != "run"
        or value.get("binding_id") != obligation.owner_id
        or type(value.get("closed_at_ms")) is not int
        or value["closed_at_ms"] < 0
        or record["event"]["kind"] != "run"
        or record["event"]["phase"] == "started"
    ):
        raise TraceContractError("invalid_completion_metadata")
    if name == "operation" and (
        obligation.kind != "operation"
        or value.get("span_id") != obligation.owner_id
        or record["event"]["span_id"] != obligation.owner_id
        or record["event"]["phase"] not in {"finished", "failed", "interrupted"}
    ):
        raise TraceContractError("invalid_completion_metadata")
    if name == "legacy_event" and (
        obligation.kind not in {"task", "attempt"}
        or any(
            value.get(key) != record["event"][key]
            for key in ("event_id", "agent_id", "task_id", "trace_id")
        )
        or value.get("event_type")
        not in (
            {"task.queued", "task.requeued"}
            if record["event"]["phase"] == "queued"
            else {"task." + record["event"]["phase"]}
        )
        or record["event"]["kind"] != "task"
        or record["event"]["task_id"] != obligation.owner_id
    ):
        raise TraceContractError("invalid_completion_metadata")
    record[name] = value
    _check_settlement_room(record)
    db.execute(
        "UPDATE trace_completion_slots SET record=? WHERE slot_id=?",
        (encode_record(record), row[0]),
    )


def materialize(db: sqlite3.Connection, slot_id: int) -> bool:
    """Move exact facts to indexed rows and release the slot in one transaction.

    This ordinary allocation may fail under pressure. It never borrows protected
    completion workspace; rollback leaves the completed record authoritative.
    """
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    workspace = getattr(db, "workspace", None)
    if workspace is not None and workspace.borrowed:
        raise TraceContractError("materialization_cannot_spend_completion_reserve")
    record = db.execute(
        "SELECT record FROM trace_completion_slots WHERE slot_id=? AND filled=1",
        (slot_id,),
    ).fetchone()
    if record is None:
        return False
    value = json.loads(record[0])
    event = value["event"]
    journal = {
        **event,
        **{
            key: value[key] for key in ("event_sha256", "event_bytes", "received_at_ms")
        },
        "event_json": canonical_bytes(event).decode(),
    }
    db.execute(
        f"INSERT INTO trace_journal({','.join(JOURNAL_COLUMNS)}) VALUES ({','.join('?' for _ in JOURNAL_COLUMNS)})",
        tuple(journal[key] for key in JOURNAL_COLUMNS),
    )
    if value["export_seq"] is not None:
        spool = {
            **value,
            "node_id": event["node_id"],
            "source_epoch": event["source_epoch"],
            "event_id": event["event_id"],
            "journal_event_id": event["event_id"],
        }
        db.execute(
            f"INSERT INTO trace_spool({','.join(SPOOL_COLUMNS)}) VALUES ({','.join('?' for _ in SPOOL_COLUMNS)})",
            tuple(spool[key] for key in SPOOL_COLUMNS),
        )
    if "receipt" in value:
        db.execute(
            f"INSERT INTO trace_requests({','.join(RECEIPT_COLUMNS)}) VALUES ({','.join('?' for _ in RECEIPT_COLUMNS)})",
            tuple(value["receipt"][key] for key in RECEIPT_COLUMNS),
        )
    from .trace_terminal import materialize as materialize_terminal

    materialize_terminal(db, value)
    from .trace_task_completion import materialize as materialize_task_event

    materialize_task_event(db, value)
    db.execute(
        "UPDATE trace_completion_slots SET owner_kind='',owner_id='',purpose='',filled=0,record=? WHERE slot_id=?",
        (b"{}".ljust(SLOT_BYTES, b" "), slot_id),
    )
    return True
