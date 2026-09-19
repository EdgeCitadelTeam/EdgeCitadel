"""Task/trace database ownership and atomic conversion of the shared schema."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from uuid import UUID, uuid4

TASK_TABLES = (
    "connectors",
    "sessions",
    "tasks",
    "task_attempts",
    "transport_outbox",
    "managed_agents",
    "restore_holds",
)
CROSS_REFERENCES = (
    ("trace_bindings", "connector_id", "connectors", "connector_id"),
    ("trace_bindings", "session_id", "sessions", "session_id"),
    ("trace_bindings", "task_id", "tasks", "task_id"),
    ("trace_requests", "connector_id", "connectors", "connector_id"),
    ("trace_task_contexts", "task_id", "tasks", "task_id"),
)
PAIR_VERSION = 24


def task_database_path(trace_path: Path) -> Path:
    return trace_path.with_name(trace_path.stem + "-tasks.sqlite3")


def attach_tasks(db: sqlite3.Connection, path: Path, *, existing: bool) -> None:
    """Existing pairs never recreate a missing member."""
    uri = path.resolve().as_uri() + ("?mode=rw" if existing else "?mode=rwc")
    db.execute("ATTACH DATABASE ? AS task_state", (uri,))
    if existing:
        # A misplaced database is not ours to change, even to set its journal mode.
        verify_pair(db, version=db.execute("PRAGMA user_version").fetchone()[0])
    if db.execute("PRAGMA task_state.journal_mode=DELETE").fetchone()[0] != "delete":
        raise sqlite3.DatabaseError("task storage requires rollback journaling")
    db.execute("PRAGMA task_state.synchronous=EXTRA")


def verify_pair(db: sqlite3.Connection, *, version: int = 26) -> None:
    if version not in (24, 25, 26):
        raise sqlite3.DatabaseError("unsupported storage pair schema")
    ids = []
    for schema in ("main", "task_state"):
        if db.execute(f"PRAGMA {schema}.user_version").fetchone()[0] != version:
            raise sqlite3.DatabaseError("storage pair schema versions differ")
    for schema in ("main", "task_state"):
        rows = db.execute(f"SELECT pair_id FROM {schema}.storage_pair").fetchall()
        if len(rows) != 1:
            raise sqlite3.DatabaseError("storage pair identity is missing")
        identity = rows[0][0]
        try:
            if str(UUID(identity)) != identity:
                raise ValueError
        except (ValueError, TypeError, AttributeError) as error:
            raise sqlite3.DatabaseError("storage pair identity is invalid") from error
        ids.append(identity)
    tables = {
        row[0]
        for row in db.execute(
            "SELECT name FROM task_state.sqlite_schema WHERE type='table'"
        )
    }
    if tables != set(TASK_TABLES) | {"storage_pair"}:
        raise sqlite3.DatabaseError("task storage has an unexpected table inventory")
    if ids[0] != ids[1]:
        raise sqlite3.DatabaseError("storage pair identities differ")
    for schema, unexpected in (
        ("main", TASK_TABLES),
        ("task_state", ("trace_journal",)),
    ):
        for name in unexpected:
            if db.execute(
                f"SELECT 1 FROM {schema}.sqlite_schema WHERE type='table' AND name=?",
                (name,),
            ).fetchone():
                raise sqlite3.DatabaseError("storage pair contains misplaced tables")


def verify_references(db: sqlite3.Connection) -> None:
    # Pair conversion also calls this before the completion schema exists.
    if db.execute(
        "SELECT 1 FROM main.sqlite_schema WHERE name='trace_completion_slots'"
    ).fetchone():
        from .trace_completed import verify_references as verify_completed

        verify_completed(db)
    for child, column, parent, key in CROSS_REFERENCES:
        if db.execute(
            f'SELECT 1 FROM main."{child}" c WHERE c."{column}" IS NOT NULL '
            f'AND NOT EXISTS (SELECT 1 FROM task_state."{parent}" p '
            f'WHERE p."{key}"=c."{column}") LIMIT 1'
        ).fetchone():
            raise sqlite3.IntegrityError("cross-store foreign key check failed")


def install_reference_guards(db: sqlite3.Connection) -> None:
    """SQLite forbids persistent cross-schema FKs; the sole writer owns these.

    Each statement sees both stores in the caller's transaction. Offline restore
    also scans references before activation; raw external writers are unsupported.
    """
    for child, column, parent, key in CROSS_REFERENCES:
        name = f"pair_{child}_{column}"
        for operation in ("INSERT", "UPDATE"):
            db.execute(
                f'CREATE TEMP TRIGGER "{name}_{operation}" BEFORE {operation} ON main."{child}" '
                f'WHEN NEW."{column}" IS NOT NULL AND NOT EXISTS '
                f'(SELECT 1 FROM task_state."{parent}" WHERE "{key}"=NEW."{column}") '
                "BEGIN SELECT RAISE(ABORT, 'cross-store foreign key constraint failed'); END"
            )
        for operation in ("DELETE", f'UPDATE OF "{key}"'):
            suffix = operation.split()[0]
            condition = (
                "" if suffix == "DELETE" else f'NEW."{key}" IS NOT OLD."{key}" AND '
            )
            db.execute(
                f'CREATE TEMP TRIGGER "{name}_{suffix}_parent" BEFORE {operation} '
                f'ON task_state."{parent}" WHEN {condition}EXISTS '
                f'(SELECT 1 FROM main."{child}" WHERE "{column}"=OLD."{key}") '
                "BEGIN SELECT RAISE(ABORT, 'cross-store foreign key constraint failed'); END"
            )


def _objects(db: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    return [
        tuple(row)
        for row in db.execute(
            "SELECT type,sql FROM main.sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL "
            "ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END",
            (table,),
        )
    ]


def _qualified(sql: str, schema: str) -> str:
    return re.sub(
        r"^(CREATE (?:UNIQUE )?(?:TABLE|INDEX) )", r"\1" + schema + ".", sql, count=1
    )


def _copy_rows(db: sqlite3.Connection, table: str, target: str) -> None:
    columns = ",".join(
        '"' + row[1] + '"' for row in db.execute(f'PRAGMA main.table_info("{table}")')
    )
    db.execute(
        f'INSERT INTO {target}(rowid,{columns}) SELECT rowid,{columns} FROM main."{table}"'
    )


def migrate_pair(db: sqlite3.Connection) -> None:
    """Caller holds one attached rollback transaction with FK enforcement off.

    Copies remain inside SQLite transactions. Refusal/crash leaves the old schema
    intact; migration needs headroom for copies and journals, never manual unlink.
    """
    if not db.in_transaction or db.execute("PRAGMA foreign_keys").fetchone()[0]:
        raise sqlite3.ProgrammingError("pair migration requires its owned transaction")
    if db.execute("SELECT 1 FROM task_state.sqlite_schema LIMIT 1").fetchone():
        raise sqlite3.DatabaseError("migration task destination is not empty")
    for table in TASK_TABLES:
        for kind, sql in _objects(db, table):
            if kind not in ("table", "index"):
                raise sqlite3.DatabaseError("unexpected task schema object")
            db.execute(_qualified(sql, "task_state"))
        _copy_rows(db, table, f'task_state."{table}"')
    # Rebuild only tables whose foreign keys cross the ownership boundary.
    for table in dict.fromkeys(row[0] for row in CROSS_REFERENCES):
        objects = _objects(db, table)
        sql = objects[0][1]
        for child, column, parent, key in CROSS_REFERENCES:
            if child == table:
                sql = sql.replace(f" REFERENCES {parent}({key})", "")
        scratch = "pair_rebuild_" + table
        sql = re.sub(
            r'^CREATE TABLE (?:"' + table + r'"|' + table + r")(?=\s*\()",
            'CREATE TABLE "' + scratch + '"',
            sql,
            count=1,
        )
        db.execute(sql)
        _copy_rows(db, table, f'main."{scratch}"')
        db.execute(f'DROP TABLE "{table}"')
        db.execute(f'ALTER TABLE "{scratch}" RENAME TO "{table}"')
        for kind, sql in objects[1:]:
            if kind != "index":
                raise sqlite3.DatabaseError("unexpected trace schema object")
            db.execute(sql)
    for table in reversed(TASK_TABLES):
        db.execute(f'DROP TABLE main."{table}"')
    identity = str(uuid4())
    for schema in ("main", "task_state"):
        db.execute(
            f"CREATE TABLE {schema}.storage_pair (singleton INTEGER PRIMARY KEY CHECK(singleton=1), pair_id TEXT NOT NULL)"
        )
        db.execute(f"INSERT INTO {schema}.storage_pair VALUES (1,?)", (identity,))
        db.execute(f"PRAGMA {schema}.user_version={PAIR_VERSION}")
        if db.execute(f"PRAGMA {schema}.foreign_key_check").fetchone():
            raise sqlite3.IntegrityError("storage migration foreign key check failed")
    verify_references(db)


def attach_task_snapshot(
    db: sqlite3.Connection, trace_path: Path, *, task_path: Path | None = None
) -> bool:
    """Attach the paired snapshot read-only; legacy shared snapshots need no pair."""
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if version < PAIR_VERSION:
        return False
    db.execute(
        "ATTACH DATABASE ? AS task_state",
        (
            (task_path or task_database_path(trace_path)).resolve().as_uri()
            + "?mode=ro",
        ),
    )
    verify_pair(db, version=version)
    return True
