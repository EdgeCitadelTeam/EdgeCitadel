"""Admission invariants for the pages a reserved completion may overwrite."""

from __future__ import annotations

import re
import sqlite3

from .trace_reservations import MAX_SLOTS, SLOT_BYTES

MAX_IDENTITIES = 512
PAGE_BYTES = 4096
# At most eight overflow pages and one existing table leaf per bounded slot.
# Counter records fit entirely on one table page. Page 1 is journaled too.
MAX_COMPLETION_PAGES = MAX_SLOTS * 9 + MAX_IDENTITIES * 2 + 1 + 1

_TEXT_BOUNDS = {
    "trace_completion_slots": {"owner_kind": 16, "owner_id": 128, "purpose": 32},
    "trace_sources": {"node_id": 64, "source_epoch": 36, "test_run_id": 36},
    "trace_export_generations": {
        "node_id": 64,
        "source_epoch": 36,
        "export_generation": 36,
        "sync_fault": 64,
    },
    "trace_presence_counter": {},
}
_SPECIAL = {
    "trace_completion_slots": {"slot_id", "filled", "record"},
    "trace_sources": {"active", "next_source_seq_bytes", "next_source_seq"},
    "trace_export_generations": {"active", "next_export_seq_bytes", "next_export_seq"},
    "trace_presence_counter": {"singleton", "next_id"},
}
_INDEXES = {
    "trace_completion_slots": {
        "trace_completion_owner": "CREATE UNIQUE INDEX trace_completion_owner ON trace_completion_slots(owner_kind,owner_id,purpose) WHERE owner_id<>''",
    },
    "trace_sources": {
        "sqlite_autoindex_trace_sources_1": None,
        "trace_one_active_source": "CREATE UNIQUE INDEX trace_one_active_source ON trace_sources(node_id) WHERE active=1",
    },
    "trace_export_generations": {
        "sqlite_autoindex_trace_export_generations_1": None,
        "trace_one_active_export": "CREATE UNIQUE INDEX trace_one_active_export ON trace_export_generations(node_id,source_epoch) WHERE active=1",
    },
    "trace_presence_counter": {},
}


def protects_schema(
    action: int, first: str | None, second: str | None, database: str | None
) -> bool:
    if (
        action == sqlite3.SQLITE_DROP_TEMP_TRIGGER
        and first
        and first.startswith("completion_geometry_")
    ):
        return True
    if action == sqlite3.SQLITE_ALTER_TABLE:
        return first == "main" and second in _TEXT_BOUNDS
    if database != "main":
        return False
    if action in {sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE}:
        return first in _TEXT_BOUNDS
    if action in {
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_DROP_INDEX,
    }:
        return second in _TEXT_BOUNDS
    return False


def _normalize(sql: str | None) -> str | None:
    return re.sub(r"\s+", "", sql).lower() if sql is not None else None


def _invalid_row(table: str, prefix: str = "") -> str:
    checks = [
        f"length(CAST({prefix}{name} AS BLOB))>{limit}"
        for name, limit in _TEXT_BOUNDS[table].items()
    ]
    if table == "trace_completion_slots":
        checks += [
            f"typeof({prefix}slot_id)<>'integer'",
            f"{prefix}slot_id NOT BETWEEN 1 AND {MAX_SLOTS}",
            f"typeof({prefix}filled)<>'integer'",
            f"{prefix}filled NOT IN (0,1)",
        ]
        blob, width = "record", SLOT_BYTES
    elif table == "trace_presence_counter":
        checks += [f"typeof({prefix}singleton)<>'integer'", f"{prefix}singleton<>1"]
        blob, width = "next_id", 20
    else:
        checks += [f"typeof({prefix}active)<>'integer'", f"{prefix}active NOT IN (0,1)"]
        blob, width = (
            (
                "next_source_seq_bytes"
                if table == "trace_sources"
                else "next_export_seq_bytes"
            ),
            20,
        )
    checks += [f"typeof({prefix}{blob})<>'blob'", f"length({prefix}{blob})<>{width}"]
    return " OR ".join(checks)


def verify_journal(db: sqlite3.Connection) -> None:
    """Observe the VFS sector geometry during the owned header transaction.

    A sector larger than a database page can journal neighbouring pages too;
    the fixed-record page bound only applies when sectors fit within one page.
    The journal belongs to SQLite and is only read here, never edited/deleted.
    """
    path = next(
        row[2] for row in db.execute("PRAGMA database_list") if row[1] == "main"
    )
    try:
        with open(path + "-journal", "rb") as journal:
            header = journal.read(28)
    except OSError as error:
        raise sqlite3.OperationalError(
            "completion journal geometry is unverifiable"
        ) from error
    sector = int.from_bytes(header[20:24], "big")
    if (
        len(header) != 28
        or not 512 <= sector <= PAGE_BYTES
        or sector & (sector - 1)
        or int.from_bytes(header[24:28], "big") != PAGE_BYTES
    ):
        raise sqlite3.NotSupportedError(
            "completion journal sector exceeds qualified page geometry"
        )


def install(db: sqlite3.Connection) -> None:
    """Verify restored data/schema, then keep ordinary admission within the bound.

    The caller owns the workspace, has recovered the pair, and has not admitted
    writers. Temporary guards belong to this handle; all production handles must
    install them. They do not fence an older executable or arbitrary raw writer.
    """
    if db.in_transaction:
        raise sqlite3.ProgrammingError(
            "geometry installation requires a closed transaction"
        )
    if db.execute("PRAGMA main.auto_vacuum").fetchone()[0] != 0:
        raise sqlite3.NotSupportedError("completion geometry requires auto_vacuum=NONE")
    if db.execute("PRAGMA main.encoding").fetchone()[0] != "UTF-8":
        raise sqlite3.NotSupportedError("completion geometry requires UTF-8 storage")
    path = next(
        row[2] for row in db.execute("PRAGMA database_list") if row[1] == "main"
    )
    with open(path, "rb") as source:
        header = source.read(100)
    if len(header) != 100 or header[20] != 0:
        raise sqlite3.NotSupportedError(
            "completion geometry requires all 4096 page bytes"
        )
    layouts = {row[1]: (row[2], row[4]) for row in db.execute("PRAGMA main.table_list")}
    for table, bounds in _TEXT_BOUNDS.items():
        columns = {
            row[1]: (row[6], row[2].upper(), row[5])
            for row in db.execute(f"PRAGMA main.table_xinfo({table})")
        }
        primary_keys = {
            "trace_completion_slots": ("slot_id",),
            "trace_sources": ("node_id", "source_epoch"),
            "trace_export_generations": (
                "node_id",
                "source_epoch",
                "export_generation",
            ),
            "trace_presence_counter": ("singleton",),
        }[table]
        expected = {
            name: (
                2 if name in {"next_source_seq", "next_export_seq"} else 0,
                "TEXT"
                if name in bounds
                else "BLOB"
                if name
                in {
                    "record",
                    "next_id",
                    "next_source_seq_bytes",
                    "next_export_seq_bytes",
                }
                else "INTEGER",
                primary_keys.index(name) + 1 if name in primary_keys else 0,
            )
            for name in bounds.keys() | _SPECIAL[table]
        }
        if columns != expected or layouts.get(table) != ("table", 0):
            raise sqlite3.NotSupportedError(
                "completion geometry has an unsupported table shape"
            )
        indexes = {
            row[0]: _normalize(row[1])
            for row in db.execute(
                "SELECT name,sql FROM main.sqlite_schema WHERE type='index' AND tbl_name=?",
                (table,),
            )
        }
        if indexes != {name: _normalize(sql) for name, sql in _INDEXES[table].items()}:
            raise sqlite3.NotSupportedError(
                "completion geometry has unsupported indexes"
            )
        limit = (
            MAX_SLOTS
            if table == "trace_completion_slots"
            else 1
            if table == "trace_presence_counter"
            else MAX_IDENTITIES
        )
        count = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if (
            count > limit
            or (table == "trace_presence_counter" and count != 1)
            or db.execute(
                f"SELECT 1 FROM {table} WHERE {_invalid_row(table)} LIMIT 1"
            ).fetchone()
        ):
            raise sqlite3.IntegrityError("completion geometry exceeds its row bounds")
        for operation in ("INSERT", "UPDATE"):
            condition = _invalid_row(table, "NEW.")
            if operation == "INSERT":
                condition += f" OR (SELECT count(*) FROM {table})>={limit}"
            db.execute(f"""CREATE TEMP TRIGGER completion_geometry_{table}_{operation}
                BEFORE {operation} ON main.{table} WHEN {condition}
                BEGIN SELECT RAISE(ABORT,'completion geometry admission limit'); END""")
