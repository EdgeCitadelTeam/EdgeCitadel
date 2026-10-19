"""Fixed-width sequence storage with derived integer read columns."""

from __future__ import annotations

import re
import sqlite3

from .trace_contract import TraceContractError

# One beyond the largest v1 JSON integer marks an exhausted sequence.
MAX_COUNTER = 2**53


def encode_counter(value: int) -> bytes:
    if type(value) is not int or not 1 <= value <= MAX_COUNTER:
        raise TraceContractError("trace_sequence_exhausted")
    return f"{value:020d}".encode("ascii")


def migrate_counters(db: sqlite3.Connection) -> None:
    """Rebuild only the small identity tables inside the paired migration.

    Generated integer columns keep reads numeric without a second stored value.
    Fixed ASCII blobs retain their SQLite serial width across every increment.
    Migration requires headroom; failure rolls back both stores, never truncates
    identities or resets positions. Foreign keys are checked before commit.
    """
    if not db.in_transaction or db.execute("PRAGMA foreign_keys").fetchone()[0]:
        raise sqlite3.ProgrammingError("counter migration requires its transaction")
    for table, column in (
        ("trace_sources", "next_source_seq"),
        ("trace_export_generations", "next_export_seq"),
    ):
        stored = column + "_bytes"
        columns = [row[1] for row in db.execute(f"PRAGMA table_info({table})")]
        if stored in columns:
            continue
        for (value,) in db.execute(f"SELECT {column} FROM {table}"):
            encode_counter(value)
        objects = db.execute(
            "SELECT type,sql FROM sqlite_schema WHERE tbl_name=? AND sql IS NOT NULL "
            "ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END",
            (table,),
        ).fetchall()
        replacement = f"""{stored} BLOB NOT NULL DEFAULT X'{encode_counter(1).hex()}'
            CHECK(typeof({stored})='blob' AND length({stored})=20
                AND hex({stored}) GLOB '{"3[0-9]" * 20}'
                AND {stored} BETWEEN X'{encode_counter(1).hex()}'
                    AND X'{encode_counter(MAX_COUNTER).hex()}'),
            {column} INTEGER GENERATED ALWAYS AS (CAST({stored} AS INTEGER)) VIRTUAL"""
        sql, count = re.subn(
            rf"{column} INTEGER NOT NULL DEFAULT 1 CHECK\({column} > 0\)",
            replacement,
            objects[0][1],
            count=1,
        )
        if count != 1 or any(kind != "index" for kind, _ in objects[1:]):
            raise sqlite3.DatabaseError("unexpected trace counter schema")
        scratch = table + "_counter_migration"
        sql, count = re.subn(
            rf'^CREATE TABLE (?:"{table}"|{table})(?=\s*\()',
            f"CREATE TABLE {scratch}",
            sql,
            count=1,
        )
        if count != 1:
            raise sqlite3.DatabaseError("unexpected trace counter table")
        db.execute(sql)
        target = ",".join(stored if name == column else name for name in columns)
        source = ",".join(
            f"CAST(printf('%020d',{column}) AS BLOB)" if name == column else name
            for name in columns
        )
        db.execute(
            f"INSERT INTO {scratch}(rowid,{target}) SELECT rowid,{source} FROM {table}"
        )
        db.execute(f"DROP TABLE {table}")
        db.execute(f"ALTER TABLE {scratch} RENAME TO {table}")
        for _, sql in objects[1:]:
            db.execute(sql)
    if db.execute("PRAGMA main.foreign_key_check").fetchone():
        raise sqlite3.IntegrityError("trace counter migration foreign key check failed")
