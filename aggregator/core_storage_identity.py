"""Read-only Core role/fence attestation inside an existing SQLite snapshot.

The caller holds lifecycle ownership, checks mount authority and starts a read
transaction before calling attest_role. Opening SQLite mode=ro can create SHM;
this module does not open files, initialize schema, or grant writer capability.
"""

from __future__ import annotations

import sqlite3
from uuid import UUID

from .core_storage import Role, StorageManifest, StorageUnavailable
from .trace_payloads import GUARDED_TABLES

IDENTITY_TABLE = "core_storage_identity"
IDENTITY_SCHEMA = (
    "CREATE TABLE core_storage_identity (generation TEXT NOT NULL,role TEXT NOT NULL)"
)
FUNCTION = "edgecitadel_core_storage_generation"
MIRROR_TABLES = frozenset(
    {
        "messages",
        "agents",
        "enrollment_invitations",
        "poison_events",
        "conversation_turns",
    }
)
TRACE_TABLES = frozenset(GUARDED_TABLES)
PREFIX = "core_storage_guard_"
MAX_SCHEMA_OBJECTS = 256


def storage_fences(
    tables: set[str] | frozenset[str], generation: str
) -> dict[str, str]:
    """Canonical version-1 fences for explicit offline provisioning only."""
    if str(UUID(generation)) != generation or not tables <= (
        MIRROR_TABLES | TRACE_TABLES | {IDENTITY_TABLE}
    ):
        raise ValueError("invalid storage fence definition")
    return {
        f"{PREFIX}{table}_{operation.lower()}": (
            f"CREATE TRIGGER {PREFIX}{table}_{operation.lower()} BEFORE {operation} ON {table} "
            f"WHEN {FUNCTION}() IS NOT '{generation}' "
            "BEGIN SELECT RAISE(ABORT,'unsupported_core_storage'); END"
        )
        for table in sorted(tables)
        for operation in ("INSERT", "UPDATE", "DELETE")
    }


def attest_role(
    connection: sqlite3.Connection, manifest: StorageManifest, role: Role
) -> None:
    """Validate the role, generation, ordinary-table inventory and exact fences.

    Trace files may retain the complete original mirror component after an
    offline relocation; its pages and fences remain part of trace accounting.
    SQLite side effects from opening the caller's connection are outside this
    function. This check does not attest physical quotas or restore freshness.
    """
    if role not in ("trace", "mirror"):
        raise ValueError("invalid storage role")
    if not connection.in_transaction:
        raise StorageUnavailable("storage_snapshot_required")
    try:
        objects = connection.execute(
            "SELECT type,name,sql FROM main.sqlite_schema WHERE substr(name,1,7) != 'sqlite_' LIMIT ?",
            (MAX_SCHEMA_OBJECTS + 1,),
        ).fetchall()
        if len(objects) > MAX_SCHEMA_OBJECTS:
            raise StorageUnavailable("storage_schema_unavailable")
        tables = {name: sql for kind, name, sql in objects if kind == "table"}
        allowed = {IDENTITY_TABLE} | (
            TRACE_TABLES if role == "trace" else MIRROR_TABLES
        )
        if role == "trace" and MIRROR_TABLES <= tables.keys():
            allowed |= MIRROR_TABLES
        if tables.keys() != allowed or tables.get(IDENTITY_TABLE) != IDENTITY_SCHEMA:
            raise StorageUnavailable("storage_schema_unavailable")
        if any(kind == "view" for kind, _, _ in objects) or any(
            not sql or not sql.startswith("CREATE TABLE ") for sql in tables.values()
        ):
            raise StorageUnavailable("storage_schema_unavailable")
        identity = connection.execute(
            "SELECT generation,role FROM main.core_storage_identity LIMIT 2"
        ).fetchall()
        if identity != [(manifest.generation, role)]:
            raise StorageUnavailable("storage_identity_mismatch")
        expected = storage_fences(allowed, manifest.generation)
        actual = {
            name: sql
            for kind, name, sql in objects
            if kind == "trigger" and name.startswith(PREFIX)
        }
        if actual != expected:
            raise StorageUnavailable("storage_fence_unavailable")
    except sqlite3.Error:
        raise StorageUnavailable("storage_database_unavailable") from None
