"""Bind explicit projection SQL identifiers to one same-database generation.

Only derived tables/indexes use placeholders. Raw ingestion tables are shared and
never rebound. Resolve a generation after BEGIN so table selection and contents
belong to the same SQLite snapshot.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


class ProjectionTables:
    def __init__(self, connection: sqlite3.Connection, namespace: str):
        if namespace and not re.fullmatch(r"g_[0-9a-f]{32}_", namespace):
            raise ValueError("invalid_projection_namespace")
        self.connection = connection
        self.namespace = namespace
        # Owned by history.at_cursor, alongside its connection-local views.
        self.history_cursor: int | None = None

    @property
    def in_transaction(self) -> bool:
        return self.connection.in_transaction

    def identifier(self, name: str) -> str:
        if not re.fullmatch(r"trace_[a-z_]+", name):
            raise ValueError("invalid_projection_identifier")
        return f'"{self.namespace}{name}"'

    def sql(self, template: str) -> str:
        # Deliberate placeholders, not token rewriting of arbitrary SQL/literals.
        return re.sub(r"\{(trace_[a-z_]+)\}", lambda m: self.identifier(m[1]), template)

    def execute(
        self, template: str, parameters: Sequence[Any] | Mapping[str, Any] = ()
    ) -> sqlite3.Cursor:
        return self.connection.execute(self.sql(template), parameters)

    def executemany(
        self, template: str, parameters: Iterable[Sequence[Any]]
    ) -> sqlite3.Cursor:
        return self.connection.executemany(self.sql(template), parameters)


def select_tables(
    connection: sqlite3.Connection, build_generation: str | None = None
) -> ProjectionTables:
    if not connection.in_transaction:
        raise ValueError("projection_transaction_required")
    if build_generation is None:
        row = connection.execute(
            "SELECT namespace FROM trace_projection_generations WHERE status='active'"
        ).fetchone()
    else:
        row = connection.execute(
            "SELECT namespace FROM trace_projection_generations WHERE generation=? AND status='building'",
            (build_generation,),
        ).fetchone()
    if row is None:
        raise ValueError("projection_generation_unavailable")
    return ProjectionTables(connection, row[0])
