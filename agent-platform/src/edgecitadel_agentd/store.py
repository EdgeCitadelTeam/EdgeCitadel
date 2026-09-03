"""SQLite-backed orchestration, connector-session, and observability state."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from cryptography.fernet import Fernet, InvalidToken
from edgecitadel_plugin_runtime.validator import ValidationError, default_validator

SCHEMA_VERSION = 6
TELEMETRY_RETENTION_MS = 30 * 24 * 60 * 60 * 1000
RETENTION_INTERVAL_MS = 60 * 60 * 1000
MAX_EVENT_RECORDS = 50_000
MAX_SPAN_RECORDS = 50_000
MAX_PRESENCE_RECORDS = 10_000
RETENTION_DELETE_BATCH = 1_000
TERMINAL_STATES = frozenset(
    {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}
)
TASK_STATES = frozenset(
    {
        "created",
        "queued",
        "offered",
        "accepted",
        "running",
        *TERMINAL_STATES,
    }
)
LEGAL_TRANSITIONS: dict[str, frozenset[str]] = {
    "created": frozenset(
        {"queued", "rejected", "cancelled", "expired", "undeliverable"}
    ),
    "queued": frozenset(
        {"offered", "rejected", "cancelled", "expired", "undeliverable"}
    ),
    "offered": frozenset(
        {"accepted", "queued", "rejected", "cancelled", "expired", "undeliverable"}
    ),
    "accepted": frozenset({"running", "rejected", "cancelled", "expired", "failed"}),
    "running": frozenset({"completed", "failed", "rejected", "cancelled", "expired"}),
    "completed": frozenset(),
    "failed": frozenset(),
    "rejected": frozenset(),
    "cancelled": frozenset(),
    "expired": frozenset(),
    "undeliverable": frozenset(),
}
AGENT_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
CONNECTOR_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,127}$")
SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "credential",
        "credentials",
        "env",
        "environment",
        "file_content",
        "password",
        "prompt",
        "response",
        "secret",
        "token",
        "tool_arguments",
        "tool_input",
    }
)


class StoreError(RuntimeError):
    """A caller-visible local state or transition failure."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _require_uuid4(value: str, label: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise StoreError(f"{label} must be a UUIDv4") from error
    if parsed.version != 4 or str(parsed) != value.lower():
        raise StoreError(f"{label} must be a canonical UUIDv4")


def _json_object(value: Mapping[str, object] | None) -> str:
    document = dict(value or {})
    _reject_sensitive(document)
    encoded = json.dumps(document, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode()) > 64 * 1024:
        raise StoreError("metadata exceeds the 64 KiB limit")
    return encoded


def _reject_sensitive(value: object, path: str = "metadata") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in SENSITIVE_KEYS:
                raise StoreError(f"sensitive field is not allowed in {path}: {key}")
            _reject_sensitive(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_sensitive(child, f"{path}[{index}]")


class AgentdStore:
    """The single-writer local state boundary used by the agentd service."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.chmod(0o700)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        self._last_retention_ms = 0
        schema_version = int(
            self._connection.execute("PRAGMA user_version").fetchone()[0]
        )
        payload_key = path.parent / "payload.key"
        if schema_version >= 5 and not payload_key.exists():
            self._connection.close()
            raise StoreError(
                "agentd payload key is missing; restore agentd.sqlite3 and payload.key from the same backup"
            )
        self._content_cipher = self._load_content_cipher(payload_key)
        self._migrate()
        path.chmod(0o600)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _migrate(self) -> None:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._migrate_locked()
            except Exception:
                self._connection.rollback()
                raise
            self._connection.commit()

    def _migrate_locked(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise StoreError(
                f"agentd database schema {version} is newer than supported {SCHEMA_VERSION}"
            )
        if version == 0:
            self._execute_migration_sql(
                """
                    CREATE TABLE connectors (
                        connector_id TEXT PRIMARY KEY,
                        host_type TEXT NOT NULL,
                        agent_id TEXT NOT NULL UNIQUE,
                        token_hash TEXT NOT NULL,
                        capabilities_json TEXT NOT NULL,
                        revoked_at_ms INTEGER,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE sessions (
                        session_id TEXT PRIMARY KEY,
                        connector_id TEXT NOT NULL REFERENCES connectors(connector_id),
                        opened_at_ms INTEGER NOT NULL,
                        lease_expires_at_ms INTEGER NOT NULL,
                        closed_at_ms INTEGER
                    );
                    CREATE INDEX sessions_connector_active
                        ON sessions(connector_id, closed_at_ms, lease_expires_at_ms);
                    CREATE TABLE tasks (
                        task_id TEXT PRIMARY KEY,
                        sender_id TEXT NOT NULL,
                        recipient_id TEXT NOT NULL,
                        skill_id TEXT,
                        state TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        trace_id TEXT NOT NULL,
                        deadline_at_ms INTEGER,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        terminal_reason TEXT
                    );
                    CREATE INDEX tasks_recipient_state
                        ON tasks(recipient_id, state, created_at_ms);
                    CREATE TABLE task_attempts (
                        attempt_id TEXT PRIMARY KEY,
                        task_id TEXT NOT NULL REFERENCES tasks(task_id),
                        session_id TEXT REFERENCES sessions(session_id),
                        state TEXT NOT NULL,
                        evidence_json TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE events (
                        event_id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        agent_id TEXT,
                        task_id TEXT,
                        trace_id TEXT,
                        attributes_json TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL
                    );
                    CREATE INDEX events_trace_time ON events(trace_id, created_at_ms);
                    CREATE TABLE spans (
                        span_id TEXT PRIMARY KEY,
                        trace_id TEXT NOT NULL,
                        parent_span_id TEXT,
                        operation TEXT NOT NULL,
                        status TEXT NOT NULL,
                        agent_id TEXT,
                        task_id TEXT,
                        attributes_json TEXT NOT NULL,
                        started_at_ms INTEGER NOT NULL,
                        ended_at_ms INTEGER
                    );
                    CREATE INDEX spans_trace_time ON spans(trace_id, started_at_ms);
                    CREATE TABLE presence_history (
                        presence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        agent_id TEXT NOT NULL,
                        state TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        observed_at_ms INTEGER NOT NULL
                    );
                    PRAGMA user_version=1;
                    """
            )
            version = 1
        if version == 1:
            self._execute_migration_sql(
                """
                    CREATE TABLE managed_agents (
                        package_id TEXT PRIMARY KEY,
                        desired_state TEXT NOT NULL,
                        record_json TEXT NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    );
                    CREATE TABLE transport_outbox (
                        message_id TEXT PRIMARY KEY,
                        task_id TEXT NOT NULL REFERENCES tasks(task_id),
                        subject TEXT NOT NULL,
                        envelope_json TEXT NOT NULL,
                        published_at_ms INTEGER,
                        created_at_ms INTEGER NOT NULL
                    );
                    CREATE INDEX transport_outbox_pending
                        ON transport_outbox(published_at_ms, created_at_ms);
                    PRAGMA user_version=2;
                    """
            )
            version = 2
        if version == 2:
            self._execute_migration_sql(
                """
                    ALTER TABLE tasks ADD COLUMN claimed_session_id TEXT
                        REFERENCES sessions(session_id);
                    ALTER TABLE connectors ADD COLUMN card_json TEXT;
                    PRAGMA user_version=3;
                    """
            )
            version = 3
        if version == 3:
            self._execute_migration_sql(
                """
                    ALTER TABLE tasks ADD COLUMN result_json TEXT;
                    PRAGMA user_version=4;
                    """
            )
            version = 4
        if version == 4:
            for row in self._connection.execute(
                "SELECT task_id, payload_json, result_json FROM tasks"
            ).fetchall():
                self._connection.execute(
                    "UPDATE tasks SET payload_json = ?, result_json = ? WHERE task_id = ?",
                    (
                        self._encrypt_existing_content(row["payload_json"]),
                        self._encrypt_existing_content(row["result_json"])
                        if row["result_json"] is not None
                        else None,
                        row["task_id"],
                    ),
                )
            for row in self._connection.execute(
                "SELECT message_id, envelope_json FROM transport_outbox"
            ).fetchall():
                self._connection.execute(
                    "UPDATE transport_outbox SET envelope_json = ? WHERE message_id = ?",
                    (
                        self._encrypt_existing_content(row["envelope_json"]),
                        row["message_id"],
                    ),
                )
            self._connection.execute("PRAGMA user_version=5")
            version = 5
        if version == 5:
            self._execute_migration_sql(
                """
                    ALTER TABLE tasks ADD COLUMN context_id TEXT;
                    PRAGMA user_version=6;
                    """
            )

    def _execute_migration_sql(self, source: str) -> None:
        """Execute simple migration statements without executescript's implicit commit."""
        for statement in source.split(";"):
            if normalized := statement.strip():
                self._connection.execute(normalized)

    @staticmethod
    def _load_content_cipher(path: Path) -> Fernet:
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise StoreError("agentd payload key must be a regular file")
            key = path.read_bytes().strip()
        else:
            key = Fernet.generate_key()
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as target:
                target.write(key + b"\n")
        path.chmod(0o600)
        try:
            return Fernet(key)
        except ValueError as error:
            raise StoreError("agentd payload key is invalid") from error

    def _encrypt_existing_content(self, encoded: str) -> str:
        if encoded.startswith("fernet:v1:"):
            return encoded
        return "fernet:v1:" + self._content_cipher.encrypt(encoded.encode()).decode()

    def _encode_content(self, value: Mapping[str, object]) -> str:
        encoded = json.dumps(dict(value), separators=(",", ":"), sort_keys=True)
        if len(encoded.encode()) > 1024 * 1024:
            raise StoreError("task content exceeds the 1 MiB limit")
        return self._encrypt_existing_content(encoded)

    def _decode_content(self, value: str) -> dict[str, object]:
        if not value.startswith("fernet:v1:"):
            raise StoreError("task content is not encrypted")
        try:
            decoded = self._content_cipher.decrypt(
                value.removeprefix("fernet:v1:").encode()
            )
            document = json.loads(decoded)
        except (InvalidToken, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StoreError("task content could not be decrypted") from error
        if not isinstance(document, dict):
            raise StoreError("task content is not an object")
        return cast(dict[str, object], document)

    def register_connector(
        self,
        *,
        connector_id: str,
        host_type: str,
        agent_id: str,
        capabilities: list[str],
        card: Mapping[str, object] | None = None,
    ) -> str:
        if not connector_id or not host_type or not agent_id:
            raise StoreError("connector_id, host_type, and agent_id are required")
        if len(connector_id) > 128 or len(host_type) > 64 or len(agent_id) > 128:
            raise StoreError("connector identity exceeds its size limit")
        if AGENT_ID_PATTERN.fullmatch(agent_id) is None:
            raise StoreError("agent_id is not a valid EdgeCitadel Agent identity")
        if CONNECTOR_ID_PATTERN.fullmatch(connector_id) is None:
            raise StoreError("connector_id is not a valid connector identity")
        if host_type not in {"pi", "claude-code", "codex", "managed-agent"}:
            raise StoreError("host_type is not supported")
        if len(capabilities) > 256 or not all(
            isinstance(item, str) and 0 < len(item) <= 128 for item in capabilities
        ):
            raise StoreError("connector capabilities are invalid")
        if len(set(capabilities)) != len(capabilities):
            raise StoreError("connector capabilities must be unique")
        if card is not None:
            try:
                default_validator().validate_card(dict(card))
            except ValidationError as error:
                raise StoreError("connector Agent Card is invalid") from error
        token = secrets.token_urlsafe(32)
        now = _now_ms()
        capabilities_json = _json_object({"items": capabilities})
        card_json = _json_object(card) if card is not None else None
        with self._lock, self._connection:
            existing = self._connection.execute(
                "SELECT connector_id, revoked_at_ms FROM connectors WHERE connector_id = ?",
                (connector_id,),
            ).fetchone()
            if existing is not None and existing["revoked_at_ms"] is not None:
                raise StoreError("connector is revoked")
            if existing is not None:
                raise StoreError("connector already exists")
            self._connection.execute(
                """
                INSERT INTO connectors (
                    connector_id, host_type, agent_id, token_hash,
                    capabilities_json, card_json, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connector_id) DO UPDATE SET
                    host_type = excluded.host_type,
                    agent_id = excluded.agent_id,
                    token_hash = excluded.token_hash,
                    capabilities_json = excluded.capabilities_json,
                    card_json = excluded.card_json,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    connector_id,
                    host_type,
                    agent_id,
                    _token_hash(token),
                    capabilities_json,
                    card_json,
                    now,
                    now,
                ),
            )
            self._record_event_locked(
                event_type="connector.registered",
                agent_id=agent_id,
                task_id=None,
                trace_id=None,
                attributes={"connector_id": connector_id, "host_type": host_type},
                now=now,
            )
        return token

    def authenticate(self, connector_id: str, token: str) -> sqlite3.Row:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM connectors WHERE connector_id = ?", (connector_id,)
            ).fetchone()
        if (
            row is None
            or row["revoked_at_ms"] is not None
            or not secrets.compare_digest(row["token_hash"], _token_hash(token))
        ):
            raise StoreError("connector authentication failed")
        return cast(sqlite3.Row, row)

    def update_connector(
        self,
        *,
        connector_id: str,
        token: str,
        host_type: str,
        agent_id: str,
        capabilities: list[str],
        card: Mapping[str, object] | None = None,
    ) -> None:
        existing = self.authenticate(connector_id, token)
        if existing["agent_id"] != agent_id or existing["host_type"] != host_type:
            raise StoreError("connector identity cannot be changed in place")
        if len(capabilities) > 256 or not all(
            isinstance(item, str) and 0 < len(item) <= 128 for item in capabilities
        ):
            raise StoreError("connector capabilities are invalid")
        if len(set(capabilities)) != len(capabilities):
            raise StoreError("connector capabilities must be unique")
        existing_capabilities = json.loads(existing["capabilities_json"])["items"]
        if set(capabilities) != set(existing_capabilities):
            raise StoreError(
                "connector capabilities cannot be changed with its own token"
            )
        if card is not None:
            try:
                default_validator().validate_card(dict(card))
            except ValidationError as error:
                raise StoreError("connector Agent Card is invalid") from error
        capabilities_json = _json_object({"items": capabilities})
        card_json = _json_object(card) if card is not None else None
        if (
            existing["capabilities_json"] == capabilities_json
            and existing["card_json"] == card_json
        ):
            return
        with self._lock, self._connection:
            now = _now_ms()
            self._connection.execute(
                """
                UPDATE connectors
                SET capabilities_json = ?, card_json = ?, updated_at_ms = ?
                WHERE connector_id = ?
                """,
                (
                    capabilities_json,
                    card_json,
                    now,
                    connector_id,
                ),
            )
            self._record_event_locked(
                event_type="connector.metadata_updated",
                agent_id=agent_id,
                task_id=None,
                trace_id=None,
                attributes={
                    "connector_id": connector_id,
                    "capability_count": len(capabilities),
                },
                now=now,
            )

    def configure_connector(
        self,
        *,
        connector_id: str,
        host_type: str,
        agent_id: str,
        capabilities: list[str],
    ) -> None:
        """Reconcile capabilities through the management authority only."""
        if len(capabilities) > 256 or not all(
            isinstance(item, str) and 0 < len(item) <= 128 for item in capabilities
        ):
            raise StoreError("connector capabilities are invalid")
        if len(set(capabilities)) != len(capabilities):
            raise StoreError("connector capabilities must be unique")
        with self._lock, self._connection:
            existing = self._connection.execute(
                "SELECT host_type, agent_id, capabilities_json FROM connectors "
                "WHERE connector_id = ?",
                (connector_id,),
            ).fetchone()
            if (
                existing is None
                or existing["host_type"] != host_type
                or existing["agent_id"] != agent_id
            ):
                raise StoreError("connector identity cannot be changed in place")
            capabilities_json = _json_object({"items": capabilities})
            if existing["capabilities_json"] == capabilities_json:
                return
            now = _now_ms()
            self._connection.execute(
                "UPDATE connectors SET capabilities_json = ?, updated_at_ms = ? "
                "WHERE connector_id = ?",
                (capabilities_json, now, connector_id),
            )
            self._record_event_locked(
                event_type="connector.capabilities_reconciled",
                agent_id=agent_id,
                task_id=None,
                trace_id=None,
                attributes={
                    "connector_id": connector_id,
                    "capability_count": len(capabilities),
                },
                now=now,
            )

    def list_connectors(self) -> list[dict[str, object]]:
        now = _now_ms()
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT connector_id, host_type, agent_id, capabilities_json,
                       card_json, revoked_at_ms, created_at_ms, updated_at_ms,
                       EXISTS (
                           SELECT 1 FROM sessions s
                           WHERE s.connector_id = connectors.connector_id
                             AND s.closed_at_ms IS NULL
                             AND s.lease_expires_at_ms > ?
                       ) AS session_active
                FROM connectors ORDER BY connector_id
                """,
                (now,),
            ).fetchall()
        return [
            {
                "connector_id": row["connector_id"],
                "host_type": row["host_type"],
                "agent_id": row["agent_id"],
                "capabilities": json.loads(row["capabilities_json"])["items"],
                "card": json.loads(row["card_json"]) if row["card_json"] else None,
                "revoked": row["revoked_at_ms"] is not None,
                "session_active": bool(row["session_active"]),
                "created_at_ms": row["created_at_ms"],
                "updated_at_ms": row["updated_at_ms"],
            }
            for row in rows
        ]

    def list_active_connectors(self) -> list[dict[str, object]]:
        now = _now_ms()
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT DISTINCT c.connector_id, c.host_type, c.agent_id,
                                c.capabilities_json, c.card_json
                FROM connectors c JOIN sessions s USING(connector_id)
                WHERE c.revoked_at_ms IS NULL AND s.closed_at_ms IS NULL
                  AND s.lease_expires_at_ms > ?
                ORDER BY c.connector_id
                """,
                (now,),
            ).fetchall()
        return [
            {
                "connector_id": row["connector_id"],
                "host_type": row["host_type"],
                "agent_id": row["agent_id"],
                "capabilities": json.loads(row["capabilities_json"])["items"],
                "card": json.loads(row["card_json"]) if row["card_json"] else None,
            }
            for row in rows
        ]

    def list_agents(self) -> list[dict[str, object]]:
        """Return local identities plus the latest NATS-observed presence."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT p.agent_id, p.state, p.reason, p.observed_at_ms
                FROM presence_history p
                JOIN (
                    SELECT agent_id, MAX(presence_id) AS latest_id
                    FROM presence_history GROUP BY agent_id
                ) latest ON latest.latest_id = p.presence_id
                ORDER BY p.agent_id
                """
            ).fetchall()
        agents: dict[str, dict[str, object]] = {
            str(row["agent_id"]): {
                "agent_id": row["agent_id"],
                "state": row["state"],
                "reason": row["reason"],
                "observed_at_ms": row["observed_at_ms"],
                "source": "nats-observed",
                "local": False,
                "capabilities": [],
            }
            for row in rows
        }
        for connector in self.list_connectors():
            agent_id = str(connector["agent_id"])
            observed = agents.get(agent_id, {})
            revoked = bool(connector["revoked"])
            session_active = bool(connector["session_active"])
            state = "unavailable" if revoked else observed.get("state", "unavailable")
            reason = (
                "connector_revoked"
                if revoked
                else observed.get("reason", "no_active_session")
            )
            if session_active:
                state = "online"
                reason = "active_session"
            agents[agent_id] = {
                **observed,
                "agent_id": agent_id,
                "state": state,
                "reason": reason,
                "source": "local-connector",
                "local": True,
                "host_type": connector["host_type"],
                "capabilities": connector["capabilities"],
            }
        return [agents[agent_id] for agent_id in sorted(agents)]

    def revoke_connector(self, connector_id: str) -> None:
        now = _now_ms()
        with self._lock, self._connection:
            sessions = self._connection.execute(
                """
                SELECT session_id FROM sessions
                WHERE connector_id = ? AND closed_at_ms IS NULL
                """,
                (connector_id,),
            ).fetchall()
            changed = self._connection.execute(
                "UPDATE connectors SET revoked_at_ms = ?, updated_at_ms = ? "
                "WHERE connector_id = ? AND revoked_at_ms IS NULL",
                (now, now, connector_id),
            ).rowcount
            if not changed:
                raise StoreError("connector does not exist or is already revoked")
            self._connection.execute(
                "UPDATE sessions SET closed_at_ms = ? "
                "WHERE connector_id = ? AND closed_at_ms IS NULL",
                (now, connector_id),
            )
            for session in sessions:
                self._recover_session_tasks_locked(str(session["session_id"]), now)
            agent_id = self._connection.execute(
                "SELECT agent_id FROM connectors WHERE connector_id = ?",
                (connector_id,),
            ).fetchone()["agent_id"]
            self._record_event_locked(
                event_type="connector.revoked",
                agent_id=str(agent_id),
                task_id=None,
                trace_id=None,
                attributes={"connector_id": connector_id},
                now=now,
            )

    def reissue_managed_connector(self, connector_id: str, agent_id: str) -> str:
        """Issue a new token only for an explicitly restarted Managed Agent."""
        now = _now_ms()
        token = secrets.token_urlsafe(32)
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT host_type, agent_id FROM connectors WHERE connector_id = ?",
                (connector_id,),
            ).fetchone()
            if (
                row is None
                or row["host_type"] != "managed-agent"
                or row["agent_id"] != agent_id
            ):
                raise StoreError("Managed Agent connector cannot be reissued")
            if self._has_active_session_locked(connector_id, now):
                raise StoreError("active Managed Agent connector cannot be reissued")
            self._connection.execute(
                """
                UPDATE connectors SET token_hash = ?, revoked_at_ms = NULL,
                    updated_at_ms = ? WHERE connector_id = ?
                """,
                (_token_hash(token), now, connector_id),
            )
            self._record_event_locked(
                event_type="connector.credential_reissued",
                agent_id=agent_id,
                task_id=None,
                trace_id=None,
                attributes={"connector_id": connector_id},
                now=now,
            )
        return token

    def reconcile_managed_agents(
        self, records: list[Mapping[str, object]]
    ) -> list[dict[str, object]]:
        normalized: list[tuple[str, str, str, int]] = []
        now = _now_ms()
        for record in records:
            package_id = record.get("package_id")
            desired_state = record.get("desired_state")
            if not isinstance(package_id, str) or not package_id:
                raise StoreError("Agent Package package_id is required")
            if desired_state not in {"running", "stopped"}:
                raise StoreError(
                    "Managed Agent desired_state must be running or stopped"
                )
            encoded = json.dumps(dict(record), separators=(",", ":"), sort_keys=True)
            if len(encoded.encode()) > 64 * 1024:
                raise StoreError("Managed Agent record exceeds the 64 KiB limit")
            normalized.append((package_id, str(desired_state), encoded, now))
        with self._lock, self._connection:
            previous = {
                str(row["package_id"]): str(row["desired_state"])
                for row in self._connection.execute(
                    "SELECT package_id, desired_state FROM managed_agents"
                ).fetchall()
            }
            retained = {record[0] for record in normalized}
            removed = sorted(previous.keys() - retained)
            if retained:
                placeholders = ",".join("?" for _ in retained)
                self._connection.execute(
                    f"DELETE FROM managed_agents WHERE package_id NOT IN ({placeholders})",
                    tuple(sorted(retained)),
                )
            else:
                self._connection.execute("DELETE FROM managed_agents")
            self._connection.executemany(
                """
                INSERT INTO managed_agents (
                    package_id, desired_state, record_json, updated_at_ms
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(package_id) DO UPDATE SET
                    desired_state = excluded.desired_state,
                    record_json = excluded.record_json,
                    updated_at_ms = excluded.updated_at_ms
                """,
                normalized,
            )
            for package_id in removed:
                self._record_event_locked(
                    event_type="managed_agent.removed",
                    agent_id=None,
                    task_id=None,
                    trace_id=None,
                    attributes={"package_id": package_id},
                    now=now,
                )
            for package_id, desired_state, _encoded, _updated_at in normalized:
                prior_state = previous.get(package_id)
                if prior_state == desired_state:
                    continue
                event_type = (
                    "managed_agent.installed"
                    if prior_state is None
                    else "managed_agent.desired_state_changed"
                )
                self._record_event_locked(
                    event_type=event_type,
                    agent_id=None,
                    task_id=None,
                    trace_id=None,
                    attributes={
                        "package_id": package_id,
                        "desired_state": desired_state,
                    },
                    now=now,
                )
        return self.list_managed_agents()

    def list_managed_agents(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT record_json FROM managed_agents ORDER BY package_id"
            ).fetchall()
        return [cast(dict[str, object], json.loads(row["record_json"])) for row in rows]

    def open_session(
        self, *, connector_id: str, token: str, lease_seconds: int = 45
    ) -> dict[str, object]:
        connector = self.authenticate(connector_id, token)
        if not 10 <= lease_seconds <= 300:
            raise StoreError("lease_seconds must be between 10 and 300")
        now = _now_ms()
        session_id = str(uuid.uuid4())
        expires = now + lease_seconds * 1000
        with self._lock, self._connection:
            active = self._connection.execute(
                """
                SELECT 1 FROM sessions
                WHERE connector_id = ? AND closed_at_ms IS NULL
                  AND lease_expires_at_ms > ?
                """,
                (connector_id, now),
            ).fetchone()
            if active is None:
                self._record_presence_locked(
                    connector["agent_id"], "online", "native_session_opened", now
                )
            self._connection.execute(
                """
                INSERT INTO sessions (
                    session_id, connector_id, opened_at_ms, lease_expires_at_ms
                ) VALUES (?, ?, ?, ?)
                """,
                (session_id, connector_id, now, expires),
            )
        return {"session_id": session_id, "lease_expires_at_ms": expires}

    def renew_session(
        self,
        *,
        connector_id: str,
        token: str,
        session_id: str,
        lease_seconds: int = 45,
    ) -> int:
        self.authenticate(connector_id, token)
        if not 10 <= lease_seconds <= 300:
            raise StoreError("lease_seconds must be between 10 and 300")
        expires = _now_ms() + lease_seconds * 1000
        with self._lock, self._connection:
            changed = self._connection.execute(
                """
                UPDATE sessions SET lease_expires_at_ms = ?
                WHERE session_id = ? AND connector_id = ? AND closed_at_ms IS NULL
                """,
                (expires, session_id, connector_id),
            ).rowcount
        if not changed:
            raise StoreError("active session was not found")
        return expires

    def close_session(self, *, connector_id: str, token: str, session_id: str) -> None:
        connector = self.authenticate(connector_id, token)
        now = _now_ms()
        with self._lock, self._connection:
            changed = self._connection.execute(
                """
                UPDATE sessions SET closed_at_ms = ?
                WHERE session_id = ? AND connector_id = ? AND closed_at_ms IS NULL
                """,
                (now, session_id, connector_id),
            ).rowcount
            if not changed:
                raise StoreError("active session was not found")
            self._recover_session_tasks_locked(session_id, now)
            if not self._has_active_session_locked(connector_id, now):
                self._record_presence_locked(
                    connector["agent_id"], "unavailable", "native_session_closed", now
                )

    def create_task(
        self,
        *,
        sender_id: str,
        recipient_id: str,
        payload: Mapping[str, object],
        skill_id: str | None = None,
        deadline_at_ms: int | None = None,
        task_id: str | None = None,
        trace_id: str | None = None,
        context_id: str | None = None,
        queue_transport: bool = True,
    ) -> dict[str, object]:
        if not sender_id or not recipient_id:
            raise StoreError("sender_id and recipient_id are required")
        if (
            AGENT_ID_PATTERN.fullmatch(sender_id) is None
            or AGENT_ID_PATTERN.fullmatch(recipient_id) is None
        ):
            raise StoreError(
                "sender_id and recipient_id must be valid Agent identities"
            )
        if skill_id is not None and (
            not isinstance(skill_id, str) or not 0 < len(skill_id) <= 128
        ):
            raise StoreError(
                "skill_id must be a non-empty string of at most 128 characters"
            )
        now = _now_ms()
        if deadline_at_ms is not None and type(deadline_at_ms) is not int:
            raise StoreError("task deadline must be an integer timestamp")
        if deadline_at_ms is not None and deadline_at_ms <= now:
            raise StoreError("task deadline must be in the future")
        if task_id is not None and not isinstance(task_id, str):
            raise StoreError("task_id must be a UUIDv4")
        if trace_id is not None and not isinstance(trace_id, str):
            raise StoreError("trace_id must be a string")
        if context_id is not None and not isinstance(context_id, str):
            raise StoreError("context_id must be a UUIDv4")
        task_id = task_id or str(uuid.uuid4())
        trace_id = trace_id or uuid.uuid4().hex
        _require_uuid4(task_id, "task_id")
        if context_id is not None:
            _require_uuid4(context_id, "context_id")
        if not re.fullmatch(r"[0-9a-f]{32}", trace_id):
            raise StoreError(
                "trace_id must contain exactly 32 lowercase hex characters"
            )
        with self._lock, self._connection:
            try:
                self._connection.execute(
                    """
                    INSERT INTO tasks (
                        task_id, sender_id, recipient_id, skill_id, state,
                        payload_json, trace_id, context_id, deadline_at_ms,
                        created_at_ms, updated_at_ms
                    ) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        sender_id,
                        recipient_id,
                        skill_id,
                        self._encode_content(payload),
                        trace_id,
                        context_id,
                        deadline_at_ms,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise StoreError("task_id already exists") from error
            self._record_event_locked(
                event_type="task.queued",
                agent_id=sender_id,
                task_id=task_id,
                trace_id=trace_id,
                attributes={"recipient_id": recipient_id, "skill_id": skill_id},
                now=now,
            )
            if queue_transport and not self._agent_is_local_locked(recipient_id):
                message_id = str(uuid.uuid4())
                transport_payload = dict(payload)
                if skill_id:
                    transport_payload["skill_id"] = skill_id
                transport_payload["trace_id"] = trace_id
                if deadline_at_ms is not None:
                    transport_payload["deadline_at_ms"] = deadline_at_ms
                envelope: dict[str, object] = {
                    "v": 1,
                    "id": message_id,
                    "type": "command",
                    "sender_id": sender_id,
                    "recipient_id": recipient_id,
                    "task_id": task_id,
                    "timestamp": self._iso_timestamp(now),
                    "payload": transport_payload,
                }
                if context_id is not None:
                    envelope["context_id"] = context_id
                self._queue_transport_locked(
                    message_id=message_id,
                    task_id=task_id,
                    subject=f"agents.{recipient_id}.inbox",
                    envelope=envelope,
                    now=now,
                )
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict[str, object]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise StoreError("task was not found")
        return self._task_mapping(row)

    def get_task_for(self, task_id: str, actor_id: str) -> dict[str, object]:
        task = self.get_task(task_id)
        if actor_id not in {task["sender_id"], task["recipient_id"]}:
            raise StoreError("task is outside the connector scope")
        return task

    def list_tasks(
        self,
        *,
        actor_id: str | None = None,
        recipient_id: str | None = None,
        include_terminal: bool = True,
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        arguments: list[object] = []
        if actor_id:
            clauses.append("(sender_id = ? OR recipient_id = ?)")
            arguments.extend((actor_id, actor_id))
        if recipient_id:
            if actor_id and recipient_id != actor_id:
                raise StoreError("recipient_id is outside the connector scope")
            clauses.append("recipient_id = ?")
            arguments.append(recipient_id)
        if not include_terminal:
            placeholders = ",".join("?" for _ in TERMINAL_STATES)
            clauses.append(f"state NOT IN ({placeholders})")
            arguments.extend(sorted(TERMINAL_STATES))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM tasks" + where + " ORDER BY created_at_ms", arguments
            ).fetchall()
        return [self._task_mapping(row) for row in rows]

    def transition_task(
        self,
        *,
        task_id: str,
        state: str,
        actor_id: str,
        reason: str | None = None,
        session_id: str | None = None,
        evidence: Mapping[str, object] | None = None,
        result: Mapping[str, object] | None = None,
        queue_transport: bool = True,
    ) -> dict[str, object]:
        if state not in TASK_STATES:
            raise StoreError("unsupported task state")
        now = _now_ms()
        if state in TERMINAL_STATES and result is not None:
            conflict: tuple[str | None, str | None] | None = None
            with self._lock:
                existing = self._connection.execute(
                    "SELECT state, result_json, recipient_id, trace_id FROM tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                if existing is not None and existing["state"] == state:
                    stored_result = (
                        self._decode_content(existing["result_json"])
                        if existing["result_json"]
                        else None
                    )
                    if stored_result != dict(result):
                        with self._connection:
                            self._record_event_locked(
                                event_type="task.result_conflict",
                                agent_id=actor_id,
                                task_id=task_id,
                                trace_id=str(existing["trace_id"]),
                                attributes={"state": state},
                                now=now,
                            )
                        conflict = (
                            str(existing["recipient_id"]),
                            str(existing["trace_id"]),
                        )
            if conflict is not None:
                raise StoreError("conflicting terminal task result was rejected")
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise StoreError("task was not found")
            current = str(row["state"])
            if state in {"accepted", "running", "completed", "failed", "rejected"}:
                if actor_id != row["recipient_id"]:
                    raise StoreError("only the recipient may execute this transition")
            if state == "cancelled" and actor_id not in {
                row["sender_id"],
                row["recipient_id"],
            }:
                raise StoreError("only the sender or recipient may cancel a task")
            if state in {"offered", "expired", "undeliverable"} and actor_id != (
                "edgecitadel-system"
            ):
                raise StoreError("only the task service may execute this transition")
            if current == state:
                if (
                    state in {"accepted", "running"}
                    and row["claimed_session_id"] != session_id
                ):
                    raise StoreError("task is claimed by another session")
                return self._task_mapping(row)
            if state not in LEGAL_TRANSITIONS[current]:
                raise StoreError(f"illegal task transition: {current} -> {state}")
            if state in {"accepted", "running", "completed", "failed", "rejected"}:
                if self._agent_is_local_locked(actor_id):
                    if session_id is None:
                        raise StoreError("an active recipient session is required")
                    active_session = self._connection.execute(
                        """
                        SELECT 1 FROM sessions s JOIN connectors c USING(connector_id)
                        WHERE s.session_id = ? AND c.agent_id = ?
                          AND s.closed_at_ms IS NULL AND s.lease_expires_at_ms > ?
                        """,
                        (session_id, actor_id, now),
                    ).fetchone()
                    if active_session is None:
                        raise StoreError("an active recipient session is required")
                    claimed_session_id = row["claimed_session_id"]
                    if state == "accepted" and claimed_session_id is None:
                        self._connection.execute(
                            "UPDATE tasks SET claimed_session_id = ? WHERE task_id = ?",
                            (session_id, task_id),
                        )
                    elif claimed_session_id != session_id:
                        raise StoreError("task is claimed by another session")
            result_json = self._encode_content(result) if result is not None else None
            self._connection.execute(
                """
                UPDATE tasks SET state = ?, updated_at_ms = ?, terminal_reason = ?,
                    result_json = COALESCE(?, result_json)
                WHERE task_id = ?
                """,
                (
                    state,
                    now,
                    reason if state in TERMINAL_STATES else None,
                    result_json,
                    task_id,
                ),
            )
            attempt_id = str(uuid.uuid4())
            self._connection.execute(
                """
                INSERT INTO task_attempts (
                    attempt_id, task_id, session_id, state, evidence_json,
                    created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    task_id,
                    session_id,
                    state,
                    _json_object(evidence),
                    now,
                    now,
                ),
            )
            self._record_event_locked(
                event_type=f"task.{state}",
                agent_id=actor_id,
                task_id=task_id,
                trace_id=row["trace_id"],
                attributes={"reason": reason} if reason else {},
                now=now,
            )
            if state in TERMINAL_STATES and queue_transport:
                message_id = str(uuid.uuid4())
                recipient_id = str(row["sender_id"])
                message_type = "result"
                wire_state = {
                    "cancelled": "canceled",
                    "expired": "failed",
                    "undeliverable": "failed",
                }.get(state, state)
                if state == "cancelled" and actor_id == row["sender_id"]:
                    recipient_id = str(row["recipient_id"])
                    message_type = "cancel"
                if self._agent_is_local_locked(recipient_id):
                    return self._task_mapping(
                        self._connection.execute(
                            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                        ).fetchone()
                    )
                envelope: dict[str, object] = {
                    "v": 1,
                    "id": message_id,
                    "type": message_type,
                    "sender_id": actor_id,
                    "recipient_id": recipient_id,
                    "task_id": task_id,
                    "timestamp": self._iso_timestamp(now),
                    "payload": dict(result or ({"reason": reason} if reason else {})),
                }
                if message_type == "result":
                    envelope["task_state"] = wire_state
                self._queue_transport_locked(
                    message_id=message_id,
                    task_id=task_id,
                    subject=f"agents.{recipient_id}.inbox",
                    envelope=envelope,
                    now=now,
                )
        return self.get_task(task_id)

    def claim_next_task(
        self, *, connector_id: str, token: str, session_id: str
    ) -> dict[str, object] | None:
        connector = self.authenticate(connector_id, token)
        now = _now_ms()
        with self._lock, self._connection:
            active = self._connection.execute(
                """
                SELECT 1 FROM sessions
                WHERE session_id = ? AND connector_id = ?
                  AND closed_at_ms IS NULL AND lease_expires_at_ms > ?
                """,
                (session_id, connector_id, now),
            ).fetchone()
            if active is None:
                raise StoreError("an active recipient session is required")
            row = self._connection.execute(
                """
                SELECT task_id, state FROM tasks
                WHERE recipient_id = ? AND state IN ('queued', 'offered')
                  AND claimed_session_id IS NULL
                ORDER BY created_at_ms LIMIT 1
                """,
                (connector["agent_id"],),
            ).fetchone()
            if row is None:
                return None
            task_id = str(row["task_id"])
            if row["state"] == "queued":
                self.transition_task(
                    task_id=task_id,
                    state="offered",
                    actor_id="edgecitadel-system",
                    queue_transport=False,
                )
            return self.transition_task(
                task_id=task_id,
                state="accepted",
                actor_id=str(connector["agent_id"]),
                session_id=session_id,
                queue_transport=False,
            )

    def ingest_transport_envelope(
        self, envelope: Mapping[str, object]
    ) -> dict[str, object]:
        """Persist a validated NATS command/result before acknowledging delivery."""
        try:
            default_validator().validate_envelope(dict(envelope))
        except ValidationError as error:
            raise StoreError("transport envelope is invalid") from error
        message_type = envelope.get("type")
        task_id = envelope.get("task_id")
        if not isinstance(task_id, str) or not task_id:
            raise StoreError("transport envelope task_id is required")
        if message_type == "result":
            requested_state = envelope.get("task_state")
            state = {
                "completed": "completed",
                "failed": "failed",
                "rejected": "rejected",
                "canceled": "cancelled",
            }.get(str(requested_state), "failed")
            try:
                current = self.get_task(task_id)
                actor_id = str(envelope.get("sender_id", ""))
                payload = envelope.get("payload")
                if (
                    actor_id == "edgecitadel-system"
                    and requested_state == "failed"
                    and isinstance(payload, Mapping)
                    and payload.get("error") == "recipient_unavailable"
                    and payload.get("trigger") == "max_deliveries"
                    and payload.get("recipient_id") == current["recipient_id"]
                    and envelope.get("recipient_id") == current["sender_id"]
                ):
                    return self.transition_task(
                        task_id=task_id,
                        state="undeliverable",
                        actor_id="edgecitadel-system",
                        reason="recipient_unavailable",
                        result=payload,
                        evidence={"transport": "nats", "trigger": "max_deliveries"},
                        queue_transport=False,
                    )
                if current["state"] == "queued":
                    current = self.transition_task(
                        task_id=task_id,
                        state="offered",
                        actor_id="edgecitadel-system",
                        evidence={"transport": "nats", "compatibility": "v1-result"},
                        queue_transport=False,
                    )
                if current["state"] == "offered" and state not in {
                    "rejected",
                    "cancelled",
                }:
                    current = self.transition_task(
                        task_id=task_id,
                        state="accepted",
                        actor_id=actor_id,
                        evidence={"transport": "nats", "compatibility": "v1-result"},
                        queue_transport=False,
                    )
                if current["state"] == "accepted" and state == "completed":
                    self.transition_task(
                        task_id=task_id,
                        state="running",
                        actor_id=actor_id,
                        evidence={"transport": "nats", "compatibility": "v1-result"},
                        queue_transport=False,
                    )
                return self.transition_task(
                    task_id=task_id,
                    state=state,
                    actor_id=actor_id,
                    reason="remote_result",
                    evidence={"transport": "nats"},
                    queue_transport=False,
                )
            except StoreError as error:
                if "illegal task transition" in str(error):
                    current = self.get_task(task_id)
                    if current["state"] in TERMINAL_STATES:
                        self.append_event(
                            event_type="task.result_conflict",
                            agent_id=str(envelope.get("sender_id", "")),
                            task_id=task_id,
                            trace_id=str(current["trace_id"]),
                            attributes={
                                "existing_state": current["state"],
                                "incoming_state": state,
                            },
                        )
                        raise StoreError(
                            "conflicting terminal task result was rejected"
                        ) from error
                raise
        if message_type == "cancel":
            return self.transition_task(
                task_id=task_id,
                state="cancelled",
                actor_id=str(envelope.get("sender_id", "")),
                reason="remote_cancel",
                evidence={"transport": "nats"},
                queue_transport=False,
            )
        if message_type not in {"command", "delegation"}:
            raise StoreError("unsupported transport envelope type")
        payload = envelope.get("payload", {})
        if not isinstance(payload, Mapping):
            raise StoreError("transport envelope payload must be an object")
        existing: dict[str, object] | None = None
        try:
            existing = self.get_task(task_id)
        except StoreError:
            pass
        if existing is not None:
            expected_skill = (
                str(payload["skill_id"])
                if isinstance(payload, Mapping) and payload.get("skill_id")
                else None
            )
            expected_deadline = (
                int(payload["deadline_at_ms"])
                if isinstance(payload, Mapping)
                and payload.get("deadline_at_ms") is not None
                else None
            )
            conflicts = [
                name
                for name, actual, expected in (
                    ("sender_id", existing["sender_id"], envelope.get("sender_id")),
                    (
                        "recipient_id",
                        existing["recipient_id"],
                        envelope.get("recipient_id"),
                    ),
                    ("skill_id", existing["skill_id"], expected_skill),
                    ("deadline_at_ms", existing["deadline_at_ms"], expected_deadline),
                    ("context_id", existing["context_id"], envelope.get("context_id")),
                    ("payload", existing["payload"], dict(payload)),
                )
                if actual != expected
            ]
            if conflicts:
                self.append_event(
                    event_type="task.duplicate_conflict",
                    agent_id="edgecitadel-system",
                    task_id=task_id,
                    trace_id=str(existing["trace_id"]),
                    attributes={"conflicting_fields": conflicts},
                )
                raise StoreError("conflicting duplicate task envelope was rejected")
            return existing
        deadline = payload.get("deadline_at_ms")
        task = self.create_task(
            task_id=task_id,
            trace_id=str(payload.get("trace_id") or uuid.uuid4().hex),
            context_id=(
                str(envelope["context_id"])
                if envelope.get("context_id") is not None
                else None
            ),
            sender_id=str(envelope.get("sender_id", "")),
            recipient_id=str(envelope.get("recipient_id", "")),
            skill_id=(str(payload["skill_id"]) if payload.get("skill_id") else None),
            payload=payload,
            deadline_at_ms=(int(deadline) if deadline is not None else None),
            queue_transport=False,
        )
        return self.transition_task(
            task_id=str(task["task_id"]),
            state="offered",
            actor_id="edgecitadel-system",
            evidence={"transport": "nats"},
        )

    def pending_transport(self, limit: int = 100) -> list[dict[str, object]]:
        if not 1 <= limit <= 1000:
            raise StoreError("transport limit must be between 1 and 1000")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT message_id, task_id, subject, envelope_json, created_at_ms
                FROM transport_outbox WHERE published_at_ms IS NULL
                ORDER BY created_at_ms LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "message_id": row["message_id"],
                "task_id": row["task_id"],
                "subject": row["subject"],
                "envelope": self._decode_content(row["envelope_json"]),
                "created_at_ms": row["created_at_ms"],
            }
            for row in rows
        ]

    def mark_transport_published(self, message_id: str) -> None:
        with self._lock, self._connection:
            changed = self._connection.execute(
                """
                UPDATE transport_outbox SET published_at_ms = ?
                WHERE message_id = ? AND published_at_ms IS NULL
                """,
                (_now_ms(), message_id),
            ).rowcount
        if not changed:
            raise StoreError("pending transport message was not found")

    def append_event(
        self,
        *,
        event_type: str,
        agent_id: str | None,
        task_id: str | None,
        trace_id: str | None,
        attributes: Mapping[str, object] | None,
    ) -> str:
        now = _now_ms()
        with self._lock, self._connection:
            return self._record_event_locked(
                event_type=event_type,
                agent_id=agent_id,
                task_id=task_id,
                trace_id=trace_id,
                attributes=attributes,
                now=now,
            )

    def observe_presence(self, *, agent_id: str, state: str, reason: str) -> None:
        if AGENT_ID_PATTERN.fullmatch(agent_id) is None:
            raise StoreError("presence agent_id is invalid")
        if state not in {"online", "unavailable", "degraded"}:
            raise StoreError("presence state is invalid")
        now = _now_ms()
        with self._lock, self._connection:
            latest = self._connection.execute(
                """
                SELECT state FROM presence_history
                WHERE agent_id = ? ORDER BY observed_at_ms DESC, presence_id DESC LIMIT 1
                """,
                (agent_id,),
            ).fetchone()
            if latest is None or latest["state"] != state:
                self._record_presence_locked(agent_id, state, reason, now)

    def record_span(
        self,
        *,
        trace_id: str,
        operation: str,
        status: str,
        agent_id: str | None = None,
        task_id: str | None = None,
        parent_span_id: str | None = None,
        span_id: str | None = None,
        started_at_ms: int | None = None,
        ended_at_ms: int | None = None,
        attributes: Mapping[str, object] | None = None,
    ) -> str:
        if not trace_id or not operation:
            raise StoreError("trace_id and operation are required")
        span_id = span_id or uuid.uuid4().hex[:16]
        started_at_ms = started_at_ms or _now_ms()
        with self._lock, self._connection:
            try:
                self._connection.execute(
                    """
                    INSERT INTO spans (
                        span_id, trace_id, parent_span_id, operation, status,
                        agent_id, task_id, attributes_json, started_at_ms, ended_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        span_id,
                        trace_id,
                        parent_span_id,
                        operation,
                        status,
                        agent_id,
                        task_id,
                        _json_object(attributes),
                        started_at_ms,
                        ended_at_ms,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise StoreError("span_id already exists") from error
        return span_id

    def list_traces(
        self, limit: int = 100, *, agent_id: str | None = None
    ) -> list[dict[str, object]]:
        if not 1 <= limit <= 1000:
            raise StoreError("trace limit must be between 1 and 1000")
        with self._lock:
            where = "WHERE agent_id = ?" if agent_id else ""
            arguments: tuple[object, ...] = (agent_id, limit) if agent_id else (limit,)
            rows = self._connection.execute(
                f"""
                SELECT trace_id, MIN(started_at_ms) AS started_at_ms,
                       MAX(COALESCE(ended_at_ms, started_at_ms)) AS ended_at_ms,
                       COUNT(*) AS span_count
                FROM spans {where} GROUP BY trace_id
                ORDER BY started_at_ms DESC LIMIT ?
                """,
                arguments,
            ).fetchall()
        return [dict(row) for row in rows]

    def get_trace(
        self, trace_id: str, *, agent_id: str | None = None
    ) -> dict[str, object]:
        with self._lock:
            scope = " AND agent_id = ?" if agent_id else ""
            arguments: tuple[object, ...] = (
                (trace_id, agent_id) if agent_id else (trace_id,)
            )
            spans = self._connection.execute(
                "SELECT * FROM spans WHERE trace_id = ?"
                + scope
                + " ORDER BY started_at_ms, span_id",
                arguments,
            ).fetchall()
            events = self._connection.execute(
                "SELECT * FROM events WHERE trace_id = ?"
                + scope
                + " ORDER BY created_at_ms, event_id",
                arguments,
            ).fetchall()
        if not spans and not events:
            raise StoreError("trace was not found")
        return {
            "trace_id": trace_id,
            "spans": [self._span_mapping(row) for row in spans],
            "events": [self._event_mapping(row) for row in events],
        }

    def purge_telemetry(
        self, *, before_ms: int | None = None, agent_id: str | None = None
    ) -> dict[str, int]:
        cutoff = before_ms if before_ms is not None else _now_ms() + 1
        with self._lock, self._connection:
            scope = " AND agent_id = ?" if agent_id else ""
            arguments: tuple[object, ...] = (
                (cutoff, agent_id) if agent_id else (cutoff,)
            )
            spans = self._connection.execute(
                "DELETE FROM spans WHERE started_at_ms < ?" + scope, arguments
            ).rowcount
            events = self._connection.execute(
                "DELETE FROM events WHERE created_at_ms < ?" + scope, arguments
            ).rowcount
            presence = (
                0
                if agent_id
                else self._connection.execute(
                    "DELETE FROM presence_history WHERE observed_at_ms < ?", (cutoff,)
                ).rowcount
            )
            self._record_event_locked(
                event_type="trace.purged",
                agent_id=agent_id,
                task_id=None,
                trace_id=None,
                attributes={
                    "before_ms": cutoff,
                    "spans_deleted": spans,
                    "events_deleted": events,
                    "presence_deleted": presence,
                },
                now=_now_ms(),
            )
        return {"spans": spans, "events": events, "presence": presence}

    def reconcile(self, now_ms: int | None = None) -> dict[str, int]:
        now = now_ms if now_ms is not None else _now_ms()
        expired_sessions = 0
        expired_tasks = 0
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT s.session_id, s.connector_id, c.agent_id
                FROM sessions s JOIN connectors c USING(connector_id)
                WHERE s.closed_at_ms IS NULL AND s.lease_expires_at_ms <= ?
                """,
                (now,),
            ).fetchall()
            for row in rows:
                self._connection.execute(
                    "UPDATE sessions SET closed_at_ms = ? WHERE session_id = ?",
                    (now, row["session_id"]),
                )
                self._recover_session_tasks_locked(str(row["session_id"]), now)
                expired_sessions += 1
                if not self._has_active_session_locked(row["connector_id"], now):
                    self._record_presence_locked(
                        row["agent_id"], "unavailable", "session_lease_expired", now
                    )
            tasks = self._connection.execute(
                """
                SELECT task_id, trace_id, sender_id FROM tasks
                WHERE deadline_at_ms IS NOT NULL AND deadline_at_ms <= ?
                  AND state NOT IN (
                      'completed', 'failed', 'rejected', 'cancelled',
                      'expired', 'undeliverable'
                  )
                """,
                (now,),
            ).fetchall()
            for row in tasks:
                self._connection.execute(
                    """
                    UPDATE tasks SET state = 'expired', updated_at_ms = ?,
                                     terminal_reason = 'deadline_exceeded'
                    WHERE task_id = ?
                    """,
                    (now, row["task_id"]),
                )
                self._record_event_locked(
                    event_type="task.expired",
                    agent_id="edgecitadel-system",
                    task_id=row["task_id"],
                    trace_id=row["trace_id"],
                    attributes={"reason": "deadline_exceeded"},
                    now=now,
                )
                if not self._agent_is_local_locked(str(row["sender_id"])):
                    message_id = str(uuid.uuid4())
                    self._queue_transport_locked(
                        message_id=message_id,
                        task_id=str(row["task_id"]),
                        subject=f"agents.{row['sender_id']}.inbox",
                        envelope={
                            "v": 1,
                            "id": message_id,
                            "type": "result",
                            "sender_id": "edgecitadel-system",
                            "recipient_id": row["sender_id"],
                            "task_id": row["task_id"],
                            "task_state": "failed",
                            "timestamp": self._iso_timestamp(now),
                            "payload": {
                                "error": "deadline_exceeded",
                                "terminal_state": "expired",
                            },
                        },
                        now=now,
                    )
                expired_tasks += 1
            if now - self._last_retention_ms >= RETENTION_INTERVAL_MS:
                cutoff = now - TELEMETRY_RETENTION_MS
                self._connection.execute(
                    "DELETE FROM spans WHERE started_at_ms < ?", (cutoff,)
                )
                self._connection.execute(
                    "DELETE FROM events WHERE created_at_ms < ?", (cutoff,)
                )
                self._connection.execute(
                    "DELETE FROM presence_history WHERE observed_at_ms < ?", (cutoff,)
                )
                self._last_retention_ms = now
            self._trim_telemetry_locked(
                "events", "event_id", "created_at_ms", MAX_EVENT_RECORDS
            )
            self._trim_telemetry_locked(
                "spans", "span_id", "started_at_ms", MAX_SPAN_RECORDS
            )
            self._trim_telemetry_locked(
                "presence_history",
                "presence_id",
                "observed_at_ms",
                MAX_PRESENCE_RECORDS,
            )
        return {"expired_sessions": expired_sessions, "expired_tasks": expired_tasks}

    def _trim_telemetry_locked(
        self, table: str, key: str, timestamp: str, maximum: int
    ) -> None:
        """Delete a bounded batch beyond the newest per-table record limit."""
        allowed = {
            ("events", "event_id", "created_at_ms"),
            ("spans", "span_id", "started_at_ms"),
            ("presence_history", "presence_id", "observed_at_ms"),
        }
        if (table, key, timestamp) not in allowed:
            raise StoreError("invalid telemetry retention table")
        self._connection.execute(
            f"DELETE FROM {table} WHERE {key} IN ("  # noqa: S608 - fixed allowlist.
            f"SELECT {key} FROM {table} ORDER BY {timestamp} DESC, {key} DESC "
            "LIMIT ? OFFSET ?)",
            (RETENTION_DELETE_BATCH, maximum),
        )

    def _recover_session_tasks_locked(self, session_id: str, now: int) -> None:
        tasks = self._connection.execute(
            """
            SELECT task_id, sender_id, trace_id, state
            FROM tasks
            WHERE claimed_session_id = ? AND state IN ('accepted', 'running')
            """,
            (session_id,),
        ).fetchall()
        for task in tasks:
            task_id = str(task["task_id"])
            if task["state"] == "accepted":
                self._connection.execute(
                    """
                    UPDATE tasks SET state = 'queued', claimed_session_id = NULL,
                        updated_at_ms = ? WHERE task_id = ?
                    """,
                    (now, task_id),
                )
                self._record_event_locked(
                    event_type="task.requeued",
                    agent_id="edgecitadel-system",
                    task_id=task_id,
                    trace_id=str(task["trace_id"]),
                    attributes={"reason": "session_closed_before_execution"},
                    now=now,
                )
                continue
            result = {"error": "executor_session_lost", "retry_safe": False}
            self._connection.execute(
                """
                UPDATE tasks SET state = 'failed', updated_at_ms = ?,
                    terminal_reason = 'executor_session_lost', result_json = ?
                WHERE task_id = ?
                """,
                (now, self._encode_content(result), task_id),
            )
            self._record_event_locked(
                event_type="task.failed",
                agent_id="edgecitadel-system",
                task_id=task_id,
                trace_id=str(task["trace_id"]),
                attributes={"reason": "executor_session_lost"},
                now=now,
            )
            sender_id = str(task["sender_id"])
            if not self._agent_is_local_locked(sender_id):
                message_id = str(uuid.uuid4())
                self._queue_transport_locked(
                    message_id=message_id,
                    task_id=task_id,
                    subject=f"agents.{sender_id}.inbox",
                    envelope={
                        "v": 1,
                        "id": message_id,
                        "type": "result",
                        "sender_id": "edgecitadel-system",
                        "recipient_id": sender_id,
                        "task_id": task_id,
                        "task_state": "failed",
                        "timestamp": self._iso_timestamp(now),
                        "payload": result,
                    },
                    now=now,
                )

    def health(self) -> dict[str, object]:
        with self._lock:
            integrity = self._connection.execute("PRAGMA quick_check").fetchone()[0]
            schema = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
            active_sessions = int(
                self._connection.execute(
                    """
                    SELECT COUNT(*) FROM sessions
                    WHERE closed_at_ms IS NULL AND lease_expires_at_ms > ?
                    """,
                    (_now_ms(),),
                ).fetchone()[0]
            )
            telemetry_records = {
                table: int(
                    self._connection.execute(
                        f"SELECT COUNT(*) FROM {table}"  # noqa: S608 - fixed tuple.
                    ).fetchone()[0]
                )
                for table in ("events", "spans", "presence_history")
            }
            database_bytes = sum(
                candidate.stat().st_size
                for candidate in (
                    self.path,
                    Path(f"{self.path}-wal"),
                    Path(f"{self.path}-shm"),
                )
                if candidate.is_file()
            )
        return {
            "status": "ready" if integrity == "ok" else "failed",
            "database": integrity,
            "schema_version": schema,
            "active_sessions": active_sessions,
            "database_bytes": database_bytes,
            "telemetry_records": telemetry_records,
        }

    def _has_active_session_locked(self, connector_id: str, now: int) -> bool:
        return (
            self._connection.execute(
                """
                SELECT 1 FROM sessions
                WHERE connector_id = ? AND closed_at_ms IS NULL
                  AND lease_expires_at_ms > ? LIMIT 1
                """,
                (connector_id, now),
            ).fetchone()
            is not None
        )

    def _agent_is_local_locked(self, agent_id: str) -> bool:
        return (
            self._connection.execute(
                """
                SELECT 1 FROM connectors
                WHERE agent_id = ? AND revoked_at_ms IS NULL LIMIT 1
                """,
                (agent_id,),
            ).fetchone()
            is not None
        )

    def _queue_transport_locked(
        self,
        *,
        message_id: str,
        task_id: str,
        subject: str,
        envelope: Mapping[str, object],
        now: int,
    ) -> None:
        try:
            default_validator().validate_envelope(dict(envelope))
        except ValidationError as error:
            raise StoreError("outbound transport envelope is invalid") from error
        self._connection.execute(
            """
            INSERT INTO transport_outbox (
                message_id, task_id, subject, envelope_json, created_at_ms
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                message_id,
                task_id,
                subject,
                self._encode_content(envelope),
                now,
            ),
        )

    @staticmethod
    def _iso_timestamp(timestamp_ms: int) -> str:
        seconds, milliseconds = divmod(timestamp_ms, 1000)
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + (
            f".{milliseconds:03d}Z"
        )

    def _record_presence_locked(
        self, agent_id: str, state: str, reason: str, now: int
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO presence_history (agent_id, state, reason, observed_at_ms)
            VALUES (?, ?, ?, ?)
            """,
            (agent_id, state, reason, now),
        )

    def _record_event_locked(
        self,
        *,
        event_type: str,
        agent_id: str | None,
        task_id: str | None,
        trace_id: str | None,
        attributes: Mapping[str, object] | None,
        now: int,
    ) -> str:
        event_id = str(uuid.uuid4())
        self._connection.execute(
            """
            INSERT INTO events (
                event_id, event_type, agent_id, task_id, trace_id,
                attributes_json, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                event_type,
                agent_id,
                task_id,
                trace_id,
                _json_object(attributes),
                now,
            ),
        )
        return event_id

    def _task_mapping(self, row: sqlite3.Row) -> dict[str, object]:
        return {
            "task_id": row["task_id"],
            "sender_id": row["sender_id"],
            "recipient_id": row["recipient_id"],
            "skill_id": row["skill_id"],
            "state": row["state"],
            "payload": self._decode_content(row["payload_json"]),
            "trace_id": row["trace_id"],
            "context_id": row["context_id"],
            "deadline_at_ms": row["deadline_at_ms"],
            "created_at_ms": row["created_at_ms"],
            "updated_at_ms": row["updated_at_ms"],
            "terminal_reason": row["terminal_reason"],
            "result": self._decode_content(row["result_json"])
            if row["result_json"]
            else None,
        }

    @staticmethod
    def _span_mapping(row: sqlite3.Row) -> dict[str, object]:
        value = dict(row)
        value["attributes"] = json.loads(value.pop("attributes_json"))
        return value

    @staticmethod
    def _event_mapping(row: sqlite3.Row) -> dict[str, object]:
        value = dict(row)
        value["attributes"] = json.loads(value.pop("attributes_json"))
        return value
