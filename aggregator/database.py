"""Aggregator SQLite persistence for the v0.1 agent messaging contract.

Schema is canonical (A2A-aligned): no legacy `receiver_id`/`message_type`
columns. The outbox mirror is the authoritative source for dashboard
conversation views (see ADR-0006).
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    v               INTEGER NOT NULL DEFAULT 1,
    type            TEXT NOT NULL,
    sender_id       TEXT NOT NULL,
    recipient_id    TEXT,
    task_id         TEXT,
    context_id      TEXT,
    task_state      TEXT,
    agent_state     TEXT,
    hop_count       INTEGER,
    timestamp       TEXT NOT NULL,
    payload         TEXT NOT NULL,   -- JSON-serialized object
    deployment      TEXT NOT NULL DEFAULT 'default'
);
CREATE INDEX IF NOT EXISTS idx_messages_sender      ON messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_recipient   ON messages(recipient_id);
CREATE INDEX IF NOT EXISTS idx_messages_task        ON messages(task_id);
CREATE INDEX IF NOT EXISTS idx_messages_context     ON messages(context_id);
CREATE INDEX IF NOT EXISTS idx_messages_type_ts     ON messages(type, timestamp);

CREATE TABLE IF NOT EXISTS agents (
    agent_id                TEXT PRIMARY KEY,
    card_json               TEXT NOT NULL,
    agent_state             TEXT NOT NULL DEFAULT 'online',
    last_heartbeat          TEXT,
    last_register           TEXT NOT NULL,
    deployment              TEXT,
    heartbeat_interval_sec  INTEGER NOT NULL DEFAULT 30
);

CREATE TABLE IF NOT EXISTS poison_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id        TEXT NOT NULL,
    consumer        TEXT NOT NULL,
    task_id         TEXT,
    original_sender TEXT,
    detected_at     TEXT NOT NULL,
    advisory_json   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poison_agent ON poison_events(agent_id, detected_at);

CREATE TABLE IF NOT EXISTS conversation_turns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    context_id      TEXT    NOT NULL,
    agent_id        TEXT    NOT NULL,
    role            TEXT    NOT NULL CHECK(role IN ('user','assistant','system')),
    content         TEXT    NOT NULL,
    token_count     INTEGER NOT NULL DEFAULT 0,
    skill_id        TEXT,
    turn_embedding  BLOB,
    created_at      REAL    NOT NULL DEFAULT (unixepoch('now','subsec'))
);
CREATE INDEX IF NOT EXISTS idx_turns_lookup
    ON conversation_turns(agent_id, context_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_turns_idle
    ON conversation_turns(created_at);
"""

_DROP_SQL = """
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS agents;
DROP TABLE IF EXISTS poison_events;
"""


def _db_path() -> str:
    return os.environ.get("DB_PATH", "/data/openclaw.db")


@contextmanager
def _conn() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(path: str, wipe: bool = False) -> None:
    """Initialize the aggregator schema at `path`.

    On `wipe=True` (or env `EDGECITADEL_DB_WIPE=1`) the canonical tables are
    dropped before being recreated. Used on first boot of the v0.1 rebuild.
    """
    os.environ["DB_PATH"] = path
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    should_wipe = wipe or os.environ.get("EDGECITADEL_DB_WIPE") == "1"
    with _conn() as c:
        if should_wipe:
            c.executescript(_DROP_SQL)
        c.executescript(SCHEMA_SQL)
        # Best-effort load of sqlite-vec extension for future semantic memory.
        # Aggregator boots fine without it; v0.3 will populate turn_embedding.
        try:
            import sqlite_vec        # noqa: F401 — provides loadable extension
            c.enable_load_extension(True)
            sqlite_vec.load(c)
            c.enable_load_extension(False)
            import logging
            logging.getLogger(__name__).info(
                "sqlite-vec loaded; v0.3 semantic memory ready")
        except Exception as e:  # noqa: BLE001
            import logging
            logging.getLogger(__name__).warning(
                "sqlite-vec not available (%s); semantic memory deferred",
                type(e).__name__)


# ── Messages ───────────────────────────────────────────────────────────

def insert_message(env: dict, deployment: str = "default") -> None:
    with _conn() as c:
        c.execute(
            """INSERT OR IGNORE INTO messages
               (id, v, type, sender_id, recipient_id, task_id, context_id,
                task_state, agent_state, hop_count, timestamp, payload, deployment)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (env["id"], env.get("v", 1), env["type"], env["sender_id"],
             env.get("recipient_id"), env.get("task_id"), env.get("context_id"),
             env.get("task_state"), env.get("agent_state"), env.get("hop_count"),
             env["timestamp"], json.dumps(env.get("payload", {})), deployment))


def query_messages(*, agent_id: str | None = None, task_id: str | None = None,
                   context_id: str | None = None, type: str | None = None,
                   since_ts: str | None = None,
                   deployment: str | None = None,
                   exclude_deployment: str | None = None,
                   limit: int = 500) -> list[dict]:
    q = "SELECT * FROM messages WHERE 1=1"
    params: list = []
    if agent_id:
        q += " AND (sender_id = ? OR recipient_id = ?)"; params += [agent_id, agent_id]
    if task_id:
        q += " AND task_id = ?"; params.append(task_id)
    if context_id:
        q += " AND context_id = ?"; params.append(context_id)
    if type:
        q += " AND type = ?"; params.append(type)
    if since_ts:
        q += " AND timestamp >= ?"; params.append(since_ts)
    if deployment:
        q += " AND deployment = ?"; params.append(deployment)
    if exclude_deployment:
        q += " AND deployment != ?"; params.append(exclude_deployment)
    q += " ORDER BY timestamp DESC LIMIT ?"; params.append(limit)
    with _conn() as c:
        rows = [dict(r) for r in c.execute(q, params).fetchall()]
    for r in rows:
        r["payload"] = json.loads(r["payload"])
    return rows


def count_messages() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]


def table_columns(name: str) -> set[str]:
    with _conn() as c:
        return {r[1] for r in c.execute(f"PRAGMA table_info({name})").fetchall()}


# ── Agents ─────────────────────────────────────────────────────────────

def upsert_agent_card(card: dict, timestamp: str) -> None:
    with _conn() as c:
        c.execute("""INSERT INTO agents
                     (agent_id, card_json, last_register, agent_state,
                      heartbeat_interval_sec, deployment)
                     VALUES (?, ?, ?, 'online', ?, ?)
                     ON CONFLICT(agent_id) DO UPDATE SET
                       card_json = excluded.card_json,
                       last_register = excluded.last_register,
                       agent_state = 'online',
                       heartbeat_interval_sec = excluded.heartbeat_interval_sec,
                       deployment = excluded.deployment""",
                  (card["name"], json.dumps(card), timestamp,
                   card["metadata"]["runtime.heartbeat_interval_sec"],
                   card["metadata"].get("runtime.deployment")))


def update_heartbeat(agent_id: str, ts: str) -> None:
    with _conn() as c:
        c.execute("UPDATE agents SET last_heartbeat = ?, agent_state = 'online' "
                  "WHERE agent_id = ?", (ts, agent_id))


def update_agent_state(agent_id: str, state: str) -> None:
    with _conn() as c:
        c.execute("UPDATE agents SET agent_state = ? WHERE agent_id = ?",
                  (state, agent_id))


def list_agents() -> list[dict]:
    with _conn() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT agent_id, card_json, agent_state, last_heartbeat, "
            "last_register, deployment, heartbeat_interval_sec "
            "FROM agents").fetchall()]
    for r in rows:
        r["card"] = json.loads(r.pop("card_json"))
    return rows


def get_agent(agent_id: str) -> dict | None:
    rows = [r for r in list_agents() if r["agent_id"] == agent_id]
    return rows[0] if rows else None


def delete_agent(agent_id: str) -> bool:
    with _conn() as c:
        cur = c.execute("DELETE FROM agents WHERE agent_id = ?", (agent_id,))
        return cur.rowcount > 0


# ── Poison events ──────────────────────────────────────────────────────

def insert_poison_event(*, agent_id: str, consumer: str, task_id: str | None,
                        original_sender: str | None, detected_at: str,
                        advisory: dict) -> None:
    with _conn() as c:
        c.execute("""INSERT INTO poison_events
                     (agent_id, consumer, task_id, original_sender,
                      detected_at, advisory_json)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (agent_id, consumer, task_id, original_sender, detected_at,
                   json.dumps(advisory)))


def recent_poison(agent_id: str | None = None, limit: int = 100) -> list[dict]:
    q = "SELECT * FROM poison_events"
    params: list = []
    if agent_id:
        q += " WHERE agent_id = ?"; params = [agent_id]
    q += " ORDER BY detected_at DESC LIMIT ?"; params.append(limit)
    with _conn() as c:
        return [dict(r) for r in c.execute(q, params).fetchall()]


def count_poison_by_agent() -> dict[str, int]:
    """Return {agent_id: count} for poison_events. Used by /api/registry."""
    with _conn() as c:
        rows = c.execute(
            "SELECT agent_id, COUNT(*) AS n FROM poison_events "
            "GROUP BY agent_id").fetchall()
    return {r["agent_id"]: r["n"] for r in rows}


# ── Conversation memory (Phase 2.5) ────────────────────────────────────

def fetch_recent_turns(*, agent_id: str, context_id: str,
                       limit: int = 200) -> list[dict]:
    """Return turns newest-first; caller walks token budget."""
    with _conn() as c:
        rows = c.execute(
            "SELECT id, role, content, token_count, skill_id, created_at "
            "FROM conversation_turns "
            "WHERE agent_id = ? AND context_id = ? "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            (agent_id, context_id, limit)).fetchall()
    return [dict(r) for r in rows]


def insert_turn(*, context_id: str, agent_id: str, role: str, content: str,
                token_count: int, skill_id: str | None) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO conversation_turns "
            "(context_id, agent_id, role, content, token_count, skill_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (context_id, agent_id, role, content, token_count, skill_id))
        return cur.lastrowid


def delete_turns_by_context(*, context_id: str) -> int:
    with _conn() as c:
        cur = c.execute("DELETE FROM conversation_turns WHERE context_id = ?",
                        (context_id,))
        return cur.rowcount


def purge_idle_contexts(*, idle_seconds: int) -> int:
    """Hard-delete all turns in contexts whose newest turn is past
    `idle_seconds` ago. Used by the memory service's cleanup loop."""
    with _conn() as c:
        cur = c.execute(
            "DELETE FROM conversation_turns WHERE context_id IN "
            "(SELECT context_id FROM conversation_turns "
            " GROUP BY context_id "
            " HAVING MAX(created_at) < unixepoch('now') - ?)",
            (idle_seconds,))
        return cur.rowcount


def list_conversations(agent_id: str | None = None) -> list[dict]:
    """Group conversation_turns by (agent_id, context_id) for the
    /api/conversations endpoint. Returns one row per conversation with
    aggregate fields."""
    q = ("SELECT agent_id, context_id, COUNT(*) AS turns, "
         "SUM(token_count) AS tokens, "
         "MIN(created_at) AS first_seen, MAX(created_at) AS last_seen, "
         "GROUP_CONCAT(DISTINCT skill_id) AS skills_csv "
         "FROM conversation_turns")
    params: list = []
    if agent_id:
        q += " WHERE agent_id = ?"
        params = [agent_id]
    q += " GROUP BY agent_id, context_id ORDER BY last_seen DESC"
    with _conn() as c:
        rows = [dict(r) for r in c.execute(q, params).fetchall()]
    out = []
    for r in rows:
        out.append({
            "context_id": r["context_id"],
            "agent_id": r["agent_id"],
            "turns": r["turns"],
            "tokens": r["tokens"] or 0,
            "first_seen": _iso_from_epoch(r["first_seen"]),
            "last_seen": _iso_from_epoch(r["last_seen"]),
            "skills": [s for s in (r["skills_csv"] or "").split(",") if s],
        })
    return out


def _iso_from_epoch(ts: float | None) -> str | None:
    """Convert UNIX epoch (subsec) to ISO 8601 with .sssZ suffix."""
    if ts is None:
        return None
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
