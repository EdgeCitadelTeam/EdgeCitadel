import json
import os
import re
import sqlite3
import time
import uuid

DB_PATH = os.environ.get("DB_PATH", "/data/openclaw.db")

# Patterns matching test agent IDs:
#   1. e2e timestamp: prefix-{13+digit timestamp}-{counter}-{random}
#   2. starts with "test-" or "test_"
#   3. ends with "-test" or "_test"
#   4. contains "-test-" or "_test_" (word-bounded)
#   5. exactly "test"
_TEST_ID_PATTERN = re.compile(
    r"(?:"
    r"-\d{13,}-\d{1,3}-[a-z0-9]{3,6}$"
    r"|^test[-_]"
    r"|[-_]test$"
    r"|[-_]test[-_]"
    r"|^test$"
    r")",
    re.IGNORECASE,
)


def _is_test_id(value: str) -> bool:
    return bool(_TEST_ID_PATTERN.search(value)) if value else False

_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
    return _conn


def init_db():
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS deployments (
            name            TEXT PRIMARY KEY,
            host            TEXT NOT NULL,
            port            INTEGER NOT NULL DEFAULT 1883,
            description     TEXT DEFAULT '',
            network         TEXT DEFAULT '',
            mqtt_user       TEXT DEFAULT '',
            mqtt_pass       TEXT DEFAULT '',
            active          INTEGER DEFAULT 1,
            registered_at   INTEGER
        );

        CREATE TABLE IF NOT EXISTS episodes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            deployment  TEXT NOT NULL,
            topic       TEXT NOT NULL,
            payload     TEXT,
            ts          INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_episodes_deployment ON episodes(deployment);
        CREATE INDEX IF NOT EXISTS idx_episodes_ts ON episodes(ts);

        CREATE TABLE IF NOT EXISTS agents (
            id              TEXT PRIMARY KEY,
            deployment      TEXT NOT NULL DEFAULT '',
            display_name    TEXT DEFAULT '',
            status          TEXT DEFAULT 'online',
            role            TEXT DEFAULT '',
            device_type     TEXT DEFAULT '',
            capabilities    TEXT DEFAULT '[]',
            ip_address      TEXT DEFAULT '',
            model           TEXT DEFAULT '',
            cpu_percent     REAL,
            memory_percent  REAL,
            last_heartbeat  TEXT,
            registered_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS messages (
            id              TEXT PRIMARY KEY,
            deployment      TEXT NOT NULL DEFAULT '',
            sender_id       TEXT NOT NULL,
            receiver_id     TEXT DEFAULT '',
            message_type    TEXT DEFAULT 'info',
            payload         TEXT DEFAULT '{}',
            correlation_id  TEXT DEFAULT '',
            timestamp       TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp);
        CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);
        CREATE INDEX IF NOT EXISTS idx_messages_receiver ON messages(receiver_id);
        CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation_id);

        CREATE TABLE IF NOT EXISTS logs (
            id          TEXT PRIMARY KEY,
            deployment  TEXT NOT NULL DEFAULT '',
            level       TEXT DEFAULT 'INFO',
            timestamp   TEXT NOT NULL,
            agent_id    TEXT DEFAULT '',
            source      TEXT DEFAULT '',
            message     TEXT DEFAULT '',
            metadata    TEXT DEFAULT '{}'
        );

        CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(timestamp);
        CREATE INDEX IF NOT EXISTS idx_logs_agent ON logs(agent_id);

        CREATE TABLE IF NOT EXISTS tasks (
            id              TEXT PRIMARY KEY,
            deployment      TEXT NOT NULL DEFAULT '',
            title           TEXT DEFAULT '',
            description     TEXT DEFAULT '',
            status          TEXT DEFAULT 'pending',
            priority        TEXT DEFAULT 'normal',
            assigned_agent  TEXT DEFAULT '',
            created_at      TEXT NOT NULL,
            started_at      TEXT,
            completed_at    TEXT,
            result          TEXT DEFAULT '{}',
            error_message   TEXT DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_agent ON tasks(assigned_agent);
    """)
    conn.commit()


# ── Deployments ──

def get_all_active_deployments() -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM deployments WHERE active = 1").fetchall()
    return [dict(r) for r in rows]


def upsert_deployment(name: str, host: str, port: int, description: str = "", network: str = "",
                      mqtt_user: str = "", mqtt_pass: str = ""):
    conn = _get_conn()
    conn.execute(
        """INSERT INTO deployments (name, host, port, description, network, mqtt_user, mqtt_pass, active, registered_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
           ON CONFLICT(name) DO UPDATE SET
               host = excluded.host,
               port = excluded.port,
               description = excluded.description,
               network = excluded.network,
               mqtt_user = excluded.mqtt_user,
               mqtt_pass = excluded.mqtt_pass,
               active = 1,
               registered_at = excluded.registered_at
        """,
        (name, host, port, description, network, mqtt_user, mqtt_pass, int(time.time())),
    )
    conn.commit()


def deactivate_deployment(name: str):
    conn = _get_conn()
    conn.execute("UPDATE deployments SET active = 0 WHERE name = ?", (name,))
    conn.commit()


def get_deployment(name: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM deployments WHERE name = ?", (name,)).fetchone()
    return dict(row) if row else None


# ── Episodes (raw MQTT) ──

def insert_episode(deployment: str, topic: str, payload: str, ts: int):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO episodes (deployment, topic, payload, ts) VALUES (?, ?, ?, ?)",
        (deployment, topic, payload, ts),
    )
    conn.commit()


def get_episodes(deployment: str | None = None, limit: int = 100) -> list[dict]:
    conn = _get_conn()
    if deployment:
        rows = conn.execute(
            "SELECT * FROM episodes WHERE deployment = ? ORDER BY ts DESC LIMIT ?",
            (deployment, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM episodes ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ── Agents ──

def upsert_agent(agent_id: str, deployment: str = "", **kwargs):
    conn = _get_conn()
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing = conn.execute("SELECT id FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if existing:
        sets = []
        vals = []
        for k, v in kwargs.items():
            if v is not None:
                sets.append(f"{k} = ?")
                vals.append(v)
        if deployment:
            sets.append("deployment = ?")
            vals.append(deployment)
        if sets:
            vals.append(agent_id)
            conn.execute(f"UPDATE agents SET {', '.join(sets)} WHERE id = ?", vals)
    else:
        caps = kwargs.get("capabilities", "[]")
        conn.execute(
            """INSERT INTO agents (id, deployment, display_name, status, role, device_type,
               capabilities, ip_address, model, last_heartbeat, registered_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                agent_id, deployment,
                kwargs.get("display_name", agent_id.replace("_", " ").title()),
                kwargs.get("status", "online"),
                kwargs.get("role", ""),
                kwargs.get("device_type", ""),
                caps if isinstance(caps, str) else json.dumps(caps),
                kwargs.get("ip_address", ""),
                kwargs.get("model", ""),
                now, now,
            ),
        )
    conn.commit()


def _is_test_agent(d: dict) -> bool:
    """Check if an agent dict represents test data."""
    return _is_test_id(d.get("id", "")) or _is_test_id(d.get("display_name", ""))


def get_all_agents(exclude_test: bool = False) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM agents ORDER BY status DESC, id").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if exclude_test and _is_test_agent(d):
            continue
        if d.get("capabilities"):
            try:
                d["capabilities"] = json.loads(d["capabilities"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
    return result


def get_agent(agent_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    if d.get("capabilities"):
        try:
            d["capabilities"] = json.loads(d["capabilities"])
        except (json.JSONDecodeError, TypeError):
            pass
    return d


def delete_agent(agent_id: str):
    conn = _get_conn()
    conn.execute("DELETE FROM agents WHERE id = ?", (agent_id,))
    conn.commit()


def mark_stale_agents_offline(timeout_seconds: int = 60) -> list[str]:
    """Mark agents offline if heartbeat is stale. Returns list of agent IDs that went offline."""
    conn = _get_conn()
    cutoff = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ",
        time.gmtime(time.time() - timeout_seconds),
    )
    rows = conn.execute(
        "SELECT id FROM agents WHERE status = 'online' AND last_heartbeat < ?",
        (cutoff,),
    ).fetchall()
    affected = [r["id"] for r in rows]
    if affected:
        conn.execute(
            "UPDATE agents SET status = 'offline' WHERE status = 'online' AND last_heartbeat < ?",
            (cutoff,),
        )
        conn.commit()
    return affected


def get_agent_stats(agent_id: str) -> dict:
    conn = _get_conn()
    msg_count = conn.execute(
        "SELECT COUNT(*) as c FROM messages WHERE sender_id = ? OR receiver_id = ?",
        (agent_id, agent_id),
    ).fetchone()["c"]
    task_count = conn.execute(
        "SELECT COUNT(*) as c FROM tasks WHERE assigned_agent = ?",
        (agent_id,),
    ).fetchone()["c"]
    return {"message_count": msg_count, "task_count": task_count}


# ── Messages ──

def insert_message(deployment: str, sender_id: str, receiver_id: str = "",
                   message_type: str = "info", payload: dict | None = None,
                   correlation_id: str = "", timestamp: str = "") -> str:
    conn = _get_conn()
    msg_id = str(uuid.uuid4())
    if not timestamp:
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload_str = json.dumps(payload) if payload else "{}"
    conn.execute(
        """INSERT INTO messages (id, deployment, sender_id, receiver_id, message_type,
           payload, correlation_id, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (msg_id, deployment, sender_id, receiver_id, message_type,
         payload_str, correlation_id, timestamp),
    )
    conn.commit()
    return msg_id


def _is_test_message(d: dict) -> bool:
    """Check if a message dict involves test agents."""
    return _is_test_id(d.get("sender_id", "")) or _is_test_id(d.get("receiver_id", ""))


def get_messages(limit: int = 50, offset: int = 0, agent: str = "",
                 msg_type: str = "", search: str = "",
                 correlation_id: str = "", exclude_test: bool = False) -> list[dict]:
    conn = _get_conn()
    clauses = []
    params: list = []
    if agent:
        clauses.append("(sender_id = ? OR receiver_id = ?)")
        params.extend([agent, agent])
    if msg_type:
        clauses.append("message_type = ?")
        params.append(msg_type)
    if search:
        clauses.append("(payload LIKE ? OR sender_id LIKE ? OR receiver_id LIKE ?)")
        params.extend([f"%{search}%"] * 3)
    if correlation_id:
        clauses.append("correlation_id = ?")
        params.append(correlation_id)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    # Fetch extra rows when filtering test data to compensate for filtered-out rows
    fetch_limit = limit * 3 if exclude_test else limit
    params.extend([fetch_limit, offset])
    rows = conn.execute(
        f"SELECT * FROM messages {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        params,
    ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        if exclude_test and _is_test_message(d):
            continue
        if d.get("payload"):
            try:
                d["payload"] = json.loads(d["payload"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
        if len(result) >= limit:
            break
    return result


FLOW_SKIP_IDS = {"dashboard", "system", "mqtt-broker", "broadcast"}


def get_message_flow(hours: int = 24, exclude_test: bool = False) -> dict:
    conn = _get_conn()
    cutoff = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ",
        time.gmtime(time.time() - hours * 3600),
    )
    rows = conn.execute(
        """SELECT sender_id, receiver_id, COUNT(*) as count
           FROM messages WHERE timestamp >= ? AND receiver_id != ''
           GROUP BY sender_id, receiver_id""",
        (cutoff,),
    ).fetchall()

    node_ids = set()
    edges = []
    for r in rows:
        d = dict(r)
        sender = d["sender_id"]
        receiver = d["receiver_id"]
        # Skip non-agent IDs from flow graph
        if sender in FLOW_SKIP_IDS or receiver in FLOW_SKIP_IDS:
            continue
        if exclude_test and (_is_test_id(sender) or _is_test_id(receiver)):
            continue
        node_ids.add(sender)
        node_ids.add(receiver)
        edges.append({"source": sender, "target": receiver, "count": d["count"]})

    return {"nodes": [{"id": n} for n in node_ids], "edges": edges}


# ── Logs ──

def insert_log(deployment: str, level: str, agent_id: str = "", source: str = "",
               message: str = "", metadata: dict | None = None):
    conn = _get_conn()
    log_id = str(uuid.uuid4())
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    meta_str = json.dumps(metadata) if metadata else "{}"
    conn.execute(
        """INSERT INTO logs (id, deployment, level, timestamp, agent_id, source, message, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (log_id, deployment, level, now, agent_id, source, message, meta_str),
    )
    conn.commit()


def get_logs(limit: int = 200, level: str = "", agent: str = "",
             source: str = "", search: str = "", exclude_test: bool = False) -> list[dict]:
    conn = _get_conn()
    clauses = []
    params: list = []
    if level:
        clauses.append("level = ?")
        params.append(level)
    if agent:
        clauses.append("agent_id = ?")
        params.append(agent)
    if source:
        clauses.append("source LIKE ?")
        params.append(f"%{source}%")
    if search:
        clauses.append("message LIKE ?")
        params.append(f"%{search}%")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    fetch_limit = limit * 3 if exclude_test else limit
    params.append(fetch_limit)
    rows = conn.execute(
        f"SELECT * FROM logs {where} ORDER BY timestamp DESC LIMIT ?",
        params,
    ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        if exclude_test and _is_test_id(d.get("agent_id", "")):
            continue
        if d.get("metadata"):
            try:
                d["metadata"] = json.loads(d["metadata"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
        if len(result) >= limit:
            break
    return result


# ── Tasks ──

def insert_task(deployment: str = "", title: str = "", description: str = "",
                assigned_agent: str = "", priority: str = "normal",
                task_id: str = "") -> str:
    conn = _get_conn()
    if not task_id:
        task_id = str(uuid.uuid4())
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """INSERT OR IGNORE INTO tasks (id, deployment, title, description, status, priority,
           assigned_agent, created_at) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)""",
        (task_id, deployment, title, description, priority, assigned_agent, now),
    )
    conn.commit()
    return task_id


def update_task(task_id: str, **kwargs):
    conn = _get_conn()
    sets = []
    vals = []
    for k, v in kwargs.items():
        if v is not None:
            if k == "result":
                v = json.dumps(v) if isinstance(v, dict) else v
            sets.append(f"{k} = ?")
            vals.append(v)
    if sets:
        vals.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()


def get_tasks(limit: int = 200, agent: str = "", status: str = "",
              exclude_test: bool = False) -> list[dict]:
    conn = _get_conn()
    clauses = []
    params: list = []
    if agent:
        clauses.append("assigned_agent = ?")
        params.append(agent)
    if status:
        clauses.append("status = ?")
        params.append(status)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    fetch_limit = limit * 3 if exclude_test else limit
    params.append(fetch_limit)
    rows = conn.execute(
        f"SELECT * FROM tasks {where} ORDER BY created_at DESC LIMIT ?",
        params,
    ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        if exclude_test and _is_test_id(d.get("assigned_agent", "")):
            continue
        if d.get("result"):
            try:
                d["result"] = json.loads(d["result"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
        if len(result) >= limit:
            break
    return result


def get_task(task_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    if d.get("result"):
        try:
            d["result"] = json.loads(d["result"])
        except (json.JSONDecodeError, TypeError):
            pass
    return d


def get_task_trace(task_id: str) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM messages WHERE correlation_id = ? ORDER BY timestamp",
        (task_id,),
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("payload"):
            try:
                d["payload"] = json.loads(d["payload"])
            except (json.JSONDecodeError, TypeError):
                pass
        result.append(d)
    return result


# ── System Status ──

def get_system_status(exclude_test: bool = False) -> dict:
    conn = _get_conn()
    if exclude_test:
        all_agents = conn.execute("SELECT id, status FROM agents").fetchall()
        non_test = [r for r in all_agents if not _is_test_id(r["id"])]
        agents_online = sum(1 for r in non_test if r["status"] == "online")
        agents_total = len(non_test)
    else:
        agents_online = conn.execute(
            "SELECT COUNT(*) as c FROM agents WHERE status = 'online'"
        ).fetchone()["c"]
        agents_total = conn.execute("SELECT COUNT(*) as c FROM agents").fetchone()["c"]
    total_messages = conn.execute("SELECT COUNT(*) as c FROM messages").fetchone()["c"]
    active_tasks = conn.execute(
        "SELECT COUNT(*) as c FROM tasks WHERE status IN ('pending', 'assigned', 'running')"
    ).fetchone()["c"]
    today_start = time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime())
    errors_today = conn.execute(
        "SELECT COUNT(*) as c FROM logs WHERE level = 'ERROR' AND timestamp >= ?",
        (today_start,),
    ).fetchone()["c"]
    return {
        "agents_online": agents_online,
        "agents_total": agents_total,
        "total_messages": total_messages,
        "active_tasks": active_tasks,
        "errors_today": errors_today,
        "nats_connected": True,
    }
