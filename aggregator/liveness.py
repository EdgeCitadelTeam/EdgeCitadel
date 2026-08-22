from __future__ import annotations

from datetime import datetime, timezone


THRESHOLD_FLOOR_SEC = 20
TOLERANCE_SEC = 5
DEFAULT_INTERVAL_SEC = 30


def heartbeat_is_stale(
    last_heartbeat: str | None,
    interval_sec: int | None,
    *,
    now: datetime | None = None,
) -> bool:
    if not last_heartbeat:
        return False
    try:
        last_seen = _parse_iso(last_heartbeat)
    except (TypeError, ValueError):
        return False
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    interval = interval_sec or DEFAULT_INTERVAL_SEC
    threshold = max(2 * interval, THRESHOLD_FLOOR_SEC) + TOLERANCE_SEC
    return (now - last_seen).total_seconds() > threshold


def effective_agent_state(
    agent: dict,
    *,
    now: datetime | None = None,
) -> str:
    if heartbeat_is_stale(
        agent.get("last_heartbeat"),
        agent.get("heartbeat_interval_sec"),
        now=now,
    ):
        return "offline"
    return agent.get("agent_state") or "offline"


def with_effective_agent_state(
    agent: dict,
    *,
    now: datetime | None = None,
) -> dict:
    row = dict(agent)
    row["agent_state"] = effective_agent_state(row, now=now)
    return row


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    parsed = datetime.fromisoformat(ts)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
