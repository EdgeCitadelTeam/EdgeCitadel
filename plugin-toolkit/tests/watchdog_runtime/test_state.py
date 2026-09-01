"""Unit tests for the pure WatchdogState class (no NATS, no asyncio)."""

from datetime import datetime, timedelta, timezone


from edgecitadel_watchdog_plugin.state import WatchdogState


def _ts(seconds_ago: float = 0, base: datetime | None = None) -> datetime:
    base = base or datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    return base - timedelta(seconds=seconds_ago)


def test_record_register_populates_declared_interval():
    s = WatchdogState()
    s.record_register("gemma-1", interval_sec=30)
    assert s.declared_interval["gemma-1"] == 30


def test_record_register_is_idempotent():
    s = WatchdogState()
    s.record_register("gemma-1", interval_sec=30)
    s.record_register("gemma-1", interval_sec=60)  # operator updated card
    assert s.declared_interval["gemma-1"] == 60


def test_record_heartbeat_updates_last_seen():
    s = WatchdogState()
    now = _ts()
    s.record_heartbeat("gemma-1", now)
    assert s.last_seen["gemma-1"] == now


def test_record_heartbeat_clears_offline_flag_and_returns_recovery_signal():
    s = WatchdogState()
    s.offline_agents.add("gemma-1")
    became_online = s.record_heartbeat("gemma-1", _ts())
    assert became_online is True
    assert "gemma-1" not in s.offline_agents


def test_record_heartbeat_without_offline_flag_returns_false():
    s = WatchdogState()
    became_online = s.record_heartbeat("gemma-1", _ts())
    assert became_online is False


def test_record_outbox_command_for_online_agent_adds_pending():
    s = WatchdogState()
    sticky = s.record_outbox_command(
        recipient_id="gemma-1", task_id="t1", sender_id="aggregator"
    )
    assert sticky is False
    assert s.pending_tasks["gemma-1"]["t1"] == "aggregator"


def test_record_outbox_command_for_offline_agent_returns_sticky_true():
    s = WatchdogState()
    s.offline_agents.add("gemma-1")
    sticky = s.record_outbox_command(
        recipient_id="gemma-1", task_id="t1", sender_id="aggregator"
    )
    assert sticky is True
    # Sticky path: do NOT add to pending_tasks (it's already failed).
    assert "gemma-1" not in s.pending_tasks or "t1" not in s.pending_tasks["gemma-1"]


def test_record_outbox_result_clears_pending_keyed_by_worker():
    s = WatchdogState()
    s.record_outbox_command(
        recipient_id="gemma-1", task_id="t1", sender_id="aggregator"
    )
    # The result envelope's sender_id is the worker (gemma-1), task_id matches.
    s.record_outbox_result(worker_id="gemma-1", task_id="t1")
    assert s.pending_tasks.get("gemma-1", {}) == {}


def test_record_outbox_result_unknown_task_is_noop():
    s = WatchdogState()
    s.record_outbox_result(worker_id="gemma-1", task_id="never-seen")
    # Should not raise, should not create empty entries.
    assert s.pending_tasks.get("gemma-1") in (None, {})


def test_staleness_check_returns_offline_agents_with_pending():
    s = WatchdogState()
    base = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    s.record_register("gemma-1", interval_sec=30)
    s.record_heartbeat("gemma-1", base - timedelta(seconds=70))  # past 2*30+5
    s.record_outbox_command(
        recipient_id="gemma-1", task_id="t1", sender_id="aggregator"
    )
    transitions = s.staleness_check(now=base)
    assert len(transitions) == 1
    agent_id, pending = transitions[0]
    assert agent_id == "gemma-1"
    assert pending == [("t1", "aggregator")]
    # Side effect: offline flag set.
    assert "gemma-1" in s.offline_agents


def test_staleness_threshold_floor_is_20s():
    s = WatchdogState()
    base = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    # Schema minimum interval is 10s; 2*10=20 ≥ 20 floor; +5 tolerance = 25.
    s.record_register("fastagent-1", interval_sec=10)
    # 24s of silence — below 25s threshold, should NOT fire.
    s.record_heartbeat("fastagent-1", base - timedelta(seconds=24))
    transitions = s.staleness_check(now=base)
    assert transitions == []
    # 26s of silence — above threshold, should fire.
    s.record_heartbeat("fastagent-1", base - timedelta(seconds=26))
    transitions = s.staleness_check(now=base)
    assert len(transitions) == 1


def test_staleness_check_skips_already_offline():
    s = WatchdogState()
    base = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    s.record_register("gemma-1", interval_sec=30)
    s.record_heartbeat("gemma-1", base - timedelta(seconds=70))
    s.offline_agents.add("gemma-1")  # already known offline
    transitions = s.staleness_check(now=base)
    assert transitions == []  # don't re-fire


def test_staleness_check_uses_default_interval_when_no_register():
    s = WatchdogState()
    base = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    # No register seen → use default 30s, threshold = max(60, 20) + 5 = 65.
    s.record_heartbeat("ghost-1", base - timedelta(seconds=70))
    transitions = s.staleness_check(now=base)
    assert len(transitions) == 1


def test_staleness_check_drains_pending_after_fan_out():
    s = WatchdogState()
    base = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    s.record_register("gemma-1", interval_sec=30)
    s.record_heartbeat("gemma-1", base - timedelta(seconds=70))
    s.record_outbox_command(
        recipient_id="gemma-1", task_id="t1", sender_id="aggregator"
    )
    s.record_outbox_command(
        recipient_id="gemma-1", task_id="t2", sender_id="aggregator"
    )
    s.staleness_check(now=base)
    # After fan-out, pending should be empty for that recipient.
    assert s.pending_tasks.get("gemma-1", {}) == {}
