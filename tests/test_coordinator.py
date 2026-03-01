"""Tests for MemoryCoordinator."""

import time

import pytest

from edgecitadel.memory import (
    AgentMemoryConfig,
    MemoryCoordinator,
    MemoryEntry,
    MemoryType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_coord() -> MemoryCoordinator:
    coord = MemoryCoordinator()
    coord.register_agent(AgentMemoryConfig("alice"))
    coord.register_agent(AgentMemoryConfig("bob"))
    return coord


def entry(key: str, value: object = "v", **kwargs: object) -> MemoryEntry:
    return MemoryEntry(key=key, value=value, **kwargs)


# ---------------------------------------------------------------------------
# Agent lifecycle
# ---------------------------------------------------------------------------


def test_register_and_list():
    coord = MemoryCoordinator()
    coord.register_agent(AgentMemoryConfig("alice"))
    assert "alice" in coord.registered_agents()


def test_double_register_raises():
    coord = MemoryCoordinator()
    coord.register_agent(AgentMemoryConfig("alice"))
    with pytest.raises(ValueError):
        coord.register_agent(AgentMemoryConfig("alice"))


def test_unregister():
    coord = MemoryCoordinator()
    coord.register_agent(AgentMemoryConfig("alice"))
    coord.unregister_agent("alice")
    assert "alice" not in coord.registered_agents()


def test_unregistered_agent_raises_on_write():
    coord = MemoryCoordinator()
    with pytest.raises(PermissionError):
        coord.write("ghost", entry("k"))


# ---------------------------------------------------------------------------
# Private memory
# ---------------------------------------------------------------------------


def test_write_and_read_private():
    coord = make_coord()
    coord.write("alice", entry("k1", 42))
    result = coord.read("alice", "k1")
    assert result is not None
    assert result.value == 42


def test_agents_have_isolated_stores():
    coord = make_coord()
    coord.write("alice", entry("shared_key", "alice_data"))
    coord.write("bob", entry("shared_key", "bob_data"))
    assert coord.read("alice", "shared_key").value == "alice_data"
    assert coord.read("bob", "shared_key").value == "bob_data"


def test_delete_private():
    coord = make_coord()
    coord.write("alice", entry("x"))
    assert coord.delete("alice", "x") is True
    assert coord.read("alice", "x") is None


# ---------------------------------------------------------------------------
# Shared memory – own namespace
# ---------------------------------------------------------------------------


def test_write_and_read_shared_own_namespace():
    coord = make_coord()
    coord.write_shared("alice", entry("note", "hello"))
    result = coord.read_shared("alice", "note")
    assert result is not None
    assert result.value == "hello"
    assert result.memory_type == MemoryType.SHARED


def test_delete_shared_own_namespace():
    coord = make_coord()
    coord.write_shared("alice", entry("note"))
    assert coord.delete_shared("alice", "note") is True
    assert coord.read_shared("alice", "note") is None


# ---------------------------------------------------------------------------
# Shared memory – cross-agent permissions
# ---------------------------------------------------------------------------


def test_read_without_permission_raises():
    coord = make_coord()
    coord.write_shared("alice", entry("secret", "shh"))
    with pytest.raises(PermissionError):
        coord.read_shared("bob", "secret", namespace="alice")


def test_read_with_permission_succeeds():
    coord = make_coord()
    coord.write_shared("alice", entry("msg", "hi bob"))
    coord.grant_read("alice", "bob")
    result = coord.read_shared("bob", "msg", namespace="alice")
    assert result.value == "hi bob"


def test_write_without_permission_raises():
    coord = make_coord()
    with pytest.raises(PermissionError):
        coord.write_shared("bob", entry("k"), namespace="alice")


def test_write_with_permission_succeeds():
    coord = make_coord()
    coord.grant_write("alice", "bob")
    coord.write_shared("bob", entry("collab", "data"), namespace="alice")
    coord.grant_read("alice", "bob")
    result = coord.read_shared("bob", "collab", namespace="alice")
    assert result.value == "data"


def test_revoke_read():
    coord = make_coord()
    coord.write_shared("alice", entry("msg", "hi"))
    coord.grant_read("alice", "bob")
    coord.revoke_read("alice", "bob")
    with pytest.raises(PermissionError):
        coord.read_shared("bob", "msg", namespace="alice")


def test_revoke_write():
    coord = make_coord()
    coord.grant_write("alice", "bob")
    coord.revoke_write("alice", "bob")
    with pytest.raises(PermissionError):
        coord.write_shared("bob", entry("k"), namespace="alice")


def test_permissions_cleaned_up_on_unregister():
    coord = make_coord()
    coord.grant_read("alice", "bob")
    coord.grant_write("alice", "bob")
    coord.unregister_agent("alice")
    # bob should no longer hold any permissions referencing alice
    assert "alice" not in coord._read_permissions.get("bob", set())
    assert "alice" not in coord._write_permissions.get("bob", set())


# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------


def test_purge_expired():
    coord = make_coord()
    coord.write("alice", MemoryEntry.with_ttl("tmp", "x", ttl_seconds=0.01))
    coord.write_shared("bob", MemoryEntry.with_ttl("tmp", "y", ttl_seconds=0.01))
    time.sleep(0.02)
    counts = coord.purge_expired()
    assert counts["alice"] == 1
    assert counts["__shared__"] == 1


def test_memory_stats():
    coord = make_coord()
    coord.write("alice", entry("a"))
    coord.write("alice", entry("b"))
    stats = coord.memory_stats()
    assert stats["agents"]["alice"]["entries"] == 2
    assert stats["agents"]["bob"]["entries"] == 0
    assert "shared" in stats


# ---------------------------------------------------------------------------
# Thread safety (basic smoke test)
# ---------------------------------------------------------------------------


def test_concurrent_writes_to_coordinator():
    import threading

    coord = MemoryCoordinator()
    for i in range(10):
        coord.register_agent(AgentMemoryConfig(f"agent_{i}"))

    errors: list = []

    def writer(agent_id: str) -> None:
        try:
            for j in range(20):
                coord.write(agent_id, entry(f"k{j}", j))
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=writer, args=(f"agent_{i}",)) for i in range(10)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
