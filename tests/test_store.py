"""Tests for MemoryStore."""

import time

import pytest

from edgecitadel.memory import EvictionPolicy, MemoryEntry, MemoryStore, MemoryType


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------


def make_entry(key: str, value: object = "v", **kwargs: object) -> MemoryEntry:
    return MemoryEntry(key=key, value=value, **kwargs)


def test_put_and_get():
    store = MemoryStore()
    store.put(make_entry("a", 1))
    entry = store.get("a")
    assert entry is not None
    assert entry.value == 1


def test_get_missing_returns_none():
    store = MemoryStore()
    assert store.get("nope") is None


def test_delete_existing_returns_true():
    store = MemoryStore()
    store.put(make_entry("a"))
    assert store.delete("a") is True
    assert store.get("a") is None


def test_delete_missing_returns_false():
    store = MemoryStore()
    assert store.delete("nope") is False


def test_contains():
    store = MemoryStore()
    store.put(make_entry("x"))
    assert store.contains("x")
    assert not store.contains("y")


def test_len():
    store = MemoryStore()
    store.put(make_entry("a"))
    store.put(make_entry("b"))
    assert len(store) == 2


def test_clear():
    store = MemoryStore()
    store.put(make_entry("a"))
    store.clear()
    assert len(store) == 0


def test_update_existing_does_not_evict():
    store = MemoryStore(max_entries=2)
    store.put(make_entry("a", 1))
    store.put(make_entry("b", 2))
    store.put(make_entry("a", 99))  # update, not new entry
    assert len(store) == 2
    assert store.get("a").value == 99


# ---------------------------------------------------------------------------
# TTL / expiry
# ---------------------------------------------------------------------------


def test_expired_entry_returns_none():
    store = MemoryStore()
    entry = MemoryEntry.with_ttl("x", "hello", ttl_seconds=0.01)
    store.put(entry)
    time.sleep(0.02)
    assert store.get("x") is None


def test_non_expired_entry_survives():
    store = MemoryStore()
    entry = MemoryEntry.with_ttl("y", "world", ttl_seconds=60)
    store.put(entry)
    assert store.get("y") is not None


def test_purge_expired():
    store = MemoryStore()
    store.put(MemoryEntry.with_ttl("a", 1, ttl_seconds=0.01))
    store.put(MemoryEntry.with_ttl("b", 2, ttl_seconds=0.01))
    store.put(make_entry("c", 3))
    time.sleep(0.02)
    removed = store.purge_expired()
    assert removed == 2
    assert len(store) == 1


# ---------------------------------------------------------------------------
# Memory type filtering
# ---------------------------------------------------------------------------


def test_by_type():
    store = MemoryStore()
    store.put(make_entry("w1", memory_type=MemoryType.WORKING))
    store.put(make_entry("w2", memory_type=MemoryType.WORKING))
    store.put(make_entry("s1", memory_type=MemoryType.SEMANTIC))
    working = store.by_type(MemoryType.WORKING)
    assert len(working) == 2
    assert all(e.memory_type == MemoryType.WORKING for e in working)


# ---------------------------------------------------------------------------
# Eviction policies
# ---------------------------------------------------------------------------


def test_lru_eviction():
    store = MemoryStore(max_entries=2, eviction_policy=EvictionPolicy.LRU)
    store.put(make_entry("a", 1))
    time.sleep(0.001)
    store.put(make_entry("b", 2))
    # Access 'a' to make it the most recently used
    store.get("a")
    time.sleep(0.001)
    # Adding 'c' should evict 'b' (least recently used)
    store.put(make_entry("c", 3))
    assert store.get("a") is not None
    assert store.get("b") is None
    assert store.get("c") is not None


def test_ttl_first_eviction():
    store = MemoryStore(max_entries=2, eviction_policy=EvictionPolicy.TTL_FIRST)
    short_lived = MemoryEntry.with_ttl("short", "s", ttl_seconds=1)
    immortal = make_entry("immortal", "i")
    store.put(short_lived)
    store.put(immortal)
    # Adding a third entry should evict the short-lived one
    store.put(make_entry("new", "n"))
    assert store.get("immortal") is not None
    assert store.get("new") is not None
    assert store.get("short") is None


def test_lowest_priority_eviction():
    store = MemoryStore(max_entries=2, eviction_policy=EvictionPolicy.LOWEST_PRIORITY)
    store.put(make_entry("hi", priority=10))
    store.put(make_entry("lo", priority=1))
    store.put(make_entry("new", priority=5))
    assert store.get("hi") is not None
    assert store.get("lo") is None


# ---------------------------------------------------------------------------
# Thread safety (basic smoke test)
# ---------------------------------------------------------------------------


def test_concurrent_writes():
    import threading

    store = MemoryStore(max_entries=1000)
    errors: list = []

    def writer(start: int) -> None:
        try:
            for i in range(50):
                store.put(make_entry(f"k{start + i}", i))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i * 50,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(store) == 200
