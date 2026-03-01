# EdgeCitadel

**Safe and effective memory coordination for edge AI agents.**

EdgeCitadel provides a lightweight, thread-safe memory coordination layer for
Agentic AI systems running on resource-constrained edge devices (IoT gateways,
embedded boards, single-board computers, etc.).

---

## Motivation

Modern agentic AI pipelines run multiple specialised agents concurrently.  On
edge hardware memory is scarce and must be carefully managed: agents need their
own private working space, a way to share state with peers, and guarantees that
expired or low-priority entries are evicted before the device runs out of
memory.  EdgeCitadel provides exactly that — with no external dependencies.

---

## Features

| Feature | Detail |
|---|---|
| **Private per-agent stores** | Each agent gets its own isolated `MemoryStore` |
| **Shared store with namespaces** | Agents publish to and consume from a global shared store |
| **Fine-grained permissions** | Read and write access to other agents' namespaces is granted explicitly |
| **TTL-based expiry** | Entries expire automatically after a configurable time-to-live |
| **Pluggable eviction policies** | LRU, TTL-first, or lowest-priority eviction when a store is full |
| **Thread safety** | All operations are protected by a re-entrant lock |
| **Zero runtime dependencies** | Pure Python 3.9+, stdlib only |

---

## Quick start

```python
from edgecitadel import AgentMemoryConfig, MemoryCoordinator, MemoryEntry, MemoryType

# Create a coordinator (shared store limited to 2 000 entries)
coord = MemoryCoordinator(shared_max_entries=2000)

# Register two agents, each with their own 500-entry private store
coord.register_agent(AgentMemoryConfig("planner", max_entries=500))
coord.register_agent(AgentMemoryConfig("executor", max_entries=500))

# Write to the planner's private memory
coord.write("planner", MemoryEntry(key="goal", value="optimise route", memory_type=MemoryType.WORKING))

# Read it back
entry = coord.read("planner", "goal")
print(entry.value)  # optimise route

# Publish a plan to the shared store (under the planner's own namespace)
coord.write_shared("planner", MemoryEntry(key="current_plan", value=["step1", "step2"]))

# Grant the executor read access to the planner's namespace, then read the plan
coord.grant_read("planner", "executor")
plan = coord.read_shared("executor", "current_plan", namespace="planner")
print(plan.value)  # ['step1', 'step2']

# Entries with a TTL expire automatically
from edgecitadel import MemoryEntry
coord.write("executor", MemoryEntry.with_ttl("sensor_reading", 42.7, ttl_seconds=5))

# Housekeeping: sweep all stores and reclaim expired entries
counts = coord.purge_expired()

# Inspect memory usage
stats = coord.memory_stats()
print(stats)
```

---

## Memory types

| Type | Purpose |
|---|---|
| `MemoryType.WORKING` | Short-lived operational memory for the current task |
| `MemoryType.EPISODIC` | Record of past events and interactions |
| `MemoryType.SEMANTIC` | General facts and domain knowledge |
| `MemoryType.SHARED` | State explicitly shared between multiple agents |

---

## Eviction policies

| Policy | Behaviour |
|---|---|
| `EvictionPolicy.LRU` | Evict the least-recently accessed entry *(default)* |
| `EvictionPolicy.TTL_FIRST` | Evict the entry closest to its expiry first |
| `EvictionPolicy.LOWEST_PRIORITY` | Evict the entry with the lowest `priority` value |

---

## Installation

```bash
pip install edgecitadel          # from PyPI (once published)
pip install -e ".[dev]"          # editable install for development
```

---

## Running tests

```bash
pytest
```

---

## Project structure

```
edgecitadel/
├── memory/
│   ├── types.py        # MemoryEntry, MemoryType, EvictionPolicy
│   ├── store.py        # Thread-safe MemoryStore
│   └── coordinator.py  # Multi-agent MemoryCoordinator
tests/
├── test_store.py
└── test_coordinator.py
```

---

## License

MIT
