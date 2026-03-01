"""
Memory coordinator: safe multi-agent memory management for edge devices.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional, Set

from .store import MemoryStore
from .types import EvictionPolicy, MemoryEntry, MemoryType


class AgentMemoryConfig:
    """Resource limits and defaults for one registered agent."""

    def __init__(
        self,
        agent_id: str,
        max_entries: int = 500,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
    ) -> None:
        self.agent_id = agent_id
        self.max_entries = max_entries
        self.eviction_policy = eviction_policy


class MemoryCoordinator:
    """Coordinates memory across multiple AI agents on an edge device.

    Each registered agent receives its own isolated :class:`MemoryStore`.
    Agents may additionally read from and write to a shared store, subject
    to explicit permission grants.

    Parameters
    ----------
    shared_max_entries:
        Maximum entries in the global shared store.  Use ``None`` for
        unlimited (not recommended on constrained hardware).
    """

    def __init__(self, shared_max_entries: Optional[int] = 2000) -> None:
        self._shared_store = MemoryStore(max_entries=shared_max_entries)
        self._agent_stores: Dict[str, MemoryStore] = {}
        self._agent_configs: Dict[str, AgentMemoryConfig] = {}
        # Mapping: agent_id -> set of agent_ids whose shared keys it may read
        self._read_permissions: Dict[str, Set[str]] = {}
        # Mapping: agent_id -> set of agent_ids whose shared keys it may write
        self._write_permissions: Dict[str, Set[str]] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Agent lifecycle
    # ------------------------------------------------------------------

    def register_agent(self, config: AgentMemoryConfig) -> None:
        """Register a new agent and allocate its private memory store."""
        with self._lock:
            if config.agent_id in self._agent_stores:
                raise ValueError(f"Agent '{config.agent_id}' is already registered.")
            self._agent_stores[config.agent_id] = MemoryStore(
                max_entries=config.max_entries,
                eviction_policy=config.eviction_policy,
            )
            self._agent_configs[config.agent_id] = config
            self._read_permissions[config.agent_id] = set()
            self._write_permissions[config.agent_id] = set()

    def unregister_agent(self, agent_id: str) -> None:
        """Unregister an agent and release all its private memory."""
        with self._lock:
            self._require_registered(agent_id)
            del self._agent_stores[agent_id]
            del self._agent_configs[agent_id]
            del self._read_permissions[agent_id]
            del self._write_permissions[agent_id]
            # Remove references to this agent in other agents' permission sets
            for perms in self._read_permissions.values():
                perms.discard(agent_id)
            for perms in self._write_permissions.values():
                perms.discard(agent_id)

    def registered_agents(self) -> List[str]:
        """Return the IDs of all currently registered agents."""
        with self._lock:
            return list(self._agent_stores.keys())

    # ------------------------------------------------------------------
    # Permission management
    # ------------------------------------------------------------------

    def grant_read(self, granting_agent: str, to_agent: str) -> None:
        """Allow *to_agent* to read *granting_agent*'s shared-store entries."""
        with self._lock:
            self._require_registered(granting_agent)
            self._require_registered(to_agent)
            self._read_permissions[to_agent].add(granting_agent)

    def revoke_read(self, granting_agent: str, to_agent: str) -> None:
        """Revoke *to_agent*'s read access to *granting_agent*'s shared entries."""
        with self._lock:
            self._require_registered(to_agent)
            self._read_permissions[to_agent].discard(granting_agent)

    def grant_write(self, granting_agent: str, to_agent: str) -> None:
        """Allow *to_agent* to write to *granting_agent*'s shared-store namespace."""
        with self._lock:
            self._require_registered(granting_agent)
            self._require_registered(to_agent)
            self._write_permissions[to_agent].add(granting_agent)

    def revoke_write(self, granting_agent: str, to_agent: str) -> None:
        """Revoke *to_agent*'s write access to *granting_agent*'s shared namespace."""
        with self._lock:
            self._require_registered(to_agent)
            self._write_permissions[to_agent].discard(granting_agent)

    # ------------------------------------------------------------------
    # Private memory access
    # ------------------------------------------------------------------

    def write(self, agent_id: str, entry: MemoryEntry) -> None:
        """Write *entry* to *agent_id*'s private memory store."""
        with self._lock:
            self._require_registered(agent_id)
            entry.owner = agent_id
            self._agent_stores[agent_id].put(entry)

    def read(self, agent_id: str, key: str) -> Optional[MemoryEntry]:
        """Read a key from *agent_id*'s private memory store."""
        with self._lock:
            self._require_registered(agent_id)
            return self._agent_stores[agent_id].get(key)

    def delete(self, agent_id: str, key: str) -> bool:
        """Delete a key from *agent_id*'s private memory store."""
        with self._lock:
            self._require_registered(agent_id)
            return self._agent_stores[agent_id].delete(key)

    # ------------------------------------------------------------------
    # Shared memory access
    # ------------------------------------------------------------------

    def write_shared(
        self,
        agent_id: str,
        entry: MemoryEntry,
        namespace: Optional[str] = None,
    ) -> None:
        """Write *entry* to the shared store.

        The entry is stored under ``<namespace>/<entry.key>`` if *namespace* is
        provided, otherwise under *agent_id*'s own namespace
        (``<agent_id>/<entry.key>``).

        Parameters
        ----------
        agent_id:
            The writing agent.  Must be registered and, if writing into another
            agent's namespace, must hold write permission for that namespace.
        entry:
            The memory entry to store.
        namespace:
            Target namespace (defaults to *agent_id*).
        """
        with self._lock:
            self._require_registered(agent_id)
            target_ns = namespace if namespace is not None else agent_id
            if target_ns != agent_id:
                self._require_write_permission(agent_id, target_ns)
            scoped_key = f"{target_ns}/{entry.key}"
            scoped_entry = MemoryEntry(
                key=scoped_key,
                value=entry.value,
                memory_type=MemoryType.SHARED,
                priority=entry.priority,
                expires_at=entry.expires_at,
                owner=agent_id,
            )
            self._shared_store.put(scoped_entry)

    def read_shared(
        self,
        agent_id: str,
        key: str,
        namespace: Optional[str] = None,
    ) -> Optional[MemoryEntry]:
        """Read a key from the shared store.

        Parameters
        ----------
        agent_id:
            The reading agent.
        key:
            The key within *namespace*.
        namespace:
            Source namespace (defaults to *agent_id*).  If reading from
            another agent's namespace, *agent_id* must hold read permission.
        """
        with self._lock:
            self._require_registered(agent_id)
            source_ns = namespace if namespace is not None else agent_id
            if source_ns != agent_id:
                self._require_read_permission(agent_id, source_ns)
            scoped_key = f"{source_ns}/{key}"
            return self._shared_store.get(scoped_key)

    def delete_shared(
        self,
        agent_id: str,
        key: str,
        namespace: Optional[str] = None,
    ) -> bool:
        """Delete a key from the shared store.

        An agent may only delete entries in its own namespace or in a namespace
        for which it holds write permission.
        """
        with self._lock:
            self._require_registered(agent_id)
            target_ns = namespace if namespace is not None else agent_id
            if target_ns != agent_id:
                self._require_write_permission(agent_id, target_ns)
            scoped_key = f"{target_ns}/{key}"
            return self._shared_store.delete(scoped_key)

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def purge_expired(self) -> Dict[str, int]:
        """Remove all expired entries across every store.

        Returns a dict mapping store name to the count of purged entries.
        """
        with self._lock:
            counts: Dict[str, int] = {"__shared__": self._shared_store.purge_expired()}
            for agent_id, store in self._agent_stores.items():
                counts[agent_id] = store.purge_expired()
            return counts

    def memory_stats(self) -> Dict[str, Any]:
        """Return a snapshot of memory usage across all stores."""
        with self._lock:
            stats: Dict[str, Any] = {
                "shared": {"entries": len(self._shared_store)},
                "agents": {},
            }
            for agent_id, store in self._agent_stores.items():
                cfg = self._agent_configs[agent_id]
                stats["agents"][agent_id] = {
                    "entries": len(store),
                    "max_entries": cfg.max_entries,
                }
            return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_registered(self, agent_id: str) -> None:
        if agent_id not in self._agent_stores:
            raise PermissionError(f"Agent '{agent_id}' is not registered.")

    def _require_read_permission(self, agent_id: str, namespace: str) -> None:
        if namespace not in self._read_permissions.get(agent_id, set()):
            raise PermissionError(
                f"Agent '{agent_id}' does not have read access to namespace '{namespace}'."
            )

    def _require_write_permission(self, agent_id: str, namespace: str) -> None:
        if namespace not in self._write_permissions.get(agent_id, set()):
            raise PermissionError(
                f"Agent '{agent_id}' does not have write access to namespace '{namespace}'."
            )
