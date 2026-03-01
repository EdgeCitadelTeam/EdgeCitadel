"""
Thread-safe in-process memory store with size limits and eviction.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .types import EvictionPolicy, MemoryEntry, MemoryType


class MemoryStore:
    """A thread-safe key-value store designed for resource-constrained edge devices.

    Parameters
    ----------
    max_entries:
        Maximum number of entries the store may hold.  When the store is full
        and a new entry is written, one entry is evicted according to
        *eviction_policy*.  Use ``None`` for an unlimited store.
    eviction_policy:
        Algorithm used to choose a victim entry when the store is full.
        Defaults to :attr:`~EvictionPolicy.LRU`.
    """

    def __init__(
        self,
        max_entries: Optional[int] = 1000,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
    ) -> None:
        self._max_entries = max_entries
        self._eviction_policy = eviction_policy
        self._data: Dict[str, MemoryEntry] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def put(self, entry: MemoryEntry) -> None:
        """Insert or update *entry*.

        If the store is at capacity a victim is evicted before the new
        entry is inserted.
        """
        with self._lock:
            # Allow update-in-place without evicting
            if entry.key not in self._data:
                self._ensure_capacity()
            self._data[entry.key] = entry

    def get(self, key: str) -> Optional[MemoryEntry]:
        """Return the entry for *key*, or ``None`` if absent or expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                del self._data[key]
                return None
            entry.touch()
            return entry

    def delete(self, key: str) -> bool:
        """Remove the entry for *key*.  Return ``True`` if it existed."""
        with self._lock:
            return self._data.pop(key, None) is not None

    def contains(self, key: str) -> bool:
        """Return ``True`` if *key* exists and has not expired."""
        return self.get(key) is not None

    def purge_expired(self) -> int:
        """Remove all expired entries.  Return the number removed."""
        with self._lock:
            expired = [k for k, v in self._data.items() if v.is_expired()]
            for key in expired:
                del self._data[key]
            return len(expired)

    def keys(self) -> List[str]:
        """Return the list of non-expired keys."""
        with self._lock:
            self.purge_expired()
            return list(self._data.keys())

    def values(self) -> List[MemoryEntry]:
        """Return all non-expired entries."""
        with self._lock:
            self.purge_expired()
            return list(self._data.values())

    def items(self) -> List[Tuple[str, MemoryEntry]]:
        """Return all non-expired (key, entry) pairs."""
        with self._lock:
            self.purge_expired()
            return list(self._data.items())

    def by_type(self, memory_type: MemoryType) -> List[MemoryEntry]:
        """Return all non-expired entries of the given *memory_type*."""
        return [e for e in self.values() if e.memory_type == memory_type]

    def clear(self) -> None:
        """Remove all entries from the store."""
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_capacity(self) -> None:
        """Evict one entry if the store has reached *max_entries*."""
        if self._max_entries is None:
            return
        if len(self._data) < self._max_entries:
            return
        # Purge expired entries first; that may be enough
        self.purge_expired()
        if len(self._data) < self._max_entries:
            return
        victim_key = self._choose_victim()
        if victim_key is not None:
            del self._data[victim_key]

    def _choose_victim(self) -> Optional[str]:
        """Return the key of the entry to evict, or ``None`` if store is empty."""
        if not self._data:
            return None

        if self._eviction_policy == EvictionPolicy.LRU:
            return min(self._data, key=lambda k: self._data[k].last_accessed)

        if self._eviction_policy == EvictionPolicy.TTL_FIRST:
            # Entries without an expiry are considered immortal; evict last
            def ttl_key(k: str) -> float:
                exp = self._data[k].expires_at
                return exp if exp is not None else float("inf")

            return min(self._data, key=ttl_key)

        if self._eviction_policy == EvictionPolicy.LOWEST_PRIORITY:
            return min(self._data, key=lambda k: self._data[k].priority)

        # Fallback: evict the first entry
        return next(iter(self._data))
