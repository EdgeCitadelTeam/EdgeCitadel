"""
Core data types for the EdgeCitadel memory coordination system.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class MemoryType(Enum):
    """Categories of memory available to agents."""

    # Short-lived operational memory for the current task
    WORKING = "working"
    # Record of past events and interactions
    EPISODIC = "episodic"
    # General facts and domain knowledge
    SEMANTIC = "semantic"
    # Memory explicitly shared between multiple agents
    SHARED = "shared"


class EvictionPolicy(Enum):
    """Strategy used to free space when a store is full."""

    # Remove the entry that was least recently accessed
    LRU = "lru"
    # Remove the entry with the earliest expiry first
    TTL_FIRST = "ttl_first"
    # Remove the entry with the lowest priority first
    LOWEST_PRIORITY = "lowest_priority"


@dataclass
class MemoryEntry:
    """A single item held in a memory store."""

    key: str
    value: Any
    memory_type: MemoryType = MemoryType.WORKING

    # Relative importance (higher = more important; used by eviction)
    priority: int = 0

    # Monotonic time (seconds) at which the entry was created
    created_at: float = field(default_factory=time.monotonic)

    # Monotonic time (seconds) at which the entry was last read or written
    last_accessed: float = field(default_factory=time.monotonic)

    # Optional absolute expiry as a monotonic time value (seconds from
    # time.monotonic() reference).  None means the entry never expires.
    expires_at: Optional[float] = None

    # Identifier of the agent that owns this entry
    owner: Optional[str] = None

    def is_expired(self) -> bool:
        """Return True if this entry has passed its expiry time."""
        if self.expires_at is None:
            return False
        return time.monotonic() >= self.expires_at

    def touch(self) -> None:
        """Update *last_accessed* to the current time."""
        self.last_accessed = time.monotonic()

    @classmethod
    def with_ttl(
        cls,
        key: str,
        value: Any,
        ttl_seconds: float,
        **kwargs: Any,
    ) -> "MemoryEntry":
        """Convenience constructor that converts a TTL into an absolute expiry."""
        now = time.monotonic()
        return cls(
            key=key,
            value=value,
            expires_at=now + ttl_seconds,
            created_at=now,
            last_accessed=now,
            **kwargs,
        )
