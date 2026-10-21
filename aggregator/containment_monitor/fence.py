"""Ingest-then-release: confirmations name a journal position the monitor must reach.

agentd confirms each control command with ``journal``: its source epoch and the
sequence of the latest event it journaled once the command took effect. Every
event that passed the command's gates before then (a dispatch, a committed
effect, a publication) is journaled at or below that position, because the node
decides those events and applies commands under one control lock. A command is
therefore *fenced* (theory 7.1: everything it did not stop is in a log the
controller has read) only once the monitor has applied that node's events up to
the position. Releases of phase-1 holds wait for that, not only for the
confirmation.

Core ingests a node's events in any order, so the monitor keeps, per node and
source epoch, a contiguous watermark: the highest sequence up to which it has
applied every event, counted from the first sequence it saw for that epoch
(earlier history, collected before the monitor existed or expired at the node,
cannot be waited for). Events above a gap wait in ``ahead``. A gap that never
closes (an event lost before Core collected it) keeps every later command on
that node unfenced, which keeps the phase-1 holds; status shows the gap.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS containment_sources (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    through_seq INTEGER NOT NULL,
    ahead_json TEXT NOT NULL,
    PRIMARY KEY(node_id, source_epoch)
);
"""


class SourceProgress:
    """Per (node, source epoch): applied through ``through``, plus ``ahead``.

    Mutations happen inside the follower's batch transaction and are written by
    ``flush``; after a failed batch, ``reload`` restores the persisted state.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        db.executescript(SCHEMA_SQL)
        self._state: dict[tuple[str, str], tuple[int, set[int]]] = {}
        self._dirty: set[tuple[str, str]] = set()
        self.reload()

    def reload(self) -> None:
        self._state = {
            (node, epoch): (through, set(json.loads(ahead)))
            for node, epoch, through, ahead in self.db.execute(
                "SELECT node_id,source_epoch,through_seq,ahead_json FROM containment_sources"
            )
        }
        self._dirty = set()

    def observe(self, node_id: str, epoch: str, seq: int) -> None:
        key = (node_id, epoch)
        through, ahead = self._state.get(key, (seq - 1, set()))
        if seq <= through:
            return
        ahead.add(seq)
        while through + 1 in ahead:
            through += 1
            ahead.discard(through)
        self._state[key] = (through, ahead)
        self._dirty.add(key)

    def flush(self) -> None:
        for node_id, epoch in self._dirty:
            through, ahead = self._state[(node_id, epoch)]
            self.db.execute(
                "INSERT INTO containment_sources(node_id,source_epoch,through_seq,ahead_json) "
                "VALUES (?,?,?,?) ON CONFLICT(node_id,source_epoch) DO UPDATE SET "
                "through_seq=excluded.through_seq,ahead_json=excluded.ahead_json",
                (node_id, epoch, through, json.dumps(sorted(ahead))),
            )
        self._dirty = set()

    def covers(self, node_id: str, journal: Mapping[str, object] | None) -> bool:
        """Whether every event of the node up to the confirmed position is applied."""
        if journal is None:
            return True  # nothing was journaled under that identity
        seq = int(journal["source_seq"])  # type: ignore[call-overload]
        if seq == 0:
            return True
        state = self._state.get((node_id, str(journal["source_epoch"])))
        return state is not None and state[0] >= seq

    def status(self) -> dict[str, dict[str, dict[str, int]]]:
        out: dict[str, dict[str, dict[str, int]]] = {}
        for (node_id, epoch), (through, ahead) in sorted(self._state.items()):
            out.setdefault(node_id, {})[epoch] = {
                "through_seq": through,
                "ahead": len(ahead),
                "gap_from": through + 1 if ahead else 0,
            }
        return out
