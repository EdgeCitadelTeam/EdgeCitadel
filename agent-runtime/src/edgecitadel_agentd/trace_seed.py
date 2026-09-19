"""Atomic physical reservation of a source's existing unfinished work."""

from __future__ import annotations

from typing import TYPE_CHECKING

from . import trace_reservations
from .trace_contract import TraceContractError
from .trace_reservations import Obligation, reserve
from .trace_task_completion import execution_obligation

if TYPE_CHECKING:
    from .store import AgentdStore
    from .storage_workspace import ReservedConnection

_TERMINAL = ("completed", "failed", "rejected", "cancelled", "expired", "undeliverable")


def _required(
    db: ReservedConnection, occupied: dict[tuple[str, str, str], bool]
) -> set[Obligation]:
    required: set[Obligation] = set()

    def add(obligation: Obligation, *, first_transition: bool = False) -> None:
        if occupied.get(obligation.key, False):
            if first_transition:
                # A prior cycle's immutable record remains authoritative. Later
                # offered/accepted admission reserves its execution boundary.
                return
            raise TraceContractError("completion_state_conflict")
        required.add(obligation)
        if len(required) > trace_reservations.MAX_SLOTS:
            raise TraceContractError("quota_exceeded")

    limit = trace_reservations.MAX_SLOTS + 1
    for row in db.execute(
        "SELECT connector_id FROM connectors WHERE revoked_at_ms IS NULL LIMIT ?",
        (limit,),
    ):
        add(Obligation("connector", row[0], "revoke"))
    for row in db.execute(
        "SELECT session_id FROM sessions WHERE closed_at_ms IS NULL LIMIT ?", (limit,)
    ):
        add(Obligation("session", row[0], "close"))
    for row in db.execute(
        f"SELECT task_id,state,claimed_session_id FROM tasks WHERE state NOT IN ({','.join('?' for _ in _TERMINAL)}) LIMIT ?",
        (*_TERMINAL, limit),
    ):
        task_id, state, session_id = row
        if state not in {"created", "queued", "offered", "accepted", "running"}:
            raise TraceContractError("invalid_task_reservation_state")
        add(Obligation("task", task_id, "terminal"))
        if state in {"created", "queued", "offered"}:
            phases = (
                ("offered", "accepted", "running")
                if state != "offered"
                else ("accepted", "running")
            )
            for phase in phases:
                add(Obligation("task", task_id, phase), first_transition=True)
        elif state == "accepted":
            add(
                execution_obligation(db, task_id, session_id)
                or Obligation("task", task_id, "running")
            )
    for row in db.execute(
        "SELECT binding_id FROM trace_bindings_all WHERE closed_at_ms IS NULL LIMIT ?",
        (limit,),
    ):
        add(Obligation("run", row[0], "terminal"))
    for row in db.execute(
        "SELECT span_id FROM trace_operations_all WHERE terminal_event_id IS NULL LIMIT ?",
        (limit,),
    ):
        add(Obligation("operation", row[0], "terminal"))
    return required


def seed_existing_work(store: AgentdStore) -> dict[str, int]:
    """Reserve all remaining obligations or leave admission and facts unchanged.

    Caller has opened the paired store under workspace ownership, installed the
    physical workspace, and has not opened admission. This function owns its
    ordinary allocation transaction; it never borrows completion capacity or
    changes task/session state, event identity, counters, or filled records.
    Every production handle must eventually use this preparation boundary;
    calling it alone does not fence a concurrently running old unreserved writer.
    """
    db = store._connection
    with store._lock:
        if db.workspace is None or db.in_transaction:
            raise TraceContractError("invalid_reservation_seed_boundary")
        with db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                "SELECT owner_kind,owner_id,purpose,filled FROM trace_completion_slots WHERE owner_id<>''"
            ).fetchall()
            occupied = {(row[0], row[1], row[2]): bool(row[3]) for row in rows}
            required = _required(db, occupied)
            missing = [item for item in required if item.key not in occupied]
            if len(occupied) + len(missing) > trace_reservations.MAX_SLOTS:
                raise TraceContractError("quota_exceeded")
            for obligation in sorted(required, key=lambda item: item.key):
                reserve(db, obligation)
            return {
                "required_pending": len(required),
                "newly_reserved": len(missing),
                "occupied": len(occupied) + len(missing),
                "completed": sum(occupied.values()),
            }
