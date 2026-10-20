"""Cost model v0 for control actions (additive across actions).

``cost(a) = c0 + sum(value(w) for w in NR(a)) + revocation term``

NR(a) is the live work that can no longer finish with all its known inputs if a is
applied: a held unit (running invocation or pending task) and, transitively, the
live parent invocations waiting for its result; a result parked at the parent's
node stops that parent. ``value(w)`` is the agent's configured weight, or
``w_tainted`` times it for work already in the declared closure of the taint
(Taint_est). Revocations stop no recorded work; they cost ``revoke_weight`` per
live normal invocation they restrict (the future work they may starve): an egress
block restricts one invocation, a grant or credential revocation every live
invocation of the agent that could use it, an admission block the target agent's.
"""

from __future__ import annotations

from collections.abc import Mapping

from .config import MonitorConfig
from .model import HOLD, Action


class CostModel:
    def __init__(
        self,
        units: Mapping[str, object],
        taint_est: set[str],
        config: MonitorConfig,
        *,
        result_parent: Mapping[str, str],
        effect_issuer: Mapping[str, str],
    ):
        self.units = units
        self.taint_est = taint_est
        self.config = config
        self.result_parent = result_parent
        self.effect_issuer = effect_issuer
        self.by_task = {u.task_id: key for key, u in units.items() if u.task_id}
        self._cache: dict[Action, tuple[float, frozenset[str]]] = {}

    def value(self, key: str) -> float:
        weight = self.config.value(self.units[key].agent)
        return weight * (self.config.w_tainted if key in self.taint_est else 1.0)

    def closure(self, keys: set[str]) -> set[str]:
        out = set()
        for key in keys:
            while key is not None and key in self.units and key not in out:
                out.add(key)
                key = self.units[key].parent
        return out

    def stopped(self, action: Action) -> frozenset[str]:
        return self._evaluate(action)[1]

    def __call__(self, action: Action) -> float:
        return self._evaluate(action)[0]

    def _evaluate(self, action: Action) -> tuple[float, frozenset[str]]:
        cached = self._cache.get(action)
        if cached is not None:
            return cached
        seeds: set[str] = set()
        extra = 0.0
        if action.op == HOLD and action.kind == "task":
            key = self.by_task.get(action.object_id)
            if key is not None and self.units[key].node == action.node_id:
                seeds = {key}  # paused at its own node
            else:
                parent = self.units[key].parent if key is not None else None
                parent = parent or self.result_parent.get(action.object_id)
                seeds = {parent} if parent is not None else set()
        elif action.op == HOLD and action.kind == "effect":
            issuer = self.effect_issuer.get(action.object_id)
            seeds = {issuer} if issuer in self.units else set()
        elif action.kind == "egress":
            key = self.by_task.get(action.object_id)
            extra = self.config.revoke_weight * (
                key is not None and key not in self.taint_est
            )
        elif action.kind in {"grant", "credential", "admission"}:
            agent = action.object_id.split(">", 1)[0]
            target = (
                action.object_id.split(">", 1)[1] if ">" in action.object_id else None
            )
            affected = [
                key
                for key, unit in self.units.items()
                if unit.agent == agent
                and key not in self.taint_est
                and (target is None or target.startswith("fx:") or unit.reaches(target))
            ]
            extra = self.config.revoke_weight * len(affected)
        stopped = frozenset(self.closure(seeds))
        cost = self.config.c0 + extra + sum(self.value(key) for key in stopped)
        self._cache[action] = (cost, stopped)
        return self._cache[action]
