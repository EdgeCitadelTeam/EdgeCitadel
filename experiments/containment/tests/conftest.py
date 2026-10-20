"""Shared fixtures: a small workload and paths to the harness and det-worker."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from harness import _worker  # noqa: E402,F401  (puts det_worker on sys.path)
from harness import groundtruth as G  # noqa: E402
from harness import spec as S  # noqa: E402

SIM_ROOT = os.environ.get("CONTAIN_SIM_ROOT")


def mini_document(**fault: object) -> dict:
    """Root r delegates to teams a and b; a1 publishes X, read by b1 across teams.

    r: R.plan -> (dA to a1.t0, dB to b1.t0) ... R.agg receives rA, rB
    a1: A.t0 (produces X) -> A.t1 (session)   b1: B.t0 (reads X, effect E_B)
    """
    return {
        "spec_version": 1,
        "name": "mini",
        "agents": [
            {"id": "r", "parent": None, "site": "s-r"},
            {"id": "a1", "parent": "r", "site": "s-a"},
            {"id": "b1", "parent": "r", "site": "s-b"},
        ],
        "placement": {"s-r": "core", "s-a": "edge-1", "s-b": "edge-2"},
        "effect_classes": [
            {"id": "act_b", "site": "s-b", "min_delay": 0.5},
            {"id": "final", "site": "s-r", "min_delay": 0.2},
        ],
        "external_inputs": ["task"],
        "tasks": [
            {
                "id": "R.plan",
                "agent": "r",
                "session": "sR",
                "compute": 1.0,
                "reads": ["task"],
            },
            {"id": "A.t0", "agent": "a1", "session": "sA", "compute": 3.0},
            {"id": "A.t1", "agent": "a1", "session": "sA", "compute": 2.0},
            {
                "id": "B.t0",
                "agent": "b1",
                "session": "sB",
                "compute": 4.0,
                "reads": ["X@v1"],
            },
            {"id": "B.n", "agent": "b1", "session": "sBn", "compute": 6.0},
            {"id": "R.agg", "agent": "r", "session": "sR", "compute": 1.0},
        ],
        "artifacts": [{"id": "X@v1", "producer": "A.t0"}],
        "messages": [
            {"id": "dA", "sender": "R.plan", "receiver": "A.t0", "latency": 0.1},
            {"id": "dB", "sender": "R.plan", "receiver": "B.t0", "latency": 0.1},
            {"id": "dBn", "sender": "R.plan", "receiver": "B.n", "latency": 0.1},
            {"id": "rA", "sender": "A.t1", "receiver": "R.agg", "latency": 0.1},
            {"id": "rB", "sender": "B.t0", "receiver": "R.agg", "latency": 0.1},
            {"id": "rBn", "sender": "B.n", "receiver": "R.agg", "latency": 0.1},
        ],
        "effects": [
            {"id": "E_B", "issuer": "B.t0", "class": "act_b", "delay": 1.0},
            {"id": "E_Bn", "issuer": "B.n", "class": "act_b", "delay": 1.0},
            {"id": "FINAL", "issuer": "R.agg", "class": "final", "delay": 0.5},
        ],
        "fault": dict(fault) or {"class": "wrong_value", "item": "X@v1"},
        "alerts": {
            "early": {"event": "produce", "item": "X@v1", "delay_s": 0.2},
            "late": {"event": "consume", "item": "X@v1", "count": 1, "delay_s": 0.2},
        },
        "rogue": {"rate": 0.0, "seed": 3},
    }


@pytest.fixture
def mini_spec() -> S.Spec:
    return S.from_dict(mini_document())


@pytest.fixture
def sim_root() -> str:
    if not SIM_ROOT:
        pytest.skip("set CONTAIN_SIM_ROOT to the offline simulator checkout")
    return SIM_ROOT


class Records:
    """Worker events and ledger records of one synthetic run (times in ms)."""

    def __init__(self, spec: S.Spec) -> None:
        self.spec = spec
        self.expected = G.expected_contents(spec)
        self.events: list[dict] = []
        self.ledger: list[dict] = []

    def event(
        self, kind: str, task: str, t: int, inv_kind: str = "task", **data
    ) -> None:
        self.events.append(
            {
                "kind": kind,
                "task": task,
                "rt": f"rt-{task}",
                "inv_kind": inv_kind,
                "t_ms": t,
                **data,
            }
        )

    def compute(self, task: str, start: int, labels: tuple[str, ...] = ()) -> None:
        self.event("start", task, start - 10)
        self.event("compute_start", task, start, labels=list(labels))

    def produce(self, task: str, t: int, labels: tuple[str, ...] = ()) -> None:
        derived = S.derive(self.spec)
        for item in derived.outputs.get(task, ()):
            value = self.expected[item]["value"] if not labels else "f" * 16
            self.event("produce", task, t, item=item, value=value, labels=list(labels))
        self.event("produced", task, t)
        for d in derived.deliveries_out.get(task, ()):
            self.event(
                "dispatch",
                task,
                t,
                purpose="deliver",
                item=d.item,
                target=d.target,
                code=None,
            )
        for child in derived.creates.get(task, ()):
            item = derived.creation[child].item
            self.event(
                "dispatch",
                task,
                t,
                purpose="create",
                item=item,
                target=child,
                code=None,
            )

    def effect(
        self,
        effect_id: str,
        issuer: str,
        t: int,
        labels: tuple[str, ...] = (),
        cls: str = "act_b",
    ) -> None:
        value = self.expected[effect_id]["value"] if not labels else "f" * 16
        for phase in ("attempted", "committed"):
            self.ledger.append(
                {
                    "effect_id": effect_id,
                    "issuer": issuer,
                    "class": cls,
                    "declared": True,
                    "origin": "declared",
                    "labels": list(labels),
                    "value": value,
                    "phase": phase,
                    "t_ms": t,
                }
            )
