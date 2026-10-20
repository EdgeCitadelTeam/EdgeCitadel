"""Deterministic content of the det-worker workload.

Every item a task outputs (an artifact version, a message, or the session state
it hands to its successor) carries content ``{"value": <16 hex>, "labels": [...]}``.
The value is a hash of the item id, the values of everything the task consumed
and the task's labels, so a wrong input changes every value downstream of it and
an independent checker can recompute the fault-free value from the workload
alone. Labels name the injected faults whose content reached the task; they are
the ground truth of content taint and are never read by the runtime or monitor.

The undeclared-action draws reproduce the simulator's hidden malfunction
(``contain.simulator.Overreach.draw``) bit for bit, so one workload instance
misbehaves identically in the simulator and on the runtime.
"""

from __future__ import annotations

import hashlib
import random
import zlib
from collections.abc import Iterable, Mapping
from typing import Any

Content = dict[str, Any]


def fault_label(fault_class: str, item: str) -> str:
    return f"fault:{fault_class}:{item}"


def labels_of(contents: Iterable[Mapping[str, Any]]) -> frozenset[str]:
    labels: set[str] = set()
    for content in contents:
        labels.update(content["labels"])
    return frozenset(labels)


def item_content(
    item_id: str, inputs: Iterable[Mapping[str, Any]], labels: Iterable[str]
) -> Content:
    """Content of one output item from the task's consumed inputs and labels."""
    values = sorted(str(content["value"]) for content in inputs)
    tags = sorted(set(labels))
    material = "\x1f".join([item_id, *values, "|", *tags])
    value = hashlib.sha256(material.encode()).hexdigest()[:16]
    return {"value": value, "labels": tags}


def external_content(item_id: str) -> Content:
    """A trusted external input (user task, configuration) seeded at run start."""
    return item_content(item_id, (), ())


def rogue_draw(
    seed: int, task_id: str, k: int, rate: float, p_eff: float
) -> tuple[bool, float, float, float, float] | None:
    """The simulator's pre-sampled malfunction draw for (task, step boundary k).

    Returns None when no undeclared action happens, else ``(is_effect, u1, u2,
    u3, u4)``: u1 picks the effect class or consumer agent, u2 the extra commit
    delay, u3 the receiving live task, u4 whether a message spawns a new
    invocation. Identical to ``Overreach.draw`` for the same seed and ids.
    """
    if rate <= 0:
        return None
    rng = random.Random(zlib.crc32(f"{seed}|{task_id}|{k}".encode()))
    if rng.random() >= rate:
        return None
    return (
        rng.random() < p_eff,
        rng.random(),
        rng.random(),
        rng.random(),
        rng.random(),
    )
