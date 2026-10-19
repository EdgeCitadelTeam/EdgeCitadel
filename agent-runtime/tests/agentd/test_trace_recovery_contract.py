"""M2 reference semantics. These are NOT durable transport/recovery tests.

M4 must feed the same action corpus to real journals/exporters/collectors and
assert these outcomes across process failure, retention and restore.
"""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_event,
    validate_export_header,
    validate_settlement,
)

FIXTURE = json.loads(
    (Path(__file__).parents[1] / "fixtures/traces/recovery.v1.json").read_text()
)


class ReferenceRecovery:
    def __init__(self):
        self.events = {}
        self.receipts = {"g1": {}}
        self.selected = {"g1": set()}
        self.source_epochs = {}
        self.graph = {}
        self.unknown = False
        self.collector_epoch = "00000000-0000-4000-8000-000000000900"

    def step(self, action):
        op = action["op"]
        if op == "receive":
            value = deepcopy(FIXTURE["input_events"][action["event"]])
            seq, generation = action["seq"], action["generation"]
            assert (
                self.source_epochs.setdefault(generation, value["source_epoch"])
                == value["source_epoch"]
            )
            digest = hashlib.sha256(canonical_bytes(value)).hexdigest()
            wrapper = {
                "schema_version": 1,
                "node_id": value["node_id"],
                "source_epoch": value["source_epoch"],
                "export_generation": "00000000-0000-4000-8000-00000000080"
                + generation[-1],
                "export_seq": seq,
                "event_sha256": digest,
                "event": value,
            }
            validate_export_header(wrapper)
            self.selected.setdefault(generation, set()).add(seq)
            ledger = self.receipts.setdefault(generation, {})
            key = (value["node_id"], value["source_epoch"], value["event_id"])
            # An occupied source position cannot silently acquire new content.
            if seq in ledger:
                assert ledger[seq] == ("accepted", digest)
                return
            try:
                validate_event(value)
                if key in self.events and self.events[key][0] != digest:
                    raise TraceContractError("identity_conflict")
            except TraceContractError:
                ledger[seq] = ("rejected", digest)
            else:
                self.events[key] = (digest, value)
                ledger[seq] = ("accepted", digest)
        elif op == "durable_loss":
            for seq in range(action["first"], action["last"] + 1):
                assert seq not in self.receipts["g1"]
                self.receipts["g1"][seq] = ("lost", None)
                self.selected["g1"].add(seq)
        elif op == "broker_ack":
            self.selected["g1"].add(action["seq"])
        elif op == "project":
            self.graph = {value["task_id"]: value for _, value in self.events.values()}
        elif op == "collector_restore":
            self.events.clear()
            self.receipts = {key: {} for key in self.receipts}
            self.graph.clear()
            self.collector_epoch = "00000000-0000-4000-8000-000000000901"
        elif op == "coverage_unknown":
            self.unknown = True
        elif op in {"local_only", "broker_expire"}:
            pass  # Neither creates a Core receipt or advances an export position.
        else:
            raise AssertionError(f"Unknown fixture operation: {op}")

    def result(self):
        settled = {}
        for generation, ledger in self.receipts.items():
            through = 0
            while through + 1 in ledger:
                through += 1
            settled[generation] = through
            checkpoint = {
                "schema_version": 1,
                "node_id": "edge-a",
                "source_epoch": self.source_epochs.get(
                    generation, "00000000-0000-4000-8000-000000000100"
                ),
                "export_generation": "00000000-0000-4000-8000-00000000080"
                + generation[-1],
                "collector_epoch": self.collector_epoch,
                "settled_export_seq": through,
                "rejected_ranges": [
                    {"first": seq, "last": seq}
                    for seq in sorted(ledger)
                    if seq <= through and ledger[seq][0] == "rejected"
                ],
                "lost_ranges": [
                    {"first": seq, "last": seq}
                    for seq in sorted(ledger)
                    if seq <= through and ledger[seq][0] == "lost"
                ],
            }
            validate_settlement(checkpoint)
        rejected = sorted(
            seq for seq, item in self.receipts["g1"].items() if item[0] == "rejected"
        )
        lost = sorted(
            seq for seq, item in self.receipts["g1"].items() if item[0] == "lost"
        )
        unresolved = any(
            v["parent_task_id"] and v["parent_task_id"] not in self.graph
            for v in self.graph.values()
        )
        projection_pending = any(
            v["task_id"] not in self.graph for _, v in self.events.values()
        )
        catching = projection_pending or any(
            any(seq > settled[g] for seq in selected)
            for g, selected in self.selected.items()
        )
        return {
            "settled": settled,
            "graph_tasks": sorted(self.graph),
            "graph_edges": [
                {
                    "from": value["parent_task_id"],
                    "to": key,
                    "kind": "parent_task",
                    "status": "resolved"
                    if value["parent_task_id"] in self.graph
                    else "unresolved",
                }
                for key, value in sorted(self.graph.items())
                if value["parent_task_id"]
            ],
            "unresolved_parent": bool(unresolved),
            "unique_events": len(self.events),
            "rejected": rejected,
            "lost": lost,
            "task_states": {key: value["phase"] for key, value in self.graph.items()},
            "coverage": {
                "gap": bool(rejected or lost),
                "catching_up": catching,
                "unknown_sources": self.unknown,
                "partial": bool(unresolved),
            },
        }


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: case["id"])
def test_recovery_contract_case(case):
    model = ReferenceRecovery()
    for action in case["actions"]:
        model.step(action)
    assert model.result() == case["expected"]


def test_every_declared_family_fits_unsupported_coverage():
    path = Path(__file__).parents[1] / "fixtures/traces/events.v1.json"
    fixtures = json.loads(path.read_text())["fixtures"]
    event = deepcopy(next(f["event"] for f in fixtures if f["name"] == "coverage"))
    event["attributes"]["unsupported_families"] = [f["name"] for f in fixtures]
    validate_event(event)


def test_restore_changes_collector_epoch_but_projection_rebuild_does_not():
    model = ReferenceRecovery()
    model.step({"op": "receive", "event": "root", "seq": 1, "generation": "g1"})
    model.step({"op": "project"})
    epoch = model.collector_epoch
    model.graph.clear()  # Derived-only loss, raw journal retained.
    model.step({"op": "project"})
    assert model.collector_epoch == epoch
    assert model.result()["task_states"]
    model.step({"op": "collector_restore"})
    assert model.collector_epoch != epoch
    assert model.result()["settled"] == {"g1": 0}
    assert model.result()["coverage"]["catching_up"]
