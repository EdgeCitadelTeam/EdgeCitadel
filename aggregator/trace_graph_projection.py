"""Explicit graph identities and ancestry resolution for projected observations.

No timestamp or task terminal state supplies a missing relationship. Logical
native roots, execution attempts and source-local spans have distinct identities.
Relationship claims are immutable; resolution is derived at the selected snapshot.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def _key(prefix: str, *parts: str) -> str:
    encoded = json.dumps(parts, separators=(",", ":")).encode()
    return prefix + ":" + hashlib.sha256(encoded).hexdigest()


def local_id(event: dict, family: str, identity: str) -> str:
    if family in {"span", "dispatch", "permission"}:
        # A caller may reuse an operation/request UUID in another authorized
        # binding. Source identity alone does not identify that operation.
        return _key(
            family,
            event["node_id"],
            event["source_epoch"],
            event["trace_id"],
            event["execution_attempt_id"] or "",
            identity,
            *([event["attributes"]["policy"]] if family == "permission" else []),
        )
    return _key(family, event["node_id"], event["source_epoch"], identity)


def edge(kind: str, parent: str, child: str) -> dict:
    return {
        "id": _key("edge", kind, parent, child),
        "kind": kind,
        "from": parent,
        "to": child,
    }


def entity_claims(event: dict) -> list[dict]:
    """Extract graph entities from an already validated immutable event.

    A missing execution identity is represented by that observation's own key;
    unrelated observations cannot be fused into an invented attempt. Permission
    decisions have their own nodes, distinct from dispatch outcomes.
    """
    kind = event["kind"]
    if kind not in {"run", "model", "tool", "dispatch", "permission"}:
        return []
    attempt = event["execution_attempt_id"]
    if kind == "run":
        identity = (
            local_id(event, "attempt", attempt)
            if attempt
            else local_id(event, "run_observation", event["event_id"])
        )
    elif kind in {"model", "tool"}:
        identity = local_id(event, "span", event["span_id"])
    else:
        identity = local_id(event, kind, event["attributes"]["dispatch_id"])
    identities = [identity]
    if (
        kind == "run"
        and event["task_id"] is None
        and event["parent_task_id"] is None
        and event["parent_run_id"] is None
    ):
        identities.insert(0, "run:" + event["trace_id"])
    phase = event["phase"]
    terminal = phase not in {"started", "requested"}
    # Dispatch 'allowed' is an authorization result, not proof of task execution.
    state = {"started": "running", "requested": "waiting"}.get(phase, phase)
    operation = event["attributes"].get("name")
    if kind == "permission":
        operation = event["attributes"]["policy"]
    return [
        {
            "id": identity,
            "terminal": terminal,
            "node": {
                "id": identity,
                "kind": kind,
                "task_id": event["task_id"],
                "agent_id": event["agent_id"],
                "state": state,
                "original_state": None,
                "operation": operation,
                "evidence_kind": event["evidence_kind"],
                "conflict": False,
                "outcome_candidate_count": int(terminal),
            },
        }
        for identity in identities
    ]


def relationship_claims(event: dict, entities: list[dict]) -> list[dict]:
    """Produce only relationships supported by explicit identity fields."""
    kind = event["kind"]
    if kind == "task":
        child = "task:" + event["task_id"]
        if event["parent_task_id"]:
            return [edge("parent_task", "task:" + event["parent_task_id"], child)]
        if event["parent_run_id"]:
            return [edge("parent_run", "run:" + event["parent_run_id"], child)]
        return []
    if kind == "link":
        attrs = event["attributes"]
        return [
            edge("join", "task:" + attrs["from_task_id"], "task:" + attrs["to_task_id"])
        ]
    if not entities:
        return []
    child = entities[-1]["id"]
    if kind == "run":
        if event["task_id"]:
            return [edge("parent_task", "task:" + event["task_id"], child)]
        if len(entities) == 2:
            return [edge("parent_run", entities[0]["id"], child)]
        if event["parent_task_id"]:
            return [edge("parent_task", "task:" + event["parent_task_id"], child)]
        if event["parent_run_id"]:
            return [edge("parent_run", "run:" + event["parent_run_id"], child)]
    elif kind == "permission":
        return [
            edge(
                "span_parent",
                local_id(event, "dispatch", event["attributes"]["dispatch_id"]),
                child,
            )
        ]
    elif event["parent_span_id"]:
        return [
            edge("span_parent", local_id(event, "span", event["parent_span_id"]), child)
        ]
    elif event["execution_attempt_id"]:
        return [
            edge(
                "span_parent",
                local_id(event, "attempt", event["execution_attempt_id"]),
                child,
            )
        ]
    elif event["task_id"]:
        return [edge("parent_task", "task:" + event["task_id"], child)]
    return []


def unresolved_node(identity: str) -> dict:
    return {
        "id": identity,
        "kind": "unresolved",
        "task_id": identity.removeprefix("task:")
        if identity.startswith("task:")
        else None,
        "agent_id": None,
        "state": "unknown",
        "original_state": None,
        "operation": None,
        "evidence_kind": None,
        "conflict": False,
        "outcome_candidate_count": 0,
    }


def resolve_graph(nodes: list[dict], claims: list[dict]) -> dict[str, Any]:
    """Resolve a bounded materialized snapshot; never drop unknown endpoints.

    Joins are not ancestry. Multiple parents invalidate every competing claim;
    cycles invalidate each cycle edge, not whichever edge happened to arrive last.
    """
    if len(nodes) > 500 or len(claims) > 1000:
        raise ValueError("projection_graph_expansion_required")
    by_id = {node["id"]: node for node in nodes}
    if len(by_id) != len(nodes):
        raise ValueError("duplicate_projection_node")
    parents: dict[str, set[str]] = {}
    for claim in claims:
        if claim["kind"] != "join":
            parents.setdefault(claim["to"], set()).add(claim["from"])
    ambiguous = {child for child, values in parents.items() if len(values) > 1}
    parent = {
        child: next(iter(values))
        for child, values in parents.items()
        if child not in ambiguous
    }
    cyclic: set[str] = set()
    done: set[str] = set()
    for start in parent:
        positions: dict[str, int] = {}
        path: list[str] = []
        current = start
        while current in parent and current not in done:
            if current in positions:
                cyclic.update(path[positions[current] :])
                break
            positions[current] = len(path)
            path.append(current)
            current = parent[current]
        done.update(path)
    edges = []
    for claim in claims:
        known = all(
            endpoint in by_id and by_id[endpoint]["kind"] != "unresolved"
            for endpoint in (claim["from"], claim["to"])
        )
        invalid = claim["kind"] != "join" and (
            claim["to"] in ambiguous or claim["to"] in cyclic
        )
        status = "invalid" if invalid else "resolved" if known else "unresolved"
        edges.append({**claim, "status": status})
        for endpoint in (claim["from"], claim["to"]):
            if endpoint not in by_id:
                by_id[endpoint] = unresolved_node(endpoint)
    if len(by_id) > 500 or len(edges) > 1000:
        raise ValueError("projection_graph_expansion_required")
    return {
        "nodes": sorted(by_id.values(), key=lambda node: node["id"]),
        "edges": sorted(edges, key=lambda item: item["id"]),
        "unresolved_ancestry": any(item["status"] != "resolved" for item in edges),
    }
