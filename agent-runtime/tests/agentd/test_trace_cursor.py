from __future__ import annotations

from dataclasses import replace

import pytest

from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_cursor import (
    CursorScope,
    cursor_scope_hash,
    decode_cursor,
    encode_cursor,
)

KEY = b"owned-cursor-fixture-key-32-bytes!!"
GENERATION = "00000000-0000-4000-8000-000000000001"


def test_scope_hash_is_stable_but_changes_with_policy_or_filter() -> None:
    filters = {"agent_id": "worker-a", "outcome": None}
    policy = {"mode": "trusted_fleet", "policy_version": 1}
    expected = cursor_scope_hash(filters, policy)
    assert cursor_scope_hash(dict(reversed(list(filters.items()))), policy) == expected
    assert cursor_scope_hash({**filters, "agent_id": "worker-b"}, policy) != expected
    assert cursor_scope_hash(filters, {**policy, "policy_version": 2}) != expected


def claims(kind: str) -> dict:
    return {
        "schema_version": 1,
        "kind": kind,
        "trace_id": None if kind == "list" else "a" * 32,
        "scope_hash": "b" * 64,
        "projection_generation": GENERATION,
        "snapshot": 8,
        "position": 8 if kind == "graph" else 5,
        "upper": 8 if kind in {"list", "changes", "history"} else 20,
        "key": "before:2"
        if kind == "history"
        else "c" * 32
        if kind == "list"
        else "task:child"
        if kind == "expansion"
        else None,
    }


def scope(value: dict) -> CursorScope:
    return CursorScope(
        **{
            field: value[field]
            for field in ("kind", "trace_id", "scope_hash", "projection_generation")
        }
    )


@pytest.mark.parametrize(
    "kind", ["list", "graph", "events", "changes", "expansion", "history"]
)
def test_cursor_roundtrip_keeps_distinct_positions_and_snapshot(kind: str) -> None:
    value = claims(kind)
    token = encode_cursor(value, KEY)
    assert decode_cursor(token, KEY, scope(value), retained_from=5) == value


def test_tampering_wrong_key_and_noncanonical_encoding_fail() -> None:
    value = claims("events")
    token = encode_cursor(value, KEY)
    for invalid in (
        token[:-1] + ("A" if token[-1] != "A" else "B"),
        token + "=",
        token + "\n",
        "x" * 4097,
        "not.a.cursor",
    ):
        with pytest.raises(TraceContractError, match="^invalid_cursor$"):
            decode_cursor(invalid, KEY, scope(value), retained_from=0)
    with pytest.raises(TraceContractError, match="^invalid_cursor$"):
        decode_cursor(token, b"wrong-key" * 4, scope(value), retained_from=0)


@pytest.mark.parametrize(
    "field,new", [("kind", "changes"), ("trace_id", "d" * 32), ("scope_hash", "e" * 64)]
)
def test_cursor_cannot_cross_type_trace_filter_or_access_scope(
    field: str, new: str
) -> None:
    value = claims("events")
    with pytest.raises(TraceContractError, match="^cursor_scope_mismatch$"):
        decode_cursor(
            encode_cursor(value, KEY),
            KEY,
            replace(scope(value), **{field: new}),
            retained_from=0,
        )


def test_rebuild_and_retention_require_resnapshot() -> None:
    value = claims("graph")
    with pytest.raises(TraceContractError, match="^generation_changed$"):
        decode_cursor(
            encode_cursor(value, KEY),
            KEY,
            replace(
                scope(value),
                projection_generation="00000000-0000-4000-8000-000000000002",
            ),
            retained_from=0,
        )
    with pytest.raises(TraceContractError, match="^history_expired$"):
        decode_cursor(encode_cursor(value, KEY), KEY, scope(value), retained_from=9)
    value = claims("changes")
    with pytest.raises(TraceContractError, match="^history_expired$"):
        decode_cursor(encode_cursor(value, KEY), KEY, scope(value), retained_from=6)


@pytest.mark.parametrize(
    "kind,field,value",
    [
        ("graph", "position", 7),
        ("events", "position", 21),
        ("changes", "upper", 9),
        ("list", "key", None),
        ("expansion", "key", None),
        ("events", "position", True),
    ],
)
def test_invalid_cursor_claims_are_not_signed(
    kind: str, field: str, value: object
) -> None:
    document = claims(kind)
    document[field] = value
    with pytest.raises(TraceContractError, match="^invalid_cursor$"):
        encode_cursor(document, KEY)


def test_short_signing_key_is_configuration_error() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        encode_cursor(claims("events"), b"short")


@pytest.mark.parametrize(
    "overrides",
    [
        {"key": "start:2"},
        {"key": "before:6"},
        {"key": "before:9"},
        {"key": None},
        {"upper": 9},
    ],
)
def test_history_cursor_cannot_misrepresent_its_frozen_range(overrides: dict) -> None:
    with pytest.raises(TraceContractError):
        encode_cursor({**claims("history"), **overrides}, KEY)
