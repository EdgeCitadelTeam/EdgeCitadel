"""Differential schema checks keep specialization equivalent to the full schema."""

from copy import deepcopy

from jsonschema import Draft202012Validator, FormatChecker
from test_trace_contract import FIXTURES

from edgecitadel_agentd.trace_contract import (
    _EVENT_VALIDATORS,
    _VALIDATORS,
    _compile_event_validators,
)


def selected(value):
    family = value.get("kind")
    return (
        _EVENT_VALIDATORS.get(family, _VALIDATORS["event"])
        if isinstance(family, str)
        else _VALIDATORS["event"]
    )


def test_valid_fixtures_and_mutated_fields_match_full_validator():
    baseline = _VALIDATORS["event"]
    values = (
        None,
        True,
        False,
        -1,
        0,
        1.5,
        "",
        "unexpected",
        [],
        {},
        *_EVENT_VALIDATORS,
    )
    compared = 0
    for fixture in FIXTURES:
        original = fixture["event"]
        assert selected(original).is_valid(original) == baseline.is_valid(original)
        for key in original:
            missing = {k: v for k, v in original.items() if k != key}
            assert selected(missing).is_valid(missing) == baseline.is_valid(missing)
            compared += 1
            for value in values:
                candidate = {**original, key: value}
                assert selected(candidate).is_valid(candidate) == baseline.is_valid(
                    candidate
                )
                compared += 1
        for key in original["attributes"]:
            for value in values:
                candidate = deepcopy(original)
                candidate["attributes"][key] = value
                assert selected(candidate).is_valid(candidate) == baseline.is_valid(
                    candidate
                )
                compared += 1
        extra = {**original, "unexpected": "sentinel"}
        assert not selected(extra).is_valid(extra)
    assert compared >= 3000


def test_unrecognized_conditions_and_else_branches_remain_enforced():
    schema = deepcopy(_VALIDATORS["event"].schema)
    schema["allOf"].extend(
        [
            {
                "if": {"properties": {"kind": {"const": "task"}}},
                "then": {"properties": {"phase": {"const": "created"}}},
                "else": {"properties": {"phase": {"const": "impossible"}}},
            },
            {
                "if": {"properties": {"phase": {"const": "created"}}},
                "then": {"properties": {"attributes": {"required": ["future_field"]}}},
            },
        ]
    )
    full = Draft202012Validator(schema, format_checker=FormatChecker())
    specialized = _compile_event_validators(schema)
    for fixture in FIXTURES:
        event = fixture["event"]
        assert specialized[event["kind"]].is_valid(event) == full.is_valid(event)
        assert not specialized[event["kind"]].is_valid(event)
