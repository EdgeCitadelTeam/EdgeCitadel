"""Tests for the task-correlation projection schema."""

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "task-correlation.v1.json"

VALID_DIRECT = {
    "type": "command",
    "sender_id": "sender-1",
    "recipient_id": "worker-1",
    "task_id": "899d8a29-8c6c-4fef-b491-1140d8371fef",
    "context_id": "6e088543-c9de-4459-a0fe-2191d20dfba1",
    "hop_count": 0,
    "payload": {"command": "printf spine:nonce"},
}
VALID_CHILD = {
    "type": "delegation",
    "sender_id": "sender-1",
    "recipient_id": "worker-1",
    "task_id": "70209f19-a984-47e3-8637-44428ebd8318",
    "context_id": "6e088543-c9de-4459-a0fe-2191d20dfba1",
    "hop_count": 1,
    "payload": {
        "command": "printf child:nonce",
        "parent_task_id": "899d8a29-8c6c-4fef-b491-1140d8371fef",
    },
}
INVALID = [
    {**VALID_DIRECT, "task_id": "not-a-uuid"},
    {**VALID_DIRECT, "hop_count": 1},
    {**VALID_CHILD, "hop_count": 0},
    {**VALID_CHILD, "payload": {"command": "missing parent"}},
]


@pytest.fixture(scope="module")
def validator():
    schema = json.loads(SCHEMA_PATH.read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


@pytest.mark.parametrize("document", [VALID_DIRECT, VALID_CHILD])
def test_task_correlation_schema_accepts_valid_documents(validator, document):
    validator.validate(document)


@pytest.mark.parametrize("document", INVALID)
def test_task_correlation_schema_rejects_invalid_documents(validator, document):
    with pytest.raises(ValidationError):
        validator.validate(document)


def test_task_correlation_schema_rejects_unknown_projection_field(validator):
    with pytest.raises(ValidationError):
        validator.validate({**VALID_DIRECT, "timestamp": "not-projected"})


@pytest.mark.parametrize(
    "field",
    [
        "type",
        "sender_id",
        "recipient_id",
        "task_id",
        "context_id",
        "hop_count",
        "payload",
    ],
)
def test_task_correlation_schema_requires_every_projection_field(validator, field):
    document = {key: value for key, value in VALID_DIRECT.items() if key != field}

    with pytest.raises(ValidationError):
        validator.validate(document)


@pytest.mark.parametrize(
    "document",
    [
        {**VALID_DIRECT, "type": "heartbeat"},
        {**VALID_DIRECT, "hop_count": -1},
        {**VALID_DIRECT, "payload": []},
    ],
    ids=["unknown-type", "negative-hop", "non-object-payload"],
)
def test_task_correlation_schema_rejects_invalid_field_domains(validator, document):
    with pytest.raises(ValidationError):
        validator.validate(document)


def test_task_correlation_schema_requires_uuid_version_four(validator):
    with pytest.raises(ValidationError):
        validator.validate(
            {
                **VALID_DIRECT,
                "task_id": "899d8a29-8c6c-1fef-b491-1140d8371fef",
            }
        )


@pytest.mark.parametrize(
    "document",
    [
        {**VALID_DIRECT, "task_id": VALID_DIRECT["task_id"].upper()},
        {**VALID_DIRECT, "context_id": VALID_DIRECT["context_id"].upper()},
        {
            **VALID_CHILD,
            "payload": {
                **VALID_CHILD["payload"],
                "parent_task_id": VALID_CHILD["payload"]["parent_task_id"].upper(),
            },
        },
    ],
    ids=["task-id", "context-id", "parent-task-id"],
)
def test_task_correlation_schema_requires_lowercase_uuid4(validator, document):
    with pytest.raises(ValidationError):
        validator.validate(document)


@pytest.mark.parametrize("field", ["sender_id", "recipient_id"])
@pytest.mark.parametrize("suffix", ["\n", "\r"], ids=["lf", "cr"])
def test_task_correlation_schema_rejects_agent_id_trailing_line_ending(
    validator, field, suffix
):
    with pytest.raises(ValidationError):
        validator.validate({**VALID_DIRECT, field: f"{VALID_DIRECT[field]}{suffix}"})


def test_task_correlation_schema_rejects_positive_hop_command(validator):
    with pytest.raises(ValidationError):
        validator.validate(
            {
                **VALID_DIRECT,
                "hop_count": 1,
                "payload": {
                    **VALID_DIRECT["payload"],
                    "parent_task_id": VALID_DIRECT["task_id"],
                },
            }
        )


def test_task_correlation_schema_requires_positive_hop_delegation(validator):
    with pytest.raises(ValidationError):
        validator.validate(
            {
                **VALID_CHILD,
                "hop_count": 0,
                "payload": {"command": "missing parent at direct hop"},
            }
        )
