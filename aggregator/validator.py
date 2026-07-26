"""Envelope and Agent Card validation against vendored schemas."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as JSONSchemaError

CORRELATED_TYPES = frozenset({"command", "delegation", "cancel", "result"})

_CORRELATION_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "schemas" / "task-correlation.v1.json"
)
_CORRELATION_VALIDATOR = Draft202012Validator(
    json.loads(_CORRELATION_SCHEMA_PATH.read_text()),
    format_checker=FormatChecker(),
)


class ValidationError(Exception):
    pass


def _error_sort_key(error: JSONSchemaError) -> tuple[Any, ...]:
    return (
        tuple(str(part) for part in error.absolute_path),
        tuple(str(part) for part in error.absolute_schema_path),
        error.message,
    )


def _validate(
    validator: Draft202012Validator,
    document: object,
    label: str,
) -> None:
    errors = sorted(validator.iter_errors(document), key=_error_sort_key)
    if errors:
        error = errors[0]
        raise ValidationError(
            f"{label} invalid: {error.message} at {list(error.absolute_path)}"
        ) from error


def _is_delegated(envelope: Mapping[str, object]) -> bool:
    payload = envelope.get("payload")
    has_parent = isinstance(payload, Mapping) and "parent_task_id" in payload
    hop_count = envelope.get("hop_count")
    has_positive_hop = (
        isinstance(hop_count, int)
        and not isinstance(hop_count, bool)
        and hop_count > 0
    )
    return envelope.get("type") == "delegation" or (
        envelope.get("type") == "result" and (has_parent or has_positive_hop)
    )


def normalize_task_correlation(
    envelope: Mapping[str, object],
) -> dict[str, object]:
    if not isinstance(envelope, Mapping):
        raise ValidationError(
            "task_correlation invalid: envelope must be a mapping"
        )

    missing = [
        field
        for field in ("type", "sender_id", "recipient_id", "task_id", "payload")
        if field not in envelope
    ]
    if missing:
        raise ValidationError(
            "task_correlation invalid: missing required "
            + ", ".join(missing)
        )

    payload = envelope["payload"]
    if not isinstance(payload, Mapping):
        raise ValidationError(
            "task_correlation invalid: payload must be a mapping"
        )

    hop_count = envelope.get("hop_count", 0)
    if type(hop_count) is not int:
        raise ValidationError(
            "task_correlation invalid: hop_count must be an integer"
        )
    if envelope["type"] == "command" and hop_count != 0:
        raise ValidationError(
            "task_correlation invalid: command hop_count must be 0"
        )

    if _is_delegated(envelope):
        missing_lineage = [
            field
            for field in ("context_id", "hop_count")
            if field not in envelope
        ]
        if "parent_task_id" not in payload:
            missing_lineage.append("parent_task_id")
        if missing_lineage:
            raise ValidationError(
                "task_correlation invalid: delegated task requires explicit "
                + ", ".join(missing_lineage)
            )
        if hop_count < 1:
            raise ValidationError(
                "task_correlation invalid: delegated task hop_count must be >= 1"
            )

    projected = {
        "type": envelope["type"],
        "sender_id": envelope["sender_id"],
        "recipient_id": envelope["recipient_id"],
        "task_id": envelope["task_id"],
        "context_id": envelope.get("context_id", envelope["task_id"]),
        "hop_count": hop_count,
        "payload": dict(payload),
    }
    return projected


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def request_fingerprint(envelope: Mapping[str, object]) -> str:
    correlated = normalize_task_correlation(envelope)
    if correlated["type"] not in ("command", "delegation"):
        raise ValidationError(
            "request_fingerprint invalid: type must be command or delegation"
        )
    _validate(_CORRELATION_VALIDATOR, correlated, "task_correlation")
    value = {
        "type": correlated["type"],
        "sender_id": correlated["sender_id"],
        "recipient_id": correlated["recipient_id"],
        "task_id": correlated["task_id"],
        "context_id": correlated["context_id"],
        "hop_count": correlated["hop_count"],
        "payload": correlated["payload"],
    }
    return hashlib.sha256(canonical_json(value)).hexdigest()


class EnvelopeValidator:
    def __init__(self, envelope_schema_path: Path, card_schema_path: Path):
        self._env = Draft202012Validator(json.loads(
            Path(envelope_schema_path).read_text()))
        self._card = Draft202012Validator(json.loads(
            Path(card_schema_path).read_text()))

    def validate_envelope(self, doc: dict) -> None:
        _validate(self._env, doc, "envelope")
        if doc.get("type") in CORRELATED_TYPES:
            correlated = normalize_task_correlation(doc)
            _validate(_CORRELATION_VALIDATOR, correlated, "task_correlation")

    def validate_card(self, doc: dict) -> None:
        _validate(self._card, doc, "agent_card")

    def validate_register(self, envelope: dict) -> None:
        """Checks register envelope payload is a valid Agent Card AND that
        envelope.sender_id matches payload.name (A2A Agent Card identity)."""
        self.validate_envelope(envelope)
        if envelope.get("type") != "register":
            raise ValidationError("validate_register called on non-register envelope")
        card = envelope.get("payload", {})
        self.validate_card(card)
        if card.get("name") != envelope.get("sender_id"):
            raise ValidationError(
                f"sender_id {envelope.get('sender_id')!r} must match "
                f"Agent Card name {card.get('name')!r}")
