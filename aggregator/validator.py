"""Envelope and Agent Card validation against vendored schemas."""
from __future__ import annotations
import json
from pathlib import Path
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JSONSchemaError


class ValidationError(Exception):
    pass


class EnvelopeValidator:
    def __init__(self, envelope_schema_path: Path, card_schema_path: Path):
        self._env = Draft202012Validator(json.loads(
            Path(envelope_schema_path).read_text()))
        self._card = Draft202012Validator(json.loads(
            Path(card_schema_path).read_text()))

    def validate_envelope(self, doc: dict) -> None:
        try:
            self._env.validate(doc)
        except JSONSchemaError as e:
            raise ValidationError(f"envelope invalid: {e.message} "
                                  f"at {list(e.absolute_path)}") from e

    def validate_card(self, doc: dict) -> None:
        try:
            self._card.validate(doc)
        except JSONSchemaError as e:
            raise ValidationError(f"agent_card invalid: {e.message} "
                                  f"at {list(e.absolute_path)}") from e

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
