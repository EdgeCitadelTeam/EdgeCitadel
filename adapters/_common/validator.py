"""Thin re-export of aggregator.validator so adapters don't import aggregator."""
from pathlib import Path
from aggregator.validator import EnvelopeValidator, ValidationError


REPO = Path(__file__).resolve().parents[2]
SCHEMAS = REPO / "schemas"


def default_validator() -> EnvelopeValidator:
    return EnvelopeValidator(
        envelope_schema_path=SCHEMAS / "envelope.v1.json",
        card_schema_path=SCHEMAS / "agent-card.v1.json",
    )


__all__ = ["EnvelopeValidator", "ValidationError", "default_validator"]
