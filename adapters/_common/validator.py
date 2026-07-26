"""Thin re-export of aggregator.validator so adapters don't import aggregator."""

from pathlib import Path

from aggregator.validator import (
    CORRELATED_TYPES,
    EnvelopeValidator,
    ValidationError,
    canonical_json,
    normalize_task_correlation,
    request_fingerprint,
)

REPO = Path(__file__).resolve().parents[2]
SCHEMAS = REPO / "schemas"


def default_validator() -> EnvelopeValidator:
    return EnvelopeValidator(
        envelope_schema_path=SCHEMAS / "envelope.v1.json",
        card_schema_path=SCHEMAS / "agent-card.v1.json",
    )


__all__ = [
    "CORRELATED_TYPES",
    "EnvelopeValidator",
    "ValidationError",
    "canonical_json",
    "default_validator",
    "normalize_task_correlation",
    "request_fingerprint",
]
