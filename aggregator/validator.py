"""Compatibility import for the shared EdgeCitadel protocol validator."""

from edgecitadel_plugin_runtime.validator import (
    CORRELATED_TYPES,
    EnvelopeValidator,
    ValidationError,
    canonical_json,
    normalize_task_correlation,
    request_fingerprint,
)

__all__ = [
    "CORRELATED_TYPES",
    "EnvelopeValidator",
    "ValidationError",
    "canonical_json",
    "normalize_task_correlation",
    "request_fingerprint",
]
