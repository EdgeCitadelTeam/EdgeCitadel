"""Opaque cursor integrity and scope; access authorization stays with the API.

The Core must persist the signing key and projection generation. Rebuilds rotate
the generation; retention is checked against authoritative retained positions.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .trace_contract import TraceContractError, canonical_bytes, validate_cursor_claims

_TOKEN = re.compile(r"([A-Za-z0-9_-]+)\.([A-Za-z0-9_-]{43})")


@dataclass(frozen=True)
class CursorScope:
    kind: str
    trace_id: str | None
    scope_hash: str
    projection_generation: str


def cursor_scope_hash(
    filters: Mapping[str, object], access_policy: Mapping[str, object]
) -> str:
    """Bind normalized filters and server-selected access policy to a cursor."""
    return hashlib.sha256(
        canonical_bytes(
            {"filters": dict(filters), "access_policy": dict(access_policy)},
            limit=2048,
        )
    ).hexdigest()


def _key(key: bytes) -> None:
    if not isinstance(key, bytes) or len(key) < 32:
        raise ValueError("cursor signing key must contain at least 32 bytes")


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _decode(value: str) -> bytes:
    result = base64.b64decode(
        value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
    )
    if _encode(result) != value:
        raise TraceContractError("invalid_cursor")
    return result


def encode_cursor(claims: dict[str, Any], key: bytes) -> str:
    _key(key)
    body = validate_cursor_claims(claims)
    signature = hmac.digest(key, b"edgecitadel.trace.cursor.v1\0" + body, "sha256")
    return _encode(body) + "." + _encode(signature)


def decode_cursor(
    token: str,
    key: bytes,
    expected: CursorScope,
    *,
    retained_from: int,
) -> dict[str, Any]:
    _key(key)
    if type(retained_from) is not int or retained_from < 0:
        raise ValueError("retained_from must be a nonnegative integer")
    if not isinstance(token, str) or len(token) > 4096:
        raise TraceContractError("invalid_cursor")
    match = _TOKEN.fullmatch(token)
    if match is None:
        raise TraceContractError("invalid_cursor")
    try:
        body, signature = (_decode(part) for part in match.groups())
        actual = hmac.digest(key, b"edgecitadel.trace.cursor.v1\0" + body, "sha256")
        if not hmac.compare_digest(signature, actual):
            raise TraceContractError("invalid_cursor")
        claims = json.loads(body)
        if validate_cursor_claims(claims) != body:
            raise TraceContractError("invalid_cursor")
    except (
        binascii.Error,
        UnicodeError,
        json.JSONDecodeError,
        RecursionError,
    ) as error:
        raise TraceContractError("invalid_cursor") from error
    if any(
        claims[field] != getattr(expected, field)
        for field in ("kind", "trace_id", "scope_hash")
    ):
        raise TraceContractError("cursor_scope_mismatch")
    if claims["projection_generation"] != expected.projection_generation:
        raise TraceContractError("generation_changed")
    position = claims["position"] if claims["kind"] == "changes" else claims["snapshot"]
    if position < retained_from:
        raise TraceContractError("history_expired")
    return claims
