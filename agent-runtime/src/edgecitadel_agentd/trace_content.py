"""Fail-closed, bounded content for durable observations, never executable artifacts."""

from __future__ import annotations

import json
import re
from typing import Any

MAX_CONTENT_BYTES = 8192
_SENSITIVE = re.compile(
    r"(?i)(secret|password|passwd|token|authorization|cookie|api[_-]?key|credential|private[_-]?key)"
)
_CREDENTIAL = re.compile(
    r"(?i)(?:bearer\s+[^\s\"'<>]+|(?:sk-|ghp_|github_pat_)[a-z0-9_-]+|"
    r"(?:[\w-]*(?:secret|password|passwd|token|authorization|cookie|api[_-]?key|credential)[\w-]*)"
    r"[\"']?\s*[:=]\s*[\"']?[^\s\"'&,;}<>]+)"
)
_URL = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s<>\"']+")
_PRIVATE_KEY = re.compile(
    r"-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|$)", re.S
)


def _text(value: str) -> str:
    # URLs can carry opaque credentials in any component. Omit the whole URL.
    value = _URL.sub("[URL omitted]", value)
    value = _PRIVATE_KEY.sub("[REDACTED]", value)
    value = re.sub(
        r"(?i)\b(?:proxy-)?authorization\s*[:=]\s*[\"']?(?:(?:bearer|basic)\s+)?[^\s\"'&,;}<>]+",
        "[REDACTED]",
        value,
    )
    return _CREDENTIAL.sub("[REDACTED]", value)


def _redact(value: object, depth: int = 0, budget: list[int] | None = None) -> Any:
    if budget is None:
        budget = [32768]
    budget[0] -= 1
    if depth > 12 or budget[0] < 0:
        raise ValueError("content_complexity")
    if type(value) is dict:
        if len(value) > 4096:
            raise ValueError("content_complexity")
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("unsupported_content")
            result[_text(key)] = (
                "[REDACTED]"
                if _SENSITIVE.search(key)
                else _redact(item, depth + 1, budget)
            )
        return result
    if type(value) in (list, tuple):
        if len(value) > 4096:
            raise ValueError("content_complexity")
        return [_redact(item, depth + 1, budget) for item in value]
    if type(value) is str:
        if len(value) > 1_048_576:
            raise ValueError("content_complexity")
        # Tool results frequently arrive as serialized JSON.
        if value.lstrip().startswith(("{", "[")):
            try:
                return _redact(json.loads(value), depth + 1, budget)
            except json.JSONDecodeError:
                pass
        return _text(value)
    if value is None or type(value) in (bool, int, float):
        return value
    if type(value) in (bytes, bytearray):
        return "[binary omitted]"
    raise ValueError("unsupported_content")


def bounded_content(fields: dict[str, object]) -> dict[str, Any]:
    """The entire serialized content object, including keys, fits 8 KiB.

    Never call repr/str on arbitrary SDK objects. Any failure drops content,
    leaving the observation's metadata and an explicit reason intact.
    """
    try:
        texts = {
            _text(key): json.dumps(
                "[REDACTED]" if _SENSITIVE.search(key) else _redact(value),
                ensure_ascii=False,
                allow_nan=False,
            )
            for key, value in fields.items()
        }

        def encoded():
            return json.dumps(texts, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )

        truncated = False
        while len(encoded()) > MAX_CONTENT_BYTES:
            truncated = True
            key = max(texts, key=lambda item: len(texts[item].encode("utf-8")))
            raw = texts[key].encode("utf-8")
            if not raw:
                raise ValueError("content_keys_oversize")
            texts[key] = raw[
                : max(0, len(raw) - max(1, len(encoded()) - MAX_CONTENT_BYTES))
            ].decode("utf-8", errors="ignore")
        return {"status": "truncated" if truncated else "available", "fields": texts}
    except Exception:  # noqa: BLE001 - fail closed at the optional content boundary
        return {"status": "unavailable", "reason": "redaction_failed", "fields": {}}


def sanitize_event_content(event: dict[str, Any]) -> dict[str, Any]:
    """Recheck producers at the durable boundary; never trust a redacted flag."""
    if "content" not in event:
        return event
    original = event["content"]
    fields = {}
    for key, value in original.get("fields", {}).items():
        try:
            fields[key] = json.loads(value)
        except (ValueError, TypeError):
            fields[key] = value
    content = bounded_content(fields)
    if original.get("status") == "unavailable":
        content = {
            "status": "unavailable",
            "reason": original.get("reason", "not_reported"),
            "fields": {},
        }
    elif original.get("status") == "truncated" and content["status"] == "available":
        content["status"] = "truncated"
    return {**event, "content": content}


def fit_event_content(event: dict[str, Any]) -> None:
    """Trim optional text before rejecting an otherwise valid metadata record."""
    content = event.get("content")
    if not content:
        return
    fields = content["fields"]
    while fields:
        excess = (
            len(json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode())
            - 16384
        )
        if excess <= 0:
            break
        key = max(fields, key=lambda key: len(fields[key].encode()))
        raw = fields[key].encode()
        if not raw:
            del fields[key]
        else:
            fields[key] = raw[: max(0, len(raw) - excess)].decode(errors="ignore")
        content["status"] = "truncated"
