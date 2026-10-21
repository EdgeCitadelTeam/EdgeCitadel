"""Adapters through which the effect gateway performs registered effects itself.

With ``effect.execute`` the worker names an adapter, an effect class, a target
and bounded data; the gateway decides, commits and performs the effect, holding
any credential of the target system. The worker never sees it.

``EDGECITADEL_EFFECT_ADAPTERS`` names a JSON file in the agentd environment. It
holds no secret, only where to find one:

    {"homeassistant": {"base_url": "http://127.0.0.1:8123",
                       "token_file": "/etc/edgecitadel/homeassistant.token",
                       "timeout_ms": 5000},
     "http": {"ledger_url": "http://127.0.0.1:9100/ledger", "timeout_ms": 5000}}

- ``homeassistant`` calls a Home Assistant service over REST with a long-lived
  access token read once from ``token_file`` (a regular file, not a symbolic
  link). Its effect class is ``device.command`` and its target is
  ``<domain>.<service>/<entity_id>``, for example ``light.turn_on/light.kitchen``:
  the authorized target is exactly what is called, so ``data`` may add service
  data but never another ``entity_id``.
- ``http`` posts a JSON record of the committed effect to one configured ledger
  URL. It serves experiment sinks: any effect class and target.

Requests go only to the configured endpoint, never through an environment
proxy. Failures are reported by stable codes; no token, URL or response body is
logged or returned.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

from .delegation_authority import EffectRule
from .trace_contract import TraceContractError

DEFAULT_HOME_ASSISTANT_URL = "http://127.0.0.1:8123"
DEFAULT_TIMEOUT_MS = 5000
MAX_TOKEN_BYTES = 4096
_SERVICE_TARGET = re.compile(
    r"(?P<domain>[a-z0-9_]{1,64})\.(?P<service>[a-z0-9_]{1,64})/"
    r"(?P<entity>[a-z0-9_]{1,64}\.[a-z0-9_]{1,128})"
)


class EffectAdapterError(Exception):
    """The adapter could not perform a committed effect; ``code`` is stable."""

    def __init__(self, code: str, http_status: int | None = None) -> None:
        self.code = code
        self.http_status = http_status
        super().__init__(code)


class EffectAdapter(Protocol):
    name: str

    def validate(self, effect: EffectRule, data: Mapping[str, Any]) -> None:
        """Raise TraceContractError for a request this adapter cannot perform."""

    def perform(
        self, effect: EffectRule, data: Mapping[str, Any], record: Mapping[str, Any]
    ) -> int:
        """Perform the effect; return the HTTP status or raise EffectAdapterError."""


def _post(url: str, body: Mapping[str, Any], headers: dict[str, str], timeout_ms: int):
    request = urllib.request.Request(
        url,
        data=json.dumps(body, separators=(",", ":"), sort_keys=True).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    # The credential must never reach a proxy taken from the environment.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout_ms / 1000) as response:
            return int(response.status)
    except urllib.error.HTTPError as error:
        raise EffectAdapterError("adapter_rejected", error.code) from None
    except TimeoutError:
        raise EffectAdapterError("adapter_timeout") from None
    except (urllib.error.URLError, OSError):
        raise EffectAdapterError("adapter_unreachable") from None


def _http_url(value: object, what: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"invalid_{what}")
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username:
        raise ValueError(f"invalid_{what}")
    return value.rstrip("/")


def _timeout(value: object) -> int:
    if value is None:
        return DEFAULT_TIMEOUT_MS
    if type(value) is not int or not 1 <= value <= 60_000:
        raise ValueError("invalid_timeout_ms")
    return value


@dataclass(frozen=True)
class HomeAssistantAdapter:
    token: str = field(repr=False)
    base_url: str = DEFAULT_HOME_ASSISTANT_URL
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    name: str = "homeassistant"

    @classmethod
    def from_settings(cls, settings: Mapping[str, Any]) -> HomeAssistantAdapter:
        if set(settings) - {"base_url", "token_file", "timeout_ms"}:
            raise ValueError("invalid_homeassistant_adapter")
        path = settings.get("token_file")
        if not isinstance(path, str):
            raise ValueError("invalid_homeassistant_token_file")
        token_file = Path(path)
        if token_file.is_symlink() or not token_file.is_file():
            raise ValueError("invalid_homeassistant_token_file")
        raw = token_file.read_bytes()
        try:
            token = raw.strip().decode("ascii") if len(raw) <= MAX_TOKEN_BYTES else ""
        except UnicodeDecodeError:
            token = ""
        if not token or any(c.isspace() or not c.isprintable() for c in token):
            raise ValueError("invalid_homeassistant_token_file")
        return cls(
            token,
            _http_url(settings.get("base_url", DEFAULT_HOME_ASSISTANT_URL), "base_url"),
            _timeout(settings.get("timeout_ms")),
        )

    def validate(self, effect: EffectRule, data: Mapping[str, Any]) -> None:
        if effect.effect_class != "device.command":
            raise TraceContractError("invalid_effect_adapter_request")
        if _SERVICE_TARGET.fullmatch(effect.target) is None or "entity_id" in data:
            raise TraceContractError("invalid_effect_adapter_request")

    def perform(
        self, effect: EffectRule, data: Mapping[str, Any], record: Mapping[str, Any]
    ) -> int:
        match = _SERVICE_TARGET.fullmatch(effect.target)
        assert match is not None  # validated when the effect was requested
        return _post(
            f"{self.base_url}/api/services/{match['domain']}/{match['service']}",
            {**data, "entity_id": match["entity"]},
            {"Authorization": f"Bearer {self.token}"},
            self.timeout_ms,
        )


@dataclass(frozen=True)
class LedgerAdapter:
    ledger_url: str
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    name: str = "http"

    @classmethod
    def from_settings(cls, settings: Mapping[str, Any]) -> LedgerAdapter:
        if set(settings) - {"ledger_url", "timeout_ms"}:
            raise ValueError("invalid_http_adapter")
        return cls(
            _http_url(settings.get("ledger_url"), "ledger_url"),
            _timeout(settings.get("timeout_ms")),
        )

    def validate(self, effect: EffectRule, data: Mapping[str, Any]) -> None:
        return None

    def perform(
        self, effect: EffectRule, data: Mapping[str, Any], record: Mapping[str, Any]
    ) -> int:
        return _post(
            self.ledger_url, {**record, "data": dict(data)}, {}, self.timeout_ms
        )


_KINDS = {"homeassistant": HomeAssistantAdapter, "http": LedgerAdapter}


def load_adapters(path: str | None) -> dict[str, EffectAdapter]:
    """The adapters configured in the JSON file at ``path`` (none if unset)."""
    if not path:
        return {}
    try:
        document = json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise ValueError("effect_adapters_unreadable") from error
    if not isinstance(document, dict) or set(document) - set(_KINDS):
        raise ValueError("invalid_effect_adapters")
    adapters: dict[str, EffectAdapter] = {}
    for name, settings in document.items():
        if not isinstance(settings, dict):
            raise ValueError("invalid_effect_adapters")
        adapters[name] = _KINDS[name].from_settings(settings)
    return adapters
