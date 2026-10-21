"""Client for the experiment's ground-truth services (outside the runtime).

The services host the append-only effect ledger (the actuator: an effect counts
as committed only when the ledger accepts its ``committed`` record), the worker
event log, and delivery readiness. Readiness carries no content: a dispatcher
learns that its child's pushed inputs have landed and then dispatches the child
through the runtime, so every byte of workload content still crosses runtime
dispatch boundaries.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any

log = logging.getLogger(__name__)
RETRIES = 5


class ServicesUnavailable(RuntimeError):
    pass


class ServicesClient:
    def __init__(self, base_url: str, token: str, *, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        # Never route the experiment's local services through an egress proxy.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _request(self, method: str, path: str, body: Mapping[str, Any] | None) -> Any:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json", "X-Harness-Token": self.token},
        )
        delay = 0.05
        for attempt in range(RETRIES):
            try:
                with self._opener.open(request, timeout=self.timeout) as response:
                    return json.loads(response.read() or b"null")
            except (urllib.error.URLError, OSError, TimeoutError) as error:
                if attempt == RETRIES - 1:
                    raise ServicesUnavailable(f"{method} {path}: {error}") from error
                time.sleep(delay)
                delay *= 2
        raise AssertionError("unreachable")

    def event(self, record: Mapping[str, Any]) -> None:
        self._request("POST", "/events", record)

    def ledger(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Record an effect phase; ``committed`` is the actuation itself."""
        return self._request("POST", "/ledger", record)

    def ready(self, task: str) -> dict[str, int]:
        """Pushed items that have landed for ``task``, with copy counts."""
        return self._request("GET", f"/ready?task={urllib.request.quote(task)}", None)[
            "items"
        ]

    def spawn(self, record: Mapping[str, Any]) -> bool:
        """Reserve one spawned invocation under the run's safety cap."""
        return bool(self._request("POST", "/spawn", record)["allowed"])
