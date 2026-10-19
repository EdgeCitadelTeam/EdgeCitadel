"""Scoped five-second broker topology observations; never infer execution paths."""

from __future__ import annotations

import asyncio
import json
from urllib.request import urlopen


class BrokerMonitor:
    def __init__(self, recorder, url):
        self.recorder, self.url = recorder, url
        self.previous = {}

    def sample(self, endpoint):
        with urlopen(self.url.rstrip("/") + "/" + endpoint, timeout=2) as response:
            raw = response.read(262145)
        if len(raw) > 262144:
            raise ValueError("monitor_response_oversize")
        data = json.loads(raw)
        # Exclude volatile traffic counters and credential-bearing configuration.
        names = {"leafz": "leafs", "routez": "routes", "gatewayz": "outbound_gateways"}
        rows = data.get(names[endpoint], [])
        if isinstance(rows, dict):
            rows = list(rows.values())
        keys = (
            "id",
            "cid",
            "rid",
            "name",
            "remote_id",
            "remote_name",
            "account",
            "is_spoke",
        )
        links = [
            {key: row[key] for key in keys if key in row}
            for row in rows
            if isinstance(row, dict)
        ]
        links.sort(key=lambda row: json.dumps(row, sort_keys=True))
        return {
            "server_id": data.get("server_id"),
            "endpoint": endpoint,
            "links": links,
            "inbound_gateways": sorted(data.get("inbound_gateways", {}).keys())
            if endpoint == "gatewayz"
            else [],
        }

    async def run(self):
        if not self.url:
            self.recorder.record(
                "unavailable",
                kind="infrastructure",
                attributes={
                    "provenance": "monitor_poll",
                    "coverage_reason": "not_configured",
                    "poll_interval_ms": 5000,
                },
            )
            return
        while True:
            for endpoint in ("leafz", "routez", "gatewayz"):
                try:
                    snapshot = await asyncio.to_thread(self.sample, endpoint)
                    phase = "changed"
                except Exception as error:  # noqa: BLE001 - monitor cannot affect messaging
                    snapshot = {
                        "endpoint": endpoint,
                        "error_type": type(error).__name__,
                    }
                    phase = "unavailable"
                if self.previous.get(endpoint) != snapshot:
                    self.recorder.record(
                        phase,
                        kind="infrastructure",
                        attributes={
                            "provenance": "monitor_poll",
                            "poll_interval_ms": 5000,
                            **(
                                {"coverage_reason": "monitor_unavailable"}
                                if phase == "unavailable"
                                else {}
                            ),
                        },
                        content={"topology": snapshot},
                    )
                    self.previous[endpoint] = snapshot
            await asyncio.sleep(5)
