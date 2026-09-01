"""Small, synchronous Home Assistant REST client used by the worker Plugin runtime."""

from __future__ import annotations

import io
import json
import time
import urllib.request
from typing import Any


class HomeAssistantError(RuntimeError):
    """Raised when Home Assistant rejects or cannot serve a request."""


class HomeAssistantClient:
    def __init__(self, base: str, token: str, *, timeout_sec: float = 20):
        self.base = base.rstrip("/")
        self.token = token
        self.timeout_sec = timeout_sec

    def request(
        self, method: str, path: str, body: Any = None, *, binary: bool = False
    ) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
                payload = response.read()
        except Exception as exc:  # noqa: BLE001
            raise HomeAssistantError(f"Home Assistant request failed: {exc}") from exc
        if binary:
            return payload
        try:
            return json.loads(payload.decode() or "null")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HomeAssistantError("Home Assistant returned invalid JSON") from exc

    def state(self, entity_id: str) -> dict:
        value = self.request("GET", "/api/states/" + entity_id)
        if not isinstance(value, dict):
            raise HomeAssistantError("state response was not an object")
        return value

    def set_light(self, entity_id: str, state: str, brightness: int = 255) -> Any:
        if state not in {"on", "off"}:
            raise ValueError("light state must be on or off")
        body: dict[str, Any] = {"entity_id": entity_id}
        if state == "on":
            body.update({"brightness": brightness, "transition": 0})
        return self.request("POST", "/api/services/light/turn_" + state, body)

    def camera_jpeg(self, entity_id: str) -> bytes:
        return self.request(
            "GET",
            "/api/camera_proxy/%s?edgecitadel=%d" % (entity_id, time.time_ns()),
            binary=True,
        )

    def camera_luma(self, entity_id: str, roi: list[int] | None = None) -> dict:
        from PIL import Image, ImageStat

        image = Image.open(io.BytesIO(self.camera_jpeg(entity_id))).convert("L")
        width, height = image.size
        if roi is not None:
            x, y, w, h = roi
            if x < 0 or y < 0 or w <= 0 or h <= 0 or x + w > width or y + h > height:
                raise ValueError("camera ROI is outside the returned image")
            image = image.crop((x, y, x + w, y + h))
        mean = ImageStat.Stat(image).mean[0]
        return {
            "mean_luma": round(mean, 3),
            "width": width,
            "height": height,
            "roi": roi,
        }
