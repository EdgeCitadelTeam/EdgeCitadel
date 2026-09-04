"""EdgeCitadel Managed Adapter for bounded Home Assistant operations."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any

from edgecitadel_agentd.managed_runtime import ManagedContext, run as run_managed_agent

from .client import HomeAssistantClient

log = logging.getLogger(__name__)

DEFAULT_BASE = "http://localhost:8123"
DEFAULT_TOKEN_FILE = "/etc/edgecitadel/homeassistant/token"
DEFAULT_MAX_STEPS = 32
DEFAULT_MAX_WAIT_SEC = 30.0


def _csv_env(name: str) -> set[str]:
    return {
        item.strip() for item in os.environ.get(name, "").split(",") if item.strip()
    }


def _int_arg(value: Any, name: str, *, minimum: int, maximum: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def _float_arg(value: Any, name: str, *, minimum: float, maximum: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


class HomeAssistantWorker:
    def __init__(
        self,
        client: HomeAssistantClient,
        *,
        allowed_lights: set[str],
        allowed_entities: set[str],
        allowed_cameras: set[str],
        camera_rois: dict[str, list[int]],
        max_steps: int = DEFAULT_MAX_STEPS,
    ):
        self.client = client
        self.allowed_lights = allowed_lights
        self.allowed_entities = allowed_entities | allowed_lights | allowed_cameras
        self.allowed_cameras = allowed_cameras
        self.camera_rois = camera_rois
        self.max_steps = max_steps

    def _require_entity(self, entity_id: str, allowed: set[str], label: str) -> None:
        if not isinstance(entity_id, str) or entity_id not in allowed:
            raise PermissionError(f"entity is not allowlisted for {label}: {entity_id}")

    def _state(self, entity_id: str) -> dict:
        self._require_entity(entity_id, self.allowed_entities, "state reads")
        return self.client.state(entity_id)

    def _set_light(self, args: dict) -> dict:
        entity_id = args.get("entity_id")
        self._require_entity(entity_id, self.allowed_lights, "light control")
        state = args.get("state")
        brightness = _int_arg(
            args.get("brightness", 255), "brightness", minimum=1, maximum=255
        )
        self.client.set_light(entity_id, state, brightness)
        confirmed = self._wait_state(
            {
                "entity_id": entity_id,
                "state": state,
                "brightness": brightness if state == "on" else None,
                "timeout_sec": args.get("confirm_timeout_sec", DEFAULT_MAX_WAIT_SEC),
                "poll_sec": args.get("poll_sec", 0.1),
            }
        )
        return {
            "entity_id": entity_id,
            "requested_state": state,
            "reported_state": confirmed["state"],
            "reported_brightness": confirmed.get("brightness"),
            "last_changed": confirmed.get("last_changed"),
        }

    def _wait_state(self, args: dict) -> dict:
        entity_id = args.get("entity_id")
        self._require_entity(entity_id, self.allowed_entities, "state waits")
        target = args.get("state")
        if not isinstance(target, str) or not target:
            raise ValueError("state is required")
        timeout = _float_arg(
            args.get("timeout_sec", DEFAULT_MAX_WAIT_SEC),
            "timeout_sec",
            minimum=0.01,
            maximum=DEFAULT_MAX_WAIT_SEC,
        )
        poll = _float_arg(
            args.get("poll_sec", 0.1), "poll_sec", minimum=0.01, maximum=5.0
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            record = self._state(entity_id)
            brightness = args.get("brightness")
            reported_brightness = (record.get("attributes") or {}).get("brightness")
            if record.get("state") == target and (
                brightness is None or reported_brightness == brightness
            ):
                return {
                    "entity_id": entity_id,
                    "state": target,
                    "brightness": reported_brightness,
                    "last_changed": record.get("last_changed"),
                }
            time.sleep(poll)
        raise TimeoutError(f"state did not reach {target}: {entity_id}")

    def _read_camera(self, args: dict) -> dict:
        entity_id = args.get("entity_id")
        self._require_entity(entity_id, self.allowed_cameras, "camera reads")
        roi = args.get("roi", self.camera_rois.get(entity_id))
        if roi is not None:
            if not isinstance(roi, list) or len(roi) != 4:
                raise ValueError("roi must be [x, y, width, height]")
            roi = [_int_arg(item, "roi", minimum=0, maximum=10000) for item in roi]
        return {"entity_id": entity_id, **self.client.camera_luma(entity_id, roi)}

    def operation(self, operation: str, args: dict) -> dict:
        if operation == "get_state":
            return self._state(args.get("entity_id"))
        if operation == "set_light":
            return self._set_light(args)
        if operation == "wait_state":
            return self._wait_state(args)
        if operation == "read_camera":
            return self._read_camera(args)
        raise ValueError(f"unsupported operation: {operation}")

    def sequence(self, steps: list[dict], *, restore: bool) -> dict:
        if not isinstance(steps, list) or not steps:
            raise ValueError("steps must be a non-empty list")
        if len(steps) > self.max_steps:
            raise ValueError(f"steps exceeds maximum of {self.max_steps}")

        originals: dict[str, dict] = {}
        for step in steps:
            if not isinstance(step, dict) or step.get("operation") == "run_sequence":
                raise ValueError("each step must be an operation object")
            if step.get("operation") == "set_light":
                entity_id = (step.get("args") or {}).get("entity_id")
                self._require_entity(entity_id, self.allowed_lights, "light control")
                if entity_id not in originals:
                    originals[entity_id] = self._state(entity_id)

        results = []
        restore_errors = []
        try:
            for index, step in enumerate(steps):
                started = time.monotonic()
                operation = step.get("operation", "")
                result = self.operation(operation, step.get("args") or {})
                results.append(
                    {
                        "index": index,
                        "operation": operation,
                        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                        "result": result,
                    }
                )
        finally:
            if restore:
                for entity_id, original in originals.items():
                    state = original.get("state")
                    if state in {"on", "off"}:
                        brightness = (
                            original.get("attributes", {}).get("brightness") or 255
                        )
                        try:
                            self.client.set_light(entity_id, state, brightness)
                            self._wait_state({"entity_id": entity_id, "state": state})
                        except Exception:
                            log.exception("failed restoring %s", entity_id)
                            restore_errors.append(entity_id)
        return {
            "steps": results,
            "restored_entities": sorted(originals) if restore else [],
            "restore_errors": restore_errors,
        }


def _load_worker() -> HomeAssistantWorker:
    token_file = os.environ.get("HA_TOKEN_FILE", DEFAULT_TOKEN_FILE)
    token = Path(token_file).read_text().strip()
    if not token:
        raise ValueError("HA_TOKEN_FILE is empty")
    rois: dict[str, list[int]] = {}
    for item in _csv_env("HA_CAMERA_ROIS"):
        entity, _, raw = item.partition(":")
        if not entity or not raw:
            raise ValueError("HA_CAMERA_ROIS entries must be entity:x:y:w:h")
        values = raw.split(":")
        if len(values) != 4:
            raise ValueError("HA_CAMERA_ROIS entries must be entity:x:y:w:h")
        rois[entity] = [
            _int_arg(v, "camera ROI", minimum=0, maximum=10000) for v in values
        ]
    return HomeAssistantWorker(
        HomeAssistantClient(os.environ.get("HA_BASE_URL", DEFAULT_BASE), token),
        allowed_lights=_csv_env("HA_ALLOWED_LIGHTS"),
        allowed_entities=_csv_env("HA_ALLOWED_ENTITIES"),
        allowed_cameras=_csv_env("HA_ALLOWED_CAMERAS"),
        camera_rois=rois,
        max_steps=_int_arg(
            os.environ.get("HA_MAX_SEQUENCE_STEPS", DEFAULT_MAX_STEPS),
            "HA_MAX_SEQUENCE_STEPS",
            minimum=1,
            maximum=100,
        ),
    )


async def handle(env: dict, ctx: ManagedContext) -> tuple[dict, str]:
    if env["type"] != "command":
        return ({"error": "unsupported_type"}, "rejected")
    args = env["payload"].get("args") or {}
    operation = args.get("operation")
    try:
        worker = _load_worker()
        if operation == "run_sequence":
            result = await asyncio.to_thread(
                worker.sequence, args.get("steps"), restore=args.get("restore", True)
            )
        else:
            result = await asyncio.to_thread(worker.operation, operation, args)
        state = "failed" if result.get("restore_errors") else "completed"
        return ({"operation": operation, "result": result}, state)
    except (PermissionError, ValueError, TimeoutError) as exc:
        state = "failed" if isinstance(exc, TimeoutError) else "rejected"
        return (
            {"operation": operation, "error": type(exc).__name__, "message": str(exc)},
            state,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("Home Assistant operation failed")
        return (
            {"operation": operation, "error": type(exc).__name__, "message": str(exc)},
            "failed",
        )


async def main() -> None:
    await run_managed_agent(Path(__file__).resolve().parent / "config.yaml", handle)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
