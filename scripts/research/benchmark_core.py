from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import median
import time
from typing import Any
import uuid

import httpx


SUITE_VERSION = 1


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass
class TrialResult:
    trial: int
    task_id: str
    context_id: str | None = None
    origin_publish_ms: float | None = None
    first_progress_ms: float | None = None
    task_latency_ms: float | None = None
    recovery_ms: float | None = None
    result_state: str | None = None
    result_count: int = 0
    progress_frames: int = 0
    duplicates_seen: int = 0
    poison_events: int = 0
    semantic_failures: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass
class BenchmarkRun:
    run_id: str
    workload: str
    mode: str
    started_at: str
    ended_at: str
    environment: dict[str, Any]
    trials: list[TrialResult]
    suite_version: int = SUITE_VERSION


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[index]


def _metric(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "p50": float(median(ordered)),
        "p95": float(percentile(ordered, 95) or ordered[-1]),
        "p99": float(percentile(ordered, 99) or ordered[-1]),
    }


def summarize(trials: list[TrialResult]) -> dict[str, Any]:
    task_latencies = [t.task_latency_ms for t in trials if t.task_latency_ms is not None]
    first_progress = [t.first_progress_ms for t in trials if t.first_progress_ms is not None]
    recovery = [t.recovery_ms for t in trials if t.recovery_ms is not None]
    return {
        "trial_count": len(trials),
        "task_latency_ms": _metric(task_latencies),
        "first_progress_ms": _metric(first_progress),
        "recovery_ms": _metric(recovery),
        "duplicates_seen": sum(t.duplicates_seen for t in trials),
        "semantic_failures": sum(t.semantic_failures for t in trials),
    }


def run_to_dict(run: BenchmarkRun) -> dict[str, Any]:
    data = asdict(run)
    data["summary"] = summarize(run.trials)
    return data


def write_run(run: BenchmarkRun, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{run.run_id}-{run.workload.lower()}.json"
    path.write_text(json.dumps(run_to_dict(run), indent=2, sort_keys=True) + "\n")
    return path


def render_markdown_summary(run: BenchmarkRun) -> str:
    summary = summarize(run.trials)
    task_metric = summary["task_latency_ms"] or {}

    def fmt(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.1f}"
        return str(value)

    return "\n".join(
        [
            "| Workload | Trials | p50 task ms | p95 task ms | p99 task ms | Duplicates | Semantic failures |",
            "|---|---:|---:|---:|---:|---:|---:|",
            (
                f"| {run.workload} | {summary['trial_count']} | "
                f"{fmt(task_metric.get('p50'))} | {fmt(task_metric.get('p95'))} | "
                f"{fmt(task_metric.get('p99'))} | {summary['duplicates_seen']} | "
                f"{summary['semantic_failures']} |"
            ),
            "",
        ]
    )


def write_markdown_summary(run: BenchmarkRun, json_path: Path) -> Path:
    path = json_path.with_suffix(".md")
    path.write_text(render_markdown_summary(run))
    return path


async def assert_stack_ready(api_base: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(f"{api_base}/system/status")
            response.raise_for_status()
            status = response.json()
    except httpx.RequestError as exc:
        raise RuntimeError(f"stack preflight failed for {api_base}: {exc}") from exc
    if not status.get("nats_connected"):
        raise RuntimeError("aggregator reports nats_connected=false")
    if not status.get("jetstream_stream_ok"):
        raise RuntimeError("aggregator reports jetstream_stream_ok=false")
    return status


def _uuid4() -> str:
    return str(uuid.uuid4())


def command_envelope(
    *,
    sender_id: str,
    recipient_id: str,
    body: str,
    task_id: str | None = None,
    context_id: str | None = None,
    envelope_id: str | None = None,
    payload_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {"body": body}
    if payload_extra:
        payload.update(payload_extra)
    env: dict[str, Any] = {
        "v": 1,
        "id": envelope_id or _uuid4(),
        "type": "command",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id or _uuid4(),
        "timestamp": now_iso(),
        "payload": payload,
    }
    if context_id:
        env["context_id"] = context_id
    return env


def delegation_envelope(
    *,
    sender_id: str,
    recipient_id: str,
    body: str,
    context_id: str,
    hop_count: int,
    task_id: str | None = None,
    payload_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    env = command_envelope(
        sender_id=sender_id,
        recipient_id=recipient_id,
        body=body,
        context_id=context_id,
        task_id=task_id,
        payload_extra=payload_extra,
    )
    env["type"] = "delegation"
    env["hop_count"] = hop_count
    return env


def cancel_envelope(*, sender_id: str, recipient_id: str, task_id: str) -> dict[str, Any]:
    return {
        "v": 1,
        "id": _uuid4(),
        "type": "cancel",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id,
        "timestamp": now_iso(),
        "payload": {},
    }


def result_envelope(
    *,
    sender_id: str,
    recipient_id: str,
    task_id: str,
    task_state: str,
    payload: dict[str, Any] | None = None,
    context_id: str | None = None,
) -> dict[str, Any]:
    env: dict[str, Any] = {
        "v": 1,
        "id": _uuid4(),
        "type": "result",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id,
        "task_state": task_state,
        "timestamp": now_iso(),
        "payload": payload or {},
    }
    if context_id:
        env["context_id"] = context_id
    return env


def progress_envelope(
    *,
    sender_id: str,
    recipient_id: str,
    task_id: str,
    payload: dict[str, Any] | None = None,
    context_id: str | None = None,
) -> dict[str, Any]:
    env: dict[str, Any] = {
        "v": 1,
        "id": _uuid4(),
        "type": "task.progress",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id,
        "task_state": "working",
        "timestamp": now_iso(),
        "payload": payload or {},
    }
    if context_id:
        env["context_id"] = context_id
    return env


def register_envelope(agent_id: str, *, metadata: dict[str, Any]) -> dict[str, Any]:
    metadata = metadata.copy()
    metadata.setdefault("runtime.conformance", "L1")
    return {
        "v": 1,
        "id": _uuid4(),
        "type": "register",
        "sender_id": agent_id,
        "timestamp": now_iso(),
        "payload": {
            "name": agent_id,
            "description": "Research benchmark fixture",
            "version": "0",
            "url": f"nats://{agent_id}",
            "provider": {"organization": "EdgeCitadel Research"},
            "capabilities": {},
            "securitySchemes": {},
            "metadata": metadata,
        },
    }


def heartbeat_envelope(agent_id: str) -> dict[str, Any]:
    return {
        "v": 1,
        "id": _uuid4(),
        "type": "heartbeat",
        "sender_id": agent_id,
        "timestamp": now_iso(),
        "payload": {},
    }


async def wait_for_messages(
    api_base: str,
    *,
    task_id: str,
    timeout_sec: float = 60.0,
    limit: int = 500,
    require_result: bool = True,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    async with httpx.AsyncClient(timeout=10) as client:
        last_rows: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            response = await client.get(
                f"{api_base}/messages",
                params={"task_id": task_id, "limit": limit},
            )
            response.raise_for_status()
            rows = response.json()
            last_rows = rows
            if not require_result or any(row["type"] == "result" for row in rows):
                return rows
            await asyncio.sleep(0.25)
    if require_result:
        raise TimeoutError(f"no terminal result for task_id={task_id}")
    return last_rows


async def wait_for_context_messages(
    api_base: str,
    *,
    context_id: str,
    expected_results: int = 1,
    timeout_sec: float = 60.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    async with httpx.AsyncClient(timeout=10) as client:
        while time.monotonic() < deadline:
            response = await client.get(
                f"{api_base}/messages",
                params={"context_id": context_id, "limit": limit},
            )
            response.raise_for_status()
            rows = response.json()
            if sum(1 for row in rows if row["type"] == "result") >= expected_results:
                return rows
            await asyncio.sleep(0.25)
    raise TimeoutError(f"not enough context results for context_id={context_id}")


async def query_poison(api_base: str, *, agent_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": limit}
    if agent_id:
        params["agent_id"] = agent_id
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(f"{api_base}/poison", params=params)
        response.raise_for_status()
        return response.json()


async def post_command(
    api_base: str,
    *,
    agent_id: str,
    sender_id: str,
    body: str,
    args: dict[str, Any] | None = None,
    context_id: str | None = None,
    skill_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"body": body}
    if args:
        payload["args"] = args
    if context_id:
        payload["context_id"] = context_id
    if skill_id:
        payload["skill_id"] = skill_id
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            f"{api_base}/command/{agent_id}",
            params={"sender_id": sender_id},
            json=payload,
        )
        response.raise_for_status()
        return response.json()
