from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import socket
import sys
import time
from typing import Any, AsyncIterator
import uuid

import httpx
from nats.aio.client import Client as NATS

from scripts.research.benchmark_core import (
    BenchmarkRun,
    TrialResult,
    assert_stack_ready,
    cancel_envelope,
    command_envelope,
    delegation_envelope,
    heartbeat_envelope,
    now_iso,
    post_command,
    query_poison,
    register_envelope,
    result_envelope,
    wait_for_context_messages,
    wait_for_messages,
    write_run,
)


RUNNER_ID = "bench-runner"


def _run_id(workload: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{workload.lower()}"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ms_since(start: float) -> float:
    return round((time.perf_counter() - start) * 1000.0, 3)


def _environment(args: Any, **extra: Any) -> dict[str, Any]:
    env = {
        "api_base": args.api_base,
        "nats_url": args.nats_url,
        "target_agent": getattr(args, "target_agent", None),
    }
    env.update({k: v for k, v in extra.items() if v is not None})
    return env


def _result_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("type") == "result"]


def _progress_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("type") == "task.progress"]


async def _connect_nats(
    nats_url: str,
    *,
    token: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> NATS:
    nc = NATS()
    kwargs: dict[str, Any] = {"servers": [nats_url], "connect_timeout": 3}
    if token:
        kwargs["token"] = token
    if user:
        kwargs["user"] = user
    if password:
        kwargs["password"] = password
    await nc.connect(**kwargs)
    return nc


async def _publish_plain(nc: NATS, subject: str, env: dict[str, Any]) -> None:
    await nc.publish(subject, json.dumps(env).encode())
    await nc.flush()


async def _publish_js(js: Any, subject: str, env: dict[str, Any], *, msg_id: str | None = None) -> Any:
    return await js.publish(
        subject,
        json.dumps(env).encode(),
        headers={"Nats-Msg-Id": msg_id or env["id"]},
    )


def _finalize(
    *,
    workload: str,
    mode: str,
    started_at: str,
    environment: dict[str, Any],
    trials: list[TrialResult],
    out_dir: Path,
) -> tuple[BenchmarkRun, Path]:
    run = BenchmarkRun(
        run_id=_run_id(workload),
        workload=workload,
        mode=mode,
        started_at=started_at,
        ended_at=now_iso(),
        environment=environment,
        trials=trials,
    )
    return run, write_run(run, out_dir)


async def _poll_task_with_progress(
    api_base: str,
    *,
    task_id: str,
    timeout_sec: float,
    limit: int = 500,
) -> tuple[list[dict[str, Any]], float | None, float]:
    start = time.perf_counter()
    deadline = start + timeout_sec
    first_progress_ms: float | None = None
    async with httpx.AsyncClient(timeout=10) as client:
        while time.perf_counter() < deadline:
            response = await client.get(
                f"{api_base}/messages",
                params={"task_id": task_id, "limit": limit},
            )
            response.raise_for_status()
            rows = response.json()
            if first_progress_ms is None and _progress_rows(rows):
                first_progress_ms = _ms_since(start)
            if _result_rows(rows):
                return rows, first_progress_ms, _ms_since(start)
            await asyncio.sleep(0.25)
    raise TimeoutError(f"no terminal result for task_id={task_id}")


@asynccontextmanager
async def _subprocess(
    *cmd: str,
    env: dict[str, str] | None = None,
) -> AsyncIterator[asyncio.subprocess.Process]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(_repo_root()),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        yield proc
    finally:
        if proc.returncode is None:
            proc.terminate()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=5)
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


def _python_env(args: Any) -> dict[str, str]:
    import os

    env = os.environ.copy()
    env["PYTHONPATH"] = str(_repo_root())
    env["NATS_URL"] = args.nats_url
    if getattr(args, "nats_token", None):
        env["NATS_TOKEN"] = args.nats_token
    else:
        env.pop("NATS_TOKEN", None)
    return env


async def run_e1(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    trials: list[TrialResult] = []
    for trial in range(1, args.trials + 1):
        start = time.perf_counter()
        response = await post_command(
            args.api_base,
            agent_id=args.target_agent,
            sender_id=RUNNER_ID,
            body=f"printf edgecitadel-e1-{trial}",
        )
        task_id = response["task_id"]
        rows = await wait_for_messages(args.api_base, task_id=task_id, timeout_sec=60)
        latency_ms = _ms_since(start)
        results = _result_rows(rows)
        progresses = _progress_rows(rows)
        trials.append(
            TrialResult(
                trial=trial,
                task_id=task_id,
                task_latency_ms=latency_ms,
                result_state=results[0].get("task_state") if results else None,
                result_count=len(results),
                progress_frames=len(progresses),
                notes=[f"{len(rows)} audit rows observed"],
            )
        )
    return _finalize(
        workload="E1",
        mode="A",
        started_at=started_at,
        environment=_environment(args),
        trials=trials,
        out_dir=out_dir,
    )


async def run_e4(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    trials: list[TrialResult] = []
    prompt = "Write exactly three short sentences about NATS agent communication."
    for trial in range(1, args.trials + 1):
        response = await post_command(
            args.api_base,
            agent_id=args.streaming_agent,
            sender_id=RUNNER_ID,
            body=prompt,
        )
        task_id = response["task_id"]
        rows, first_progress_ms, task_latency_ms = await _poll_task_with_progress(
            args.api_base,
            task_id=task_id,
            timeout_sec=180,
        )
        results = _result_rows(rows)
        progresses = _progress_rows(rows)
        notes = ["model-inclusive timing"]
        if not progresses:
            notes.append("no task.progress rows observed")
        trials.append(
            TrialResult(
                trial=trial,
                task_id=task_id,
                first_progress_ms=first_progress_ms,
                task_latency_ms=task_latency_ms,
                result_state=results[0].get("task_state") if results else None,
                result_count=len(results),
                progress_frames=len(progresses),
                semantic_failures=0 if results else 1,
                notes=notes,
            )
        )
    return _finalize(
        workload="E4",
        mode="A",
        started_at=started_at,
        environment=_environment(args, streaming_agent=args.streaming_agent),
        trials=trials,
        out_dir=out_dir,
    )


async def _register_offline_fixture(args: Any, agent_id: str) -> None:
    metadata = {
        "runtime.kind": "native",
        "runtime.roles": ["worker"],
        "runtime.heartbeat_interval_sec": 10,
        "runtime.deployment": "test",
        "runtime.tags": ["benchmark", "offline-fixture"],
    }
    nc = await _connect_nats(args.nats_url, token=args.nats_token)
    try:
        await _publish_plain(nc, f"agents.{agent_id}.register", register_envelope(agent_id, metadata=metadata))
        await _publish_plain(nc, f"agents.{agent_id}.heartbeat", heartbeat_envelope(agent_id))
    finally:
        await nc.close()


async def run_e5(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    trials: list[TrialResult] = []
    for trial in range(1, args.trials + 1):
        offline_agent = f"bench-offline-{uuid.uuid4().hex[:8]}"
        await _register_offline_fixture(args, offline_agent)
        start = time.perf_counter()
        response = await post_command(
            args.api_base,
            agent_id=offline_agent,
            sender_id=RUNNER_ID,
            body="printf offline",
        )
        task_id = response["task_id"]
        try:
            rows = await wait_for_messages(args.api_base, task_id=task_id, timeout_sec=120)
        except TimeoutError as exc:
            raise TimeoutError("watchdog did not synthesize recipient_offline") from exc
        recovery_ms = _ms_since(start)
        results = _result_rows(rows)
        result = results[0] if results else {}
        poison = await query_poison(args.api_base, agent_id=offline_agent)
        error = (result.get("payload") or {}).get("error")
        trials.append(
            TrialResult(
                trial=trial,
                task_id=task_id,
                recovery_ms=recovery_ms,
                result_state=result.get("task_state"),
                result_count=len(results),
                poison_events=len(poison),
                semantic_failures=0 if error == "recipient_offline" else 1,
                notes=[f"offline_agent={offline_agent}", f"error={error}"],
            )
        )
    return _finalize(
        workload="E5",
        mode="A",
        started_at=started_at,
        environment=_environment(args),
        trials=trials,
        out_dir=out_dir,
    )


async def run_e7(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    trials: list[TrialResult] = []
    nc = await _connect_nats(args.nats_url, token=args.nats_token)
    try:
        js = nc.jetstream()
        for trial in range(1, args.trials + 1):
            env = command_envelope(
                sender_id=RUNNER_ID,
                recipient_id=args.target_agent,
                body=f"printf edgecitadel-e7-dedupe-{trial}",
            )
            start = time.perf_counter()
            first_ack = await _publish_js(js, f"agents.{args.target_agent}.inbox", env, msg_id=env["id"])
            await _publish_plain(nc, f"agents.{RUNNER_ID}.outbox", env)
            second_ack = await _publish_js(js, f"agents.{args.target_agent}.inbox", env, msg_id=env["id"])
            rows = await wait_for_messages(args.api_base, task_id=env["task_id"], timeout_sec=60)
            latency_ms = _ms_since(start)
            results = _result_rows(rows)
            duplicate = bool(getattr(second_ack, "duplicate", False))
            first_seq = getattr(first_ack, "seq", None)
            second_seq = getattr(second_ack, "seq", None)
            if first_seq is not None and second_seq == first_seq:
                duplicate = True
            trials.append(
                TrialResult(
                    trial=trial,
                    task_id=env["task_id"],
                    task_latency_ms=latency_ms,
                    result_state=results[0].get("task_state") if results else None,
                    result_count=len(results),
                    duplicates_seen=1 if duplicate else 0,
                    semantic_failures=0 if len(results) == 1 else 1,
                    notes=[f"first_seq={first_seq}", f"second_seq={second_seq}", f"second_duplicate={duplicate}"],
                )
            )
    finally:
        await nc.close()
    return _finalize(
        workload="E7",
        mode="A",
        started_at=started_at,
        environment=_environment(args),
        trials=trials,
        out_dir=out_dir,
    )


async def run_e6(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    trials: list[TrialResult] = []
    fixture = _repo_root() / "scripts" / "research" / "fixtures" / "crash_consumer.py"
    counter = _repo_root() / "tmp" / f"bench-crash-{uuid.uuid4().hex[:8]}.count"
    counter.parent.mkdir(parents=True, exist_ok=True)
    nc = await _connect_nats(args.nats_url, token=args.nats_token)
    try:
        js = nc.jetstream()
        for trial in range(1, args.trials + 1):
            env = command_envelope(
                sender_id=RUNNER_ID,
                recipient_id="bench-crash",
                body=f"crash redelivery {trial}",
            )
            start = time.perf_counter()
            crash = await asyncio.create_subprocess_exec(
                sys.executable,
                str(fixture),
                "--mode",
                "crash-first",
                "--counter",
                str(counter),
                cwd=str(_repo_root()),
                env=_python_env(args),
            )
            await asyncio.sleep(0.5)
            await _publish_js(js, "agents.bench-crash.inbox", env)
            await _publish_plain(nc, f"agents.{RUNNER_ID}.outbox", env)
            code = await crash.wait()
            if code != 91:
                raise RuntimeError(f"crash fixture exited with {code}, expected 91")
            complete = await asyncio.create_subprocess_exec(
                sys.executable,
                str(fixture),
                "--mode",
                "complete",
                "--counter",
                str(counter),
                cwd=str(_repo_root()),
                env=_python_env(args),
            )
            rows = await wait_for_messages(args.api_base, task_id=env["task_id"], timeout_sec=60)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(complete.wait(), timeout=5)
            side_effects = int(counter.read_text().strip()) if counter.exists() else 0
            results = _result_rows(rows)
            trials.append(
                TrialResult(
                    trial=trial,
                    task_id=env["task_id"],
                    recovery_ms=_ms_since(start),
                    result_state=results[0].get("task_state") if results else None,
                    result_count=len(results),
                    semantic_failures=0 if side_effects == 2 and len(results) == 1 else 1,
                    notes=[f"side_effect_counter={side_effects}", "duplicate side effect is expected"],
                )
            )
    finally:
        await nc.close()
    return _finalize(
        workload="E6",
        mode="A",
        started_at=started_at,
        environment=_environment(args, fixture_agent="bench-crash"),
        trials=trials,
        out_dir=out_dir,
    )


async def _run_fixture_workload(
    args: Any,
    *,
    fixture_name: str,
    fixture_args: list[str],
    workload: str,
    mode: str,
    trials: list[TrialResult],
    started_at: str,
    out_dir: Path,
    extra_env: dict[str, Any] | None = None,
) -> tuple[BenchmarkRun, Path]:
    return _finalize(
        workload=workload,
        mode=mode,
        started_at=started_at,
        environment=_environment(args, fixture=fixture_name, **(extra_env or {})),
        trials=trials,
        out_dir=out_dir,
    )


async def run_e2(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    fixture = _repo_root() / "scripts" / "research" / "fixtures" / "delegation_agents.py"
    trials: list[TrialResult] = []
    async with _subprocess(
        sys.executable,
        str(fixture),
        "--scenario",
        "e2",
        env=_python_env(args),
    ):
        await asyncio.sleep(1.0)
        for trial in range(1, args.trials + 1):
            context_id = str(uuid.uuid4())
            start = time.perf_counter()
            response = await post_command(
                args.api_base,
                agent_id="bench-delegator",
                sender_id=RUNNER_ID,
                body=f"delegate trial {trial}",
                context_id=context_id,
            )
            task_id = response["task_id"]
            rows = await wait_for_context_messages(
                args.api_base,
                context_id=context_id,
                expected_results=2,
                timeout_sec=60,
            )
            results = _result_rows(rows)
            delegations = [row for row in rows if row.get("type") == "delegation"]
            semantic_failures = 0
            if not delegations or len(results) < 2:
                semantic_failures = 1
            trials.append(
                TrialResult(
                    trial=trial,
                    task_id=task_id,
                    context_id=context_id,
                    task_latency_ms=_ms_since(start),
                    result_state="completed" if any(row.get("task_id") == task_id and row.get("task_state") == "completed" for row in results) else None,
                    result_count=len(results),
                    semantic_failures=semantic_failures,
                    notes=[f"delegation_rows={len(delegations)}", f"context_rows={len(rows)}"],
                )
            )
    return await _run_fixture_workload(
        args,
        fixture_name="delegation_agents.py",
        fixture_args=["--scenario", "e2"],
        workload="E2",
        mode="A",
        trials=trials,
        started_at=started_at,
        out_dir=out_dir,
    )


async def run_e3(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    fixture = _repo_root() / "scripts" / "research" / "fixtures" / "delegation_agents.py"
    trials: list[TrialResult] = []
    async with _subprocess(
        sys.executable,
        str(fixture),
        "--scenario",
        "e3",
        env=_python_env(args),
    ):
        await asyncio.sleep(1.0)
        for trial in range(1, args.trials + 1):
            context_id = str(uuid.uuid4())
            start = time.perf_counter()
            response = await post_command(
                args.api_base,
                agent_id="bench-hop-1",
                sender_id=RUNNER_ID,
                body=f"multihop trial {trial}",
                context_id=context_id,
            )
            rows = await wait_for_context_messages(
                args.api_base,
                context_id=context_id,
                expected_results=3,
                timeout_sec=60,
            )
            hop_counts = sorted(row.get("hop_count") for row in rows if row.get("type") == "delegation" and row.get("hop_count") is not None)
            limit_context = str(uuid.uuid4())
            limit_env = delegation_envelope(
                sender_id=RUNNER_ID,
                recipient_id="bench-hop-limit",
                body="limit",
                context_id=limit_context,
                hop_count=8,
            )
            nc = await _connect_nats(args.nats_url, token=args.nats_token)
            try:
                js = nc.jetstream()
                await _publish_js(js, "agents.bench-hop-limit.inbox", limit_env)
                await _publish_plain(nc, f"agents.{RUNNER_ID}.outbox", limit_env)
            finally:
                await nc.close()
            limit_rows = await wait_for_messages(args.api_base, task_id=limit_env["task_id"], timeout_sec=60)
            rejected = any(row.get("task_state") == "rejected" and (row.get("payload") or {}).get("error") == "hop_count_exceeded" for row in limit_rows)
            semantic_failures = 0 if hop_counts and rejected else 1
            trials.append(
                TrialResult(
                    trial=trial,
                    task_id=response["task_id"],
                    context_id=context_id,
                    task_latency_ms=_ms_since(start),
                    result_count=len(_result_rows(rows)),
                    semantic_failures=semantic_failures,
                    notes=[f"hop_counts={hop_counts}", f"limit_rejected={rejected}"],
                )
            )
    return await _run_fixture_workload(
        args,
        fixture_name="delegation_agents.py",
        fixture_args=["--scenario", "e3"],
        workload="E3",
        mode="A",
        trials=trials,
        started_at=started_at,
        out_dir=out_dir,
    )


async def run_e8(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    fixture = _repo_root() / "scripts" / "research" / "fixtures" / "cancel_agent.py"
    trials: list[TrialResult] = []
    nc = await _connect_nats(args.nats_url, token=args.nats_token)
    try:
        js = nc.jetstream()
        async with _subprocess(sys.executable, str(fixture), env=_python_env(args)):
            await asyncio.sleep(1.0)
            for trial in range(1, args.trials + 1):
                cmd = command_envelope(
                    sender_id=RUNNER_ID,
                    recipient_id="bench-cancel",
                    body=f"long running {trial}",
                )
                start = time.perf_counter()
                await _publish_js(js, "agents.bench-cancel.inbox", cmd)
                await _publish_plain(nc, f"agents.{RUNNER_ID}.outbox", cmd)
                progress_seen = 0
                while progress_seen < 2:
                    rows = await wait_for_messages(
                        args.api_base,
                        task_id=cmd["task_id"],
                        timeout_sec=5,
                        require_result=False,
                    )
                    progress_seen = len(_progress_rows(rows))
                    await asyncio.sleep(0.1)
                cancel_at = time.perf_counter()
                cancel = cancel_envelope(
                    sender_id=RUNNER_ID,
                    recipient_id="bench-cancel",
                    task_id=cmd["task_id"],
                )
                await _publish_js(js, "agents.bench-cancel.inbox", cancel)
                await _publish_plain(nc, f"agents.{RUNNER_ID}.outbox", cancel)
                rows = await wait_for_messages(args.api_base, task_id=cmd["task_id"], timeout_sec=30)
                results = _result_rows(rows)
                progresses = _progress_rows(rows)
                progress_after_cancel = sum(
                    1
                    for row in progresses
                    if row.get("timestamp") and row["timestamp"] > datetime.fromtimestamp(cancel_at, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                )
                final_state = results[0].get("task_state") if results else None
                trials.append(
                    TrialResult(
                        trial=trial,
                        task_id=cmd["task_id"],
                        task_latency_ms=_ms_since(start),
                        result_state=final_state,
                        result_count=len(results),
                        progress_frames=len(progresses),
                        semantic_failures=0 if final_state == "canceled" else 1,
                        notes=[f"progress_after_cancel={progress_after_cancel}"],
                    )
                )
    finally:
        await nc.close()
    return _finalize(
        workload="E8",
        mode="A",
        started_at=started_at,
        environment=_environment(args, fixture_agent="bench-cancel"),
        trials=trials,
        out_dir=out_dir,
    )


async def _run_mqtt_publish(args: Any, *, topic: str, payload: str) -> None:
    script = _repo_root() / "scripts" / "research" / "mqtt_publish.mjs"
    proc = await asyncio.create_subprocess_exec(
        "node",
        str(script),
        "--url",
        args.mqtt_url,
        "--topic",
        topic,
        "--payload",
        payload,
        cwd=str(_repo_root()),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"mqtt publish failed: {stderr.decode().strip() or stdout.decode().strip()}")


async def _wait_for_gateway_log(api_base: str, topic: str, timeout_sec: float = 20) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    async with httpx.AsyncClient(timeout=10) as client:
        while time.monotonic() < deadline:
            response = await client.get(
                f"{api_base}/messages",
                params={"agent_id": "bench-mqtt-gateway", "type": "log", "limit": 100},
            )
            response.raise_for_status()
            rows = response.json()
            if any((row.get("payload") or {}).get("mqtt_topic") == topic for row in rows):
                return rows
            await asyncio.sleep(0.25)
    raise TimeoutError(f"no gateway log observed for {topic}")


async def run_e9(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    fixture = _repo_root() / "scripts" / "research" / "fixtures" / "mqtt_gateway.py"
    trials: list[TrialResult] = []
    async with _subprocess(sys.executable, str(fixture), env=_python_env(args)):
        await asyncio.sleep(1.0)
        for trial in range(1, args.trials + 1):
            topic = f"devices/pi-{trial}/telemetry"
            start = time.perf_counter()
            await _run_mqtt_publish(args, topic=topic, payload='{"temperature_c":22.5}')
            rows = await _wait_for_gateway_log(args.api_base, topic)
            matched = [row for row in rows if (row.get("payload") or {}).get("mqtt_topic") == topic]
            trials.append(
                TrialResult(
                    trial=trial,
                    task_id=f"mqtt-log-{trial}",
                    task_latency_ms=_ms_since(start),
                    result_count=len(matched),
                    semantic_failures=0 if matched else 1,
                    notes=[f"mqtt_topic={topic}", "normalized telemetry log"],
                )
            )
    return _finalize(
        workload="E9",
        mode="B",
        started_at=started_at,
        environment=_environment(args, mqtt_url=args.mqtt_url),
        trials=trials,
        out_dir=out_dir,
    )


async def run_e10(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    fixture = _repo_root() / "scripts" / "research" / "fixtures" / "mqtt_gateway.py"
    trials: list[TrialResult] = []
    async with _subprocess(sys.executable, str(fixture), env=_python_env(args)):
        await asyncio.sleep(1.0)
        for trial in range(1, args.trials + 1):
            topic = f"devices/pi-{trial}/command/{args.target_agent}"
            body = f"printf mqtt-{trial}"
            start = time.perf_counter()
            await _run_mqtt_publish(args, topic=topic, payload=json.dumps({"body": body}))
            rows = await _wait_for_gateway_log(args.api_base, topic)
            task_ids = [
                (row.get("payload") or {}).get("task_id")
                for row in rows
                if (row.get("payload") or {}).get("mqtt_topic") == topic
            ]
            task_id = next((item for item in task_ids if item), None)
            if not task_id:
                raise TimeoutError(f"gateway log missing task_id for {topic}")
            task_rows = await wait_for_messages(args.api_base, task_id=task_id, timeout_sec=60)
            results = _result_rows(task_rows)
            trials.append(
                TrialResult(
                    trial=trial,
                    task_id=task_id,
                    task_latency_ms=_ms_since(start),
                    result_state=results[0].get("task_state") if results else None,
                    result_count=len(results),
                    semantic_failures=0 if len(results) == 1 else 1,
                    notes=[f"mqtt_topic={topic}", "mqtt command normalized to native command"],
                )
            )
    return _finalize(
        workload="E10",
        mode="B",
        started_at=started_at,
        environment=_environment(args, mqtt_url=args.mqtt_url),
        trials=trials,
        out_dir=out_dir,
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def run_e11(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    await assert_stack_ready(args.api_base)
    port = _free_port()
    env = _python_env(args)
    env["A2A_GATEWAY_PORT"] = str(port)
    env["A2A_TARGET_AGENT"] = args.target_agent
    gateway_url = f"http://127.0.0.1:{port}"
    trials: list[TrialResult] = []
    async with _subprocess(
        sys.executable,
        "-m",
        "uvicorn",
        "scripts.research.fixtures.a2a_mock_gateway:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        env=env,
    ):
        await asyncio.sleep(1.5)
        async with httpx.AsyncClient(timeout=10) as client:
            for trial in range(1, args.trials + 1):
                direct_start = time.perf_counter()
                direct = await post_command(
                    args.api_base,
                    agent_id=args.target_agent,
                    sender_id=RUNNER_ID,
                    body=f"printf gateway-control-{trial}",
                )
                await wait_for_messages(args.api_base, task_id=direct["task_id"], timeout_sec=60)
                direct_ms = _ms_since(direct_start)

                gateway_start = time.perf_counter()
                response = await client.post(
                    f"{gateway_url}/tasks/send",
                    json={"target_agent": args.target_agent, "body": f"printf gateway-{trial}"},
                )
                response.raise_for_status()
                gateway_task = response.json()["task_id"]
                rows = await wait_for_messages(args.api_base, task_id=gateway_task, timeout_sec=60)
                gateway_ms = _ms_since(gateway_start)
                results = _result_rows(rows)
                trials.append(
                    TrialResult(
                        trial=trial,
                        task_id=gateway_task,
                        task_latency_ms=gateway_ms,
                        result_state=results[0].get("task_state") if results else None,
                        result_count=len(results),
                        semantic_failures=0 if len(results) == 1 else 1,
                        notes=[f"native_control_ms={direct_ms}", f"gateway_overhead_ms={round(gateway_ms - direct_ms, 3)}"],
                    )
                )
    return _finalize(
        workload="E11",
        mode="C",
        started_at=started_at,
        environment=_environment(args, gateway_url=gateway_url),
        trials=trials,
        out_dir=out_dir,
    )


async def run_e12(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    started_at = now_iso()
    nats_server = shutil.which("nats-server")
    if not nats_server:
        raise RuntimeError("nats-server binary is required for E12 disposable auth probe")
    conf = _repo_root() / "scripts" / "research" / "fixtures" / "nats-auth-e12.conf"
    proc = await asyncio.create_subprocess_exec(
        nats_server,
        "-c",
        str(conf),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    trials: list[TrialResult] = []
    try:
        await asyncio.sleep(1.0)
        url = "nats://127.0.0.1:14222"
        false_allows = 0
        false_denies = 0

        sender = await _connect_nats(url, user="bench_sender", password="bench")
        worker = await _connect_nats(url, user="bench_worker", password="bench")
        denied = await _connect_nats(url, user="bench_denied", password="bench")
        try:
            allowed_seen = asyncio.Event()

            async def cb(_msg: Any) -> None:
                allowed_seen.set()

            sub = await worker.subscribe("agents.shell-1.inbox", cb=cb)
            await sender.publish("agents.shell-1.inbox", b"allowed")
            await sender.flush()
            try:
                await asyncio.wait_for(allowed_seen.wait(), timeout=2)
            except asyncio.TimeoutError:
                false_denies += 1
            await sub.unsubscribe()

            try:
                await denied.publish("agents.shell-1.inbox", b"denied")
                await denied.flush()
                false_allows += 1
            except Exception:
                pass

            try:
                denied_sub = await denied.subscribe("agents.shell-1.outbox")
                await denied_sub.unsubscribe()
                false_allows += 1
            except Exception:
                pass
        finally:
            await sender.close()
            await worker.close()
            await denied.close()

        trials.append(
            TrialResult(
                trial=1,
                task_id="e12-auth-policy",
                result_state="completed",
                semantic_failures=false_allows + false_denies,
                notes=[f"false_allows={false_allows}", f"false_denies={false_denies}"],
            )
        )
    finally:
        if proc.returncode is None:
            proc.terminate()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=5)
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    return _finalize(
        workload="E12",
        mode="A",
        started_at=started_at,
        environment={"nats_url": "nats://127.0.0.1:14222", "nats_config": str(conf)},
        trials=trials,
        out_dir=out_dir,
    )


async def run_security_temporal(args: Any, out_dir: Path) -> tuple[BenchmarkRun, Path]:
    from scripts.research.fixtures.iot_event_simulator import run_scenarios

    started_at = now_iso()
    trials: list[TrialResult] = []
    for index, scenario in enumerate(run_scenarios(), start=1):
        metrics = scenario.evaluate()
        semantic_failures = (
            metrics["unsafe_recommendations"]
            + metrics["fake_tool_calls_attempted"]
            + int(metrics["prompt_injection_followed"])
            + metrics["stale_events_accepted"]
        )
        trials.append(
            TrialResult(
                trial=index,
                task_id=scenario.scenario_id,
                result_state="safe" if semantic_failures == 0 else "failed",
                semantic_failures=semantic_failures,
                notes=[f"{key}={value}" for key, value in metrics.items()],
            )
        )
    return _finalize(
        workload="SECURITY_TEMPORAL",
        mode="B",
        started_at=started_at,
        environment={"simulator": "synthetic", "real_devices": False},
        trials=trials,
        out_dir=out_dir,
    )
