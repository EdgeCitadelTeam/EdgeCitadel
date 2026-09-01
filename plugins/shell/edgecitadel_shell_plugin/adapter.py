"""EdgeCitadel shell Plugin runtime — nats-py async, JetStream pull consumer."""

from __future__ import annotations
import asyncio
import logging
from pathlib import Path

from edgecitadel_plugin_runtime.pull_consumer import Context
from edgecitadel_plugin_runtime.template import main as run_adapter

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


async def handle(env: dict, ctx: Context) -> tuple[dict, str]:
    if env["type"] != "command":
        return ({"error": "unsupported_type"}, "rejected")

    body = env["payload"].get("body", "").strip()
    args = env["payload"].get("args") or {}
    timeout_sec = int(args.get("timeout_sec", DEFAULT_TIMEOUT))

    if not body:
        return ({"error": "empty_command"}, "rejected")

    # periodic in_progress keepalive is handled by PullConsumer
    proc = await asyncio.create_subprocess_shell(
        body, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        return (
            {"error": "timeout", "body": f"command timed out after {timeout_sec}s"},
            "failed",
        )

    rc = proc.returncode
    text = (out or b"").decode(errors="replace")
    if err:
        text += "\n" + err.decode(errors="replace")
    state = "completed" if rc == 0 else "failed"
    payload = {"body": text[:64_000], "returncode": rc}
    if rc != 0:
        payload["error"] = "nonzero_exit"
    return (payload, state)


async def main():
    # template.main reads config.yaml, registers, heartbeats, drains inbox
    # We inject our handler by monkey-patching template.handle before run
    from edgecitadel_plugin_runtime import template

    template.handle = handle
    config = Path(__file__).resolve().parent / "config.yaml"
    await run_adapter(config)
