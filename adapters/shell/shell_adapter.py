#!/usr/bin/env python3
"""
EdgeCitadel Shell Adapter — L1 reference implementation of docs/agent-contract.md.

This is the minimum viable conformant agent: connects over MQTT, publishes an
Agent Card on register, heartbeats, listens on its inbox, and runs a configured
shell command for each inbound `command`. If this adapter needs more than ~300
lines, the contract is wrong.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from jsonschema import Draft202012Validator

log = logging.getLogger("shell-adapter")

# ── Config ────────────────────────────────────────────────────────────────────

def _env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        sys.exit(f"{name} is required")
    return val or ""

AGENT_ID          = _env("AGENT_ID", required=True)
AGENT_DISPLAY     = _env("AGENT_DISPLAY", AGENT_ID.replace("-", " ").title())
AGENT_ROLE        = _env("AGENT_ROLE", "Shell Worker")
AGENT_DEVICE_TYPE = _env("AGENT_DEVICE_TYPE", "server")
AGENT_TAGS        = [t.strip() for t in _env("AGENT_TAGS", "").split(",") if t.strip()]
CITADEL_HOST      = _env("CITADEL_HOST", "127.0.0.1")
CITADEL_PORT      = int(_env("CITADEL_PORT", "1883"))
NATS_TOKEN        = _env("NATS_TOKEN", required=True)
HEARTBEAT_SEC     = int(_env("HEARTBEAT_SEC", "30"))
SHELL_COMMAND     = _env("SHELL_COMMAND", "")  # empty → echo body
SHELL_TIMEOUT_SEC = int(_env("SHELL_TIMEOUT_SEC", "60"))

SCHEMA_DIR = Path(_env("SCHEMA_DIR", str(Path(__file__).resolve().parent.parent.parent / "schemas")))

# ── Schema validators ─────────────────────────────────────────────────────────

def _load_validator(name: str) -> Draft202012Validator:
    path = SCHEMA_DIR / name
    with open(path) as f:
        return Draft202012Validator(json.load(f))

ENVELOPE_VALIDATOR = _load_validator("envelope.v1.json")
CARD_VALIDATOR     = _load_validator("agent-card.v1.json")

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"

def local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"

def envelope(type_: str, payload: dict, *, recipient_id: str | None = None,
             correlation_id: str | None = None, chain_id: str | None = None) -> dict:
    env: dict = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": type_,
        "sender_id": AGENT_ID,
        "timestamp": now_iso(),
        "payload": payload,
    }
    if recipient_id: env["recipient_id"] = recipient_id
    if correlation_id: env["correlation_id"] = correlation_id
    if chain_id: env["chain_id"] = chain_id
    # Validate before emitting — fail loud on the producer, not silently on the consumer.
    errors = sorted(ENVELOPE_VALIDATOR.iter_errors(env), key=lambda e: e.path)
    if errors:
        raise RuntimeError(f"envelope validation failed: {errors[0].message}")
    return env

def agent_card() -> dict:
    card = {
        "v": 1,
        "agent_id": AGENT_ID,
        "display_name": AGENT_DISPLAY,
        "role": AGENT_ROLE,
        "device_type": AGENT_DEVICE_TYPE,
        "runtime": {
            "framework": "shell",
            "framework_version": "0.1.0",
            "model": None,
            "model_host": "none",
            "roles": ["worker"],
            "conformance": "L1",
        },
        "tags": AGENT_TAGS,
        "capabilities": [
            {
                "name": "shell_exec",
                "description": (
                    f"Runs configured shell command ({SHELL_COMMAND!r}) with the "
                    "message body as stdin. stdout becomes the result body."
                ) if SHELL_COMMAND else "Echoes the message body back as the result.",
                "modalities": ["text"],
            }
        ],
        "endpoints": {
            "inbox": f"agents.{AGENT_ID}.inbox",
            "mcp": None,
            "a2a": None,
        },
        "heartbeat_interval_sec": HEARTBEAT_SEC,
        "listens_broadcast": True,
        "chain_depth_limit": 3,
        "timestamp": now_iso(),
    }
    errors = sorted(CARD_VALIDATOR.iter_errors(card), key=lambda e: e.path)
    if errors:
        raise RuntimeError(f"agent-card validation failed: {errors[0].message}")
    return card

# ── State machine ─────────────────────────────────────────────────────────────

@dataclass
class State:
    status: str = "offline"  # offline | connecting | online | busy | error
    stop: bool = False
    client: mqtt.Client | None = None

S = State()

def set_status(new: str, reason: str | None = None):
    if S.status == new: return
    log.info("status: %s → %s%s", S.status, new, f" ({reason})" if reason else "")
    S.status = new
    if S.client and S.client.is_connected():
        payload = {"status": new}
        if reason: payload["reason"] = reason
        publish(f"agents/{AGENT_ID}/status", envelope("status", payload), retain=True)

# ── Publish ───────────────────────────────────────────────────────────────────

def publish(topic: str, env: dict, *, retain: bool = False):
    assert S.client is not None
    S.client.publish(topic, json.dumps(env), qos=1, retain=retain)

def send_result(to: str, correlation_id: str, body: str, error: str | None = None):
    payload: dict = {"body": body}
    if error: payload["error"] = error
    publish(f"agents/{to}/inbox",
            envelope("result", payload, recipient_id=to, correlation_id=correlation_id))

# ── Handlers ──────────────────────────────────────────────────────────────────

def run_shell(body: str) -> tuple[str, str | None]:
    """Returns (stdout, error_or_None)."""
    if not SHELL_COMMAND:
        return body, None  # echo mode
    try:
        result = subprocess.run(
            SHELL_COMMAND, shell=True, input=body, capture_output=True,
            text=True, timeout=SHELL_TIMEOUT_SEC,
        )
        if result.returncode != 0:
            return result.stderr or result.stdout, f"exit_code_{result.returncode}"
        return result.stdout, None
    except subprocess.TimeoutExpired:
        return "", f"timeout_after_{SHELL_TIMEOUT_SEC}s"
    except Exception as e:
        return "", f"exec_error: {e}"

def handle_envelope(env: dict):
    errors = sorted(ENVELOPE_VALIDATOR.iter_errors(env), key=lambda e: e.path)
    if errors:
        log.warning("dropping non-conformant message: %s", errors[0].message)
        return

    t = env["type"]
    sender = env["sender_id"]

    if t in ("command", "delegation"):
        corr = env.get("correlation_id") or env["id"]
        if S.status == "error":
            send_result(sender, corr, "", error="agent_error")
            return
        if S.status == "busy":
            send_result(sender, corr, "", error="busy")
            return
        set_status("busy")
        body = (env.get("payload") or {}).get("body", "")
        stdout, err = run_shell(body)
        send_result(sender, corr, stdout, error=err)
        set_status("online")
    elif t == "broadcast":
        log.info("broadcast from %s: %s", sender, (env.get("payload") or {}).get("body", ""))
    # heartbeat/result/status/log from others are not our concern as a minimal L1 worker.

def on_mqtt_message(_client, _ud, msg):
    try:
        env = json.loads(msg.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        log.warning("unparseable message on %s: %s", msg.topic, e)
        return
    try:
        handle_envelope(env)
    except Exception:
        log.exception("handler crashed")
        set_status("error", reason="handler_exception")

def on_mqtt_connect(client, _ud, _flags, rc, _props=None):
    if rc != 0:
        set_status("error", reason=f"mqtt_connect_rc_{rc}")
        return
    log.info("connected to %s:%d", CITADEL_HOST, CITADEL_PORT)
    client.subscribe(f"agents/{AGENT_ID}/inbox", qos=1)
    client.subscribe("system/broadcast", qos=0)
    # Retained register so late subscribers see our Card.
    publish(f"agents/{AGENT_ID}/register", envelope("register", agent_card()), retain=True)
    set_status("online")

def on_mqtt_disconnect(_client, _ud, rc, _props=None, _reason=None):
    if S.stop: return
    log.warning("disconnected (rc=%s); paho will auto-reconnect", rc)
    set_status("error", reason=f"mqtt_disconnect_rc_{rc}")

# ── Heartbeat loop ────────────────────────────────────────────────────────────

def _cpu_percent() -> float:
    try:
        import psutil  # optional
        return float(psutil.cpu_percent(interval=None))
    except ImportError:
        return 0.0

def _mem_percent() -> float:
    try:
        import psutil
        return float(psutil.virtual_memory().percent)
    except ImportError:
        return 0.0

def heartbeat_loop():
    while not S.stop:
        time.sleep(HEARTBEAT_SEC)
        if S.stop or not (S.client and S.client.is_connected()): continue
        try:
            payload = {
                "status": S.status,
                "cpu_percent": _cpu_percent(),
                "memory_percent": _mem_percent(),
                "ip_address": local_ip(),
                "power_source": "unknown",
            }
            publish(f"agents/{AGENT_ID}/heartbeat", envelope("heartbeat", payload))
        except Exception:
            log.exception("heartbeat failed")

# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"shell-adapter-{AGENT_ID}",
        clean_session=True,
    )
    client.username_pw_set("shell-adapter", NATS_TOKEN)

    # Last Will: if we drop ungracefully, broker publishes offline for us.
    will = envelope("status", {"status": "offline", "reason": "will"})
    client.will_set(f"agents/{AGENT_ID}/status", json.dumps(will), qos=1, retain=True)

    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message
    client.on_disconnect = on_mqtt_disconnect
    S.client = client

    def shutdown(*_):
        if S.stop: return
        S.stop = True
        log.info("shutting down")
        try:
            set_status("offline", reason="clean_shutdown")
            client.disconnect()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    set_status("connecting")
    client.connect(CITADEL_HOST, CITADEL_PORT, keepalive=max(HEARTBEAT_SEC * 2, 60))

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    try:
        client.loop_forever(retry_first_connection=True)
    finally:
        shutdown()

if __name__ == "__main__":
    main()
