#!/usr/bin/env python3
"""Simulate a conversation between macmini-backend and openclaw agents via NATS."""

import asyncio
import json
import time
import uuid
import argparse

import nats


NATS_URL = "nats://localhost:4222"


async def publish(nc, subject: str, payload: dict):
    await nc.publish(subject, json.dumps(payload).encode())
    print(f"  -> {subject}: {payload.get('type', '?')} from {payload.get('sender', '?')}")
    await asyncio.sleep(0.3)


async def register_agents(nc):
    """Register both agents so they appear in the dashboard."""
    await publish(nc, "agents.macmini-backend.register", {
        "agent_id": "macmini-backend",
        "sender_id": "macmini-backend",
        "display_name": "MacMini Backend",
        "type": "register",
        "role": "coordinator",
        "device_type": "mac-mini",
        "model": "gpt-4o",
        "ip_address": "192.168.1.10",
        "capabilities": ["task_routing", "code_review", "orchestration"],
    })

    await publish(nc, "agents.openclaw.register", {
        "agent_id": "openclaw",
        "sender_id": "openclaw",
        "display_name": "OpenClaw Edge Agent",
        "type": "register",
        "role": "executor",
        "device_type": "edge-node",
        "model": "llama-3.1-8b",
        "ip_address": "192.168.1.50",
        "capabilities": ["code_execution", "file_ops", "system_monitoring", "local_inference"],
    })


async def send_heartbeats(nc):
    """Send heartbeats so agents show as online."""
    await publish(nc, "agents.macmini-backend.heartbeat", {
        "agent_id": "macmini-backend",
        "sender_id": "macmini-backend",
        "type": "heartbeat",
        "status": "online",
        "cpu_percent": 23.5,
        "memory_percent": 41.2,
        "ip_address": "192.168.1.10",
    })

    await publish(nc, "agents.openclaw.heartbeat", {
        "agent_id": "openclaw",
        "sender_id": "openclaw",
        "type": "heartbeat",
        "status": "online",
        "cpu_percent": 55.8,
        "memory_percent": 67.3,
        "ip_address": "192.168.1.50",
    })


async def simulate_conversation(nc):
    """Simulate a multi-turn conversation between the two agents."""

    # Conversation 1: System health check
    corr1 = str(uuid.uuid4())
    print("\n--- Conversation 1: System Health Check ---")

    await publish(nc, "agents.openclaw.inbox", {
        "sender": "macmini-backend",
        "sender_id": "macmini-backend",
        "receiver": "openclaw",
        "receiver_id": "openclaw",
        "type": "command",
        "message_type": "command",
        "correlation_id": corr1,
        "command": "system_health_check",
        "message": "Run system health check on disk, cpu, memory, network",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(1)

    await publish(nc, "agents.openclaw.outbox", {
        "sender": "openclaw",
        "sender_id": "openclaw",
        "receiver": "macmini-backend",
        "receiver_id": "macmini-backend",
        "type": "result",
        "message_type": "result",
        "correlation_id": corr1,
        "status": "success",
        "message": "Health check complete",
        "result": {
            "disk_usage": "42% (128GB / 300GB)",
            "cpu_load": "55.8% (4 cores)",
            "memory_usage": "67.3% (4.2GB / 6.3GB)",
            "network": "192.168.1.50, latency 2ms to gateway",
            "uptime": "14d 6h 23m",
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(1.5)

    # Conversation 2: Code deployment task
    corr2 = str(uuid.uuid4())
    print("\n--- Conversation 2: Deploy Updated Model ---")

    await publish(nc, "agents.openclaw.inbox", {
        "sender": "macmini-backend",
        "sender_id": "macmini-backend",
        "receiver": "openclaw",
        "receiver_id": "openclaw",
        "type": "command",
        "message_type": "command",
        "correlation_id": corr2,
        "command": "deploy_model",
        "message": "Deploy llama-3.1-8b-instruct-q4 model",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(1)

    await publish(nc, "agents.openclaw.outbox", {
        "sender": "openclaw",
        "sender_id": "openclaw",
        "receiver": "macmini-backend",
        "receiver_id": "macmini-backend",
        "type": "info",
        "message_type": "info",
        "correlation_id": corr2,
        "message": "Downloading model llama-3.1-8b-instruct-q4.gguf (4.3GB)... 45% complete",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(2)

    await publish(nc, "agents.openclaw.outbox", {
        "sender": "openclaw",
        "sender_id": "openclaw",
        "receiver": "macmini-backend",
        "receiver_id": "macmini-backend",
        "type": "info",
        "message_type": "info",
        "correlation_id": corr2,
        "message": "Download complete. Stopping inference server for hot-swap...",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(1.5)

    await publish(nc, "agents.openclaw.outbox", {
        "sender": "openclaw",
        "sender_id": "openclaw",
        "receiver": "macmini-backend",
        "receiver_id": "macmini-backend",
        "type": "result",
        "message_type": "result",
        "correlation_id": corr2,
        "status": "success",
        "message": "Model deployed successfully",
        "result": {
            "model_deployed": "llama-3.1-8b-instruct-q4.gguf",
            "size": "4.3GB",
            "inference_server": "restarted",
            "warmup_latency_ms": 320,
            "tokens_per_second": 42.7,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(1.5)

    # Conversation 3: Run a local inference task
    corr3 = str(uuid.uuid4())
    print("\n--- Conversation 3: Local Inference Request ---")

    await publish(nc, "agents.openclaw.inbox", {
        "sender": "macmini-backend",
        "sender_id": "macmini-backend",
        "receiver": "openclaw",
        "receiver_id": "openclaw",
        "type": "command",
        "message_type": "command",
        "correlation_id": corr3,
        "command": "run_inference",
        "message": "Summarize the system logs from the last hour and flag any anomalies.",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(2)

    await publish(nc, "agents.openclaw.outbox", {
        "sender": "openclaw",
        "sender_id": "openclaw",
        "receiver": "macmini-backend",
        "receiver_id": "macmini-backend",
        "type": "result",
        "message_type": "result",
        "correlation_id": corr3,
        "status": "success",
        "message": (
            "System log summary (last hour):\n"
            "- 142 INFO entries, 3 WARN, 0 ERROR\n"
            "- WARN: Memory usage peaked at 89% at 14:23 UTC (GC resolved)\n"
            "- WARN: NATS reconnect at 14:31 UTC (server restart detected)\n"
            "- WARN: Disk I/O spike at 14:45 UTC during model download\n"
            "No critical anomalies detected. All warnings are transient."
        ),
        "result": {
            "tokens_used": 127,
            "latency_ms": 1840,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    await asyncio.sleep(1)

    # Conversation 4: macmini-backend acknowledges
    await publish(nc, "agents.openclaw.inbox", {
        "sender": "macmini-backend",
        "sender_id": "macmini-backend",
        "receiver": "openclaw",
        "receiver_id": "openclaw",
        "type": "command",
        "message_type": "command",
        "correlation_id": corr3,
        "command": "ack",
        "message": "Inference result received. Storing in central log. No action needed.",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })


async def main():
    parser = argparse.ArgumentParser(description="Simulate agent conversation via NATS")
    parser.add_argument("--url", default=NATS_URL, help="NATS server URL")
    parser.add_argument("--loop", action="store_true", help="Keep sending heartbeats after conversation")
    args = parser.parse_args()

    nc = await nats.connect(args.url)
    print(f"Connected to NATS at {args.url}")

    print("\n=== Registering agents ===")
    await register_agents(nc)

    await asyncio.sleep(1)

    print("\n=== Sending initial heartbeats ===")
    await send_heartbeats(nc)

    await asyncio.sleep(1)

    print("\n=== Starting conversation simulation ===")
    await simulate_conversation(nc)

    print("\n=== Conversation simulation complete ===")

    if args.loop:
        print("\nSending heartbeats every 30s (Ctrl+C to stop)...")
        try:
            while True:
                await send_heartbeats(nc)
                await asyncio.sleep(30)
        except KeyboardInterrupt:
            print("\nStopped.")

    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
