#!/usr/bin/env python3
"""Simulate a conversation between macmini-backend and openclaw agents via MQTT."""

import json
import time
import uuid
import argparse

import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
BROKER_USER = "iot_agent"
BROKER_PASS = "openclaw_secret"


def publish(client: mqtt.Client, topic: str, payload: dict):
    client.publish(topic, json.dumps(payload))
    print(f"  -> {topic}: {payload.get('type', '?')} from {payload.get('sender', '?')}")
    time.sleep(0.3)


def register_agents(client: mqtt.Client):
    """Register both agents so they appear in the dashboard."""
    publish(client, "agents/register/macmini-backend", {
        "agent_id": "macmini-backend",
        "display_name": "MacMini Backend",
        "type": "register",
        "role": "coordinator",
        "device_type": "mac-mini",
        "model": "gpt-4o",
        "ip_address": "192.168.1.10",
        "capabilities": ["task_routing", "code_review", "orchestration"],
    })

    publish(client, "agents/register/openclaw", {
        "agent_id": "openclaw",
        "display_name": "OpenClaw Edge Agent",
        "type": "register",
        "role": "executor",
        "device_type": "edge-node",
        "model": "llama-3.1-8b",
        "ip_address": "192.168.1.50",
        "capabilities": ["code_execution", "file_ops", "system_monitoring", "local_inference"],
    })


def send_heartbeats(client: mqtt.Client):
    """Send heartbeats so agents show as online."""
    publish(client, "agents/heartbeat/macmini-backend", {
        "agent_id": "macmini-backend",
        "type": "heartbeat",
        "status": "online",
        "cpu_percent": 23.5,
        "memory_percent": 41.2,
        "ip_address": "192.168.1.10",
    })

    publish(client, "agents/heartbeat/openclaw", {
        "agent_id": "openclaw",
        "type": "heartbeat",
        "status": "online",
        "cpu_percent": 55.8,
        "memory_percent": 67.3,
        "ip_address": "192.168.1.50",
    })


def simulate_conversation(client: mqtt.Client):
    """Simulate a multi-turn conversation between the two agents."""

    # Conversation 1: System health check
    corr1 = str(uuid.uuid4())
    print("\n--- Conversation 1: System Health Check ---")

    publish(client, "agents/inbox/openclaw", {
        "sender": "macmini-backend",
        "receiver": "openclaw",
        "type": "command",
        "correlation_id": corr1,
        "command": "system_health_check",
        "payload": {
            "check_targets": ["disk", "cpu", "memory", "network"],
            "verbose": True,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(1)

    publish(client, "agents/inbox/macmini-backend", {
        "sender": "openclaw",
        "receiver": "macmini-backend",
        "type": "result",
        "correlation_id": corr1,
        "status": "success",
        "result": {
            "disk_usage": "42% (128GB / 300GB)",
            "cpu_load": "55.8% (4 cores)",
            "memory_usage": "67.3% (4.2GB / 6.3GB)",
            "network": "192.168.1.50, latency 2ms to gateway",
            "uptime": "14d 6h 23m",
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(1.5)

    # Conversation 2: Code deployment task
    corr2 = str(uuid.uuid4())
    print("\n--- Conversation 2: Deploy Updated Model ---")

    publish(client, "agents/inbox/openclaw", {
        "sender": "macmini-backend",
        "receiver": "openclaw",
        "type": "command",
        "correlation_id": corr2,
        "command": "deploy_model",
        "payload": {
            "model_name": "llama-3.1-8b-instruct-q4",
            "source": "s3://openclaw-models/llama-3.1-8b-instruct-q4.gguf",
            "target_path": "/opt/models/active/",
            "restart_inference": True,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(1)

    publish(client, "agents/inbox/macmini-backend", {
        "sender": "openclaw",
        "receiver": "macmini-backend",
        "type": "info",
        "correlation_id": corr2,
        "message": "Downloading model llama-3.1-8b-instruct-q4.gguf (4.3GB)... 45% complete",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(2)

    publish(client, "agents/inbox/macmini-backend", {
        "sender": "openclaw",
        "receiver": "macmini-backend",
        "type": "info",
        "correlation_id": corr2,
        "message": "Download complete. Stopping inference server for hot-swap...",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(1.5)

    publish(client, "agents/inbox/macmini-backend", {
        "sender": "openclaw",
        "receiver": "macmini-backend",
        "type": "result",
        "correlation_id": corr2,
        "status": "success",
        "result": {
            "model_deployed": "llama-3.1-8b-instruct-q4.gguf",
            "size": "4.3GB",
            "inference_server": "restarted",
            "warmup_latency_ms": 320,
            "tokens_per_second": 42.7,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(1.5)

    # Conversation 3: Run a local inference task
    corr3 = str(uuid.uuid4())
    print("\n--- Conversation 3: Local Inference Request ---")

    publish(client, "agents/inbox/openclaw", {
        "sender": "macmini-backend",
        "receiver": "openclaw",
        "type": "command",
        "correlation_id": corr3,
        "command": "run_inference",
        "payload": {
            "prompt": "Summarize the system logs from the last hour and flag any anomalies.",
            "max_tokens": 512,
            "temperature": 0.3,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(2)

    publish(client, "agents/inbox/macmini-backend", {
        "sender": "openclaw",
        "receiver": "macmini-backend",
        "type": "result",
        "correlation_id": corr3,
        "status": "success",
        "result": {
            "response": (
                "System log summary (last hour):\n"
                "- 142 INFO entries, 3 WARN, 0 ERROR\n"
                "- WARN: Memory usage peaked at 89% at 14:23 UTC (GC resolved)\n"
                "- WARN: MQTT reconnect at 14:31 UTC (broker restart detected)\n"
                "- WARN: Disk I/O spike at 14:45 UTC during model download\n"
                "No critical anomalies detected. All warnings are transient."
            ),
            "tokens_used": 127,
            "latency_ms": 1840,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    time.sleep(1)

    # Conversation 4: macmini-backend acknowledges
    publish(client, "agents/inbox/openclaw", {
        "sender": "macmini-backend",
        "receiver": "openclaw",
        "type": "command",
        "correlation_id": corr3,
        "command": "ack",
        "payload": {
            "message": "Inference result received. Storing in central log. No action needed.",
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })


def main():
    parser = argparse.ArgumentParser(description="Simulate agent conversation via MQTT")
    parser.add_argument("--host", default=BROKER_HOST)
    parser.add_argument("--port", type=int, default=BROKER_PORT)
    parser.add_argument("--user", default=BROKER_USER)
    parser.add_argument("--password", default=BROKER_PASS)
    parser.add_argument("--loop", action="store_true", help="Keep sending heartbeats after conversation")
    args = parser.parse_args()

    client = mqtt.Client(client_id="conversation-simulator")
    client.username_pw_set(args.user, args.password)
    client.connect(args.host, args.port)
    client.loop_start()

    print("Connected to MQTT broker")

    print("\n=== Registering agents ===")
    register_agents(client)

    time.sleep(1)

    print("\n=== Sending initial heartbeats ===")
    send_heartbeats(client)

    time.sleep(1)

    print("\n=== Starting conversation simulation ===")
    simulate_conversation(client)

    print("\n=== Conversation simulation complete ===")

    if args.loop:
        print("\nSending heartbeats every 30s (Ctrl+C to stop)...")
        try:
            while True:
                send_heartbeats(client)
                time.sleep(30)
        except KeyboardInterrupt:
            print("\nStopped.")

    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
