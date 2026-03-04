import asyncio
import logging
import socket

import psutil

logger = logging.getLogger(__name__)

AGENT_ID = "orchestrator"
DISPLAY_NAME = "EdgeCitadel Server"
ROLE = "orchestrator"
DEVICE_TYPE = "x86_server"
CAPABILITIES = ["mqtt_broker", "dashboard", "command_dispatch", "health_monitoring"]
HEARTBEAT_INTERVAL = 30


def _get_local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


async def orchestrator_heartbeat_loop(mqtt_svc):
    ip = _get_local_ip()

    # Wait for MQTT connection
    for _ in range(20):
        if mqtt_svc._client is not None:
            break
        await asyncio.sleep(0.5)
    else:
        logger.error("MQTT client not available, orchestrator service not starting")
        return

    # Register
    try:
        await mqtt_svc.publish("agents/register/orchestrator", {
            "agent_id": AGENT_ID,
            "display_name": DISPLAY_NAME,
            "role": ROLE,
            "device_type": DEVICE_TYPE,
            "capabilities": CAPABILITIES,
            "ip_address": ip,
            "status": "online",
        })
        logger.info("Orchestrator registered as '%s'", DISPLAY_NAME)
    except Exception:
        logger.exception("Failed to publish orchestrator registration")

    # Heartbeat loop
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await mqtt_svc.publish("agents/heartbeat/orchestrator", {
                "agent_id": AGENT_ID,
                "display_name": DISPLAY_NAME,
                "status": "online",
                "cpu_percent": psutil.cpu_percent(interval=None),
                "memory_percent": psutil.virtual_memory().percent,
                "ip_address": ip,
            })
        except asyncio.CancelledError:
            logger.info("Orchestrator heartbeat cancelled")
            return
        except Exception:
            logger.exception("Error in orchestrator heartbeat loop")
