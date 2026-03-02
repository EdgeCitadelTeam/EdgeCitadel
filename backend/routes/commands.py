from fastapi import APIRouter, HTTPException

from schemas import BroadcastRequest, CommandRequest

router = APIRouter(prefix="/api", tags=["commands"])

# Reference to mqtt_service set by main.py at startup
mqtt_service = None


@router.post("/command/{agent_name}")
async def send_command(agent_name: str, body: CommandRequest):
    if mqtt_service is None:
        raise HTTPException(503, "MQTT service not available")
    try:
        await mqtt_service.publish(
            f"agents/inbox/{agent_name}",
            {
                "sender": "dashboard",
                "receiver": agent_name,
                "type": body.message_type,
                **body.payload,
            },
        )
    except RuntimeError:
        raise HTTPException(503, "MQTT client not connected")
    return {"status": "sent", "topic": f"agents/inbox/{agent_name}"}


@router.post("/broadcast")
async def broadcast(body: BroadcastRequest):
    if mqtt_service is None:
        raise HTTPException(503, "MQTT service not available")
    try:
        await mqtt_service.publish(
            "agents/broadcast",
            {
                "sender": "dashboard",
                "type": body.message_type,
                **body.payload,
            },
        )
    except RuntimeError:
        raise HTTPException(503, "MQTT client not connected")
    return {"status": "broadcast_sent", "topic": "agents/broadcast"}
