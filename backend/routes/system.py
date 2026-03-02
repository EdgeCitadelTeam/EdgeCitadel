from datetime import timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Agent, Message, SystemLog, Task, get_session, utcnow
from schemas import SystemStatus, TopologyResponse
from services import message_service

router = APIRouter(prefix="/api/system", tags=["system"])

# References set by main.py at startup
ws_manager = None
mqtt_service = None


@router.get("/status", response_model=SystemStatus)
async def get_status(session: AsyncSession = Depends(get_session)):
    agents_online = (
        await session.execute(
            select(func.count()).select_from(Agent).where(Agent.status == "online")
        )
    ).scalar() or 0

    agents_total = (
        await session.execute(select(func.count()).select_from(Agent))
    ).scalar() or 0

    total_messages = (
        await session.execute(select(func.count()).select_from(Message))
    ).scalar() or 0

    active_tasks = (
        await session.execute(
            select(func.count())
            .select_from(Task)
            .where(Task.status.in_(["pending", "assigned", "running"]))
        )
    ).scalar() or 0

    today_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    errors_today = (
        await session.execute(
            select(func.count())
            .select_from(SystemLog)
            .where(SystemLog.level == "ERROR", SystemLog.timestamp >= today_start)
        )
    ).scalar() or 0

    return SystemStatus(
        agents_online=agents_online,
        agents_total=agents_total,
        total_messages=total_messages,
        active_tasks=active_tasks,
        errors_today=errors_today,
        ws_connections=ws_manager.connection_count if ws_manager else {},
        mqtt_connected=mqtt_service._client is not None if mqtt_service else False,
    )


@router.get("/topology", response_model=TopologyResponse)
async def get_topology(
    hours: int = 24,
    session: AsyncSession = Depends(get_session),
):
    return await message_service.get_communication_flow(session, hours=hours)
