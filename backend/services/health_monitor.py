import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select

from config import settings
from database import Agent, async_session, utcnow

logger = logging.getLogger(__name__)


async def health_monitor_loop(ws_manager):
    while True:
        try:
            await asyncio.sleep(settings.health_check_interval)
            await _check_agent_health(ws_manager)
        except asyncio.CancelledError:
            logger.info("Health monitor cancelled")
            return
        except Exception:
            logger.exception("Error in health monitor loop")


async def _check_agent_health(ws_manager):
    now = utcnow()
    timeout = timedelta(seconds=settings.heartbeat_timeout)
    warning_threshold = timedelta(seconds=30)

    async with async_session() as session:
        result = await session.execute(
            select(Agent).where(Agent.status == "online")
        )
        agents = result.scalars().all()

        for agent in agents:
            if agent.last_heartbeat is None:
                continue

            elapsed = now - agent.last_heartbeat
            if elapsed > timeout:
                agent.status = "offline"
                agent.updated_at = now
                await session.commit()

                logger.warning("Agent %s marked offline (no heartbeat for %ss)", agent.id, elapsed.total_seconds())
                await ws_manager.broadcast({
                    "event": "agent_status_change",
                    "data": {"agent_id": agent.id, "status": "offline"},
                })
            elif elapsed > warning_threshold:
                logger.info("Agent %s heartbeat delayed (%ss)", agent.id, elapsed.total_seconds())
