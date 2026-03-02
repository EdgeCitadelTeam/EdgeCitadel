from datetime import datetime

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import SystemLog, utcnow


async def get_logs(
    session: AsyncSession,
    level: str | None = None,
    agent_id: str | None = None,
    source: str | None = None,
    search: str | None = None,
    since: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[SystemLog], int]:
    query = select(SystemLog)
    count_query = select(func.count()).select_from(SystemLog)
    conditions = []

    if level:
        conditions.append(SystemLog.level == level)
    if agent_id:
        conditions.append(SystemLog.agent_id == agent_id)
    if source:
        conditions.append(SystemLog.source == source)
    if search:
        conditions.append(SystemLog.message.contains(search))
    if since:
        conditions.append(SystemLog.timestamp >= since)

    if conditions:
        query = query.where(and_(*conditions))
        count_query = count_query.where(and_(*conditions))

    total = (await session.execute(count_query)).scalar() or 0
    result = await session.execute(
        query.order_by(SystemLog.timestamp.desc()).offset(offset).limit(limit)
    )
    return list(result.scalars().all()), total


async def add_system_log(
    session: AsyncSession,
    level: str,
    message: str,
    agent_id: str | None = None,
    source: str | None = None,
    metadata: dict | None = None,
) -> SystemLog:
    log = SystemLog(
        level=level,
        agent_id=agent_id,
        source=source,
        message=message,
        metadata_=metadata,
        timestamp=utcnow(),
    )
    session.add(log)
    await session.commit()
    await session.refresh(log)
    return log
