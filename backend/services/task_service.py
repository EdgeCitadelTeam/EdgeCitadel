import uuid
from datetime import datetime

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Message, Task, utcnow


async def get_tasks(
    session: AsyncSession,
    status: str | None = None,
    assigned_agent: str | None = None,
    priority: str | None = None,
    since: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[Task], int]:
    query = select(Task)
    count_query = select(func.count()).select_from(Task)
    conditions = []

    if status:
        conditions.append(Task.status == status)
    if assigned_agent:
        conditions.append(Task.assigned_agent == assigned_agent)
    if priority:
        conditions.append(Task.priority == priority)
    if since:
        conditions.append(Task.created_at >= since)

    if conditions:
        query = query.where(and_(*conditions))
        count_query = count_query.where(and_(*conditions))

    total = (await session.execute(count_query)).scalar() or 0
    result = await session.execute(
        query.order_by(Task.created_at.desc()).offset(offset).limit(limit)
    )
    return list(result.scalars().all()), total


async def create_task(session: AsyncSession, data: dict) -> Task:
    task = Task(
        id=data.get("id") or str(uuid.uuid4()),
        title=data.get("title", "Untitled Task"),
        description=data.get("description"),
        status="pending",
        assigned_agent=data.get("assigned_agent"),
        priority=data.get("priority", "normal"),
        correlation_id=data.get("correlation_id") or str(uuid.uuid4()),
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def update_task(session: AsyncSession, task_id: str, data: dict) -> Task | None:
    result = await session.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if not task:
        return None

    for key, value in data.items():
        if hasattr(task, key) and value is not None:
            setattr(task, key, value)

    now = utcnow()
    if data.get("status") == "running" and not task.started_at:
        task.started_at = now
    if data.get("status") in ("completed", "failed") and not task.completed_at:
        task.completed_at = now

    await session.commit()
    await session.refresh(task)
    return task


async def get_task_trace(session: AsyncSession, task_id: str) -> list[Message]:
    task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    if not task or not task.correlation_id:
        return []
    result = await session.execute(
        select(Message)
        .where(Message.correlation_id == task.correlation_id)
        .order_by(Message.timestamp)
    )
    return list(result.scalars().all())


async def get_task_counts_by_status(session: AsyncSession) -> dict:
    result = await session.execute(
        select(Task.status, func.count()).group_by(Task.status)
    )
    return dict(result.all())
