from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Agent, utcnow


async def get_all_agents(session: AsyncSession) -> list[Agent]:
    result = await session.execute(
        select(Agent).order_by(
            # Online agents first
            Agent.status.desc(),
            Agent.display_name,
        )
    )
    return list(result.scalars().all())


async def get_agent_by_name(session: AsyncSession, agent_id: str) -> Agent | None:
    result = await session.execute(select(Agent).where(Agent.id == agent_id))
    return result.scalar_one_or_none()


async def create_agent(session: AsyncSession, agent_id: str, data: dict) -> Agent:
    agent = Agent(
        id=agent_id,
        display_name=data.get("display_name", agent_id),
        role=data.get("role"),
        device_type=data.get("device_type"),
        model=data.get("model"),
        capabilities=data.get("capabilities"),
        ip_address=data.get("ip_address"),
        status=data.get("status", "offline"),
    )
    session.add(agent)
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent(session: AsyncSession, agent_id: str, data: dict) -> Agent | None:
    result = await session.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        return None
    for key, value in data.items():
        if hasattr(agent, key) and value is not None:
            setattr(agent, key, value)
    agent.updated_at = utcnow()
    await session.commit()
    await session.refresh(agent)
    return agent


async def delete_agent(session: AsyncSession, agent_id: str) -> bool:
    result = await session.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        return False
    await session.delete(agent)
    await session.commit()
    return True


async def get_agent_stats(session: AsyncSession, agent_id: str) -> dict:
    from database import Message, Task

    msg_count = await session.execute(
        select(func.count()).where(
            (Message.sender_id == agent_id) | (Message.receiver_id == agent_id)
        )
    )
    task_count = await session.execute(
        select(func.count()).where(Task.assigned_agent == agent_id)
    )
    return {
        "message_count": msg_count.scalar() or 0,
        "task_count": task_count.scalar() or 0,
    }


async def get_agent_count_by_status(session: AsyncSession) -> dict:
    result = await session.execute(
        select(Agent.status, func.count()).group_by(Agent.status)
    )
    return dict(result.all())
