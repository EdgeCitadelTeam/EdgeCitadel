from datetime import datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import Message


async def get_messages(
    session: AsyncSession,
    agent: str | None = None,
    sender: str | None = None,
    receiver: str | None = None,
    message_type: str | None = None,
    topic: str | None = None,
    correlation_id: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[Message], int]:
    query = select(Message)
    count_query = select(func.count()).select_from(Message)
    conditions = []

    if agent:
        conditions.append(
            or_(Message.sender_id == agent, Message.receiver_id == agent)
        )
    if sender:
        conditions.append(Message.sender_id == sender)
    if receiver:
        conditions.append(Message.receiver_id == receiver)
    if message_type:
        conditions.append(Message.message_type == message_type)
    if topic:
        conditions.append(Message.mqtt_topic.contains(topic))
    if correlation_id:
        conditions.append(Message.correlation_id == correlation_id)
    if since:
        conditions.append(Message.timestamp >= since)
    if until:
        conditions.append(Message.timestamp <= until)
    if search:
        conditions.append(Message.raw_payload.contains(search))

    if conditions:
        query = query.where(and_(*conditions))
        count_query = count_query.where(and_(*conditions))

    total = (await session.execute(count_query)).scalar() or 0
    result = await session.execute(
        query.order_by(Message.timestamp.desc()).offset(offset).limit(limit)
    )
    return list(result.scalars().all()), total


async def get_conversations(session: AsyncSession, limit: int = 50) -> list[dict]:
    result = await session.execute(
        select(
            Message.correlation_id,
            func.count().label("message_count"),
            func.min(Message.timestamp).label("started_at"),
            func.max(Message.timestamp).label("last_message_at"),
        )
        .where(Message.correlation_id.isnot(None))
        .group_by(Message.correlation_id)
        .order_by(func.max(Message.timestamp).desc())
        .limit(limit)
    )
    return [
        {
            "correlation_id": row.correlation_id,
            "message_count": row.message_count,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "last_message_at": row.last_message_at.isoformat() if row.last_message_at else None,
        }
        for row in result.all()
    ]


async def get_communication_flow(
    session: AsyncSession, hours: int = 24
) -> dict:
    from datetime import timedelta
    from database import utcnow

    since = utcnow() - timedelta(hours=hours)

    result = await session.execute(
        select(
            Message.sender_id,
            Message.receiver_id,
            func.count().label("count"),
        )
        .where(
            and_(
                Message.sender_id.isnot(None),
                Message.receiver_id.isnot(None),
                Message.timestamp >= since,
            )
        )
        .group_by(Message.sender_id, Message.receiver_id)
    )

    nodes = set()
    edges = []
    for row in result.all():
        nodes.add(row.sender_id)
        nodes.add(row.receiver_id)
        edges.append({
            "source": row.sender_id,
            "target": row.receiver_id,
            "count": row.count,
        })

    return {
        "nodes": [{"id": n} for n in nodes],
        "edges": edges,
    }
