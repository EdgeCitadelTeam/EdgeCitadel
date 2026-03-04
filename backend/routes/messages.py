from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import MessageResponse, PaginatedMessages, TopologyResponse
from services import message_service

router = APIRouter(prefix="/api/messages", tags=["messages"])


@router.get("", response_model=PaginatedMessages)
async def list_messages(
    agent: str | None = None,
    sender: str | None = Query(None, alias="from"),
    receiver: str | None = Query(None, alias="to"),
    message_type: str | None = Query(None, alias="type"),
    topic: str | None = None,
    correlation_id: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    search: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    # Hide heartbeat/register noise unless explicitly requested
    exclude_types = None
    if not message_type:
        exclude_types = ["heartbeat", "register"]

    messages, total = await message_service.get_messages(
        session,
        agent=agent,
        sender=sender,
        receiver=receiver,
        message_type=message_type,
        topic=topic,
        correlation_id=correlation_id,
        since=since,
        until=until,
        search=search,
        exclude_types=exclude_types,
        limit=limit,
        offset=offset,
    )
    return PaginatedMessages(
        items=messages,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/conversations")
async def list_conversations(
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    return await message_service.get_conversations(session, limit=limit)


@router.get("/flow", response_model=TopologyResponse)
async def get_flow(
    hours: int = Query(24, ge=1, le=168),
    session: AsyncSession = Depends(get_session),
):
    return await message_service.get_communication_flow(session, hours=hours)
