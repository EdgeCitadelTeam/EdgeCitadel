from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import PaginatedLogs
from services import log_service

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("", response_model=PaginatedLogs)
async def list_logs(
    level: str | None = None,
    agent: str | None = None,
    source: str | None = None,
    search: str | None = None,
    since: datetime | None = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    logs, total = await log_service.get_logs(
        session,
        level=level,
        agent_id=agent,
        source=source,
        search=search,
        since=since,
        limit=limit,
        offset=offset,
    )
    return PaginatedLogs(items=logs, total=total, limit=limit, offset=offset)
