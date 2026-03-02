import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import (
    MessageResponse,
    PaginatedTasks,
    TaskCreate,
    TaskResponse,
    TaskUpdate,
)
from services import task_service

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

# Reference to mqtt_service set by main.py at startup
mqtt_service = None


@router.get("", response_model=PaginatedTasks)
async def list_tasks(
    status: str | None = None,
    agent: str | None = None,
    priority: str | None = None,
    since: datetime | None = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    tasks, total = await task_service.get_tasks(
        session,
        status=status,
        assigned_agent=agent,
        priority=priority,
        since=since,
        limit=limit,
        offset=offset,
    )
    return PaginatedTasks(items=tasks, total=total, limit=limit, offset=offset)


@router.post("", response_model=TaskResponse, status_code=201)
async def create_task(body: TaskCreate, session: AsyncSession = Depends(get_session)):
    task = await task_service.create_task(session, body.model_dump())

    # Publish task assignment to MQTT if agent assigned
    if task.assigned_agent and mqtt_service:
        try:
            await mqtt_service.publish(
                f"agents/task/{task.assigned_agent}/assign",
                {
                    "task_id": task.id,
                    "title": task.title,
                    "description": task.description,
                    "priority": task.priority,
                    "assigned_agent": task.assigned_agent,
                    "sender": "dashboard",
                    "type": "task_assign",
                    "correlation_id": task.correlation_id,
                },
            )
        except Exception:
            pass  # Task created in DB even if MQTT publish fails

    return task


@router.patch("/{task_id}", response_model=TaskResponse)
async def update_task(
    task_id: str,
    body: TaskUpdate,
    session: AsyncSession = Depends(get_session),
):
    task = await task_service.update_task(
        session, task_id, body.model_dump(exclude_none=True)
    )
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@router.get("/{task_id}/trace", response_model=list[MessageResponse])
async def get_task_trace(task_id: str, session: AsyncSession = Depends(get_session)):
    return await task_service.get_task_trace(session, task_id)
