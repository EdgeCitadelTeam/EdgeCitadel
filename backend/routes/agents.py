from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import AgentCreate, AgentResponse, AgentUpdate
from services import agent_service

router = APIRouter(prefix="/api/agents", tags=["agents"])


@router.get("", response_model=list[AgentResponse])
async def list_agents(session: AsyncSession = Depends(get_session)):
    agents = await agent_service.get_all_agents(session)
    return agents


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(agent_id: str, session: AsyncSession = Depends(get_session)):
    agent = await agent_service.get_agent_by_name(session, agent_id)
    if not agent:
        raise HTTPException(404, "Agent not found")
    return agent


@router.post("", response_model=AgentResponse, status_code=201)
async def create_agent(body: AgentCreate, session: AsyncSession = Depends(get_session)):
    existing = await agent_service.get_agent_by_name(session, body.id)
    if existing:
        raise HTTPException(409, "Agent already exists")
    return await agent_service.create_agent(session, body.id, body.model_dump(exclude={"id"}))


@router.patch("/{agent_id}", response_model=AgentResponse)
async def update_agent(
    agent_id: str,
    body: AgentUpdate,
    session: AsyncSession = Depends(get_session),
):
    agent = await agent_service.update_agent(
        session, agent_id, body.model_dump(exclude_none=True)
    )
    if not agent:
        raise HTTPException(404, "Agent not found")
    return agent


@router.delete("/{agent_id}", status_code=204)
async def delete_agent(agent_id: str, session: AsyncSession = Depends(get_session)):
    deleted = await agent_service.delete_agent(session, agent_id)
    if not deleted:
        raise HTTPException(404, "Agent not found")


@router.get("/{agent_id}/stats")
async def get_agent_stats(agent_id: str, session: AsyncSession = Depends(get_session)):
    agent = await agent_service.get_agent_by_name(session, agent_id)
    if not agent:
        raise HTTPException(404, "Agent not found")
    return await agent_service.get_agent_stats(session, agent_id)
