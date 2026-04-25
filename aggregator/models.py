from typing import Literal, Optional
from pydantic import BaseModel, Field


class Envelope(BaseModel):
    v: Literal[1] = 1
    id: str
    type: Literal["register", "heartbeat", "status", "command", "result",
                  "delegation", "cancel", "log", "broadcast", "task.progress"]
    sender_id: str
    recipient_id: Optional[str] = None
    task_id: Optional[str] = None
    context_id: Optional[str] = None
    task_state: Optional[Literal["submitted", "working", "input-required",
                                 "completed", "failed", "canceled", "rejected",
                                 "auth-required"]] = None
    agent_state: Optional[Literal["online", "offline", "busy", "error"]] = None
    hop_count: Optional[int] = Field(default=None, ge=0)
    timestamp: str
    payload: dict


class CommandRequest(BaseModel):
    body: str
    args: Optional[dict] = None


class CommandResponse(BaseModel):
    task_id: str
    recipient_id: str
    accepted_at: str
