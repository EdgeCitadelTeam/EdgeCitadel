from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class Envelope(BaseModel):
    v: Literal[1] = 1
    id: str
    type: Literal[
        "register",
        "heartbeat",
        "status",
        "command",
        "result",
        "delegation",
        "cancel",
        "log",
        "broadcast",
        "task.progress",
    ]
    sender_id: str
    recipient_id: Optional[str] = None
    task_id: Optional[str] = None
    context_id: Optional[str] = None
    task_state: Optional[
        Literal[
            "submitted",
            "working",
            "input-required",
            "completed",
            "failed",
            "canceled",
            "rejected",
            "auth-required",
        ]
    ] = None
    agent_state: Optional[Literal["online", "offline", "busy", "error"]] = None
    hop_count: Optional[int] = Field(default=None, ge=0)
    timestamp: str
    payload: dict


class CommandRequest(BaseModel):
    body: str
    args: Optional[dict] = None
    skill_id: Optional[str] = None
    context_id: Optional[str] = None

    @field_validator("context_id")
    @classmethod
    def context_id_must_be_uuid4(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = UUID(value)
        if parsed.version != 4 or str(parsed) != value:
            raise ValueError("context_id must be canonical UUIDv4")
        return str(parsed)


class CommandResponse(BaseModel):
    task_id: str
    recipient_id: str
    accepted_at: str


class RegistryQueue(BaseModel):
    pending: int = 0
    ack_pending: int = 0


class RegistryEntry(BaseModel):
    agent_id: str
    card: dict
    agent_state: str
    last_heartbeat: Optional[str] = None
    last_register: str
    deployment: Optional[str] = None
    heartbeat_interval_sec: int
    queue: RegistryQueue
    poison_count: int


class EnrollmentInvitationRequest(BaseModel):
    agent_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    expires_in_seconds: int = Field(default=900, ge=60, le=86400)


class EnrollmentRedeemRequest(BaseModel):
    token: str = Field(min_length=32, max_length=256)
    messaging_mode: Literal["single-client", "nats_leaf"] = "single-client"


class EnrollmentRedeemResponse(BaseModel):
    agent_id: str
    nats_token: str | None = None
    leaf_username: str | None = None
    leaf_password: str | None = None
