from datetime import datetime

from pydantic import BaseModel, Field


# --- Agent ---

class AgentResponse(BaseModel):
    id: str
    display_name: str | None = None
    role: str | None = None
    device_type: str | None = None
    model: str | None = None
    status: str = "offline"
    capabilities: dict | list | None = None
    ip_address: str | None = None
    cpu_percent: float | None = None
    memory_percent: float | None = None
    last_heartbeat: datetime | None = None
    metadata: dict | None = Field(None, alias="metadata_")
    registered_at: datetime | None = None
    updated_at: datetime | None = None

    class Config:
        from_attributes = True
        populate_by_name = True


class AgentCreate(BaseModel):
    id: str
    display_name: str | None = None
    role: str | None = None
    device_type: str | None = None
    model: str | None = None
    capabilities: dict | list | None = None
    ip_address: str | None = None
    status: str = "offline"


class AgentUpdate(BaseModel):
    display_name: str | None = None
    role: str | None = None
    device_type: str | None = None
    model: str | None = None
    status: str | None = None
    capabilities: dict | list | None = None
    ip_address: str | None = None
    cpu_percent: float | None = None
    memory_percent: float | None = None


# --- Message ---

class MessageResponse(BaseModel):
    id: int
    mqtt_topic: str
    sender_id: str | None = None
    receiver_id: str | None = None
    message_type: str | None = None
    correlation_id: str | None = None
    payload: dict | None = None
    raw_payload: str | None = None
    timestamp: datetime
    received_at: datetime | None = None

    class Config:
        from_attributes = True


class PaginatedMessages(BaseModel):
    items: list[MessageResponse]
    total: int
    limit: int
    offset: int


# --- Task ---

class TaskResponse(BaseModel):
    id: str
    title: str | None = None
    description: str | None = None
    status: str = "pending"
    assigned_agent: str | None = None
    priority: str | None = None
    correlation_id: str | None = None
    result: dict | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    class Config:
        from_attributes = True


class TaskCreate(BaseModel):
    title: str
    description: str | None = None
    assigned_agent: str | None = None
    priority: str = "normal"


class TaskUpdate(BaseModel):
    title: str | None = None
    description: str | None = None
    status: str | None = None
    assigned_agent: str | None = None
    priority: str | None = None
    result: dict | None = None
    error_message: str | None = None


class PaginatedTasks(BaseModel):
    items: list[TaskResponse]
    total: int
    limit: int
    offset: int


# --- Log ---

class LogResponse(BaseModel):
    id: int
    level: str
    agent_id: str | None = None
    source: str | None = None
    message: str
    metadata: dict | None = Field(None, alias="metadata_")
    timestamp: datetime

    class Config:
        from_attributes = True
        populate_by_name = True


class PaginatedLogs(BaseModel):
    items: list[LogResponse]
    total: int
    limit: int
    offset: int


# --- Commands ---

class CommandRequest(BaseModel):
    message_type: str = "command"
    payload: dict = {}


class BroadcastRequest(BaseModel):
    message_type: str = "broadcast"
    payload: dict = {}


# --- System ---

class SystemStatus(BaseModel):
    agents_online: int
    agents_total: int
    total_messages: int
    active_tasks: int
    errors_today: int
    ws_connections: dict
    mqtt_connected: bool


class TopologyResponse(BaseModel):
    nodes: list[dict]
    edges: list[dict]
