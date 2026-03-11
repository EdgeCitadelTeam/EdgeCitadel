from pydantic import BaseModel


class DeploymentConfig(BaseModel):
    name: str
    host: str
    port: int = 1883
    description: str = ""
    network: str = ""
    mqtt_user: str = ""
    mqtt_pass: str = ""


class PublishRequest(BaseModel):
    deployment: str = ""
    topic: str  # NATS subject
    payload: str
