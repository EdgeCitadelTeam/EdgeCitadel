from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_user: str = "iot_agent"
    mqtt_pass: str = "openclaw_secret"
    database_url: str = "sqlite+aiosqlite:///./data/openclaw.db"
    log_level: str = "INFO"
    heartbeat_timeout: int = 60
    health_check_interval: int = 15
    cors_origins: str = "http://localhost:3000"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
