from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file."""

    APP_NAME: str = "DataBridge"
    APP_VERSION: str = "1.0.0"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    LOG_LEVEL: str = "info"
    # Comma-separated in env (e.g. Railway) or default list for local dev
    CORS_ORIGINS: list[str] = [
        "http://localhost:8080",
        "http://localhost:5173",
        "http://127.0.0.1:8080",
    ]
    # If set and the directory exists, serve the Vite build (production / Docker)
    STATIC_DIR: str | None = None
    # Batch size for data migration (rows per INSERT batch)
    MIGRATION_BATCH_SIZE: int = 10_000
    # Number of tables to migrate in parallel
    MIGRATION_PARALLELISM: int = 4

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: Any) -> list[str]:
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return [
            "http://localhost:8080",
            "http://localhost:5173",
            "http://127.0.0.1:8080",
        ]


settings = Settings()
