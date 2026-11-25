from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


def normalize_async_database_url(url: Optional[str]) -> Optional[str]:
    """Normalizes Postgres connection URLs to asyncpg dialect (postgresql+asyncpg://).
    
    Handles standard Render database URLs (which begin with postgresql:// or postgres://)
    as well as existing asyncpg schemes.
    """
    if not url:
        return url
    if url.startswith("postgres://"):
        return "postgresql+asyncpg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + url[len("postgresql://"):]
    elif url.startswith("postgresql+asyncpg://"):
        return url
    return url


def normalize_sync_database_url(url: Optional[str]) -> Optional[str]:
    """Normalizes Postgres connection URLs to synchronous driver dialect (postgresql://)."""
    if not url:
        return url
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url[len("postgresql+asyncpg://"):]
    elif url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        return url
    return url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    PROJECT_NAME: str = "Distributed Task Execution Engine"
    VERSION: str = "0.1.0"
    ENV: str = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 10000
    PORT: Optional[int] = None
    API_KEY: Optional[str] = None

    # PostgreSQL Persistence
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "task_engine"
    DATABASE_URL: Optional[str] = None

    # Redis Queue & Coordination
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_URL: Optional[str] = None

    # Worker Defaults
    WORKER_CONCURRENCY: int = 4
    WORKER_HEARTBEAT_INTERVAL: int = 5
    WORKER_DEAD_THRESHOLD: int = 15
    DEFAULT_JOB_TIMEOUT: int = 300

    @property
    def server_port(self) -> int:
        """Effective API port binding, preferring $PORT (Render) over API_PORT."""
        return self.PORT or self.API_PORT

    @property
    def async_database_url(self) -> str:
        """Returns the async SQLAlchemy database URL (using asyncpg)."""
        if self.DATABASE_URL:
            return normalize_async_database_url(self.DATABASE_URL)
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def sync_database_url(self) -> str:
        """Returns the sync SQLAlchemy database URL (used by Alembic migrations)."""
        if self.DATABASE_URL:
            return normalize_sync_database_url(self.DATABASE_URL)
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def redis_connection_url(self) -> str:
        """Returns standard Redis connection string."""
        if self.REDIS_URL:
            return self.REDIS_URL
        auth_part = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        return f"redis://{auth_part}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
