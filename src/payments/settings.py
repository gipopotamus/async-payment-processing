"""Validated configuration supplied by each process's composition root."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class EnvironmentSettings(BaseSettings):
    """Read one process's settings from a shared environment and dotenv file."""

    model_config = SettingsConfigDict(
        env_prefix="PAYMENTS_",
        env_file=".env",
        env_file_encoding="utf-8",
        # Each settings group reads its own fields from the shared Compose dotenv.
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
    )


class Settings(EnvironmentSettings):
    """Configure the API without requiring credentials for other processes.

    Attributes:
        api_key: Shared credential required by every application endpoint.
        webhook_allowed_origins: Callback scheme/host/port allowlist; empty denies all.
    """

    api_key: SecretStr = Field(min_length=16)
    webhook_allowed_origins: frozenset[str] = frozenset()


class DatabaseSettings(EnvironmentSettings):
    """Configure PostgreSQL independently of API authentication.

    Attributes:
        database_host: Hostname reachable from the current process.
        database_port: PostgreSQL port, defaulting to the local Compose mapping.
        database_user: Database login role.
        database_password: Required password, concealed in representations.
        database_name: Application database name.
    """

    database_host: str = Field(default="127.0.0.1", min_length=1)
    database_port: int = Field(default=55432, ge=1, le=65535)
    database_user: str = Field(default="payments", min_length=1)
    database_password: SecretStr = Field(min_length=1)
    database_name: str = Field(default="payments", min_length=1)

    @property
    def url(self) -> URL:
        """Build an async URL without interpolating or misparsing credentials."""
        return URL.create(
            "postgresql+asyncpg",
            username=self.database_user,
            password=self.database_password.get_secret_value(),
            host=self.database_host,
            port=self.database_port,
            database=self.database_name,
        )
