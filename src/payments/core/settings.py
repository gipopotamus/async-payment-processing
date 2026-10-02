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


class WebhookSettings(EnvironmentSettings):
    """Configure callback admission independently of the API credential."""

    webhook_allowed_origins: frozenset[str] = frozenset()


class Settings(WebhookSettings):
    """Configure the API without requiring credentials for other processes.

    Attributes:
        api_key: Shared credential required by every application endpoint.
        webhook_allowed_origins: Callback scheme/host/port allowlist; empty denies all.
    """

    api_key: SecretStr = Field(min_length=16)


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


class BrokerSettings(EnvironmentSettings):
    """Configure AMQP independently of API authentication.

    Attributes:
        broker_host: Hostname reachable from this worker.
        broker_port: AMQP port, matching the development Compose mapping.
        broker_user: Required broker login.
        broker_password: Required password, hidden from representations.
        broker_vhost: RabbitMQ virtual host.
    """

    broker_host: str = Field(default="127.0.0.1", min_length=1)
    broker_port: int = Field(default=5673, ge=1, le=65535)
    broker_user: str = Field(min_length=1)
    broker_password: SecretStr = Field(min_length=1)
    broker_vhost: str = "/"


class RelaySettings(EnvironmentSettings):
    """Bound publication locks and polling without limiting eventual recovery."""

    relay_publish_timeout: float = Field(default=5, gt=0, le=60)
    relay_poll_interval: float = Field(default=1, gt=0, le=60)


class ConsumerSettings(EnvironmentSettings):
    """Bound external operations and back off redelivery during storage outages."""

    processing_timeout: float = Field(default=10, gt=0, le=60)
    webhook_timeout: float = Field(default=5, gt=0, le=60)
    consumer_requeue_delay: float = Field(default=1, gt=0, le=60)
