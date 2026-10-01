"""Validated configuration supplied by each process's composition root."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Load configuration without exposing secret values in representations.

    Attributes:
        api_key: Shared credential required by every application endpoint.
    """

    model_config = SettingsConfigDict(
        env_prefix="PAYMENTS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        frozen=True,
    )

    api_key: SecretStr = Field(min_length=16)
