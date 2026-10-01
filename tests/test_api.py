"""Checks for the authentication boundary and isolated dependency composition."""

from typing import Final

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from payments.api import create_app
from payments.settings import Settings

TEST_KEY: Final = "test-only-primary-api-key"
OTHER_KEY: Final = "test-only-secondary-api-key"


@pytest.mark.parametrize("api_key", [None, "incorrect", ""])
async def test_health_rejects_invalid_credentials(api_key: str | None) -> None:
    """Protect health just like every other application endpoint."""
    app = create_app(Settings(api_key=SecretStr(TEST_KEY), _env_file=None))
    headers = {"X-API-Key": api_key} if api_key is not None else {}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health", headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}


async def test_configuration_is_scoped_to_each_application() -> None:
    """Keep injected credentials independent across application instances."""
    first_app = create_app(Settings(api_key=SecretStr(TEST_KEY), _env_file=None))
    second_app = create_app(Settings(api_key=SecretStr(OTHER_KEY), _env_file=None))

    async with (
        AsyncClient(transport=ASGITransport(app=first_app), base_url="http://test") as first,
        AsyncClient(transport=ASGITransport(app=second_app), base_url="http://test") as second,
    ):
        accepted = await first.get("/health", headers={"X-API-Key": TEST_KEY})
        rejected = await second.get("/health", headers={"X-API-Key": TEST_KEY})
        also_accepted = await second.get("/health", headers={"X-API-Key": OTHER_KEY})

    assert accepted.status_code == also_accepted.status_code == 200
    assert accepted.json() == also_accepted.json() == {"status": "ok"}
    assert rejected.status_code == 401
