"""Checks for the authentication boundary and isolated dependency composition."""

import socket
from typing import Final

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from payments.api.app import create_app
from payments.core.settings import DatabaseSettings, Settings
from payments.infrastructure.database import create_database

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


@pytest.mark.parametrize("method", ["GET", "POST"])
async def test_real_database_connection_failure_returns_retryable_response(method: str) -> None:
    """Handle an actual refused driver connection on both lookup and creation routes."""
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        database = create_database(
            DatabaseSettings(
                database_host="127.0.0.1",
                database_port=reserved.getsockname()[1],
                database_password=SecretStr("unused-test-password"),
                _env_file=None,
            )
        )
        app = create_app(
            Settings(
                api_key=SecretStr(TEST_KEY),
                webhook_allowed_origins=frozenset({"http://receiver.test"}),
                _env_file=None,
            ),
            database,
        )
        try:
            async with (
                app.router.lifespan_context(app),
                AsyncClient(
                    transport=ASGITransport(app=app),
                    base_url="http://test",
                    headers={"X-API-Key": TEST_KEY, "Idempotency-Key": "db-outage"},
                ) as client,
            ):
                path = "/api/v1/payments"
                if method == "GET":
                    response = await client.get(f"{path}/00000000-0000-0000-0000-000000000001")
                else:
                    response = await client.post(
                        path,
                        json={
                            "amount": "125.50",
                            "currency": "RUB",
                            "webhook_url": "http://receiver.test/callback",
                        },
                    )
            assert response.status_code == 503
            assert response.headers["Retry-After"] == "1"
            assert response.json() == {"detail": "Payment storage is temporarily unavailable"}
        finally:
            await database.close()
