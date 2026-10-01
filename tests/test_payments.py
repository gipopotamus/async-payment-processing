"""Payment HTTP contracts and concurrency against real migrated PostgreSQL schemas."""

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Final
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import func, select, update

from payments.api import create_app
from payments.database import Database
from payments.domain import JsonValue, PaymentStatus
from payments.models import OutboxEvent, Payment
from payments.settings import Settings

pytestmark = pytest.mark.integration
TEST_KEY: Final = "test-only-payment-api-key"


def payment_body(**changes: JsonValue) -> dict[str, JsonValue]:
    """Build a request whose callback points to an explicitly allowed test origin."""
    return {
        "amount": "125.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://receiver.test/callback",
        **changes,
    }


@pytest.fixture
async def client(database: Database) -> AsyncIterator[AsyncClient]:
    """Exercise the real lifespan with an injected isolated database and policy."""
    settings = Settings(
        api_key=SecretStr(TEST_KEY),
        webhook_allowed_origins=frozenset({"http://receiver.test"}),
        _env_file=None,
    )
    app = create_app(settings, database)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": TEST_KEY, "Idempotency-Key": "order-42"},
        ) as resource,
    ):
        yield resource


async def assert_counts(database: Database, expected: int) -> None:
    """Count both sides of the atomic payment/event pair."""
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == expected
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == expected


async def test_creation_details_and_current_status_replay(
    client: AsyncClient, database: Database
) -> None:
    """Commit one event, preserve decimal money, and replay the current stored status."""
    accepted = await client.post("/api/v1/payments", json=payment_body())
    assert accepted.status_code == 202
    result = accepted.json()
    assert set(result) == {"payment_id", "status", "created_at"}
    assert result["status"] == "pending"
    details = await client.get(f"/api/v1/payments/{result['payment_id']}")
    assert details.status_code == 200
    assert details.json() == {
        **result,
        **payment_body(),
        "idempotency_key": "order-42",
        "processed_at": None,
        "webhook_status": "pending",
        "webhook_attempts": 0,
        "webhook_delivered_at": None,
    }
    async with database.sessions() as session, session.begin():
        event = await session.scalar(select(OutboxEvent))
        assert event is not None
        assert event.destination == "payments.new"
        assert event.event_type == "payment.created"
        assert event.payload == {
            "event_id": str(event.id),
            "payment_id": result["payment_id"],
            "stage": "processing",
            "attempt": 1,
        }
        assert event.published_at is None
        await session.execute(
            update(Payment).values(status=PaymentStatus.SUCCEEDED, processed_at=func.now())
        )
    replay = await client.post("/api/v1/payments", json=payment_body())
    assert replay.status_code == 202
    assert replay.json() == {**result, "status": "succeeded"}
    await assert_counts(database, 1)


async def test_concurrent_equivalent_replays(client: AsyncClient, database: Database) -> None:
    """Make concurrent inserts contend on the real unique constraint with one winner."""
    responses = await asyncio.gather(
        *[client.post("/api/v1/payments", json=payment_body()) for _ in range(10)]
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.text for response in responses}) == 1
    await assert_counts(database, 1)


async def test_concurrent_conflicting_requests(client: AsyncClient, database: Database) -> None:
    """Return 409 for the losing payload without creating a second outbox record."""
    responses = await asyncio.gather(
        client.post("/api/v1/payments", json=payment_body(amount="10.00")),
        client.post("/api/v1/payments", json=payment_body(amount="20.00")),
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    await assert_counts(database, 1)


async def test_normalized_payload_replay(client: AsyncClient, database: Database) -> None:
    """Normalize decimal representation, defaults, URL, and nested object key order."""
    original = {
        "amount": "125.5",
        "currency": "EUR",
        "webhook_url": "http://RECEIVER.test:80",
        "metadata": {"b": {"second": 2, "first": 1}, "a": [True, None]},
    }
    normalized = {
        **original,
        "amount": "125.50",
        "description": "",
        "webhook_url": "http://receiver.test/",
        "metadata": {"a": [True, None], "b": {"first": 1, "second": 2}},
    }
    first = await client.post("/api/v1/payments", json=original)
    second = await client.post("/api/v1/payments", json=normalized)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    await assert_counts(database, 1)


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "125.51"},
        {"currency": "USD"},
        {"description": "Another order"},
        {"metadata": {"order_id": 43}},
        {"webhook_url": "http://receiver.test/another"},
    ],
)
async def test_changed_payload_conflicts(
    client: AsyncClient, database: Database, changes: dict[str, JsonValue]
) -> None:
    """Include every business input in the fingerprint, including callback URLs."""
    assert (await client.post("/api/v1/payments", json=payment_body())).status_code == 202
    conflict = await client.post("/api/v1/payments", json=payment_body(**changes))
    assert conflict.status_code == 409
    await assert_counts(database, 1)


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": 125.5},
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "NaN"},
        {"amount": "Infinity"},
        {"amount": "1.001"},
        {"amount": "10000000000000000.00"},
        {"currency": "GBP"},
        {"metadata": []},
        {"unexpected": "value"},
        {"description": "private\x00text"},
        {"metadata": {"nested": ["private\ud800text"]}},
        {"metadata": {"bad\x00key": 1}},
    ],
)
async def test_invalid_body_is_rejected_without_writes(
    client: AsyncClient, database: Database, changes: dict[str, JsonValue]
) -> None:
    """Reject unsupported money and JSON values without leaking the submitted body."""
    response = await client.post(
        "/api/v1/payments",
        content=json.dumps(payment_body(**changes)),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert all("input" not in error and "ctx" not in error for error in response.json()["detail"])
    assert "private" not in response.text
    await assert_counts(database, 0)


async def test_nonfinite_metadata(client: AsyncClient, database: Database) -> None:
    """Reject nonstandard JSON NaN before fingerprinting or JSONB persistence."""
    response = await client.post(
        "/api/v1/payments",
        content=json.dumps(payment_body(metadata={"nested": [float("nan")]})),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    await assert_counts(database, 0)


@pytest.mark.parametrize(
    "url",
    [
        "http://other.test/callback",
        "https://receiver.test/callback",
        "http://receiver.test:8080/callback",
        "http://user:password@receiver.test/callback",
        "http://receiver.test/callback#fragment",
        "http://receiver.test:0/callback",
    ],
)
async def test_webhook_policy(client: AsyncClient, database: Database, url: str) -> None:
    """Enforce exact configured origins and reject credentials and fragments."""
    response = await client.post("/api/v1/payments", json=payment_body(webhook_url=url))
    assert response.status_code == 422
    await assert_counts(database, 0)


@pytest.mark.parametrize("key", ["", "has space", "x" * 256])
async def test_invalid_idempotency_key(client: AsyncClient, database: Database, key: str) -> None:
    """Require a nonempty printable ASCII token with the database's maximum length."""
    response = await client.post(
        "/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": key}
    )
    assert response.status_code == 422
    await assert_counts(database, 0)


async def test_missing_key_authentication_and_lookup(
    client: AsyncClient, database: Database
) -> None:
    """Distinguish missing headers, credentials, invalid IDs, and absent payments."""
    client.headers.pop("Idempotency-Key")
    assert (await client.post("/api/v1/payments", json=payment_body())).status_code == 422
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    client.headers.pop("X-API-Key")
    assert (await client.post("/api/v1/payments", json=payment_body())).status_code == 401
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 401
    await assert_counts(database, 0)


async def test_outbox_failure_rolls_back_payment(client: AsyncClient, database: Database) -> None:
    """Return a sanitized retryable failure and roll back the inserted payment."""
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "ALTER TABLE outbox ADD CONSTRAINT reject_outbox_for_test CHECK (false)"
        )
    response = await client.post("/api/v1/payments", json=payment_body())
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    assert response.json() == {"detail": "Payment storage is temporarily unavailable"}
    await assert_counts(database, 0)
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "ALTER TABLE outbox DROP CONSTRAINT reject_outbox_for_test"
        )
    retry = await client.post("/api/v1/payments", json=payment_body())
    assert retry.status_code == 202
    await assert_counts(database, 1)
