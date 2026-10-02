"""Real PostgreSQL checks for migration fidelity and transactional integrity."""

from contextlib import aclosing
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid7

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, inspect, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from payments.api.app import get_session
from payments.core.domain import Currency, PaymentStatus, WebhookStatus
from payments.infrastructure.database import Database
from payments.infrastructure.models import OutboxEvent, Payment

pytestmark = pytest.mark.integration


def make_payment(amount: Decimal = Decimal("125.50")) -> Payment:
    """Build a valid payment with an explicit ID for transactional event creation."""
    return Payment(
        id=uuid7(),
        amount=amount,
        currency=Currency.RUB,
        idempotency_key="test-payment-key",
        request_hash="0" * 64,
        webhook_url="http://receiver.test/payment-result",
    )


async def test_money_and_creation_defaults_roundtrip(database: Database) -> None:
    """Preserve decimal money and load a pending payment from a fresh session."""
    payment = make_payment()
    async with database.sessions.begin() as session:
        session.add(payment)

    async with database.sessions() as session:
        stored = await session.get(Payment, payment.id)

    assert stored is not None
    assert stored.amount == Decimal("125.50")
    assert stored.currency is Currency.RUB
    assert stored.status is PaymentStatus.PENDING
    assert stored.processed_at is None
    assert stored.created_at.tzinfo is not None
    assert stored.payment_metadata == {}
    assert stored.processing_attempts == stored.webhook_attempts == 0


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-0.01"), Decimal("NaN")])
async def test_database_rejects_invalid_money(database: Database, amount: Decimal) -> None:
    """Reject non-positive and NaN amounts even when application validation is bypassed."""
    with pytest.raises(IntegrityError):
        async with database.sessions.begin() as session:
            session.add(make_payment(amount))


async def test_database_enforces_unique_idempotency_key(database: Database) -> None:
    """Prevent a second payment with an existing request key."""
    async with database.sessions.begin() as session:
        session.add(make_payment())

    with pytest.raises(IntegrityError):
        async with database.sessions.begin() as session:
            session.add(make_payment())


async def test_invalid_outbox_rolls_back_payment_creation(database: Database) -> None:
    """Keep both tables empty when the event part of a creation transaction fails."""
    with pytest.raises(IntegrityError):
        async with database.sessions.begin() as session:
            session.add(make_payment())
            await session.flush()
            session.add(
                OutboxEvent(
                    payment_id=uuid7(),
                    event_type="payment.created",
                    destination="payments.new",
                    payload={"payment_id": "missing-payment"},
                )
            )

    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_webhook_delivery_requires_a_payment_result(database: Database) -> None:
    """Reject a delivered notification for a payment with no terminal result."""
    payment = make_payment()
    payment.webhook_status = WebhookStatus.DELIVERED
    payment.webhook_delivered_at = datetime.now(UTC)

    with pytest.raises(IntegrityError):
        async with database.sessions.begin() as session:
            session.add(payment)


async def test_session_dependency_rolls_back_uncommitted_work(database: Database) -> None:
    """Close request-scoped sessions without accidentally committing pending writes."""
    async with aclosing(get_session(database)) as sessions:
        session = await anext(sessions)
        session.add(make_payment())
        await session.flush()

    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


def check_migration_roundtrip(connection: Connection) -> None:
    """Check model parity before and after a downgrade/upgrade of the isolated schema."""
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    command.check(config)
    command.downgrade(config, "base")
    assert inspect(connection).get_table_names() == ["alembic_version"]
    command.upgrade(config, "head")
    command.check(config)


async def test_migrations_roundtrip_without_schema_drift(database: Database) -> None:
    """Apply frozen migrations rather than creating tables directly from ORM models."""
    async with database.engine.begin() as connection:
        await connection.run_sync(check_migration_roundtrip)
