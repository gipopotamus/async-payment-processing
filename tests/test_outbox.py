"""Relay durability and failure windows against real PostgreSQL transactions."""

import asyncio
import socket
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

import pytest
from pydantic import SecretStr
from sqlalchemy import func, update
from sqlalchemy.exc import IntegrityError

from payments.broker import RabbitEventPublisher, create_broker
from payments.database import Database
from payments.domain import NEW_PAYMENTS_QUEUE, Publication, PublicationError
from payments.models import OutboxEvent
from payments.outbox import OutboxRelay, publication_backoff
from payments.settings import BrokerSettings
from payments.worker import run_relay

pytestmark = pytest.mark.integration


async def test_real_adapter_retains_intent_when_broker_is_unavailable(database: Database) -> None:
    """Use the actual AMQP client against an unlistened local port and persist recovery."""
    event_id = await add_event(database)
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        broker = create_broker(
            BrokerSettings(
                broker_host="127.0.0.1",
                broker_port=reserved.getsockname()[1],
                broker_user="test",
                broker_password=SecretStr("test-only-password"),
                _env_file=None,
            )
        )
        try:
            assert await OutboxRelay(database, RabbitEventPublisher(broker)).publish_next()
        finally:
            await broker.stop()
    event = await load_event(database, event_id)
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert event.last_error == "PublicationError"
    assert event.available_at > datetime.now(UTC)


class RecordingPublisher:
    """Record external acceptance separately from database commit for crash checks."""

    def __init__(self, failure: bool = False, block: bool = False) -> None:
        """Control transient failures and stalls without emulating AMQP confirmations."""
        self.failure = failure
        self.block = block
        self.events: list[Publication] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(self, event: Publication) -> None:
        """Signal acceptance, optionally block, or report a sanitized broker failure."""
        self.events.append(event)
        self.started.set()
        if self.block:
            await self.release.wait()
        if self.failure:
            raise PublicationError("Broker unavailable with sensitive credentials")


async def add_event(database: Database, delay: timedelta = timedelta()) -> UUID:
    """Persist a standalone intent in the schema reserved for this integration test."""
    event_id = uuid7()
    async with database.sessions() as session, session.begin():
        session.add(
            OutboxEvent(
                id=event_id,
                event_type="relay.test",
                destination=NEW_PAYMENTS_QUEUE,
                payload={"event_id": str(event_id)},
                available_at=datetime.now(UTC) + delay,
            )
        )
    return event_id


async def load_event(database: Database, event_id: UUID) -> OutboxEvent:
    """Read committed state through a fresh session after a simulated restart."""
    async with database.sessions() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        return event


async def test_due_events_order_and_confirmation(database: Database) -> None:
    """Drain due events in order, leave future work pending, and never republish success."""
    future_id = await add_event(database, timedelta(hours=1))
    first_id = await add_event(database, timedelta(seconds=-2))
    second_id = await add_event(database, timedelta(seconds=-1))
    publisher = RecordingPublisher()
    relay = OutboxRelay(database, publisher)
    assert await relay.publish_next()
    assert await relay.publish_next()
    assert not await relay.publish_next()
    assert [event.event_id for event in publisher.events] == [first_id, second_id]
    assert (await load_event(database, future_id)).published_at is None
    for event_id in [first_id, second_id]:
        event = await load_event(database, event_id)
        assert event.published_at is not None
        assert event.publish_attempts == 1
        assert event.last_error is None


async def test_failure_schedule_survives_restart(database: Database) -> None:
    """Persist unlimited publication retries and recover with the original event ID."""
    event_id = await add_event(database)
    failure = RecordingPublisher(failure=True)
    before = datetime.now(UTC)
    assert await OutboxRelay(database, failure).publish_next()
    event = await load_event(database, event_id)
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert event.last_error == "PublicationError"
    assert event.available_at >= before + timedelta(seconds=2)
    assert event.available_at <= datetime.now(UTC) + timedelta(seconds=2)
    assert not await OutboxRelay(database, failure).publish_next()

    for attempt in range(2, 5):
        async with database.sessions() as session, session.begin():
            await session.execute(update(OutboxEvent).values(available_at=func.now()))
        before = datetime.now(UTC)
        assert await OutboxRelay(database, failure).publish_next()
        event = await load_event(database, event_id)
        assert event.publish_attempts == attempt
        assert event.available_at >= before + publication_backoff(attempt)
    async with database.sessions() as session, session.begin():
        await session.execute(update(OutboxEvent).values(available_at=func.now()))
    success = RecordingPublisher()
    assert await OutboxRelay(database, success).publish_next()
    recovered = await load_event(database, event_id)
    assert recovered.published_at is not None
    assert recovered.publish_attempts == 5
    assert recovered.last_error is None
    assert success.events[0] == failure.events[0]
    assert publication_backoff(1000000) == timedelta(seconds=60)


async def test_competing_relays_skip_locked_event(database: Database) -> None:
    """Prevent concurrent publication while another relay waits for confirmation."""
    event_id = await add_event(database)
    stalled = RecordingPublisher(block=True)
    other = RecordingPublisher()
    task = asyncio.create_task(OutboxRelay(database, stalled).publish_next())
    try:
        await asyncio.wait_for(stalled.started.wait(), timeout=2)
        assert not await asyncio.wait_for(OutboxRelay(database, other).publish_next(), timeout=2)
        assert other.events == []
        assert (await load_event(database, event_id)).published_at is None
        stalled.release.set()
        assert await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert (await load_event(database, event_id)).published_at is not None


async def test_cancelled_relay_rolls_back_and_releases_lock(database: Database) -> None:
    """Keep work recoverable after shutdown with an ambiguous publication outcome."""
    event_id = await add_event(database)
    publisher = RecordingPublisher(block=True)
    task = asyncio.create_task(OutboxRelay(database, publisher).publish_next())
    try:
        await asyncio.wait_for(publisher.started.wait(), timeout=2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    pending = await load_event(database, event_id)
    assert pending.published_at is None
    assert pending.publish_attempts == 0
    resumed = RecordingPublisher()
    assert await OutboxRelay(database, resumed).publish_next()
    assert resumed.events == publisher.events


async def test_publication_timeout_persists_retry(database: Database) -> None:
    """Release a stuck publication's lock and retain its retry without hanging the worker."""
    event_id = await add_event(database)
    stalled = RecordingPublisher(block=True)
    assert await OutboxRelay(database, stalled, publish_timeout=0.05).publish_next()
    event = await load_event(database, event_id)
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert event.last_error == "TimeoutError"
    assert event.available_at > datetime.now(UTC)


async def test_confirmation_before_failed_commit_repeats_identity(database: Database) -> None:
    """Expose the documented duplicate window without losing the publication intent."""
    event_id = await add_event(database)
    publisher = RecordingPublisher()
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "ALTER TABLE outbox ADD CONSTRAINT fail_confirmed_update CHECK (published_at IS NULL)"
        )
    with pytest.raises(IntegrityError):
        await OutboxRelay(database, publisher).publish_next()
    pending = await load_event(database, event_id)
    assert pending.published_at is None
    assert pending.publish_attempts == 0
    assert len(publisher.events) == 1
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql("ALTER TABLE outbox DROP CONSTRAINT fail_confirmed_update")
    assert await OutboxRelay(database, publisher).publish_next()
    assert publisher.events[0] == publisher.events[1]
    assert (await load_event(database, event_id)).published_at is not None


async def test_worker_drains_and_stops(database: Database) -> None:
    """Let the separate process loop drain work and wake promptly when shutdown is requested."""
    event_id = await add_event(database)
    publisher = RecordingPublisher()
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_relay(OutboxRelay(database, publisher), stop, poll_interval=0.01)
    )
    try:
        async with asyncio.timeout(2):
            while (await load_event(database, event_id)).published_at is None:
                await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(publisher.events) == 1
