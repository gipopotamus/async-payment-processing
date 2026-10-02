"""Workflow state, retry persistence, and duplicate/failure windows on real PostgreSQL."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch
from uuid import UUID, uuid7

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from payments.application.processing import PaymentProcessor
from payments.application.services import WebhookPolicy, request_fingerprint
from payments.core.domain import (
    DEAD_LETTER_QUEUE,
    Currency,
    GatewayError,
    InvalidWorkflow,
    NewPayment,
    PaymentSnapshot,
    PaymentStatus,
    WebhookError,
    WebhookStatus,
    WorkflowEvent,
    WorkflowNotReady,
    WorkflowStage,
)
from payments.infrastructure.adapters import EmulatedGateway, HttpWebhookSender, gateway_outcome
from payments.infrastructure.database import Database
from payments.infrastructure.models import OutboxEvent, Payment
from payments.infrastructure.repository import PaymentRepository
from payments.infrastructure.workflow_repository import WorkflowRepository
from payments.workers.schemas import WorkflowEnvelope

pytestmark = pytest.mark.integration


class FakeGateway:
    """Return controlled terminal/technical outcomes while retaining call evidence."""

    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED, failures: int = 0) -> None:
        """Configure a business result and the number of preceding technical failures."""
        self.result = result
        self.failures = failures
        self.calls = 0

    async def process(self, payment: PaymentSnapshot) -> PaymentStatus:
        """Emulate an operation without sleeping for the production gateway delay."""
        self.calls += 1
        if self.calls <= self.failures:
            raise GatewayError("sensitive external request information")
        return self.result


class FakeSender:
    """Record accepted callback identities even when a later database commit fails."""

    def __init__(self, failures: int = 0) -> None:
        """Configure failures before a successful delivery."""
        self.failures = failures
        self.events: list[UUID] = []

    async def send(self, payment: PaymentSnapshot, event_id: UUID) -> None:
        """Retain the external call identity before reporting its outcome."""
        self.events.append(event_id)
        if len(self.events) <= self.failures:
            raise WebhookError("sensitive callback credentials")


async def seed_payment(
    database: Database, url: str = "http://receiver.test/callback"
) -> WorkflowEvent:
    """Use the production creation transaction to seed a payment and its source event."""
    command = NewPayment(Decimal("125.50"), Currency.RUB, "Order", {}, url)
    async with database.sessions() as session:
        payment = await PaymentRepository(session).create_or_get(
            command, str(uuid7()), request_fingerprint(command)
        )
    return await stage_event(database, payment.payment_id, WorkflowStage.PROCESSING, 1)


async def stage_event(
    database: Database,
    payment_id: UUID,
    stage: WorkflowStage,
    attempt: int,
    make_due: bool = False,
) -> WorkflowEvent:
    """Load a real persisted intent, optionally advancing its due date for fast tests."""
    async with database.sessions() as session, session.begin():
        event = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.payment_id == payment_id,
                OutboxEvent.payload["stage"].astext == stage.value,
                OutboxEvent.payload["attempt"].as_integer() == attempt,
                OutboxEvent.destination != DEAD_LETTER_QUEUE,
            )
        )
        assert event is not None
        if make_due:
            event.available_at = func.clock_timestamp()
        return WorkflowEnvelope.model_validate(event.payload).to_event()


async def state(database: Database, payment_id: UUID) -> Payment:
    """Read committed payment state through a fresh session."""
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


async def intents(database: Database, payment_id: UUID) -> list[OutboxEvent]:
    """Inspect durable events after the transaction without relying on ORM identity maps."""
    async with database.sessions() as session:
        return list(
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.payment_id == payment_id)
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
            )
        )


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_terminal_result_and_webhook_duplicates(
    database: Database, result: PaymentStatus
) -> None:
    """Process once, notify business declines too, and skip replayed completed stages."""
    event = await seed_payment(database)
    gateway, sender = FakeGateway(result), FakeSender()
    processor = PaymentProcessor(WorkflowRepository(database), gateway, sender)
    await asyncio.gather(*[processor.handle(event) for _ in range(5)])
    processed = await state(database, event.payment_id)
    assert processed.status == result
    assert processed.processed_at is not None
    assert processed.processing_attempts == gateway.calls == 1
    webhook = await stage_event(database, event.payment_id, WorkflowStage.WEBHOOK, 1)
    await asyncio.gather(*[processor.handle(webhook) for _ in range(5)])
    finished = await state(database, event.payment_id)
    assert finished.status == result
    assert finished.processed_at == processed.processed_at
    assert finished.webhook_status == WebhookStatus.DELIVERED
    assert finished.webhook_attempts == len(sender.events) == 1
    assert finished.webhook_delivered_at is not None
    assert len(await intents(database, event.payment_id)) == 2


async def test_webhook_two_failures_then_success_with_restart(database: Database) -> None:
    """Preserve payment result and independent retry state across new processor instances."""
    event = await seed_payment(database)
    gateway, sender = FakeGateway(), FakeSender(failures=2)
    await PaymentProcessor(WorkflowRepository(database), gateway, sender).handle(event)
    terminal = await state(database, event.payment_id)
    for attempt in range(1, 4):
        webhook = await stage_event(
            database, event.payment_id, WorkflowStage.WEBHOOK, attempt, make_due=True
        )
        before = datetime.now(UTC)
        processor = PaymentProcessor(WorkflowRepository(database), gateway, sender)
        await processor.handle(webhook)
        await processor.handle(webhook)
        payment = await state(database, event.payment_id)
        assert payment.webhook_attempts == attempt
        assert payment.status == terminal.status
        assert payment.processed_at == terminal.processed_at
        if attempt < 3:
            queued = await intents(database, event.payment_id)
            retry = queued[-1]
            assert retry.available_at >= before + timedelta(seconds=2**attempt)
            assert retry.payload["attempt"] == attempt + 1
            assert payment.webhook_status == WebhookStatus.PENDING
            assert payment.webhook_error == "WebhookError"
    assert payment.webhook_status == WebhookStatus.DELIVERED
    assert payment.webhook_error is None
    assert gateway.calls == 1
    assert len(sender.events) == 3
    assert len(set(sender.events)) == 1
    assert not any(item.destination == DEAD_LETTER_QUEUE for item in queued)


async def test_exhausted_webhook_does_not_fail_payment(database: Database) -> None:
    """Durably dead-letter exactly once after three delivery failures and retain success."""
    event = await seed_payment(database)
    gateway, sender = FakeGateway(), FakeSender(failures=99)
    processor = PaymentProcessor(WorkflowRepository(database), gateway, sender)
    await processor.handle(event)
    for attempt in range(1, 4):
        webhook = await stage_event(
            database, event.payment_id, WorkflowStage.WEBHOOK, attempt, make_due=True
        )
        await processor.handle(webhook)
        await processor.handle(webhook)
    payment = await state(database, event.payment_id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_status == WebhookStatus.FAILED
    assert payment.webhook_attempts == 3
    assert payment.webhook_delivered_at is None
    events = await intents(database, event.payment_id)
    dead = [item for item in events if item.destination == DEAD_LETTER_QUEUE]
    assert len(dead) == 1
    assert dead[0].payload["stage"] == "webhook"
    assert dead[0].payload["attempt"] == 3
    assert dead[0].payload["reason"] == "WebhookError"
    assert "sensitive" not in str(dead[0].payload)
    assert gateway.calls == 1
    assert len(sender.events) == 3


@pytest.mark.parametrize("failures", [2, 99])
async def test_processing_technical_retries(database: Database, failures: int) -> None:
    """Retry technical faults three times and publish a final failure result on exhaustion."""
    initial = await seed_payment(database)
    gateway, sender = FakeGateway(failures=failures), FakeSender()
    for attempt in range(1, 4):
        event = await stage_event(
            database, initial.payment_id, WorkflowStage.PROCESSING, attempt, make_due=True
        )
        before = datetime.now(UTC)
        processor = PaymentProcessor(WorkflowRepository(database), gateway, sender)
        await processor.handle(event)
        await processor.handle(event)
        payment = await state(database, initial.payment_id)
        assert payment.processing_attempts == gateway.calls == attempt
        if attempt < 3:
            assert payment.status == PaymentStatus.PENDING
            events = await intents(database, initial.payment_id)
            assert events[-1].available_at >= before + timedelta(seconds=2**attempt)
    assert payment.status == (PaymentStatus.SUCCEEDED if failures == 2 else PaymentStatus.FAILED)
    assert payment.processed_at is not None
    webhook = await stage_event(database, initial.payment_id, WorkflowStage.WEBHOOK, 1)
    await processor.handle(webhook)
    payment = await state(database, initial.payment_id)
    assert payment.webhook_status == WebhookStatus.DELIVERED
    dead = [
        item
        for item in await intents(database, initial.payment_id)
        if item.destination == DEAD_LETTER_QUEUE
    ]
    assert len(dead) == (0 if failures == 2 else 1)
    if dead:
        assert dead[0].payload["stage"] == "processing"
        assert dead[0].payload["attempt"] == 3


async def test_early_retry_and_unknown_source_preserve_state(database: Database) -> None:
    """Leave scheduled work recoverable and reject fabricated identities without executing it."""
    initial = await seed_payment(database)
    gateway = FakeGateway(failures=1)
    processor = PaymentProcessor(WorkflowRepository(database), gateway, FakeSender())
    await processor.handle(initial)
    retry = await stage_event(database, initial.payment_id, WorkflowStage.PROCESSING, 2)
    with pytest.raises(WorkflowNotReady):
        await processor.handle(retry)
    with pytest.raises(InvalidWorkflow):
        await processor.handle(
            WorkflowEvent(uuid7(), initial.payment_id, WorkflowStage.PROCESSING, 2)
        )
    assert (await state(database, initial.payment_id)).processing_attempts == 1
    assert gateway.calls == 1


async def test_webhook_acceptance_before_failed_commit_reuses_id(database: Database) -> None:
    """Demonstrate at-least-once HTTP effects while preventing repeated gateway processing."""
    initial = await seed_payment(database)
    gateway, sender = FakeGateway(), FakeSender()
    processor = PaymentProcessor(WorkflowRepository(database), gateway, sender)
    await processor.handle(initial)
    webhook = await stage_event(database, initial.payment_id, WorkflowStage.WEBHOOK, 1)
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "ALTER TABLE payments ADD CONSTRAINT fail_delivery_commit "
            "CHECK (webhook_status <> 'delivered')"
        )
    with pytest.raises(IntegrityError):
        await processor.handle(webhook)
    pending = await state(database, initial.payment_id)
    assert pending.status == PaymentStatus.SUCCEEDED
    assert pending.webhook_attempts == 0
    assert pending.webhook_status == WebhookStatus.PENDING
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql(
            "ALTER TABLE payments DROP CONSTRAINT fail_delivery_commit"
        )
    await PaymentProcessor(WorkflowRepository(database), gateway, sender).handle(webhook)
    assert len(sender.events) == 2
    assert sender.events[0] == sender.events[1]
    assert gateway.calls == 1
    assert (await state(database, initial.payment_id)).webhook_attempts == 1


async def test_cancellation_rolls_back_processing(database: Database) -> None:
    """Leave a cancelled gateway attempt recoverable with no new event or counted attempt."""
    initial = await seed_payment(database)
    started = asyncio.Event()

    class StalledGateway:
        """Wait indefinitely until the consumer task is cancelled."""

        async def process(self, payment: PaymentSnapshot) -> PaymentStatus:
            """Signal operation entry before waiting for cancellation."""
            started.set()
            await asyncio.Event().wait()
            return PaymentStatus.SUCCEEDED

    processor = PaymentProcessor(WorkflowRepository(database), StalledGateway(), FakeSender())
    task = asyncio.create_task(processor.handle(initial))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    payment = await state(database, initial.payment_id)
    assert payment.processing_attempts == 0
    assert payment.status == PaymentStatus.PENDING
    assert len(await intents(database, initial.payment_id)) == 1


async def test_malformed_intents_deduplicate_without_sensitive_data(database: Database) -> None:
    """Keep one DLQ intent after repeated invalid input, without storing its raw body."""
    store = WorkflowRepository(database)
    body = b'{"password":"private-credential",broken-json'
    await asyncio.gather(*[store.record_invalid(body, "", "malformed_message") for _ in range(5)])
    async with database.sessions() as session:
        events = list(await session.scalars(select(OutboxEvent)))
    assert len(events) == 1
    event = events[0]
    assert event.destination == DEAD_LETTER_QUEUE
    assert event.payment_id is None
    assert event.payload["reason"] == "malformed_message"
    assert "private-credential" not in str(event.payload)


async def test_gateway_timeout_schedules_retry(database: Database) -> None:
    """Persist a timeout as one technical attempt while leaving caller cancellation distinct."""
    event = await seed_payment(database)

    async def stalled(delay: float) -> None:
        """Ignore elapsed time until the use case's total operation deadline expires."""
        await asyncio.Event().wait()

    processor = PaymentProcessor(
        WorkflowRepository(database),
        EmulatedGateway(stalled),
        FakeSender(),
        processing_timeout=0.02,
    )
    await processor.handle(event)
    payment = await state(database, event.payment_id)
    assert payment.processing_attempts == 1
    assert payment.processing_error == "TimeoutError"
    assert payment.status == PaymentStatus.PENDING
    retry = await stage_event(database, event.payment_id, WorkflowStage.PROCESSING, 2)
    assert retry.attempt == 2


async def test_local_http_delivery_with_actual_gateway(database: Database) -> None:
    """Deliver over loopback HTTP with real gateway latency and persisted HTTP retries."""
    bodies: list[dict[str, object]] = []
    headers: list[bytes] = []

    async def receive(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Return two HTTP failures and a success without relying on external servers."""
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in head.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            body = await reader.readexactly(length)
            headers.append(head)
            bodies.append(json.loads(body))
            status = b"500 Internal Server Error" if len(bodies) < 3 else b"204 No Content"
            writer.write(
                b"HTTP/1.1 " + status + b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receive, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    async with server, httpx.AsyncClient(timeout=1, trust_env=False) as client:
        initial = await seed_payment(database, f"{origin}/callback")
        processor = PaymentProcessor(
            WorkflowRepository(database),
            EmulatedGateway(),
            HttpWebhookSender(client, WebhookPolicy(frozenset({origin}))),
        )
        await processor.handle(initial)
        for attempt in range(1, 4):
            event = await stage_event(
                database, initial.payment_id, WorkflowStage.WEBHOOK, attempt, make_due=True
            )
            await processor.handle(event)
        payment = await state(database, initial.payment_id)
        assert payment.status == gateway_outcome(initial.payment_id)[0]
        assert payment.webhook_status == WebhookStatus.DELIVERED
        assert payment.webhook_attempts == 3
    assert len(bodies) == 3
    assert bodies[0] == bodies[1] == bodies[2]
    assert all(b"x-api-key:" not in head.lower() for head in headers)


async def test_total_webhook_timeout_preserves_result(database: Database) -> None:
    """Bound the full callback operation even when the HTTP transport waits indefinitely."""
    event = await seed_payment(database)

    async def stalled(request: httpx.Request) -> httpx.Response:
        """Wait for outer cancellation instead of producing response headers."""
        await asyncio.Event().wait()
        return httpx.Response(204)

    policy = WebhookPolicy(frozenset({"http://receiver.test"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(stalled)) as client:
        processor = PaymentProcessor(
            WorkflowRepository(database),
            FakeGateway(),
            HttpWebhookSender(client, policy),
            webhook_timeout=0.02,
        )
        await processor.handle(event)
        webhook = await stage_event(database, event.payment_id, WorkflowStage.WEBHOOK, 1)
        await processor.handle(webhook)
    payment = await state(database, event.payment_id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_status == WebhookStatus.PENDING
    assert payment.webhook_attempts == 1
    assert payment.webhook_error == "TimeoutError"
    assert (await stage_event(database, event.payment_id, WorkflowStage.WEBHOOK, 2)).attempt == 2


async def test_retry_deadline_uses_database_clock(database: Database) -> None:
    """Keep backoff correct when the application's wall clock differs from PostgreSQL."""
    event = await seed_payment(database)
    before = datetime.now(UTC)
    with patch("payments.application.processing.datetime") as clock:
        clock.now.return_value = before + timedelta(hours=1)
        await PaymentProcessor(
            WorkflowRepository(database), FakeGateway(failures=1), FakeSender()
        ).handle(event)
    retry = (await intents(database, event.payment_id))[-1]
    assert before + timedelta(seconds=2) <= retry.available_at
    assert retry.available_at <= datetime.now(UTC) + timedelta(seconds=2)
