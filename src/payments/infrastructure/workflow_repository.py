"""Locked PostgreSQL workflow transactions and deduplicated malformed-message intents."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from hashlib import sha256
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from payments.core.domain import (
    DEAD_LETTER_QUEUE,
    NEW_PAYMENTS_QUEUE,
    InvalidWorkflow,
    PaymentWork,
    WorkflowEvent,
    WorkflowNotReady,
)
from payments.infrastructure.database import Database
from payments.infrastructure.models import OutboxEvent, Payment
from payments.infrastructure.repository import payment_snapshot


class WorkflowRepository:
    """Own one transaction per stage and yield ORM-independent state to the use case."""

    def __init__(self, database: Database) -> None:
        """Borrow a process-scoped database; every message gets an isolated session."""
        self._database = database

    @asynccontextmanager
    async def work(self, event: WorkflowEvent) -> AsyncIterator[PaymentWork]:
        """Lock the payment, verify its event source, then persist state and intents.

        Raises:
            InvalidWorkflow: A message does not match a stored publication/payment.
            WorkflowNotReady: The event arrived before its persisted due date.
        """
        # Keep state and scheduled events atomic during the bounded external operation.
        async with self._database.session() as session, session.begin():
            payment = await session.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            stored = await session.get(OutboxEvent, event.event_id)
            if (
                payment is None
                or stored is None
                or stored.payment_id != event.payment_id
                or stored.destination != NEW_PAYMENTS_QUEUE
                or stored.payload != event.payload()
            ):
                raise InvalidWorkflow("Message does not match a stored workflow intent")
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            if stored.available_at > now:
                raise WorkflowNotReady("Workflow intent is not due")
            work = PaymentWork(
                payment_snapshot(payment),
                payment.processing_attempts,
                payment.processing_error,
                payment.webhook_error,
            )
            yield work
            payment.status = work.payment.status
            payment.processed_at = work.payment.processed_at
            payment.processing_attempts = work.processing_attempts
            payment.processing_error = work.processing_error
            payment.webhook_status = work.payment.webhook_status
            payment.webhook_attempts = work.payment.webhook_attempts
            payment.webhook_delivered_at = work.payment.webhook_delivered_at
            payment.webhook_error = work.webhook_error
            for intent in work.events:
                session.add(
                    OutboxEvent(
                        id=intent.event_id,
                        payment_id=intent.payment_id,
                        event_type=intent.event_type,
                        destination=intent.destination,
                        payload=intent.payload,
                        available_at=func.clock_timestamp() + intent.delay,
                    )
                )

    async def record_invalid(
        self, body: bytes, message_id: str, reason: str, payment_id: UUID | None = None
    ) -> None:
        """Hash the untrusted source and insert one sanitized DLQ intent on redelivery."""
        digest = sha256(message_id.encode("utf-8", errors="replace"))
        digest.update(b"\0")
        digest.update(body)
        fingerprint = digest.hexdigest()
        event_id = uuid5(NAMESPACE_URL, f"payments.invalid:{fingerprint}")
        safe_reason = "invalid_workflow" if reason == "invalid_workflow" else "malformed_message"
        async with self._database.session() as session, session.begin():
            known_id = (
                await session.scalar(select(Payment.id).where(Payment.id == payment_id))
                if payment_id is not None
                else None
            )
            await session.execute(
                insert(OutboxEvent)
                .values(
                    id=event_id,
                    payment_id=known_id,
                    event_type="payment.dead-letter",
                    destination=DEAD_LETTER_QUEUE,
                    payload={
                        "event_id": str(event_id),
                        "payment_id": str(known_id) if known_id is not None else None,
                        "stage": "validation",
                        "attempt": 0,
                        "reason": safe_reason,
                        "source_hash": fingerprint,
                    },
                )
                .on_conflict_do_nothing(index_elements=[OutboxEvent.id])
            )
