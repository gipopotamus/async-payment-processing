"""Payment workflow rules using injected transaction and external-operation ports."""

import asyncio
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid5, uuid7

from payments.domain import (
    DEAD_LETTER_QUEUE,
    MAX_ATTEMPTS,
    NEW_PAYMENTS_QUEUE,
    GatewayError,
    InvalidWorkflow,
    PaymentSnapshot,
    PaymentStatus,
    PaymentWork,
    ScheduledPublication,
    WebhookError,
    WebhookStatus,
    WorkflowEvent,
    WorkflowStage,
)


class WorkflowStore(Protocol):
    """Lock payment state and commit changes together with newly scheduled intents."""

    def work(self, event: WorkflowEvent) -> AbstractAsyncContextManager[PaymentWork]:
        """Yield stored state after source/due-date validation; roll back on exceptions."""
        ...

    async def record_invalid(
        self, body: bytes, message_id: str, reason: str, payment_id: UUID | None = None
    ) -> None:
        """Deduplicate a sanitized DLQ intent without persisting an untrusted body."""
        ...


class PaymentGateway(Protocol):
    """Return a stable terminal result for the payment's idempotent operation ID."""

    async def process(self, payment: PaymentSnapshot) -> PaymentStatus:
        """Return succeeded/failed; raise GatewayError for a technical failure."""
        ...


class WebhookSender(Protocol):
    """Deliver a terminal result with a stable receiver-deduplication identity."""

    async def send(self, payment: PaymentSnapshot, event_id: UUID) -> None:
        """Accept only successful HTTP delivery, or raise WebhookError."""
        ...


def webhook_event_id(payment_id: UUID) -> UUID:
    """Keep the result notification's ID stable across retries and process restarts."""
    return uuid5(payment_id, "payment.result")


def schedule(work: PaymentWork, stage: WorkflowStage, attempt: int, delay: int = 0) -> None:
    """Append one stage intent for atomic persistence with the current payment state."""
    event = WorkflowEvent(uuid7(), work.payment.payment_id, stage, attempt)
    work.events.append(
        ScheduledPublication(
            event.event_id,
            event.payment_id,
            f"payment.{stage.value}",
            NEW_PAYMENTS_QUEUE,
            event.payload(),
            timedelta(seconds=delay),
        )
    )


def dead_letter(work: PaymentWork, stage: WorkflowStage, attempt: int, reason: str) -> None:
    """Retain an exhausted stage as an outbox intent with no external direct publish."""
    event_id = uuid7()
    payment_id = work.payment.payment_id
    work.events.append(
        ScheduledPublication(
            event_id,
            payment_id,
            "payment.dead-letter",
            DEAD_LETTER_QUEUE,
            {
                "event_id": str(event_id),
                "payment_id": str(payment_id),
                "stage": stage.value,
                "attempt": attempt,
                "reason": reason,
            },
            timedelta(),
        )
    )


class PaymentProcessor:
    """Apply one stage with state-based duplicate handling and durable retries.

    External effects may repeat if cancellation or commit failure rolls back state.
    Gateway idempotency and the stable webhook identity define that failure contract.
    """

    def __init__(
        self,
        store: WorkflowStore,
        gateway: PaymentGateway,
        sender: WebhookSender,
        processing_timeout: float = 10,
        webhook_timeout: float = 5,
    ) -> None:
        """Inject transaction ownership, external adapters, and operation deadlines."""
        self._store = store
        self._gateway = gateway
        self._sender = sender
        self._processing_timeout = processing_timeout
        self._webhook_timeout = webhook_timeout

    async def handle(self, event: WorkflowEvent) -> None:
        """Commit state/retry/DLQ before returning to the transport for acknowledgement."""
        async with self._store.work(event) as work:
            if event.stage == WorkflowStage.PROCESSING:
                await self._process(work, event.attempt)
            else:
                await self._deliver(work, event.attempt)

    async def record_invalid(
        self, body: bytes, message_id: str, reason: str, payment_id: UUID | None = None
    ) -> None:
        """Commit one sanitized dead-letter intent for malformed message redeliveries."""
        await self._store.record_invalid(body, message_id, reason, payment_id)

    async def _process(self, work: PaymentWork, attempt: int) -> None:
        if work.payment.status != PaymentStatus.PENDING or attempt <= work.processing_attempts:
            return
        if attempt != work.processing_attempts + 1:
            raise InvalidWorkflow("Unexpected processing attempt")
        work.processing_attempts = attempt
        try:
            async with asyncio.timeout(self._processing_timeout):
                result = await self._gateway.process(work.payment)
            if result not in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}:
                raise GatewayError("Nonterminal gateway result")
        except (GatewayError, TimeoutError) as error:
            work.processing_error = type(error).__name__
            if attempt < MAX_ATTEMPTS:
                schedule(work, WorkflowStage.PROCESSING, attempt + 1, 2**attempt)
                return
            result = PaymentStatus.FAILED
            dead_letter(work, WorkflowStage.PROCESSING, attempt, work.processing_error)
        else:
            work.processing_error = None
        work.payment = replace(work.payment, status=result, processed_at=datetime.now(UTC))
        schedule(work, WorkflowStage.WEBHOOK, 1)

    async def _deliver(self, work: PaymentWork, attempt: int) -> None:
        payment = work.payment
        if payment.status == PaymentStatus.PENDING:
            raise InvalidWorkflow("Webhook requires a terminal payment")
        if payment.webhook_status != WebhookStatus.PENDING or attempt <= payment.webhook_attempts:
            return
        if attempt != payment.webhook_attempts + 1:
            raise InvalidWorkflow("Unexpected webhook attempt")
        work.payment = replace(payment, webhook_attempts=attempt)
        try:
            async with asyncio.timeout(self._webhook_timeout):
                await self._sender.send(payment, webhook_event_id(payment.payment_id))
        except (WebhookError, TimeoutError) as error:
            work.webhook_error = type(error).__name__
            if attempt < MAX_ATTEMPTS:
                schedule(work, WorkflowStage.WEBHOOK, attempt + 1, 2**attempt)
            else:
                work.payment = replace(work.payment, webhook_status=WebhookStatus.FAILED)
                dead_letter(work, WorkflowStage.WEBHOOK, attempt, work.webhook_error)
        else:
            work.webhook_error = None
            work.payment = replace(
                work.payment,
                webhook_status=WebhookStatus.DELIVERED,
                webhook_delivered_at=datetime.now(UTC),
            )
