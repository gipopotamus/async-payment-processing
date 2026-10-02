"""Shared payment vocabulary and limits, independent of transport and storage."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Final
from uuid import UUID

AMOUNT_PRECISION: Final = 18
AMOUNT_SCALE: Final = 2
MAX_ATTEMPTS: Final = 3
NEW_PAYMENTS_QUEUE: Final = "payments.new"
PAYMENT_CREATED_EVENT: Final = "payment.created"
DEAD_LETTER_QUEUE: Final = "payments.dlq"
DEAD_LETTER_EXCHANGE: Final = "payments.dead-letter"

type JsonValue = str | int | float | bool | None | list[JsonValue] | dict[str, JsonValue]


class Currency(StrEnum):
    """Currencies supported by the assignment, each with two fractional digits."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Distinguish an unprocessed payment from its terminal gateway outcome."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WebhookStatus(StrEnum):
    """Track result delivery independently of the payment's gateway outcome."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


class WorkflowStage(StrEnum):
    """Separate gateway processing from delivery of its terminal result."""

    PROCESSING = "processing"
    WEBHOOK = "webhook"


@dataclass(frozen=True)
class NewPayment:
    """Carry validated creation data independently of an HTTP request."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str


@dataclass(frozen=True)
class PaymentSnapshot:
    """Return fully loaded payment data without exposing a live ORM object."""

    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_status: WebhookStatus
    webhook_attempts: int
    webhook_delivered_at: datetime | None
    request_hash: str = field(repr=False)


class IdempotencyConflict(Exception):
    """Signal that an existing key belongs to a different normalized request."""


class PaymentNotFound(Exception):
    """Signal that no payment exists for the requested identifier."""


class InvalidWebhook(Exception):
    """Signal that a callback destination is outside the configured policy."""


@dataclass(frozen=True)
class Publication:
    """Carry a stable outbox identity and payload into the broker adapter."""

    event_id: UUID
    payment_id: UUID | None
    event_type: str
    destination: str
    payload: dict[str, JsonValue]


class PublicationError(Exception):
    """Signal a retryable publication failure without embedding sensitive details."""


@dataclass(frozen=True)
class WorkflowEvent:
    """Identify a persisted stage and expected attempt, independently of AMQP."""

    event_id: UUID
    payment_id: UUID
    stage: WorkflowStage
    attempt: int

    def payload(self) -> dict[str, JsonValue]:
        """Return the canonical JSON contract used by the outbox and consumer."""
        return {
            "event_id": str(self.event_id),
            "payment_id": str(self.payment_id),
            "stage": self.stage.value,
            "attempt": self.attempt,
        }


@dataclass(frozen=True)
class ScheduledPublication(Publication):
    """Add durable availability to an event intent produced in a payment transaction."""

    delay: timedelta


@dataclass
class PaymentWork:
    """Carry locked state and new publication intents without exposing ORM entities."""

    payment: PaymentSnapshot
    processing_attempts: int
    processing_error: str | None
    webhook_error: str | None
    events: list[ScheduledPublication] = field(default_factory=list)


class GatewayError(Exception):
    """Signal a technical gateway failure; business declines are terminal results."""


class WebhookError(Exception):
    """Signal a failed callback attempt without exposing URL credentials or bodies."""


class InvalidWorkflow(Exception):
    """Signal a message that does not match a stored publication or valid payment stage."""


class WorkflowNotReady(Exception):
    """Keep an early message recoverable until its durable due date arrives."""
