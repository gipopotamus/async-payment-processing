"""Shared payment vocabulary and limits, independent of transport and storage."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Final
from uuid import UUID

AMOUNT_PRECISION: Final = 18
AMOUNT_SCALE: Final = 2
MAX_ATTEMPTS: Final = 3
NEW_PAYMENTS_QUEUE: Final = "payments.new"
PAYMENT_CREATED_EVENT: Final = "payment.created"

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
