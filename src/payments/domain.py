"""Shared payment vocabulary and limits, independent of transport and storage."""

from enum import StrEnum
from typing import Final

AMOUNT_PRECISION: Final = 18
AMOUNT_SCALE: Final = 2
MAX_ATTEMPTS: Final = 3

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
