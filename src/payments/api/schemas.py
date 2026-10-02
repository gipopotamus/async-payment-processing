"""HTTP contracts, including exact money input and PostgreSQL-safe JSON data."""

from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from payments.core.domain import (
    AMOUNT_PRECISION,
    AMOUNT_SCALE,
    Currency,
    NewPayment,
    PaymentStatus,
    WebhookStatus,
)
from payments.core.domain import JsonValue as DomainJsonValue


def validate_json(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Reject non-finite numbers and strings unsupported by PostgreSQL JSONB."""
    pending: list[JsonValue] = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, str):
            validate_text(item)
        elif isinstance(item, float) and not isfinite(item):
            raise ValueError("Metadata numbers must be finite")
    return value


def validate_text(value: str) -> str:
    """Reject characters that cannot be represented by PostgreSQL UTF-8 text."""
    if "\x00" in value:
        raise ValueError("Text must not contain NUL characters")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("Text must contain valid Unicode characters") from error
    return value


class CreatePaymentRequest(BaseModel):
    """Accept decimal strings and supported currencies without silent rounding."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    amount: Annotated[
        Decimal,
        Field(gt=0, max_digits=AMOUNT_PRECISION, decimal_places=AMOUNT_SCALE, allow_inf_nan=False),
    ]
    currency: Currency
    description: str = Field(default="", max_length=2000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount", mode="before", json_schema_input_type=str)
    @classmethod
    def require_decimal_string(cls, value: object) -> str:
        """Avoid precision loss from JSON numbers decoded as binary floats."""
        if not isinstance(value, str):
            raise ValueError("Amount must be a decimal string")
        return value

    @field_validator("description")
    @classmethod
    def validate_description(cls, value: str) -> str:
        """Validate text before it reaches PostgreSQL."""
        return validate_text(value)

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Ensure nested values can be stored without changing their meaning."""
        return validate_json(value)

    def to_command(self) -> NewPayment:
        """Translate validated HTTP fields into the application's input contract."""
        metadata: dict[str, DomainJsonValue] = self.metadata
        return NewPayment(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=metadata,
            webhook_url=str(self.webhook_url),
        )


class AcceptedPayment(BaseModel):
    """Acknowledge a committed creation or an equivalent replay."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(AcceptedPayment):
    """Expose payment details and notification progress without internal hashes."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    webhook_status: WebhookStatus
    webhook_attempts: int
    webhook_delivered_at: datetime | None
