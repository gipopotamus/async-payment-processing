"""PostgreSQL persistence models and database-enforced integrity constraints."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid7

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Text,
    func,
    text,
)
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.core.domain import (
    AMOUNT_PRECISION,
    AMOUNT_SCALE,
    MAX_ATTEMPTS,
    Currency,
    JsonValue,
    PaymentStatus,
    WebhookStatus,
)


class Base(DeclarativeBase):
    """Share stable constraint names with Alembic's schema comparison."""

    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


def enum_type(enum_class: type[StrEnum], name: str) -> SqlEnum:
    """Persist enum values as strings with a named CHECK constraint."""
    return SqlEnum(
        enum_class,
        name=name,
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        values_callable=lambda members: [member.value for member in members],
    )


class Payment(Base):
    """Persist a payment, its idempotent request identity, and delivery progress.

    Terminal results have processed_at set. Webhook delivery and attempt counters
    remain independent so notification retries cannot restart a completed payment.
    """

    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0 AND amount <> 'NaN'::numeric", name="positive_amount"),
        CheckConstraint("length(btrim(idempotency_key)) > 0", name="nonempty_idempotency_key"),
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="valid_request_hash"),
        CheckConstraint("length(webhook_url) > 0", name="nonempty_webhook_url"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="object_metadata"),
        CheckConstraint(
            f"processing_attempts BETWEEN 0 AND {MAX_ATTEMPTS}", name="processing_attempts"
        ),
        CheckConstraint(f"webhook_attempts BETWEEN 0 AND {MAX_ATTEMPTS}", name="webhook_attempts"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name="processed_timestamp",
        ),
        CheckConstraint(
            "(webhook_status = 'delivered' AND webhook_delivered_at IS NOT NULL) OR "
            "(webhook_status <> 'delivered' AND webhook_delivered_at IS NULL)",
            name="webhook_timestamp",
        ),
        CheckConstraint(
            "webhook_status = 'pending' OR status <> 'pending'", name="webhook_requires_result"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    amount: Mapped[Decimal] = mapped_column(Numeric(AMOUNT_PRECISION, AMOUNT_SCALE))
    currency: Mapped[Currency] = mapped_column(enum_type(Currency, "currency"))
    description: Mapped[str] = mapped_column(Text, default="", server_default="")
    # DeclarativeBase.metadata is reserved; retain the public column name in SQL.
    payment_metadata: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[PaymentStatus] = mapped_column(
        enum_type(PaymentStatus, "payment_status"),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_attempts: Mapped[int] = mapped_column(SmallInteger, default=0, server_default="0")
    processing_error: Mapped[str | None] = mapped_column(Text)
    webhook_status: Mapped[WebhookStatus] = mapped_column(
        enum_type(WebhookStatus, "webhook_status"),
        default=WebhookStatus.PENDING,
        server_default=WebhookStatus.PENDING.value,
    )
    webhook_attempts: Mapped[int] = mapped_column(SmallInteger, default=0, server_default="0")
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_error: Mapped[str | None] = mapped_column(Text)


class OutboxEvent(Base):
    """Retain a broker publication intent until confirmed delivery.

    payment_id is optional so malformed incoming messages can still be represented
    as DLQ intents. An existing payment cannot be deleted while its events remain.
    """

    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("length(event_type) > 0", name="nonempty_event_type"),
        CheckConstraint("length(destination) > 0", name="nonempty_destination"),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="object_payload"),
        CheckConstraint("publish_attempts >= 0", name="nonnegative_publish_attempts"),
        Index(
            "ix_outbox_unpublished_available_at",
            "available_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    payment_id: Mapped[UUID | None] = mapped_column(ForeignKey("payments.id", ondelete="RESTRICT"))
    event_type: Mapped[str] = mapped_column(String(64))
    destination: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)
