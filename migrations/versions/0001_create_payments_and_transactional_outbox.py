"""create payments and transactional outbox.

Revision ID: 0001
Revises: None
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this revision's frozen schema changes."""
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column(
            "currency",
            sa.Enum(
                "RUB",
                "USD",
                "EUR",
                name=op.f("ck_payments_currency"),
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("description", sa.Text(), server_default="", nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "pending",
                "succeeded",
                "failed",
                name=op.f("ck_payments_payment_status"),
                native_enum=False,
                create_constraint=True,
            ),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_attempts", sa.SmallInteger(), server_default="0", nullable=False),
        sa.Column("processing_error", sa.Text(), nullable=True),
        sa.Column(
            "webhook_status",
            sa.Enum(
                "pending",
                "delivered",
                "failed",
                name=op.f("ck_payments_webhook_status"),
                native_enum=False,
                create_constraint=True,
            ),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("webhook_attempts", sa.SmallInteger(), server_default="0", nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name=op.f("ck_payments_processed_timestamp"),
        ),
        sa.CheckConstraint(
            "(webhook_status = 'delivered' AND webhook_delivered_at IS NOT NULL) OR "
            "(webhook_status <> 'delivered' AND webhook_delivered_at IS NULL)",
            name=op.f("ck_payments_webhook_timestamp"),
        ),
        sa.CheckConstraint(
            "amount > 0 AND amount <> 'NaN'::numeric", name=op.f("ck_payments_positive_amount")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata) = 'object'", name=op.f("ck_payments_object_metadata")
        ),
        sa.CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$'", name=op.f("ck_payments_valid_request_hash")
        ),
        sa.CheckConstraint(
            "webhook_status = 'pending' OR status <> 'pending'",
            name=op.f("ck_payments_webhook_requires_result"),
        ),
        sa.CheckConstraint(
            "length(btrim(idempotency_key)) > 0", name=op.f("ck_payments_nonempty_idempotency_key")
        ),
        sa.CheckConstraint(
            "length(webhook_url) > 0", name=op.f("ck_payments_nonempty_webhook_url")
        ),
        sa.CheckConstraint(
            "processing_attempts BETWEEN 0 AND 3", name=op.f("ck_payments_processing_attempts")
        ),
        sa.CheckConstraint(
            "webhook_attempts BETWEEN 0 AND 3", name=op.f("ck_payments_webhook_attempts")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("destination", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name=op.f("ck_outbox_object_payload")
        ),
        sa.CheckConstraint("length(destination) > 0", name=op.f("ck_outbox_nonempty_destination")),
        sa.CheckConstraint("length(event_type) > 0", name=op.f("ck_outbox_nonempty_event_type")),
        sa.CheckConstraint(
            "publish_attempts >= 0", name=op.f("ck_outbox_nonnegative_publish_attempts")
        ),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name=op.f("fk_outbox_payment_id_payments"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_unpublished_available_at",
        "outbox",
        ["available_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Reverse this revision; use only on disposable or backed-up databases."""
    op.drop_index(
        "ix_outbox_unpublished_available_at",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
    op.drop_table("payments")
