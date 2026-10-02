"""PostgreSQL adapter for atomic, concurrent idempotent payment creation."""

from uuid import UUID, uuid7

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.core.domain import (
    NEW_PAYMENTS_QUEUE,
    PAYMENT_CREATED_EVENT,
    NewPayment,
    PaymentSnapshot,
    WorkflowEvent,
    WorkflowStage,
)
from payments.infrastructure.models import OutboxEvent, Payment


def payment_snapshot(payment: Payment) -> PaymentSnapshot:
    """Detach loaded columns from persistence and preserve delivery progress."""
    return PaymentSnapshot(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_status=payment.webhook_status,
        webhook_attempts=payment.webhook_attempts,
        webhook_delivered_at=payment.webhook_delivered_at,
        request_hash=payment.request_hash,
    )


class PaymentRepository:
    """Own creation transactions in a request-scoped READ COMMITTED session.

    ON CONFLICT waits for a competing insert's transaction. A subsequent SELECT
    gets a fresh snapshot and sees its committed payment; only the winner creates
    the initial outbox event. Neither API locks nor direct broker calls are needed.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Borrow the session; its request dependency owns cleanup."""
        self._session = session

    async def create_or_get(
        self, command: NewPayment, idempotency_key: str, request_hash: str
    ) -> PaymentSnapshot:
        """Atomically commit payment and event, or load an existing payment by key."""
        async with self._session.begin():
            statement = (
                insert(Payment)
                .values(
                    id=uuid7(),
                    amount=command.amount,
                    currency=command.currency,
                    description=command.description,
                    payment_metadata=command.metadata,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    webhook_url=command.webhook_url,
                )
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment)
            )
            payment = (await self._session.execute(statement)).scalar_one_or_none()
            if payment is None:
                payment = await self._session.scalar(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
                if payment is None:
                    raise RuntimeError("Idempotent creation requires READ COMMITTED isolation")
            else:
                event_id = uuid7()
                self._session.add(
                    OutboxEvent(
                        id=event_id,
                        payment_id=payment.id,
                        event_type=PAYMENT_CREATED_EVENT,
                        destination=NEW_PAYMENTS_QUEUE,
                        payload=WorkflowEvent(
                            event_id, payment.id, WorkflowStage.PROCESSING, 1
                        ).payload(),
                    )
                )
            return payment_snapshot(payment)

    async def get(self, payment_id: UUID) -> PaymentSnapshot | None:
        """Read a payment without changing its state or publishing an event."""
        payment = await self._session.get(Payment, payment_id)
        return payment_snapshot(payment) if payment is not None else None
