"""Loopback demo receiver with controlled failures and in-memory event deduplication."""

import argparse
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict

from payments.domain import Currency, PaymentStatus


class Notification(BaseModel):
    """Validate the terminal-result contract produced by the payment consumer."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    created_at: datetime
    processed_at: datetime


class Receipt(BaseModel):
    """Report observed attempts and whether the receiver accepted the unique event."""

    notification: Notification
    attempts: int
    accepted: bool


@dataclass
class ReceiverState:
    """Hold only local demo evidence, isolated for each application instance."""

    notifications: dict[UUID, Notification] = field(default_factory=dict)
    attempts: dict[UUID, int] = field(default_factory=dict)
    accepted: set[UUID] = field(default_factory=set)


def create_receiver(failures: int = 2) -> FastAPI:
    """Fail the first N attempts per event, then accept and deduplicate identical replays."""
    if failures < 0:
        raise ValueError("Failures must be nonnegative")
    # ponytail: in-memory demo deduplication; durable receiver storage for production.
    state = ReceiverState()
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.post("/callback", status_code=204)
    async def callback(notification: Notification) -> Response:
        """Reject changed bodies for an event ID and emulate transient delivery failures."""
        event_id = notification.event_id
        previous = state.notifications.get(event_id)
        if previous is not None and previous != notification:
            raise HTTPException(409, "Event ID already belongs to another result")
        state.notifications[event_id] = notification
        state.attempts[event_id] = state.attempts.get(event_id, 0) + 1
        if event_id not in state.accepted and state.attempts[event_id] <= failures:
            raise HTTPException(503, "Emulated transient receiver failure")
        state.accepted.add(event_id)
        return Response(status_code=204)

    @app.get("/receipts")
    async def receipts() -> list[Receipt]:
        """Inspect only the notifications received by this local demo process."""
        return [
            Receipt(
                notification=value, attempts=state.attempts[key], accepted=key in state.accepted
            )
            for key, value in state.notifications.items()
        ]

    return app


def main() -> None:
    """Serve on loopback with configurable failures for success and DLQ demonstrations."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--failures", type=int, default=2)
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    if args.failures < 0 or not 1 <= args.port <= 65535:
        parser.error("failures must be nonnegative and port must be between 1 and 65535")
    uvicorn.run(create_receiver(args.failures), host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
