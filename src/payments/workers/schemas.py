"""Validation of messages received from RabbitMQ."""

from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from payments.core.domain import MAX_ATTEMPTS, WorkflowEvent, WorkflowStage


class WorkflowEnvelope(BaseModel):
    """Validate the broker boundary after raw bytes have reached the handler."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    event_id: UUID
    payment_id: UUID
    stage: WorkflowStage
    attempt: Annotated[StrictInt, Field(ge=1, le=MAX_ATTEMPTS)]

    def to_event(self) -> WorkflowEvent:
        """Detach the application's workflow identity from Pydantic and AMQP."""
        return WorkflowEvent(self.event_id, self.payment_id, self.stage, self.attempt)
