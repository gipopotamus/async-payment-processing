"""HTTP composition root and API-wide authentication dependencies."""

import logging
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from hmac import compare_digest
from importlib.metadata import version
from typing import Annotated, Literal, cast
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Security, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from payments.database import Database, create_database
from payments.domain import IdempotencyConflict, InvalidWebhook, PaymentNotFound
from payments.repository import PaymentRepository
from payments.schemas import AcceptedPayment, CreatePaymentRequest, PaymentDetails
from payments.services import PaymentService, WebhookPolicy
from payments.settings import DatabaseSettings, Settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Report process liveness without claiming dependency readiness."""

    status: Literal["ok"] = "ok"


def get_settings(request: Request) -> Settings:
    """Resolve configuration from the current application instance."""
    return cast(Settings, request.app.state.settings)


def get_database(request: Request) -> Database:
    """Resolve the database composed during this application's lifespan."""
    return cast(Database, request.app.state.database)


async def get_session(
    database: Annotated[Database, Depends(get_database)],
) -> AsyncGenerator[AsyncSession]:
    """Provide an isolated session and roll back uncommitted work on close."""
    async with database.sessions() as session:
        yield session


def get_webhook_policy(request: Request) -> WebhookPolicy:
    """Resolve the prevalidated callback policy of the current application."""
    return cast(WebhookPolicy, request.app.state.webhook_policy)


def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    policy: Annotated[WebhookPolicy, Depends(get_webhook_policy)],
) -> PaymentService:
    """Compose a request-scoped use case without global persistence objects."""
    return PaymentService(PaymentRepository(session), policy)


def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    """Reject missing or invalid credentials using a constant-time comparison."""
    expected_key = settings.api_key.get_secret_value().encode("utf-8")
    if api_key is None or not compare_digest(api_key.encode("utf-8"), expected_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


def get_health() -> HealthResponse:
    """Confirm that the API process can serve an authenticated request."""
    return HealthResponse()


async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[!-~]+$")
    ],
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> AcceptedPayment:
    """Return an accepted payment only after its creation transaction commits."""
    try:
        payment = await service.create(body.to_command(), idempotency_key)
    except IdempotencyConflict as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    except InvalidWebhook as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)) from error
    return AcceptedPayment.model_validate(payment)


async def get_payment(
    payment_id: UUID,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentDetails:
    """Read payment details without exposing internal request fingerprints."""
    try:
        payment = await service.get(payment_id)
    except PaymentNotFound as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error)) from error
    return PaymentDetails.model_validate(payment)


async def validation_error(_request: Request, error: Exception) -> JSONResponse:
    """Report invalid field locations without echoing submitted values or metadata."""
    assert isinstance(error, RequestValidationError)
    details = [
        {"loc": item["loc"], "msg": item["msg"], "type": item["type"]} for item in error.errors()
    ]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"detail": details}
    )


async def storage_error(_request: Request, error: Exception) -> JSONResponse:
    """Expose a retryable storage failure without logging SQL values or credentials."""
    logger.error("Payment storage failure: %s", type(error).__name__)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Payment storage is temporarily unavailable"},
        headers={"Retry-After": "1"},
    )


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    """Compose an API with explicit configuration injection.

    Args:
        settings: Validated configuration. Load from the environment when omitted.
        database: Borrowed database resource. Otherwise create and own one at startup.

    Returns:
        An application whose routes require the configured API key.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Close only resources created by this application's composition root."""
        resource = database if database is not None else create_database(DatabaseSettings())
        app.state.database = resource
        try:
            yield
        finally:
            if database is None:
                await resource.close()

    app = FastAPI(
        title="Async Payment Processing",
        version=version("async-payment-processing"),
        dependencies=[Depends(require_api_key)],
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings if settings is not None else Settings()
    app.state.webhook_policy = WebhookPolicy(app.state.settings.webhook_allowed_origins)
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(SQLAlchemyError, storage_error)
    app.add_api_route("/health", get_health, methods=["GET"], tags=["health"])
    app.add_api_route(
        "/api/v1/payments",
        create_payment,
        methods=["POST"],
        status_code=status.HTTP_202_ACCEPTED,
        tags=["payments"],
    )
    app.add_api_route(
        "/api/v1/payments/{payment_id}", get_payment, methods=["GET"], tags=["payments"]
    )
    return app
