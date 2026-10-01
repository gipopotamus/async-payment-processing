"""HTTP composition root and API-wide authentication dependencies."""

from hmac import compare_digest
from importlib.metadata import version
from typing import Annotated, Literal, cast

from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from payments.settings import Settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class HealthResponse(BaseModel):
    """Report process liveness without claiming dependency readiness."""

    status: Literal["ok"] = "ok"


def get_settings(request: Request) -> Settings:
    """Resolve configuration from the current application instance."""
    return cast(Settings, request.app.state.settings)


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


def create_app(settings: Settings | None = None) -> FastAPI:
    """Compose an API with explicit configuration injection.

    Args:
        settings: Validated configuration. Load from the environment when omitted.

    Returns:
        An application whose routes require the configured API key.
    """
    app = FastAPI(
        title="Async Payment Processing",
        version=version("async-payment-processing"),
        dependencies=[Depends(require_api_key)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings if settings is not None else Settings()
    app.add_api_route("/health", get_health, methods=["GET"], tags=["health"])
    return app
