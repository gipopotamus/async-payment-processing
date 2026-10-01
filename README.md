# Async Payment Processing

A Python assignment: accept payments, process them asynchronously through an
emulated gateway, and deliver their results via webhook.

## Current status

Milestone 1 provides a runnable FastAPI foundation, explicit configuration
injection, API-key authentication, quality checks, and the implementation contract.
Payment endpoints, persistence, broker integration, and Docker deployment are
planned milestones; they are not implemented yet.

## Development

Use Python 3.14 and [uv](https://docs.astral.sh/uv/). Exact dependency versions and
artifact hashes are committed in `uv.lock`.

```powershell
uv sync --locked
Copy-Item .env.example .env
uv run --locked uvicorn payments.api:create_app --factory --host 127.0.0.1 --port 8000
```

Replace the example credential in `.env` before exposing the API. Configuration
has no default API key; startup fails when the key is missing or too short.

In another terminal, using the development example credential:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health -Headers @{
    'X-API-Key' = 'local-development-key-change-me'
}
```

The response is `{"status":"ok"}`. This checks process liveness only, not database
or broker readiness. Missing or invalid credentials return `401`. Automatic docs
and OpenAPI routes are disabled so they cannot bypass API authentication.

## Quality checks

```powershell
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest
```

Public classes and functions have Google-style docstrings, checked by Ruff.
Business behavior must have deterministic tests; integration tests will use real
PostgreSQL and RabbitMQ once those components are introduced.

## Commit sequence

Each milestone is a separate local commit. Publication and the next milestone
require the user's approval after that commit.

1. Foundation: package, configuration DI, authentication, checks, architecture.
2. Persistence and environment: SQLAlchemy models, Alembic, Docker Compose.
3. Payment API: validation, transactional creation, concurrent idempotency, GET.
4. Outbox worker: confirmed publication, durable scheduling, recovery.
5. Consumer: gateway processing, webhook delivery, retries, DLQ.
6. Acceptance: failure-window integration tests, runnable examples, final README.

## Approved architecture

The final Compose environment has `postgres`, `rabbitmq`, `api`, `outbox-worker`,
and `consumer`, plus a one-shot migration service. The three application processes
share one image. There is one payment consumer; the outbox relay is a separate
worker. See [the implementation contract](docs/architecture.md) for transaction
boundaries, retry semantics, and dependency injection rules.
