# Local acceptance evidence

Checked on Windows on 2026-10-02, without Docker. The application used Python
3.14.7, PostgreSQL 18.6, RabbitMQ 4.3.6, and Erlang/OTP 28.5.0.7. PostgreSQL and
RabbitMQ ran locally with isolated development data; no Windows service was
installed. All credentials used in examples are development placeholders.

## Automated live-broker checks

`tests/test_live_broker.py` uses actual RabbitMQ and PostgreSQL, with a unique
virtual host and migrated database schema per test. It verifies:

- Persistent confirmed publications reach both durable queues. An unroutable
  mandatory publication raises instead of being marked delivered.
- A database failure after a real broker confirmation rolls back the outbox update.
  Retrying produces two messages with the same event ID; only the committed
  publication counts in PostgreSQL. This demonstrates the at-least-once boundary.
- The API, relay, consumer, actual gateway delay, and loopback HTTP receiver complete
  delivery after two HTTP failures. Replayed processing does not repeat the gateway.
  Malformed JSON reaches a real DLQ without retaining the original secret-bearing body.

Run these checks using the explicit test settings in the README. The broker user
needs management permission to create/delete temporary virtual hosts and grant
itself permissions. Ordinary application credentials need only their configured
virtual host; management access is a test-runner responsibility.

## Additional process-level recovery checks

A local acceptance run started the API, relay, consumer, and demo receiver as four
separate OS processes, using a disposable database and virtual host. It passed:

1. Stop the RabbitMQ application, then POST a payment: API returns `202`, payment
   remains `pending`, and the unpublished outbox event retains a scheduled retry.
   Start RabbitMQ again: the original payment completes without another POST.
2. Restart the consumer after its first failed webhook: persisted attempts and due
   dates recover; the gateway is counted once and webhook succeeds on attempt three.
3. Publish a confirmed persistent message to `payments.dlq`, then shut down the
   entire Erlang/RabbitMQ process and restart against the same data directory:
   the virtual host, queue, and message survive and can be consumed.
4. Use the demo receiver with `--failures 3`: three actual HTTP errors set webhook
   status to `failed` and produce a message in the real DLQ. The observed payment
   result remained `succeeded`; delivery failure did not alter it.

These disruptive restart checks were performed against an isolated local broker,
not against a shared broker or by CI. The temporary database and virtual host were
removed and the four application child processes stopped after the run. To repeat,
use a dedicated broker and the README's four process commands; use its own
`rabbitmqctl stop_app`, `start_app`, and `shutdown` commands for the corresponding
failure windows. Preserve its data directory for the full process restart.

## Verification boundaries

Ruff, formatting, strict mypy (32 source files), all 98 tests with explicit live
service settings (none skipped), and Alembic schema-drift checks pass locally.
The lockfile consistency check and actionlint workflow validation also pass.
GitHub Actions also passed these checks with PostgreSQL and RabbitMQ service
containers: [verified run](https://github.com/gipopotamus/async-payment-processing/actions/runs/37028803693).

Docker Compose configuration is provided, but image build and Compose startup have
not been tested because Docker is unavailable locally. CI service containers do
not establish that the application's Dockerfile or Compose deployment works.
The demo recipient keeps deduplication state in memory; production recipients need
durable state and must atomically combine deduplication with their business effects.
External effects are at-least-once, and DNS/egress restrictions remain a deployment
responsibility, as described in the architecture contract.
