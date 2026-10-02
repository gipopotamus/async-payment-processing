# Асинхронная обработка платежей

Сервис принимает платёж, обрабатывает его через эмулятор шлюза и отправляет результат на webhook.
Стек: FastAPI, PostgreSQL, RabbitMQ/FastStream. API, outbox worker и consumer работают отдельно.

## Локальный запуск

Нужны Python 3.14, uv, PostgreSQL и RabbitMQ.

```powershell
uv sync --locked
Copy-Item .env.example .env
```

В `.env` укажите доступ к БД и брокеру, API-ключ и разрешённый адрес webhook
в `PAYMENTS_WEBHOOK_ALLOWED_ORIGINS`. Пример настроен на PostgreSQL на порту 55432
и RabbitMQ на 5673. Пользователь и база `payments` должны существовать.

```powershell
uv run --locked alembic upgrade head
```

Затем запустите каждую команду в отдельном терминале:

```powershell
uv run --locked uvicorn payments.api.app:create_app --factory --host 127.0.0.1 --port 8000
uv run --locked python -m payments.workers.outbox
uv run --locked python -m payments.workers.consumer
uv run --locked python -m examples.receiver --failures 2 --port 8081
```

Последняя команда запускает тестовый webhook: первые две попытки завершатся ошибкой,
третья — успешно.

## Пример запроса

Ключ ниже соответствует `.env.example`. Если заменили его, подставьте свой.

```powershell
$headers = @{
    'X-API-Key' = 'local-development-key-change-me'
    'Idempotency-Key' = 'order-42'
}
$body = @{
    amount = '125.50'
    currency = 'RUB'
    description = 'Заказ 42'
    metadata = @{ order_id = 42 }
    webhook_url = 'http://127.0.0.1:8081/callback'
} | ConvertTo-Json

$payment = Invoke-RestMethod http://127.0.0.1:8000/api/v1/payments `
    -Method Post -Headers $headers -ContentType 'application/json; charset=utf-8' -Body $body
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/payments/$($payment.payment_id)" -Headers $headers
```

POST возвращает `202` и ID платежа. Сумма передаётся строкой, валюты — `RUB`, `USD`, `EUR`.
Повтор с тем же ключом и телом вернёт тот же платёж; другое тело с этим ключом — `409`.
При ошибках webhook выполняются до трёх попыток с задержками 2 и 4 секунды.
После трёх неудачных попыток событие попадает в DLQ.
Получатель должен отсекать повторы по `event_id` или заголовку `Idempotency-Key`.

## Docker

После настройки `.env`:

```powershell
docker compose up --build -d
```

Webhook должен быть доступен из контейнера consumer: замените локальный URL и разрешённый
адрес в `.env`. Сборка и запуск в Docker пока не проверены.

## Проверки

```powershell
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest
```

Для интеграционных тестов задайте `PAYMENTS_TEST_DATABASE_URL` на отдельную БД с именем,
оканчивающимся на `_test`, и `PAYMENTS_TEST_BROKER_MANAGEMENT_URL` на RabbitMQ Management.
Без этих настроек соответствующие тесты пропускаются.

Подробнее: [архитектура](docs/architecture.md), [проверенные сценарии](docs/acceptance.md).
