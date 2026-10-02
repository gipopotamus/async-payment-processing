"""Verify the demo recipient prevents conflicting or repeated external effects."""

from uuid import uuid7

import httpx

from examples.receiver import create_receiver


async def test_receiver_deduplicates_and_rejects_changed_result() -> None:
    """Keep one accepted result across retries and reject reuse with a different body."""
    app = create_receiver(failures=1)
    notification = {
        "event_id": str(uuid7()),
        "payment_id": str(uuid7()),
        "status": "succeeded",
        "amount": "125.50",
        "currency": "RUB",
        "created_at": "2026-10-02T00:00:00Z",
        "processed_at": "2026-10-02T00:00:03Z",
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://receiver"
    ) as client:
        assert (await client.post("/callback", json=notification)).status_code == 503
        assert (await client.post("/callback", json=notification)).status_code == 204
        assert (await client.post("/callback", json=notification)).status_code == 204
        changed = notification | {"status": "failed"}
        assert (await client.post("/callback", json=changed)).status_code == 409
        receipts = (await client.get("/receipts")).json()
        assert len(receipts) == 1
        assert receipts[0]["accepted"]
        assert receipts[0]["attempts"] == 3
        assert receipts[0]["notification"]["status"] == "succeeded"
