"""Billing API contracts with isolated session and settings boundaries."""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from gpustack.api.auth import get_admin_user, get_current_user
from gpustack.api.exceptions import register_handlers
from gpustack.routes import billing, quota_rules
from gpustack.server.db import get_session


@pytest.fixture
def api(monkeypatch):
    app = FastAPI()
    register_handlers(app)
    app.include_router(
        billing.router, prefix="/v2/billing", dependencies=[Depends(get_admin_user)]
    )
    app.include_router(
        quota_rules.router,
        prefix="/v2/quota-rules",
        dependencies=[Depends(get_admin_user)],
    )
    user = SimpleNamespace(is_admin=True)
    session = MagicMock()
    session.exec = AsyncMock(return_value=MagicMock())
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_session] = lambda: session
    settings = {}

    async def get_setting(key):
        return settings.get(key)

    async def set_setting(key, value):
        settings[key] = value

    monkeypatch.setattr(billing.system_settings, "get_setting", get_setting)
    monkeypatch.setattr(billing.system_settings, "set_setting", set_setting)
    return app, user, session, settings


@pytest.mark.asyncio
async def test_price_roundtrip_and_validation(api):
    app, _, _, _ = api
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        initial = (await client.get("/v2/billing/price-list")).json()
        assert initial["token_to_money"] == {"unit_tokens": 1_000_000, "amount": 0}
        initial.update(
            currency="CNY", token_to_money={"unit_tokens": 1000, "amount": 2}
        )
        assert (
            await client.put("/v2/billing/price-list", json=initial)
        ).status_code == 200
        assert (await client.get("/v2/billing/price-list")).json() == initial
        assert (
            await client.put("/v2/billing/price-list", json={"currency": "bad!"})
        ).status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/v2/billing/summary",
        "/v2/billing/price-list",
        "/v2/quota-rules",
        "/v2/quota-rules/alerts",
    ],
)
async def test_admin_only(api, path):
    app, user, _, _ = api
    user.is_admin = False
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get(path)).status_code == 403


@pytest.mark.asyncio
async def test_summary_prices_models_and_fractional_gpu_time(api):
    app, _, session, settings = api
    settings["price_list"] = {
        "currency": "CNY",
        "token_to_money": {"unit_tokens": 1000, "amount": 2},
        "gpu_hour_prices": {"default": 4},
    }
    session.exec.side_effect = [
        MagicMock(all=lambda: [("model-a", 1000, 500, 2)]),
        MagicMock(all=lambda: [("gpu", 3600, Decimal("0.25"))]),
    ]
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            "/v2/billing/summary?start=2026-11-01&end=2026-11-02"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_tokens"] == 1500 and data["request_count"] == 2
        assert data["prompt_cost"] == 2 and data["completion_cost"] == 1
        assert data["gpu_hours"] == 0.25 and data["total_cost"] == 4
        start, end = billing._bucket_window(date(2026, 11, 1), date(2026, 11, 2))
        assert start.isoformat() == "2026-11-01T00:00:00"
        assert end.isoformat() == "2026-11-03T00:00:00"


@pytest.mark.asyncio
@pytest.mark.parametrize("dimension", ["organization", "user", "api_key", "model"])
async def test_breakdown_keeps_identity_and_prices_models_before_aggregation(
    api, dimension
):
    app, _, session, settings = api
    settings["price_list"] = {
        "token_to_money": {"unit_tokens": 1000, "amount": 1},
        "token_prices": {
            "expensive": {"input_per_million": 2000, "output_per_million": 3000}
        },
    }
    session.exec.return_value.all.return_value = [
        (7, "same-name", "expensive", 1000, 1000, 1),
        (7, "same-name", "other", 1000, 0, 1),
        (8, "same-name", "other", 1000, 0, 1),
    ]
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        data = (
            await client.get(f"/v2/billing/token-breakdown?group_by={dimension}")
        ).json()
        assert len(data["items"]) == 2
        first = data["items"][0]
        assert first["key"] == "7" and first["cost"] == 6
        assert first["prompt_cost"] == 3 and first["completion_cost"] == 3
        assert first["total_tokens"] == 3000 and first["requests"] == 2


@pytest.mark.asyncio
async def test_quota_payload_validation(api):
    app, _, _, _ = api
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        base = {
            "subject_type": "global",
            "kind": "token_budget",
            "window_seconds": 60,
            "limit_value": 100,
        }
        for change in [
            {"window_seconds": 0},
            {"limit_value": -1},
            {"subject_id": 7},
            {"subject_type": "organization"},
        ]:
            assert (
                await client.post("/v2/quota-rules", json={**base, **change})
            ).status_code == 422
