"""Admin billing API: cost attribution over the existing metering tables.

Two cost families, one price list:

- **Token billing** — ``model_usages`` (the daily rollup) grouped by model /
  user / API key, priced at the per-million token rates.
- **GPU-time billing** — ``metered_usage`` ``instance.uptime`` rows priced
  per GPU-hour with the SKU snapshot as the rate lookup key; fractional
  cards fall out of the ``sku_count`` factor naturally.

Quantities stay the metering tables' business (they deliberately store none
of this); this layer only multiplies rates in at read time, so a price edit
repricing history is a feature, not a migration. All routes are admin-only
(mounted via admin_routers); org-level attribution rides the existing
``consumer_principal_id`` columns, exposed here as group-by dimensions.
"""

import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field
from typing import Annotated
from sqlalchemy import func
from sqlmodel import col, select

from gpustack.server import system_settings
from gpustack.server.deps import SessionDep
from gpustack.schemas.metered_usage import MeteredUsage
from gpustack.schemas.model_usage import ModelUsage
from gpustack.utils.pricing import (
    gpu_time_cost,
    resolve_gpu_hour_price,
    resolve_token_price,
    token_cost,
)

logger = logging.getLogger(__name__)

router = APIRouter()

PRICE_LIST_SETTING_KEY = "price_list"

# The meter key the GPU-hour pricing applies to; other meter keys (future
# storage meters, etc.) are not priced here.
INSTANCE_UPTIME_METER = "instance.uptime"


class TokenConversion(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    unit_tokens: int = Field(default=1_000_000, gt=0)
    amount: float = Field(default=0, ge=0)


class TokenPrice(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    input_per_million: float = Field(ge=0)
    output_per_million: float = Field(ge=0)


class PriceList(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    token_to_money: TokenConversion = Field(default_factory=TokenConversion)
    token_prices: dict[str, TokenPrice] = Field(default_factory=dict)
    gpu_hour_prices: dict[str, Annotated[float, Field(ge=0)]] = Field(
        default_factory=dict
    )


async def _load_price_list() -> PriceList:
    stored = await system_settings.get_setting(PRICE_LIST_SETTING_KEY) or {}
    return PriceList(**{k: v for k, v in stored.items() if k in PriceList.model_fields})


@router.get("/price-list", response_model=PriceList)
async def get_price_list():
    return await _load_price_list()


@router.put("/price-list", response_model=PriceList)
async def update_price_list(price_list: PriceList):
    await system_settings.set_setting(PRICE_LIST_SETTING_KEY, price_list.model_dump())
    return price_list


def _date_range(start: Optional[date], end: Optional[date]) -> tuple:
    today = date.today()
    end = end or today
    start = start or end - timedelta(days=30)
    if start > end:
        start, end = end, start
    return start, end


def _bucket_window(start: date, end: date) -> tuple:
    """The metered-usage hour-bucket window covering a date range, as naive
    UTC datetimes: [start 00:00, day-after-end 00:00).

    ``bucket_start`` is a UTCDateTime column, whose bind hook needs a real
    datetime rather than a bare date for correct UTC column binding.
    """
    from datetime import datetime, time

    return (
        datetime.combine(start, time.min),
        datetime.combine(end + timedelta(days=1), time.min),
    )


@router.get("/summary")
async def billing_summary(
    session: SessionDep,
    start: Annotated[Optional[date], Query()] = None,
    end: Annotated[Optional[date], Query()] = None,
):
    """Totals for the window: tokens, requests, GPU-hours, and their costs."""
    price_list = await _load_price_list()
    start, end = _date_range(start, end)

    token_rows = (
        await session.exec(
            select(
                ModelUsage.model_name,
                func.sum(col(ModelUsage.prompt_token_count)),
                func.sum(col(ModelUsage.completion_token_count)),
                func.sum(col(ModelUsage.request_count)),
            )
            .where(
                col(ModelUsage.date) >= start,
                col(ModelUsage.date) <= end,
            )
            .group_by(col(ModelUsage.model_name))
        )
    ).all()

    token_cost_total = 0.0
    prompt_cost = 0.0
    completion_cost = 0.0
    prompt_tokens = 0
    completion_tokens = 0
    request_count = 0
    per_model = []
    for model_name, prompt, completion, requests in token_rows:
        price = resolve_token_price(price_list.model_dump(), model_name)
        cost = token_cost(price, prompt or 0, completion or 0)
        token_cost_total += cost
        prompt_cost += token_cost(price, prompt or 0, 0)
        completion_cost += token_cost(price, 0, completion or 0)
        prompt_tokens += prompt or 0
        completion_tokens += completion or 0
        request_count += requests or 0
        per_model.append(
            {"model_name": model_name, "cost": cost, "requests": requests or 0}
        )

    gpu_hours = 0.0
    gpu_cost_total = 0.0
    start_dt, end_dt = _bucket_window(start, end)
    gpu_rows = (
        await session.exec(
            select(
                col(MeteredUsage.sku),
                col(MeteredUsage.quantity),
                col(MeteredUsage.sku_count),
            ).where(
                col(MeteredUsage.meter_key) == INSTANCE_UPTIME_METER,
                col(MeteredUsage.bucket_start) >= start_dt,
                col(MeteredUsage.bucket_start) < end_dt,
            )
        )
    ).all()
    for sku, quantity, sku_count in gpu_rows:
        rate = resolve_gpu_hour_price(price_list.model_dump(), sku)
        # sku_count is a Decimal from the numeric column — coerce before
        # arithmetic (Decimal/float raises).
        sku_count = float(sku_count or 0)
        gpu_cost_total += gpu_time_cost(rate, quantity or 0, sku_count)
        gpu_hours += (quantity or 0) * sku_count / 3600.0

    per_model.sort(key=lambda row: row["cost"], reverse=True)
    return {
        "currency": price_list.currency,
        "start": start,
        "end": end,
        "token_cost": token_cost_total,
        "prompt_cost": prompt_cost,
        "completion_cost": completion_cost,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "request_count": request_count,
        "gpu_hours": gpu_hours,
        "gpu_cost": gpu_cost_total,
        "total_cost": token_cost_total + gpu_cost_total,
        "top_models": per_model[:10],
    }


@router.get("/token-breakdown")
async def token_billing_breakdown(
    session: SessionDep,
    group_by: Annotated[
        str, Query(pattern="^(model|user|api_key|organization)$")
    ] = "model",
    start: Annotated[Optional[date], Query()] = None,
    end: Annotated[Optional[date], Query()] = None,
):
    """Cost attribution over the token window, grouped by the chosen
    dimension. Prices resolve per group member (model name for model rows;
    the model each user/key consumed for user/key rows)."""
    price_list = await _load_price_list()
    start, end = _date_range(start, end)

    dimensions = {
        "model": (ModelUsage.model_id, ModelUsage.model_name),
        "user": (ModelUsage.user_id, ModelUsage.user_name),
        "api_key": (ModelUsage.api_key_id, ModelUsage.api_key_name),
        "organization": (ModelUsage.consumer_principal_id, ModelUsage.consumer_name),
    }
    identity, dimension = (col(c) for c in dimensions[group_by])

    rows = (
        await session.exec(
            select(
                identity,
                dimension,
                col(ModelUsage.model_name),
                func.sum(col(ModelUsage.prompt_token_count)),
                func.sum(col(ModelUsage.completion_token_count)),
                func.sum(col(ModelUsage.request_count)),
            )
            .where(
                col(ModelUsage.date) >= start,
                col(ModelUsage.date) <= end,
            )
            .group_by(identity, dimension, col(ModelUsage.model_name))
        )
    ).all()

    aggregated: dict = {}
    for entity_id, name, model_name, prompt, completion, requests in rows:
        key = (
            str(entity_id)
            if entity_id is not None
            else (name if group_by == "model" and name else "unattributed")
        )
        label = name or (f"#{entity_id}" if entity_id is not None else "(unattributed)")
        entry = aggregated.setdefault(
            key,
            {
                "label": label,
                "prompt": 0,
                "completion": 0,
                "requests": 0,
                "cost": 0.0,
                "prompt_cost": 0.0,
                "completion_cost": 0.0,
            },
        )
        price = resolve_token_price(price_list.model_dump(), model_name)
        entry["cost"] += token_cost(price, prompt or 0, completion or 0)
        entry["prompt_cost"] += token_cost(price, prompt or 0, 0)
        entry["completion_cost"] += token_cost(price, 0, completion or 0)
        entry["prompt"] += prompt or 0
        entry["completion"] += completion or 0
        entry["requests"] += requests or 0

    items = [
        {"key": key, "total_tokens": values["prompt"] + values["completion"], **values}
        for key, values in sorted(aggregated.items(), key=lambda kv: -kv[1]["cost"])
    ]
    return {
        "currency": price_list.currency,
        "group_by": group_by,
        "start": start,
        "end": end,
        "items": items,
    }
