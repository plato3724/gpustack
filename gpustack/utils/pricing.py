"""Pricing resolution and cost computation for the billing layer.

The price list lives in ``system_settings`` under ``price_list`` and is a
plain JSON document the operator edits through the admin API:

    {
      "currency": "USD",
      "token_prices": {
        "default": {"input_per_million": 0.0, "output_per_million": 0.0},
        "qwen3-32b": {"input_per_million": 0.5, "output_per_million": 1.5}
      },
      "gpu_hour_prices": {"default": 0.0, "nvidia-h100-80gb-hbm3": 4.5}
    }

Resolution is exact-match-then-default: a model name (or a metered-usage
``sku``) with its own entry is priced by it; anything else falls back to
``default``. Costs are floats of the configured currency — this layer
computes display/billing numbers, not ledgers, so no decimal machinery
beyond rounding at the presentation edge.

Pure functions throughout: the routes hand in the price list they loaded,
so every computation is testable without a database.
"""

from typing import Dict, Optional

DEFAULT_TOKEN_PRICE = {"input_per_million": 0.0, "output_per_million": 0.0}


def resolve_token_price(
    price_list: Optional[dict], model_name: Optional[str]
) -> Dict[str, float]:
    """Token unit price for a model: exact model name, else ``default``."""
    prices = (price_list or {}).get("token_prices") or {}
    entry = prices.get(model_name) if model_name else None
    if entry is None:
        entry = prices.get("default")
    if entry is None:
        conversion = (price_list or {}).get("token_to_money") or {}
        rate = (
            conversion.get("amount", 0)
            * 1_000_000
            / conversion.get("unit_tokens", 1_000_000)
        )
        entry = {"input_per_million": rate, "output_per_million": rate}
    return {
        "input_per_million": float(entry.get("input_per_million") or 0.0),
        "output_per_million": float(entry.get("output_per_million") or 0.0),
    }


def token_cost(
    price: Dict[str, float], prompt_tokens: int, completion_tokens: int
) -> float:
    """Cost of one usage aggregate at ``price`` (per-million rates)."""
    return (
        prompt_tokens / 1_000_000 * price["input_per_million"]
        + completion_tokens / 1_000_000 * price["output_per_million"]
    )


def resolve_gpu_hour_price(price_list: Optional[dict], sku: Optional[str]) -> float:
    """Per-GPU-hour price for a metered-usage SKU: exact sku, else default."""
    prices = (price_list or {}).get("gpu_hour_prices") or {}
    if sku is not None and sku in prices:
        return float(prices[sku] or 0.0)
    return float(prices.get("default") or 0.0)


def gpu_time_cost(price_per_hour: float, quantity: int, sku_count: float) -> float:
    """Cost of one ``metered_usage`` instance-uptime row.

    ``quantity`` is metered wall-clock seconds and ``sku_count`` the card
    count (fractional for a sliced card), so the row's GPU-hours are
    ``quantity * sku_count / 3600`` — the same conversion the metering read
    path documents; pricing multiplies the rate in, nothing else.

    Both quantities are coerced to float because ``sku_count`` comes back
    from the DB as a ``Decimal`` (the metering schema's numeric column), and
    Decimal/float mixed arithmetic raises.
    """
    return float(quantity) * float(sku_count) / 3600.0 * float(price_per_hour)
