import pytest

"""Unit tests for the pricing layer: resolution order, token cost, GPU-time
cost with fractional cards, and the price-list shape the API round-trips."""

from gpustack.routes.billing import PriceList
from gpustack.utils.pricing import (
    gpu_time_cost,
    resolve_gpu_hour_price,
    resolve_token_price,
    token_cost,
)


def test_token_price_resolution_exact_then_default():
    price_list = {
        "token_prices": {
            "default": {"input_per_million": 0.1, "output_per_million": 0.2},
            "qwen3-32b": {"input_per_million": 0.5, "output_per_million": 1.5},
        }
    }
    exact = resolve_token_price(price_list, "qwen3-32b")
    assert exact == {"input_per_million": 0.5, "output_per_million": 1.5}
    fallback = resolve_token_price(price_list, "unknown-model")
    assert fallback == {"input_per_million": 0.1, "output_per_million": 0.2}
    # No price list at all → free (never crashes billing).
    assert resolve_token_price(None, "qwen3-32b") == {
        "input_per_million": 0.0,
        "output_per_million": 0.0,
    }


def test_token_cost_millions_math():
    price = {"input_per_million": 1.0, "output_per_million": 3.0}
    # 2M input tokens = 2.0; 0.5M output = 1.5 → 3.5.
    assert token_cost(price, 2_000_000, 500_000) == pytest.approx(3.5)


def test_gpu_hour_price_resolution_uses_sku_then_default():
    price_list = {"gpu_hour_prices": {"default": 1.0, "sku-a": 4.5}}
    assert resolve_gpu_hour_price(price_list, "sku-a") == 4.5
    assert resolve_gpu_hour_price(price_list, "sku-unknown") == 1.0
    assert resolve_gpu_hour_price(None, "sku-a") == 0.0


def test_gpu_time_cost_handles_fractional_cards():
    # One hour on a quarter of a card at 4.0/hr = 1.0.
    assert gpu_time_cost(4.0, quantity=3600, sku_count=0.25) == pytest.approx(1.0)
    # Five hours across 2 cards at 3.0/hr = 30.0.
    assert gpu_time_cost(3.0, quantity=5 * 3600, sku_count=2) == pytest.approx(30.0)
    # Zero-quantity rows price to zero, not NaN.
    assert gpu_time_cost(4.0, quantity=0, sku_count=0.25) == 0.0


def test_price_list_model_defaults_and_roundtrip():
    price_list = PriceList()
    assert price_list.currency == "USD"
    assert price_list.token_prices == {} and price_list.gpu_hour_prices == {}
    payload = PriceList(
        currency="EUR",
        token_prices={"default": {"input_per_million": 0.2, "output_per_million": 0.8}},
        gpu_hour_prices={"default": 2.0},
    ).model_dump()
    assert PriceList(**payload).currency == "EUR"


def test_conversion_is_fallback_to_model_and_default_rates():
    prices = {"token_to_money": {"unit_tokens": 1000, "amount": 2}}
    assert token_cost(resolve_token_price(prices, "m"), 500, 500) == 2
    prices["token_prices"] = {"m": {"input_per_million": 1, "output_per_million": 3}}
    assert token_cost(resolve_token_price(prices, "m"), 1_000_000, 1_000_000) == 4
    prices["token_prices"]["default"] = {
        "input_per_million": 0,
        "output_per_million": 0,
    }
    assert token_cost(resolve_token_price(prices, "other"), 1_000_000, 0) == 0


@pytest.mark.parametrize(
    "payload",
    [
        {"currency": "oops"},
        {"token_to_money": {"unit_tokens": 0}},
        {"token_to_money": {"amount": -1}},
        {"token_to_money": {"amount": float("nan")}},
        {"token_prices": {"m": {"input_per_million": -1, "output_per_million": 0}}},
        {"gpu_hour_prices": {"default": -1}},
        {"gpu_hour_prices": {"x": float("inf")}},
    ],
)
def test_price_list_rejects_invalid_rates(payload):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PriceList(**payload)
