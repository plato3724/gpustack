"""Unit tests for the quota engine: sliding window semantics, rule matching,
request gating, token accrual, budget threshold alerts, and the guard
dependency's subject resolution.

The DB is stubbed; the engine's rule source and the event write path are
monkeypatched, so these exercise the decision logic and its wiring.
"""

import time
from types import SimpleNamespace

import pytest

from gpustack.api import quotas as quotas_api
from gpustack.server import quota_engine
from gpustack.schemas.quota_rules import QuotaRuleKind, QuotaSubjectType
from gpustack.utils.rate_limit import SlidingWindowCounter

# ---------------------------------------------------------------------------
# SlidingWindowCounter — window semantics
# ---------------------------------------------------------------------------


def test_counter_allows_within_limit():
    counter = SlidingWindowCounter()
    for _ in range(3):
        decision = counter.take("k", 1, window_seconds=60, limit=3)
        assert decision.allowed
    assert decision.total == 3


def test_counter_denies_over_limit_with_retry_after():
    counter = SlidingWindowCounter()
    for _ in range(2):
        counter.take("k", 1, window_seconds=60, limit=2)
    decision = counter.take("k", 1, window_seconds=60, limit=2)
    assert not decision.allowed
    assert decision.retry_after >= 1
    assert decision.total == 2  # the denied take didn't count


def test_counter_window_slides():
    counter = SlidingWindowCounter()
    for _ in range(3):
        counter.take("k", 1, window_seconds=1, limit=3)
    # Age out the window, then take again.
    time.sleep(1.05)
    decision = counter.take("k", 1, window_seconds=1, limit=3)
    assert decision.allowed
    assert decision.total == 1


def test_counter_keys_are_independent():
    counter = SlidingWindowCounter()
    counter.take("a", 10, window_seconds=60, limit=10)
    decision = counter.take("b", 1, window_seconds=60, limit=10)
    assert decision.allowed


# ---------------------------------------------------------------------------
# Engine — rule matching and gating
# ---------------------------------------------------------------------------


def _rule(
    id,
    kind,
    subject_type=QuotaSubjectType.GLOBAL,
    subject_id=None,
    window_seconds=60,
    limit_value=10,
):
    return quota_engine.RuleSnapshot(
        id=id,
        subject_type=subject_type,
        subject_id=subject_id,
        kind=kind,
        window_seconds=window_seconds,
        limit_value=limit_value,
    )


def _stub_rules(monkeypatch, rules):
    async def fake_load():
        return rules

    monkeypatch.setattr(quota_engine, "load_rules", fake_load)


@pytest.mark.asyncio
async def test_check_request_allows_without_rules(monkeypatch):
    _stub_rules(monkeypatch, [])
    assert await quota_engine.check_request(user_id=7) is None


@pytest.mark.asyncio
async def test_check_request_global_rule_gates_everyone(monkeypatch):
    _stub_rules(monkeypatch, [_rule(1, QuotaRuleKind.REQUEST_RATE, limit_value=1)])
    assert await quota_engine.check_request(user_id=7) is None
    denial = await quota_engine.check_request(user_id=8)
    assert denial is not None and not denial.allowed
    # Distinct users share the global window: user 7's take is in it, user
    # 8's take was denied and not counted.
    assert denial.total == 1


@pytest.mark.asyncio
async def test_check_request_subject_rules_are_scoped(monkeypatch):
    _stub_rules(
        monkeypatch,
        [
            _rule(
                2,
                QuotaRuleKind.REQUEST_RATE,
                subject_type=QuotaSubjectType.USER,
                subject_id=7,
                limit_value=1,
            )
        ],
    )
    assert await quota_engine.check_request(user_id=7) is None
    assert await quota_engine.check_request(user_id=7) is not None
    # A different user is untouched by user 7's rule.
    assert await quota_engine.check_request(user_id=8) is None


@pytest.mark.asyncio
async def test_check_request_ignores_other_kinds(monkeypatch):
    _stub_rules(
        monkeypatch,
        [
            _rule(3, QuotaRuleKind.TOKEN_RATE, limit_value=1),
            _rule(4, QuotaRuleKind.TOKEN_BUDGET, limit_value=1),
        ],
    )
    assert await quota_engine.check_request(user_id=7) is None


@pytest.mark.asyncio
async def test_accrue_tokens_blocks_token_rate(monkeypatch):
    _stub_rules(
        monkeypatch,
        [
            _rule(
                5,
                QuotaRuleKind.TOKEN_RATE,
                subject_type=QuotaSubjectType.USER,
                subject_id=7,
                window_seconds=60,
                limit_value=100,
            )
        ],
    )

    async def accrue():
        await quota_engine.accrue_usage(total_tokens=60, user_id=7)

    await accrue()
    await accrue()
    # 120 tokens accrued against a 100-token window — the *next* request from
    # this user is the one that gets cut off (tokens are known post-response).
    denial = await quota_engine.check_request(user_id=7)
    assert denial is not None and denial.total == 120


@pytest.mark.asyncio
async def test_budget_alerts_fire_per_threshold(monkeypatch):
    events = []

    class _FakeSessionCtx:
        async def __aenter__(self):
            return SimpleNamespace()

        async def __aexit__(self, *exc):
            return False

    class FakeResourceEvent:
        @classmethod
        async def create(cls, session, source):
            events.append(source)

    monkeypatch.setattr(quota_engine, "ResourceEvent", FakeResourceEvent)
    # quota_engine binds async_session at import time — patch its reference,
    # not gpustack.server.db's.
    monkeypatch.setattr(quota_engine, "async_session", lambda: _FakeSessionCtx())
    quota_engine._alert_dedupe.clear()
    _stub_rules(
        monkeypatch,
        [
            _rule(
                6,
                QuotaRuleKind.TOKEN_BUDGET,
                subject_type=QuotaSubjectType.API_KEY,
                subject_id=11,
                window_seconds=3600,
                limit_value=100,
            )
        ],
    )

    # 85 tokens → crosses 80 only.
    await quota_engine.accrue_usage(total_tokens=85, api_key_id=11)
    assert len(events) == 1 and events[0]["spec_snapshot"]["threshold_percent"] == 80

    # 96 → crosses 95 (100 not yet).
    await quota_engine.accrue_usage(total_tokens=11, api_key_id=11)
    assert [e["spec_snapshot"]["threshold_percent"] for e in events] == [80, 95]

    # Crossing 100% emits its own event, retaining the actual overage.
    await quota_engine.accrue_usage(total_tokens=5, api_key_id=11)
    assert [e["spec_snapshot"]["threshold_percent"] for e in events] == [80, 95, 100]
    await quota_engine.accrue_usage(total_tokens=5, api_key_id=11)
    assert len(events) == 3


# ---------------------------------------------------------------------------
# Guard dependency — subject resolution
# ---------------------------------------------------------------------------


def test_subject_from_request_ignores_spoofed_org_header():
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id=7),
            api_key=SimpleNamespace(id=42, owner_principal_id=3),
        ),
        headers={"X-Organization-Id": "999"},
    )
    subject = quotas_api.subject_from_request(request)
    assert subject == {
        "user_id": 7,
        "api_key_id": 42,
        "organization_id": 3,
        "model_id": None,
    }


def test_subject_from_request_tolerates_missing_state():
    request = SimpleNamespace(state=SimpleNamespace(), headers={})
    subject = quotas_api.subject_from_request(request)
    assert subject["user_id"] is None
    assert subject["api_key_id"] is None
    assert subject["organization_id"] is None


@pytest.fixture(autouse=True)
def isolated_counters(monkeypatch):
    monkeypatch.setattr(quota_engine, "GLOBAL_COUNTER", SlidingWindowCounter())
    quota_engine._alert_dedupe.clear()


@pytest.mark.asyncio
async def test_request_accrual_does_not_double_charge(monkeypatch):
    _stub_rules(monkeypatch, [_rule(20, QuotaRuleKind.REQUEST_RATE, limit_value=2)])
    assert await quota_engine.check_request(user_id=7) is None
    await quota_engine.accrue_usage(total_tokens=1, request_count=1, user_id=7)
    assert await quota_engine.check_request(user_id=7) is None
    assert await quota_engine.check_request(user_id=7) is not None


@pytest.mark.asyncio
async def test_denied_overlap_does_not_consume_other_rule(monkeypatch):
    rules = [
        _rule(21, QuotaRuleKind.REQUEST_RATE, limit_value=2),
        _rule(22, QuotaRuleKind.TOKEN_BUDGET, limit_value=1),
    ]
    _stub_rules(monkeypatch, rules)
    quota_engine.GLOBAL_COUNTER.add("quota:22:global", 1, 60)
    assert await quota_engine.check_request() is not None
    assert quota_engine.GLOBAL_COUNTER.total("quota:21:global", 60) == 0


@pytest.mark.asyncio
async def test_model_and_organization_rules_gate_resolved_subjects(monkeypatch):
    _stub_rules(
        monkeypatch,
        [
            _rule(
                23,
                QuotaRuleKind.REQUEST_RATE,
                subject_type=QuotaSubjectType.MODEL,
                subject_id=9,
                limit_value=1,
            ),
            _rule(
                24,
                QuotaRuleKind.REQUEST_RATE,
                subject_type=QuotaSubjectType.ORGANIZATION,
                subject_id=3,
                limit_value=2,
            ),
        ],
    )
    assert await quota_engine.check_request(model_id={9, 10}, organization_id=3) is None
    assert await quota_engine.check_request(model_id={9}, organization_id=3) is not None
    assert await quota_engine.check_request(model_id={11}, organization_id=3) is None
    assert await quota_engine.check_request(organization_id=3) is not None


def test_counter_records_overage_and_recovers(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("gpustack.utils.rate_limit.time.monotonic", lambda: now[0])
    counter = SlidingWindowCounter()
    assert counter.add("budget", 101, 10) == 101
    assert not counter.take("budget", 0, 10, 100).allowed
    now[0] = 11
    assert counter.take("budget", 0, 10, 100).allowed


@pytest.mark.asyncio
async def test_guard_returns_retry_after_when_budget_is_exhausted(monkeypatch):
    _stub_rules(monkeypatch, [_rule(25, QuotaRuleKind.TOKEN_RATE, limit_value=100)])
    quota_engine.GLOBAL_COUNTER.add("quota:25:global", 100, 60)
    request = SimpleNamespace(state=SimpleNamespace(), headers={})
    response = await quotas_api.quota_guard(request)
    assert response.status_code == 429
    assert int(response.headers["Retry-After"]) >= 1


@pytest.mark.asyncio
async def test_gateway_checks_quota_even_with_cached_marker(monkeypatch):
    from gpustack.routes import token
    from gpustack.schemas.model_routes import AccessPolicyEnum
    from gpustack.security import AUTH_CACHE_HEADER

    _stub_rules(monkeypatch, [_rule(26, QuotaRuleKind.TOKEN_RATE, limit_value=1)])
    quota_engine.GLOBAL_COUNTER.add("quota:26:global", 1, 60)

    class Routes:
        def __init__(self, session):
            pass

        async def get_model_auth_info_by_name(self, name):
            return AccessPolicyEnum.PUBLIC, None

        async def resolve_route_targets(self, name):
            return [SimpleNamespace(model_id=9)]

    async def caller(*args):
        return None, None, "none", False

    monkeypatch.setattr(token, "load_rules", quota_engine.load_rules)
    monkeypatch.setattr(token, "ModelRouteService", Routes)
    monkeypatch.setattr(token, "_resolve_caller", caller)
    monkeypatch.setattr(token, "gateway_token_auth", lambda request: None)
    request = SimpleNamespace(
        headers={AUTH_CACHE_HEADER: "cached", "x-higress-llm-model": "model"},
        state=SimpleNamespace(),
        app=SimpleNamespace(state=SimpleNamespace(jwt_manager=object())),
    )
    response = await token.server_auth(request, SimpleNamespace())
    assert response.status_code == 429
