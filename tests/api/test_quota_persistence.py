"""Quota usage survives a new engine instance that reloads the same database.

The deque is swapped out between "processes". SQL is a real sqlite file, so
hydrate, restart and the next admission all see committed samples.
"""

from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.api import quotas as quotas_api
from gpustack.routes.quota_rules import _public_rule
from gpustack.schemas.quota_rules import QuotaRule, QuotaRuleKind, QuotaSubjectType
from gpustack.schemas.quota_usage import QuotaUsageSample
from gpustack.server import quota_engine, quota_usage_store
from gpustack.utils.rate_limit import SlidingWindowCounter


class Clock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _rule(
    rule_id,
    kind,
    *,
    subject_type=QuotaSubjectType.GLOBAL,
    subject_id=None,
    window_seconds=60,
    limit_value=100,
):
    return quota_engine.RuleSnapshot(
        id=rule_id,
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


def _restart(monkeypatch, clock: Clock) -> None:
    """New counter and a forced reload, sharing the clock and the database."""
    monkeypatch.setattr(quota_engine, "GLOBAL_COUNTER", SlidingWindowCounter(now=clock))
    monkeypatch.setattr(quota_engine, "_hydrated", False)
    quota_engine._synced.clear()
    quota_engine._alert_dedupe.clear()


def _orm_rule(snapshot) -> QuotaRule:
    return QuotaRule(
        id=snapshot.id,
        subject_type=snapshot.subject_type,
        subject_id=snapshot.subject_id,
        kind=snapshot.kind,
        window_seconds=snapshot.window_seconds,
        limit_value=snapshot.limit_value,
        enabled=True,
    )


@pytest.fixture
def clock():
    return Clock(1_700_000_000.0)


@pytest.fixture(autouse=True)
def persisted_engine(monkeypatch, clock):
    monkeypatch.setattr(quota_engine, "_persistence_enabled", True)
    # These tests have no resource_events table. Threshold writes are covered
    # by the test that records them; other cases only need the usage rows.

    class _SilentEvent:
        @classmethod
        async def create(cls, session, source):
            return None

    monkeypatch.setattr(quota_engine, "ResourceEvent", _SilentEvent)
    _restart(monkeypatch, clock)
    yield
    quota_engine._synced.clear()
    quota_engine._alert_dedupe.clear()


@pytest_asyncio.fixture
async def usage_db(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'quota.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(QuotaUsageSample.__table__.create)

    def open_session():
        return AsyncSession(engine, expire_on_commit=False)

    monkeypatch.setattr(quota_engine, "async_session", open_session)
    yield open_session
    await engine.dispose()


async def _rows(open_session, counter_key: str):
    async with open_session() as session:
        found = (
            await session.exec(
                select(QuotaUsageSample)
                .where(QuotaUsageSample.counter_key == counter_key)
                .order_by(QuotaUsageSample.bucket_second)
            )
        ).all()
        return [(row.bucket_second, row.amount) for row in found]


@pytest.mark.asyncio
async def test_token_budget_used_and_remaining_survive_restart(
    monkeypatch, clock, usage_db
):
    rule = _rule(1, QuotaRuleKind.TOKEN_BUDGET, limit_value=100)
    _stub_rules(monkeypatch, [rule])

    await quota_engine.accrue_usage(total_tokens=40)
    clock.value += 1
    await quota_engine.accrue_usage(total_tokens=10)
    assert await _rows(usage_db, "quota:1:global") == [
        (1_700_000_000, 40),
        (1_700_000_001, 10),
    ]

    _restart(monkeypatch, clock)
    public = await _public_rule(_orm_rule(rule))
    assert public.used_value == 50
    assert public.remaining_value == 50
    # The reloaded window is what the next admission enforces, overage included.
    await quota_engine.accrue_usage(total_tokens=60)
    _restart(monkeypatch, clock)
    denial = await quota_engine.check_request()
    assert denial is not None and denial.total == 110
    public = await _public_rule(_orm_rule(rule))
    assert public.used_value == 110
    assert public.remaining_value == 0


@pytest.mark.asyncio
async def test_same_second_samples_coalesce_across_restart(
    monkeypatch, clock, usage_db
):
    rule = _rule(2, QuotaRuleKind.TOKEN_RATE, limit_value=80)
    _stub_rules(monkeypatch, [rule])
    await quota_engine.accrue_usage(total_tokens=40)
    await quota_engine.accrue_usage(total_tokens=40)
    assert await _rows(usage_db, "quota:2:global") == [(1_700_000_000, 80)]

    _restart(monkeypatch, clock)
    # 80 of 80 is exhausted. The reloaded second is one bucket, not two adds.
    assert await quota_engine.rule_usage(_orm_rule(rule)) == 80
    denial = await quota_engine.check_request()
    assert denial is not None and denial.total == 80


@pytest.mark.asyncio
async def test_request_rate_admission_survives_restart_and_is_not_double_counted(
    monkeypatch, clock, usage_db
):
    _stub_rules(monkeypatch, [_rule(3, QuotaRuleKind.REQUEST_RATE, limit_value=2)])
    assert await quota_engine.check_request(user_id=7) is None
    # Accrual must not consume a second request; admission already did.
    await quota_engine.accrue_usage(total_tokens=5, request_count=1, user_id=7)
    assert await quota_engine.check_request(user_id=8) is None
    assert await quota_engine.check_request(user_id=9) is not None
    assert await _rows(usage_db, "quota:3:global") == [(1_700_000_000, 2)]

    clock.value += 4
    _restart(monkeypatch, clock)
    denial = await quota_engine.check_request(user_id=7)
    assert denial is not None and denial.total == 2
    assert denial.retry_after == 56


@pytest.mark.asyncio
async def test_token_rate_denial_uses_reloaded_samples(monkeypatch, clock, usage_db):
    rule = _rule(
        4,
        QuotaRuleKind.TOKEN_RATE,
        subject_type=QuotaSubjectType.USER,
        subject_id=7,
        limit_value=50,
    )
    _stub_rules(monkeypatch, [rule])
    await quota_engine.accrue_usage(total_tokens=50, user_id=7)

    _restart(monkeypatch, clock)
    assert await quota_engine.rule_usage(_orm_rule(rule)) == 50
    assert await quota_engine.check_request(user_id=7) is not None
    assert await quota_engine.check_request(user_id=8) is None


@pytest.mark.asyncio
async def test_window_slides_using_persisted_timestamps(monkeypatch, clock, usage_db):
    rule = _rule(5, QuotaRuleKind.TOKEN_BUDGET, window_seconds=10, limit_value=100)
    _stub_rules(monkeypatch, [rule])
    await quota_engine.accrue_usage(total_tokens=50)
    clock.value += 11

    _restart(monkeypatch, clock)
    assert await quota_engine.rule_usage(_orm_rule(rule)) == 0
    assert await quota_engine.check_request() is None
    await quota_engine.accrue_usage(total_tokens=10)
    _restart(monkeypatch, clock)
    assert await quota_engine.rule_usage(_orm_rule(rule)) == 10
    assert await _rows(usage_db, "quota:5:global") == [(1_700_000_011, 10)]


@pytest.mark.asyncio
async def test_denied_request_does_not_persist_other_rule(monkeypatch, clock, usage_db):
    _stub_rules(
        monkeypatch,
        [
            _rule(21, QuotaRuleKind.REQUEST_RATE, limit_value=2),
            _rule(22, QuotaRuleKind.TOKEN_BUDGET, limit_value=1),
        ],
    )
    await quota_engine.accrue_usage(total_tokens=1)
    _restart(monkeypatch, clock)
    assert await quota_engine.check_request() is not None
    assert await _rows(usage_db, "quota:21:global") == []
    assert await _rows(usage_db, "quota:22:global") == [(1_700_000_000, 1)]


@pytest.mark.asyncio
async def test_reset_rule_deletes_persisted_samples(monkeypatch, clock, usage_db):
    rule = _rule(6, QuotaRuleKind.TOKEN_BUDGET, limit_value=100)
    _stub_rules(monkeypatch, [rule])
    await quota_engine.accrue_usage(total_tokens=40)
    await quota_engine.reset_rule(6)

    _restart(monkeypatch, clock)
    assert await quota_engine.rule_usage(_orm_rule(rule)) == 0
    assert await _rows(usage_db, "quota:6:global") == []


@pytest.mark.asyncio
async def test_restart_still_returns_429(monkeypatch, clock, usage_db):
    _stub_rules(monkeypatch, [_rule(7, QuotaRuleKind.REQUEST_RATE, limit_value=1)])
    assert await quota_engine.check_request() is None
    clock.value += 3
    _restart(monkeypatch, clock)

    request = SimpleNamespace(state=SimpleNamespace(), headers={})
    response = await quotas_api.quota_guard(request)
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "57"


@pytest.mark.asyncio
async def test_failed_flush_is_retried_with_the_unsynced_second(
    monkeypatch, clock, usage_db
):
    _stub_rules(monkeypatch, [_rule(8, QuotaRuleKind.TOKEN_BUDGET, limit_value=1000)])
    calls = {"n": 0}
    real_sync = quota_usage_store.sync_buckets

    async def flaky(session, counter_key, samples, cutoff_ms):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("disk full")
        return await real_sync(session, counter_key, samples, cutoff_ms)

    monkeypatch.setattr(quota_usage_store, "sync_buckets", flaky)
    await quota_engine.accrue_usage(total_tokens=40)
    assert calls["n"] == 1
    assert await _rows(usage_db, "quota:8:global") == []

    clock.value += 1
    await quota_engine.accrue_usage(total_tokens=15)
    assert calls["n"] == 2

    _restart(monkeypatch, clock)
    assert (
        await quota_engine.rule_usage(
            _orm_rule(_rule(8, QuotaRuleKind.TOKEN_BUDGET, limit_value=1000))
        )
        == 55
    )


@pytest.mark.asyncio
async def test_budget_alerts_still_fire_when_usage_is_persisted(monkeypatch, usage_db):
    events = []

    class FakeResourceEvent:
        @classmethod
        async def create(cls, session, source):
            events.append(source)

    monkeypatch.setattr(quota_engine, "ResourceEvent", FakeResourceEvent)
    _stub_rules(
        monkeypatch,
        [
            _rule(
                9,
                QuotaRuleKind.TOKEN_BUDGET,
                subject_type=QuotaSubjectType.API_KEY,
                subject_id=11,
                window_seconds=3600,
                limit_value=100,
            )
        ],
    )
    await quota_engine.accrue_usage(total_tokens=85, api_key_id=11)
    await quota_engine.accrue_usage(total_tokens=11, api_key_id=11)
    await quota_engine.accrue_usage(total_tokens=5, api_key_id=11)
    await quota_engine.accrue_usage(total_tokens=5, api_key_id=11)
    assert [event["spec_snapshot"]["threshold_percent"] for event in events] == [
        80,
        95,
        100,
    ]


@pytest.mark.asyncio
async def test_unloaded_database_does_not_admit_or_accrue_as_zero(
    monkeypatch, clock, usage_db
):
    rule = _rule(10, QuotaRuleKind.TOKEN_BUDGET, limit_value=100)
    _stub_rules(monkeypatch, [rule])
    async with usage_db() as session:
        session.add(
            QuotaUsageSample(
                rule_id=10,
                counter_key="quota:10:global",
                bucket_second=int(clock.value),
                recorded_at_ms=int(round(clock.value * 1000)),
                amount=25,
                window_seconds=60,
            )
        )
        await session.commit()

    real_load = quota_usage_store.load_samples
    state = {"fail": True}

    async def maybe_load(session):
        if state["fail"]:
            raise RuntimeError("db down")
        return await real_load(session)

    monkeypatch.setattr(quota_usage_store, "load_samples", maybe_load)
    with pytest.raises(RuntimeError, match="db down"):
        await quota_engine.check_request()
    # Accrual must not treat the failed load as an empty window and add to it.
    await quota_engine.accrue_usage(total_tokens=100)
    assert quota_engine.GLOBAL_COUNTER.total("quota:10:global", 60) == 0

    state["fail"] = False
    _restart(monkeypatch, clock)
    assert await quota_engine.rule_usage(_orm_rule(rule)) == 25
