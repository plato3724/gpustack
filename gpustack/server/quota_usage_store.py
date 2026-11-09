"""SQL read/write for quota window samples.

The quota engine is the only caller. It keeps a process-local deque as
the admission source of truth and uses these helpers to mirror that
deque into ``quota_usage_samples``.

Writes are absolute bucket amounts (the coalesced total for that
second), not increments. That way a retry after a lost commit replaces
the second instead of adding the same usage twice. Unchanged seconds
are left in place so a long budget window is not rewritten on every
request — a 30-day window can hold one row per second.

This module does not lock. The engine serializes calls for one process
with its mutation lock. Two server processes can still read-modify-write
the same bucket; that race is documented on the engine and is not
solved here.
"""

from typing import List, Sequence

from sqlalchemy import delete
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.schemas.quota_usage import QuotaUsageSample


async def load_samples(session: AsyncSession) -> Sequence[QuotaUsageSample]:
    """Return every stored sample. The caller drops rows outside the window."""
    return (await session.exec(select(QuotaUsageSample))).all()


async def delete_expired(session: AsyncSession, now: float) -> None:
    """Remove rows that are at least one second past their own window.

    The extra second is deliberate. Expiry that decides ``used_value``
    uses the sample's millisecond stamp in Python so it matches the
    in-memory ``<= now - window`` test. Doing that comparison in SQL
    can shave a sample off early when the dialect rounds the bound.
    This statement only reclaims rows that are unambiguously finished.
    """
    now_ms = now * 1000
    await session.exec(
        delete(QuotaUsageSample).where(
            QuotaUsageSample.recorded_at_ms
            <= now_ms - (QuotaUsageSample.window_seconds * 1000) - 1000
        )
    )


async def sync_buckets(
    session: AsyncSession,
    counter_key: str,
    samples: List[QuotaUsageSample],
    cutoff_ms: int,
) -> None:
    """Upsert ``samples`` and delete this counter's expired rows.

    ``samples`` is only the buckets whose amount or stamp changed.
    The delete uses the engine's precise cutoff, which is tighter than
    :func:`delete_expired`.
    """
    await session.exec(
        delete(QuotaUsageSample).where(
            QuotaUsageSample.counter_key == counter_key,
            QuotaUsageSample.recorded_at_ms <= cutoff_ms,
        )
    )
    # The bulk DELETE is emitted immediately, but flush before SELECT so
    # a bucket we just expired cannot be updated and then inserted again.
    await session.flush()
    for sample in samples:
        if sample.recorded_at_ms <= cutoff_ms or sample.amount <= 0:
            continue
        existing = (
            await session.exec(
                select(QuotaUsageSample).where(
                    QuotaUsageSample.counter_key == counter_key,
                    QuotaUsageSample.bucket_second == sample.bucket_second,
                )
            )
        ).first()
        if existing is None:
            session.add(sample)
            continue
        existing.rule_id = sample.rule_id
        existing.recorded_at_ms = sample.recorded_at_ms
        existing.amount = sample.amount
        existing.window_seconds = sample.window_seconds


async def delete_rule_samples(session: AsyncSession, rule_id: int) -> None:
    """Drop every sample for ``rule_id`` (admin edit or delete)."""
    await session.exec(
        delete(QuotaUsageSample).where(QuotaUsageSample.rule_id == rule_id)
    )
