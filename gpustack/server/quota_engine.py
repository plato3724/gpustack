"""Admission, token accrual and budget threshold events.

Request quotas are consumed atomically at admission. Token quotas gate
later requests after the usage collector commits completed usage; actual
overages are retained. Gateway routes must use remote authorization for
the admission guard to run.

Durable versus ephemeral
------------------------
The samples that make up ``used_value`` are durable. Each counter
(``quota:{rule_id}:{subject}``) is a deque of ``(unix_time, amount)``
pairs kept in :data:`GLOBAL_COUNTER` and mirrored to
``quota_usage_samples``, one coalesced UTC second per row. The first
admission, accrual or usage read in a process loads those rows back and
keeps sliding them by wall-clock age, so a restart on the same database
does not zero ``used_value`` / remaining.

Still ephemeral, on purpose:

- The 10-second rule snapshot (``quota_rules`` itself is already in SQL).
- Budget-alert dedupe. A restart can emit 80/95/100 again the next time
  usage is accrued while the window is still over that threshold. The
  events that were written stay in ``resource_events``.
- Anything not yet committed. A crash between the memory update and the
  SQL commit drops that latest sample on restart. This process still
  counts it, and the next successful write of the counter upserts every
  second that never landed.

When rows are written
---------------------
- ``request_rate`` is written when admission allows the request, before
  ``check_request`` returns. A denial consumes nothing and writes nothing.
- ``token_rate`` and ``token_budget`` are written from ``accrue_usage``
  after the usage collector has committed. Amounts above the limit are
  stored. Admission of those kinds only reads the window (amount 0).
- Samples with ``recorded_at_ms <= now - window`` are deleted on that
  write. Startup also deletes rows that are a full second past the
  window; the precise cut is the in-memory ``<= now - window`` test,
  applied again when the rows are imported.
- Editing or deleting a rule deletes that rule's rows. Restart does not.
  Creating a rule does not clear other rules.

Concurrency
-----------
The mutation lock (``_lock``) is an asyncio lock held across the memory
update and the SQL transaction, so one process cannot persist a stale
snapshot over a newer one. It is not re-entrant: hydrate and persist
must not call back into check / accrue / reset.

The lock is process-local. A second server process hydrates its own copy
and then writes absolute bucket amounts from that copy. Both processes
can admit, and the later write of a given second replaces the earlier
one. There is no distributed lock. Run one server process for these caps.

Wall-clock steps move the window: a forward jump expires samples early,
a backward jump keeps them until real time catches up. Token caps are
still enforced on the *next* request; in-flight work can exceed the limit
because the token count is known only after the collector commits.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from sqlmodel import select

from gpustack.schemas.quota_rules import QuotaRule, QuotaRuleKind, QuotaSubjectType
from gpustack.schemas.quota_usage import QuotaUsageSample
from gpustack.schemas.resource_events import (
    ResourceEvent,
    EVENT_TYPE_UPDATED,
)
from gpustack.server import quota_usage_store
from gpustack.server.db import async_session
from gpustack.utils.rate_limit import SlidingWindowCounter, WindowDecision

logger = logging.getLogger(__name__)

RULES_CACHE_TTL_SECONDS = 10.0

# Budget depletion thresholds that raise an event, in percent.
BUDGET_ALERT_THRESHOLDS = (80, 95, 100)

RESOURCE_TYPE_QUOTA_RULE = "quota_rule"

# Wall clock so sample stamps survive a restart. Monotonic time resets
# with the process and cannot be stored.
GLOBAL_COUNTER = SlidingWindowCounter(now=time.time)

_cache: dict = {"rules": None, "loaded_at": 0.0}

# False until this process has loaded SQL samples (or persistence is off).
_hydrated = False

# Turned off by unit tests that stub the database and only need the deque.
_persistence_enabled = True

# Last payload successfully committed for each bucket:
# counter key -> bucket second -> (recorded_at_ms, amount, window_seconds).
# Compared with the deque so a steady-state write upserts one second, and
# a write that failed to commit is retried in full on the next mutation.
# Touched only while the mutation lock is held.
_synced: Dict[str, Dict[int, Tuple[int, int, int]]] = {}

# Not re-entrant. Lazily bound to the running loop so a new process (and
# test cases, which each get a loop) does not reuse a lock from another.
_mutation_lock: Optional[asyncio.Lock] = None
_mutation_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def _lock() -> asyncio.Lock:
    global _mutation_lock, _mutation_lock_loop
    loop = asyncio.get_running_loop()
    if _mutation_lock is None or _mutation_lock_loop is not loop:
        _mutation_lock = asyncio.Lock()
        _mutation_lock_loop = loop
    return _mutation_lock


def invalidate_rules_cache() -> None:
    _cache["rules"] = None
    _cache["loaded_at"] = 0.0


@dataclass(frozen=True)
class RuleSnapshot:
    id: int
    subject_type: QuotaSubjectType
    subject_id: Optional[int]
    kind: QuotaRuleKind
    window_seconds: int
    limit_value: int


async def load_rules() -> List[RuleSnapshot]:
    now = time.monotonic()
    if (
        _cache["rules"] is not None
        and now - _cache["loaded_at"] < RULES_CACHE_TTL_SECONDS
    ):
        return _cache["rules"]

    try:
        async with async_session() as session:
            rows = (await session.exec(select(QuotaRule))).all()
            rules = [
                RuleSnapshot(
                    id=row.id,
                    subject_type=QuotaSubjectType(row.subject_type),
                    subject_id=row.subject_id,
                    kind=QuotaRuleKind(row.kind),
                    window_seconds=row.window_seconds,
                    limit_value=row.limit_value,
                )
                for row in rows
                if row.enabled
            ]
    except Exception:
        # Keep enforcing the last known rules during a DB outage. Without
        # any known snapshot, fail instead of silently admitting requests.
        logger.exception("quota engine: rule load failed; using stale rules")
        if _cache["rules"] is not None:
            return _cache["rules"]
        raise

    _cache["rules"] = rules
    _cache["loaded_at"] = now
    return rules


def _matching_rules(
    rules: List[RuleSnapshot],
    *,
    user_id: Optional[int],
    api_key_id: Optional[int],
    organization_id: Optional[int],
    model_id: Optional[int],
) -> List[RuleSnapshot]:
    subject_ids: Dict[QuotaSubjectType, Optional[int]] = {
        QuotaSubjectType.USER: user_id,
        QuotaSubjectType.API_KEY: api_key_id,
        QuotaSubjectType.ORGANIZATION: organization_id,
        QuotaSubjectType.MODEL: model_id,
    }
    matched = []
    for rule in rules:
        if rule.subject_type == QuotaSubjectType.GLOBAL:
            matched.append(rule)
            continue
        subject_id = subject_ids.get(rule.subject_type)
        if subject_id is not None and rule.subject_id in (
            subject_id if isinstance(subject_id, (set, list)) else [subject_id]
        ):
            matched.append(rule)
    return matched


def _counter_key(rule: RuleSnapshot, subject_value: Optional[int]) -> str:
    return f"quota:{rule.id}:{subject_value if subject_value is not None else 'global'}"


def _subject_value(rule: RuleSnapshot, ids: Dict[QuotaSubjectType, Optional[int]]):
    if rule.subject_type == QuotaSubjectType.GLOBAL:
        return None
    return ids.get(rule.subject_type)


def _rule_id_from_key(key: str) -> int:
    # quota:{rule_id}:{subject}. Subject is "global" or a decimal id.
    _prefix, rule_id, _subject = key.split(":", 2)
    return int(rule_id)


def _payload(
    stamp: float, amount: int, window_seconds: int
) -> Tuple[int, Tuple[int, int, int]]:
    """Bucket plus the tuple persisted and compared against ``_synced``.

    Milliseconds are rounded so a value loaded from SQL and exported again
    compares equal and is not rewritten on the next request.
    """
    recorded_at_ms = int(round(stamp * 1000))
    bucket = int(stamp)
    return bucket, (recorded_at_ms, int(amount), int(window_seconds))


def _cutoff_ms(now: float, window_seconds: int) -> int:
    """Inclusive upper bound matching ``stamp <= now - window`` at ms resolution."""
    return int(round((now - window_seconds) * 1000))


def _desired_map(key: str) -> Dict[int, Tuple[int, int, int]]:
    window = GLOBAL_COUNTER.window_of(key)
    desired: Dict[int, Tuple[int, int, int]] = {}
    for stamp, amount in GLOBAL_COUNTER.export_samples(key):
        if amount <= 0 or window <= 0:
            continue
        bucket, payload = _payload(stamp, amount, window)
        desired[bucket] = payload
    return desired


def _rows_for(
    key: str, desired: Dict[int, Tuple[int, int, int]]
) -> List[QuotaUsageSample]:
    known = _synced.get(key, {})
    rows: List[QuotaUsageSample] = []
    rule_id = _rule_id_from_key(key)
    for bucket, payload in desired.items():
        if known.get(bucket) == payload:
            continue
        recorded_at_ms, amount, window = payload
        rows.append(
            QuotaUsageSample(
                rule_id=rule_id,
                counter_key=key,
                bucket_second=bucket,
                recorded_at_ms=recorded_at_ms,
                amount=amount,
                window_seconds=window,
            )
        )
    return rows


async def _hydrate_locked() -> None:
    """Load SQL samples into the empty process-local counter.

    Called with the mutation lock held. A failure leaves ``_hydrated``
    false so the next call tries again. Admission fails closed until the
    first successful load: treating an empty deque as zero usage and then
    writing it would replace the current second with a partial amount.
    """
    global _hydrated
    if _hydrated or not _persistence_enabled:
        return

    now = GLOBAL_COUNTER.now()
    async with async_session() as session:
        # Housekeeping only. Rows still inside the window (and the one
        # second of slack) stay; Python below decides what is countable.
        await quota_usage_store.delete_expired(session, now)
        stored = await quota_usage_store.load_samples(session)
        loaded = [
            (
                row.counter_key,
                row.recorded_at_ms,
                int(row.amount),
                int(row.window_seconds),
            )
            for row in stored
        ]
        await session.commit()

    grouped: Dict[str, List[Tuple[float, int]]] = {}
    windows: Dict[str, int] = {}
    for key, recorded_at_ms, amount, window in loaded:
        if amount <= 0 or window <= 0:
            continue
        if recorded_at_ms <= _cutoff_ms(now, window):
            continue
        grouped.setdefault(key, []).append((recorded_at_ms / 1000.0, amount))
        windows[key] = window
        bucket, payload = _payload(recorded_at_ms / 1000.0, amount, window)
        _synced.setdefault(key, {})[bucket] = payload

    for key, samples in grouped.items():
        GLOBAL_COUNTER.import_samples(key, samples, windows[key])
    _hydrated = True
    logger.debug("quota engine: hydrated %s usage sample row(s)", len(loaded))


async def _persist_keys(keys: List[str]) -> None:
    """Mirror ``keys`` into SQL. No-op when persistence is disabled.

    On success, ``_synced`` becomes the deque we wrote. On failure it is
    left unchanged, so the next mutation of those keys upserts every
    second that did not commit — not only the newest tail. The memory
    update that prompted the write is kept either way; this process
    continues to enforce it.
    """
    if not _persistence_enabled:
        return
    unique_keys = list(dict.fromkeys(keys))
    if not unique_keys:
        return

    now = GLOBAL_COUNTER.now()
    planned = []
    for key in unique_keys:
        window = GLOBAL_COUNTER.window_of(key)
        if window <= 0:
            # A key we never read has no window. Persisting it would use a
            # zero cutoff and delete live rows.
            logger.error("quota engine: skip persist for %s without a window", key)
            continue
        desired = _desired_map(key)
        planned.append((key, desired, _rows_for(key, desired), _cutoff_ms(now, window)))
    if not planned:
        return

    async with async_session() as session:
        for key, _desired, rows, cutoff in planned:
            await quota_usage_store.sync_buckets(session, key, rows, cutoff)
        await session.commit()
    for key, desired, _rows, _cutoff in planned:
        _synced[key] = desired


async def _persist_keys_safely(keys: List[str]) -> None:
    try:
        await _persist_keys(keys)
    except Exception:
        # The request or accrual already happened in memory. Failing the
        # caller here would invite a retry that counts the same usage
        # again once SQL is back. The unsynced seconds stay dirty in
        # ``_synced`` and are written on the next successful mutation.
        logger.exception(
            "quota engine: failed to persist usage; this process keeps the "
            "samples and will retry them on the next write"
        )


def _drop_rule_memory(rule_id: int) -> None:
    prefix = f"quota:{rule_id}:"
    GLOBAL_COUNTER.pop_prefix(prefix)
    for key in [k for k in _synced if k.startswith(prefix)]:
        _synced.pop(key, None)
    alert_prefix = f"quota-alert:{rule_id}:"
    for key in [k for k in _alert_dedupe if k.startswith(alert_prefix)]:
        _alert_dedupe.pop(key, None)


async def check_request(
    *,
    user_id: Optional[int] = None,
    api_key_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    model_id: Optional[int] = None,
) -> Optional[WindowDecision]:
    """Pre-request gate for request rates, token rates and token budgets.

    Returns a denial with retry_after when any applicable quota is exhausted.
    Rule-load failures propagate when there is no cached snapshot. The first
    call also fails if usage samples cannot be loaded: an empty counter is
    not treated as zero usage while SQL still holds a window.
    """
    matched = _matching_rules(
        await load_rules(),
        user_id=user_id,
        api_key_id=api_key_id,
        organization_id=organization_id,
        model_id=model_id,
    )
    checks = [
        (
            _counter_key(rule, rule.subject_id),
            1 if rule.kind == QuotaRuleKind.REQUEST_RATE else 0,
            rule.window_seconds,
            rule.limit_value,
        )
        for rule in matched
    ]
    async with _lock():
        await _hydrate_locked()
        decision = GLOBAL_COUNTER.take_many(checks)
        if decision.allowed:
            # Amount 0 reads the token window and must not write a sample.
            consumed = [key for key, amount, _window, _limit in checks if amount > 0]
            await _persist_keys_safely(consumed)
    return None if decision.allowed else decision


async def accrue_usage(
    *,
    total_tokens: int = 0,
    request_count: int = 1,
    user_id: Optional[int] = None,
    api_key_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    model_id: Optional[int] = None,
    user_name: Optional[str] = None,
) -> None:
    """Post-response accounting: tokens into token windows, requests into
    rate windows, budget threshold events as budgets deplete. Never raises —
    usage accounting must not break the response path.

    Request counters are consumed only by the pre-request guard, so this
    path does not add ``request_count`` again. Token samples are written
    here, after the collector's commit.
    """
    try:
        ids = {
            QuotaSubjectType.USER: user_id,
            QuotaSubjectType.API_KEY: api_key_id,
            QuotaSubjectType.ORGANIZATION: organization_id,
            QuotaSubjectType.MODEL: model_id,
        }
        rules = await load_rules()
        matched = _matching_rules(
            rules,
            user_id=user_id,
            api_key_id=api_key_id,
            organization_id=organization_id,
            model_id=model_id,
        )
        pending_events = []
        async with _lock():
            await _hydrate_locked()
            touched = []
            for rule in matched:
                if rule.kind == QuotaRuleKind.REQUEST_RATE or total_tokens <= 0:
                    continue
                subject_value = _subject_value(rule, ids)
                key = _counter_key(rule, subject_value)
                total = GLOBAL_COUNTER.add(key, total_tokens, rule.window_seconds)
                touched.append(key)
                if rule.kind == QuotaRuleKind.TOKEN_BUDGET:
                    pending_events.append((rule, subject_value, total))
            await _persist_keys_safely(touched)
        for rule, subject_value, total in pending_events:
            await _raise_budget_events(rule, subject_value, total)

    except Exception:
        logger.exception("quota engine: usage accrual failed")


async def _raise_budget_events(
    rule: RuleSnapshot, subject_value: Optional[int], window_total: int
) -> None:
    from datetime import datetime, timezone

    for threshold in BUDGET_ALERT_THRESHOLDS:
        if window_total * 100 < rule.limit_value * threshold:
            continue
        event_key = f"quota-alert:{rule.id}:{threshold}"
        if _recently_alerted(event_key, rule.window_seconds):
            continue
        label = f"{rule.subject_type.value}" + (
            f":{subject_value}" if subject_value is not None else ""
        )
        async with async_session() as session:
            await ResourceEvent.create(
                session,
                {
                    "occurred_at": datetime.now(timezone.utc),
                    "resource_type": RESOURCE_TYPE_QUOTA_RULE,
                    "resource_id": rule.id,
                    "resource_name": f"{rule.kind.value}/{label}"[:255],
                    "event_type": EVENT_TYPE_UPDATED,
                    "event_message": (
                        f"Token budget {threshold}% used "
                        f"({window_total}/{rule.limit_value} tokens in the last "
                        f"{rule.window_seconds}s)"
                    )[:1024],
                    "spec_snapshot": {
                        "rule_id": rule.id,
                        "kind": rule.kind.value,
                        "subject_type": rule.subject_type.value,
                        "subject_id": subject_value,
                        "window_total": window_total,
                        "limit_value": rule.limit_value,
                        "threshold_percent": threshold,
                    },
                },
            )

        _mark_alerted(event_key, rule.window_seconds)


# (expires_at_monotonic) per alert key — one event per threshold per window.
# Process-local. A restart can emit the same threshold again; the previous
# resource_events rows are not removed.
_alert_dedupe: Dict[str, float] = {}


def _recently_alerted(event_key: str, window_seconds: int) -> bool:
    expires = _alert_dedupe.get(event_key)
    now = time.monotonic()
    if expires is not None and expires > now:
        return True
    return False


def _mark_alerted(event_key: str, window_seconds: int) -> None:
    _alert_dedupe[event_key] = time.monotonic() + window_seconds
    # Bound the map; stale entries are the only growth path.
    if len(_alert_dedupe) > 10_000:
        now = time.monotonic()
        for key in [k for k, v in _alert_dedupe.items() if v <= now]:
            _alert_dedupe.pop(key, None)


async def rule_usage(rule: QuotaRule) -> int:
    """In-window total for the admin quota list.

    Hydrates from SQL first so ``used_value`` after a restart matches the
    samples admission will enforce. The key is the rule's own subject, the
    same key ``check_request`` uses for that rule.
    """
    async with _lock():
        await _hydrate_locked()
        return GLOBAL_COUNTER.total(
            _counter_key(rule, rule.subject_id), rule.window_seconds
        )


async def reset_rule(rule_id: int) -> None:
    """Clear one rule's samples in memory and in SQL.

    Admin update and delete call this. It is an operator reset, not a
    restart: the SQL rows are deleted, so the next process will not load
    them. Other rules are untouched. Alert dedupe for the rule is dropped
    with the samples; it was never durable.
    """
    async with _lock():
        if _persistence_enabled:
            async with async_session() as session:
                await quota_usage_store.delete_rule_samples(session, rule_id)
                await session.commit()
        _drop_rule_memory(rule_id)
