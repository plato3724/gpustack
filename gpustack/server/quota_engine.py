"""Process-local admission, token accrual and budget threshold events.

Request quotas are consumed atomically at admission. Token quotas gate later
requests after the usage collector commits completed usage; actual overages
are retained. Counters reset on process restart and on local rule edits.
Gateway routes must use remote authorization for the admission guard to run.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from sqlmodel import select

from gpustack.schemas.quota_rules import QuotaRule, QuotaRuleKind, QuotaSubjectType
from gpustack.schemas.resource_events import (
    ResourceEvent,
    EVENT_TYPE_UPDATED,
)
from gpustack.server.db import async_session
from gpustack.utils.rate_limit import GLOBAL_COUNTER, WindowDecision

logger = logging.getLogger(__name__)

RULES_CACHE_TTL_SECONDS = 10.0

# Budget depletion thresholds that raise an event, in percent.
BUDGET_ALERT_THRESHOLDS = (80, 95, 100)

RESOURCE_TYPE_QUOTA_RULE = "quota_rule"

_cache: dict = {"rules": None, "loaded_at": 0.0}


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


async def check_request(
    *,
    user_id: Optional[int] = None,
    api_key_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    model_id: Optional[int] = None,
) -> Optional[WindowDecision]:
    """Pre-request gate for request rates, token rates and token budgets.

    Returns a denial with retry_after when any applicable quota is exhausted.
    Rule-load failures propagate when there is no cached snapshot.
    """
    matched = _matching_rules(
        await load_rules(),
        user_id=user_id,
        api_key_id=api_key_id,
        organization_id=organization_id,
        model_id=model_id,
    )
    decision = GLOBAL_COUNTER.take_many(
        [
            (
                _counter_key(rule, rule.subject_id),
                1 if rule.kind == QuotaRuleKind.REQUEST_RATE else 0,
                rule.window_seconds,
                rule.limit_value,
            )
            for rule in matched
        ]
    )
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
    usage accounting must not break the response path."""
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
        for rule in matched:
            # Request counters are consumed only by the pre-request guard.
            if rule.kind == QuotaRuleKind.REQUEST_RATE or total_tokens <= 0:
                continue
            subject_value = _subject_value(rule, ids)
            total = GLOBAL_COUNTER.add(
                _counter_key(rule, subject_value), total_tokens, rule.window_seconds
            )
            if rule.kind == QuotaRuleKind.TOKEN_BUDGET:
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


def rule_usage(rule: QuotaRule) -> int:
    return GLOBAL_COUNTER.total(
        _counter_key(rule, rule.subject_id), rule.window_seconds
    )


def reset_rule(rule_id: int) -> None:
    # Only the rule's declared subject can own a counter.
    for key in list(GLOBAL_COUNTER._events):
        if key.startswith(f"quota:{rule_id}:"):
            GLOBAL_COUNTER.reset(key)
    for key in list(_alert_dedupe):
        if key.startswith(f"quota-alert:{rule_id}:"):
            _alert_dedupe.pop(key, None)
