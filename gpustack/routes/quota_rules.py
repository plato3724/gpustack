"""Admin CRUD over quota rules (token budgets and rate limits).

Writes invalidate the engine's rule cache immediately, so an operator's
change bites within the load path's next read — no restart, no waiting out
the TTL on the writing instance.
"""

import logging

from fastapi import APIRouter
from sqlmodel import select, col
from gpustack.schemas.resource_events import ResourceEvent

from gpustack.api.exceptions import InvalidException, NotFoundException
from gpustack.server.deps import SessionDep
from gpustack.server import quota_engine
from gpustack.schemas.common import Pagination
from gpustack.schemas.quota_rules import (
    QuotaRule,
    QuotaRuleCreate,
    QuotaRulePublic,
    QuotaRulesPublic,
    QuotaRuleUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Windows that make operational sense; keeps operators from disabling the
# feature with a 0-second window or a negative limit.
_MIN_WINDOW_SECONDS = 1
_MIN_LIMIT_VALUE = 1


def _validate_subjects(payload: QuotaRuleCreate | QuotaRuleUpdate) -> None:
    if payload.window_seconds < _MIN_WINDOW_SECONDS:
        raise InvalidException(
            message=f"window_seconds must be >= {_MIN_WINDOW_SECONDS}"
        )
    if payload.limit_value < _MIN_LIMIT_VALUE:
        raise InvalidException(message=f"limit_value must be >= {_MIN_LIMIT_VALUE}")
    if payload.subject_type != "global" and payload.subject_id is None:
        raise InvalidException(
            message=f"subject_id is required for subject_type={payload.subject_type}"
        )


def _public_rule(rule: QuotaRule) -> QuotaRulePublic:
    result = QuotaRulePublic.model_validate(rule)
    result.used_value = quota_engine.rule_usage(rule)
    result.remaining_value = max(0, rule.limit_value - result.used_value)
    return result


def _to_public(rules: list[QuotaRule]) -> QuotaRulesPublic:
    count = len(rules)
    return QuotaRulesPublic(
        items=[_public_rule(r) for r in rules],
        pagination=Pagination(page=1, perPage=max(count, 1), total=count, totalPage=1),
    )


@router.get("", response_model=QuotaRulesPublic)
async def list_quota_rules(session: SessionDep):
    rules = await QuotaRule.all(session)
    return _to_public(rules)


@router.post("", response_model=QuotaRulePublic)
async def create_quota_rule(session: SessionDep, payload: QuotaRuleCreate):
    _validate_subjects(payload)
    rule = await QuotaRule.create(session, payload.model_dump())
    quota_engine.invalidate_rules_cache()
    return _public_rule(rule)


@router.get("/alerts")
async def list_quota_alerts(session: SessionDep):
    rows = (
        await session.exec(
            select(ResourceEvent)
            .where(col(ResourceEvent.resource_type) == "quota_rule")
            .order_by(col(ResourceEvent.occurred_at).desc())
            .limit(100)
        )
    ).all()
    return {
        "items": [
            {
                "id": r.id,
                "occurred_at": r.occurred_at,
                "message": r.event_message,
                "rule_id": r.resource_id,
            }
            for r in rows
        ]
    }


@router.get("/{id}", response_model=QuotaRulePublic)
async def get_quota_rule(session: SessionDep, id: int):
    rule = await QuotaRule.one_by_id(session, id)
    if rule is None:
        raise NotFoundException(message=f"Quota rule {id} not found")
    return _public_rule(rule)


@router.put("/{id}", response_model=QuotaRulePublic)
async def update_quota_rule(session: SessionDep, id: int, payload: QuotaRuleUpdate):
    _validate_subjects(payload)
    rule = await QuotaRule.one_by_id(session, id)
    if rule is None:
        raise NotFoundException(message=f"Quota rule {id} not found")
    for field, value in payload.model_dump().items():
        setattr(rule, field, value)
    await rule.save(session)
    quota_engine.reset_rule(rule.id)
    quota_engine.invalidate_rules_cache()
    return _public_rule(rule)


@router.delete("/{id}")
async def delete_quota_rule(session: SessionDep, id: int):
    rule = await QuotaRule.one_by_id(session, id)
    if rule is None:
        raise NotFoundException(message=f"Quota rule {id} not found")
    await rule.delete(session)
    quota_engine.reset_rule(rule.id)
    quota_engine.invalidate_rules_cache()
    return None
