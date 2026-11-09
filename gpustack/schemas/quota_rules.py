"""Quota rules: centrally-managed token budgets and rate limits.

One table covers the three enforcement kinds from the billing governance
surface:

- ``request_rate`` — requests per window, enforced pre-request (a caller
  cannot crowd out others).
- ``token_rate`` — tokens per window, accrued post-response; a caller that
  burns through the window's tokens is denied until it slides.
- ``token_budget`` — same mechanics, longer windows (day/month); the engine
  raises threshold events (80% / 95% / 100%) as the budget depletes.

Subjects: a rule binds to a ``global`` subject (every caller), or to one
user / API key / model / organization by id. Rules overlap on purpose — the
strictest applicable rule wins per check.
"""

from enum import Enum
from typing import Optional

import sqlalchemy as sa
from sqlmodel import Column, Field, SQLModel
from pydantic import model_validator

from gpustack.mixins import BaseModelMixin
from gpustack.schemas.common import ListParams, PaginatedList


class QuotaSubjectType(str, Enum):
    GLOBAL = "global"
    USER = "user"
    API_KEY = "api_key"
    MODEL = "model"
    ORGANIZATION = "organization"


class QuotaRuleKind(str, Enum):
    REQUEST_RATE = "request_rate"
    TOKEN_RATE = "token_rate"
    TOKEN_BUDGET = "token_budget"


class QuotaRule(SQLModel, BaseModelMixin, table=True):
    __tablename__ = "quota_rules"
    __table_args__ = (
        sa.Index("idx_quota_rules_subject", "subject_type", "subject_id"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)

    subject_type: QuotaSubjectType = Field(
        sa_column=Column(sa.String(32), nullable=False)
    )
    # NULL for the global subject; the id of the user / API key / model /
    # organization otherwise.
    subject_id: Optional[int] = Field(default=None, sa_column=Column(sa.Integer))

    kind: QuotaRuleKind = Field(sa_column=Column(sa.String(32), nullable=False))
    # Window length in seconds. Budgets use long windows (86400, 2592000);
    # rates short ones (60). No calendar alignment — trailing windows are
    # not pinned to a timezone. Usage inside the window is stored as
    # per-second samples (see quota_usage_samples), not as a single total.
    window_seconds: int = Field(nullable=False)
    # Requests (request_rate) or tokens (the other two) allowed per window.
    limit_value: int = Field(sa_column=Column(sa.BigInteger, nullable=False))
    enabled: bool = Field(default=True, nullable=False)

    model_config = {"protected_namespaces": ()}


class QuotaRuleBase(SQLModel):
    subject_type: QuotaSubjectType
    subject_id: Optional[int] = Field(default=None, gt=0)
    kind: QuotaRuleKind
    window_seconds: int = Field(ge=1, le=31_536_000)
    limit_value: int = Field(ge=1, le=9_007_199_254_740_991)
    enabled: bool = True

    @model_validator(mode="after")
    def validate_subject(self):
        if self.subject_type == QuotaSubjectType.GLOBAL:
            if self.subject_id is not None:
                raise ValueError("global rules must not have subject_id")
        elif self.subject_id is None:
            raise ValueError("subject_id is required for non-global rules")
        return self


class QuotaRuleCreate(QuotaRuleBase):
    pass


class QuotaRuleUpdate(QuotaRuleBase):
    pass


class QuotaRulePublic(QuotaRuleBase):
    id: int
    used_value: int = 0
    remaining_value: int = 0

    model_config = {"from_attributes": True}


class QuotaRulesPublic(PaginatedList):
    items: list[QuotaRulePublic]


class QuotaRuleListParams(ListParams):
    pass
