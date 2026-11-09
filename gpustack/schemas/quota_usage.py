"""Persisted samples for quota sliding windows.

One row is one coalesced second of usage for one counter
(``quota:{rule_id}:{subject}``). The row's timestamp is what lets a new
server process drop the part of the window that has already aged out.
A single stored total cannot do that: after a restart it would either
count forever or forget the age of each contribution.

Rows are hard-deleted when they slide out of the window or when an
admin edits or deletes the rule. There is no soft-delete column on
purpose — these rows are a cache of recent usage, not an audit ledger
(``model_usages`` / ``resource_events`` already keep that history).
"""

from typing import Optional

import sqlalchemy as sa
from sqlmodel import Column, Field, SQLModel


class QuotaUsageSample(SQLModel, table=True):
    __tablename__ = "quota_usage_samples"
    __table_args__ = (
        # One coalesced bucket per counter. The engine upserts the latest
        # second instead of inserting every request.
        sa.UniqueConstraint(
            "counter_key",
            "bucket_second",
            name="uq_quota_usage_samples_bucket",
        ),
        sa.Index("idx_quota_usage_samples_rule", "rule_id"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    # Not a foreign key: sample DDL must not block quota-rule changes, and
    # the engine deletes a rule's rows itself when the rule is edited or
    # removed. A leftover row is ignored once its rule id is gone.
    rule_id: int = Field(sa_column=Column(sa.Integer(), nullable=False))
    # ``quota:{rule_id}:{subject_id|global}`` — the same string the
    # in-memory counter uses, so hydrate and admission cannot drift apart
    # by inventing a second key format.
    counter_key: str = Field(sa_column=Column(sa.String(255), nullable=False))
    # UTC unix second, matching ``int(stamp)`` inside SlidingWindowCounter.
    # BigInteger so the key survives the 2038 signed-32-bit boundary.
    bucket_second: int = Field(sa_column=Column(sa.BigInteger(), nullable=False))
    # Wall-clock milliseconds of the latest event folded into this second.
    # Retry-After is computed from this stamp, not from the bucket floor.
    recorded_at_ms: int = Field(sa_column=Column(sa.BigInteger(), nullable=False))
    amount: int = Field(sa_column=Column(sa.BigInteger(), nullable=False))
    # Copied from the rule at write time so a restart can expire rows
    # without joining ``quota_rules``. An edit clears the rows anyway.
    window_seconds: int = Field(sa_column=Column(sa.Integer(), nullable=False))
