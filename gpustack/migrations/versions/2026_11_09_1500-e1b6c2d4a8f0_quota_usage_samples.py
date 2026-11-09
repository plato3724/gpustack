"""quota_usage_samples: durable sliding-window usage

Token budgets, token rates and request rates keep one coalesced sample
per counter per UTC second. A restart loads the rows that are still
inside the window and keeps sliding them by wall-clock age. A single
stored total cannot expire the old part of the window.

No foreign key to quota_rules: the engine deletes a rule's samples when
the rule is edited or removed, and sample DDL must stay portable across
PostgreSQL, openGauss, MySQL and OceanBase without dialect-specific
referential actions. Columns are plain integers and strings — no native
enums and no JSON.

Revision ID: e1b6c2d4a8f0
Revises: d0f4a1e6c9b3
Create Date: 2026-11-09 15:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import index_exists, table_exists


# revision identifiers, used by Alembic.
revision: str = 'e1b6c2d4a8f0'
down_revision: Union[str, None] = 'd0f4a1e6c9b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE_NAME = 'quota_usage_samples'

_COLUMNS = (
    sa.Column('id', sa.Integer(), primary_key=True),
    sa.Column('rule_id', sa.Integer(), nullable=False),
    sa.Column('counter_key', sa.String(length=255), nullable=False),
    sa.Column('bucket_second', sa.BigInteger(), nullable=False),
    sa.Column('recorded_at_ms', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('window_seconds', sa.Integer(), nullable=False),
)

_CONSTRAINTS = (
    sa.UniqueConstraint(
        'counter_key',
        'bucket_second',
        name='uq_quota_usage_samples_bucket',
    ),
)

_INDEXES = (('idx_quota_usage_samples_rule', ['rule_id']),)


def upgrade() -> None:
    if not table_exists(_TABLE_NAME):
        op.create_table(_TABLE_NAME, *_COLUMNS, *_CONSTRAINTS)
    for index_name, columns in _INDEXES:
        if not index_exists(_TABLE_NAME, index_name):
            op.create_index(index_name, _TABLE_NAME, columns)


def downgrade() -> None:
    # Dropping the table discards in-window usage. The samples can be
    # rebuilt only from new traffic; there is no separate ledger to
    # restore them from, so this downgrade is intentionally lossy.
    if not table_exists(_TABLE_NAME):
        return
    for index_name, _ in _INDEXES:
        if index_exists(_TABLE_NAME, index_name):
            op.drop_index(index_name, table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
