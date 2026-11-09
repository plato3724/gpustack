"""quota_rules: token budgets and rate limits

One table, three kinds (request_rate / token_rate / token_budget) bound to a
global, user, api_key, model, or organization subject. Enforcement is
sliding-window over trailing seconds; threshold events for budgets land in
resource_events. See the schema docstring for the full contract.

Revision ID: d0f4a1e6c9b3
Revises: c9e3f0d5b8a2
Create Date: 2026-09-28 23:40:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import index_exists, table_exists


# revision identifiers, used by Alembic.
revision: str = 'd0f4a1e6c9b3'
down_revision: Union[str, None] = 'c9e3f0d5b8a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE_NAME = 'quota_rules'

_COLUMNS = (
    sa.Column('id', sa.Integer(), primary_key=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.Column('deleted_at', sa.DateTime(), nullable=True),
    sa.Column(
        'subject_type',
        sa.String(length=32),
        nullable=False,
    ),
    sa.Column('subject_id', sa.Integer(), nullable=True),
    sa.Column(
        'kind',
        sa.String(length=32),
        nullable=False,
    ),
    sa.Column('window_seconds', sa.Integer(), nullable=False),
    sa.Column('limit_value', sa.BigInteger(), nullable=False),
    sa.Column('enabled', sa.Boolean(), nullable=False),
)

_INDEXES = (('idx_quota_rules_subject', ['subject_type', 'subject_id']),)


def upgrade() -> None:
    if not table_exists(_TABLE_NAME):
        op.create_table(_TABLE_NAME, *_COLUMNS)
    for index_name, columns in _INDEXES:
        if not index_exists(_TABLE_NAME, index_name):
            op.create_index(index_name, _TABLE_NAME, columns)


def downgrade() -> None:
    if not table_exists(_TABLE_NAME):
        return
    for index_name, _ in _INDEXES:
        if index_exists(_TABLE_NAME, index_name):
            op.drop_index(index_name, table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
