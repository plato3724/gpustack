"""system_settings: platform key/value settings

Persist the billing price list as a validated feature-owned JSON setting.

Revision ID: c9e3f0d5b8a2
Revises: c8d3e6f1a902
Create Date: 2026-09-28 23:10:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from gpustack.migrations.utils import index_exists, table_exists


# revision identifiers, used by Alembic.
revision: str = 'c9e3f0d5b8a2'
down_revision: Union[str, None] = 'c8d3e6f1a902'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE_NAME = 'system_settings'

_COLUMNS = (
    sa.Column('id', sa.Integer(), primary_key=True),
    sa.Column('key', sa.String(length=255), nullable=False),
    sa.Column('value', sa.JSON(), nullable=True),
)

_CONSTRAINTS = (sa.UniqueConstraint('key', name='uq_system_settings_key'),)

_INDEXES = (('ix_system_settings_key', ['key']),)


def upgrade() -> None:
    if not table_exists(_TABLE_NAME):
        op.create_table(_TABLE_NAME, *_COLUMNS, *_CONSTRAINTS)
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
