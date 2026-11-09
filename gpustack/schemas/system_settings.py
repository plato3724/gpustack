"""Validated feature settings persisted as JSON; billing owns price_list."""

from typing import Optional

import sqlalchemy as sa
from sqlmodel import Column, Field, SQLModel

from gpustack.mixins.active_record import ActiveRecordMixin
from gpustack.schemas.common import JSON


class SystemSetting(SQLModel, ActiveRecordMixin, table=True):
    __tablename__ = 'system_settings'
    __table_args__ = (sa.UniqueConstraint("key", name="uq_system_settings_key"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    key: str = Field(nullable=False, index=True)
    value: Optional[dict] = Field(default=None, sa_column=Column(JSON(), nullable=True))
