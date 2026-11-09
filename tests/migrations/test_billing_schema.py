"""Check portable DDL and agreement between quota migrations and ORM columns."""

import importlib.util
from pathlib import Path

import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.schema import CreateTable

from gpustack.schemas.quota_rules import QuotaRule
from gpustack.schemas.system_settings import SystemSetting


@pytest.mark.parametrize(
    "filename,model",
    [
        ("2026_09_28_2310-c9e3f0d5b8a2_system_settings.py", SystemSetting),
        ("2026_09_28_2340-d0f4a1e6c9b3_quota_rules.py", QuotaRule),
    ],
)
def test_billing_tables_match_migrations_and_compile_on_supported_dialects(
    filename, model
):
    path = (
        Path(__file__).resolve().parents[2] / "gpustack/migrations/versions" / filename
    )
    spec = importlib.util.spec_from_file_location("billing_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    table = Table(module._TABLE_NAME, MetaData(), *module._COLUMNS)
    assert set(table.columns.keys()) == set(model.__table__.columns.keys())
    # openGauss and OceanBase use these same SQLAlchemy dialects. These DDL
    # contracts need no JSONB operations or native enum types.
    for dialect in (mysql.dialect(), postgresql.dialect()):
        sql = str(CreateTable(table).compile(dialect=dialect))
        assert "CREATE TABLE" in sql and "ENUM(" not in sql
