"""Billing settings access with a short-lived process cache.

Writes invalidate the local cache; other server processes pick up edits within
five seconds. The owning admin API validates price-list values.
"""

import time
from typing import Optional

from sqlmodel import col, select

from gpustack.schemas.system_settings import SystemSetting
from gpustack.server.db import async_session

# Peers pick up operator edits within this window; the writing instance is
# invalidated synchronously (see set_setting).
CACHE_TTL_SECONDS = 5.0

_cache: dict = {"values": {}, "loaded_at": 0.0}


def invalidate_settings_cache() -> None:
    """Drop the cached snapshot (called after a local write)."""
    _cache["values"] = {}
    _cache["loaded_at"] = 0.0


async def get_all_settings() -> dict:
    """All settings as {key: value}, from cache when fresh."""
    now = time.monotonic()
    if _cache["loaded_at"] and now - _cache["loaded_at"] < CACHE_TTL_SECONDS:
        return _cache["values"]

    async with async_session() as session:
        rows = (await session.exec(select(SystemSetting))).all()
        values = {row.key: row.value for row in rows}

    _cache["values"] = values
    _cache["loaded_at"] = now
    return values


async def get_setting(key: str, default: Optional[dict] = None) -> Optional[dict]:
    settings = await get_all_settings()
    value = settings.get(key)
    return value if value is not None else default


async def set_setting(key: str, value: dict) -> None:
    """Upsert one setting and invalidate the local cache."""
    async with async_session() as session:
        existing = (
            await session.exec(
                select(SystemSetting).where(col(SystemSetting.key) == key)
            )
        ).first()
        if existing is None:
            await SystemSetting.create(session, {"key": key, "value": value})
        else:
            existing.value = value
            await existing.save(session)
    invalidate_settings_cache()
