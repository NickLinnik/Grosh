from datetime import datetime
from typing import Annotated
from zoneinfo import ZoneInfo

import asyncpg
from fastapi import APIRouter, Depends
from grosh_shared.domain.models import User
from grosh_shared.http.errors import ErrorCode, raise_problem
from pydantic import BaseModel

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.repositories.settings_repo import SettingsRepo

router = APIRouter(prefix="/settings", tags=["settings"])

_settings_repo = SettingsRepo()


def get_settings_repo() -> SettingsRepo:
    return _settings_repo


class UserSettings(BaseModel):
    default_rate_source: str | None = None
    timezone: str = "UTC"
    updated_at: datetime | None = None


class UpdateSettingsRequest(BaseModel):
    default_rate_source: str | None = None
    timezone: str | None = None


@router.get("", status_code=200)
async def get_settings(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[SettingsRepo, Depends(get_settings_repo)],
) -> UserSettings:
    row = await repo.get_by_user_id(conn, user.id)
    if row is None:
        return UserSettings()
    return UserSettings(
        default_rate_source=row.default_rate_source,
        timezone=row.timezone,
        updated_at=row.updated_at,
    )


@router.put("", status_code=200)
async def update_settings(
    body: UpdateSettingsRequest,
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[SettingsRepo, Depends(get_settings_repo)],
) -> UserSettings:
    if body.default_rate_source is not None:
        valid = await repo.validate_rate_source(conn, body.default_rate_source)
        if not valid:
            raise_problem(
                422,
                ErrorCode.VALIDATION_ERROR,
                f"Unknown rate source: '{body.default_rate_source}'.",
            )

    if body.timezone is not None:
        try:
            ZoneInfo(body.timezone)
        except (KeyError, ValueError):
            raise_problem(
                422,
                ErrorCode.VALIDATION_ERROR,
                f"Invalid timezone: '{body.timezone}'.",
            )

    row = await repo.upsert(conn, user.id, body.default_rate_source, body.timezone)
    return UserSettings(
        default_rate_source=row.default_rate_source,
        timezone=row.timezone,
        updated_at=row.updated_at,
    )
