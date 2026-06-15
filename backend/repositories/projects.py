from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.postgres import ApiKey, Project
from backend.model.auth import (
    generate_api_key,
    get_api_key_prefix,
    hash_api_key,
    verify_api_key_hash,
)
from backend.model.config import Settings


async def get_active_project_by_api_key(
    db: AsyncSession,
    api_key: str,
    settings: Settings,
) -> tuple[Project, ApiKey] | None:
    key_prefix = get_api_key_prefix(api_key)

    stmt = (
        select(Project, ApiKey)
        .join(ApiKey, ApiKey.project_id == Project.id)
        .where(
            ApiKey.key_prefix == key_prefix,
            ApiKey.is_active.is_(True),
            ApiKey.revoked_at.is_(None),
            Project.is_active.is_(True),
        )
    )

    result = await db.execute(stmt)

    for project, stored_api_key in result.all():
        if verify_api_key_hash(
            api_key,
            settings.api_key_hash_secret,
            stored_api_key.key_hash,
        ):
            return project, stored_api_key

    return None


async def create_project_api_key(
    db: AsyncSession,
    project_id: UUID,
    name: str,
    settings: Settings,
) -> tuple[ApiKey, str]:
    raw_api_key = generate_api_key()
    api_key = ApiKey(
        project_id=project_id,
        name=name,
        key_hash=hash_api_key(raw_api_key, settings.api_key_hash_secret),
        key_prefix=get_api_key_prefix(raw_api_key),
    )

    db.add(api_key)
    await db.commit()
    await db.refresh(api_key)

    return api_key, raw_api_key
