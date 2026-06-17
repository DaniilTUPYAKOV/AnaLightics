import datetime
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


def build_rotated_api_key_name(
    old_name: str,
    new_name: str | None,
) -> str:
    if new_name is not None:
        return new_name

    rotated_at = datetime.datetime.now(datetime.timezone.utc)
    return f"{old_name}-rotated-{rotated_at:%Y%m%d}"


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


async def get_active_project_api_key(
    db: AsyncSession,
    project_id: UUID,
    api_key_id: UUID,
) -> ApiKey | None:
    stmt = select(ApiKey).where(
        ApiKey.id == api_key_id,
        ApiKey.project_id == project_id,
        ApiKey.is_active.is_(True),
        ApiKey.revoked_at.is_(None),
    )

    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def rotate_project_api_key(
    db: AsyncSession,
    project_id: UUID,
    api_key_id: UUID,
    new_name: str | None,
    settings: Settings,
) -> tuple[ApiKey, str] | None:
    old_api_key = await get_active_project_api_key(db, project_id, api_key_id)

    if old_api_key is None:
        return None

    return await create_project_api_key(
        db,
        project_id,
        build_rotated_api_key_name(old_api_key.name, new_name),
        settings,
    )


async def revoke_project_api_key(
    db: AsyncSession,
    project_id: UUID,
    api_key_id: UUID,
) -> ApiKey | None:
    api_key = await get_active_project_api_key(db, project_id, api_key_id)

    if api_key is None:
        return None

    api_key.is_active = False
    api_key.revoked_at = datetime.datetime.now(datetime.timezone.utc)

    await db.commit()
    await db.refresh(api_key)

    return api_key
