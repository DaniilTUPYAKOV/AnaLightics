from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.postgres import ApiKey, Project
from backend.model.auth import hash_api_key


async def get_active_project_by_api_key(
    db: AsyncSession,
    api_key: str,
) -> tuple[Project, ApiKey] | None:
    stmt = (
        select(Project, ApiKey)
        .join(ApiKey, ApiKey.project_id == Project.id)
        .where(
            ApiKey.key_hash == hash_api_key(api_key),
            ApiKey.is_active.is_(True),
            ApiKey.revoked_at.is_(None),
            Project.is_active.is_(True),
        )
    )

    result = await db.execute(stmt)
    return result.one_or_none()
