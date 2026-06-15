from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.postgres import Project


async def get_active_project_by_api_key(
    db: AsyncSession,
    api_key: str,
) -> Project | None:
    stmt = select(Project).where(
        Project.api_key == api_key,
        Project.is_active == True,
    )

    result = await db.execute(stmt)
    return result.scalar_one_or_none()