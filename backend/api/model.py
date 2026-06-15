

from uuid import UUID

from pydantic import BaseModel


class ProjectContext(BaseModel):
    project_id: UUID
    api_key_id: UUID | None = None
    rate_limit_per_minute: int