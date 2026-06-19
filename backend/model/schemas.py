from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class Event(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "url": "https://example.com/catalog",
                "title": "Catalog",
                "referrer": None,
                "user_agent": "Mozilla/5.0",
                "screen_width": 1920,
                "screen_height": 1080,
                "timestamp": "2026-06-19T10:30:00+00:00",
                "event_type": "page_view",
            }
        },
    )

    url: HttpUrl  # Полный URL страницы, на которой произошло событие
    title: str = Field(min_length=1, max_length=300)  # Заголовок текущей страницы
    referrer: HttpUrl | None = None  # URL страницы-источника
    user_agent: str = Field(
        min_length=1,
        max_length=1000,
    )  # Информация о браузере, ОС, устройстве.
    screen_width: int = Field(gt=0, le=16384)  # Ширина экрана устройства в пикселях.
    screen_height: int = Field(gt=0, le=16384)  # Высота экрана устройства в пикселях.
    timestamp: datetime  # Временная метка события в формате ISO
    event_type: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )  # Тип события: lowercase letters, digits, underscores.


class APIKeyCheck(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "is_valid": True,
                "project_id": "00000000-0000-0000-0000-000000000001",
            }
        }
    )

    is_valid: bool # Валидность API-ключа
    project_id: str | None = None  # Идентификатор проекта


class APIKeyCreate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"name": "Production tracking key"}}
    )

    name: str = Field(min_length=1, max_length=100)


class APIKeyRotate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"name": "Production tracking key v2"}}
    )

    name: str | None = Field(default=None, min_length=1, max_length=100)


class APIKeyCreated(BaseModel):
    id: UUID
    project_id: UUID
    name: str
    key_prefix: str
    api_key: str


class APIKeyRevoked(BaseModel):
    id: UUID
    project_id: UUID
    name: str
    revoked_at: datetime


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: dict = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error": {
                    "code": "RATE_LIMIT_EXCEEDED",
                    "message": "Too many events",
                    "request_id": "7cc3e0e1-0000-4000-9000-000000000000",
                    "details": {},
                }
            }
        }
    )

    error: ErrorBody
