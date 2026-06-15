from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    is_valid: bool # Валидность API-ключа
    project_id: str | None = None  # Идентификатор проекта
