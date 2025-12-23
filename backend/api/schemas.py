from pydantic import BaseModel
from typing import Optional

class Event(BaseModel):
    url: str  # Полный URL страницы, на которой произошло событие
    title: str  # Заголовок текущей страницы
    referrer: Optional[str] = None  # URL страницы‑источника, откуда пользователь перешёл на текущую страницу. 
    user_agent: str  # Содержит информацию о браузере, ОС, устройстве.
    screen_width: int  # Ширина экрана устройства в пикселях.
    screen_height: int  # Высота экрана устройства в пикселях.
    timestamp: str  # Временная метка события в формате ISO
    event_type: str  # Тип зафиксированного пользовательского события.

class APIKeyCheck(BaseModel):
    is_valid: bool # Валидность API-ключа
    project_id: Optional[str] = None  # Идентификатор проекта
