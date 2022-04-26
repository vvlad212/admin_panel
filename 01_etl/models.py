"""
Класс моделей данных.

Описывает структуру таблиц
"""

import uuid
from dataclasses import dataclass, field
import datetime

@dataclass
class FilmWorkPersonGenre:
    """Класс описывающий структуру таблицы Film_work."""
    type: str
    created: str
    modified: datetime.timezone.utc
    genres: dict
    title: str
    description: str
    director: dict
    actors_names: dict
    writers_names: dict
    actors: list[dict]
    writers: list[dict]

    id: uuid.UUID = field(default_factory=uuid.uuid4)
    rating: float = field(default=0.0)

