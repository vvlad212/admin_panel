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

    #type: str
    #created: str
    #modified: datetime.timezone.utc
    genre: dict
    title: str
    description: str

    actors_names: dict
    writers_names: dict
    actors: list[dict]
    writers: list[dict]

    director: list = field(default_factory=list.append)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    imdb_rating: float = field(default=0.0)

