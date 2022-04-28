"""
Класс моделей данных.

Описывает структуру таблиц
"""

import uuid
from dataclasses import dataclass, field
import datetime
from pydantic import BaseModel


class FilmWorkPersonGenre(BaseModel):
    """Класс описывающий структуру таблицы Film_work."""

    def __int__(self):

        imdb_rating: 0.0
        id: uuid.uuid4
        writers_names : []

    genre: list
    title: str
    description: str
    director: list
    actors_names: list
    writers_names: list
    actors: list[dict]
    writers: list[dict]

    id: uuid.UUID  # = field(default_factory=uuid.uuid4)
    imdb_rating: float  # = field(default=0.0)
