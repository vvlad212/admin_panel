"""
Класс моделей данных.

Описывает структуру таблиц
"""

import uuid
from pydantic import BaseModel


class FilmWorkPersonGenre(BaseModel):
    """Класс описывающий структуру таблицы Film_work."""

    genre: list = []
    title: str
    description: str = None
    director: list[str]
    actors_names: list = None
    writers_names: list = None
    actors: list[dict] = None
    writers: list[dict] = None
    id: uuid.UUID
    imdb_rating: float = None
