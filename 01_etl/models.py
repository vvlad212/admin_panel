"""
Класс моделей данных.

Описывает структуру таблиц
"""

import uuid

from pydantic import BaseModel
from pydantic.class_validators import Optional


class FilmWorkPersonGenre(BaseModel):
    """Класс описывающий структуру таблицы Film_work."""

    genre: list = []
    title: str
    #  TODO: all none:
    description: Optional[str] = None
    director: list[str]
    actors_names: Optional[list] = None
    writers_names: Optional[list] = None
    actors: Optional[list[dict]] = None
    writers: Optional[list[dict]] = None
    id: uuid.UUID
    imdb_rating: Optional[float] = None
