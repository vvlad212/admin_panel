"""
Класс моделей данных.

Описывает структуру таблиц
"""

import uuid

from pydantic import BaseModel
from pydantic.class_validators import Optional


class FilmWorkPersonGenre(BaseModel):
    """Класс описывающий структуру таблицы Film_work."""

    genre: Optional[list] = None
    title: Optional[str] = None
    description: Optional[str] = None
    director: Optional[list] = None
    actors_names: Optional[list] = None
    writers_names: Optional[list] = None
    actors: Optional[list[dict]] = None
    writers: Optional[list[dict]] = None
    imdb_rating: Optional[float] = None
    id: uuid.UUID
