import logging
from typing import Optional, Dict

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from backoff import backoff

logger = logging.getLogger(__name__)


class PostgresOperations:
    """Класс, содержащий методы для работы с Postgres."""

    conn: Optional[_connection] = None

    def __init__(self, dsl: Dict[str, str]):
        self.dsl = dsl

    @backoff()
    def get_ps_cursor(self):
        """Получение курсора для Postgres.

        Returns:
            connection.cursor:
        """
        self.conn = self.postgres_connection()
        return self.conn.cursor()

    @backoff()
    def get_data_cursor(self, query: str):
        """Получение данных из БД Postgres.

        Returns:
            connection.cursor:
        """
        try:
            pg_cursor = self.get_ps_cursor()
            pg_cursor.execute(query)
            return pg_cursor
        except psycopg2.Error:
            logger.exception('Error getting data from Postgres')

    @backoff()
    def postgres_connection(self):
        """Подключение к БД Postgres.

        Returns:
            connection:
        """
        with psycopg2.connect(**self.dsl,
                              cursor_factory=DictCursor) as connection:
            return connection
