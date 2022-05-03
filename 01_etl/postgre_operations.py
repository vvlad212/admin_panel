import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
import logging

from psycopg2.extras import DictCursor
from backoff import backoff

logger = logging.getLogger(__name__)
load_dotenv()


class Postgres_operations:
    """Класс, содержащий методы для работы с Postgres."""

    def __init__(self, dsl: [str, str]):
        self.dsl = dsl

    @backoff()
    def postgres_cursor(self, connection: _connection):
        """Получение курсора для Postgres.

        Returns:
            connection.cursor:
        """
        return connection.cursor()

    @backoff()
    def get_data_from_postgres(self, pg_conn: _connection, query: str):
        """Получение данных из БД Postgres.

        Returns:
            connection.cursor:
        """
        try:
            pg_cursor = self.postgres_cursor(pg_conn)
            pg_cursor.execute(query)
            return pg_cursor
        except psycopg2.Error:
            logger.exception('Ошибка при получении данных из Postgres')

    @backoff()
    def postgres_connection(self):
        """Подключение к БД Postgres.

        Returns:
            connection:
        """
        with psycopg2.connect(**self.dsl,
                              cursor_factory=DictCursor) as self.connection:
            return self.connection
