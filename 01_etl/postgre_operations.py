import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
import os
import logging

logger = logging.getLogger(__name__)
load_dotenv()
dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
}


def postgre_cursor(connection: _connection):
    """Получение курсора для Postgres.

    Returns:
        connection.cursor:
    """
    return connection.cursor()


def get_data_to_postgre(pg_conn: _connection, query: str):
    """Вставка данных в БД postgre.

    Вставка осуществляется пачками, размер определяется параметром page_size
    """
    try:
        pg_cursor = postgre_cursor(pg_conn)
        pg_cursor.execute(query)
        return pg_cursor
    except psycopg2.Error:
        logger.exception('Ошибка при получении данных из postgre')
        return 'false'
