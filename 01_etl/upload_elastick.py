import psycopg2
import logging

from functools import wraps
from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_batch, DictCursor
from logging import config as logger_conf
from log_config import log_conf
from postgre_operations import get_data_to_postgre

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            ...

        return inner

    return func_wrapper


dsn = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
}


def to_es_from_postgre(pg_conn: _connection, page_size: int):
    """Основной метод загрузки данных из SQLite в Postgres."""

    ps_exec_cursor = get_data_to_postgre(pg_conn, 'SELECT * FROM content.film_work')

    while True:
        data = ps_exec_cursor.fetchmany(page_size)
        if not data:
            break
        pass


if __name__ == '__main__':
    page_size = 100
    try:
        with psycopg2.connect(**dsn,cursor_factory=DictCursor) as pg_conn:
            to_es_from_postgre(pg_conn, page_size)



    except psycopg2.OperationalError:
        logger.exception('Ошибка подключения к БД Postgres')
