import psycopg2
import logging

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from logging import config as logger_conf
from log_config import log_conf
from postgre_operations import get_data_to_postgre,dsl
from queries import get_all_data_from_postgre as get_all
from models import FilmWorkPersonGenre

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def to_es_from_postgre(pg_conn: _connection, page_size: int):
    """Основной метод загрузки данных из SQLite в Postgres."""

    ps_exec_cursor = get_data_to_postgre(pg_conn, get_all)

    while True:
        data = ps_exec_cursor.fetchmany(page_size)
        if not data:
            break
        insert_pack = [(FilmWorkPersonGenre(**row))
                       for row in data]
        for row in insert_pack:
            pass
        pass

if __name__ == '__main__':
    page_size = 10
    try:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            to_es_from_postgre(pg_conn, page_size)



    except psycopg2.OperationalError:
        logger.exception('Ошибка подключения к БД Postgres')
