import datetime
import os
import psycopg2
import logging
from elasticsearch import client
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from logging import config as logger_conf
from log_config import log_conf

from postgre_operations import get_data_to_postgre, dsl
from queries import get_all
from models import FilmWorkPersonGenre
from elastick_operations import create_body, upload_to_elastick, elastick_connection
import state

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def to_es_from_postgre(pg_conn: _connection, page_size: int, es_client: client, state: state.State):
    """Основной метод загрузки данных из SQLite в Postgres."""

    current_state = datetime.datetime.now().isoformat()
    if os.path.exists(state.file_path):
        current_state = state.get_state('modified')

    ps_exec_cursor = get_data_to_postgre(pg_conn, get_all(current_state))

    while True:
        data = ps_exec_cursor.fetchmany(page_size)
        if not data:
            break
        bulk = []

        modified = data[0]['modified']
        [[bulk.append(r) for r in create_body(FilmWorkPersonGenre(**row).__dict__)] for row in data]

        es_resp = upload_to_elastick(es_client, bulk)
        state = state.set_state('modified', modified)
        pass


if __name__ == '__main__':
    page_size = 10000
    try:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn, elastick_connection(
                os.environ.get('ES_URL')) as es_client:
            state = state.State(state.BaseStorage)
            state.file_path = '/Users/vladislavzujkov/PycharmProjects/Yandex_learn/test.json'
            to_es_from_postgre(pg_conn, page_size, es_client, state)



    except psycopg2.OperationalError:
        logger.exception('Ошибка подключения к БД Postgres')
