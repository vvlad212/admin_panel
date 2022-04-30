import os
import psycopg2
import logging
from elasticsearch import client
from psycopg2.extensions import connection as _connection
from logging import config as logger_conf
from log_config import log_conf

from postgre_operations import get_data_to_postgre, postgre_connection
from queries import get_all
from models import FilmWorkPersonGenre
from elastic_operations import create_body, upload_to_elastic, elastick_connection
from state import State, JsonFileStorage

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def to_es_from_postgre(pg_conn: _connection, page_size: int, es_client: client, state: State):
    """Основной метод загрузки данных из elastic в Postgres."""

    if os.path.exists(state.storage.file_path):
        current_state = state.get_state('modified')
    else:
        current_state = '1990-01-01'

    ps_exec_cursor = get_data_to_postgre(pg_conn, get_all(current_state))

    while True:
        data = ps_exec_cursor.fetchmany(page_size)
        if not data:
            logger.info("Data in elastic is updated")
            break
        # TODO брать модель
        modified = str(data[-1]['modified'])
        bulk = []
        [[bulk.append(r) for r in create_body(FilmWorkPersonGenre(**row).__dict__)] for row in data]

        es_resp = upload_to_elastic(es_client, bulk)
        if not es_resp['errors']:
            state.set_state('modified', modified)
        else:
            logger.error('Write error in ES', es_resp)
            state.set_state('modified', modified)


if __name__ == '__main__':
    page_size = 100
    try:
        postgre_connection = postgre_connection()
        es_client = elastick_connection(os.environ.get('ES_URL'))
        storage = JsonFileStorage(os.environ.get('STATE_FILE'))
        state = State(storage)

        to_es_from_postgre(postgre_connection, page_size, es_client, state)

    except psycopg2.OperationalError:
        logger.exception('Ошибка подключения к БД Postgres')
