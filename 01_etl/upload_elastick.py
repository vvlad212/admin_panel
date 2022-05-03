import os
import psycopg2
import logging
from logging import config as logger_conf
from log_config import log_conf

from postgre_operations import Postgre_operations
from queries import get_all_query
from models import FilmWorkPersonGenre
from elastic_operations import Elastic
from state import State, JsonFileStorage

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


class ETL:
    """
    Класс загрузки из PS в ES.
    """

    def __init__(self, page_size=100):
        self.ps = Postgre_operations()
        self.es = Elastic()
        self.state = State(JsonFileStorage(os.environ.get('STATE_FILE')))
        self.pg_conn = self.ps.postgre_connection()
        self.es_client = self.es.elastick_connection(os.getenv('ES_URL'))
        self.current_state = '1990-01-01'
        self.page_size = page_size

    def to_es_from_postgre(self):
        """Основной метод загрузки данных из elastic в Postgres."""

        if os.path.exists(self.state.storage.file_path):
            self.current_state = self.state.get_state('modified')

        ps_exec_cursor = self.ps.get_data_to_postgre(self.pg_conn,
                                                get_all_query(
                                                    self.current_state))

        while True:
            data = ps_exec_cursor.fetchmany(self.page_size)
            if not data:
                logger.info("Data in elastic is updated")
                break
            # TODO брать модель
            modified = str(data[-1]['modified'])
            bulk = []
            [
                [
                    bulk.append(r)
                    for r in
                    self.es.create_body(FilmWorkPersonGenre(**row).__dict__)
                ]
                for row in data
            ]

            es_resp = self.es.upload_to_elastic(bulk)
            if not es_resp['errors']:
                self.state.set_state('modified', modified)
            else:
                logger.error('Write error in ES', es_resp)
                self.state.set_state('modified', modified)


if __name__ == '__main__':
    page_size = 100
    try:
        # postgre_connection = Postgre_operations
        # es_client = elastick_connection(os.environ.get('ES_URL'))


        etl = ETL(page_size)
        etl.to_es_from_postgre()
        pass


    except psycopg2.OperationalError:
        logger.exception('Ошибка подключения к БД Postgres')
