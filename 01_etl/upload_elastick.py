import logging
import os
from logging import config as logger_conf

import psycopg2
from dotenv import load_dotenv

from elastic_operations import Elastic
from log_config import log_conf
from models import FilmWorkPersonGenre
from postgre_operations import Postgres_operations
from queries import get_all_query
from state import JsonFileStorage, State

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)
load_dotenv()


class ETL:
    """
    Класс реализующий работу ETL пайп лайна.
    """

    def __init__(
            self,
            state_file_path: str,
            elastic_connection_url: str,
            pg_dsl: dict,
            query_page_size=100
    ):

        self.ps_exec_cursor = None
        self.ps = Postgres_operations(pg_dsl)
        self.es = Elastic()
        self.state = State(JsonFileStorage(state_file_path))
        self.pg_conn = self.ps.postgres_connection()
        self.es_client = self.es.elastick_connection(elastic_connection_url)
        self.current_state = '1990-01-01'
        self.page_size = query_page_size
        self.query_models = FilmWorkPersonGenre

    def run(self):
        data = self.extract_from_postgres()
        for i in iter(data):
            bulk, modified = self.transform_data(i)
            self.load_to_elastic(bulk, modified)

    def extract_from_postgres(self):
        """Метод получения данных из Postgres."""

        if os.path.exists(self.state.storage.file_path):
            self.current_state = self.state.get_state('modified')

        data_cursor = self.ps.get_data_from_postgres(self.pg_conn, get_all_query(self.current_state))
        while True:
            data = data_cursor.fetchmany(self.page_size)
            if not data:
                logger.info("Data in elastic is updated")
                break
            yield data

    def transform_data(self, data):
        """Подготовка данных для вставки в Elasctic."""

        modified = str(data[-1]['modified'])
        bulk = []

        for row in data:
            model_row = self.query_models(**row).__dict__
            bulk.append(
                {
                    "index": {
                        "_index": "movies",
                        "_id": f"{model_row.pop('id')}"
                    }
                }
            )
            bulk.append(model_row)
        return bulk, modified
        # self.load_to_elastic(bulk, modified)

    def load_to_elastic(self, bulk: list, modified: str):
        """Загрузка подготовленных данных в Elastic."""

        es_resp = self.es.upload_to_elastic(bulk, self.es_client)
        if not es_resp['errors']:
            self.state.set_state('modified', modified)
        else:
            logger.error('Write error in ES', es_resp)


if __name__ == '__main__':
    page_size = 10
    state_file = os.environ.get('STATE_FILE')
    elastic_url = os.getenv('ES_URL')
    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
    }
    try:
        etl = ETL(
            query_page_size=page_size,
            state_file_path=state_file,
            elastic_connection_url=elastic_url,
            pg_dsl=dsl
        )
        etl.run()

    except psycopg2.OperationalError:
        logger.exception('Ошибка подключения к БД Postgres')
