import logging
import os
import time
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
            query_page_size=100,
            update_freq=10
    ):
        self.update_freq = update_freq
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
        """Запуск бесконечного ETL процесса."""
        while True:
            get_state = self.state.get_state('modified')
            if get_state is not None:
                self.current_state = get_state

            for page in iter(self.extract_from_postgres()):
                bulk, modified = self.transform_data(page)
                self.load_to_elastic(bulk, modified)
            time.sleep(self.update_freq)

    def extract_from_postgres(self):
        """Метод получения данных из Postgres."""

        data_cursor = self.ps.get_data_from_postgres(self.pg_conn, get_all_query(self.current_state))
        while True:
            data = data_cursor.fetchmany(self.page_size)
            if not data:
                logger.info("Data in elastic is updated")
                break
            yield data

    def transform_data(self, data):
        """Подготовка данных для вставки в Elastic."""

        modified = str(data[-1]['modified'])
        bulk = []

        for row in data:
            model_row = self.query_models(**row)
            bulk.append(
                {
                    "index": {
                        "_index": "movies",
                        "_id": f"{model_row.id}"
                    }
                }
            )
            bulk.append(model_row.__dict__)
        return bulk, modified

    def load_to_elastic(self, bulk: list, modified: str):
        """Загрузка подготовленных данных в Elastic."""

        es_resp = self.es.upload_to_elastic(bulk, self.es_client)
        if not es_resp['errors']:
            self.state.set_state('modified', modified)
        else:
            logger.error('Write error in ES', es_resp)


if __name__ == '__main__':
    page_size = 100
    state_file = os.environ.get('STATE_FILE')
    elastic_url = os.getenv('ES_URL')
    update_frequency = 5
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
            pg_dsl=dsl,
            update_freq=update_frequency
        )

        etl.run()

    except Exception as ex:
        logger.exception(ex)
