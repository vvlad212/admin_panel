import logging

from elasticsearch import Elasticsearch
from backoff import backoff
from logging import config as logger_conf
from log_config import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


class Elastic:
    """Класс, содержащий методы для операций с Elastic."""

    def __init__(self, es_url='localhost:9200'):
        self.es_url = es_url

    @backoff()
    def upload_to_elastic(self, bulk_list: list, client):
        """Выполнение bulk запроса к Elastic.

            Returns:
                dict:
        """
        if client.ping():
            resp = client.bulk(body=bulk_list)
            return resp

    @backoff()
    def elastick_connection(self, es_url: str):
        """Подключение к Elastic.

            Returns:
                Elasticsearch:
        """

        return Elasticsearch(es_url)

    def create_body(self, model_row: dict):
        """Создание bulk строки для одного документа.

            Returns:
                tuple(dict,dict):
        """

        row = {
            "index": {
                "_index": "movies",
                "_id": f"{model_row['id']}"
            }
        }
        return row, model_row
