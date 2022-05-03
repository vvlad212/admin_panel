import logging

from elasticsearch import client, Elasticsearch
from backoff import backoff
from logging import config as logger_conf
from log_config import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)

class Elastic:
    def __init__(self, es_url = 'localhost:9200'):
        self.es_url = es_url

    def create_body(self, model_row: dict):
        """Создание bulk строки для одного документа."""

        row = {
            "index": {
                "_index": "movies",
                "_id": f"{model_row['id']}"
            }
        }
        return row, model_row


    @backoff()
    def upload_to_elastic(self, bulk_list: list):
        """Выполнение bulk запроса к elastick."""

        if self.client.ping():
            resp = self.client.bulk(body=bulk_list)
            return resp


    @backoff()
    def elastick_connection(self, es_url: str):
        """Подключение к elastick"""

        self.client = Elasticsearch(es_url)
        if self.client.ping():
            return self.client
        else:
            logger.error("Error ES connect")
            raise ValueError("Error ES connect", es_url, self.client)
