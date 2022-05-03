import logging
from logging import config as logger_conf

from elasticsearch import Elasticsearch

from backoff import backoff
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
        return client.bulk(body=bulk_list)

    @backoff()
    def elastick_connection(self, es_url: str):
        """Подключение к Elastic.

            Returns:
                Elasticsearch:
        """

        return Elasticsearch(es_url)
