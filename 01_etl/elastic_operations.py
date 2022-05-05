import logging
from logging import config as logger_conf

from elasticsearch import Elasticsearch, ElasticsearchException

from backoff import backoff
from log_config import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


class Elastic:
    """Класс, содержащий методы для операций с Elastic."""

    def __init__(self, es_url: str = 'localhost:9200'):
        self.es_url = es_url
        self.client = self.elastic_connection()

    def upload_to_elastic(self, bulk_list: list):
        """Выполнение bulk запроса к Elastic.

        Returns:
            dict:
        """
        try:
            return self.client.bulk(body=bulk_list)

        except ElasticsearchException:
            self.client = self.elastic_connection()

    @backoff()
    def elastic_connection(self) -> Elasticsearch:
        """Подключение к Elastic.

        Returns:
            Elasticsearch:
        """
        client = Elasticsearch(self.es_url)
        if client.ping():
            logger.info("ES connection OK")
        else:
            raise ElasticsearchException
        return client


