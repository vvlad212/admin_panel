import logging
from logging import config as logger_conf

from elasticsearch import Elasticsearch, ElasticsearchException

from backoff import backoff
from log_config import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


class Elastic:
    """Класс, содержащий методы для операций с Elastic."""

    def __init__(self, es_url='localhost:9200'):
        self.es_url = es_url
        self.client = self.elastick_connection()

    @backoff()
    def upload_to_elastic(self, bulk_list: list):
        """Выполнение bulk запроса к Elastic.

            Returns:
                dict:
        """
        try:
            return self.client.bulk(body=bulk_list)

        except ElasticsearchException:
            self.elastick_connection()

    @backoff()
    def elastick_connection(self):
        """Подключение к Elastic.

            Returns:
                Elasticsearch:
        """
        self.client = Elasticsearch(self.es_url)

        return self.client
        #if es_conn.ping():

