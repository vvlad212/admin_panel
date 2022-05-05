import logging

from elasticsearch import Elasticsearch, ElasticsearchException

from backoff import backoff

logger = logging.getLogger(__name__)


class Elastic:
    """Класс, содержащий методы для операций с Elastic."""

    def __init__(self, es_url: str = 'localhost:9200'):
        self.es_url = es_url
        self.client = self.elastic_connection()

    def upload_to_elastic(self, bulk_list: list) -> dict:
        """Выполнение bulk запроса к Elastic.

        Args:
            bulk_list:

        Returns:
            dict
        """
        try:
            return self.client.bulk(body=bulk_list)

        except ElasticsearchException:
            logger.error('Lost connection to Elastic')
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
