import logging

from elasticsearch import client, Elasticsearch
from backoff import backoff
from logging import config as logger_conf
from log_config import log_conf

logger_conf.dictConfig(log_conf)
logger = logging.getLogger(__name__)


def create_body(model_row: dict):
    """Создание bulk строки для одного документа."""

    row = {
        "index": {
            "_index": "movies",
            "_id": f"{model_row['id']}"
        }
    }
    return row, model_row


@backoff()
def upload_to_elastic(es_client: client, bulk_list: list):
    """Выполнение bulk запроса к elastick."""

    if es_client.ping():
        resp = es_client.bulk(body=bulk_list)
        return resp
    else:
        # упал эластик
        pass


@backoff()
def elastick_connection(es_url: str):
    """Подключение к elastick"""

    client = Elasticsearch(es_url)
    if client.ping():
        return client
    else:
        logger.error("Error ES connect")
        raise ValueError("Error ES connect", es_url, client)
