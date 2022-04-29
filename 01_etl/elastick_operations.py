from contextlib import contextmanager

from elasticsearch import client, Elasticsearch


def create_body(model_row: dict):
    """Создание bulk строки для одного документа."""
    #model_row.pop['modified']
    row = {
        "index": {
            "_index": "movies",
            "_id": f"{model_row['id']}"
        }
    }
    return row, model_row


def upload_to_elastick(es_client: client, bulk_list: list):
    """Выполнение bulk запроса к elastick."""

    if es_client.ping:
        resp = es_client.bulk(body=bulk_list)
        return resp
    else:
        # упал эластик
        pass


@contextmanager
def elastick_connection(es_url: str):
    """Подключение к elastick"""
    client = Elasticsearch(es_url)
    yield client
    client.close()
