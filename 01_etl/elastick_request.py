resp = client.bulk(
    body=[
        {"index": {"_index": "test", "_id": "1"}},
        {"field1": "value1"},
        {"delete": {"_index": "test", "_id": "2"}},
        {"create": {"_index": "test", "_id": "3"}},
        {"field1": "value3"},
        {"update": {"_id": "1", "_index": "test"}},
        {"doc": {"field2": "value2"}},
    ],
)
print(resp)