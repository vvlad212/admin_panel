from elasticsearch import Elasticsearch

# Create the client instance
client = Elasticsearch("http://127.0.0.1:9200")

# Successful response!
a = client.info()

resp = client.bulk(
    body=[
        {"index": {"_index": "movies", "_id": "1"}},
        {
            "imdb_rating": "5",
            "genre": 3,
            "title": 'test_title',
            "description": 'test_description',
            "director": 'director1 director2',
            "actors_names": 'actors_name1 actors_name2',
            "writers_names": 'test_description',
            "actors": [
                {
                    'id': 'test_actors_id1',
                    'name': 'test_actors_name1'
                },
                {
                    'id': 'test_actors_id2',
                    'name': 'test_actors_name2'
                }
            ],
            "writers": [
                {
                    'id': 'test_writers_id1',
                    'name': 'test_writers_name1'
                },
                {
                    'id': 'test_writers_id2',
                    'name': 'test_writers_name2'
                }
            ],
        }
    ],
)



pass
