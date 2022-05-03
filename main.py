from http.client import HTTPException
import scipy
from scipy import spatial

from fastapi import FastAPI
from fastapi import Header, APIRouter

from typing import List, Dict, Any, Optional

app = FastAPI()


class BucketStorage:
    def __init__(self):
        self.__buckets = {}

    def get(self, name):
        return self.__buckets[name]

    def remove(self, name):
        del self.__buckets[name]

    def add(self, bucket):
        self.__buckets[bucket.name] = bucket


class Bucket:
    def __init__(self, name, shape):
        self.name = name
        self.shape = shape
        self.__vectors = {}

    def add(self, name, vector):
        self.__vectors[name] = vector

    def get(self, name):
        return self.__vectors[name]

    def all(self, sort_by_similiarity_to_name, distance_metric="cosine", distance_max=None):
        ## fixme bruteforce
        vectors = self.__vectors.values()
        similiar_to = self.__vectors[sort_by_similiarity_to_name]
        if distance_metric=="cosine":
            sort_key = lambda x: spatial.distance.cosine(x, similiar_to)
        elif distance_metric=="euclidian":
            sort_key = lambda x: spatial.distance.euclidean(x, similiar_to)
        else:
            sort_key = None ## fixme probably error
        vectors = sorted(vectors, key=sort_key)

        if distance_max is not None:
            distance_max_float = float(distance_max)
            vectors = [i for i in filter(lambda x: sort_key(x) <= distance_max_float, vectors)]
        return vectors

    def get_count(self):
        return len(self.__vectors)

    def get_names(self, vectors):
        """
        non-efficiently seeks for vectors in storage
        #todo use second hash table if needed
        :param vectors: vectors to seek in vectors
        :return:
        """
        names = []
        for query_vector in vectors:
            for name, vector in self.__vectors.items():
                if query_vector == vector:
                    names.append(name)

        return names


STORAGE = BucketStorage()


# Create new bucket
# POST /bucket_name : {“shape”:[4,(1024*768)]}
@app.post('/buckets/{name}', status_code=201)
async def add_bucket(name: str, b: Dict[Any, Any]):
    STORAGE.add(Bucket(name, b['shape']))
    return {}


# Delete Bucket
# DELETE /bucket_name
@app.delete('/buckets/{name}')
async def delete_bucket(name: str):
    try:
        STORAGE.remove(name)
    except:
        raise HTTPException(404)



# Add Vectors
# PUT /bucket_name/ {vectors: [{“id”: “ID1”, v: [3, 4, 3.3]}] }
@app.put('/buckets/{name}')
async def add_vectors(name: str, json: Dict[Any, Any]):
    bucket = STORAGE.get(name)
    for vector in json['vectors']:
        bucket.add(vector['name'], vector['v'])
    return {}




# Get bucket info
# GET /bucket_name    {count: 44, name: "bucket_name", shape: []}
@app.get('/buckets/{name}')
async def get_bucket(name: str):
    try:
        bucket = STORAGE.get(name)
        return {
            'name': bucket.name,
            'shape': bucket.shape,
            'count': bucket.get_count()
        }
    except:
        raise HTTPException(404)


# Get count of the closest vectors
# POST /bucket_name/similar/count {query: [[500], [333]], distance_metric: "cosine", distance_max: 40}  -> {count: 22} . The valid distance metrics are “cosine” and “euclidean”.
@app.get('/buckets/{name}/{vector_id}/similar_count')
async def get_similar_vectors_count(name: str, vector_id: str, distance_metric="cosine", distance_max=40):
    bucket = STORAGE.get(name)
    vectors = bucket.all(vector_id, distance_metric, distance_max)
    return {'vectors_count': len(vectors)}

# Get the vectors within a specified distance to a given vector.
# POST /bucket_name/similar {query: [[500], [333]], distance_metric: "cosine", distance_max: 40} -> {vectors: [ { id: “ID1”.  [232, 323]  }. ] }
@app.get('/buckets/{name}/{vector_id}/similar')
async def get_similar_vectors(name: str, vector_id: str, distance_metric="cosine", distance_max=None):
    bucket = STORAGE.get(name)
    vectors = bucket.all(vector_id, distance_metric, distance_max)
    return {'vectors': vectors}


# Calculate distance between 2 vectors
# This doesn’t actually store or fetch any data. (for testing purposes)
# POST /calculate  {v1: [2, 4, 5], v2: [2, 2, 5]}
@app.post('/calculate', status_code=201)
async def calculate(json: Dict[Any, Any]):
    v1 = json['v1']
    v2 = json['v2']
    metric = json['metric']
    if metric == "cosine":
        return {'metric': metric, 'value': spatial.distance.cosine(v1, v2)}
    elif metric == "euclidian":
        return {'metric': metric, 'value': spatial.distance.euclidean(v1, v2)}
    else:
        raise HTTPException(404)



# Get IDs for Vectors
#     Pass vectors to the server, for each vector, if it exists, return the ID, if it does not exist, return null.
# POST /bucket_name/ids_for_vector/ {vectors: [[3, 4, 12], [1, 4]]}
# Get Vectors for IDs
#     POST /bucket_name/vectors {ids: [“id1”, “id2”, “id3”]}