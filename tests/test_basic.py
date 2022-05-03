from fastapi.testclient import TestClient

from main import app

c = TestClient(app)

def test_bucket_list():
    r = c.post("/buckets/newbucket1", json={"name": "newbucket1", "shape": [1, 6]})
    assert r.status_code == 201
    assert r.json() == {}
    r = c.get("/buckets/newbucket1")
    assert r.status_code == 200
    assert r.json() == {"name": "newbucket1", 'count': 0, 'shape': [1, 6]}
    r = c.delete("/buckets/newbucket1")
    assert r.status_code == 200
    # r = c.get("/newbucket1")
    # assert r.status_code == 404

def test_calculate():
    r1 = c.post("/calculate", json={"v1": [4.3, 4.5, 666], "v2": [4.3, 4.5, 665], "metric": "cosine"})
    assert r1.status_code == 201
    r2 = c.post("/calculate", json={"v1": [4.3, 4.5, 666], "v2": [4.3, 4.5, 666], "metric": "cosine"})
    assert r1.status_code == 201
    assert r1.json()['value'] > r2.json()['value']


def test_similiar_verctors():
    r = c.post("/buckets/x", json={"name": "x", "shape": [3]})
    assert r.status_code == 201
    c.put("/buckets/x", json={'vectors':[
                                  {"name": "v1", "v": [1, 2, 3]},
                                  {"name": "v2", "v": [1, 1, 1]},
          {"name": "v3", "v": [2, 2, 2]}]})


    r = c.get("/buckets/x/v1/similar")
    assert r.status_code == 200
    vectors = r.json()
    assert vectors == {'vectors': [[1, 2, 3], [1, 1, 1], [2, 2, 2]]}

    r = c.get("/buckets/x/v1/similar?distance_max=0")
    assert r.status_code == 200
    vectors = r.json()
    assert vectors == {'vectors': [[1, 2, 3]]}

    r = c.get("/buckets/x/v1/similar?distance_max=34")
    assert r.status_code == 200
    vectors = r.json()
    assert vectors == {'vectors': [[1, 2, 3], [1, 1, 1], [2, 2, 2]]}




