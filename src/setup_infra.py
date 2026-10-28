"""Phase 1 setup: create MinIO buckets and MongoDB collections/indexes (idempotent)."""
from minio import Minio
from pymongo import MongoClient, GEOSPHERE

BUCKETS = ["airtraffic-raw", "airtraffic-processed"]
COLLECTIONS = ["tma_boundaries", "flight_trajectories", "holding_events"]


def setup_minio():
    client = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)
    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"MinIO: created bucket '{bucket}'")
        else:
            print(f"MinIO: bucket '{bucket}' already exists")


def setup_mongodb():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
    db = client["airtraffic_db"]
    existing = db.list_collection_names()
    for name in COLLECTIONS:
        if name not in existing:
            db.create_collection(name)
            print(f"MongoDB: created collection '{name}'")
        else:
            print(f"MongoDB: collection '{name}' already exists")
    # 2dsphere index on TMA boundary geometries (GeoJSON stored in 'geometry')
    db["tma_boundaries"].create_index([("geometry", GEOSPHERE)])
    print("MongoDB: 2dsphere index ensured on tma_boundaries.geometry")


if __name__ == "__main__":
    setup_minio()
    setup_mongodb()

