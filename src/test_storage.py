from minio import Minio
from pymongo import MongoClient

def test_minio():
    print("--- TESTING MINIO ---")
    try:
        # Connect to MinIO exposed on localhost:9000
        client = Minio(
            "localhost:9000",
            access_key="admin",
            secret_key="password123",
            secure=False # Set to False because we are using HTTP, not HTTPS
        )
        
        # Test creating the raw data bucket defined in Phase 1
        bucket_name = "airtraffic-raw"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
            
        print("MinIO connection: SUCCESS")
    except Exception as e:
        print(f"MinIO connection failed: {e}")

def test_mongodb():
    print("\n--- TESTING MONGODB ---")
    try:
        # Connect to MongoDB exposed on localhost:27017
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        
        # Trigger a server ping to validate connection
        client.admin.command('ping')
        
        # Connect to a test database and collection
        db = client["airtraffic_db"]
        collection = db["tma_boundaries"]
        
        # Insert a test document
        test_doc = {"name": "Test TMA", "type": "Polygon"}
        result = collection.insert_one(test_doc)
        print(f"Inserted document with ID: {result.inserted_id}")
        
        # Retrieve the document
        doc = collection.find_one({"name": "Test TMA"})
        print(f"Retrieved document successfully: {doc['name']}")
        
        # Clean up by deleting the test document
        collection.delete_one({"name": "Test TMA"})
        
        print("MongoDB connection: SUCCESS")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")

if __name__ == "__main__":
    test_minio()
    test_mongodb()