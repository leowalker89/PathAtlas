import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from backend.database.mongodb_jobfocus import mongodb
from bson import ObjectId

# List of ObjectIds for the specific job listings we want to copy
TARGET_IDS = [
    "677c8cf322f97cda28296db4",  # Microsoft
    "677c51ec22f97cda28296c6f",  # Mistral AI
    "67905e1622f97cda2829ff7d",  # Apple
    "67916c2c22f97cda282a0278",  # Figma
    "67916c3422f97cda282a027b",  # Khan Academy
]

def create_test_listings():
    """
    Create a test collection by copying specific job listings from the main collection.
    """
    source_collection = mongodb.get_collection('jobs_db', 'job_listings')
    test_collection = mongodb.get_collection('jobs_db', 'test_job_listings')
    
    # Drop existing test collection if it exists
    test_collection.drop()
    
    # Query the source collection for our specific documents
    query = {"_id": {"$in": [ObjectId(id_) for id_ in TARGET_IDS]}}
    documents = source_collection.find(query)
    
    # Insert the documents into the test collection
    if documents:
        test_collection.insert_many(list(documents))
    
    # Verify the insertion
    count = test_collection.count_documents({})
    print(f"Successfully created test collection with {count} documents")
    
    # Create an index on created_at for efficient sorting
    test_collection.create_index([("created_at", -1)])
    print("Created index on created_at field")

if __name__ == "__main__":
    create_test_listings()
