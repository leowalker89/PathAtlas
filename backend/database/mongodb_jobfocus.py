"""
MongoDB connection and collection management module.
Provides singleton connection manager and collection access functions.
"""

from typing import Optional
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.database import Database
from dotenv import load_dotenv
import os
from logfire import Logfire

# Initialize logging
logger = Logfire()

class MongoDB:
    """
    MongoDB connection manager implementing singleton pattern.
    
    Manages database connection and provides access to collections.
    Ensures single database connection is maintained throughout the application.
    """
    _instance: Optional['MongoDB'] = None
    _client: Optional[MongoClient] = None
    
    def __new__(cls):
        """Singleton pattern to ensure single database connection"""
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize MongoDB connection if not already initialized"""
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """
        Establish connection to MongoDB using credentials from environment variables.
        """
        load_dotenv()
        
        uri = os.getenv('MONGODB_URI')
        if not uri:
            raise ValueError("MongoDB URI not found in environment variables")
        
        # Debug log to see what URI we're using (with masked password)
        masked_uri = uri.replace(os.getenv('MONGODB_PASSWORD', ''), '***')
        logger.info(f"Attempting to connect with URI: {masked_uri}")
        
        try:
            self._client = MongoClient(uri, 
                                     server_api=ServerApi('1'),
                                     serverSelectionTimeoutMS=5000)  # 5 second timeout
            # Test connection
            self._client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            raise
    
    def get_collection(self, db_name: str, collection_name: str) -> Collection:
        """
        Get a MongoDB collection.
        
        Args:
            db_name (str): Name of the database
            collection_name (str): Name of the collection
            
        Returns:
            Collection: MongoDB collection object
            
        Raises:
            ConnectionError: If MongoDB connection cannot be established
        """
        if not self._client:
            self._connect()
            
        if not self._client:  # If still None after connection attempt
            raise ConnectionError("Failed to establish MongoDB connection")
            
        db: Database = self._client.get_database(db_name)
        return db.get_collection(collection_name)
    
    def close(self):
        """Close MongoDB connection and cleanup resources."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB connection closed")

# Create a global instance
mongodb = MongoDB()

def get_jobs_collection() -> Collection:
    """
    Get the jobs collection from MongoDB.
    
    Returns:
        Collection: MongoDB collection for storing job listings
    """
    return mongodb.get_collection('jobs_db', 'job_listings')

def get_searches_collection() -> Collection:
    """
    Get the searches collection from MongoDB.
    
    Returns:
        Collection: MongoDB collection for storing job search metadata
    """
    return mongodb.get_collection('jobs_db', 'job_searches')