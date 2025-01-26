import os
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from logger_config import logger


class MongoConnector:
    mongo_url = f"{os.getenv("MONGO_URL")}?authSource=admin"
    url_collection_name = "urls"
    db_name = "crawler_db"

    def __init__(self):
        """
        Initialize MongoConnector with a persistent MongoDB client.
        """
        self.client = None
        self.db = None

        try:
            self.client = MongoClient(self.mongo_url)
            self.db = self.client[self.db_name]
            logger.debug("MongoDB connection established.")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def save_url_to_db(self, url: str, page_file_path: str, last_modified_date: datetime):
        """
        Saves the fetched url to a MongoDB database.
        """
        collection = self.db[self.url_collection_name]
        document = {"url": url, "htmlPath": page_file_path, "last_modified": last_modified_date}
        result = collection.insert_one(document)
        logger.debug(f"Inserted URL with ID: {result.inserted_id}")

    def close(self):
        """
        Close the MongoDB connection.
        """
        if self.client:
            self.client.close()
            logger.debug("MongoDB connection closed.")
