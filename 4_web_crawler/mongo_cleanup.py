#!/usr/bin/env python3
"""
Script to remove all web crawler data from MongoDB without interactive confirmation
"""

import logging
from pymongo import MongoClient
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("mongo_cleanup")

def cleanup_mongodb():
    """Remove all web crawler data from MongoDB"""
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient("mongodb://localhost:27017/")
        
        # Access crawler database
        db = client["crawler"]
        
        # List and drop all collections
        collections = db.list_collection_names()
        
        if not collections:
            logger.info("No collections found in the crawler database")
        else:
            logger.info(f"Found {len(collections)} collections to drop: {collections}")
            
            for collection in collections:
                logger.info(f"Dropping collection: {collection}")
                db[collection].drop()
                
            logger.info("All crawler collections dropped successfully")
        
        # Optionally drop the entire database
        logger.info("Dropping entire crawler database")
        client.drop_database("crawler")
        
        # Check for any URLs collection in other databases that might be related
        all_dbs = client.list_database_names()
        for db_name in all_dbs:
            if db_name in ['admin', 'config', 'local']:
                continue
                
            db = client[db_name]
            if 'urls' in db.list_collection_names() or 'pages' in db.list_collection_names():
                logger.info(f"Found crawler-related collections in database: {db_name}")
                
                # Ask for confirmation before dropping collections in other databases
                for collection in ['urls', 'pages', 'domains', 'stats']:
                    if collection in db.list_collection_names():
                        logger.info(f"Dropping collection {db_name}.{collection}")
                        db[collection].drop()
        
        logger.info("MongoDB cleanup completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up MongoDB: {e}")
        return False

if __name__ == "__main__":
    print("MongoDB Crawler Data Cleanup")
    print("--------------------------")
    print("This script will remove all web crawler collections from MongoDB")
    print()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--force':
        # Non-interactive mode for scripting
        success = cleanup_mongodb()
        sys.exit(0 if success else 1)
    else:
        # Interactive mode
        proceed = input("Do you want to proceed with MongoDB cleanup? (y/n): ")
        if proceed.lower() != 'y':
            print("Cleanup cancelled")
            sys.exit(0)
            
        success = cleanup_mongodb()
        print(f"\nMongoDB cleanup: {'Completed' if success else 'Failed'}") 