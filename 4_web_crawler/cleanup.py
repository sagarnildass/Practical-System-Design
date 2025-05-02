#!/usr/bin/env python3
"""
Cleanup script to remove all web crawler data from MongoDB
and list files to be removed
"""

import os
import sys
import logging
import shutil
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("cleanup")

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
            
        # Optional: Drop the entire database
        # client.drop_database("crawler")
        # logger.info("Dropped entire crawler database")
            
        logger.info("MongoDB cleanup completed")
        
    except Exception as e:
        logger.error(f"Error cleaning up MongoDB: {e}")
        return False
        
    return True

def cleanup_files():
    """List and remove files related to simple_crawler"""
    try:
        crawler_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Files directly related to simple_crawler
        simple_crawler_files = [
            os.path.join(crawler_dir, "simple_crawler.py"),
            os.path.join(crawler_dir, "README_SIMPLE.md"),
            os.path.join(crawler_dir, "simple_crawler.log")
        ]
        
        # Check storage directories
        storage_dir = os.path.join(crawler_dir, "storage")
        if os.path.exists(storage_dir):
            logger.info(f"Will remove storage directory: {storage_dir}")
            simple_crawler_files.append(storage_dir)
            
        # List all files that will be removed
        logger.info("The following files will be removed:")
        for file_path in simple_crawler_files:
            if os.path.exists(file_path):
                logger.info(f"  - {file_path}")
            else:
                logger.info(f"  - {file_path} (not found)")
                
        # Confirm removal
        confirm = input("Do you want to proceed with removal? (y/n): ")
        if confirm.lower() != 'y':
            logger.info("File removal cancelled")
            return False
            
        # Remove files and directories
        for file_path in simple_crawler_files:
            if os.path.exists(file_path):
                if os.path.isdir(file_path):
                    logger.info(f"Removing directory: {file_path}")
                    shutil.rmtree(file_path)
                else:
                    logger.info(f"Removing file: {file_path}")
                    os.remove(file_path)
                    
        logger.info("File cleanup completed")
        
    except Exception as e:
        logger.error(f"Error cleaning up files: {e}")
        return False
        
    return True

if __name__ == "__main__":
    print("Web Crawler Cleanup Utility")
    print("---------------------------")
    print("This script will:")
    print("1. Remove all web crawler collections from MongoDB")
    print("2. List and remove files related to simple_crawler")
    print()
    
    proceed = input("Do you want to proceed? (y/n): ")
    if proceed.lower() != 'y':
        print("Cleanup cancelled")
        sys.exit(0)
        
    # Clean up MongoDB
    print("\nStep 1: Cleaning up MongoDB...")
    mongo_success = cleanup_mongodb()
    
    # Clean up files
    print("\nStep 2: Cleaning up files...")
    files_success = cleanup_files()
    
    # Summary
    print("\nCleanup Summary:")
    print(f"MongoDB cleanup: {'Completed' if mongo_success else 'Failed'}")
    print(f"File cleanup: {'Completed' if files_success else 'Failed'}") 