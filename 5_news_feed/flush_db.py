#!/usr/bin/env python3
"""
Flush database and cache for the news feed system.

This script clears all data from the MongoDB database and Redis cache.
"""

import logging
import argparse
import sys
from pymongo import MongoClient
import redis

# Try to import local config if exists, otherwise use default config
try:
    import config_local as config
    print("Using local configuration")
except ImportError:
    import config
    print("Using default configuration")

from config import DATABASE, REDIS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

def flush_mongodb(host="localhost", port=27017, db_name="news_feed", confirm=False, recreate=False):
    """
    Flush all data from MongoDB database.
    
    Args:
        host: MongoDB host
        port: MongoDB port
        db_name: Database name
        confirm: Whether to prompt for confirmation
        recreate: Whether to recreate collections and indexes
        
    Returns:
        True if successful, False otherwise
    """
    if not confirm:
        response = input(f"Are you sure you want to flush the MongoDB database '{db_name}'? [y/N] ")
        if response.lower() not in ("y", "yes"):
            logger.info("MongoDB flush aborted")
            return False
    
    # Connect to MongoDB
    uri = f"mongodb://{host}:{port}"
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        # Drop database
        client.drop_database(db_name)
        logger.info(f"Dropped database: {db_name}")
        
        if recreate:
            # Recreate collections with indexes
            db = client[db_name]
            
            # Create collections
            collections = [
                "users", "posts", "relationships", "actions", 
                "media", "news_feed", "notifications"
            ]
            
            for coll_name in collections:
                db.create_collection(coll_name)
                logger.info(f"Created collection: {coll_name}")
                
            # Create indexes
            from pymongo import ASCENDING, DESCENDING
            
            # Users collection
            db.users.create_index([("username", ASCENDING)], unique=True)
            
            # Posts collection
            db.posts.create_index([("user_id", ASCENDING)])
            db.posts.create_index([("created_at", DESCENDING)])
            
            # Media collection
            db.media.create_index([("post_id", ASCENDING)])
            
            # Relationships collection
            db.relationships.create_index(
                [("user_id", ASCENDING), ("friend_id", ASCENDING)],
                unique=True
            )
            
            # Actions collection
            db.actions.create_index(
                [("user_id", ASCENDING), ("post_id", ASCENDING), 
                 ("action_type", ASCENDING)],
                unique=True
            )
            db.actions.create_index([("post_id", ASCENDING)])
            
            # News feed collection
            db.news_feed.create_index([("user_id", ASCENDING)])
            db.news_feed.create_index(
                [("user_id", ASCENDING), ("created_at", DESCENDING)]
            )
            
            logger.info("Created database indexes")
            
        if recreate:
            logger.info("💾 MongoDB database flushed and reinitialized successfully")
        else:
            logger.info("💾 MongoDB database flushed successfully")
            
        return True
    except Exception as e:
        logger.error(f"Failed to flush MongoDB: {e}")
        return False

def flush_redis(host="localhost", port=6379, db=0, password=None, confirm=False):
    """
    Flush all data from Redis cache.
    
    Args:
        host: Redis host
        port: Redis port
        db: Redis database number
        password: Redis password
        confirm: Whether to prompt for confirmation
        
    Returns:
        True if successful, False otherwise
    """
    if not confirm:
        response = input(f"Are you sure you want to flush Redis database {db}? [y/N] ")
        if response.lower() not in ("y", "yes"):
            logger.info("Redis flush aborted")
            return False
    
    try:
        # Connect to Redis
        r = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        
        # Test connection
        r.ping()
        logger.info("Successfully connected to Redis")
        
        # Get initial key count
        initial_count = r.dbsize()
        
        # Flush database
        r.flushdb()
        logger.info(f"Flushed Redis database {db} ({initial_count} keys removed)")
        
        logger.info("🔥 Redis cache flushed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to flush Redis: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Flush database and cache for news feed")
    parser.add_argument("--mongo-only", action="store_true", help="Flush only MongoDB")
    parser.add_argument("--redis-only", action="store_true", help="Flush only Redis")
    parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation prompts")
    parser.add_argument("--use-init-db", action="store_true", help="Use init_db.py for database initialization")
    parser.add_argument("--recreate", action="store_true", help="Recreate collections and indexes after dropping the database")
    args = parser.parse_args()
    
    success = True
    
    if not args.redis_only:
        # Flush MongoDB
        if args.use_init_db:
            # Use the init_db.py script
            try:
                logger.info("Dropping MongoDB database...")
                client = MongoClient(
                    host=DATABASE["host"],
                    port=DATABASE["port"],
                    serverSelectionTimeoutMS=5000
                )
                client.drop_database(DATABASE["name"])
                logger.info(f"Dropped database: {DATABASE['name']}")
                client.close()
                
                # Import and run init_db
                from init_db import init_db
                success = init_db(
                    host=DATABASE["host"],
                    port=DATABASE["port"],
                    db_name=DATABASE["name"]
                ) and success
            except Exception as e:
                logger.error(f"Failed to use init_db.py: {e}")
                success = False
        else:
            # Use our internal function
            success = flush_mongodb(
                host=DATABASE["host"],
                port=DATABASE["port"],
                db_name=DATABASE["name"],
                confirm=args.force,
                recreate=args.recreate
            ) and success
        
    if not args.mongo_only:
        # Flush Redis
        success = flush_redis(
            host=REDIS["host"],
            port=REDIS["port"],
            db=REDIS["db"],
            password=REDIS["password"],
            confirm=args.force
        ) and success
    
    if success:
        logger.info("✅ All data successfully flushed")
        return 0
    else:
        logger.error("❌ Failed to flush all data")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 