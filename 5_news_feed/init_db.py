"""
MongoDB database initialization script.

Run this script to create the MongoDB database and collections
with proper indexes for the news feed system.
"""

import logging
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

def init_db(host="localhost", port=27017, db_name="news_feed"):
    """
    Initialize the MongoDB database.
    
    Args:
        host: MongoDB host
        port: MongoDB port
        db_name: Database name
    
    Returns:
        True if successful, False otherwise
    """
    # Connect to MongoDB
    uri = f"mongodb://{host}:{port}"
    
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        # Create database
        db = client[db_name]
        logger.info(f"Using database: {db_name}")
        
        # Create collections
        collections = [
            "users", "posts", "relationships", "actions", 
            "media", "news_feed", "notifications"
        ]
        
        for coll_name in collections:
            if coll_name not in db.list_collection_names():
                db.create_collection(coll_name)
                logger.info(f"Created collection: {coll_name}")
            else:
                logger.info(f"Collection already exists: {coll_name}")
        
        # Create indexes
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
        
        logger.info("Database initialization completed successfully")
        return True
        
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return False
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False
    finally:
        client.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("Usage: python init_db.py [host] [port] [db_name]")
        print("Default: host=localhost, port=27017, db_name=news_feed")
        sys.exit(0)
        
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 27017
    db_name = sys.argv[3] if len(sys.argv) > 3 else "news_feed"
    
    success = init_db(host, port, db_name)
    
    if success:
        logger.info("✅ Database initialization successful")
        sys.exit(0)
    else:
        logger.error("❌ Database initialization failed")
        sys.exit(1) 