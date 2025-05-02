"""
Configuration settings for the news feed system.
"""

import os

# General settings
APP_NAME = "NewsFeed"
DEBUG = os.getenv("DEBUG", "False") == "True"
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
API_VERSION = "v1"
PORT = int(os.getenv("PORT", 5000))

# Database settings
DATABASE = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 27017)),
    "name": os.getenv("DB_NAME", "news_feed"),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    # Number of database connections in the connection pool
    "pool_size": int(os.getenv("DB_POOL_SIZE", 100)),
    # Replica set name for MongoDB replication
    "replica_set": os.getenv("DB_REPLICA_SET", None),
}

# Redis cache settings
REDIS = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db": int(os.getenv("REDIS_DB", 0)),
    "password": os.getenv("REDIS_PASSWORD", None),
    # Cache expiration times in seconds
    "expiration": {
        "news_feed": 60 * 60,  # 1 hour
        "user": 60 * 60 * 24,  # 24 hours
        "post": 60 * 60 * 24,  # 24 hours
        "relationship": 60 * 60 * 24,  # 24 hours
        "counter": 60 * 60,  # 1 hour
    },
    # Maximum number of post IDs to store in a user's news feed
    "max_feed_size": 1000,
}

# Queue settings (RabbitMQ/Kafka)
QUEUE = {
    "host": os.getenv("QUEUE_HOST", "localhost"),
    "port": int(os.getenv("QUEUE_PORT", 5672)),
    "user": os.getenv("QUEUE_USER", "guest"),
    "password": os.getenv("QUEUE_PASSWORD", "guest"),
    # Queue names
    "names": {
        "fanout": "fanout_queue",
        "notification": "notification_queue",
    },
}

# Fanout settings
FANOUT = {
    # Threshold for celebrity accounts (pull-based approach)
    "celebrity_threshold": 5000,
    # Maximum number of friends to process in a single fanout batch
    "batch_size": 100,
    # Number of worker threads for fanout service
    "num_workers": 10,
}

# API rate limits (requests per minute)
RATE_LIMITS = {
    "publish_post": 10,
    "read_feed": 100,
    "relationship_update": 20,
}

# Media settings
MEDIA = {
    "upload_folder": os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads"),
    "allowed_extensions": {"png", "jpg", "jpeg", "gif", "mp4", "mov"},
    "max_content_length": 10 * 1024 * 1024,  # 10MB
    # CDN settings
    "cdn_base_url": os.getenv("CDN_BASE_URL", "http://localhost:8000/static/"),
    "cdn_enabled": os.getenv("CDN_ENABLED", "False") == "True",
}

# Logging configuration
LOGGING = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
}

# Performance tuning
PERFORMANCE = {
    # Timeout for API requests in seconds
    "api_timeout": 5,
    # Number of posts to fetch in a single batch
    "post_batch_size": 20,
    # Maximum delay between fanout batches in seconds
    "max_fanout_delay": 2,
    # Use asynchronous fanout
    "async_fanout": True,
}

# Feature flags
FEATURES = {
    "enable_push_notifications": True,
    "enable_analytics": True,
    "enable_content_filtering": True,
} 