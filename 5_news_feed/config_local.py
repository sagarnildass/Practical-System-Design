"""
Local configuration overrides for development.
"""

import os
from config import *

# Database settings for local development
DATABASE = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 27017)),
    "name": os.getenv("DB_NAME", "news_feed"),
    "user": None,  # No authentication for local MongoDB
    "password": None,
    "pool_size": int(os.getenv("DB_POOL_SIZE", 10)),
    "replica_set": None,
}

# Redis cache settings (unchanged)
# REDIS = {...}

# Use smaller worker pools for local development
FANOUT["num_workers"] = 2 