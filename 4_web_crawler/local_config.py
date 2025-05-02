"""
Local configuration override for web crawler
"""

import os
from typing import Dict, List, Any, Optional

# MongoDB and Redis connection settings
MONGODB_URI = "mongodb://localhost:27017/"
REDIS_URI = "redis://localhost:6379/0"

# Base storage path
STORAGE_PATH = os.path.join(os.path.dirname(__file__), "storage")
HTML_STORAGE_PATH = os.path.join(STORAGE_PATH, "html")
LOG_PATH = os.path.join(STORAGE_PATH, "logs")

# Ensure directories exist
os.makedirs(STORAGE_PATH, exist_ok=True)
os.makedirs(HTML_STORAGE_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

# Seed URLs - customize these for your initial crawl
SEED_URLS = [
    "https://sagarnildas.com/",
]

# Politeness settings - adjust these to be more gentle with websites
DOWNLOAD_DELAY = 2.0  # Increased delay between requests to be more polite
MAX_REQUESTS_PER_DOMAIN = 5  # Fewer concurrent requests per domain

# Using simpler storage to start (no Redis Bloom filter)
USE_SIMPLE_URL_SEEN = True 