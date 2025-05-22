"""
Configuration settings for the web crawler
"""

import os
from typing import Dict, List, Any, Optional

# General settings
MAX_WORKERS = 100  # Maximum number of worker threads/processes
MAX_DEPTH = 10  # Maximum depth to crawl from seed URLs
CRAWL_TIMEOUT = 30  # Timeout for HTTP requests in seconds
USER_AGENT = "Mozilla/5.0 WebCrawler/1.0 (+https://example.org/bot)"

# Politeness settings
ROBOTSTXT_OBEY = True  # Whether to obey robots.txt rules
DOWNLOAD_DELAY = 1.0  # Delay between requests to the same domain (seconds)
MAX_REQUESTS_PER_DOMAIN = 10  # Maximum concurrent requests per domain
RESPECT_CRAWL_DELAY = True  # Respect Crawl-delay in robots.txt
RETRY_TIMES = 3  # Number of retries for failed requests
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]  # HTTP codes to retry

# URL settings
ALLOWED_DOMAINS: Optional[List[str]] = None  # Domains to restrict crawling to (None = all domains)
EXCLUDED_DOMAINS: List[str] = []  # Domains to exclude from crawling
ALLOWED_SCHEMES = ["http", "https"]  # URL schemes to allow
URL_FILTERS = [
    # Only filter out binary and media files
    r".*\.(jpg|jpeg|gif|png|ico|mp3|mp4|wav|avi|mov|mpeg|pdf|zip|rar|gz|exe|dmg|pkg|iso|bin)$",
]  # Regex patterns to filter out URLs

# Storage settings
MONGODB_URI = "mongodb://localhost:27017/"
MONGODB_DB = "webcrawler"
REDIS_URI = "redis://localhost:6379/0"
STORAGE_PATH = os.path.join(os.path.dirname(__file__), "storage")
HTML_STORAGE_PATH = os.path.join(STORAGE_PATH, "html")
LOG_PATH = os.path.join(STORAGE_PATH, "logs")

# Frontier settings
FRONTIER_QUEUE_SIZE = 100000  # Maximum number of URLs in the frontier queue
PRIORITY_QUEUE_NUM = 5  # Number of priority queues
HOST_QUEUE_NUM = 1000  # Number of host queues for politeness

# Content settings
MAX_CONTENT_SIZE = 10 * 1024 * 1024  # Maximum size of HTML content to download (10MB)
ALLOWED_CONTENT_TYPES = [
    "text/html",
    "application/xhtml+xml",
    "text/plain",  # Some servers might serve HTML as text/plain
    "application/html",
    "*/*",  # Accept any content type
]  # Allowed content types

# DNS settings
DNS_CACHE_SIZE = 10000  # Maximum number of entries in DNS cache
DNS_CACHE_TIMEOUT = 3600  # DNS cache timeout in seconds

# Logging settings
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# Seed URLs
SEED_URLS = [
    "https://en.wikipedia.org/",
    "https://www.nytimes.com/",
    "https://www.bbc.com/",
    "https://www.github.com/",
    "https://www.reddit.com/",
]

# Override settings with environment variables
def get_env_settings() -> Dict[str, Any]:
    """Get settings from environment variables"""
    env_settings = {}
    
    for key, value in globals().items():
        if key.isupper():  # Only consider uppercase variables as settings
            env_value = os.environ.get(f"WEBCRAWLER_{key}")
            if env_value is not None:
                # Convert to appropriate type based on default value
                if isinstance(value, bool):
                    env_settings[key] = env_value.lower() in ("true", "1", "yes")
                elif isinstance(value, int):
                    env_settings[key] = int(env_value)
                elif isinstance(value, float):
                    env_settings[key] = float(env_value)
                elif isinstance(value, list):
                    # Assume comma-separated values
                    env_settings[key] = [item.strip() for item in env_value.split(",")]
                else:
                    env_settings[key] = env_value
    
    return env_settings

# Update settings with environment variables
globals().update(get_env_settings()) 