"""
Configuration settings for the URL shortener service.
"""

# Base URL for the shortened URLs
BASE_URL = "http://localhost:8000/"

# SQLite Database file path
# This is defined in db.py - kept as reference here
# DB_PATH = 'url_shortener.db'

# Redis configuration
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,  # Set if Redis requires authentication
    'decode_responses': True  # Return strings instead of bytes
}

# Cache expiration time (in seconds)
CACHE_EXPIRATION = 60 * 60 * 24  # 24 hours

# Rate limiting (requests per minute per IP)
RATE_LIMIT = 100

# Use 301 (permanent) or 302 (temporary) redirects
# 301: Better for SEO, lower server load due to browser caching
# 302: Better for analytics as all requests come through the service
REDIRECT_TYPE = 302

# Analytics settings
ENABLE_ANALYTICS = True

# Character set for encoding (Base62: 0-9, a-z, A-Z)
CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" 