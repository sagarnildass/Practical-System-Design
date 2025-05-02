"""
Cache layer for the URL shortener service.
Implements Redis-based caching for better performance with fallback to no-caching when Redis is unavailable.
"""

import redis
import logging
import json
from config import REDIS_CONFIG, CACHE_EXPIRATION

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple in-memory cache for fallback when Redis is unavailable
in_memory_cache = {
    'short_urls': {},  # {short_url: {'id': url_id, 'long_url': long_url}}
    'long_urls': {},   # {hash(long_url): {'id': url_id, 'short_url': short_url}}
    'clicks': {}       # {url_id: click_count}
}

# Attempt to create Redis connection
try:
    redis_client = redis.Redis(**REDIS_CONFIG)
    redis_client.ping()  # Test connection
    logger.info("Redis connection established successfully")
except redis.ConnectionError as e:
    logger.warning(f"Redis connection error: {e}. Will use in-memory fallback.")
    redis_client = None
except Exception as e:
    logger.warning(f"Unexpected error connecting to Redis: {e}. Will use in-memory fallback.")
    redis_client = None

class URLCache:
    """Cache operations for URL shortener"""
    
    @staticmethod
    def is_available():
        """Check if Redis is available"""
        if redis_client is None:
            return False
        try:
            return redis_client.ping()
        except:
            return False
    
    @staticmethod
    def get_long_url(short_url):
        """
        Get the long URL from cache by short URL
        
        Args:
            short_url (str): The shortened URL
            
        Returns:
            tuple: (url_id, long_url) or (None, None) if not found or error
        """
        # Try Redis first
        if URLCache.is_available():
            try:
                # Try to get from cache
                value = redis_client.get(f"short:{short_url}")
                if value:
                    # Parse the stored JSON value
                    data = json.loads(value)
                    logger.info(f"Cache hit for short URL: {short_url}")
                    return data.get("id"), data.get("long_url")
                
                logger.info(f"Cache miss for short URL: {short_url}")
                return None, None
            except Exception as e:
                logger.error(f"Error retrieving from Redis cache: {e}")
                # Fall through to in-memory cache
        
        # Fallback to in-memory cache
        if short_url in in_memory_cache['short_urls']:
            data = in_memory_cache['short_urls'][short_url]
            logger.info(f"In-memory cache hit for short URL: {short_url}")
            return data.get('id'), data.get('long_url')
        
        logger.info(f"In-memory cache miss for short URL: {short_url}")
        return None, None
    
    @staticmethod
    def get_short_url(long_url):
        """
        Get the short URL from cache by long URL
        
        Args:
            long_url (str): The original long URL
            
        Returns:
            tuple: (url_id, short_url) or (None, None) if not found or error
        """
        # Try Redis first
        if URLCache.is_available():
            try:
                # Use hash of long URL as key for better performance
                key = f"long:{hash(long_url)}"
                value = redis_client.get(key)
                if value:
                    # Parse the stored JSON value
                    data = json.loads(value)
                    logger.info(f"Cache hit for long URL: {long_url[:50]}...")
                    return data.get("id"), data.get("short_url")
                
                logger.info(f"Cache miss for long URL: {long_url[:50]}...")
                return None, None
            except Exception as e:
                logger.error(f"Error retrieving from Redis cache: {e}")
                # Fall through to in-memory cache
        
        # Fallback to in-memory cache
        long_url_hash = str(hash(long_url))
        if long_url_hash in in_memory_cache['long_urls']:
            data = in_memory_cache['long_urls'][long_url_hash]
            logger.info(f"In-memory cache hit for long URL: {long_url[:50]}...")
            return data.get('id'), data.get('short_url')
        
        logger.info(f"In-memory cache miss for long URL: {long_url[:50]}...")
        return None, None
    
    @staticmethod
    def cache_url_mapping(url_id, short_url, long_url):
        """
        Cache both short_url -> long_url and long_url -> short_url mappings
        
        Args:
            url_id (int): The URL ID
            short_url (str): The shortened URL
            long_url (str): The original long URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Try Redis first
        redis_success = False
        if URLCache.is_available():
            try:
                # Prepare data for caching
                short_data = json.dumps({"id": url_id, "long_url": long_url})
                long_data = json.dumps({"id": url_id, "short_url": short_url})
                
                # Cache both directions with expiration
                redis_client.setex(f"short:{short_url}", CACHE_EXPIRATION, short_data)
                redis_client.setex(f"long:{hash(long_url)}", CACHE_EXPIRATION, long_data)
                
                logger.info(f"Cached URL mapping in Redis: {short_url} <-> {long_url[:50]}...")
                redis_success = True
            except Exception as e:
                logger.error(f"Error caching in Redis: {e}")
                # Fall through to in-memory cache
        
        # Always update in-memory cache as fallback
        try:
            in_memory_cache['short_urls'][short_url] = {"id": url_id, "long_url": long_url}
            in_memory_cache['long_urls'][str(hash(long_url))] = {"id": url_id, "short_url": short_url}
            logger.info(f"Cached URL mapping in memory: {short_url} <-> {long_url[:50]}...")
            return True
        except Exception as e:
            logger.error(f"Error caching in memory: {e}")
            return redis_success  # If Redis succeeded but memory failed, still return true
    
    @staticmethod
    def remove_url_mapping(short_url, long_url):
        """
        Remove a URL mapping from cache
        
        Args:
            short_url (str): The shortened URL
            long_url (str): The original long URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        success = True
        
        # Try Redis first
        if URLCache.is_available():
            try:
                # Remove both directions from cache
                redis_client.delete(f"short:{short_url}")
                redis_client.delete(f"long:{hash(long_url)}")
                logger.info(f"Removed URL mapping from Redis cache: {short_url}")
            except Exception as e:
                logger.error(f"Error removing from Redis cache: {e}")
                success = False
        
        # Also remove from in-memory cache
        try:
            if short_url in in_memory_cache['short_urls']:
                del in_memory_cache['short_urls'][short_url]
            
            long_url_hash = str(hash(long_url))
            if long_url_hash in in_memory_cache['long_urls']:
                del in_memory_cache['long_urls'][long_url_hash]
            
            logger.info(f"Removed URL mapping from in-memory cache: {short_url}")
            return True
        except Exception as e:
            logger.error(f"Error removing from in-memory cache: {e}")
            return success  # If Redis succeeded but memory failed, still return true
    
    @staticmethod
    def increment_click_count(url_id):
        """
        Increment the click count for a URL in cache
        
        Args:
            url_id (int): The URL ID
            
        Returns:
            int: The new click count or -1 if error
        """
        # Try Redis first
        if URLCache.is_available():
            try:
                # Increment click counter
                key = f"clicks:{url_id}"
                count = redis_client.incr(key)
                
                # Set expiration if it's a new key
                if count == 1:
                    redis_client.expire(key, CACHE_EXPIRATION)
                
                # Also update in-memory cache for consistency
                url_id_str = str(url_id)
                in_memory_cache['clicks'][url_id_str] = count
                    
                return count
            except Exception as e:
                logger.error(f"Error incrementing click count in Redis: {e}")
                # Fall through to in-memory cache
        
        # Fallback to in-memory cache
        try:
            url_id_str = str(url_id)
            if url_id_str not in in_memory_cache['clicks']:
                in_memory_cache['clicks'][url_id_str] = 0
            
            in_memory_cache['clicks'][url_id_str] += 1
            return in_memory_cache['clicks'][url_id_str]
        except Exception as e:
            logger.error(f"Error incrementing click count in memory: {e}")
            return -1
    
    @staticmethod
    def get_click_count(url_id):
        """
        Get the click count for a URL from cache
        
        Args:
            url_id (int): The URL ID
            
        Returns:
            int: The click count or -1 if not in cache or error
        """
        # Try Redis first
        if URLCache.is_available():
            try:
                # Get click counter
                key = f"clicks:{url_id}"
                count = redis_client.get(key)
                
                if count:
                    return int(count)
                # Fall through to in-memory cache if not in Redis
            except Exception as e:
                logger.error(f"Error getting click count from Redis: {e}")
                # Fall through to in-memory cache
        
        # Fallback to in-memory cache
        try:
            url_id_str = str(url_id)
            if url_id_str in in_memory_cache['clicks']:
                return in_memory_cache['clicks'][url_id_str]
            return 0  # Not found in memory cache
        except Exception as e:
            logger.error(f"Error getting click count from memory: {e}")
            return -1 