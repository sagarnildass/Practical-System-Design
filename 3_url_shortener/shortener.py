"""
URL shortening service with Base62 encoding.
"""

import logging
from config import CHARSET
from db import URLRepository
from cache import URLCache
from id_generator import generate_id

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class URLShortener:
    """
    Main URL shortening service
    """
    
    @staticmethod
    def encode_base62(num):
        """
        Encode a number to Base62 string
        
        Args:
            num (int): The number to encode
            
        Returns:
            str: Base62-encoded string
        """
        if num == 0:
            return CHARSET[0]
            
        base62 = ""
        base = len(CHARSET)
        
        while num > 0:
            base62 = CHARSET[num % base] + base62
            num //= base
            
        return base62
    
    @staticmethod
    def decode_base62(base62_str):
        """
        Decode a Base62 string to number
        
        Args:
            base62_str (str): The Base62 string to decode
            
        Returns:
            int: Decoded number
        """
        num = 0
        base = len(CHARSET)
        
        for char in base62_str:
            num = num * base + CHARSET.index(char)
            
        return num
    
    @staticmethod
    def shorten_url(long_url):
        """
        Shorten a URL
        
        Args:
            long_url (str): The original long URL
            
        Returns:
            tuple: (short_url, is_new) where is_new is True if a new short URL was created
        """
        if not long_url:
            logger.error("Cannot shorten empty URL")
            return None, False
            
        # Normalize URL: add http:// if no protocol specified
        if not long_url.startswith('http://') and not long_url.startswith('https://'):
            long_url = 'http://' + long_url
        
        # Check if URL already exists in cache
        url_id, short_url = URLCache.get_short_url(long_url)
        if url_id and short_url:
            logger.info(f"URL already exists in cache: {short_url}")
            return short_url, False
        
        # Check if URL already exists in database
        url_id, short_url = URLRepository.get_url_by_long_url(long_url)
        if url_id and short_url:
            logger.info(f"URL already exists in database: {short_url}")
            # Update cache
            URLCache.cache_url_mapping(url_id, short_url, long_url)
            return short_url, False
        
        # Generate a new unique ID
        url_id = generate_id()
        
        # Convert ID to short URL using Base62
        short_url = URLShortener.encode_base62(url_id)
        
        # Save to database
        if URLRepository.save_url(url_id, short_url, long_url):
            # Update cache
            URLCache.cache_url_mapping(url_id, short_url, long_url)
            logger.info(f"Created new short URL: {short_url} for {long_url[:50]}...")
            return short_url, True
        else:
            logger.error(f"Failed to save URL: {long_url[:50]}...")
            return None, False
    
    @staticmethod
    def get_long_url(short_url):
        """
        Get the original long URL from a short URL
        
        Args:
            short_url (str): The shortened URL
            
        Returns:
            tuple: (url_id, long_url) or (None, None) if not found
        """
        if not short_url:
            logger.error("Empty short URL")
            return None, None
        
        # Try to get from cache first
        url_id, long_url = URLCache.get_long_url(short_url)
        if url_id and long_url:
            # Update analytics
            URLCache.increment_click_count(url_id)
            return url_id, long_url
        
        # If not in cache, get from database
        url_id, long_url = URLRepository.get_url_by_short_url(short_url)
        if url_id and long_url:
            # Update cache
            URLCache.cache_url_mapping(url_id, short_url, long_url)
            # Update analytics
            URLCache.increment_click_count(url_id)
            return url_id, long_url
        
        logger.warning(f"Short URL not found: {short_url}")
        return None, None
    
    @staticmethod
    def get_url_stats(short_url):
        """
        Get statistics for a short URL
        
        Args:
            short_url (str): The shortened URL
            
        Returns:
            dict: Statistics including click count, or None if not found
        """
        # First, check if the URL exists
        url_id, long_url = URLShortener.get_long_url(short_url)
        if not url_id:
            return None
            
        # Get click count from cache first
        cache_clicks = URLCache.get_click_count(url_id)
        
        # If not in cache or cache reports error, get from database
        if cache_clicks < 0:
            db_clicks = URLRepository.get_click_count(url_id)
            clicks = db_clicks
        else:
            clicks = cache_clicks
        
        return {
            'short_url': short_url,
            'long_url': long_url,
            'clicks': clicks
        } 