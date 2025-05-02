"""
Test script for the URL shortener to verify it works correctly without external dependencies.
This script directly tests the core functionality without needing to run the full API server.
"""

import sys
import logging
import os
from shortener import URLShortener
from db import URLRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_shortener")

def test_shortener():
    """
    Test the URL shortener core functionality
    """
    logger.info("Starting URL shortener test...")
    
    # Create database and tables
    try:
        URLRepository.initialize_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False
    
    # Test basic shortening and retrieval
    test_url = "https://www.example.com/very/long/url/path?param1=value1&param2=value2"
    logger.info(f"Testing URL shortening for: {test_url}")
    
    # Shorten the URL
    short_url, is_new = URLShortener.shorten_url(test_url)
    if not short_url:
        logger.error("Failed to shorten URL")
        return False
        
    logger.info(f"URL shortened successfully: {short_url} (New: {is_new})")
    
    # Retrieve the URL
    url_id, long_url = URLShortener.get_long_url(short_url)
    if not long_url:
        logger.error("Failed to retrieve long URL")
        return False
        
    logger.info(f"Retrieved long URL successfully: {long_url}")
    
    # Verify the retrieved URL matches the original
    if long_url != test_url:
        logger.error(f"Retrieved URL does not match original: {long_url} != {test_url}")
        return False
    
    # Test shortening same URL again (should return existing short URL)
    short_url2, is_new2 = URLShortener.shorten_url(test_url)
    if not short_url2 or is_new2:
        logger.error("Failed when shortening the same URL again")
        return False
        
    logger.info(f"Re-shortened URL correctly: {short_url2} (New: {is_new2})")
    
    if short_url != short_url2:
        logger.error(f"Short URLs don't match for same long URL: {short_url} != {short_url2}")
        return False
    
    # Test analytics/click tracking
    logger.info("Testing click tracking")
    
    # Simulate clicks
    for i in range(3):
        URLShortener.get_long_url(short_url)
    
    # Get stats
    stats = URLShortener.get_url_stats(short_url)
    if not stats:
        logger.error("Failed to get URL stats")
        return False
    
    logger.info(f"URL stats: {stats}")
    if stats['clicks'] < 3:
        logger.warning(f"Click count is less than expected: {stats['clicks']} < 3")
    
    # Test with a different URL
    logger.info("Testing with a second URL")
    test_url2 = "https://www.example.org/another/path"
    short_url3, is_new3 = URLShortener.shorten_url(test_url2)
    if not short_url3 or not is_new3:
        logger.error("Failed to shorten second URL")
        return False
    
    logger.info(f"Second URL shortened successfully: {short_url3}")
    
    if short_url == short_url3:
        logger.error("Second URL produced same short URL as first URL")
        return False
    
    logger.info("All tests passed!")
    
    # Display database file location
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'url_shortener.db'))
    logger.info(f"SQLite database created at: {db_path}")
    
    return True

if __name__ == "__main__":
    success = test_shortener()
    sys.exit(0 if success else 1) 