#!/usr/bin/env python3
"""
Test script for the web crawler - tests only the URL frontier and downloader
without requiring MongoDB
"""

import os
import sys
import time
import logging
import threading
from urllib.parse import urlparse
import redis

# Make sure we're in the right directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(script_dir, 'test_crawler.log'))
    ]
)
logger = logging.getLogger("test_crawler")

# Import our modules
import config
from frontier import URLFrontier
from models import URL, Priority, URLStatus
from downloader import HTMLDownloader
from parser import HTMLParser
from robots import RobotsHandler
from dns_resolver import DNSResolver

# Import local configuration if available
try:
    import local_config
    # Override config settings with local settings
    for key in dir(local_config):
        if key.isupper():
            setattr(config, key, getattr(local_config, key))
    logger.info("Loaded local configuration")
except ImportError:
    logger.warning("No local_config.py found - using default config")

def test_redis():
    """Test Redis connection"""
    try:
        logger.info(f"Testing Redis connection to {config.REDIS_URI}")
        r = redis.from_url(config.REDIS_URI)
        r.ping()
        logger.info("Redis connection successful")
        return True
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return False

def test_robots_txt():
    """Test robots.txt handling"""
    try:
        logger.info("Testing robots.txt handling")
        robots_handler = RobotsHandler()
        test_urls = [
            "https://www.google.com/",
            "https://www.github.com/",
            "https://sagarnildas.com/",
        ]
        
        for url in test_urls:
            logger.info(f"Checking robots.txt for {url}")
            allowed, crawl_delay = robots_handler.can_fetch(url)
            logger.info(f"  Allowed: {allowed}, Crawl delay: {crawl_delay}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing robots.txt: {e}")
        return False

def test_dns_resolver():
    """Test DNS resolver"""
    try:
        logger.info("Testing DNS resolver")
        dns_resolver = DNSResolver()
        test_domains = [
            "www.google.com",
            "www.github.com",
            "example.com",
        ]
        
        for domain in test_domains:
            logger.info(f"Resolving {domain}")
            ip = dns_resolver.resolve(f"https://{domain}/")
            logger.info(f"  IP: {ip}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing DNS resolver: {e}")
        return False

def test_url_frontier():
    """Test URL frontier"""
    try:
        logger.info("Testing URL frontier")
        frontier = URLFrontier()
        
        # Clear frontier
        frontier.clear()
        
        # Add some URLs
        test_urls = [
            "https://www.google.com/",
            "https://www.github.com/",
            "https://sagarnildas.com/",
        ]
        
        for i, url in enumerate(test_urls):
            url_obj = URL(
                url=url,
                priority=Priority.MEDIUM,
                status=URLStatus.PENDING,
                depth=0
            )
            added = frontier.add_url(url_obj)
            logger.info(f"Added {url}: {added}")
        
        # Check size
        size = frontier.size()
        logger.info(f"Frontier size: {size}")
        
        # Get next URL
        url = frontier.get_next_url()
        if url:
            logger.info(f"Next URL: {url.url} (priority: {url.priority})")
        else:
            logger.info("No URL available")
        
        return True
    except Exception as e:
        logger.error(f"Error testing URL frontier: {e}")
        return False

def test_downloader():
    """Test HTML downloader"""
    try:
        logger.info("Testing HTML downloader")
        downloader = HTMLDownloader()
        
        test_urls = [
            URL(url="https://sagarnildas.com/", priority=Priority.MEDIUM, status=URLStatus.PENDING, depth=0),
            URL(url="https://www.google.com/", priority=Priority.MEDIUM, status=URLStatus.PENDING, depth=0),
        ]
        
        for url_obj in test_urls:
            logger.info(f"Downloading {url_obj.url}")
            page = downloader.download(url_obj)
            if page:
                logger.info(f"  Downloaded {page.content_length} bytes, status: {page.status_code}")
                # Test parsing
                parser = HTMLParser()
                urls, metadata = parser.parse(page)
                logger.info(f"  Extracted {len(urls)} URLs and {len(metadata)} metadata items")
            else:
                logger.info(f"  Download failed: {url_obj.error}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing HTML downloader: {e}")
        return False

def run_tests():
    """Run all tests"""
    logger.info("Starting crawler component tests")
    
    tests = [
        ("Redis", test_redis),
        ("Robots.txt", test_robots_txt),
        ("DNS Resolver", test_dns_resolver),
        ("URL Frontier", test_url_frontier),
        ("HTML Downloader", test_downloader),
    ]
    
    results = []
    for name, test_func in tests:
        logger.info(f"\n=== Testing {name} ===")
        start_time = time.time()
        success = test_func()
        elapsed = time.time() - start_time
        
        result = {
            "name": name,
            "success": success,
            "time": elapsed
        }
        results.append(result)
        
        logger.info(f"=== {name} test {'succeeded' if success else 'failed'} in {elapsed:.2f}s ===\n")
    
    # Print summary
    logger.info("\n=== Test Summary ===")
    all_success = True
    for result in results:
        status = "SUCCESS" if result["success"] else "FAILED"
        logger.info(f"{result['name']}: {status} ({result['time']:.2f}s)")
        if not result["success"]:
            all_success = False
    
    if all_success:
        logger.info("All tests passed!")
    else:
        logger.warning("Some tests failed. Check logs for details.")
    
    return all_success

if __name__ == "__main__":
    run_tests()