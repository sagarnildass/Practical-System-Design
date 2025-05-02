#!/usr/bin/env python3
"""
Main script to run the web crawler with command line arguments
"""

import os
import sys
import time
import logging
import argparse
import signal
from urllib.parse import urlparse

# Add the current directory to path if needed
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

# Configure logging - do this first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(script_dir, 'crawler.log'))
    ]
)
logger = logging.getLogger("run_crawler")

# Now import the crawler components
logger.info("Importing crawler modules...")
try:
    from crawler import Crawler
    from models import Priority
    logger.info("Successfully imported crawler modules")
except Exception as e:
    logger.error(f"Error importing crawler modules: {e}", exc_info=True)
    sys.exit(1)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run the web crawler with custom settings')
    
    parser.add_argument('--seed', nargs='+', metavar='URL',
                        help='One or more seed URLs to start crawling')
                        
    parser.add_argument('--depth', type=int, default=None, 
                        help='Maximum crawl depth')
                        
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of worker threads')
                        
    parser.add_argument('--delay', type=float, default=None,
                        help='Delay between requests to the same domain (in seconds)')
                        
    parser.add_argument('--respect-robots', dest='respect_robots', action='store_true',
                        help='Respect robots.txt rules')
                        
    parser.add_argument('--ignore-robots', dest='respect_robots', action='store_false',
                        help='Ignore robots.txt rules')
    
    parser.add_argument('--user-agent', type=str, default=None,
                       help='User agent to use for requests')
                       
    parser.add_argument('--async', dest='async_mode', action='store_true',
                       help='Use async mode for requests')
    
    parser.add_argument('--domain-filter', type=str, default=None,
                       help='Only crawl URLs that match this domain')
                       
    parser.add_argument('--reset-db', action='store_true',
                       help='Reset MongoDB and flush Redis data before starting')
    
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set log level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    return args

def reset_databases():
    """Reset MongoDB and flush Redis data"""
    success = True
    
    # Reset MongoDB
    try:
        logger.info("Starting MongoDB cleanup...")
        from mongo_cleanup import cleanup_mongodb
        mongo_success = cleanup_mongodb()
        if not mongo_success:
            logger.warning("MongoDB cleanup may not have been completely successful")
            success = False
        else:
            logger.info("MongoDB cleanup completed successfully")
    except Exception as e:
        logger.error(f"Error cleaning up MongoDB: {e}", exc_info=True)
        success = False
    
    # Flush Redis
    try:
        logger.info("Starting Redis flush...")
        import redis
        logger.debug("Connecting to Redis to flush data...")
        
        # Set a timeout for Redis connection
        r = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=5)
        
        # Check if Redis is available
        try:
            logger.debug("Testing Redis connection...")
            ping_result = r.ping()
            logger.debug(f"Redis ping result: {ping_result}")
            
            # If connection works, flush all data
            logger.info("Flushing all Redis data...")
            result = r.flushall()
            logger.info(f"Redis flush result: {result}")
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
            success = False
    except Exception as e:
        logger.error(f"Error flushing Redis: {e}", exc_info=True)
        success = False
        
    return success

def setup_signal_handlers(crawler_instance):
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        if crawler_instance and crawler_instance.running:
            logger.info("Stopping crawler...")
            crawler_instance.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def run_crawler():
    """Run the crawler with command-line arguments"""
    args = parse_arguments()
    crawler = None
    
    try:
        logger.info("Starting the web crawler...")
        
        # Reset database if requested
        if args.reset_db:
            logger.info("Resetting MongoDB and flushing Redis data...")
            if not reset_databases():
                logger.warning("Database reset was not completely successful")
        
        # Create crawler instance
        logger.info("Creating crawler instance...")
        crawler = Crawler()
        logger.info("Crawler instance created successfully")
        
        # Setup signal handlers
        setup_signal_handlers(crawler)
        
        # Override settings from command line if provided
        if args.depth is not None:
            import config
            config.MAX_DEPTH = args.depth
            logger.info(f"Setting maximum depth to {args.depth}")
            
        if args.delay is not None:
            import config
            config.DELAY_BETWEEN_REQUESTS = args.delay
            logger.info(f"Setting delay between requests to {args.delay} seconds")
            
        if args.respect_robots is not None:
            import config
            config.RESPECT_ROBOTS_TXT = args.respect_robots
            logger.info(f"Respect robots.txt: {args.respect_robots}")
            
        if args.user_agent is not None:
            import config
            config.USER_AGENT = args.user_agent
            logger.info(f"Using user agent: {args.user_agent}")
            
        # Add seed URLs if provided
        if args.seed:
            logger.info(f"Adding {len(args.seed)} seed URLs")
            seed_urls = []
            for url in args.seed:
                if not (url.startswith('http://') or url.startswith('https://')):
                    url = 'https://' + url
                seed_urls.append(url)
                logger.debug(f"Added seed URL: {url}")
                    
            # Add the URLs to the frontier
            logger.info("Adding seed URLs to frontier...")
            added = crawler.add_seed_urls(seed_urls, Priority.VERY_HIGH)
            logger.info(f"Successfully added {added} seed URLs to the frontier")
            
        # Apply domain filter if provided
        if args.domain_filter:
            import config
            
            # Allow both domain.com or http://domain.com formats
            domain = args.domain_filter
            if domain.startswith('http://') or domain.startswith('https://'):
                domain = urlparse(domain).netloc
                
            config.ALLOWED_DOMAINS = [domain]
            logger.info(f"Filtering to domain: {domain}")
        
        # Start the crawler
        num_workers = args.workers if args.workers is not None else 4
        
        logger.info(f"Starting crawler with {num_workers} workers...")
        crawler.start(num_workers=num_workers, async_mode=args.async_mode)
        # If we get here, crawler has finished or was stopped
        logger.info("Crawler finished")
        
    except KeyboardInterrupt:
        logger.info("Crawler interrupted by user")
        if crawler and crawler.running:
            logger.info("Stopping crawler...")
            crawler.stop()
    except Exception as e:
        logger.error(f"Error running crawler: {e}", exc_info=True)
        if crawler and crawler.running:
            try:
                logger.info("Attempting to stop crawler after error...")
                crawler.stop()
            except:
                pass
        
if __name__ == "__main__":
    run_crawler() 