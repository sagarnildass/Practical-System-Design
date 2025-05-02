#!/usr/bin/env python3
"""
Example script that demonstrates how to use the web crawler programmatically.

This example:
1. Initializes the crawler
2. Adds seed URLs
3. Starts the crawler with 2 workers
4. Monitors progress for a specific duration
5. Pauses, resumes, and stops the crawler
6. Exports crawl data

Usage:
    python example.py [--time=<seconds>] [--workers=<num>] [--async]

Options:
    --time=<seconds>    Duration of the crawl in seconds [default: 60]
    --workers=<num>     Number of worker threads [default: 2]
    --async             Use asynchronous mode
"""

import time
import logging
import sys
import json
import os
import signal
import threading
from docopt import docopt

from crawler import Crawler
from models import URLStatus, Priority
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('example')


def log_stats(crawler, interval=5):
    """Log crawler statistics periodically"""
    stats = crawler.stats
    elapsed = time.time() - stats['start_time']
    
    logger.info(f"=== Crawler Statistics (after {int(elapsed)}s) ===")
    logger.info(f"Pages crawled: {stats['pages_crawled']}")
    logger.info(f"Pages failed: {stats['pages_failed']}")
    logger.info(f"URLs discovered: {stats['urls_discovered']}")
    logger.info(f"URLs filtered: {stats['urls_filtered']}")
    logger.info(f"Domains crawled: {len(stats['domains_crawled'])}")
    logger.info(f"Frontier size: {crawler.frontier.size()}")
    
    # Status code distribution
    status_codes = stats['status_codes']
    if status_codes:
        logger.info("Status code distribution:")
        for status, count in sorted(status_codes.items()):
            logger.info(f"  {status}: {count}")
    
    # Check if crawler is still running
    if crawler.running and not crawler.stop_event.is_set():
        # Schedule next logging
        timer = threading.Timer(interval, log_stats, args=[crawler, interval])
        timer.daemon = True
        timer.start()


def example_crawl(duration=60, workers=2, async_mode=False):
    """
    Example crawler use
    
    Args:
        duration: Duration of the crawl in seconds
        workers: Number of worker threads
        async_mode: Whether to use async mode
    """
    logger.info("Initializing web crawler...")
    
    # Initialize crawler
    crawler = Crawler()
    
    # Add seed URLs
    seed_urls = [
        'https://en.wikipedia.org/wiki/Web_crawler',
        'https://en.wikipedia.org/wiki/Search_engine',
        'https://en.wikipedia.org/wiki/Web_indexing',
        'https://python.org',
        'https://www.example.com'
    ]
    logger.info(f"Adding {len(seed_urls)} seed URLs...")
    crawler.add_seed_urls(seed_urls)
    
    # Set up signal handling
    def signal_handler(sig, frame):
        logger.info("Received interrupt signal, stopping crawler")
        crawler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start a thread to log stats periodically
    log_stats(crawler, interval=5)
    
    # Start the crawler in a separate thread
    logger.info(f"Starting crawler with {workers} workers (async={async_mode})...")
    crawler_thread = threading.Thread(
        target=crawler.start,
        kwargs={'num_workers': workers, 'async_mode': async_mode}
    )
    crawler_thread.daemon = True
    crawler_thread.start()
    
    # Let the crawler run for a while
    logger.info(f"Crawler will run for {duration} seconds...")
    time.sleep(duration // 2)
    
    # Pause crawler
    logger.info("Pausing crawler for 5 seconds...")
    crawler.pause()
    time.sleep(5)
    
    # Resume crawler
    logger.info("Resuming crawler...")
    crawler.resume()
    time.sleep(duration // 2)
    
    # Stop crawler
    logger.info("Stopping crawler...")
    crawler.stop()
    
    # Wait for crawler to stop
    crawler_thread.join(timeout=10)
    
    # Export crawl data
    export_dir = os.path.join(config.STORAGE_PATH, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    export_file = os.path.join(export_dir, 'example_crawl_results.json')
    
    logger.info(f"Exporting crawl data to {export_file}...")
    export_results(crawler, export_file)
    
    logger.info("Crawl example completed")
    
    # Print summary
    print_summary(crawler)


def export_results(crawler, output_file):
    """
    Export crawler results to a file
    
    Args:
        crawler: Crawler instance
        output_file: Output file path
    """
    try:
        # Get MongoDB collections
        pages_collection = crawler.db.pages_collection
        urls_collection = crawler.db.urls_collection
        
        # Get data
        pages = list(pages_collection.find({}, {'_id': 0}).limit(1000))
        urls = list(urls_collection.find({}, {'_id': 0}).limit(1000))
        
        # Prepare export data
        export_data = {
            'metadata': {
                'crawl_duration': time.time() - crawler.stats['start_time'],
                'pages_crawled': crawler.stats['pages_crawled'],
                'urls_discovered': crawler.stats['urls_discovered'],
                'domains_crawled': list(crawler.stats['domains_crawled']),
                'exported_pages': len(pages),
                'exported_urls': len(urls),
                'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'pages': pages,
            'urls': urls,
            'stats': crawler.stats
        }
        
        # Convert datetime objects to strings for JSON serialization
        export_data = json.loads(json.dumps(export_data, default=str))
        
        # Write to file
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"Exported data to {output_file}")
    except Exception as e:
        logger.error(f"Error exporting results: {e}")


def print_summary(crawler):
    """
    Print a summary of the crawl
    
    Args:
        crawler: Crawler instance
    """
    stats = crawler.stats
    
    print("\n=============== CRAWL SUMMARY ===============")
    print(f"Duration: {time.time() - stats['start_time']:.2f} seconds")
    print(f"Pages crawled: {stats['pages_crawled']}")
    print(f"Pages failed: {stats['pages_failed']}")
    print(f"URLs discovered: {stats['urls_discovered']}")
    print(f"URLs filtered: {stats['urls_filtered']}")
    print(f"Domains crawled: {len(stats['domains_crawled'])}")
    
    if stats['domains_crawled']:
        print("\nTop domains:")
        domain_counts = {}
        # Count pages per domain
        for page in crawler.db.pages_collection.find({}, {'domain': 1}):
            domain = page.get('domain', 'unknown')
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        # Display top domains
        for domain, count in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {domain}: {count} pages")
    
    print("\nHTTP Status Codes:")
    for status, count in sorted(stats['status_codes'].items()):
        print(f"  {status}: {count}")
    
    print("\nContent Types:")
    for content_type, count in sorted(stats['content_types'].items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {content_type}: {count}")
    
    print("=============================================\n")


if __name__ == '__main__':
    # Parse command-line arguments
    args = docopt(__doc__)
    
    duration = int(args['--time'])
    workers = int(args['--workers'])
    async_mode = args['--async']
    
    try:
        example_crawl(duration, workers, async_mode)
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in example: {e}")
        logger.exception(e) 