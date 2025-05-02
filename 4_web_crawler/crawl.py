#!/usr/bin/env python3
"""
Command-line interface for the web crawler.

Usage:
    crawl.py start [--workers=<num>] [--async] [--seed=<url>...]
    crawl.py stop
    crawl.py pause
    crawl.py resume
    crawl.py stats
    crawl.py clean [--days=<days>]
    crawl.py export [--format=<format>] [--output=<file>]
    crawl.py set-max-depth <depth>
    crawl.py add-seed <url>...
    crawl.py (-h | --help)
    crawl.py --version

Options:
    -h --help           Show this help message
    --version           Show version
    --workers=<num>     Number of worker threads [default: 4]
    --async             Use asynchronous mode
    --seed=<url>        Seed URL(s) to start crawling
    --days=<days>       Days threshold for data cleaning [default: 90]
    --format=<format>   Export format (json, csv) [default: json]
    --output=<file>     Output file path [default: crawl_data.json]
"""

import os
import sys
import time
import json
import signal
import logging
import csv
from typing import List, Dict, Any
from docopt import docopt
import datetime
import traceback

from models import URL, URLStatus, Priority
from crawler import Crawler
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

# Global crawler instance
crawler = None


def initialize_crawler() -> Crawler:
    """Initialize the crawler instance"""
    global crawler
    if crawler is None:
        crawler = Crawler()
    return crawler


def start_crawler(workers: int, async_mode: bool, seed_urls: List[str]) -> None:
    """
    Start the crawler
    
    Args:
        workers: Number of worker threads
        async_mode: Whether to use async mode
        seed_urls: List of seed URLs to add
    """
    crawler = initialize_crawler()
    
    # Add seed URLs if provided
    if seed_urls:
        num_added = crawler.add_seed_urls(seed_urls)
        logger.info(f"Added {num_added} seed URLs")
    
    # Start crawler
    try:
        crawler.start(num_workers=workers, async_mode=async_mode)
    except KeyboardInterrupt:
        logger.info("Crawler interrupted by user")
        crawler.stop()
    except Exception as e:
        logger.error(f"Error starting crawler: {e}")
        logger.error(traceback.format_exc())
        crawler.stop()


def stop_crawler() -> None:
    """Stop the crawler"""
    if crawler is None:
        logger.error("Crawler is not running")
        return
    
    crawler.stop()
    logger.info("Crawler stopped")


def pause_crawler() -> None:
    """Pause the crawler"""
    if crawler is None:
        logger.error("Crawler is not running")
        return
    
    crawler.pause()
    logger.info("Crawler paused")


def resume_crawler() -> None:
    """Resume the crawler"""
    if crawler is None:
        logger.error("Crawler is not running")
        return
    
    crawler.resume()
    logger.info("Crawler resumed")


def show_stats() -> None:
    """Show crawler statistics"""
    if crawler is None:
        logger.error("Crawler is not running")
        return
    
    # Get crawler stats
    stats = crawler.stats
    
    # Calculate elapsed time
    elapsed = time.time() - stats['start_time']
    elapsed_str = str(datetime.timedelta(seconds=int(elapsed)))
    
    # Format statistics
    print("\n=== Crawler Statistics ===")
    print(f"Running time: {elapsed_str}")
    print(f"Pages crawled: {stats['pages_crawled']}")
    print(f"Pages failed: {stats['pages_failed']}")
    print(f"URLs discovered: {stats['urls_discovered']}")
    print(f"URLs filtered: {stats['urls_filtered']}")
    
    # Calculate pages per second
    pages_per_second = stats['pages_crawled'] / elapsed if elapsed > 0 else 0
    print(f"Crawl rate: {pages_per_second:.2f} pages/second")
    
    # Domain statistics
    domains = len(stats['domains_crawled'])
    print(f"Domains crawled: {domains}")
    
    # Status code statistics
    print("\n--- HTTP Status Codes ---")
    for status, count in sorted(stats['status_codes'].items()):
        print(f"  {status}: {count}")
    
    # Content type statistics
    print("\n--- Content Types ---")
    for content_type, count in sorted(stats['content_types'].items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {content_type}: {count}")
    
    # Frontier size
    print(f"\nFrontier size: {crawler.frontier.size()}")
    
    # DNS cache statistics
    dns_stats = crawler.dns_resolver.get_stats()
    print(f"\nDNS cache: {dns_stats['hit_count']} hits, {dns_stats['miss_count']} misses, {dns_stats['size']} entries")
    
    print("\n=========================\n")


def clean_data(days: int) -> None:
    """
    Clean old data
    
    Args:
        days: Days threshold for data cleaning
    """
    try:
        if crawler is None:
            initialize_crawler()
        
        # Get MongoDB connection
        storage = crawler.mongo_client
        
        # Clean old pages
        old_pages = storage.clean_old_pages(days)
        
        # Clean failed URLs
        failed_urls = storage.clean_failed_urls()
        
        logger.info(f"Cleaned {old_pages} old pages and {failed_urls} failed URLs")
        print(f"Cleaned {old_pages} old pages and {failed_urls} failed URLs")
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        print(f"Error cleaning data: {e}")


def export_data(export_format: str, output_file: str) -> None:
    """
    Export crawler data
    
    Args:
        export_format: Format to export (json, csv)
        output_file: Output file path
    """
    try:
        if crawler is None:
            initialize_crawler()
        
        # Get MongoDB connection
        db = crawler.db
        
        # Get data
        pages = list(db.pages_collection.find({}, {'_id': 0}))
        urls = list(db.urls_collection.find({}, {'_id': 0}))
        stats = list(db.stats_collection.find({}, {'_id': 0}))
        
        # Prepare export data
        export_data = {
            'metadata': {
                'exported_at': datetime.datetime.now().isoformat(),
                'pages_count': len(pages),
                'urls_count': len(urls),
                'stats_count': len(stats),
            },
            'pages': pages,
            'urls': urls,
            'stats': stats
        }
        
        # Convert datetime objects to strings
        export_data = json.loads(json.dumps(export_data, default=str))
        
        # Export based on format
        if export_format.lower() == 'json':
            with open(output_file, 'w') as f:
                json.dump(export_data, f, indent=2)
            logger.info(f"Data exported to {output_file} in JSON format")
            print(f"Data exported to {output_file} in JSON format")
        elif export_format.lower() == 'csv':
            # Split export into multiple CSV files
            base_name = os.path.splitext(output_file)[0]
            
            # Export pages
            pages_file = f"{base_name}_pages.csv"
            if pages:
                with open(pages_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=pages[0].keys())
                    writer.writeheader()
                    writer.writerows(pages)
            
            # Export URLs
            urls_file = f"{base_name}_urls.csv"
            if urls:
                with open(urls_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=urls[0].keys())
                    writer.writeheader()
                    writer.writerows(urls)
            
            # Export stats
            stats_file = f"{base_name}_stats.csv"
            if stats:
                with open(stats_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=stats[0].keys())
                    writer.writeheader()
                    writer.writerows(stats)
            
            logger.info(f"Data exported to {base_name}_*.csv files in CSV format")
            print(f"Data exported to {base_name}_*.csv files in CSV format")
        else:
            logger.error(f"Unsupported export format: {export_format}")
            print(f"Unsupported export format: {export_format}")
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        print(f"Error exporting data: {e}")


def set_max_depth(depth: int) -> None:
    """
    Set maximum crawl depth
    
    Args:
        depth: Maximum crawl depth
    """
    try:
        depth = int(depth)
        if depth < 0:
            logger.error("Depth must be a positive integer")
            print("Depth must be a positive integer")
            return
        
        # Update configuration
        config.MAX_DEPTH = depth
        
        logger.info(f"Maximum crawl depth set to {depth}")
        print(f"Maximum crawl depth set to {depth}")
    except ValueError:
        logger.error("Depth must be a valid integer")
        print("Depth must be a valid integer")


def add_seed_urls(urls: List[str]) -> None:
    """
    Add seed URLs to the crawler
    
    Args:
        urls: List of URLs to add
    """
    if crawler is None:
        initialize_crawler()
    
    num_added = crawler.add_seed_urls(urls)
    logger.info(f"Added {num_added} seed URLs")
    print(f"Added {num_added} seed URLs")


def handle_signal(sig, frame):
    """Handle signal interrupts"""
    if sig == signal.SIGINT:
        logger.info("Received SIGINT, stopping crawler")
        stop_crawler()
        sys.exit(0)
    elif sig == signal.SIGTERM:
        logger.info("Received SIGTERM, stopping crawler")
        stop_crawler()
        sys.exit(0)


def main():
    """Main entry point"""
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Parse arguments
    args = docopt(__doc__, version='Web Crawler 1.0')
    
    # Handle commands
    if args['start']:
        workers = int(args['--workers'])
        async_mode = args['--async']
        seed_urls = args['--seed'] if args['--seed'] else []
        start_crawler(workers, async_mode, seed_urls)
    elif args['stop']:
        stop_crawler()
    elif args['pause']:
        pause_crawler()
    elif args['resume']:
        resume_crawler()
    elif args['stats']:
        show_stats()
    elif args['clean']:
        days = int(args['--days'])
        clean_data(days)
    elif args['export']:
        export_format = args['--format']
        output_file = args['--output']
        export_data(export_format, output_file)
    elif args['set-max-depth']:
        depth = args['<depth>']
        set_max_depth(depth)
    elif args['add-seed']:
        urls = args['<url>']
        add_seed_urls(urls)
    else:
        print(__doc__)


if __name__ == '__main__':
    main() 