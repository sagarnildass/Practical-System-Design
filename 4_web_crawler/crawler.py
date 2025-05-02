"""
Main crawler class to coordinate the web crawling process
"""

import time
import logging
import os
import asyncio
import threading
from typing import List, Dict, Set, Tuple, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor
import signal
import json
import datetime
from urllib.parse import urlparse
import traceback
from pymongo import MongoClient
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from models import URL, Page, URLStatus, Priority
from frontier import URLFrontier
from downloader import HTMLDownloader
from parser import HTMLParser
from robots import RobotsHandler
from dns_resolver import DNSResolver
import config

# Import local configuration if available
try:
    import local_config
    # Override config settings with local settings
    for key in dir(local_config):
        if key.isupper():
            setattr(config, key, getattr(local_config, key))
    print(f"Loaded local configuration from {local_config.__file__}")
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class Crawler:
    """
    Main crawler class that coordinates the web crawling process
    
    Manages:
    - URL Frontier
    - HTML Downloader
    - HTML Parser
    - Content Storage
    - Monitoring and Statistics
    """
    
    def __init__(self, 
                 mongo_uri: Optional[str] = None,
                 redis_uri: Optional[str] = None,
                 metrics_port: int = 9100):
        """
        Initialize the crawler
        
        Args:
            mongo_uri: MongoDB URI for content storage
            redis_uri: Redis URI for URL frontier
            metrics_port: Port for Prometheus metrics server
        """
        self.mongo_uri = mongo_uri or config.MONGODB_URI
        self.redis_uri = redis_uri or config.REDIS_URI
        self.metrics_port = metrics_port
        
        # Create components
        self.frontier = URLFrontier()
        self.robots_handler = RobotsHandler()
        self.dns_resolver = DNSResolver()
        self.downloader = HTMLDownloader(self.dns_resolver, self.robots_handler)
        self.parser = HTMLParser()
        
        # Connect to MongoDB
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[config.MONGODB_DB]
        self.pages_collection = self.db['pages']
        self.urls_collection = self.db['urls']
        self.stats_collection = self.db['stats']
        
        # Ensure indexes
        self._create_indexes()
        
        # Initialize statistics
        self.stats = {
            'pages_crawled': 0,
            'pages_failed': 0,
            'urls_discovered': 0,
            'urls_filtered': 0,
            'start_time': time.time(),
            'domains_crawled': set(),
            'content_types': {},
            'status_codes': {},
        }
        
        # Set up metrics
        self._setup_metrics()
        
        # Flag to control crawling
        self.running = False
        self.paused = False
        self.stop_event = threading.Event()
        
        # Create storage directories if they don't exist
        os.makedirs(config.HTML_STORAGE_PATH, exist_ok=True)
        os.makedirs(config.LOG_PATH, exist_ok=True)
    
    def _create_indexes(self):
        """Create indexes for MongoDB collections"""
        try:
            # Pages collection indexes
            self.pages_collection.create_index('url', unique=True)
            self.pages_collection.create_index('content_hash')
            self.pages_collection.create_index('crawled_at')
            
            # URLs collection indexes
            self.urls_collection.create_index('url', unique=True)
            self.urls_collection.create_index('normalized_url', unique=True)
            self.urls_collection.create_index('domain')
            self.urls_collection.create_index('status')
            self.urls_collection.create_index('priority')
            
            logger.info("MongoDB indexes created")
        except Exception as e:
            logger.error(f"Error creating MongoDB indexes: {e}")
    
    def _setup_metrics(self):
        """Set up Prometheus metrics"""
        # Counters
        self.pages_crawled_counter = Counter('crawler_pages_crawled_total', 'Total pages crawled')
        self.pages_failed_counter = Counter('crawler_pages_failed_total', 'Total pages failed')
        self.urls_discovered_counter = Counter('crawler_urls_discovered_total', 'Total URLs discovered')
        self.urls_filtered_counter = Counter('crawler_urls_filtered_total', 'Total URLs filtered')
        
        # Gauges
        self.frontier_size_gauge = Gauge('crawler_frontier_size', 'Size of URL frontier')
        self.active_threads_gauge = Gauge('crawler_active_threads', 'Number of active crawler threads')
        
        # Histograms
        self.download_time_histogram = Histogram('crawler_download_time_seconds', 'Time to download pages')
        self.page_size_histogram = Histogram('crawler_page_size_bytes', 'Size of downloaded pages')
        
        # Start metrics server
        try:
            start_http_server(self.metrics_port)
            logger.info(f"Metrics server started on port {self.metrics_port}")
        except Exception as e:
            logger.error(f"Error starting metrics server: {e}")
    
    def add_seed_urls(self, urls: List[str], priority: Priority = Priority.VERY_HIGH) -> int:
        """
        Add seed URLs to the frontier
        
        Args:
            urls: List of URLs to add
            priority: Priority for the seed URLs
            
        Returns:
            Number of URLs added
        """
        added = 0
        for url in urls:
            url_obj = URL(
                url=url,
                status=URLStatus.PENDING,
                priority=priority,
                depth=0  # Seed URLs are at depth 0
            )
            
            # Save URL to database
            try:
                self.urls_collection.update_one(
                    {'url': url},
                    {'$set': url_obj.dict()},
                    upsert=True
                )
            except Exception as e:
                logger.error(f"Error saving seed URL to database: {e}")
            
            # Add to frontier
            if self.frontier.add_url(url_obj):
                added += 1
                self.urls_discovered_counter.inc()
                logger.info(f"Added seed URL: {url}")
        
        return added
    
    def start(self, num_workers: int = None, async_mode: bool = False) -> None:
        """
        Start the crawler
        
        Args:
            num_workers: Number of worker threads
            async_mode: Whether to use async mode
        """
        if self.running:
            logger.warning("Crawler is already running")
            return
        
        num_workers = num_workers or config.MAX_WORKERS
        
        # Reset stop event
        self.stop_event.clear()
        
        # Add seed URLs if frontier is empty
        if self.frontier.size() == 0:
            logger.info("Adding seed URLs")
            self.add_seed_urls(config.SEED_URLS)
        
        # Start crawler
        self.running = True
        
        # Register signal handlers
        self._register_signal_handlers()
        
        logger.info(f"Starting crawler with {num_workers} workers")
        
        if async_mode:
            # Use asyncio for crawler
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self._crawl_async(num_workers))
            except KeyboardInterrupt:
                logger.info("Crawler stopped by user")
            except Exception as e:
                logger.error(f"Error in async crawler: {e}")
                logger.error(traceback.format_exc())
            finally:
                self._cleanup()
        else:
            # Use threads for crawler
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                try:
                    # Submit worker tasks
                    futures = [executor.submit(self._crawl_worker) for _ in range(num_workers)]
                    
                    # Wait for completion
                    for future in futures:
                        future.result()
                except KeyboardInterrupt:
                    logger.info("Crawler stopped by user")
                except Exception as e:
                    logger.error(f"Error in threaded crawler: {e}")
                    logger.error(traceback.format_exc())
                finally:
                    self._cleanup()
    
    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown"""
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, shutting down")
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _crawl_worker(self) -> None:
        """Worker function for threaded crawler"""
        try:
            self.active_threads_gauge.inc()
            
            while self.running and not self.stop_event.is_set():
                # Check if paused
                if self.paused:
                    time.sleep(1)
                    continue
                
                # Get next URL from frontier
                url_obj = self.frontier.get_next_url()
                
                # No URL available, wait and retry
                if url_obj is None:
                    time.sleep(1)
                    continue
                
                try:
                    # Process the URL
                    self._process_url(url_obj)
                    
                    # Update statistics
                    self._update_stats()
                    
                except Exception as e:
                    logger.error(f"Error processing URL {url_obj.url}: {e}")
                    logger.error(traceback.format_exc())
                    
                    # Update URL status to failed
                    self._mark_url_failed(url_obj, str(e))
        except Exception as e:
            logger.error(f"Unhandled error in worker thread: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.active_threads_gauge.dec()
    
    async def _crawl_async(self, num_workers: int) -> None:
        """Async worker function for asyncio crawler"""
        try:
            self.active_threads_gauge.inc(num_workers)
            
            # Create tasks
            tasks = [self._async_worker() for _ in range(num_workers)]
            
            # Wait for all tasks to complete
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Unhandled error in async crawler: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.active_threads_gauge.dec(num_workers)
    
    async def _async_worker(self) -> None:
        """Async worker function"""
        try:
            while self.running and not self.stop_event.is_set():
                # Check if paused
                if self.paused:
                    await asyncio.sleep(1)
                    continue
                
                # Get next URL from frontier
                url_obj = self.frontier.get_next_url()
                
                # No URL available, wait and retry
                if url_obj is None:
                    await asyncio.sleep(1)
                    continue
                
                try:
                    # Process the URL
                    await self._process_url_async(url_obj)
                    
                    # Update statistics
                    self._update_stats()
                    
                except Exception as e:
                    logger.error(f"Error processing URL {url_obj.url}: {e}")
                    logger.error(traceback.format_exc())
                    
                    # Update URL status to failed
                    self._mark_url_failed(url_obj, str(e))
        except Exception as e:
            logger.error(f"Unhandled error in async worker: {e}")
            logger.error(traceback.format_exc())
    
    def _process_url(self, url_obj: URL) -> None:
        """
        Process a URL
        
        Args:
            url_obj: URL object to process
        """
        url = url_obj.url
        logger.debug(f"Processing URL: {url}")
        
        # Download page
        with self.download_time_histogram.time():
            page = self.downloader.download(url_obj)
        
        # If download failed
        if page is None:
            self.pages_failed_counter.inc()
            self.stats['pages_failed'] += 1
            self._mark_url_failed(url_obj, url_obj.error or "Download failed")
            return
        
        # Record page size
        self.page_size_histogram.observe(page.content_length)
        
        # Check for duplicate content
        content_hash = page.content_hash
        duplicate = self._check_duplicate_content(content_hash, url)
        
        if duplicate:
            logger.info(f"Duplicate content detected for URL {url}")
            page.is_duplicate = True
            
            # Mark URL as duplicate but still store the page
            self._mark_url_completed(url_obj)
        else:
            # Parse page and extract URLs
            extracted_urls, metadata = self.parser.parse(page)
            
            # Store page metadata
            page.metadata = metadata
            
            # Process extracted URLs
            self._process_extracted_urls(extracted_urls, url_obj, metadata)
            
            # Mark URL as completed
            self._mark_url_completed(url_obj)
        
        # Store page
        self._store_page(page)
        
        # Update statistics
        self.pages_crawled_counter.inc()
        self.stats['pages_crawled'] += 1
        
        # Add domain to statistics
        domain = url_obj.domain
        self.stats['domains_crawled'].add(domain)
        
        # Update content type statistics
        content_type = page.content_type.split(';')[0].strip()
        self.stats['content_types'][content_type] = self.stats['content_types'].get(content_type, 0) + 1
        
        # Update status code statistics
        status_code = page.status_code
        self.stats['status_codes'][str(status_code)] = self.stats['status_codes'].get(str(status_code), 0) + 1
    
    async def _process_url_async(self, url_obj: URL) -> None:
        """
        Process a URL asynchronously
        
        Args:
            url_obj: URL object to process
        """
        url = url_obj.url
        logger.debug(f"Processing URL (async): {url}")
        
        # Download page
        download_start = time.time()
        page = await self.downloader.download_async(url_obj)
        download_time = time.time() - download_start
        self.download_time_histogram.observe(download_time)
        
        # If download failed
        if page is None:
            self.pages_failed_counter.inc()
            self.stats['pages_failed'] += 1
            self._mark_url_failed(url_obj, url_obj.error or "Download failed")
            return
        
        # Record page size
        self.page_size_histogram.observe(page.content_length)
        
        # Check for duplicate content
        content_hash = page.content_hash
        duplicate = self._check_duplicate_content(content_hash, url)
        
        if duplicate:
            logger.info(f"Duplicate content detected for URL {url}")
            page.is_duplicate = True
            
            # Mark URL as duplicate but still store the page
            self._mark_url_completed(url_obj)
        else:
            # Parse page and extract URLs
            extracted_urls, metadata = self.parser.parse(page)
            
            # Store page metadata
            page.metadata = metadata
            
            # Process extracted URLs
            self._process_extracted_urls(extracted_urls, url_obj, metadata)
            
            # Mark URL as completed
            self._mark_url_completed(url_obj)
        
        # Store page
        self._store_page(page)
        
        # Update statistics
        self.pages_crawled_counter.inc()
        self.stats['pages_crawled'] += 1
    
    def _check_duplicate_content(self, content_hash: str, url: str) -> bool:
        """
        Check if content has been seen before
        
        Args:
            content_hash: Hash of the content
            url: URL of the page
            
        Returns:
            True if content is a duplicate, False otherwise
        """
        try:
            # Check if content hash exists in database
            existing = self.pages_collection.find_one(
                {'content_hash': content_hash, 'url': {'$ne': url}}
            )
            return existing is not None
        except Exception as e:
            logger.error(f"Error checking duplicate content: {e}")
            return False
    
    def _process_extracted_urls(self, urls: List[str], parent_url_obj: URL, metadata: Dict[str, Any]) -> None:
        """
        Process extracted URLs
        
        Args:
            urls: List of URLs to process
            parent_url_obj: Parent URL object
            metadata: Metadata from the parent page
        """
        parent_url = parent_url_obj.url
        parent_depth = parent_url_obj.depth
        
        # Check max depth
        if parent_depth >= config.MAX_DEPTH:
            logger.debug(f"Max depth reached for {parent_url}")
            return
        
        for url in urls:
            # Calculate priority based on URL and metadata
            priority = self.parser.calculate_priority(url, metadata)
            
            # Create URL object
            url_obj = URL(
                url=url,
                status=URLStatus.PENDING,
                priority=priority,
                depth=parent_depth + 1,
                parent_url=parent_url
            )
            
            # Add to frontier
            if self.frontier.add_url(url_obj):
                # URL was added to frontier
                self.urls_discovered_counter.inc()
                self.stats['urls_discovered'] += 1
                
                # Save URL to database
                try:
                    self.urls_collection.update_one(
                        {'url': url},
                        {'$set': url_obj.dict()},
                        upsert=True
                    )
                except Exception as e:
                    logger.error(f"Error saving URL to database: {e}")
            else:
                # URL was not added (filtered or duplicate)
                self.urls_filtered_counter.inc()
                self.stats['urls_filtered'] += 1
    
    def _mark_url_completed(self, url_obj: URL) -> None:
        """
        Mark URL as completed
        
        Args:
            url_obj: URL object to mark as completed
        """
        try:
            url_obj.status = URLStatus.COMPLETED
            url_obj.last_crawled = datetime.datetime.now()
            
            # Update in database
            self.urls_collection.update_one(
                {'url': url_obj.url},
                {'$set': {
                    'status': url_obj.status,
                    'last_crawled': url_obj.last_crawled
                }}
            )
        except Exception as e:
            logger.error(f"Error marking URL as completed: {e}")
    
    def _mark_url_failed(self, url_obj: URL, error: str) -> None:
        """
        Mark URL as failed
        
        Args:
            url_obj: URL object to mark as failed
            error: Error message
        """
        try:
            url_obj.status = URLStatus.FAILED
            url_obj.error = error
            url_obj.last_crawled = datetime.datetime.now()
            url_obj.retries += 1
            
            # Update in database
            self.urls_collection.update_one(
                {'url': url_obj.url},
                {'$set': {
                    'status': url_obj.status,
                    'error': url_obj.error,
                    'last_crawled': url_obj.last_crawled,
                    'retries': url_obj.retries
                }}
            )
            
            # If retries not exceeded, add back to frontier with lower priority
            if url_obj.retries < config.RETRY_TIMES:
                # Lower priority by one level (to a maximum of VERY_LOW)
                new_priority = min(Priority.VERY_LOW, Priority(url_obj.priority + 1))
                url_obj.priority = new_priority
                url_obj.status = URLStatus.PENDING
                
                # Add back to frontier
                self.frontier.add_url(url_obj)
                
        except Exception as e:
            logger.error(f"Error marking URL as failed: {e}")
    
    def _store_page(self, page: Page) -> None:
        """
        Store a page in the database and optionally on disk
        
        Args:
            page: Page object to store
        """
        try:
            # Convert page to dict for MongoDB
            page_dict = page.dict()
            
            # Store in MongoDB
            self.pages_collection.update_one(
                {'url': page.url},
                {'$set': page_dict},
                upsert=True
            )
            
            # Optionally store HTML content on disk
            if not page.is_duplicate:
                domain = self._extract_domain(page.url)
                domain_dir = os.path.join(config.HTML_STORAGE_PATH, domain)
                os.makedirs(domain_dir, exist_ok=True)
                
                # Create filename from URL
                filename = self._url_to_filename(page.url)
                filepath = os.path.join(domain_dir, filename)
                
                # Write HTML to file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(page.content)
                
                logger.debug(f"Stored HTML content for {page.url} at {filepath}")
        except Exception as e:
            logger.error(f"Error storing page {page.url}: {e}")
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        parsed = urlparse(url)
        return parsed.netloc.replace(':', '_')
    
    def _url_to_filename(self, url: str) -> str:
        """Convert URL to filename"""
        # Hash the URL to create a safe filename
        url_hash = self._hash_url(url)
        return f"{url_hash}.html"
    
    def _hash_url(self, url: str) -> str:
        """Create a hash of a URL"""
        import hashlib
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def _update_stats(self) -> None:
        """Update and log statistics"""
        # Update frontier size gauge
        self.frontier_size_gauge.set(self.frontier.size())
        
        # Log statistics periodically
        if self.stats['pages_crawled'] % 100 == 0:
            self._log_stats()
    
    def _log_stats(self) -> None:
        """Log crawler statistics"""
        # Calculate elapsed time
        elapsed = time.time() - self.stats['start_time']
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Get current statistics
        pages_crawled = self.stats['pages_crawled']
        pages_failed = self.stats['pages_failed']
        urls_discovered = self.stats['urls_discovered']
        urls_filtered = self.stats['urls_filtered']
        domains_crawled = len(self.stats['domains_crawled'])
        frontier_size = self.frontier.size()
        
        # Calculate pages per second
        pages_per_second = pages_crawled / elapsed if elapsed > 0 else 0
        
        # Log statistics
        logger.info(
            f"Crawler running for {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} - "
            f"Pages: {pages_crawled} ({pages_per_second:.2f}/s) - "
            f"Failed: {pages_failed} - "
            f"URLs Discovered: {urls_discovered} - "
            f"URLs Filtered: {urls_filtered} - "
            f"Domains: {domains_crawled} - "
            f"Frontier: {frontier_size}"
        )
        
        # Save statistics to database
        try:
            stats_copy = self.stats.copy()
            stats_copy['domains_crawled'] = list(stats_copy['domains_crawled'])
            stats_copy['timestamp'] = datetime.datetime.now()
            
            self.stats_collection.insert_one(stats_copy)
        except Exception as e:
            logger.error(f"Error saving statistics to database: {e}")
    
    def stop(self) -> None:
        """Stop the crawler"""
        if not self.running:
            logger.warning("Crawler is not running")
            return
        
        logger.info("Stopping crawler")
        self.stop_event.set()
        self.running = False
    
    def pause(self) -> None:
        """Pause the crawler"""
        if not self.running:
            logger.warning("Crawler is not running")
            return
        
        logger.info("Pausing crawler")
        self.paused = True
    
    def resume(self) -> None:
        """Resume the crawler"""
        if not self.running:
            logger.warning("Crawler is not running")
            return
        
        logger.info("Resuming crawler")
        self.paused = False
    
    def checkpoint(self) -> bool:
        """
        Save crawler state for recovery
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Creating crawler checkpoint")
        
        # Checkpoint the frontier
        frontier_checkpoint = self.frontier.checkpoint()
        
        # Save current statistics
        try:
            stats_copy = self.stats.copy()
            stats_copy['domains_crawled'] = list(stats_copy['domains_crawled'])
            stats_copy['checkpoint_time'] = datetime.datetime.now()
            
            with open(os.path.join(config.STORAGE_PATH, 'crawler_stats.json'), 'w') as f:
                json.dump(stats_copy, f)
            
            logger.info("Crawler checkpoint created")
            return frontier_checkpoint
        except Exception as e:
            logger.error(f"Error creating crawler checkpoint: {e}")
            return False
    
    def restore(self) -> bool:
        """
        Restore crawler state from checkpoint
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Restoring crawler from checkpoint")
        
        # Restore frontier
        frontier_restored = self.frontier.restore()
        
        # Restore statistics
        try:
            stats_path = os.path.join(config.STORAGE_PATH, 'crawler_stats.json')
            if os.path.exists(stats_path):
                with open(stats_path, 'r') as f:
                    saved_stats = json.load(f)
                
                # Restore stats
                self.stats = saved_stats
                self.stats['domains_crawled'] = set(self.stats['domains_crawled'])
                
                logger.info("Crawler statistics restored")
            else:
                logger.warning("No statistics checkpoint found")
            
            return frontier_restored
        except Exception as e:
            logger.error(f"Error restoring crawler checkpoint: {e}")
            return False
    
    def _cleanup(self) -> None:
        """Clean up resources when crawler stops"""
        # Create final checkpoint
        self.checkpoint()
        
        # Log final statistics
        self._log_stats()
        
        # Reset flags
        self.running = False
        self.paused = False
        
        logger.info("Crawler stopped")


if __name__ == "__main__":
    # Create and start crawler
    crawler = Crawler()
    
    try:
        crawler.start()
    except KeyboardInterrupt:
        logger.info("Crawler interrupted by user")
    finally:
        crawler.stop() 