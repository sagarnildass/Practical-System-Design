"""
URL Frontier implementation for web crawler

The URL Frontier maintains URLs to be crawled with two main goals:
1. Prioritization - Important URLs are crawled first
2. Politeness - Avoid overloading web servers with too many requests
"""

import time
import logging
import heapq
import pickle
import threading
import random
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import deque
import redis
from redis.exceptions import RedisError
import mmh3
import os

from models import URL, Priority, URLStatus
import config

# Import local configuration if available
try:
    import local_config
    # Override config settings with local settings
    for key in dir(local_config):
        if key.isupper():
            setattr(config, key, getattr(local_config, key))
    logging.info("Loaded local configuration")
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class URLFrontier:
    """
    URL Frontier implementation with prioritization and politeness
    
    Architecture:
    - Front queues: Priority-based queues
    - Back queues: Host-based queues for politeness
    
    This uses Redis for persistent storage to handle large number of URLs
    and enable distributed crawling.
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """Initialize the URL Frontier"""
        self.redis = redis_client or redis.from_url(config.REDIS_URI)
        self.priority_count = config.PRIORITY_QUEUE_NUM  # Number of priority queues
        self.host_count = config.HOST_QUEUE_NUM  # Number of host queues
        self.url_seen_key = "webcrawler:url_seen"  # Bloom filter for seen URLs
        self.priority_queue_key_prefix = "webcrawler:priority_queue:"
        self.host_queue_key_prefix = "webcrawler:host_queue:"
        self.lock = threading.RLock()  # Thread-safe operations
        
        # Simple mode uses Redis Set instead of Bloom filter
        self.use_simple_mode = getattr(config, 'USE_SIMPLE_URL_SEEN', False)
        logger.info(f"URLFrontier using simple mode: {self.use_simple_mode}")
        
        # Ensure directory for checkpoint exists
        if not os.path.exists(config.STORAGE_PATH):
            os.makedirs(config.STORAGE_PATH)
        
        # Initialize URL seen storage
        self._init_url_seen()
    
    def _init_url_seen(self):
        """Initialize URL seen storage based on configuration"""
        try:
            # If using simple mode, just use a Redis set
            if self.use_simple_mode:
                if not self.redis.exists(self.url_seen_key):
                    logger.info("Initializing URL seen set")
                    self.redis.sadd(self.url_seen_key, "initialized")
                return
                
            # Try to use Bloom filter
            if not self.redis.exists(self.url_seen_key):
                logger.info("Initializing URL seen bloom filter")
                try:
                    # Use a bloom filter with 100 million items and 0.01 false positive rate
                    # This requires approximately 119.5 MB of memory
                    self.redis.execute_command("BF.RESERVE", self.url_seen_key, 0.01, 100000000)
                except RedisError as e:
                    logger.error(f"Failed to initialize bloom filter: {e}")
                    logger.info("Falling back to simple set for URL seen detection")
                    self.use_simple_mode = True
                    # Initialize a set instead
                    if not self.redis.exists(self.url_seen_key):
                        self.redis.sadd(self.url_seen_key, "initialized")
        except RedisError as e:
            logger.error(f"Error initializing URL seen: {e}")
            # Fallback to set if bloom filter is not available
            self.use_simple_mode = True
            if not self.redis.exists(self.url_seen_key):
                self.redis.sadd(self.url_seen_key, "initialized")
    
    def add_url(self, url: URL) -> bool:
        """
        Add a URL to the frontier
        
        Args:
            url: URL object to add
            
        Returns:
            bool: True if added, False if already seen or error
        """
        try:
            with self.lock:
                logger.debug(f"Attempting to add URL to frontier: {url.url}")
                
                # Check if URL has been seen
                if self._is_url_seen(url.normalized_url):
                    logger.debug(f"URL already seen, skipping: {url.url}")
                    return False
                
                # Mark URL as seen
                self._mark_url_seen(url.normalized_url)
                
                # Convert URL to serialized format
                url_data = pickle.dumps(url)
                
                # Add to priority queue
                priority_queue = self._get_priority_queue_key(url.priority)
                self.redis.rpush(priority_queue, url_data)
                
                logger.debug(f"Successfully added URL to frontier: {url.url} (priority: {url.priority})")
                return True
        except Exception as e:
            logger.error(f"Error adding URL to frontier: {e}")
            return False
    
    def get_next_url(self) -> Optional[URL]:
        """
        Get the next URL to crawl, respecting prioritization and politeness
        
        Returns:
            URL object or None if no URLs are available
        """
        try:
            with self.lock:
                # First, select a priority queue using biased random selection
                priority_queue = self._select_priority_queue()
                if not priority_queue:
                    logger.debug("No priority queue available")
                    return None
                
                logger.debug(f"Selected priority queue: {priority_queue}")
                
                # Get URL from the priority queue
                url_data = self.redis.lpop(priority_queue)
                if not url_data:
                    logger.debug(f"No URLs in priority queue: {priority_queue}")
                    return None
                
                # Deserialize URL
                url = pickle.loads(url_data)
                logger.debug(f"Got URL from frontier: {url.url}")
                
                # Add to host queue for politeness
                host_queue = self._get_host_queue_key(url.domain)
                self.redis.rpush(host_queue, url_data)
                
                # Get the timestamp of the last request to this host
                last_access_key = f"webcrawler:last_access:{url.domain}"
                last_access = self.redis.get(last_access_key)
                
                # If host was accessed recently, apply delay
                if last_access:
                    last_time = float(last_access)
                    current_time = time.time()
                    elapsed = current_time - last_time
                    
                    # Apply crawl delay if needed
                    crawl_delay = config.DOWNLOAD_DELAY
                    if elapsed < crawl_delay:
                        logger.debug(f"Respecting crawl delay for {url.domain}, waiting {crawl_delay - elapsed:.2f}s")
                        # Put URL back in the priority queue
                        self.redis.lpush(priority_queue, url_data)
                        # Return None to indicate no URL is ready
                        return None
                
                # Update last access time for the host
                self.redis.set(f"webcrawler:last_access:{url.domain}", time.time())
                
                # Update URL status to in progress
                url.status = URLStatus.IN_PROGRESS
                
                return url
                
        except Exception as e:
            logger.error(f"Error getting next URL from frontier: {e}")
            return None
    
    def _is_url_seen(self, url: str) -> bool:
        """Check if URL has been seen"""
        try:
            if self.use_simple_mode:
                # Use simple Redis set
                return bool(self.redis.sismember(self.url_seen_key, url))
            else:
                # Try using bloom filter
                return bool(self.redis.execute_command("BF.EXISTS", self.url_seen_key, url))
        except RedisError:
            # Fallback to set
            return bool(self.redis.sismember(self.url_seen_key, url))
    
    def _mark_url_seen(self, url: str) -> None:
        """Mark URL as seen"""
        try:
            if self.use_simple_mode:
                # Use simple Redis set
                self.redis.sadd(self.url_seen_key, url)
            else:
                # Try using bloom filter
                try:
                    self.redis.execute_command("BF.ADD", self.url_seen_key, url)
                except RedisError:
                    # Fallback to set
                    self.redis.sadd(self.url_seen_key, url)
        except RedisError:
            # Fallback to set
            self.redis.sadd(self.url_seen_key, url)
    
    def _get_priority_queue_key(self, priority: Priority) -> str:
        """Get Redis key for priority queue"""
        return f"{self.priority_queue_key_prefix}{priority.value}"
    
    def _get_host_queue_key(self, domain: str) -> str:
        """Get Redis key for host queue using consistent hashing"""
        host_hash = mmh3.hash(domain) % self.host_count
        return f"{self.host_queue_key_prefix}{host_hash}"
    
    def _select_priority_queue(self) -> Optional[str]:
        """
        Select a priority queue using biased random selection
        
        Higher priority queues have higher probability of being selected
        """
        # Check which queues have items
        non_empty_queues = []
        for priority in range(1, self.priority_count + 1):
            queue_key = f"{self.priority_queue_key_prefix}{priority}"
            queue_length = self.redis.llen(queue_key)
            if queue_length > 0:
                # Use inverted priority as weight (priority 1 has highest weight)
                weight = self.priority_count - priority + 1
                non_empty_queues.append((queue_key, weight))
        
        if not non_empty_queues:
            return None
        
        # Select queue using weighted random selection
        total_weight = sum(weight for _, weight in non_empty_queues)
        rand_val = random.random() * total_weight
        cumulative_weight = 0
        
        for queue_key, weight in non_empty_queues:
            cumulative_weight += weight
            if rand_val <= cumulative_weight:
                return queue_key
        
        # Fallback: return the first non-empty queue
        return non_empty_queues[0][0]
    
    def size(self) -> int:
        """Get the total number of URLs in the frontier"""
        total = 0
        try:
            # Count URLs in priority queues
            for priority in range(1, self.priority_count + 1):
                queue_key = f"{self.priority_queue_key_prefix}{priority}"
                total += self.redis.llen(queue_key)
            
            # Count URLs in host queues
            for host_id in range(self.host_count):
                queue_key = f"{self.host_queue_key_prefix}{host_id}"
                total += self.redis.llen(queue_key)
                
            return total
        except RedisError as e:
            logger.error(f"Error getting frontier size: {e}")
            return -1
            
    def get_stats(self) -> Dict[str, Any]:
        """Get frontier statistics"""
        stats = {
            "size": self.size(),
            "priority_queues": {},
            "host_queues": {},
        }
        
        try:
            # Get priority queue stats
            for priority in range(1, self.priority_count + 1):
                queue_key = f"{self.priority_queue_key_prefix}{priority}"
                stats["priority_queues"][f"priority_{priority}"] = self.redis.llen(queue_key)
            
            # Get host queue stats (just count total host queues with items)
            host_queue_count = 0
            for host_id in range(self.host_count):
                queue_key = f"{self.host_queue_key_prefix}{host_id}"
                if self.redis.llen(queue_key) > 0:
                    host_queue_count += 1
            
            stats["host_queues"]["count_with_items"] = host_queue_count
            
            # Add URLs seen count if using simple mode
            if self.use_simple_mode:
                stats["urls_seen"] = self.redis.scard(self.url_seen_key)
            
            return stats
        except RedisError as e:
            logger.error(f"Error getting frontier stats: {e}")
            return stats
    
    def checkpoint(self, filepath: Optional[str] = None) -> bool:
        """
        Save frontier state to disk for recovery
        
        Args:
            filepath: Path to save the checkpoint file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not filepath:
            filepath = os.path.join(config.STORAGE_PATH, "frontier_checkpoint.pkl")
            
        try:
            # Get all queue keys
            keys = []
            for priority in range(1, self.priority_count + 1):
                keys.append(f"{self.priority_queue_key_prefix}{priority}")
            
            for host_id in range(self.host_count):
                keys.append(f"{self.host_queue_key_prefix}{host_id}")
            
            # Get queue contents
            queues = {}
            for key in keys:
                queue_content = self.redis.lrange(key, 0, -1)
                if queue_content:
                    queues[key] = queue_content
            
            # Save to file
            with open(filepath, 'wb') as f:
                pickle.dump(queues, f)
                
            logger.info(f"Frontier checkpoint saved to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving frontier checkpoint: {e}")
            return False
    
    def restore(self, filepath: Optional[str] = None) -> bool:
        """
        Restore frontier state from checkpoint
        
        Args:
            filepath: Path to the checkpoint file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not filepath:
            filepath = os.path.join(config.STORAGE_PATH, "frontier_checkpoint.pkl")
            
        if not os.path.exists(filepath):
            logger.warning(f"Checkpoint file not found: {filepath}")
            return False
            
        try:
            # Load queues from file
            with open(filepath, 'rb') as f:
                queues = pickle.load(f)
            
            # Restore queues
            for key, queue_content in queues.items():
                # Delete existing queue
                self.redis.delete(key)
                
                # Restore queue content
                if queue_content:
                    self.redis.rpush(key, *queue_content)
            
            logger.info(f"Frontier restored from checkpoint: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error restoring frontier from checkpoint: {e}")
            return False
    
    def clear(self) -> bool:
        """
        Clear all queues in the frontier
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Delete all queue keys
            keys = []
            for priority in range(1, self.priority_count + 1):
                keys.append(f"{self.priority_queue_key_prefix}{priority}")
            
            for host_id in range(self.host_count):
                keys.append(f"{self.host_queue_key_prefix}{host_id}")
            
            if keys:
                self.redis.delete(*keys)
            
            # Reset URL seen filter (optional)
            self.redis.delete(self.url_seen_key)
            
            logger.info("Frontier cleared")
            return True
        except Exception as e:
            logger.error(f"Error clearing frontier: {e}")
            return False 