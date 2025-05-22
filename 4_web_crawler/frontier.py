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
import json

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
    and enable distributed crawling. In deployment mode, it can also use
    in-memory storage.
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None, use_memory: bool = False):
        """Initialize the URL Frontier"""
        self.use_memory = use_memory
        if use_memory:
            # Initialize in-memory storage
            self.memory_storage = {
                'seen_urls': set(),
                'priority_queues': [[] for _ in range(config.PRIORITY_QUEUE_NUM)],
                'host_queues': [[] for _ in range(config.HOST_QUEUE_NUM)]
            }
        else:
            # Use Redis
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
        if not self.use_memory:
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
    
    def add_url(self, url_obj: URL) -> bool:
        """Add a URL to the frontier"""
        with self.lock:
            url = url_obj.url
            
            # Check if URL has been seen
            if self.use_memory:
                if url in self.memory_storage['seen_urls']:
                    return False
                self.memory_storage['seen_urls'].add(url)
            else:
                if self.use_simple_mode:
                    if self.redis.sismember(self.url_seen_key, url):
                        return False
                    self.redis.sadd(self.url_seen_key, url)
                else:
                    if self._check_url_seen(url):
                        return False
                    self._mark_url_seen(url)
            
            # Add to priority queue
            priority_index = url_obj.priority.value % self.priority_count
            if self.use_memory:
                self.memory_storage['priority_queues'][priority_index].append(url_obj)
            else:
                priority_key = f"{self.priority_queue_key_prefix}{priority_index}"
                self.redis.rpush(priority_key, url_obj.json())
            
            return True
    
    def get_next_url(self) -> Optional[URL]:
        """Get the next URL to crawl"""
        with self.lock:
            # Try each priority queue
            for i in range(self.priority_count):
                if self.use_memory:
                    queue = self.memory_storage['priority_queues'][i]
                    if queue:
                        return queue.pop(0)
                else:
                    priority_key = f"{self.priority_queue_key_prefix}{i}"
                    url_data = self.redis.lpop(priority_key)
                    if url_data:
                        return URL.parse_raw(url_data)
            return None
    
    def _check_url_seen(self, url: str) -> bool:
        """Check if URL has been seen"""
        if self.use_memory:
            return url in self.memory_storage['seen_urls']
        elif self.use_simple_mode:
            return self.redis.sismember(self.url_seen_key, url)
        else:
            # Using Redis Bloom filter
            return bool(self.redis.getbit(self.url_seen_key, self._hash_url(url)))
    
    def _mark_url_seen(self, url: str) -> None:
        """Mark URL as seen"""
        if self.use_memory:
            self.memory_storage['seen_urls'].add(url)
        elif self.use_simple_mode:
            self.redis.sadd(self.url_seen_key, url)
        else:
            # Using Redis Bloom filter
            self.redis.setbit(self.url_seen_key, self._hash_url(url), 1)
    
    def _hash_url(self, url: str) -> int:
        """Hash URL for Bloom filter"""
        return hash(url) % (1 << 32)  # 32-bit hash
    
    def size(self) -> int:
        """Get the total size of all queues"""
        if self.use_memory:
            return sum(len(q) for q in self.memory_storage['priority_queues'])
        else:
            total = 0
            for i in range(self.priority_count):
                priority_key = f"{self.priority_queue_key_prefix}{i}"
                total += self.redis.llen(priority_key)
            return total
    
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
    
    def checkpoint(self) -> bool:
        """Save frontier state"""
        if self.use_memory:
            # No need to checkpoint in-memory storage
            return True
            
        try:
            # Save priority queues
            for i in range(self.priority_count):
                priority_key = f"{self.priority_queue_key_prefix}{i}"
                queue_data = []
                while True:
                    url_data = self.redis.lpop(priority_key)
                    if not url_data:
                        break
                    queue_data.append(url_data)
                
                # Save to file
                checkpoint_file = os.path.join(config.STORAGE_PATH, f"priority_queue_{i}.json")
                with open(checkpoint_file, 'w') as f:
                    json.dump(queue_data, f)
                
                # Restore queue
                for url_data in reversed(queue_data):
                    self.redis.rpush(priority_key, url_data)
            
            return True
        except Exception as e:
            logger.error(f"Error creating frontier checkpoint: {e}")
            return False
    
    def restore(self) -> bool:
        """Restore frontier state"""
        if self.use_memory:
            # No need to restore in-memory storage
            return True
            
        try:
            # Restore priority queues
            for i in range(self.priority_count):
                checkpoint_file = os.path.join(config.STORAGE_PATH, f"priority_queue_{i}.json")
                if os.path.exists(checkpoint_file):
                    with open(checkpoint_file, 'r') as f:
                        queue_data = json.load(f)
                    
                    # Clear existing queue
                    priority_key = f"{self.priority_queue_key_prefix}{i}"
                    self.redis.delete(priority_key)
                    
                    # Restore queue
                    for url_data in queue_data:
                        self.redis.rpush(priority_key, url_data)
            
            return True
        except Exception as e:
            logger.error(f"Error restoring frontier checkpoint: {e}")
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