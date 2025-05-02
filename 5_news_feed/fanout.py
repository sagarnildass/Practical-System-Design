"""
Fanout service for the news feed system.

This module implements the fanout service for the news feed system,
which distributes posts to follower feeds.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty

from config import FANOUT
from models import RelationshipType

logger = logging.getLogger(__name__)


class FanoutService:
    """
    Fanout service for distributing posts to follower feeds.
    
    The fanout service uses different strategies based on the user's popularity:
    - For regular users: push model (eager)
    - For celebrity users: pull model (lazy)
    """
    
    def __init__(self, db_service, cache_service):
        """
        Initialize the fanout service.
        
        Args:
            db_service: Database service for retrieving user relationships
            cache_service: Cache service for storing feeds
        """
        self.db = db_service
        self.cache = cache_service
        self.queue = Queue()
        self.running = False
        self.thread = None
        
        # Thread pool for parallel fanout
        self.executor = ThreadPoolExecutor(
            max_workers=FANOUT["num_workers"],
            thread_name_prefix="fanout_worker"
        )
        
        # Statistics
        self.stats = {
            "total_fanouts": 0,
            "eager_fanouts": 0,
            "lazy_fanouts": 0,
            "feeds_updated": 0,
            "avg_fanout_time": 0,
            "last_fanout_time": 0
        }
        
        # Lock for thread-safe statistics updates
        self.stats_lock = threading.Lock()
        
        # Start the service automatically
        self.start()
        
    def start(self):
        """
        Start the fanout service.
        """
        if self.running:
            logger.warning("Fanout service is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._process_queue, name="fanout_service")
        self.thread.daemon = True
        self.thread.start()
        logger.info("Fanout service started")
        
    def stop(self):
        """
        Stop the fanout service.
        """
        if not self.running:
            logger.warning("Fanout service is not running")
            return
            
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
            logger.info("Fanout service stopped")
            
    def _process_queue(self):
        """
        Process the fanout queue.
        """
        while self.running:
            try:
                # Get the next fanout task
                task = self.queue.get(timeout=1)
                
                if task:
                    user_id, post_id, timestamp = task
                    self._execute_fanout(user_id, post_id, timestamp)
                    
                self.queue.task_done()
            except Empty:
                # No task available, just continue waiting
                pass
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    logger.error(f"Error processing fanout queue: {e}")
                    
                # Make sure task_done is called if there was a task
                try:
                    self.queue.task_done()
                except:
                    pass
            
    def _execute_fanout(self, user_id, post_id, timestamp):
        """
        Execute the fanout operation for a post.
        
        Args:
            user_id: ID of the user who created the post
            post_id: ID of the post to distribute
            timestamp: Timestamp of the post
        """
        start_time = time.time()
        
        # Determine if this is a celebrity user
        follower_count = self.db.get_follower_count(user_id)
        is_celebrity = follower_count > FANOUT["celebrity_threshold"]
        
        # Update statistics
        with self.stats_lock:
            self.stats["total_fanouts"] += 1
            
            if is_celebrity:
                self.stats["lazy_fanouts"] += 1
            else:
                self.stats["eager_fanouts"] += 1
                
        # Add post to user's own feed
        self.cache.news_feed.add_post_to_feed(user_id, post_id, timestamp)
        
        # For celebrity users, use pull model (lazy)
        if is_celebrity:
            logger.debug(f"Using lazy fanout for user {user_id} with {follower_count} followers")
            # Store the post in the cache
            post = self.db.get_post(post_id)
            self.cache.content.set_post(post)
            
            # Mark this user as a celebrity in the cache
            self.cache.redis.set(f"celebrity:{user_id}", 1, 
                                ex=self.cache.REDIS["expiration"]["user"])
            
        # For regular users, use push model (eager)
        else:
            logger.debug(f"Using eager fanout for user {user_id} with {follower_count} followers")
            followers = self.db.get_followers(user_id)
            
            # Group followers into batches
            batch_size = FANOUT["batch_size"]
            follower_batches = [
                followers[i:i + batch_size] 
                for i in range(0, len(followers), batch_size)
            ]
            
            # Submit batches to thread pool
            futures = []
            for batch in follower_batches:
                future = self.executor.submit(
                    self._fanout_batch, user_id, post_id, timestamp, batch
                )
                futures.append(future)
                
            # Wait for all fanout operations to complete
            feeds_updated = 0
            for future in futures:
                try:
                    feeds_updated += future.result()
                except Exception as e:
                    logger.error(f"Error in fanout batch: {e}")
                    
            # Update statistics
            with self.stats_lock:
                self.stats["feeds_updated"] += feeds_updated
                
        # Record fanout time
        fanout_time = time.time() - start_time
        with self.stats_lock:
            self.stats["last_fanout_time"] = fanout_time
            # Update running average
            n = self.stats["total_fanouts"]
            self.stats["avg_fanout_time"] = (
                (self.stats["avg_fanout_time"] * (n - 1) + fanout_time) / n
            )
            
        logger.debug(f"Fanout completed for post {post_id} in {fanout_time:.2f}s")
            
    def _fanout_batch(self, user_id, post_id, timestamp, followers):
        """
        Distribute a post to a batch of followers.
        
        Args:
            user_id: ID of the user who created the post
            post_id: ID of the post to distribute
            timestamp: Timestamp of the post
            followers: List of follower IDs
            
        Returns:
            Number of feeds updated
        """
        updated = 0
        for follower_id in followers:
            try:
                # Check for blocked relationship - handle different possible formats
                try:
                    rel_type = self.db.get_relationship_type(follower_id, user_id)
                    
                    # Handle different possible formats of relationship type
                    is_blocked = False
                    if rel_type is not None:
                        # Check if it's an enum
                        if hasattr(rel_type, 'value') and hasattr(RelationshipType, 'BLOCK'):
                            is_blocked = (rel_type == RelationshipType.BLOCK)
                        # Check if it's a string matching the enum value
                        elif isinstance(rel_type, str) and hasattr(RelationshipType, 'BLOCK'):
                            is_blocked = (rel_type == RelationshipType.BLOCK.value or 
                                         rel_type.upper() == 'BLOCK')
                        # Direct string comparison as a fallback
                        elif isinstance(rel_type, str):
                            is_blocked = rel_type.upper() == 'BLOCK'
                            
                    if is_blocked:
                        logger.debug(f"Skipping blocked user {follower_id} for post {post_id}")
                        continue
                        
                except Exception as e:
                    logger.warning(f"Error checking relationship between {follower_id} and {user_id}: {e}")
                    # Continue with fanout even if we can't check relationship, but log warning
                
                # Add post to follower's feed safely
                try:
                    # First try to add to database for persistence
                    self.db.add_to_feed(follower_id, post_id)
                except Exception as e:
                    logger.warning(f"Error adding post {post_id} to database feed for user {follower_id}: {e}")
                
                # Then try to add to cache for fast retrieval
                try:
                    success = self.cache.news_feed.add_post_to_feed(
                        follower_id, post_id, timestamp
                    )
                    
                    if success:
                        updated += 1
                except Exception as e:
                    logger.warning(f"Error adding post {post_id} to cache feed for user {follower_id}: {e}")
                    
            except Exception as e:
                logger.error(f"Error adding post {post_id} to feed for user {follower_id}: {e}")
                
        return updated
        
    def fanout_post(self, user_id, post_id, timestamp):
        """
        Queue a post for fanout.
        
        Args:
            user_id: ID of the user who created the post
            post_id: ID of the post to distribute
            timestamp: Timestamp of the post
        """
        # Queue the fanout task
        self.queue.put((user_id, post_id, timestamp))
        logger.debug(f"Post {post_id} queued for fanout")
        
        # For testing purposes, immediately execute the fanout
        self._execute_fanout(user_id, post_id, timestamp)
        
    def get_stats(self):
        """
        Get fanout service statistics.
        
        Returns:
            Dictionary of statistics
        """
        with self.stats_lock:
            return dict(self.stats)


class FanoutManager:
    """
    Manager for the fanout service.
    
    This class provides an interface for starting and stopping the fanout service.
    """
    
    def __init__(self, db_service, cache_service):
        """
        Initialize the fanout manager.
        """
        self.service = FanoutService(db_service, cache_service)
        
    def start(self):
        """
        Start the fanout service.
        """
        self.service.start()
        
    def stop(self):
        """
        Stop the fanout service.
        """
        self.service.stop()
        
    def is_running(self):
        """
        Check if the fanout service is running.
        
        Returns:
            True if the fanout service is running, False otherwise
        """
        return self.service.running
        
    def fanout_post(self, user_id, post_id, timestamp):
        """
        Queue a post for fanout.
        
        Args:
            user_id: ID of the user who created the post
            post_id: ID of the post to distribute
            timestamp: Timestamp of the post
        """
        self.service.fanout_post(user_id, post_id, timestamp)
        
    def get_stats(self):
        """
        Get fanout service statistics.
        
        Returns:
            Dictionary of statistics
        """
        return self.service.get_stats() 