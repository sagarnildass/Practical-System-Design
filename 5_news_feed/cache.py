"""
Cache module for the news feed system.

This module provides a Redis-based caching layer for the news feed system,
implementing the 5-tier cache architecture described in the design.
"""

import json
import logging
import redis
import time

from config import REDIS

logger = logging.getLogger(__name__)


class Cache:
    """
    Main cache interface for the news feed system.
    """
    
    def __init__(self):
        """
        Initialize cache connection to Redis.
        """
        try:
            self.redis = redis.Redis(
                host=REDIS["host"],
                port=REDIS["port"],
                db=REDIS["db"],
                password=REDIS["password"],
                decode_responses=True
            )
            logger.info("Cache connection established")
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None

    def is_connected(self):
        """
        Check if the cache is connected.
        """
        if not self.redis:
            return False
            
        try:
            return self.redis.ping()
        except redis.RedisError:
            return False


class NewsFeedCache(Cache):
    """
    Cache implementation for news feed data.
    
    Stores ordered lists of post IDs for each user's feed.
    Uses Redis sorted sets with post timestamps as score.
    """
    
    def add_post_to_feed(self, user_id, post_id, timestamp):
        """
        Add a post to a user's news feed.
        
        Args:
            user_id: The ID of the user whose feed to update
            post_id: The ID of the post to add
            timestamp: The timestamp of the post (used for sorting)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert timestamp to float for Redis sorted set score
            score = time.mktime(timestamp.timetuple())
            
            # Add post to user's feed sorted set
            key = f"feed:{user_id}"
            self.redis.zadd(key, {post_id: score})
            
            # Trim feed to max size
            self.redis.zremrangebyrank(key, 0, -REDIS["max_feed_size"]-1)
            
            # Set expiration
            self.redis.expire(key, REDIS["expiration"]["news_feed"])
            
            return True
        except Exception as e:
            logger.error(f"Error adding post to feed: {e}")
            return False
    
    def get_news_feed(self, user_id, offset=0, limit=20):
        """
        Get the news feed for a user.
        
        Args:
            user_id: The ID of the user
            offset: Starting position for pagination
            limit: Maximum number of posts to return
            
        Returns:
            List of post IDs in reverse chronological order
        """
        try:
            # Get posts from feed, newest first
            key = f"feed:{user_id}"
            post_ids = self.redis.zrevrange(key, offset, offset + limit - 1)
            
            # Convert all IDs to strings
            post_ids = [str(post_id) for post_id in post_ids]
            
            # Refresh expiration
            if post_ids:
                self.redis.expire(key, REDIS["expiration"]["news_feed"])
            
            return post_ids
        except Exception as e:
            logger.error(f"Error retrieving news feed: {e}")
            return []
    
    def remove_post_from_feeds(self, post_id):
        """
        Remove a post from all feeds (e.g., when post is deleted).
        
        NOTE: This operation can be computationally expensive in a large system.
        In production, this might be handled differently (e.g., through cleanup jobs).
        
        Args:
            post_id: The ID of the post to remove
            
        Returns:
            Number of feeds updated
        """
        try:
            # Find all feed keys
            feed_keys = self.redis.keys("feed:*")
            count = 0
            
            # Remove post from each feed
            for key in feed_keys:
                removed = self.redis.zrem(key, post_id)
                if removed:
                    count += 1
                    
            return count
        except Exception as e:
            logger.error(f"Error removing post from feeds: {e}")
            return 0


class ContentCache(Cache):
    """
    Cache implementation for content objects.
    
    Stores serialized posts and users.
    """
    
    def set_post(self, post):
        """
        Cache a post object.
        
        Args:
            post: Post object to cache
            
        Returns:
            True if successful, False otherwise
        """
        try:
            key = f"post:{post.post_id}"
            post_dict = post.to_dict()
            
            # Create a flattened dictionary with only primitive types for Redis
            redis_dict = {}
            for k, v in post_dict.items():
                try:
                    if isinstance(v, (str, int, float, bool)) or v is None:
                        redis_dict[k] = v
                    elif isinstance(v, list):
                        # For lists, convert to JSON string with error handling for each item
                        simplified_list = []
                        for item in v:
                            if isinstance(item, (str, int, float, bool)) or item is None:
                                simplified_list.append(item)
                            elif hasattr(item, 'to_dict'):
                                # If item has to_dict method, use it
                                simplified_list.append(json.dumps(item.to_dict()))
                            else:
                                # Otherwise convert to string
                                simplified_list.append(str(item))
                        redis_dict[k] = json.dumps(simplified_list)
                    elif isinstance(v, dict):
                        # For dictionaries, ensure all values are serializable
                        serializable_dict = {}
                        for dict_k, dict_v in v.items():
                            if isinstance(dict_v, (str, int, float, bool)) or dict_v is None:
                                serializable_dict[dict_k] = dict_v
                            else:
                                serializable_dict[dict_k] = str(dict_v)
                        redis_dict[k] = json.dumps(serializable_dict)
                    elif hasattr(v, 'to_dict'):
                        # If object has to_dict method, use it
                        redis_dict[k] = json.dumps(v.to_dict())
                    else:
                        # For anything else, convert to string
                        redis_dict[k] = str(v)
                except Exception as e:
                    # If any specific field causes an error, log it and use string conversion
                    logger.warning(f"Error serializing field '{k}' for post {post.post_id}: {e}")
                    redis_dict[k] = str(v)
                
            # Store post data as hash
            self.redis.hmset(key, redis_dict)
            self.redis.expire(key, REDIS["expiration"]["post"])
            
            return True
        except Exception as e:
            logger.error(f"Error caching post: {e}")
            return False
    
    def get_post(self, post_id):
        """
        Retrieve a post from cache.
        
        Args:
            post_id: ID of the post to retrieve
            
        Returns:
            Dictionary containing post data or None if not found
        """
        try:
            key = f"post:{post_id}"
            post_data = self.redis.hgetall(key)
            
            if not post_data:
                return None
                
            # Refresh expiration
            self.redis.expire(key, REDIS["expiration"]["post"])
            
            # Try to decode any JSON values
            for k, v in post_data.items():
                if isinstance(v, str) and (v.startswith('[') or v.startswith('{')):
                    try:
                        post_data[k] = json.loads(v)
                    except json.JSONDecodeError:
                        # Keep as string if JSON decoding fails
                        logger.debug(f"Failed to decode JSON for field '{k}' in post {post_id}")
                        pass
                        
            return post_data
        except Exception as e:
            logger.error(f"Error retrieving post from cache: {e}")
            return None
    
    def get_posts(self, post_ids):
        """
        Retrieve multiple posts from cache.
        
        Args:
            post_ids: List of post IDs to retrieve
            
        Returns:
            Dictionary mapping post IDs to post data
        """
        result = {}
        pipeline = self.redis.pipeline()
        
        try:
            # Queue all get operations
            for post_id in post_ids:
                key = f"post:{post_id}"
                pipeline.hgetall(key)
            
            # Execute pipeline
            responses = pipeline.execute()
            
            # Process responses
            for i, post_id in enumerate(post_ids):
                if responses[i]:
                    post_data = responses[i]
                    
                    # Try to decode any JSON values
                    for k, v in post_data.items():
                        if isinstance(v, str) and (v.startswith('[') or v.startswith('{')):
                            try:
                                post_data[k] = json.loads(v)
                            except:
                                # Keep as string if JSON decoding fails
                                pass
                    
                    result[post_id] = post_data
                    # Refresh expiration in background
                    self.redis.expire(f"post:{post_id}", REDIS["expiration"]["post"])
            
            return result
        except Exception as e:
            logger.error(f"Error retrieving posts from cache: {e}")
            return result
    
    def set_user(self, user):
        """
        Cache a user object.
        
        Args:
            user: User object to cache
            
        Returns:
            True if successful, False otherwise
        """
        try:
            key = f"user:{user.user_id}"
            user_dict = user.to_dict()
            
            # Ensure all values are primitive types
            for k, v in user_dict.items():
                if not isinstance(v, (str, int, float, bool, type(None))):
                    user_dict[k] = str(v)
                    
            # Store user data as hash
            self.redis.hmset(key, user_dict)
            self.redis.expire(key, REDIS["expiration"]["user"])
            
            # Also store username lookup
            if user.username:
                username_key = f"username:{user.username}"
                self.redis.set(username_key, user.user_id)
                self.redis.expire(username_key, REDIS["expiration"]["user"])
                
            return True
        except Exception as e:
            logger.error(f"Error caching user: {e}")
            return False
    
    def get_user(self, user_id):
        """
        Retrieve a user from cache.
        
        Args:
            user_id: ID of the user to retrieve
            
        Returns:
            Dictionary containing user data or None if not found
        """
        try:
            key = f"user:{user_id}"
            user_data = self.redis.hgetall(key)
            
            if not user_data:
                return None
                
            # Refresh expiration
            self.redis.expire(key, REDIS["expiration"]["user"])
            
            return user_data
        except Exception as e:
            logger.error(f"Error retrieving user from cache: {e}")
            return None


class SocialGraphCache(Cache):
    """
    Cache implementation for social graph data.
    
    Stores relationships between users.
    """
    
    def add_relationship(self, user_id, friend_id, relationship_type):
        """
        Add a relationship between two users.
        
        Args:
            user_id: ID of the first user
            friend_id: ID of the second user
            relationship_type: Type of relationship
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert RelationshipType to string if it's an Enum
            relationship_type_value = relationship_type.value if hasattr(relationship_type, 'value') else str(relationship_type)
            
            # Add friend to user's friend set
            self.redis.sadd(f"friends:{user_id}", friend_id)
            
            # Store relationship details
            key = f"relationship:{user_id}:{friend_id}"
            self.redis.hset(key, "type", relationship_type_value)
            
            # Set expirations
            self.redis.expire(f"friends:{user_id}", REDIS["expiration"]["relationship"])
            self.redis.expire(f"relationship:{user_id}:{friend_id}", REDIS["expiration"]["relationship"])
            
            return True
        except Exception as e:
            logger.error(f"Error adding relationship to cache: {e}")
            return False
    
    def get_friends(self, user_id):
        """
        Get a list of friend IDs for a user.
        
        Args:
            user_id: ID of the user
            
        Returns:
            List of friend IDs
        """
        try:
            key = f"friends:{user_id}"
            friends = self.redis.smembers(key)
            
            # Refresh expiration
            if friends:
                self.redis.expire(key, REDIS["expiration"]["relationship"])
                
            return list(friends)
        except Exception as e:
            logger.error(f"Error retrieving friends from cache: {e}")
            return []
    
    def get_relationship_type(self, user_id, friend_id):
        """
        Get the type of relationship between two users.
        
        Args:
            user_id: ID of the first user
            friend_id: ID of the second user
            
        Returns:
            Relationship type as string, or None if not found
        """
        try:
            key = f"relationship:{user_id}:{friend_id}"
            relationship_type = self.redis.hget(key, "type")
            
            # Refresh expiration
            if relationship_type:
                self.redis.expire(key, REDIS["expiration"]["relationship"])
                
            return relationship_type
        except Exception as e:
            logger.error(f"Error retrieving relationship type from cache: {e}")
            return None


class ActionCache(Cache):
    """
    Cache implementation for user actions on posts.
    
    Stores information about likes, comments, shares, etc.
    """
    
    def add_action(self, user_id, post_id, action_type):
        """
        Add a user action on a post.
        
        Args:
            user_id: ID of the user
            post_id: ID of the post
            action_type: Type of action
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert ActionType to string if it's an Enum
            action_type_value = action_type.value if hasattr(action_type, 'value') else str(action_type)
            
            key = f"action:{post_id}:{action_type_value}"
            self.redis.sadd(key, user_id)
            self.redis.expire(key, REDIS["expiration"]["post"])
            
            # Also update counter
            counter_key = f"counter:{post_id}:{action_type_value}"
            self.redis.incr(counter_key)
            self.redis.expire(counter_key, REDIS["expiration"]["counter"])
            
            return True
        except Exception as e:
            logger.error(f"Error adding action to cache: {e}")
            return False
    
    def remove_action(self, user_id, post_id, action_type):
        """
        Remove a user action from a post.
        
        Args:
            user_id: ID of the user
            post_id: ID of the post
            action_type: Type of action
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert ActionType to string if it's an Enum
            action_type_value = action_type.value if hasattr(action_type, 'value') else str(action_type)
            
            key = f"action:{post_id}:{action_type_value}"
            removed = self.redis.srem(key, user_id)
            
            # Update counter if action was removed
            if removed:
                counter_key = f"counter:{post_id}:{action_type_value}"
                self.redis.decr(counter_key)
                self.redis.expire(counter_key, REDIS["expiration"]["counter"])
                
            return True
        except Exception as e:
            logger.error(f"Error removing action from cache: {e}")
            return False
    
    def get_users_by_action(self, post_id, action_type, limit=100):
        """
        Get the list of users who performed an action on a post.
        
        Args:
            post_id: ID of the post
            action_type: Type of action
            limit: Maximum number of users to return
            
        Returns:
            List of user IDs
        """
        try:
            key = f"action:{post_id}:{action_type.value}"
            # Use SSCAN for large sets to avoid blocking
            cursor = 0
            users = set()
            
            while len(users) < limit:
                cursor, subset = self.redis.sscan(key, cursor, count=100)
                users.update(subset)
                
                # End if we've scanned the entire set
                if cursor == 0:
                    break
                    
                # End if we've reached the limit
                if len(users) >= limit:
                    break
            
            return list(users)[:limit]
        except Exception as e:
            logger.error(f"Error retrieving users by action from cache: {e}")
            return []
    
    def has_action(self, user_id, post_id, action_type):
        """
        Check if a user has performed a specific action on a post.
        
        Args:
            user_id: ID of the user
            post_id: ID of the post
            action_type: Type of action
            
        Returns:
            True if the user has performed the action, False otherwise
        """
        try:
            # Convert ActionType to string if it's an Enum
            action_type_value = action_type.value if hasattr(action_type, 'value') else str(action_type)
            
            key = f"action:{post_id}:{action_type_value}"
            return self.redis.sismember(key, user_id)
        except Exception as e:
            logger.error(f"Error checking action from cache: {e}")
            return False


class CounterCache(Cache):
    """
    Cache implementation for counters.
    
    Stores counts for likes, comments, shares, etc.
    """
    
    def get_counter(self, post_id, counter_type):
        """
        Get the value of a counter.
        
        Args:
            post_id: ID of the post
            counter_type: Type of counter
            
        Returns:
            Counter value as integer
        """
        try:
            key = f"counter:{post_id}:{counter_type}"
            count = self.redis.get(key)
            
            # Refresh expiration
            if count:
                self.redis.expire(key, REDIS["expiration"]["counter"])
                return int(count)
            else:
                return 0
        except Exception as e:
            logger.error(f"Error retrieving counter from cache: {e}")
            return 0
    
    def get_counters(self, post_id):
        """
        Get all counters for a post.
        
        Args:
            post_id: ID of the post
            
        Returns:
            Dictionary mapping counter types to values
        """
        try:
            counters = {}
            counter_keys = self.redis.keys(f"counter:{post_id}:*")
            
            for key in counter_keys:
                counter_type = key.split(":")[-1]
                counters[counter_type] = int(self.redis.get(key) or 0)
                
                # Refresh expiration
                self.redis.expire(key, REDIS["expiration"]["counter"])
                
            return counters
        except Exception as e:
            logger.error(f"Error retrieving counters from cache: {e}")
            return {} 