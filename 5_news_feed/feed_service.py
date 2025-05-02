"""
Feed service for the news feed system.

This module implements the feed service for the news feed system,
providing functionality for publishing posts and retrieving feeds.
"""

import logging
import time
from datetime import datetime

from config import PERFORMANCE, FANOUT
from models import ActionType, Post, PostType, RelationshipType

logger = logging.getLogger(__name__)


class FeedService:
    """
    Feed service for the news feed system.
    
    This class provides an interface for publishing posts to the news feed
    and retrieving news feeds for users.
    """
    
    def __init__(self, db_service, cache_service, fanout_service):
        """
        Initialize the feed service.
        
        Args:
            db_service: Database service for storing and retrieving data
            cache_service: Cache service for caching data
            fanout_service: Fanout service for distributing posts to followers
        """
        self.db = db_service
        self.cache = cache_service
        self.fanout = fanout_service
        
    def publish_post(self, user_id, content, post_type=PostType.TEXT, 
                    media_urls=None, media_types=None):
        """
        Publish a post to the news feed.
        
        Args:
            user_id: ID of the user publishing the post
            content: Content of the post
            post_type: Type of post
            media_urls: List of URLs to media attached to the post
            media_types: List of media types corresponding to media_urls
            
        Returns:
            The published Post object, or None if failed
        """
        start_time = time.time()
        
        try:
            # Create post in database
            post = self.db.create_post(
                user_id=user_id,
                content=content,
                post_type=post_type,
                media_urls=media_urls,
                media_types=media_types
            )
            
            if not post:
                logger.error(f"Failed to create post for user {user_id}")
                return None
                
            # Cache the post
            try:
                self.cache.content.set_post(post)
            except Exception as e:
                logger.error(f"Error caching post: {e}")
                # Continue even if caching fails
            
            # Add to the user's own feed directly
            # First add to database
            try:
                self.db.add_to_feed(user_id, post.post_id)
            except Exception as e:
                logger.error(f"Error adding post to user's own feed in database: {e}")
            
            # Then add to cache
            try:
                self.cache.news_feed.add_post_to_feed(user_id, post.post_id, post.created_at)
            except Exception as e:
                logger.error(f"Error adding post to user's own feed in cache: {e}")
            
            # Get followers to immediately add to their feeds as well
            try:
                followers = self.db.get_followers(user_id)
                for follower_id in followers:
                    try:
                        # Check relationship type to skip blocked users
                        rel_type = self.db.get_relationship_type(follower_id, user_id)
                        if rel_type == RelationshipType.BLOCK:
                            logger.debug(f"Skipping blocked user {follower_id} for post {post.post_id}")
                            continue
                            
                        # Add to database
                        self.db.add_to_feed(follower_id, post.post_id)
                        # Add to cache
                        self.cache.news_feed.add_post_to_feed(follower_id, post.post_id, post.created_at)
                    except Exception as e:
                        logger.error(f"Error processing follower {follower_id}: {e}")
                        # Continue with other followers even if one fails
            except Exception as e:
                logger.error(f"Error getting followers: {e}")
            
            # Also start fanout process for followers (for completeness)
            try:
                self.fanout.fanout_post(user_id, post.post_id, post.created_at)
            except Exception as e:
                logger.error(f"Error starting fanout process: {e}")
            
            logger.info(f"Published post {post.post_id} for user {user_id} in {time.time() - start_time:.2f}s")
            return post
        except Exception as e:
            logger.error(f"Error publishing post for user {user_id}: {e}")
            return None
            
    def get_news_feed(self, user_id, limit=20, offset=0):
        """
        Get news feed for a user.
        
        Args:
            user_id: ID of the user
            limit: Maximum number of posts to retrieve
            offset: Number of posts to skip
            
        Returns:
            List of Post objects in the user's feed
        """
        try:
            # Try to get feed from cache
            feed_post_ids = self.cache.news_feed.get_news_feed(user_id, offset, limit)
            
            if not feed_post_ids:
                # Cache miss, get friend posts from database
                friend_ids = self.db.get_friends(user_id, relationship_type=RelationshipType.FOLLOW)
                
                # Always include user's own posts
                ids_to_fetch = list(friend_ids) + [user_id]
                
                # Get posts from database
                posts = self.db.get_posts_by_users(
                    user_ids=ids_to_fetch,
                    limit=limit,
                    offset=offset
                )
                
                # Enrich posts with user data and action counts
                return self._enrich_posts(posts, user_id)
            
            # Handle pagination on cached feed
            if not feed_post_ids:
                return []
                
            # Get posts for the IDs from cache or database
            posts = []
            for post_id in feed_post_ids:
                # Try to get post from cache
                post_data = self.cache.content.get_post(post_id)
                
                if not post_data:
                    # Cache miss, get from database
                    post = self.db.get_post(post_id)
                    if post:
                        posts.append(post)
                        self.cache.content.set_post(post)
                else:
                    # Create Post object from cache data
                    post = Post.from_dict(post_data)
                    posts.append(post)
                        
            # Enrich posts with user data and action counts
            return self._enrich_posts(posts, user_id)
        except Exception as e:
            logger.error(f"Error retrieving feed for user {user_id}: {e}")
            return []
            
    def _enrich_posts(self, posts, current_user_id=None):
        """
        Enrich posts with additional data such as user info and action counts.
        
        Args:
            posts: List of Post objects to enrich
            current_user_id: ID of the current user, to check if they liked each post
            
        Returns:
            List of enriched Post objects
        """
        if not posts:
            return []
            
        try:
            for post in posts:
                # Get user info
                user_data = self.cache.content.get_user(post.user_id)
                if not user_data:
                    user = self.db.get_user(post.user_id)
                    if user:
                        user_data = user.to_dict()
                        self.cache.content.set_user(user)
                        
                if user_data:
                    post.username = user_data.get("username")
                    post.profile_picture_url = user_data.get("profile_picture_url")
                    
                # Get action counts (likes, comments, shares)
                action_types = [ActionType.LIKE, ActionType.COMMENT, ActionType.SHARE]
                
                for action_type in action_types:
                    # Try to get count from cache
                    count = self.cache.counter.get_counter(
                        post.post_id, action_type.value
                    )
                    
                    if count == 0:
                        # Cache miss, get from database
                        count = self.db.get_action_count(post.post_id, action_type)
                        
                    # Add count to post
                    setattr(post, f"{action_type.value}_count", count)
                
                # Check if current user has liked the post
                if current_user_id:
                    # Try to get from cache first
                    liked = self.cache.action.has_action(
                        user_id=current_user_id,
                        post_id=post.post_id,
                        action_type=ActionType.LIKE
                    )
                    
                    if not liked:
                        # Check in database
                        liked = self.db.has_action(
                            user_id=current_user_id,
                            post_id=post.post_id,
                            action_type=ActionType.LIKE
                        )
                    
                    post.liked_by_me = liked
                    
            return posts
        except Exception as e:
            logger.error(f"Error enriching posts: {e}")
            return posts
            
    def get_user_feed(self, user_id, limit=20, offset=0):
        """
        Get posts published by a specific user.
        
        Args:
            user_id: ID of the user
            limit: Maximum number of posts to retrieve
            offset: Number of posts to skip
            
        Returns:
            List of Post objects published by the user
        """
        try:
            posts = self.db.get_user_posts(user_id, limit, offset)
            return self._enrich_posts(posts, user_id)
        except Exception as e:
            logger.error(f"Error retrieving posts for user {user_id}: {e}")
            return []
            
    def like_post(self, user_id, post_id):
        """
        Like a post.
        
        Args:
            user_id: ID of the user liking the post
            post_id: ID of the post to like
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create action in database
            action = self.db.create_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.LIKE
            )
            
            if not action:
                return False
                
            # Update cache
            self.cache.action.add_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.LIKE
            )
            
            logger.info(f"User {user_id} liked post {post_id}")
            return True
        except Exception as e:
            logger.error(f"Error liking post {post_id} by user {user_id}: {e}")
            return False
            
    def unlike_post(self, user_id, post_id):
        """
        Unlike a post.
        
        Args:
            user_id: ID of the user unliking the post
            post_id: ID of the post to unlike
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Delete action from database
            success = self.db.delete_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.LIKE
            )
            
            if not success:
                return False
                
            # Update cache
            self.cache.action.remove_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.LIKE
            )
            
            logger.info(f"User {user_id} unliked post {post_id}")
            return True
        except Exception as e:
            logger.error(f"Error unliking post {post_id} by user {user_id}: {e}")
            return False
            
    def comment_on_post(self, user_id, post_id, content):
        """
        Comment on a post.
        
        Args:
            user_id: ID of the user commenting
            post_id: ID of the post to comment on
            content: Content of the comment
            
        Returns:
            The created Post object (comment), or None if failed
        """
        try:
            # Get the original post
            original_post = self.db.get_post(post_id)
            if not original_post:
                logger.error(f"Original post {post_id} not found")
                return None
                
            # Create comment as a new post with reference to original
            comment = self.db.create_post(
                user_id=user_id,
                content=content,
                post_type=PostType.COMMENT
            )
            
            if not comment:
                return None
                
            # Create action to link comment to original post
            action = self.db.create_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.COMMENT
            )
            
            # Update cache
            self.cache.content.set_post(comment)
            self.cache.action.add_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.COMMENT
            )
            
            logger.info(f"User {user_id} commented on post {post_id}")
            return comment
        except Exception as e:
            logger.error(f"Error commenting on post {post_id} by user {user_id}: {e}")
            return None
            
    def share_post(self, user_id, post_id, content=None):
        """
        Share a post.
        
        Args:
            user_id: ID of the user sharing the post
            post_id: ID of the post to share
            content: Optional additional content for the share
            
        Returns:
            The created Post object (share), or None if failed
        """
        try:
            # Get the original post
            original_post = self.db.get_post(post_id)
            if not original_post:
                logger.error(f"Original post {post_id} not found")
                return None
                
            # Create share content
            share_content = content or f"Shared post {post_id}"
            
            # Create share as a new post
            share = self.db.create_post(
                user_id=user_id,
                content=share_content,
                post_type=PostType.SHARE
            )
            
            if not share:
                return None
                
            # Create action to link share to original post
            action = self.db.create_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.SHARE
            )
            
            # Update cache
            self.cache.content.set_post(share)
            self.cache.action.add_action(
                user_id=user_id,
                post_id=post_id,
                action_type=ActionType.SHARE
            )
            
            # Fanout the share
            self.fanout.fanout_post(user_id, share.post_id, share.created_at)
            
            logger.info(f"User {user_id} shared post {post_id}")
            return share
        except Exception as e:
            logger.error(f"Error sharing post {post_id} by user {user_id}: {e}")
            return None
            
    def delete_post(self, user_id, post_id):
        """
        Delete a post.
        
        Args:
            user_id: ID of the user deleting the post
            post_id: ID of the post to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the post
            post = self.db.get_post(post_id)
            if not post:
                logger.error(f"Post {post_id} not found")
                return False
                
            # Check if user owns the post
            if post.user_id != user_id:
                logger.error(f"User {user_id} is not authorized to delete post {post_id}")
                return False
                
            # Delete the post from database
            success = self.db.delete_post(post_id)
            if not success:
                return False
                
            # Remove from cache
            # This is a potentially expensive operation
            self.cache.news_feed.remove_post_from_feeds(post_id)
            
            logger.info(f"User {user_id} deleted post {post_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting post {post_id} by user {user_id}: {e}")
            return False
            
    def follow_user(self, user_id, friend_id):
        """
        Follow a user.
        
        Args:
            user_id: ID of the user following
            friend_id: ID of the user to follow
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create relationship in database
            relationship = self.db.create_relationship(
                user_id=user_id,
                friend_id=friend_id,
                relationship_type=RelationshipType.FOLLOW
            )
            
            if not relationship:
                return False
                
            # Update cache
            self.cache.social_graph.add_relationship(
                user_id=user_id,
                friend_id=friend_id,
                relationship_type=RelationshipType.FOLLOW
            )
            
            # Get recent posts from the followed user and add to follower's feed
            recent_posts = self.db.get_user_posts(
                friend_id, limit=PERFORMANCE["post_batch_size"]
            )
            
            for post in recent_posts:
                self.cache.news_feed.add_post_to_feed(
                    user_id, post.post_id, post.created_at
                )
                
            logger.info(f"User {user_id} followed user {friend_id}")
            return True
        except Exception as e:
            logger.error(f"Error following user {friend_id} by user {user_id}: {e}")
            return False
            
    def unfollow_user(self, user_id, friend_id):
        """
        Unfollow a user.
        
        Args:
            user_id: ID of the user unfollowing
            friend_id: ID of the user to unfollow
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Delete relationship from database
            success = self.db.delete_relationship(
                user_id=user_id,
                friend_id=friend_id
            )
            
            if not success:
                return False
                
            # No need to update cache as relationships expire naturally
            
            logger.info(f"User {user_id} unfollowed user {friend_id}")
            return True
        except Exception as e:
            logger.error(f"Error unfollowing user {friend_id} by user {user_id}: {e}")
            return False
            
    def block_user(self, user_id, friend_id):
        """
        Block a user.
        
        Args:
            user_id: ID of the user blocking
            friend_id: ID of the user to block
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create the relationship
            relationship = self.db.create_relationship(
                user_id=user_id,
                friend_id=friend_id,
                relationship_type=RelationshipType.BLOCK
            )
            
            if relationship:
                # Also add to cache
                self.cache.social_graph.add_relationship(
                    user_id=user_id,
                    friend_id=friend_id,
                    relationship_type=RelationshipType.BLOCK
                )
                
                logger.info(f"User {user_id} blocked user {friend_id}")
                return True
            else:
                logger.error(f"Failed to block user {friend_id}")
                return False
        except Exception as e:
            logger.error(f"Error blocking user {friend_id}: {e}")
            return False
            
    def unblock_user(self, user_id, friend_id):
        """
        Unblock a user.
        
        Args:
            user_id: ID of the user doing the unblocking
            friend_id: ID of the user to unblock
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the existing relationship
            relationship = self.db.get_relationship(user_id, friend_id)
            
            # Only unblock if the relationship exists and is a block
            if not relationship or relationship.relationship_type != RelationshipType.BLOCK:
                logger.warning(f"Cannot unblock: No block relationship exists between {user_id} and {friend_id}")
                return False
                
            # Delete relationship from database
            success = self.db.delete_relationship(
                user_id=user_id,
                friend_id=friend_id
            )
            
            if not success:
                return False
                
            # No need to update cache as relationships expire naturally
            
            logger.info(f"User {user_id} unblocked user {friend_id}")
            return True
        except Exception as e:
            logger.error(f"Error unblocking user {friend_id} by user {user_id}: {e}")
            return False 