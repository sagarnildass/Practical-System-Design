"""
Database service for the news feed system.

This module provides a service for interacting with MongoDB,
including user management, post CRUD, and relationship tracking.
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Union, Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pymongo

# Try to import local config if exists, otherwise use default config
try:
    import config_local as config
    print("Database: Using local configuration")
except ImportError:
    import config
    print("Database: Using default configuration")

from config import DATABASE, LOGGING
from models import User, Post, Action, Relationship, NewsFeedItem
from models import ActionType, PostType, RelationshipType

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOGGING["level"]),
    format=LOGGING["format"]
)
logger = logging.getLogger(__name__)


class DatabaseService:
    """
    Database service for the news feed system.
    
    This class provides an interface to MongoDB for storing and retrieving
    news feed data, including users, posts, relationships, and actions.
    """
    
    def __init__(self):
        """
        Initialize the database service.
        """
        self.client = None
        self.db = None
        self.connect()
        
    def connect(self):
        """
        Connect to MongoDB.
        """
        try:
            # Construct connection URI
            if config.DATABASE.get("user") and config.DATABASE.get("password"):
                uri = f"mongodb://{config.DATABASE['user']}:{config.DATABASE['password']}@{config.DATABASE['host']}:{config.DATABASE['port']}/{config.DATABASE['name']}"
            else:
                uri = f"mongodb://{config.DATABASE['host']}:{config.DATABASE['port']}/{config.DATABASE['name']}"
                
            # Add connection options
            uri += "?retryWrites=true&w=majority"
            
            # Connect to MongoDB
            self.client = MongoClient(
                uri,
                maxPoolSize=config.DATABASE.get("pool_size", 100),
                serverSelectionTimeoutMS=5000
            )
            
            # Ping the server to check the connection
            self.client.admin.command('ping')
            
            # Get database reference
            self.db = self.client[config.DATABASE['name']]
            
            # Create indexes
            self._create_indexes()
            
            logger.info("Connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.client = None
            self.db = None
            
    def is_connected(self):
        """
        Check if the database is connected.
        
        Returns:
            True if connected, False otherwise
        """
        if self.client is None or self.db is None:
            return False
            
        try:
            # Ping the server
            self.client.admin.command('ping')
            return True
        except:
            return False
    
    def _create_indexes(self):
        """
        Create indexes for collections.
        """
        if self.db is None:
            logger.warning("Cannot create indexes, not connected to database")
            return
            
        try:
            # Users collection
            try:
                self.db.users.create_index([("username", pymongo.ASCENDING)], unique=True)
                logger.debug("Created users index")
            except Exception as e:
                logger.warning(f"Failed to create users index: {e}")
            
            # Posts collection
            try:
                self.db.posts.create_index([("user_id", pymongo.ASCENDING)])
                self.db.posts.create_index([("created_at", pymongo.DESCENDING)])
                logger.debug("Created posts indexes")
            except Exception as e:
                logger.warning(f"Failed to create posts indexes: {e}")
            
            # Media collection
            try:
                self.db.media.create_index([("post_id", pymongo.ASCENDING)])
                logger.debug("Created media index")
            except Exception as e:
                logger.warning(f"Failed to create media index: {e}")
            
            # Relationships collection
            try:
                self.db.relationships.create_index(
                    [("user_id", pymongo.ASCENDING), ("friend_id", pymongo.ASCENDING)],
                    unique=True
                )
                logger.debug("Created relationships index")
            except Exception as e:
                logger.warning(f"Failed to create relationships index: {e}")
            
            # Actions collection
            try:
                self.db.actions.create_index(
                    [("user_id", pymongo.ASCENDING), ("post_id", pymongo.ASCENDING), 
                     ("action_type", pymongo.ASCENDING)],
                    unique=True
                )
                self.db.actions.create_index([("post_id", pymongo.ASCENDING)])
                logger.debug("Created actions indexes")
            except Exception as e:
                logger.warning(f"Failed to create actions indexes: {e}")
            
            # News feed collection
            try:
                self.db.news_feed.create_index([("user_id", pymongo.ASCENDING)])
                self.db.news_feed.create_index(
                    [("user_id", pymongo.ASCENDING), ("created_at", pymongo.DESCENDING)]
                )
                logger.debug("Created news feed indexes")
            except Exception as e:
                logger.warning(f"Failed to create news feed indexes: {e}")
            
            logger.info("Database indexes created")
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
    
    def close(self):
        """
        Close the database connection.
        """
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            logger.info("Database connection closed")
    
    # User methods
    
    def create_user(self, username: str, email: str, 
                   profile_picture_url: Optional[str] = None) -> Optional[User]:
        """
        Create a new user.
        
        Args:
            username: The username of the new user
            email: The email address of the new user
            profile_picture_url: URL to the user's profile picture
            
        Returns:
            The created User object, or None if creation failed
        """
        if not self.is_connected():
            logger.error("Cannot create user, not connected to database")
            return None
            
        try:
            # Create user object with the allowed parameters
            user = User(username=username, profile_picture_url=profile_picture_url)
            
            # Set additional properties manually
            user.email = email
            
            # We don't need to set user_id, created_at, or updated_at as they're already
            # handled in the User constructor
            
            result = self.db.users.insert_one(user.to_dict())
            
            if result.acknowledged:
                logger.info(f"Created user {username} with ID {user.user_id}")
                return user
            else:
                logger.error(f"Failed to create user {username}")
                return None
        except PyMongoError as e:
            logger.error(f"Database error creating user {username}: {e}")
            return None
    
    def get_user(self, user_id: str) -> Optional[User]:
        """
        Get a user by ID.
        
        Args:
            user_id: The ID of the user to retrieve
            
        Returns:
            The User object, or None if not found
        """
        if not self.is_connected():
            logger.error("Cannot get user, not connected to database")
            return None
            
        try:
            user_dict = self.db.users.find_one({"user_id": user_id})
            
            if user_dict:
                return User.from_dict(user_dict)
            else:
                logger.debug(f"User {user_id} not found")
                return None
        except PyMongoError as e:
            logger.error(f"Database error retrieving user {user_id}: {e}")
            return None
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """
        Get a user by username.
        
        Args:
            username: The username of the user to retrieve
            
        Returns:
            The User object, or None if not found
        """
        if not self.is_connected():
            logger.error("Cannot get user, not connected to database")
            return None
            
        try:
            user_dict = self.db.users.find_one({"username": username})
            
            if user_dict:
                return User.from_dict(user_dict)
            else:
                logger.debug(f"User with username {username} not found")
                return None
        except PyMongoError as e:
            logger.error(f"Database error retrieving user with username {username}: {e}")
            return None
    
    def update_user(self, user: User) -> bool:
        """
        Update a user.
        
        Args:
            user: The User object to update
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot update user, not connected to database")
            return False
            
        try:
            # Update the timestamp
            user.updated_at = datetime.now()
            
            result = self.db.users.update_one(
                {"user_id": user.user_id},
                {"$set": user.to_dict()}
            )
            
            if result.matched_count > 0:
                logger.info(f"Updated user {user.user_id}")
                return True
            else:
                logger.warning(f"User {user.user_id} not found for update")
                return False
        except PyMongoError as e:
            logger.error(f"Database error updating user {user.user_id}: {e}")
            return False
    
    def delete_user(self, user_id: str) -> bool:
        """
        Delete a user.
        
        Args:
            user_id: The ID of the user to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot delete user, not connected to database")
            return False
            
        try:
            # Delete the user
            result = self.db.users.delete_one({"user_id": user_id})
            
            if result.deleted_count > 0:
                logger.info(f"Deleted user {user_id}")
                
                # Delete related data
                self.db.posts.delete_many({"user_id": user_id})
                self.db.relationships.delete_many(
                    {"$or": [{"user_id": user_id}, {"friend_id": user_id}]}
                )
                self.db.actions.delete_many({"user_id": user_id})
                self.db.news_feed.delete_many({"user_id": user_id})
                
                return True
            else:
                logger.warning(f"User {user_id} not found for deletion")
                return False
        except PyMongoError as e:
            logger.error(f"Database error deleting user {user_id}: {e}")
            return False
    
    # Post methods
    
    def create_post(self, user_id: str, content: str, 
                   post_type: PostType = PostType.TEXT,
                   media_urls: Optional[List[str]] = None,
                   media_types: Optional[List[str]] = None) -> Optional[Post]:
        """
        Create a new post.
        
        Args:
            user_id: The ID of the user creating the post
            content: The content of the post
            post_type: The type of post
            media_urls: List of URLs to media attached to the post
            media_types: List of media types corresponding to media_urls
            
        Returns:
            The created Post object, or None if creation failed
        """
        if not self.is_connected():
            logger.error("Cannot create post, not connected to database")
            return None
            
        try:
            # Create the post
            post = Post(user_id=user_id, content=content, post_type=post_type)
                
            result = self.db.posts.insert_one(post.to_dict())
            
            if not result.acknowledged:
                logger.error(f"Failed to create post for user {user_id}")
                return None
                
            # Add media if provided
            if media_urls and len(media_urls) > 0:
                if not media_types or len(media_types) != len(media_urls):
                    # Default to same type for all if not specified
                    media_types = [media_types[0] if media_types else "image"] * len(media_urls)
                    
                media_items = []
                for i, url in enumerate(media_urls):
                    media = Media(
                        post_id=post.post_id,
                        media_type=media_types[i],
                        media_url=url
                    )
                    media_items.append(media.to_dict())
                    
                if media_items:
                    self.db.media.insert_many(media_items)
                    
            logger.info(f"Created post {post.post_id} for user {user_id}")
            return post
        except PyMongoError as e:
            logger.error(f"Database error creating post for user {user_id}: {e}")
            return None
    
    def get_post(self, post_id: str) -> Optional[Post]:
        """
        Get a post by ID.
        
        Args:
            post_id: The ID of the post to retrieve
            
        Returns:
            The Post object, or None if not found
        """
        if not self.is_connected():
            logger.error("Cannot get post, not connected to database")
            return None
            
        try:
            post_dict = self.db.posts.find_one({"post_id": post_id})
            
            if not post_dict:
                logger.debug(f"Post {post_id} not found")
                return None
                
            # Convert to Post object
            post = Post.from_dict(post_dict)
            
            # Get media
            media_dicts = list(self.db.media.find({"post_id": post_id}))
            post.media = [Media.from_dict(m) for m in media_dicts]
            
            return post
        except PyMongoError as e:
            logger.error(f"Database error retrieving post {post_id}: {e}")
            return None
    
    def get_user_posts(self, user_id: str, 
                      limit: int = 20, 
                      offset: int = 0) -> List[Post]:
        """
        Get posts created by a user.
        
        Args:
            user_id: The ID of the user
            limit: Maximum number of posts to retrieve
            offset: Number of posts to skip
            
        Returns:
            List of Post objects
        """
        if not self.is_connected():
            logger.error("Cannot get user posts, not connected to database")
            return []
            
        try:
            # Get posts
            post_dicts = list(self.db.posts.find(
                {"user_id": user_id}
            ).sort(
                "created_at", pymongo.DESCENDING
            ).skip(offset).limit(limit))
            
            if not post_dicts:
                return []
                
            # Convert to Post objects
            posts = [Post.from_dict(p) for p in post_dicts]
            
            # Get media for each post
            post_ids = [p.post_id for p in posts]
            media_dicts = list(self.db.media.find({"post_id": {"$in": post_ids}}))
            
            # Group media by post_id
            media_by_post = {}
            for m in media_dicts:
                post_id = m["post_id"]
                if post_id not in media_by_post:
                    media_by_post[post_id] = []
                media_by_post[post_id].append(Media.from_dict(m))
                
            # Add media to posts
            for post in posts:
                post.media = media_by_post.get(post.post_id, [])
                
            return posts
        except PyMongoError as e:
            logger.error(f"Database error retrieving posts for user {user_id}: {e}")
            return []
    
    def get_posts_by_users(self, user_ids: List[str], 
                         limit: int = 20, 
                         offset: int = 0) -> List[Post]:
        """
        Get posts created by a list of users.
        
        Args:
            user_ids: List of user IDs
            limit: Maximum number of posts to retrieve
            offset: Number of posts to skip
            
        Returns:
            List of Post objects
        """
        if not self.is_connected():
            logger.error("Cannot get posts by users, not connected to database")
            return []
            
        try:
            # Get posts
            post_dicts = list(self.db.posts.find(
                {"user_id": {"$in": user_ids}}
            ).sort(
                "created_at", pymongo.DESCENDING
            ).skip(offset).limit(limit))
            
            if not post_dicts:
                return []
                
            # Convert to Post objects
            posts = [Post.from_dict(p) for p in post_dicts]
            
            # Get media for each post
            post_ids = [p.post_id for p in posts]
            media_dicts = list(self.db.media.find({"post_id": {"$in": post_ids}}))
            
            # Group media by post_id
            media_by_post = {}
            for m in media_dicts:
                post_id = m["post_id"]
                if post_id not in media_by_post:
                    media_by_post[post_id] = []
                media_by_post[post_id].append(Media.from_dict(m))
                
            # Add media to posts
            for post in posts:
                post.media = media_by_post.get(post.post_id, [])
                
            return posts
        except PyMongoError as e:
            logger.error(f"Database error retrieving posts for users {user_ids}: {e}")
            return []
    
    def update_post(self, post: Post) -> bool:
        """
        Update a post.
        
        Args:
            post: The Post object to update
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot update post, not connected to database")
            return False
            
        try:
            # Update the timestamp
            post.updated_at = datetime.now()
            
            # Update post
            post_dict = post.to_dict()
            
            # Remove media from post dict (stored separately)
            if "media" in post_dict:
                del post_dict["media"]
                
            result = self.db.posts.update_one(
                {"post_id": post.post_id},
                {"$set": post_dict}
            )
            
            if result.matched_count > 0:
                logger.info(f"Updated post {post.post_id}")
                
                # Update media
                if post.media:
                    # Delete existing media
                    self.db.media.delete_many({"post_id": post.post_id})
                    
                    # Insert new media
                    media_dicts = [m.to_dict() for m in post.media]
                    if media_dicts:
                        self.db.media.insert_many(media_dicts)
                        
                return True
            else:
                logger.warning(f"Post {post.post_id} not found for update")
                return False
        except PyMongoError as e:
            logger.error(f"Database error updating post {post.post_id}: {e}")
            return False
    
    def delete_post(self, post_id: str) -> bool:
        """
        Delete a post.
        
        Args:
            post_id: The ID of the post to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot delete post, not connected to database")
            return False
            
        try:
            # Delete the post
            result = self.db.posts.delete_one({"post_id": post_id})
            
            if result.deleted_count > 0:
                logger.info(f"Deleted post {post_id}")
                
                # Delete related data
                self.db.media.delete_many({"post_id": post_id})
                self.db.actions.delete_many({"post_id": post_id})
                
                return True
            else:
                logger.warning(f"Post {post_id} not found for deletion")
                return False
        except PyMongoError as e:
            logger.error(f"Database error deleting post {post_id}: {e}")
            return False
    
    # Relationship methods
    
    def create_relationship(self, user_id: str, friend_id: str, 
                           relationship_type: RelationshipType) -> Optional[Relationship]:
        """
        Create a relationship between two users.
        
        Args:
            user_id: The ID of the first user
            friend_id: The ID of the second user
            relationship_type: The type of relationship
            
        Returns:
            The created Relationship object, or None if creation failed
        """
        if not self.is_connected():
            logger.error("Cannot create relationship, not connected to database")
            return None
            
        try:
            # Check if users exist
            if not self.get_user(user_id) or not self.get_user(friend_id):
                logger.error(f"Cannot create relationship, one or both users don't exist: {user_id}, {friend_id}")
                return None
                
            # Check if relationship already exists
            existing = self.db.relationships.find_one({
                "user_id": user_id,
                "friend_id": friend_id
            })
            
            if existing:
                # Update relationship type if different
                if existing["relationship_type"] != relationship_type.value:
                    result = self.db.relationships.update_one(
                        {"_id": existing["_id"]},
                        {"$set": {"relationship_type": relationship_type.value}}
                    )
                    
                    if result.modified_count > 0:
                        logger.info(f"Updated relationship type between {user_id} and {friend_id} to {relationship_type.value}")
                        existing["relationship_type"] = relationship_type.value
                    else:
                        logger.error(f"Failed to update relationship type between {user_id} and {friend_id}")
                
                return Relationship.from_dict(existing)
                
            # Create new relationship
            relationship = Relationship(
                user_id=user_id,
                friend_id=friend_id,
                relationship_type=relationship_type
            )
            
            result = self.db.relationships.insert_one(relationship.to_dict())
            
            if result.acknowledged:
                logger.info(f"Created relationship: {user_id} {relationship_type.value} {friend_id}")
                return relationship
            else:
                logger.error(f"Failed to create relationship: {user_id} {relationship_type.value} {friend_id}")
                return None
        except PyMongoError as e:
            logger.error(f"Database error creating relationship: {user_id} {relationship_type.value} {friend_id}: {e}")
            return None
    
    def get_relationship(self, user_id: str, friend_id: str) -> Optional[Relationship]:
        """
        Get the relationship between two users.
        
        Args:
            user_id: The ID of the first user
            friend_id: The ID of the second user
            
        Returns:
            The Relationship object, or None if not found
        """
        if not self.is_connected():
            logger.error("Cannot get relationship, not connected to database")
            return None
            
        try:
            relationship_dict = self.db.relationships.find_one({
                "user_id": user_id,
                "friend_id": friend_id
            })
            
            if relationship_dict:
                return Relationship.from_dict(relationship_dict)
            else:
                return None
        except PyMongoError as e:
            logger.error(f"Database error retrieving relationship between {user_id} and {friend_id}: {e}")
            return None
    
    def get_relationship_type(self, user_id: str, friend_id: str) -> Optional[RelationshipType]:
        """
        Get the type of relationship between two users.
        
        Args:
            user_id: The ID of the first user
            friend_id: The ID of the second user
            
        Returns:
            The relationship type, or None if not found
        """
        relationship = self.get_relationship(user_id, friend_id)
        if relationship:
            return relationship.relationship_type
        return None
    
    def get_friends(self, user_id: str, 
                   relationship_type: Optional[RelationshipType] = None) -> List[str]:
        """
        Get the list of friend IDs for a user.
        
        Args:
            user_id: The ID of the user
            relationship_type: Optional filter by relationship type
            
        Returns:
            List of friend IDs
        """
        if not self.is_connected():
            logger.error("Cannot get friends, not connected to database")
            return []
            
        try:
            query = {"user_id": user_id}
            
            if relationship_type:
                query["relationship_type"] = relationship_type.value
                
            friend_documents = self.db.relationships.find(query)
            return [doc["friend_id"] for doc in friend_documents]
        except PyMongoError as e:
            logger.error(f"Database error retrieving friends for user {user_id}: {e}")
            return []
    
    def get_followers(self, user_id: str) -> List[str]:
        """
        Get the list of follower IDs for a user.
        
        Args:
            user_id: The ID of the user
            
        Returns:
            List of follower IDs
        """
        if not self.is_connected():
            logger.error("Cannot get followers, not connected to database")
            return []
            
        try:
            # Find users who have a FOLLOW relationship with the specified user
            follower_documents = self.db.relationships.find({
                "friend_id": user_id,
                "relationship_type": RelationshipType.FOLLOW.value
            })
            
            return [doc["user_id"] for doc in follower_documents]
        except PyMongoError as e:
            logger.error(f"Database error retrieving followers for user {user_id}: {e}")
            return []
    
    def get_follower_count(self, user_id: str) -> int:
        """
        Get the number of followers for a user.
        
        Args:
            user_id: The ID of the user
            
        Returns:
            Number of followers
        """
        if not self.is_connected():
            logger.error("Cannot get follower count, not connected to database")
            return 0
            
        try:
            count = self.db.relationships.count_documents({
                "friend_id": user_id,
                "relationship_type": RelationshipType.FOLLOW.value
            })
            
            return count
        except PyMongoError as e:
            logger.error(f"Database error retrieving follower count for user {user_id}: {e}")
            return 0
    
    def delete_relationship(self, user_id: str, friend_id: str) -> bool:
        """
        Delete a relationship between two users.
        
        Args:
            user_id: The ID of the first user
            friend_id: The ID of the second user
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot delete relationship, not connected to database")
            return False
            
        try:
            result = self.db.relationships.delete_one({
                "user_id": user_id,
                "friend_id": friend_id
            })
            
            if result.deleted_count > 0:
                logger.info(f"Deleted relationship between {user_id} and {friend_id}")
                return True
            else:
                logger.warning(f"Relationship between {user_id} and {friend_id} not found for deletion")
                return False
        except PyMongoError as e:
            logger.error(f"Database error deleting relationship between {user_id} and {friend_id}: {e}")
            return False
    
    # Action methods
    
    def create_action(self, user_id: str, post_id: str, 
                     action_type: ActionType) -> Optional[Action]:
        """
        Create a user action on a post.
        
        Args:
            user_id: The ID of the user
            post_id: The ID of the post
            action_type: The type of action
            
        Returns:
            The created Action object, or None if creation failed
        """
        if not self.is_connected():
            logger.error("Cannot create action, not connected to database")
            return None
            
        try:
            # Check if action already exists
            existing = self.db.actions.find_one({
                "user_id": user_id,
                "post_id": post_id,
                "action_type": action_type.value
            })
            
            if existing:
                logger.debug(f"Action already exists: {user_id} {action_type.value} {post_id}")
                return Action.from_dict(existing)
                
            # Create new action
            action = Action(
                user_id=user_id,
                post_id=post_id,
                action_type=action_type
            )
            
            result = self.db.actions.insert_one(action.to_dict())
            
            if result.acknowledged:
                logger.info(f"Created action: {user_id} {action_type.value} {post_id}")
                return action
            else:
                logger.error(f"Failed to create action: {user_id} {action_type.value} {post_id}")
                return None
        except PyMongoError as e:
            logger.error(f"Database error creating action: {user_id} {action_type.value} {post_id}: {e}")
            return None
    
    def get_action(self, user_id: str, post_id: str, 
                  action_type: ActionType) -> Optional[Action]:
        """
        Get a user action on a post.
        
        Args:
            user_id: The ID of the user
            post_id: The ID of the post
            action_type: The type of action
            
        Returns:
            The Action object, or None if not found
        """
        if not self.is_connected():
            logger.error("Cannot get action, not connected to database")
            return None
            
        try:
            action_dict = self.db.actions.find_one({
                "user_id": user_id,
                "post_id": post_id,
                "action_type": action_type.value
            })
            
            if action_dict:
                return Action.from_dict(action_dict)
            else:
                return None
        except PyMongoError as e:
            logger.error(f"Database error retrieving action: {user_id} {action_type.value} {post_id}: {e}")
            return None
    
    def get_actions_by_post(self, post_id: str, 
                           action_type: Optional[ActionType] = None) -> List[Action]:
        """
        Get actions for a post.
        
        Args:
            post_id: The ID of the post
            action_type: Optional filter by action type
            
        Returns:
            List of Action objects
        """
        if not self.is_connected():
            logger.error("Cannot get actions, not connected to database")
            return []
            
        try:
            query = {"post_id": post_id}
            
            if action_type:
                query["action_type"] = action_type.value
                
            action_dicts = self.db.actions.find(query)
            return [Action.from_dict(a) for a in action_dicts]
        except PyMongoError as e:
            logger.error(f"Database error retrieving actions for post {post_id}: {e}")
            return []
    
    def get_action_count(self, post_id: str, 
                        action_type: ActionType) -> int:
        """
        Get the count of actions for a post.
        
        Args:
            post_id: The ID of the post
            action_type: The type of action
            
        Returns:
            Number of actions
        """
        if not self.is_connected():
            logger.error("Cannot get action count, not connected to database")
            return 0
            
        try:
            count = self.db.actions.count_documents({
                "post_id": post_id,
                "action_type": action_type.value
            })
            
            return count
        except PyMongoError as e:
            logger.error(f"Database error retrieving action count for post {post_id}: {e}")
            return 0
    
    def delete_action(self, user_id: str, post_id: str, 
                     action_type: ActionType) -> bool:
        """
        Delete a user action on a post.
        
        Args:
            user_id: The ID of the user
            post_id: The ID of the post
            action_type: The type of action
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot delete action, not connected to database")
            return False
            
        try:
            result = self.db.actions.delete_one({
                "user_id": user_id,
                "post_id": post_id,
                "action_type": action_type.value
            })
            
            if result.deleted_count > 0:
                logger.info(f"Deleted action: {user_id} {action_type.value} {post_id}")
                return True
            else:
                logger.warning(f"Action not found for deletion: {user_id} {action_type.value} {post_id}")
                return False
        except PyMongoError as e:
            logger.error(f"Database error deleting action: {user_id} {action_type.value} {post_id}: {e}")
            return False
    
    # News feed methods
    
    def add_to_feed(self, user_id: str, post_id: str) -> Optional[NewsFeedItem]:
        """
        Add a post to a user's news feed.
        
        Args:
            user_id: The ID of the user
            post_id: The ID of the post
            
        Returns:
            The created NewsFeedItem object, or None if creation failed
        """
        if not self.is_connected():
            logger.error("Cannot add to feed, not connected to database")
            return None
            
        try:
            # Check if post is already in feed
            existing = self.db.news_feed.find_one({
                "user_id": user_id,
                "post_id": post_id
            })
            
            if existing:
                logger.debug(f"Post {post_id} already in feed for user {user_id}")
                return NewsFeedItem.from_dict(existing)
                
            # Add post to feed
            feed_item = NewsFeedItem(post_id=post_id)
            
            # Set user_id manually since NewsFeedItem doesn't include it in constructor
            feed_item_dict = feed_item.to_dict()
            feed_item_dict["user_id"] = user_id
            
            result = self.db.news_feed.insert_one(feed_item_dict)
            
            if result.acknowledged:
                logger.debug(f"Added post {post_id} to feed for user {user_id}")
                return feed_item
            else:
                logger.error(f"Failed to add post {post_id} to feed for user {user_id}")
                return None
        except PyMongoError as e:
            logger.error(f"Database error adding post {post_id} to feed for user {user_id}: {e}")
            return None
    
    def get_news_feed(self, user_id: str, 
                     limit: int = 20, 
                     offset: int = 0) -> List[Post]:
        """
        Get a user's news feed.
        
        Args:
            user_id: The ID of the user
            limit: Maximum number of posts to retrieve
            offset: Number of posts to skip
            
        Returns:
            List of Post objects
        """
        if not self.is_connected():
            logger.error("Cannot get news feed, not connected to database")
            return []
            
        try:
            # Get feed items
            feed_items = list(self.db.news_feed.find(
                {"user_id": user_id}
            ).sort(
                "created_at", pymongo.DESCENDING
            ).skip(offset).limit(limit))
            
            if not feed_items:
                return []
                
            # Get post IDs
            post_ids = [item["post_id"] for item in feed_items]
            
            # Get posts
            posts = []
            for post_id in post_ids:
                post = self.get_post(post_id)
                if post:
                    posts.append(post)
                    
            return posts
        except PyMongoError as e:
            logger.error(f"Database error retrieving news feed for user {user_id}: {e}")
            return []
    
    def remove_from_feed(self, user_id: str, post_id: str) -> bool:
        """
        Remove a post from a user's news feed.
        
        Args:
            user_id: The ID of the user
            post_id: The ID of the post
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot remove from feed, not connected to database")
            return False
            
        try:
            result = self.db.news_feed.delete_one({
                "user_id": user_id,
                "post_id": post_id
            })
            
            if result.deleted_count > 0:
                logger.debug(f"Removed post {post_id} from feed for user {user_id}")
                return True
            else:
                logger.warning(f"Post {post_id} not found in feed for user {user_id}")
                return False
        except PyMongoError as e:
            logger.error(f"Database error removing post {post_id} from feed for user {user_id}: {e}")
            return False
    
    def get_stats(self):
        """
        Get database statistics.
        
        Returns:
            Dictionary of statistics
        """
        if not self.is_connected():
            logger.error("Cannot get stats, not connected to database")
            return {}
            
        try:
            stats = {
                "users": self.db.users.count_documents({}),
                "posts": self.db.posts.count_documents({}),
                "relationships": self.db.relationships.count_documents({}),
                "actions": self.db.actions.count_documents({}),
                "news_feed_items": self.db.news_feed.count_documents({})
            }
            
            return stats
        except PyMongoError as e:
            logger.error(f"Database error retrieving stats: {e}")
            return {}

    def get_all_users(self):
        """Get all users from the database."""
        if not self.is_connected():
            logger.error("Cannot get all users, not connected to database")
            return []
            
        try:
            # Get all users from the MongoDB collection
            user_dicts = list(self.db.users.find())
            
            # Convert to User objects
            return [User.from_dict(user_dict) for user_dict in user_dicts]
        except PyMongoError as e:
            logger.error(f"Database error retrieving all users: {e}")
            return []

    def has_action(self, user_id: str, post_id: str, 
                  action_type: ActionType) -> bool:
        """
        Check if a user has performed a specific action on a post.
        
        Args:
            user_id: ID of the user
            post_id: ID of the post
            action_type: Type of action (LIKE, COMMENT, etc.)
            
        Returns:
            True if the action exists, False otherwise
        """
        if not self.is_connected():
            logger.error("Cannot check action, not connected to database")
            return False
            
        try:
            action = self.db.actions.find_one({
                "user_id": user_id,
                "post_id": post_id,
                "action_type": action_type.value
            })
            
            return action is not None
        except Exception as e:
            logger.error(f"Error checking if action exists: {e}")
            return False