"""
Data models for the news feed system.
"""

from datetime import datetime
from enum import Enum
import uuid


class PostType(Enum):
    """
    Type of post content.
    """
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    COMMENT = "comment"  # Added for comments
    SHARE = "share"      # Added for shares


class MediaType(Enum):
    """
    Type of media content.
    """
    IMAGE = "image" 
    VIDEO = "video"


class RelationshipType(Enum):
    """
    Type of relationship between users.
    """
    FRIEND = "friend"
    FOLLOW = "follow"
    BLOCK = "block"
    MUTE = "mute"


class ActionType(Enum):
    """
    Types of actions a user can take on a post.
    """
    LIKE = "like"
    COMMENT = "comment"
    SHARE = "share"
    SAVE = "save"


class User:
    """
    Represents a user in the system.
    """
    def __init__(self, username, email=None, profile_picture_url=None):
        self.user_id = str(uuid.uuid4())
        self.username = username
        self.email = email
        self.profile_picture_url = profile_picture_url
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
    
    def to_dict(self):
        """
        Convert User object to dictionary.
        """
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "profile_picture_url": self.profile_picture_url,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """
        Create User object from dictionary.
        """
        user = cls(
            data["username"], 
            data.get("email"),
            data.get("profile_picture_url")
        )
        user.user_id = data["user_id"]
        user.created_at = datetime.fromisoformat(data["created_at"])
        user.updated_at = datetime.fromisoformat(data["updated_at"])
        return user


class Post:
    """
    Represents a post created by a user.
    """
    def __init__(self, user_id, content, post_type=PostType.TEXT):
        self.post_id = str(uuid.uuid4())
        self.user_id = user_id
        self.content = content
        self.post_type = post_type
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.media_ids = []
    
    def to_dict(self):
        """
        Convert Post object to dictionary.
        """
        result = {
            "post_id": self.post_id,
            "user_id": self.user_id,
            "content": self.content,
            "post_type": self.post_type.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "media_ids": self.media_ids
        }
        
        # Include username and profile_picture_url if they exist
        if hasattr(self, 'username'):
            result["username"] = self.username
        
        if hasattr(self, 'profile_picture_url'):
            result["profile_picture_url"] = self.profile_picture_url
            
        # Include like, comment, and share counts if they exist
        for action_type in ['like', 'comment', 'share']:
            count_attr = f"{action_type}_count"
            if hasattr(self, count_attr):
                result[count_attr] = getattr(self, count_attr)
        
        # Include liked_by_me if it exists
        if hasattr(self, 'liked_by_me'):
            result["liked_by_me"] = self.liked_by_me
            
        return result
    
    @classmethod
    def from_dict(cls, data):
        """
        Create Post object from dictionary.
        """
        post = cls(
            data["user_id"], 
            data["content"], 
            PostType(data["post_type"])
        )
        post.post_id = data["post_id"]
        post.created_at = datetime.fromisoformat(data["created_at"])
        post.updated_at = datetime.fromisoformat(data["updated_at"])
        post.media_ids = data.get("media_ids", [])
        return post


class Media:
    """
    Represents media attached to a post.
    """
    def __init__(self, post_id, media_type, media_url):
        self.media_id = str(uuid.uuid4())
        self.post_id = post_id
        self.media_type = media_type
        self.media_url = media_url
        self.created_at = datetime.now()
    
    def to_dict(self):
        """
        Convert Media object to dictionary.
        """
        return {
            "media_id": self.media_id,
            "post_id": self.post_id,
            "media_type": self.media_type.value,
            "media_url": self.media_url,
            "created_at": self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """
        Create Media object from dictionary.
        """
        media = cls(
            data["post_id"],
            MediaType(data["media_type"]),
            data["media_url"]
        )
        media.media_id = data["media_id"]
        media.created_at = datetime.fromisoformat(data["created_at"])
        return media


class Relationship:
    """
    Represents a relationship between two users.
    """
    def __init__(self, user_id, friend_id, relationship_type=RelationshipType.FRIEND):
        self.user_id = user_id
        self.friend_id = friend_id
        self.relationship_type = relationship_type
        self.relationship_id = str(uuid.uuid4())
        self.created_at = datetime.now()
    
    def to_dict(self):
        """
        Convert Relationship object to dictionary.
        """
        return {
            "relationship_id": self.relationship_id,
            "user_id": self.user_id,
            "friend_id": self.friend_id,
            "relationship_type": self.relationship_type.value,
            "created_at": self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """
        Create Relationship object from dictionary.
        """
        relationship = cls(
            data["user_id"],
            data["friend_id"],
            RelationshipType(data["relationship_type"])
        )
        relationship.relationship_id = data["relationship_id"]
        relationship.created_at = datetime.fromisoformat(data["created_at"])
        return relationship


class Action:
    """
    Represents an action a user takes on a post.
    """
    def __init__(self, user_id, post_id, action_type):
        self.user_id = user_id
        self.post_id = post_id
        self.action_type = action_type
        self.action_id = str(uuid.uuid4())
        self.created_at = datetime.now()
    
    def to_dict(self):
        """
        Convert Action object to dictionary.
        """
        return {
            "action_id": self.action_id,
            "user_id": self.user_id,
            "post_id": self.post_id,
            "action_type": self.action_type.value,
            "created_at": self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """
        Create Action object from dictionary.
        """
        action = cls(
            data["user_id"],
            data["post_id"],
            ActionType(data["action_type"])
        )
        action.action_id = data["action_id"]
        action.created_at = datetime.fromisoformat(data["created_at"])
        return action


class NewsFeedItem:
    """
    Represents an item in a user's news feed.
    """
    def __init__(self, post_id, user_id=None, timestamp=None):
        self.feed_item_id = str(uuid.uuid4())
        self.post_id = post_id
        self.user_id = user_id
        self.timestamp = timestamp or datetime.now()
    
    def to_dict(self):
        """
        Convert NewsFeedItem object to dictionary.
        """
        return {
            "feed_item_id": self.feed_item_id,
            "post_id": self.post_id,
            "user_id": self.user_id,
            "timestamp": self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """
        Create NewsFeedItem object from dictionary.
        """
        feed_item = cls(
            data["post_id"],
            data.get("user_id"),
            datetime.fromisoformat(data["timestamp"])
        )
        feed_item.feed_item_id = data["feed_item_id"]
        return feed_item 