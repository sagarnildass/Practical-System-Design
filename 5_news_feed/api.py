"""
RESTful API for the news feed system.

This module implements a RESTful API for the news feed system
using FastAPI, providing endpoints for user management, post CRUD,
feed retrieval, and user relationships.
"""

import logging
import time
import os
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional, Union, Any

from fastapi import FastAPI, Depends, HTTPException, Request, Response, Header, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, EmailStr
import uvicorn

# Try to import local config if exists, otherwise use default config
try:
    import config_local as config
    print("Using local configuration")
except ImportError:
    import config
    print("Using default configuration")

from config import API_VERSION, PORT, RATE_LIMITS, LOGGING
from feed_service import FeedService
from database import DatabaseService
from cache import (
    ActionCache, ContentCache, CounterCache, 
    NewsFeedCache, SocialGraphCache
)
from fanout import FanoutManager
from models import ActionType, PostType, RelationshipType

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOGGING["level"]),
    format=LOGGING["format"]
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="News Feed API",
    description="RESTful API for a scalable news feed system",
    version=API_VERSION
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
db_service = None
cache_service = None
fanout_manager = None
feed_service = None

# Pydantic models for request/response validation
class UserCreate(BaseModel):
    username: str
    email: EmailStr
    profile_picture_url: Optional[str] = None

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    profile_picture_url: Optional[str] = None

class UserResponse(BaseModel):
    user_id: str
    username: str
    email: str
    profile_picture_url: Optional[str] = None
    created_at: str
    updated_at: str

class PostCreate(BaseModel):
    content: str
    post_type: str = "TEXT"
    media_urls: Optional[List[str]] = None
    media_types: Optional[List[str]] = None

class PostResponse(BaseModel):
    post_id: str
    user_id: str
    content: str
    post_type: str
    created_at: str
    updated_at: str
    media_ids: Optional[List[str]] = None
    username: Optional[str] = None
    profile_picture_url: Optional[str] = None
    like_count: Optional[int] = 0
    comment_count: Optional[int] = 0
    share_count: Optional[int] = 0
    liked_by_me: Optional[bool] = False

class CommentCreate(BaseModel):
    content: str

class ShareCreate(BaseModel):
    content: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    database: str
    cache: str
    fanout: str
    timestamp: str

class StatsResponse(BaseModel):
    timestamp: str
    database: Optional[Dict[str, int]] = None
    cache: Optional[Dict[str, Any]] = None
    fanout: Optional[Dict[str, Any]] = None

class FeedResponse(BaseModel):
    posts: List[PostResponse]
    limit: int
    offset: int
    count: int

class ActionResponse(BaseModel):
    success: bool

class UsersListResponse(BaseModel):
    users: List[UserResponse]
    count: int

class ActionListResponse(BaseModel):
    actions: List[Dict[str, Any]]
    count: int


def init_services():
    """
    Initialize all services needed for the API.
    """
    global db_service, cache_service, fanout_manager, feed_service
    
    # Initialize database service
    db_service = DatabaseService()
    
    # Initialize cache service
    cache_service = type('CacheService', (), {
        'news_feed': NewsFeedCache(),
        'content': ContentCache(),
        'social_graph': SocialGraphCache(),
        'action': ActionCache(),
        'counter': CounterCache(),
        'redis': NewsFeedCache().redis  # Share redis connection
    })
    
    # Initialize fanout manager
    fanout_manager = FanoutManager(db_service, cache_service)
    
    # Initialize feed service
    feed_service = FeedService(db_service, cache_service, fanout_manager)
    
    logger.info("All services initialized")
    
    return db_service, cache_service, feed_service, fanout_manager


# Initialize services on startup
@app.on_event("startup")
def startup_event():
    """
    Initialize services when the application starts.
    """
    global db_service, cache_service, feed_service, fanout_manager
    db_service, cache_service, feed_service, fanout_manager = init_services()
    
    logger.info("Initializing services...")
    
    # Initialize database
    db_service.connect()
    logger.info("Database service initialized")
    
    # Start fanout service
    fanout_manager.start()
    logger.info("Fanout service started")
    
    logger.info("All services initialized successfully")


@app.on_event("shutdown")
def shutdown_event():
    if fanout_manager:
        fanout_manager.stop()
    if db_service:
        db_service.close()
    logger.info("Services shut down")


# Dependency to get the current user ID
async def get_current_user(x_user_id: Optional[str] = Header(None)):
    if not x_user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return x_user_id


# Rate limiting dependency
def rate_limit(limit_key: str):
    async def limit_dependency(request: Request):
        if not cache_service or not cache_service.redis:
            # Skip rate limiting if Redis is not available
            return True
            
        # Get user_id from request
        user_id = request.headers.get('X-User-ID', 'anonymous')
        
        # Rate limit key
        key = f"rate_limit:{limit_key}:{user_id}"
        
        # Get current count
        count = cache_service.redis.get(key)
        
        try:
            # Check if we've hit the rate limit
            max_requests = RATE_LIMITS.get(limit_key, 10)  # Default to 10 if not specified
            if count and int(count) >= max_requests:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded",
                    headers={"Retry-After": "60"},  # 1 minute
                )
            
            # Increment count
            pipe = cache_service.redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, 60)  # 1 minute window
            pipe.execute()
            
            return True
        except Exception as e:
            logger.error(f"Rate limiting error: {e}")
            return True  # Allow the request in case of error
    return limit_dependency


# Middleware for request timing and logging
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(
        f"{request.method} {request.url.path} {response.status_code} {process_time:.2f}s"
    )
    return response


# Health check endpoint
@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    # Check database connection
    db_status = "unknown"
    cache_status = "unknown"
    fanout_status = "unknown"
    
    try:
        # Check database connection
        db_status = "ok" if db_service and db_service.is_connected() else "error"
    except Exception as e:
        logger.error(f"Health check - database error: {e}")
        db_status = "error" 
    
    try:
        # Check cache connection
        cache_status = "ok" if cache_service and cache_service.redis and cache_service.news_feed.is_connected() else "error"
    except Exception as e:
        logger.error(f"Health check - cache error: {e}")
        cache_status = "error"
    
    try:
        # Check fanout service
        fanout_status = "ok" if fanout_manager and fanout_manager.is_running() else "error"
    except Exception as e:
        logger.error(f"Health check - fanout error: {e}")
        fanout_status = "error"
    
    # Overall status
    status = "ok" if db_status == "ok" and cache_status == "ok" and fanout_status == "ok" else "error"
    
    return {
        'status': status,
        'database': db_status,
        'cache': cache_status,
        'fanout': fanout_status,
        'timestamp': datetime.now().isoformat()
    }


# System statistics endpoint
@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    stats = {
        'timestamp': datetime.now().isoformat()
    }
    
    # Database stats
    if db_service and db_service.is_connected():
        stats['database'] = db_service.get_stats()
        
    # Cache stats
    if cache_service.redis and cache_service.news_feed.is_connected():
        # Get Redis info
        info = cache_service.redis.info()
        stats['cache'] = {
            'used_memory': info.get('used_memory_human', 'N/A'),
            'used_memory_peak': info.get('used_memory_peak_human', 'N/A'),
            'connected_clients': info.get('connected_clients', 'N/A'),
            'uptime_in_seconds': info.get('uptime_in_seconds', 'N/A')
        }
        
    # Fanout stats
    if fanout_manager and fanout_manager.is_running():
        stats['fanout'] = fanout_manager.get_stats()
        
    return stats


# User endpoints
@app.post("/api/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    user_data: UserCreate,
    _: bool = Depends(rate_limit("create_user"))
):
    # Check if username already exists
    existing_user = db_service.get_user_by_username(user_data.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already exists"
        )
        
    # Create user
    user = db_service.create_user(
        username=user_data.username,
        email=user_data.email,
        profile_picture_url=user_data.profile_picture_url
    )
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )
        
    # Cache user data
    cache_service.content.set_user(user)
    
    return user.to_dict()


@app.get("/api/users", response_model=List[UserResponse])
async def get_all_users():
    """Get all users."""
    # Get all users from database
    users = db_service.get_all_users()
    
    if not users:
        return []
        
    # Return list of user dictionaries
    return [user.to_dict() for user in users]


@app.get("/api/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: str):
    # Try to get user from cache
    user_data = cache_service.content.get_user(user_id)
    
    if user_data:
        # Cache hit
        return user_data
        
    # Cache miss, get from database
    user = db_service.get_user(user_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
        
    # Cache for future requests
    cache_service.content.set_user(user)
    
    return user.to_dict()


@app.put("/api/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str, 
    user_data: UserUpdate, 
    current_user: str = Depends(get_current_user)
):
    # Check if the authenticated user is the user being updated
    if current_user != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unauthorized"
        )
        
    # Get current user data
    user = db_service.get_user(user_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
        
    # Update fields
    if user_data.profile_picture_url is not None:
        user.profile_picture_url = user_data.profile_picture_url
        
    if user_data.email is not None:
        user.email = user_data.email
        
    # Update in database
    success = db_service.update_user(user)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )
        
    # Update cache
    cache_service.content.set_user(user)
    
    return user.to_dict()


@app.delete("/api/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: str, 
    current_user: str = Depends(get_current_user)
):
    # Check if the authenticated user is the user being deleted
    if current_user != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unauthorized"
        )
        
    # Delete from database
    success = db_service.delete_user(user_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )


# Post endpoints
@app.post("/api/posts", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def create_post(
    post_data: PostCreate, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("publish_post"))
):
    # Get post type
    try:
        post_type = PostType[post_data.post_type]
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid post type: {post_data.post_type}"
        )
        
    # Create post
    post = feed_service.publish_post(
        user_id=current_user,
        content=post_data.content,
        post_type=post_type,
        media_urls=post_data.media_urls,
        media_types=post_data.media_types
    )
    
    if not post:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create post"
        )
    
    # Enrich post with user info and action counts
    enriched_posts = feed_service._enrich_posts([post])
    
    if not enriched_posts:
        return post.to_dict()  # Fallback to original post if enrichment fails
        
    return enriched_posts[0].to_dict()


@app.get("/api/posts/{post_id}", response_model=PostResponse)
async def get_post(post_id: str):
    # Try to get post from cache
    post_data = cache_service.content.get_post(post_id)
    
    if post_data:
        # Cache hit
        post = Post.from_dict(post_data)
    else:
        # Cache miss, get from database
        post = db_service.get_post(post_id)
        
        if not post:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Post not found"
            )
            
        # Cache for future requests
        cache_service.content.set_post(post)
        
    # Enrich post with user info and action counts
    enriched_posts = feed_service._enrich_posts([post])
    
    if not enriched_posts:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to enrich post data"
        )
        
    return enriched_posts[0].to_dict()


@app.delete("/api/posts/{post_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_post(
    post_id: str, 
    current_user: str = Depends(get_current_user)
):
    # Delete post
    success = feed_service.delete_post(current_user, post_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete post"
        )


@app.get("/api/users/{user_id}/posts", response_model=FeedResponse)
async def get_user_posts(
    user_id: str, 
    limit: int = 20, 
    offset: int = 0
):
    # Get posts
    posts = db_service.get_user_posts(user_id, limit, offset)
    
    # Enrich posts with user info
    enriched_posts = feed_service._enrich_posts(posts)
    
    return {
        'posts': [post.to_dict() for post in enriched_posts],
        'limit': limit,
        'offset': offset,
        'count': len(posts)
    }


# Feed endpoints
@app.get("/api/feed", response_model=FeedResponse)
async def get_feed(
    limit: int = 20, 
    offset: int = 0, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("get_feed"))
):
    # Get feed
    posts = feed_service.get_news_feed(current_user, limit, offset)
    
    # Make sure posts are enriched
    if posts and (not hasattr(posts[0], 'username') or not posts[0].username):
        posts = feed_service._enrich_posts(posts)
    
    return {
        'posts': [post.to_dict() for post in posts],
        'limit': limit,
        'offset': offset,
        'count': len(posts)
    }


# Action endpoints
@app.post("/api/posts/{post_id}/like", response_model=ActionResponse)
async def like_post(
    post_id: str, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("post_action"))
):
    success = feed_service.like_post(current_user, post_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to like post"
        )
        
    return {'success': True}


@app.delete("/api/posts/{post_id}/like", response_model=ActionResponse)
async def unlike_post(
    post_id: str, 
    current_user: str = Depends(get_current_user)
):
    success = feed_service.unlike_post(current_user, post_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to unlike post"
        )
        
    return {'success': True}


@app.post("/api/posts/{post_id}/comment", response_model=PostResponse, 
          status_code=status.HTTP_201_CREATED)
async def comment_on_post(
    post_id: str, 
    comment_data: CommentCreate, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("post_action"))
):
    # Create comment
    comment = feed_service.comment_on_post(
        user_id=current_user,
        post_id=post_id,
        content=comment_data.content
    )
    
    if not comment:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to comment on post"
        )
        
    return comment.to_dict()


@app.post("/api/posts/{post_id}/share", response_model=PostResponse, 
          status_code=status.HTTP_201_CREATED)
async def share_post(
    post_id: str, 
    share_data: ShareCreate, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("post_action"))
):
    # Create share
    share = feed_service.share_post(
        user_id=current_user,
        post_id=post_id,
        content=share_data.content
    )
    
    if not share:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to share post"
        )
        
    return share.to_dict()


@app.get("/api/posts/{post_id}/actions", response_model=ActionListResponse)
async def get_post_actions(
    post_id: str, 
    action_type: Optional[str] = None
):
    if action_type:
        try:
            action_type_enum = ActionType[action_type]
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid action type: {action_type}"
            )
    else:
        action_type_enum = None
        
    # Get actions
    actions = db_service.get_actions_by_post(post_id, action_type_enum)
    
    return {
        'actions': [action.to_dict() for action in actions],
        'count': len(actions)
    }


# Relationship endpoints
@app.post("/api/users/{friend_id}/follow", response_model=ActionResponse)
async def follow_user(
    friend_id: str, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("relationship_action"))
):
    # Check if trying to follow self
    if current_user == friend_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot follow yourself"
        )
        
    # Follow user
    success = feed_service.follow_user(current_user, friend_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to follow user"
        )
        
    return {'success': True}


@app.delete("/api/users/{friend_id}/follow", response_model=ActionResponse)
async def unfollow_user(
    friend_id: str, 
    current_user: str = Depends(get_current_user)
):
    # Unfollow user
    success = feed_service.unfollow_user(current_user, friend_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to unfollow user"
        )
        
    return {'success': True}


@app.post("/api/users/{friend_id}/block", response_model=ActionResponse)
async def block_user(
    friend_id: str, 
    current_user: str = Depends(get_current_user),
    _: bool = Depends(rate_limit("relationship_action"))
):
    # Block user
    success = feed_service.block_user(current_user, friend_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to block user"
        )
        
    return {'success': True}


@app.delete("/api/users/{friend_id}/block", response_model=ActionResponse)
async def unblock_user(
    friend_id: str, 
    current_user: str = Depends(get_current_user)
):
    # Unblock user
    success = feed_service.unblock_user(current_user, friend_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to unblock user"
        )
        
    return {'success': True}


@app.get("/api/users/{user_id}/followers", response_model=UsersListResponse)
async def get_followers(user_id: str):
    # Get followers
    follower_ids = db_service.get_followers(user_id)
    
    followers = []
    for follower_id in follower_ids:
        # Try to get from cache
        user_data = cache_service.content.get_user(follower_id)
        
        if not user_data:
            # Cache miss, get from database
            user = db_service.get_user(follower_id)
            if user:
                user_data = user.to_dict()
                cache_service.content.set_user(user)
                
        if user_data:
            followers.append(user_data)
            
    return {
        'users': followers,
        'count': len(followers)
    }


@app.get("/api/users/{user_id}/following", response_model=UsersListResponse)
async def get_following(user_id: str):
    # Get friends with follow relationship
    friend_ids = db_service.get_friends(
        user_id, relationship_type=RelationshipType.FOLLOW
    )
    
    following = []
    for friend_id in friend_ids:
        # Try to get from cache
        user_data = cache_service.content.get_user(friend_id)
        
        if not user_data:
            # Cache miss, get from database
            user = db_service.get_user(friend_id)
            if user:
                user_data = user.to_dict()
                cache_service.content.set_user(user)
                
        if user_data:
            following.append(user_data)
            
    return {
        'users': following,
        'count': len(following)
    }


@app.get("/api/users/{user_id}/stats")
async def get_user_stats(user_id: str):
    """Get stats for a user (post count, follower count, following count)."""
    try:
        # Get post count
        posts_count = db_service.db.posts.count_documents({"user_id": user_id})
        
        # Get follower count
        followers_count = db_service.get_follower_count(user_id)
        
        # Get following count
        following_ids = db_service.get_friends(user_id, relationship_type=RelationshipType.FOLLOW)
        following_count = len(following_ids)
        
        return {
            "posts_count": posts_count,
            "followers_count": followers_count,
            "following_count": following_count
        }
    except Exception as e:
        logger.error(f"Error getting stats for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user stats: {str(e)}"
        )


@app.get("/api/posts/{post_id}/comments")
async def get_post_comments(post_id: str):
    """Get all comments for a post."""
    try:
        # Get actions with COMMENT type for this post
        comment_actions = db_service.get_actions_by_post(post_id, ActionType.COMMENT)
        
        if not comment_actions:
            return []
            
        # Extract comment data
        comments = []
        for action in comment_actions:
            # Get the user who made the comment
            user = db_service.get_user(action.user_id)
            
            if user:
                # In our real implementation, comments are created as posts with COMMENT type
                # For the comment text, we're using the content of that new post 
                # But for simplicity in this demo, we'll use the most recent post by this user
                user_posts = db_service.get_user_posts(action.user_id, limit=5)
                comment_content = "Comment" # Default placeholder
                
                # Find a comment post that was created around the same time as the action
                comment_post = None
                for post in user_posts:
                    # Use the post closest in time to when the comment action was created
                    if post.post_type == PostType.COMMENT or abs((post.created_at - action.created_at).total_seconds()) < 5:
                        comment_post = post
                        comment_content = post.content
                        break
                
                # Create a comment object with the necessary info
                comment = {
                    "comment_id": action.action_id,
                    "post_id": post_id,
                    "user_id": action.user_id,
                    "username": user.username,
                    "profile_picture_url": user.profile_picture_url,
                    "content": comment_content,
                    "created_at": action.created_at.isoformat()
                }
                comments.append(comment)
        
        # Sort comments by created_at, newest first
        comments.sort(key=lambda x: x["created_at"], reverse=True)
        
        return comments
    except Exception as e:
        logger.error(f"Error getting comments for post {post_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get comments: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=PORT, reload=True) 