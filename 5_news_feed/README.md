# News Feed System

A highly scalable, real-time news feed system similar to social media platforms like Twitter, Facebook, or Instagram. This system processes and displays content updates from followed accounts in a user's personalized feed.

## Architecture Overview

The news feed system follows a multi-tier architecture with the following key components:

1. **Data Models**: Core entities like users, posts, relationships, and actions
2. **Database Layer**: MongoDB-based persistence for all entities
3. **Cache Layer**: Redis-based caching system with multiple specialized caches
4. **Fanout Service**: Efficient distribution of new posts to follower feeds
5. **Feed Service**: Core business logic for feed operations
6. **RESTful API**: HTTP interface for client applications

## Components

### Data Models (`models.py`)

The system uses the following main data models:

- **User**: Represents a user account with profile information
- **Post**: Content created by users (text, images, videos)
- **Relationship**: Connections between users (follow, block)
- **Action**: User interactions with posts (like, comment, share)
- **NewsFeedItem**: An entry in a user's personalized feed

### Database Service (`database.py`)

- MongoDB-backed persistence layer
- Efficient indexes for fast queries
- Transaction support for complex operations
- Methods for CRUD operations on all entities

### Cache System (`cache.py`)

Five-tier caching strategy using Redis:

1. **NewsFeedCache**: Stores feed items by user
2. **ContentCache**: Caches post and user data
3. **SocialGraphCache**: Caches user relationships
4. **ActionCache**: Stores user actions on posts
5. **CounterCache**: Maintains counters for likes, comments, etc.

### Fanout Service (`fanout.py`)

Distributes new posts to follower feeds using two strategies:

- **Push model (eager)**: For regular users, immediately pushes posts to follower feeds
- **Pull model (lazy)**: For celebrity users with many followers, defers distribution

Features:
- Asynchronous processing using a queue
- Batched processing for efficiency
- Configurable thresholds for celebrity status

### Feed Service (`feed_service.py`)

Core business logic for:
- Publishing posts
- Retrieving personalized feeds
- Processing user actions (like, comment, share)
- Managing user relationships (follow, unfollow, block)

### API (`api.py`)

RESTful API with endpoints for all operations:
- User management
- Post creation and retrieval
- Feed generation
- Social interactions
- Includes authentication, rate limiting, and request logging

## API Endpoints

### Health and Status

- `GET /api/health`: Health check for the system
- `GET /api/stats`: System statistics and metrics

### User Management

- `POST /api/users`: Create a new user
- `GET /api/users/<user_id>`: Get user profile
- `PUT /api/users/<user_id>`: Update user profile
- `DELETE /api/users/<user_id>`: Delete a user

### Posts

- `POST /api/posts`: Create a new post
- `GET /api/posts/<post_id>`: Get post by ID
- `DELETE /api/posts/<post_id>`: Delete a post
- `GET /api/users/<user_id>/posts`: Get posts by a specific user

### Feed

- `GET /api/feed`: Get personalized news feed

### Actions

- `POST /api/posts/<post_id>/like`: Like a post
- `DELETE /api/posts/<post_id>/like`: Unlike a post
- `POST /api/posts/<post_id>/comment`: Comment on a post
- `POST /api/posts/<post_id>/share`: Share a post
- `GET /api/posts/<post_id>/actions`: Get actions on a post

### Relationships

- `POST /api/users/<friend_id>/follow`: Follow a user
- `DELETE /api/users/<friend_id>/follow`: Unfollow a user
- `POST /api/users/<friend_id>/block`: Block a user
- `DELETE /api/users/<friend_id>/block`: Unblock a user
- `GET /api/users/<user_id>/followers`: Get followers of a user
- `GET /api/users/<user_id>/following`: Get users followed by a user

## Performance Considerations

### Scaling Strategies

- **Database Sharding**: Partition data by user ID
- **Read Replicas**: Scale read operations
- **Caching**: Reduce database load and latency
- **Celebrity Problem**: Special handling for high-follower accounts
- **Asynchronous Processing**: Offload heavy operations to background workers

### Cache Efficiency

- Sorted sets in Redis for time-ordered feeds
- TTL-based cache eviction
- Pipeline operations for batch processing
- Selective cache updates for frequently accessed data

## Setup and Configuration

Configuration is managed through `config.py`:

- Database settings
- Redis cache parameters
- Fanout thresholds and batching
- API rate limits
- Logging configuration

## Requirements

- Python 3.7+
- MongoDB
- Redis
- Flask
- PyMongo
- redis-py

## Running the System

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure environment variables or update `config.py`

3. Start the API server:
   ```
   python api.py
   ```

## Future Enhancements

- Elasticsearch integration for full-text search
- Real-time notifications using WebSockets
- Media processing and CDN integration
- Analytics and trending topics
- Recommendation engine for feed personalization 