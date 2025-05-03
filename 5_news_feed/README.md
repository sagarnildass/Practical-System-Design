# News Feed System

A highly scalable, real-time news feed system similar to social media platforms like Twitter, Facebook, or Instagram. This system processes and displays content updates from followed accounts in a user's personalized feed.

## Getting Started

### Prerequisites

- Python 3.7+
- MongoDB
- Redis
- Node.js and npm (for the UI)

### Installation

1. **Backend Setup**:
   ```bash
   # Install backend dependencies
   pip install -r requirements.txt
   
   # Configure environment variables or update config.py
   
   # Start the API server
   uvicorn api:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **Frontend Setup**:
   ```bash
   # Navigate to the UI directory
   cd ui
   
   # Install frontend dependencies
   npm install
   
   # Start the development server
   npm run dev
   ```

### Creating Virtual Users and Content

To populate the system with sample data:

1. **Run the test script to create users and content**:
   ```bash
   python test_api.py
   ```

   This will create:
   - Several test users (Alice, Bob, Charlie, Diana, Emma, etc.)
   - Sample posts for each user
   - Follow relationships between users
   - Various interactions (likes, comments, shares)

2. **Alternatively, use the API directly**:
   ```bash
   # Create a new user
   curl -X POST "http://localhost:8000/api/users" -H "Content-Type: application/json" -d '{"username":"newuser","email":"newuser@example.com"}'
   
   # Create a post
   curl -X POST "http://localhost:8000/api/posts" -H "Content-Type: application/json" -H "X-User-ID: USER_ID_HERE" -d '{"content":"Hello world!","post_type":"TEXT"}'
   ```

3. **Set up user relationships**:
   ```bash
   # Make one user follow another
   curl -X POST "http://localhost:8000/api/users/TARGET_USER_ID/follow" -H "X-User-ID: FOLLOWER_USER_ID"
   ```

## Using the UI

1. **Select a User**: Use the dropdown at the top to choose which user you want to interact as.

2. **Browse the Feed**: The feed tab shows posts from users you follow, including their original posts, shares, and interactions.

3. **User Posts**: View and create posts from the selected user.

4. **Network**: See followers and following, with options to follow back or unfollow users.

5. **Interactions**:
   - Like/unlike posts with the heart button
   - Comment on posts using the comment button
   - Share posts using the share button and adding your comment

## Features

### Core Features

- **User Management**: Create accounts, update profiles, follow/unfollow users
- **Content Creation**: Post text, images, videos, and links
- **Social Interactions**: Like, comment, and share posts
- **Personalized Feeds**: View content from followed accounts
- **Network Management**: View followers and following lists

### UI Features

- **Responsive Design**: Works on desktop and mobile devices
- **Real-time Updates**: Actions like follows/unfollows update immediately
- **User Switching**: Test different user perspectives without logging out
- **Content Hierarchy**: Clear visual distinction between posts, shares, and comments
- **Interactive Elements**: Buttons for likes, comments, shares with immediate feedback

### Backend Features

- **Scalable Architecture**: Multi-tier design with caching and database layers
- **Efficient Fanout**: Smart distribution of content to user feeds
- **Caching Strategy**: Five-tier Redis caching for performance
- **API-First Design**: RESTful endpoints for all operations
- **Data Consistency**: Reliable data storage with optimistic UI updates

## Architecture Overview

The news feed system follows a multi-tier architecture with the following key components:

1. **Data Models**: Core entities like users, posts, relationships, and actions
2. **Database Layer**: MongoDB-based persistence for all entities
3. **Cache Layer**: Redis-based caching system with multiple specialized caches
4. **Fanout Service**: Efficient distribution of new posts to follower feeds
5. **Feed Service**: Core business logic for feed operations
6. **RESTful API**: HTTP interface for client applications
7. **Frontend UI**: React-based user interface with Next.js

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

### Frontend (`ui/`)

- React-based user interface
- Next.js framework
- Tailwind CSS for styling
- Component-based architecture
- Responsive design for all devices

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

## Troubleshooting

### Common Issues

1. **API Connection Problems**:
   - Ensure the API server is running at http://localhost:8000
   - Check CORS settings if making cross-origin requests

2. **Missing Content in Feed**:
   - Verify that users have follow relationships established
   - Check if posts have been created recently

3. **UI Not Displaying Properly**:
   - Clear browser cache and refresh
   - Ensure all npm dependencies are installed

4. **Database Connection Issues**:
   - Verify MongoDB is running and accessible
   - Check connection strings in config

### Support

For additional help, please open an issue in the project repository.

## Future Enhancements

- Elasticsearch integration for full-text search
- Real-time notifications using WebSockets
- Media processing and CDN integration
- Analytics and trending topics
- Recommendation engine for feed personalization 