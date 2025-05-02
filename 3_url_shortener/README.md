# URL Shortener System

This is a production-ready URL shortener service implementation that demonstrates key concepts used in large-scale URL shortening services like TinyURL or Bit.ly.

## System Design Features

- **Base62 Encoding:** Efficiently converts IDs to short URL strings using alphanumeric characters
- **Unique ID Generation:** Generates unique IDs for each new URL using a distributed ID generator based on Twitter's Snowflake
- **Caching Layer:** Uses Redis with in-memory fallback for high-performance URL retrieval
- **Persistent Storage:** Stores URL mappings in SQLite database (easy to set up and run locally)
- **FastAPI Framework:** Modern, high-performance Python web framework with automatic OpenAPI documentation
- **Async Support:** Leverages FastAPI's asynchronous capabilities for high throughput
- **Type Validation:** Uses Pydantic models for request/response validation
- **High Throughput:** Handles thousands of requests per second
- **Analytics Capability:** Tracks click counts and timestamps (optional)
- **Fault Tolerance:** Graceful degradation with fallback mechanisms for Redis

## Core Components

- `db.py`: Database models and SQLite connection management
- `cache.py`: Redis cache implementation with in-memory fallback
- `shortener.py`: URL shortening service with Base62 encoding
- `api.py`: API endpoints and Pydantic models
- `id_generator.py`: Snowflake-based distributed ID generator
- `app.py`: Main application entry point
- `config.py`: Configuration settings
- `test_shortener.py`: Test script to verify core functionality

## System Architecture

The system follows a layered architecture:

1. **API Layer:** FastAPI-based API with automatic OpenAPI documentation
2. **Service Layer:** Contains business logic for URL shortening and redirection
3. **Caching Layer:** Redis-based caching with in-memory fallback for high-performance URL lookups
4. **Persistence Layer:** SQLite database for durable storage

## Data Flow

### URL Shortening Flow
1. Client submits a long URL via API
2. System validates the request using Pydantic models
3. System checks if URL already exists in cache/database
4. If not found, generates a unique ID
5. Converts ID to a short URL string using Base62 encoding
6. Stores mapping in database and cache
7. Returns the short URL to the client

### URL Redirection Flow
1. Client requests a short URL
2. System checks the cache for the short URL
3. If not in cache, queries the database
4. If found, returns HTTP 301/302 redirect to the original URL
5. If not found, returns 404 error
6. Optionally updates analytics data

## Prerequisites

- Python 3.8 or higher
- Redis (optional - system includes fallback mechanism)

## Installation

1. Clone the repository
2. Install the requirements:
   ```
   pip install -r requirements.txt
   ```

## Running the System

### Quick Testing

Run the test script to verify core functionality:
```
python test_shortener.py
```

This will test URL shortening, retrieval, and click tracking functionality using the SQLite database.

### Running the Web Application

1. Start the application:
   ```
   python app.py
   ```
   
2. By default, the application runs on port 8000. If this port is in use, you can specify a different port:
   ```
   PORT=8001 python app.py
   ```

3. Access the web interface at: `http://localhost:8000/` (or your custom port)
4. View the API documentation at: `http://localhost:8000/docs` (or your custom port)

## API Usage

```bash
# Create a short URL
curl -X POST -H "Content-Type: application/json" -d '{"url":"https://example.com/very/long/url"}' http://localhost:8000/api/v1/shorten

# Redirect using a short URL
curl -L http://localhost:8000/abc123

# Get URL statistics
curl http://localhost:8000/api/v1/stats/abc123
```

## Web Interface

The system includes a simple web interface for shortening URLs, accessible at the root URL (`/`). This interface allows:

1. Entering a long URL to be shortened
2. Getting back the shortened URL
3. One-click copying of the shortened URL

## Fallback Mechanisms

The system includes fallback mechanisms to ensure it remains operational even when some components are unavailable:

1. **Redis Fallback:** If Redis is unavailable, the system automatically falls back to an in-memory cache
2. **Database Retry Logic:** Failed database operations have retry logic to handle transient failures

## Extending the System

The modular design allows for easy extension:

1. **Custom Analytics:** Extend the click tracking to include more data points
2. **Rate Limiting:** Configure the rate limits in `config.py`
3. **Custom Short URL Generation:** Modify the Base62 encoding in `shortener.py`
4. **Database Migration:** Replace SQLite with another database by modifying `db.py`

## Example

A URL like `https://www.sagarnildas.com/?utm_source=API&utm_medium=cpc&utm_campaign=test_url_shortener&utm_id=system_design` was shortened to `Gmi1j7Nxfi`, demonstrating the system's ability to handle long URLs with tracking parameters. 