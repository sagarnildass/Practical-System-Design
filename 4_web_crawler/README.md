---
title: AI_SEO_Crawler
app_file: seo_analyzer_ui.py
sdk: gradio
sdk_version: 5.30.0
---
# Web Crawler Documentation

A scalable web crawler with configurability, politeness, and content extraction capabilities.

## Table of Contents

- [Architecture](#architecture)
- [Setup](#setup)
- [Usage](#usage)
- [Components](#components)
- [Troubleshooting](#troubleshooting)

## Architecture

The web crawler consists of the following key components:

1. **URL Frontier**: Manages URLs to be crawled with prioritization
2. **DNS Resolver**: Caches DNS lookups to improve performance
3. **Robots Handler**: Ensures compliance with robots.txt
4. **HTML Downloader**: Downloads web pages with error handling
5. **HTML Parser**: Extracts URLs and metadata from web pages
6. **Storage**: MongoDB for storage of URLs and metadata
7. **Crawler**: Main crawler orchestration
8. **API**: REST API for controlling the crawler

## Setup

### Requirements

- Python 3.8+
- MongoDB
- Redis server

### Installation

1. Install MongoDB:
   ```bash
   # For Ubuntu
   sudo apt-get install -y mongodb
   sudo systemctl start mongodb
   sudo systemctl enable mongodb
   
   # Verify MongoDB is running
   sudo systemctl status mongodb
   ```

2. Install Redis:
   ```bash
   sudo apt-get install redis-server
   sudo systemctl start redis-server
   
   # Verify Redis is running
   redis-cli ping  # Should return PONG
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a local configuration file:
   ```bash
   cp config.py local_config.py
   ```

5. Edit `local_config.py` to customize settings:
   ```python
   # Example configuration
   SEED_URLS = ["https://example.com"]  # Start URLs
   MAX_DEPTH = 3                        # Crawl depth
   MAX_WORKERS = 4                      # Number of worker threads
   DELAY_BETWEEN_REQUESTS = 1           # Politeness delay
   ```

## Usage

### Running the Crawler

To run the crawler with default settings:

```bash
cd 4_web_crawler
python run_crawler.py
```

To specify custom seed URLs:

```bash
python run_crawler.py --seed https://example.com https://another-site.com
```

To limit crawl depth:

```bash
python run_crawler.py --depth 2
```

To run with more worker threads:

```bash
python run_crawler.py --workers 8
```

### Sample Commands

Here are some common use cases with sample commands:

#### Crawl a Single Domain

This command crawls only example.com, not following external links:

```bash
python run_crawler.py --seed example.com --domain-filter example.com
```

#### Fresh Start (Reset Database)

This clears both MongoDB and Redis before starting, solving duplicate key errors:

```bash
python run_crawler.py --seed example.com --reset-db
```

#### Custom Speed and Depth

Control the crawler's speed and depth:

```bash
python run_crawler.py --seed example.com --depth 3 --workers 4 --delay 0.5
```

#### Crawl Multiple Sites

Crawl multiple websites at once:

```bash
python run_crawler.py --seed example.com blog.example.org docs.example.com
```

#### Ignore robots.txt Rules

Use with caution, as this ignores website crawling policies:

```bash
python run_crawler.py --seed example.com --ignore-robots
```

#### Set Custom User Agent

Identity the crawler with a specific user agent:

```bash
python run_crawler.py --seed example.com --user-agent "MyCustomBot/1.0"
```

#### Crawl sagarnildas.com

To specifically crawl sagarnildas.com with optimal settings:

```bash
python run_crawler.py --seed sagarnildas.com --domain-filter sagarnildas.com --reset-db --workers 2 --depth 3 --verbose
```

### Using the API

The crawler provides a REST API for control and monitoring:

```bash
cd 4_web_crawler
python api.py
```

The API will be available at http://localhost:8000

#### API Endpoints

- `GET /status` - Get crawler status
- `GET /stats` - Get detailed statistics
- `POST /start` - Start the crawler
- `POST /stop` - Stop the crawler
- `POST /seed` - Add seed URLs
- `GET /pages` - List crawled pages
- `GET /urls` - List discovered URLs

### Checking Results

Monitor the crawler through:

1. Console output:
   ```bash
   tail -f crawler.log
   ```

2. MongoDB collections:
   ```bash
   # Start mongo shell
   mongo
   
   # Switch to crawler database
   use crawler
   
   # Count discovered URLs
   db.urls.count()
   
   # View crawled pages
   db.pages.find().limit(5)
   ```

3. API statistics:
   ```bash
   curl http://localhost:8000/stats
   ```

## Components

The crawler has several key components that work together:

### URL Frontier

Manages the queue of URLs to be crawled with priority-based scheduling.

### DNS Resolver

Caches DNS lookups to improve performance and reduce load on DNS servers.

### Robots Handler

Ensures compliance with robots.txt rules to be a good web citizen.

### HTML Downloader

Downloads web pages with error handling, timeouts, and retries.

### HTML Parser

Extracts URLs and metadata from web pages.

### Crawler

The main component that orchestrates the crawling process.

## Troubleshooting

### MongoDB Errors

If you see duplicate key errors:

```
ERROR: Error saving seed URL to database: E11000 duplicate key error
```

Clean MongoDB collections:

```bash
cd 4_web_crawler
python mongo_cleanup.py
```

### Redis Connection Issues

If the crawler can't connect to Redis:

1. Check if Redis is running:
   ```bash
   sudo systemctl status redis-server
   ```

2. Verify Redis connection:
   ```bash
   redis-cli ping
   ```

### Performance Issues

If the crawler is running slowly:

1. Increase worker threads in `local_config.py`:
   ```python
   MAX_WORKERS = 8
   ```

2. Adjust the politeness delay:
   ```python
   DELAY_BETWEEN_REQUESTS = 0.5  # Half-second delay
   ```

3. Optimize DNS caching:
   ```python
   DNS_CACHE_SIZE = 10000
   DNS_CACHE_TTL = 7200  # 2 hours
   ```

### Crawler Not Starting

If the crawler won't start:

1. Check for MongoDB connection:
   ```bash
   mongo --eval "db.version()"
   ```

2. Ensure Redis is running:
   ```bash
   redis-cli info
   ```

3. Look for error messages in the logs:
   ```bash
   cat crawler.log
   ```

## Configuration Reference

Key configurations in `config.py` or `local_config.py`:

```python
# General settings
MAX_WORKERS = 4            # Number of worker threads
MAX_DEPTH = 3              # Maximum crawl depth
SEED_URLS = ["https://example.com"]  # Initial URLs

# Politeness settings
RESPECT_ROBOTS_TXT = True  # Whether to respect robots.txt
USER_AGENT = "MyBot/1.0"   # User agent for requests
DELAY_BETWEEN_REQUESTS = 1 # Delay between requests to the same domain

# Storage settings
MONGODB_URI = "mongodb://localhost:27017/"
MONGODB_DB = "crawler" 

# DNS settings
DNS_CACHE_SIZE = 10000
DNS_CACHE_TTL = 3600       # 1 hour

# Logging settings
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
``` 