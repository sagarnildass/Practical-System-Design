"""
Web API for the web crawler.

This module provides a FastAPI-based web API for controlling and monitoring the web crawler.
"""

import os
import sys
import time
import json
import logging
import datetime
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query, Path, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, HttpUrl, Field
import uvicorn

from crawler import Crawler
from models import URL, URLStatus, Priority
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Web Crawler API",
    description="API for controlling and monitoring the web crawler",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global crawler instance
crawler = None


def get_crawler() -> Crawler:
    """Get or initialize the crawler instance"""
    global crawler
    if crawler is None:
        crawler = Crawler()
    return crawler


# API Models
class SeedURL(BaseModel):
    url: HttpUrl
    priority: Optional[str] = Field(
        default="NORMAL",
        description="URL priority (VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW)"
    )


class SeedURLs(BaseModel):
    urls: List[SeedURL]


class CrawlerStatus(BaseModel):
    running: bool
    paused: bool
    start_time: Optional[float] = None
    uptime_seconds: Optional[float] = None
    pages_crawled: int
    pages_failed: int
    urls_discovered: int
    urls_filtered: int
    domains_crawled: int
    frontier_size: int


class CrawlerConfig(BaseModel):
    max_depth: int = Field(..., description="Maximum crawl depth")
    max_workers: int = Field(..., description="Maximum number of worker threads")
    delay_between_requests: float = Field(..., description="Delay between requests to the same domain (seconds)")


class PageDetail(BaseModel):
    url: str
    domain: str
    status_code: int
    content_type: str
    content_length: int
    crawled_at: str
    is_seed: bool
    depth: int
    title: Optional[str] = None
    description: Optional[str] = None


class URLDetail(BaseModel):
    url: str
    normalized_url: str
    domain: str
    status: str
    priority: str
    depth: int
    parent_url: Optional[str] = None
    last_crawled: Optional[str] = None
    error: Optional[str] = None
    retries: int


class DomainStats(BaseModel):
    domain: str
    pages_count: int
    successful_requests: int
    failed_requests: int
    avg_page_size: float
    content_types: Dict[str, int]
    status_codes: Dict[str, int]


# API Routes
@app.get("/")
async def read_root():
    """Root endpoint"""
    return {
        "name": "Web Crawler API",
        "version": "1.0.0",
        "description": "API for controlling and monitoring the web crawler",
        "endpoints": {
            "GET /": "This help message",
            "GET /status": "Get crawler status",
            "GET /stats": "Get crawler statistics",
            "GET /config": "Get crawler configuration",
            "PUT /config": "Update crawler configuration",
            "POST /start": "Start the crawler",
            "POST /stop": "Stop the crawler",
            "POST /pause": "Pause the crawler",
            "POST /resume": "Resume the crawler",
            "GET /pages": "List crawled pages",
            "GET /pages/{url}": "Get page details",
            "GET /urls": "List discovered URLs",
            "GET /urls/{url}": "Get URL details",
            "POST /seed": "Add seed URLs",
            "GET /domains": "Get domain statistics",
            "GET /domains/{domain}": "Get statistics for a specific domain",
        }
    }


@app.get("/status", response_model=CrawlerStatus)
async def get_status(crawler: Crawler = Depends(get_crawler)):
    """Get crawler status"""
    status = {
        "running": crawler.running,
        "paused": crawler.paused,
        "start_time": crawler.stats.get('start_time'),
        "uptime_seconds": time.time() - crawler.stats.get('start_time', time.time()) if crawler.running else None,
        "pages_crawled": crawler.stats.get('pages_crawled', 0),
        "pages_failed": crawler.stats.get('pages_failed', 0),
        "urls_discovered": crawler.stats.get('urls_discovered', 0),
        "urls_filtered": crawler.stats.get('urls_filtered', 0),
        "domains_crawled": len(crawler.stats.get('domains_crawled', set())),
        "frontier_size": crawler.frontier.size()
    }
    return status


@app.get("/stats")
async def get_stats(crawler: Crawler = Depends(get_crawler)):
    """Get detailed crawler statistics"""
    stats = crawler.stats.copy()
    
    # Convert sets to lists for JSON serialization
    for key, value in stats.items():
        if isinstance(value, set):
            stats[key] = list(value)
    
    # Add uptime
    if stats.get('start_time'):
        stats['uptime_seconds'] = time.time() - stats['start_time']
        stats['uptime_formatted'] = str(datetime.timedelta(seconds=int(stats['uptime_seconds'])))
    
    # Add DNS cache statistics if available
    try:
        dns_stats = crawler.dns_resolver.get_stats()
        stats['dns_cache'] = dns_stats
    except (AttributeError, Exception) as e:
        logger.warning(f"Failed to get DNS stats: {e}")
        stats['dns_cache'] = {'error': 'Stats not available'}
    
    # Add frontier statistics if available
    try:
        stats['frontier_size'] = crawler.frontier.size()
        if hasattr(crawler.frontier, 'get_stats'):
            frontier_stats = crawler.frontier.get_stats()
            stats['frontier'] = frontier_stats
        else:
            stats['frontier'] = {'size': crawler.frontier.size()}
    except Exception as e:
        logger.warning(f"Failed to get frontier stats: {e}")
        stats['frontier'] = {'error': 'Stats not available'}
    
    return stats


@app.get("/config", response_model=CrawlerConfig)
async def get_config():
    """Get crawler configuration"""
    return {
        "max_depth": config.MAX_DEPTH,
        "max_workers": config.MAX_WORKERS,
        "delay_between_requests": config.DELAY_BETWEEN_REQUESTS
    }


@app.put("/config", response_model=CrawlerConfig)
async def update_config(
    crawler_config: CrawlerConfig,
    crawler: Crawler = Depends(get_crawler)
):
    """Update crawler configuration"""
    # Update configuration
    config.MAX_DEPTH = crawler_config.max_depth
    config.MAX_WORKERS = crawler_config.max_workers
    config.DELAY_BETWEEN_REQUESTS = crawler_config.delay_between_requests
    
    return crawler_config


@app.post("/start")
async def start_crawler(
    background_tasks: BackgroundTasks,
    num_workers: int = Query(None, description="Number of worker threads"),
    async_mode: bool = Query(False, description="Whether to use async mode"),
    crawler: Crawler = Depends(get_crawler)
):
    """Start the crawler"""
    if crawler.running:
        return {"status": "Crawler is already running"}
    
    # Start crawler in background
    def start_crawler_task():
        try:
            crawler.start(num_workers=num_workers, async_mode=async_mode)
        except Exception as e:
            logger.error(f"Error starting crawler: {e}")
    
    background_tasks.add_task(start_crawler_task)
    
    return {"status": "Crawler starting in background"}


@app.post("/stop")
async def stop_crawler(crawler: Crawler = Depends(get_crawler)):
    """Stop the crawler"""
    if not crawler.running:
        return {"status": "Crawler is not running"}
    
    crawler.stop()
    return {"status": "Crawler stopped"}


@app.post("/pause")
async def pause_crawler(crawler: Crawler = Depends(get_crawler)):
    """Pause the crawler"""
    if not crawler.running:
        return {"status": "Crawler is not running"}
    
    if crawler.paused:
        return {"status": "Crawler is already paused"}
    
    crawler.pause()
    return {"status": "Crawler paused"}


@app.post("/resume")
async def resume_crawler(crawler: Crawler = Depends(get_crawler)):
    """Resume the crawler"""
    if not crawler.running:
        return {"status": "Crawler is not running"}
    
    if not crawler.paused:
        return {"status": "Crawler is not paused"}
    
    crawler.resume()
    return {"status": "Crawler resumed"}


@app.get("/pages")
async def list_pages(
    limit: int = Query(10, ge=1, le=100, description="Number of pages to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    domain: Optional[str] = Query(None, description="Filter by domain"),
    status_code: Optional[int] = Query(None, description="Filter by HTTP status code"),
    crawler: Crawler = Depends(get_crawler)
):
    """List crawled pages"""
    # Build query
    query = {}
    if domain:
        query['domain'] = domain
    if status_code:
        query['status_code'] = status_code
    
    # Execute query
    try:
        pages = list(crawler.db.pages_collection.find(
            query,
            {'_id': 0}
        ).skip(offset).limit(limit))
        
        # Count total pages matching query
        total_count = crawler.db.pages_collection.count_documents(query)
        
        return {
            "pages": pages,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        logger.error(f"Error listing pages: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pages/{url:path}", response_model=PageDetail)
async def get_page(
    url: str,
    include_content: bool = Query(False, description="Include page content"),
    crawler: Crawler = Depends(get_crawler)
):
    """Get page details"""
    try:
        # Decode URL from path parameter
        url = url.replace("___", "/")
        
        # Find page in database
        page = crawler.db.pages_collection.find_one({'url': url}, {'_id': 0})
        
        if not page:
            raise HTTPException(status_code=404, detail="Page not found")
        
        # Load content if requested
        if include_content:
            try:
                if crawler.use_s3:
                    content = crawler._load_content_s3(url)
                else:
                    content = crawler._load_content_disk(url)
                
                if content:
                    page['content'] = content
            except Exception as e:
                logger.error(f"Error loading content for {url}: {e}")
                page['content'] = None
        
        return page
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting page {url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/urls")
async def list_urls(
    limit: int = Query(10, ge=1, le=100, description="Number of URLs to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    status: Optional[str] = Query(None, description="Filter by URL status"),
    domain: Optional[str] = Query(None, description="Filter by domain"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    crawler: Crawler = Depends(get_crawler)
):
    """List discovered URLs"""
    # Build query
    query = {}
    if status:
        query['status'] = status
    if domain:
        query['domain'] = domain
    if priority:
        query['priority'] = priority
    
    # Execute query
    try:
        urls = list(crawler.db.urls_collection.find(
            query,
            {'_id': 0}
        ).skip(offset).limit(limit))
        
        # Count total URLs matching query
        total_count = crawler.db.urls_collection.count_documents(query)
        
        return {
            "urls": urls,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        logger.error(f"Error listing URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/urls/{url:path}", response_model=URLDetail)
async def get_url(
    url: str,
    crawler: Crawler = Depends(get_crawler)
):
    """Get URL details"""
    try:
        # Decode URL from path parameter
        url = url.replace("___", "/")
        
        # Find URL in database
        url_obj = crawler.db.urls_collection.find_one({'url': url}, {'_id': 0})
        
        if not url_obj:
            raise HTTPException(status_code=404, detail="URL not found")
        
        return url_obj
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting URL {url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/seed")
async def add_seed_urls(
    seed_urls: SeedURLs,
    crawler: Crawler = Depends(get_crawler)
):
    """Add seed URLs to the frontier"""
    try:
        urls_added = 0
        for seed in seed_urls.urls:
            url = str(seed.url)
            priority = getattr(Priority, seed.priority, Priority.NORMAL)
            
            # Create URL object
            url_obj = URL(
                url=url,
                status=URLStatus.PENDING,
                priority=priority,
                depth=0  # Seed URLs are at depth 0
            )
            
            # Add to frontier
            if crawler.frontier.add_url(url_obj):
                # Save URL to database
                crawler.urls_collection.update_one(
                    {'url': url},
                    {'$set': url_obj.dict()},
                    upsert=True
                )
                
                urls_added += 1
                logger.info(f"Added seed URL: {url}")
        
        return {"status": "success", "urls_added": urls_added}
    except Exception as e:
        logger.error(f"Error adding seed URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/domains")
async def list_domains(
    limit: int = Query(10, ge=1, le=100, description="Number of domains to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    crawler: Crawler = Depends(get_crawler)
):
    """Get domain statistics"""
    try:
        # Get domains with counts
        domain_counts = crawler.db.pages_collection.aggregate([
            {"$group": {
                "_id": "$domain",
                "pages_count": {"$sum": 1},
                "avg_page_size": {"$avg": "$content_length"}
            }},
            {"$sort": {"pages_count": -1}},
            {"$skip": offset},
            {"$limit": limit}
        ])
        
        # Get total domains count
        total_domains = len(crawler.stats.get('domains_crawled', set()))
        
        # Format result
        domains = []
        for domain in domain_counts:
            domains.append({
                "domain": domain["_id"],
                "pages_count": domain["pages_count"],
                "avg_page_size": domain["avg_page_size"]
            })
        
        return {
            "domains": domains,
            "total": total_domains,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        logger.error(f"Error listing domains: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/domains/{domain}", response_model=DomainStats)
async def get_domain_stats(
    domain: str,
    crawler: Crawler = Depends(get_crawler)
):
    """Get statistics for a specific domain"""
    try:
        # Get basic domain stats
        domain_stats = crawler.db.pages_collection.aggregate([
            {"$match": {"domain": domain}},
            {"$group": {
                "_id": "$domain",
                "pages_count": {"$sum": 1},
                "successful_requests": {"$sum": {"$cond": [{"$lt": ["$status_code", 400]}, 1, 0]}},
                "failed_requests": {"$sum": {"$cond": [{"$gte": ["$status_code", 400]}, 1, 0]}},
                "avg_page_size": {"$avg": "$content_length"}
            }}
        ]).next()
        
        # Get content type distribution
        content_types = crawler.db.pages_collection.aggregate([
            {"$match": {"domain": domain}},
            {"$group": {
                "_id": "$content_type",
                "count": {"$sum": 1}
            }}
        ])
        
        content_type_map = {}
        for ct in content_types:
            content_type_map[ct["_id"]] = ct["count"]
        
        # Get status code distribution
        status_codes = crawler.db.pages_collection.aggregate([
            {"$match": {"domain": domain}},
            {"$group": {
                "_id": "$status_code",
                "count": {"$sum": 1}
            }}
        ])
        
        status_code_map = {}
        for sc in status_codes:
            status_code_map[str(sc["_id"])] = sc["count"]
        
        # Format result
        result = {
            "domain": domain,
            "pages_count": domain_stats["pages_count"],
            "successful_requests": domain_stats["successful_requests"],
            "failed_requests": domain_stats["failed_requests"],
            "avg_page_size": domain_stats["avg_page_size"],
            "content_types": content_type_map,
            "status_codes": status_code_map
        }
        
        return result
    except StopIteration:
        # Domain not found
        raise HTTPException(status_code=404, detail=f"Domain '{domain}' not found")
    except Exception as e:
        logger.error(f"Error getting domain stats for {domain}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Run the API server
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 