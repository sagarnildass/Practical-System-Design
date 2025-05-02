"""
Data models for the web crawler
"""

import time
import hashlib
import tldextract
from urllib.parse import urlparse, urljoin, urlunparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from pydantic import BaseModel, Field, HttpUrl, validator
from enum import Enum


class URLStatus(str, Enum):
    """Status of a URL in the crawl process"""
    PENDING = "pending"  # Not yet processed
    IN_PROGRESS = "in_progress"  # Currently being processed
    COMPLETED = "completed"  # Successfully processed
    FAILED = "failed"  # Failed to process
    FILTERED = "filtered"  # Filtered out based on rules
    ROBOTSTXT_EXCLUDED = "robotstxt_excluded"  # Excluded by robots.txt


class Priority(int, Enum):
    """Priority levels for URLs"""
    VERY_HIGH = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4
    VERY_LOW = 5


class URL(BaseModel):
    """URL model with metadata for crawling"""
    url: str
    normalized_url: str = ""  # Normalized version of the URL
    domain: str = ""  # Domain extracted from the URL
    depth: int = 0  # Depth from seed URL
    discovered_at: datetime = Field(default_factory=datetime.now)
    last_crawled: Optional[datetime] = None
    status: URLStatus = URLStatus.PENDING
    priority: Priority = Priority.MEDIUM
    parent_url: Optional[str] = None  # URL that led to this URL
    retries: int = 0  # Number of times retried
    error: Optional[str] = None  # Error message if failed
    metadata: Dict[str, Any] = Field(default_factory=dict)  # Additional metadata

    @validator("normalized_url", pre=True, always=True)
    def set_normalized_url(cls, v, values):
        """Normalize the URL if not already set"""
        if not v and "url" in values:
            return normalize_url(values["url"])
        return v

    @validator("domain", pre=True, always=True)
    def set_domain(cls, v, values):
        """Extract domain from URL if not already set"""
        if not v and "url" in values:
            parsed = tldextract.extract(values["url"])
            return f"{parsed.domain}.{parsed.suffix}" if parsed.suffix else parsed.domain
        return v


class RobotsInfo(BaseModel):
    """Information from robots.txt for a domain"""
    domain: str
    allowed: bool = True  # Whether crawling is allowed
    crawl_delay: Optional[float] = None  # Crawl delay in seconds
    last_fetched: datetime = Field(default_factory=datetime.now)
    user_agents: Dict[str, Dict[str, Any]] = Field(default_factory=dict)  # Info per user agent
    status_code: Optional[int] = None  # HTTP status code when fetching robots.txt


class Page(BaseModel):
    """Web page model with content and metadata"""
    url: str
    status_code: int
    content: str  # HTML content
    content_type: str
    content_length: int
    content_hash: str  # Hash of the content for duplicate detection
    headers: Dict[str, str] = Field(default_factory=dict)
    links: List[str] = Field(default_factory=list)  # Links extracted from the page
    crawled_at: datetime = Field(default_factory=datetime.now)
    redirect_url: Optional[str] = None  # URL after redirects
    elapsed_time: float = 0.0  # Time taken to fetch the page
    is_duplicate: bool = False  # Whether this is duplicate content
    metadata: Dict[str, Any] = Field(default_factory=dict)  # Additional metadata


class DomainStats(BaseModel):
    """Statistics for a domain"""
    domain: str
    pages_crawled: int = 0
    successful_crawls: int = 0
    failed_crawls: int = 0
    last_crawled: Optional[datetime] = None
    robots_info: Optional[RobotsInfo] = None
    crawl_times: List[float] = Field(default_factory=list)  # Recent crawl times
    errors: Dict[int, int] = Field(default_factory=dict)  # Status code counts for errors


def normalize_url(url: str) -> str:
    """Normalize URL for consistent comparison and storage"""
    try:
        parsed = urlparse(url)
        
        # Convert to lowercase
        netloc = parsed.netloc.lower()
        path = parsed.path
        
        # Remove default ports
        if netloc.endswith(":80") and parsed.scheme == "http":
            netloc = netloc[:-3]
        elif netloc.endswith(":443") and parsed.scheme == "https":
            netloc = netloc[:-4]
        
        # Add trailing slash to path if empty
        if not path:
            path = "/"
        
        # Remove fragment
        fragment = ""
        
        # Recreate URL without fragments and with normalized components
        normalized = urlunparse((
            parsed.scheme,
            netloc,
            path,
            parsed.params,
            parsed.query,
            fragment
        ))
        
        return normalized
    except Exception:
        # If normalization fails, return the original URL
        return url


def calculate_content_hash(content: str) -> str:
    """Calculate hash of content for duplicate detection"""
    return hashlib.md5(content.encode('utf-8')).hexdigest() 