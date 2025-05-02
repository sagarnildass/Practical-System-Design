"""
Storage component for the web crawler.

Handles storing and retrieving crawled web pages using:
1. MongoDB for metadata, URL information, and crawl stats
2. Disk-based storage for HTML content
3. Optional Amazon S3 integration for scalable storage
"""

import os
import logging
import time
import datetime
import hashlib
import json
import gzip
import shutil
from typing import Dict, List, Optional, Union, Any, Tuple
from urllib.parse import urlparse
import pymongo
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError, BulkWriteError
import boto3
from botocore.exceptions import ClientError

from models import Page, URL
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class StorageManager:
    """
    Storage manager for web crawler data
    
    Handles:
    - MongoDB for metadata, URL information, and stats
    - Disk-based storage for HTML content
    - Optional Amazon S3 integration
    """
    
    def __init__(self, 
                 mongo_uri: Optional[str] = None,
                 use_s3: bool = False,
                 compress_html: bool = True,
                 max_disk_usage_gb: float = 100.0):
        """
        Initialize the storage manager
        
        Args:
            mongo_uri: MongoDB connection URI
            use_s3: Whether to use Amazon S3 for HTML storage
            compress_html: Whether to compress HTML content
            max_disk_usage_gb: Maximum disk space to use in GB
        """
        self.mongo_uri = mongo_uri or config.MONGODB_URI
        self.use_s3 = use_s3
        self.compress_html = compress_html
        self.max_disk_usage_gb = max_disk_usage_gb
        
        # Connect to MongoDB
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[config.MONGODB_DB]
        
        # MongoDB collections
        self.pages_collection = self.db['pages']
        self.urls_collection = self.db['urls']
        self.stats_collection = self.db['stats']
        
        # Create necessary indexes
        self._create_indexes()
        
        # S3 client (if enabled)
        self.s3_client = None
        if self.use_s3:
            self._init_s3_client()
        
        # Ensure storage directories exist
        self._ensure_directories()
        
        # Bulk operation buffers
        self.page_buffer = []
        self.url_buffer = []
        self.max_buffer_size = 100
        
        # Statistics
        self.stats = {
            'pages_stored': 0,
            'pages_retrieved': 0,
            'urls_stored': 0,
            'urls_retrieved': 0,
            'disk_space_used': 0,
            's3_objects_stored': 0,
            'mongodb_size': 0,
            'storage_errors': 0,
            'start_time': time.time()
        }
    
    def _create_indexes(self) -> None:
        """Create necessary indexes in MongoDB collections"""
        try:
            # Pages collection indexes
            self.pages_collection.create_index('url', unique=True)
            self.pages_collection.create_index('content_hash')
            self.pages_collection.create_index('crawled_at')
            self.pages_collection.create_index('domain')
            
            # URLs collection indexes
            self.urls_collection.create_index('url', unique=True)
            self.urls_collection.create_index('normalized_url')
            self.urls_collection.create_index('domain')
            self.urls_collection.create_index('status')
            self.urls_collection.create_index('priority')
            self.urls_collection.create_index('last_crawled')
            
            logger.info("MongoDB indexes created")
        except PyMongoError as e:
            logger.error(f"Error creating MongoDB indexes: {e}")
            self.stats['storage_errors'] += 1
    
    def _init_s3_client(self) -> None:
        """Initialize AWS S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=config.AWS_ACCESS_KEY,
                aws_secret_access_key=config.AWS_SECRET_KEY,
                region_name=config.AWS_REGION
            )
            logger.info("S3 client initialized")
            
            # Create bucket if it doesn't exist
            self._ensure_s3_bucket()
        except Exception as e:
            logger.error(f"Error initializing S3 client: {e}")
            self.use_s3 = False
            self.stats['storage_errors'] += 1
    
    def _ensure_s3_bucket(self) -> None:
        """Create S3 bucket if it doesn't exist"""
        if not self.s3_client:
            return
        
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=config.S3_BUCKET)
            logger.info(f"S3 bucket '{config.S3_BUCKET}' exists")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(
                        Bucket=config.S3_BUCKET,
                        CreateBucketConfiguration={
                            'LocationConstraint': config.AWS_REGION
                        }
                    )
                    logger.info(f"Created S3 bucket '{config.S3_BUCKET}'")
                except ClientError as ce:
                    logger.error(f"Error creating S3 bucket: {ce}")
                    self.use_s3 = False
                    self.stats['storage_errors'] += 1
            else:
                logger.error(f"Error checking S3 bucket: {e}")
                self.use_s3 = False
                self.stats['storage_errors'] += 1
    
    def _ensure_directories(self) -> None:
        """Ensure storage directories exist"""
        # Create main storage directory
        os.makedirs(config.STORAGE_PATH, exist_ok=True)
        
        # Create HTML storage directory
        os.makedirs(config.HTML_STORAGE_PATH, exist_ok=True)
        
        # Create log directory
        os.makedirs(config.LOG_PATH, exist_ok=True)
        
        logger.info("Storage directories created")
    
    def store_page(self, page: Page, flush: bool = False) -> bool:
        """
        Store a crawled page
        
        Args:
            page: Page object to store
            flush: Whether to flush page buffer immediately
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Store page content based on configuration
            if self.use_s3:
                content_stored = self._store_content_s3(page)
            else:
                content_stored = self._store_content_disk(page)
            
            if not content_stored:
                logger.warning(f"Failed to store content for {page.url}")
                self.stats['storage_errors'] += 1
                return False
            
            # Remove HTML content from page object for MongoDB storage
            page_dict = page.dict(exclude={'content'})
            
            # Convert datetime fields to proper format
            if page.crawled_at:
                page_dict['crawled_at'] = page.crawled_at
            
            # Add to buffer
            self.page_buffer.append(
                UpdateOne(
                    {'url': page.url},
                    {'$set': page_dict},
                    upsert=True
                )
            )
            
            # Update statistics
            self.stats['pages_stored'] += 1
            
            # Check if buffer should be flushed
            if flush or len(self.page_buffer) >= self.max_buffer_size:
                return self.flush_page_buffer()
            
            return True
        except Exception as e:
            logger.error(f"Error storing page {page.url}: {e}")
            self.stats['storage_errors'] += 1
            return False
    
    def _store_content_disk(self, page: Page) -> bool:
        """
        Store page content on disk
        
        Args:
            page: Page to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check disk space
            if not self._check_disk_space():
                logger.warning("Disk space limit exceeded")
                return False
            
            # Create directory for domain if it doesn't exist
            domain = self._extract_domain(page.url)
            domain_dir = os.path.join(config.HTML_STORAGE_PATH, domain)
            os.makedirs(domain_dir, exist_ok=True)
            
            # Create filename
            filename = self._url_to_filename(page.url)
            
            # Full path for the file
            if self.compress_html:
                filepath = os.path.join(domain_dir, f"{filename}.gz")
                
                # Compress and write HTML to file
                with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                    f.write(page.content)
            else:
                filepath = os.path.join(domain_dir, f"{filename}.html")
                
                # Write HTML to file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(page.content)
            
            # Update disk space used
            file_size = os.path.getsize(filepath)
            self.stats['disk_space_used'] += file_size
            
            logger.debug(f"Stored HTML content for {page.url} at {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error storing content on disk for {page.url}: {e}")
            self.stats['storage_errors'] += 1
            return False
    
    def _store_content_s3(self, page: Page) -> bool:
        """
        Store page content in S3
        
        Args:
            page: Page to store
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            logger.warning("S3 client not initialized, falling back to disk storage")
            return self._store_content_disk(page)
        
        try:
            # Create key for S3 object
            domain = self._extract_domain(page.url)
            filename = self._url_to_filename(page.url)
            
            # S3 key
            s3_key = f"{domain}/{filename}"
            if self.compress_html:
                s3_key += ".gz"
                
                # Compress content
                content_bytes = gzip.compress(page.content.encode('utf-8'))
                content_type = 'application/gzip'
            else:
                s3_key += ".html"
                content_bytes = page.content.encode('utf-8')
                content_type = 'text/html'
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=config.S3_BUCKET,
                Key=s3_key,
                Body=content_bytes,
                ContentType=content_type,
                Metadata={
                    'url': page.url,
                    'crawled_at': page.crawled_at.isoformat() if page.crawled_at else '',
                    'content_hash': page.content_hash or ''
                }
            )
            
            # Update statistics
            self.stats['s3_objects_stored'] += 1
            
            logger.debug(f"Stored HTML content for {page.url} in S3 at {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error storing content in S3 for {page.url}: {e}")
            self.stats['storage_errors'] += 1
            
            # Fall back to disk storage
            logger.info(f"Falling back to disk storage for {page.url}")
            return self._store_content_disk(page)
    
    def store_url(self, url_obj: URL, flush: bool = False) -> bool:
        """
        Store URL information
        
        Args:
            url_obj: URL object to store
            flush: Whether to flush URL buffer immediately
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert URL object to dict
            url_dict = url_obj.dict()
            
            # Add to buffer
            self.url_buffer.append(
                UpdateOne(
                    {'url': url_obj.url},
                    {'$set': url_dict},
                    upsert=True
                )
            )
            
            # Update statistics
            self.stats['urls_stored'] += 1
            
            # Check if buffer should be flushed
            if flush or len(self.url_buffer) >= self.max_buffer_size:
                return self.flush_url_buffer()
            
            return True
        except Exception as e:
            logger.error(f"Error storing URL {url_obj.url}: {e}")
            self.stats['storage_errors'] += 1
            return False
    
    def flush_page_buffer(self) -> bool:
        """
        Flush page buffer to MongoDB
        
        Returns:
            True if successful, False otherwise
        """
        if not self.page_buffer:
            return True
        
        try:
            # Execute bulk operation
            result = self.pages_collection.bulk_write(self.page_buffer, ordered=False)
            
            # Clear buffer
            buffer_size = len(self.page_buffer)
            self.page_buffer = []
            
            logger.debug(f"Flushed {buffer_size} pages to MongoDB")
            return True
        except BulkWriteError as e:
            logger.error(f"Error in bulk write for pages: {e.details}")
            self.stats['storage_errors'] += 1
            
            # Clear buffer
            self.page_buffer = []
            return False
        except Exception as e:
            logger.error(f"Error flushing page buffer: {e}")
            self.stats['storage_errors'] += 1
            
            # Clear buffer
            self.page_buffer = []
            return False
    
    def flush_url_buffer(self) -> bool:
        """
        Flush URL buffer to MongoDB
        
        Returns:
            True if successful, False otherwise
        """
        if not self.url_buffer:
            return True
        
        try:
            # Execute bulk operation
            result = self.urls_collection.bulk_write(self.url_buffer, ordered=False)
            
            # Clear buffer
            buffer_size = len(self.url_buffer)
            self.url_buffer = []
            
            logger.debug(f"Flushed {buffer_size} URLs to MongoDB")
            return True
        except BulkWriteError as e:
            logger.error(f"Error in bulk write for URLs: {e.details}")
            self.stats['storage_errors'] += 1
            
            # Clear buffer
            self.url_buffer = []
            return False
        except Exception as e:
            logger.error(f"Error flushing URL buffer: {e}")
            self.stats['storage_errors'] += 1
            
            # Clear buffer
            self.url_buffer = []
            return False
    
    def get_page(self, url: str) -> Optional[Page]:
        """
        Retrieve a page by URL
        
        Args:
            url: URL of the page to retrieve
            
        Returns:
            Page object if found, None otherwise
        """
        try:
            # Get page metadata from MongoDB
            page_doc = self.pages_collection.find_one({'url': url})
            
            if not page_doc:
                return None
            
            # Create Page object from document
            page = Page(**page_doc)
            
            # Load content based on configuration
            if self.use_s3:
                content = self._load_content_s3(url)
            else:
                content = self._load_content_disk(url)
            
            if content:
                page.content = content
            
            # Update statistics
            self.stats['pages_retrieved'] += 1
            
            return page
        except Exception as e:
            logger.error(f"Error retrieving page {url}: {e}")
            self.stats['storage_errors'] += 1
            return None
    
    def _load_content_disk(self, url: str) -> Optional[str]:
        """
        Load page content from disk
        
        Args:
            url: URL of the page
            
        Returns:
            Page content if found, None otherwise
        """
        try:
            # Get domain and filename
            domain = self._extract_domain(url)
            filename = self._url_to_filename(url)
            
            # Check for compressed file first
            compressed_path = os.path.join(config.HTML_STORAGE_PATH, domain, f"{filename}.gz")
            uncompressed_path = os.path.join(config.HTML_STORAGE_PATH, domain, f"{filename}.html")
            
            if os.path.exists(compressed_path):
                # Load compressed content
                with gzip.open(compressed_path, 'rt', encoding='utf-8') as f:
                    return f.read()
            elif os.path.exists(uncompressed_path):
                # Load uncompressed content
                with open(uncompressed_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                logger.warning(f"Content file not found for {url}")
                return None
        except Exception as e:
            logger.error(f"Error loading content from disk for {url}: {e}")
            self.stats['storage_errors'] += 1
            return None
    
    def _load_content_s3(self, url: str) -> Optional[str]:
        """
        Load page content from S3
        
        Args:
            url: URL of the page
            
        Returns:
            Page content if found, None otherwise
        """
        if not self.s3_client:
            logger.warning("S3 client not initialized, falling back to disk loading")
            return self._load_content_disk(url)
        
        try:
            # Get domain and filename
            domain = self._extract_domain(url)
            filename = self._url_to_filename(url)
            
            # Try both compressed and uncompressed keys
            s3_key_compressed = f"{domain}/{filename}.gz"
            s3_key_uncompressed = f"{domain}/{filename}.html"
            
            try:
                # Try compressed file first
                response = self.s3_client.get_object(
                    Bucket=config.S3_BUCKET,
                    Key=s3_key_compressed
                )
                
                # Decompress content
                content_bytes = response['Body'].read()
                return gzip.decompress(content_bytes).decode('utf-8')
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    # Try uncompressed file
                    try:
                        response = self.s3_client.get_object(
                            Bucket=config.S3_BUCKET,
                            Key=s3_key_uncompressed
                        )
                        content_bytes = response['Body'].read()
                        return content_bytes.decode('utf-8')
                    except ClientError as e2:
                        if e2.response['Error']['Code'] == 'NoSuchKey':
                            logger.warning(f"Content not found in S3 for {url}")
                            
                            # Try loading from disk as fallback
                            return self._load_content_disk(url)
                        else:
                            raise e2
                else:
                    raise e
        except Exception as e:
            logger.error(f"Error loading content from S3 for {url}: {e}")
            self.stats['storage_errors'] += 1
            
            # Try loading from disk as fallback
            return self._load_content_disk(url)
    
    def get_url(self, url: str) -> Optional[URL]:
        """
        Retrieve URL information by URL
        
        Args:
            url: URL to retrieve
            
        Returns:
            URL object if found, None otherwise
        """
        try:
            # Get URL information from MongoDB
            url_doc = self.urls_collection.find_one({'url': url})
            
            if not url_doc:
                return None
            
            # Create URL object from document
            url_obj = URL(**url_doc)
            
            # Update statistics
            self.stats['urls_retrieved'] += 1
            
            return url_obj
        except Exception as e:
            logger.error(f"Error retrieving URL {url}: {e}")
            self.stats['storage_errors'] += 1
            return None
    
    def get_urls_by_status(self, status: str, limit: int = 100) -> List[URL]:
        """
        Retrieve URLs by status
        
        Args:
            status: Status of URLs to retrieve
            limit: Maximum number of URLs to retrieve
            
        Returns:
            List of URL objects
        """
        try:
            # Get URLs from MongoDB
            url_docs = list(self.urls_collection.find({'status': status}).limit(limit))
            
            # Create URL objects from documents
            url_objs = [URL(**doc) for doc in url_docs]
            
            # Update statistics
            self.stats['urls_retrieved'] += len(url_objs)
            
            return url_objs
        except Exception as e:
            logger.error(f"Error retrieving URLs by status {status}: {e}")
            self.stats['storage_errors'] += 1
            return []
    
    def get_urls_by_domain(self, domain: str, limit: int = 100) -> List[URL]:
        """
        Retrieve URLs by domain
        
        Args:
            domain: Domain of URLs to retrieve
            limit: Maximum number of URLs to retrieve
            
        Returns:
            List of URL objects
        """
        try:
            # Get URLs from MongoDB
            url_docs = list(self.urls_collection.find({'domain': domain}).limit(limit))
            
            # Create URL objects from documents
            url_objs = [URL(**doc) for doc in url_docs]
            
            # Update statistics
            self.stats['urls_retrieved'] += len(url_objs)
            
            return url_objs
        except Exception as e:
            logger.error(f"Error retrieving URLs by domain {domain}: {e}")
            self.stats['storage_errors'] += 1
            return []
    
    def store_stats(self, stats: Dict[str, Any]) -> bool:
        """
        Store crawler statistics
        
        Args:
            stats: Statistics to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create statistics document
            stats_doc = stats.copy()
            stats_doc['timestamp'] = datetime.datetime.now()
            
            # Convert sets to lists for MongoDB
            for key, value in stats_doc.items():
                if isinstance(value, set):
                    stats_doc[key] = list(value)
            
            # Store in MongoDB
            self.stats_collection.insert_one(stats_doc)
            
            return True
        except Exception as e:
            logger.error(f"Error storing statistics: {e}")
            self.stats['storage_errors'] += 1
            return False
    
    def _check_disk_space(self) -> bool:
        """
        Check if disk space limit is exceeded
        
        Returns:
            True if space is available, False otherwise
        """
        # Convert max disk usage to bytes
        max_bytes = self.max_disk_usage_gb * 1024 * 1024 * 1024
        
        # Check if limit is exceeded
        return self.stats['disk_space_used'] < max_bytes
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        parsed = urlparse(url)
        return parsed.netloc.replace(':', '_')
    
    def _url_to_filename(self, url: str) -> str:
        """Convert URL to filename"""
        # Hash the URL to create a safe filename
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def clean_old_pages(self, days: int = 90) -> int:
        """
        Remove pages older than a specified number of days
        
        Args:
            days: Number of days after which pages are considered old
            
        Returns:
            Number of pages removed
        """
        try:
            # Calculate cutoff date
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
            
            # Find old pages
            old_pages = list(self.pages_collection.find({
                'crawled_at': {'$lt': cutoff_date}
            }, {'url': 1}))
            
            if not old_pages:
                logger.info(f"No pages older than {days} days found")
                return 0
            
            # Remove from database
            delete_result = self.pages_collection.delete_many({
                'crawled_at': {'$lt': cutoff_date}
            })
            
            # Remove content files
            count = 0
            for page in old_pages:
                url = page['url']
                domain = self._extract_domain(url)
                filename = self._url_to_filename(url)
                
                # Check disk
                compressed_path = os.path.join(config.HTML_STORAGE_PATH, domain, f"{filename}.gz")
                uncompressed_path = os.path.join(config.HTML_STORAGE_PATH, domain, f"{filename}.html")
                
                if os.path.exists(compressed_path):
                    os.remove(compressed_path)
                    count += 1
                
                if os.path.exists(uncompressed_path):
                    os.remove(uncompressed_path)
                    count += 1
                
                # Check S3
                if self.s3_client:
                    s3_key_compressed = f"{domain}/{filename}.gz"
                    s3_key_uncompressed = f"{domain}/{filename}.html"
                    
                    try:
                        self.s3_client.delete_object(
                            Bucket=config.S3_BUCKET,
                            Key=s3_key_compressed
                        )
                        count += 1
                    except:
                        pass
                    
                    try:
                        self.s3_client.delete_object(
                            Bucket=config.S3_BUCKET,
                            Key=s3_key_uncompressed
                        )
                        count += 1
                    except:
                        pass
            
            logger.info(f"Removed {delete_result.deleted_count} old pages and {count} content files")
            return delete_result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning old pages: {e}")
            self.stats['storage_errors'] += 1
            return 0
    
    def clean_failed_urls(self, retries: int = 3) -> int:
        """
        Remove URLs that have failed repeatedly
        
        Args:
            retries: Number of retries after which a URL is considered permanently failed
            
        Returns:
            Number of URLs removed
        """
        try:
            # Delete failed URLs with too many retries
            delete_result = self.urls_collection.delete_many({
                'status': 'FAILED',
                'retries': {'$gte': retries}
            })
            
            logger.info(f"Removed {delete_result.deleted_count} permanently failed URLs")
            return delete_result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning failed URLs: {e}")
            self.stats['storage_errors'] += 1
            return 0
    
    def calculate_storage_stats(self) -> Dict[str, Any]:
        """
        Calculate storage statistics
        
        Returns:
            Dictionary of storage statistics
        """
        stats = {
            'timestamp': datetime.datetime.now(),
            'pages_count': 0,
            'urls_count': 0,
            'disk_space_used_mb': 0,
            's3_objects_count': 0,
            'mongodb_size_mb': 0,
        }
        
        try:
            # Count pages and URLs
            stats['pages_count'] = self.pages_collection.count_documents({})
            stats['urls_count'] = self.urls_collection.count_documents({})
            
            # Calculate disk space used
            total_size = 0
            for root, _, files in os.walk(config.HTML_STORAGE_PATH):
                total_size += sum(os.path.getsize(os.path.join(root, name)) for name in files)
            stats['disk_space_used_mb'] = total_size / (1024 * 1024)
            
            # Calculate MongoDB size
            db_stats = self.db.command('dbStats')
            stats['mongodb_size_mb'] = db_stats['storageSize'] / (1024 * 1024)
            
            # Count S3 objects if enabled
            if self.s3_client:
                try:
                    s3_objects = 0
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=config.S3_BUCKET):
                        if 'Contents' in page:
                            s3_objects += len(page['Contents'])
                    stats['s3_objects_count'] = s3_objects
                except Exception as e:
                    logger.error(f"Error counting S3 objects: {e}")
            
            # Update internal statistics
            self.stats['disk_space_used'] = total_size
            self.stats['mongodb_size'] = db_stats['storageSize']
            
            return stats
        except Exception as e:
            logger.error(f"Error calculating storage statistics: {e}")
            self.stats['storage_errors'] += 1
            return stats
    
    def close(self) -> None:
        """Close connections and perform cleanup"""
        # Flush any pending buffers
        self.flush_page_buffer()
        self.flush_url_buffer()
        
        # Close MongoDB connection
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
        
        # Log final statistics
        logger.info(f"Storage manager closed. Pages stored: {self.stats['pages_stored']}, URLs stored: {self.stats['urls_stored']}") 