"""
HTML Downloader component for web crawler
"""

import time
import logging
import requests
from requests.exceptions import RequestException
from typing import Dict, Optional, Tuple, List, Any
from urllib.parse import urlparse
import aiohttp
import asyncio
from aiohttp.client_exceptions import ClientError
import hashlib

from models import URL, Page, calculate_content_hash
from dns_resolver import DNSResolver
from robots import RobotsHandler
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class HTMLDownloader:
    """
    HTML Downloader responsible for downloading web pages
    
    Features:
    - Respects robots.txt rules
    - Uses DNS caching for performance
    - Handles errors and retries
    - Supports both synchronous and asynchronous downloads
    """
    
    def __init__(self, 
                 dns_resolver: Optional[DNSResolver] = None,
                 robots_handler: Optional[RobotsHandler] = None,
                 user_agent: Optional[str] = None):
        """
        Initialize HTML Downloader
        
        Args:
            dns_resolver: DNS resolver for hostname resolution
            robots_handler: Handler for robots.txt
            user_agent: User agent to use for requests
        """
        self.dns_resolver = dns_resolver or DNSResolver()
        self.robots_handler = robots_handler or RobotsHandler()
        self.user_agent = user_agent or config.USER_AGENT
        
        # Create request session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
    
    def download(self, url_obj: URL) -> Optional[Page]:
        """
        Download an HTML page from a URL
        
        Args:
            url_obj: URL object to download
            
        Returns:
            Page object or None if download fails
        """
        url = url_obj.url
        try:
            # Check robots.txt first
            if config.ROBOTSTXT_OBEY:
                allowed, crawl_delay = self.robots_handler.can_fetch(url)
                if not allowed:
                    logger.info(f"URL not allowed by robots.txt: {url}")
                    url_obj.status = "robotstxt_excluded"
                    return None
                
                # Respect crawl delay if specified
                if crawl_delay and crawl_delay > 0:
                    time.sleep(crawl_delay)
            
            # Resolve DNS
            ip_address = self.dns_resolver.resolve(url)
            if not ip_address:
                logger.warning(f"Failed to resolve DNS for URL: {url}")
                url_obj.error = "DNS resolution failed"
                return None
            
            # Download page
            start_time = time.time()
            response = self.session.get(
                url,
                timeout=config.CRAWL_TIMEOUT,
                allow_redirects=True,
                stream=True  # Stream to avoid downloading large files fully
            )
            
            # Check content type
            content_type = response.headers.get('Content-Type', '').lower()
            is_html = any(allowed_type in content_type for allowed_type in config.ALLOWED_CONTENT_TYPES)
            
            if not is_html:
                logger.info(f"Skipping non-HTML content ({content_type}): {url}")
                url_obj.error = f"Non-HTML content type: {content_type}"
                return None
            
            # Check content length
            content_length = int(response.headers.get('Content-Length', 0))
            if content_length > config.MAX_CONTENT_SIZE:
                logger.info(f"Skipping large content ({content_length} bytes): {url}")
                url_obj.error = f"Content too large: {content_length} bytes"
                return None
            
            # Read content (with size limit)
            content = b""
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                content += chunk
                if len(content) > config.MAX_CONTENT_SIZE:
                    logger.info(f"Content exceeded max size during download: {url}")
                    url_obj.error = f"Content exceeded max size: {len(content)} bytes"
                    return None
            
            # Decode content
            try:
                html_content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    # Try with a more forgiving encoding
                    html_content = content.decode('iso-8859-1')
                except UnicodeDecodeError:
                    logger.warning(f"Failed to decode content for URL: {url}")
                    url_obj.error = "Failed to decode content"
                    return None
            
            # Calculate hash for duplicate detection
            content_hash = calculate_content_hash(html_content)
            
            elapsed_time = time.time() - start_time
            
            # Create page object
            page = Page(
                url=url,
                status_code=response.status_code,
                content=html_content,
                content_type=content_type,
                content_length=len(content),
                content_hash=content_hash,
                headers={k.lower(): v for k, v in response.headers.items()},
                crawled_at=time.time(),
                redirect_url=response.url if response.url != url else None,
                elapsed_time=elapsed_time
            )
            
            logger.info(f"Downloaded {len(content)} bytes from {url} in {elapsed_time:.2f}s")
            return page
            
        except RequestException as e:
            logger.warning(f"Request error for URL {url}: {e}")
            url_obj.error = f"Request error: {str(e)}"
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error downloading URL {url}: {e}")
            url_obj.error = f"Unexpected error: {str(e)}"
            return None
    
    async def download_async(self, url_obj: URL, session: Optional[aiohttp.ClientSession] = None) -> Optional[Page]:
        """
        Download an HTML page asynchronously
        
        Args:
            url_obj: URL object to download
            session: Optional aiohttp session to use
            
        Returns:
            Page object or None if download fails
        """
        url = url_obj.url
        own_session = False
        
        try:
            # Check robots.txt first (blocking call)
            if config.ROBOTSTXT_OBEY:
                allowed, crawl_delay = self.robots_handler.can_fetch(url)
                if not allowed:
                    logger.info(f"URL not allowed by robots.txt: {url}")
                    url_obj.status = "robotstxt_excluded"
                    return None
                
                # Respect crawl delay if specified
                if crawl_delay and crawl_delay > 0:
                    await asyncio.sleep(crawl_delay)
            
            # Resolve DNS (blocking call, but cached)
            ip_address = self.dns_resolver.resolve(url)
            if not ip_address:
                logger.warning(f"Failed to resolve DNS for URL: {url}")
                url_obj.error = "DNS resolution failed"
                return None
            
            # Create session if not provided
            if session is None:
                own_session = True
                session = aiohttp.ClientSession(headers={
                    'User-Agent': self.user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0'
                })
            
            # Download page
            start_time = time.time()
            async with session.get(url, timeout=config.CRAWL_TIMEOUT, allow_redirects=True) as response:
                # Check content type
                content_type = response.headers.get('Content-Type', '').lower()
                is_html = any(allowed_type in content_type for allowed_type in config.ALLOWED_CONTENT_TYPES)
                
                if not is_html:
                    logger.info(f"Skipping non-HTML content ({content_type}): {url}")
                    url_obj.error = f"Non-HTML content type: {content_type}"
                    return None
                
                # Check content length
                content_length = int(response.headers.get('Content-Length', 0))
                if content_length > config.MAX_CONTENT_SIZE:
                    logger.info(f"Skipping large content ({content_length} bytes): {url}")
                    url_obj.error = f"Content too large: {content_length} bytes"
                    return None
                
                # Read content (with size limit)
                content = b""
                async for chunk in response.content.iter_chunked(1024*1024):  # 1MB chunks
                    content += chunk
                    if len(content) > config.MAX_CONTENT_SIZE:
                        logger.info(f"Content exceeded max size during download: {url}")
                        url_obj.error = f"Content exceeded max size: {len(content)} bytes"
                        return None
                
                # Decode content
                try:
                    html_content = content.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        # Try with a more forgiving encoding
                        html_content = content.decode('iso-8859-1')
                    except UnicodeDecodeError:
                        logger.warning(f"Failed to decode content for URL: {url}")
                        url_obj.error = "Failed to decode content"
                        return None
                
                # Calculate hash for duplicate detection
                content_hash = calculate_content_hash(html_content)
                
                elapsed_time = time.time() - start_time
                
                # Create page object
                page = Page(
                    url=url,
                    status_code=response.status,
                    content=html_content,
                    content_type=content_type,
                    content_length=len(content),
                    content_hash=content_hash,
                    headers={k.lower(): v for k, v in response.headers.items()},
                    crawled_at=time.time(),
                    redirect_url=str(response.url) if str(response.url) != url else None,
                    elapsed_time=elapsed_time
                )
                
                logger.info(f"Downloaded {len(content)} bytes from {url} in {elapsed_time:.2f}s")
                return page
                
        except (ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request error for URL {url}: {e}")
            url_obj.error = f"Request error: {str(e)}"
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error downloading URL {url}: {e}")
            url_obj.error = f"Unexpected error: {str(e)}"
            return None
            
        finally:
            # Close session if we created it
            if own_session and session:
                await session.close()
    
    async def bulk_download(self, urls: List[URL], concurrency: int = 10) -> Dict[str, Optional[Page]]:
        """
        Download multiple URLs concurrently
        
        Args:
            urls: List of URL objects to download
            concurrency: Maximum number of concurrent downloads
            
        Returns:
            Dictionary mapping URL strings to Page objects
        """
        results = {}
        
        # Create a session to be shared across requests
        async with aiohttp.ClientSession(headers={
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }) as session:
            # Create a semaphore to limit concurrency
            semaphore = asyncio.Semaphore(concurrency)
            
            async def download_with_semaphore(url_obj):
                async with semaphore:
                    return await self.download_async(url_obj, session)
            
            # Create download tasks
            tasks = [download_with_semaphore(url_obj) for url_obj in urls]
            
            # Wait for all tasks to complete
            pages = await asyncio.gather(*tasks)
            
            # Map results
            for url_obj, page in zip(urls, pages):
                results[url_obj.url] = page
                
        return results 