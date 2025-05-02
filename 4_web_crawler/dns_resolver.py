"""
DNS resolver with caching for web crawler
"""

import socket
import logging
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime, timedelta
from cachetools import TTLCache
import threading
import dns
import dns.resolver

import config

# Import local configuration if available
try:
    import local_config
    # Override config settings with local settings
    for key in dir(local_config):
        if key.isupper():
            setattr(config, key, getattr(local_config, key))
    logging.info("Loaded local configuration")
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class DNSResolver:
    """
    DNS resolver with caching to improve performance
    
    DNS resolution can be a bottleneck for crawlers due to the synchronous
    nature of many DNS interfaces. This class provides a cached resolver
    to reduce the number of DNS lookups.
    """
    
    def __init__(self, cache_size: int = 10000, cache_ttl: int = 3600):
        """
        Initialize DNS resolver
        
        Args:
            cache_size: Maximum number of DNS records to cache
            cache_ttl: Time to live for cache entries in seconds
        """
        self.cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        self.lock = threading.RLock()  # Thread-safe operations
        self.resolver = dns.resolver.Resolver()
        self.resolver.timeout = 3.0  # Timeout for DNS requests in seconds
        self.resolver.lifetime = 5.0  # Total timeout for all DNS requests
        
        # Stats tracking
        self.hit_count = 0
        self.miss_count = 0
    
    def resolve(self, url: str) -> Optional[str]:
        """
        Resolve a URL to an IP address
        
        Args:
            url: URL to resolve
            
        Returns:
            IP address or None if resolution fails
        """
        try:
            parsed = urlparse(url)
            hostname = parsed.netloc.split(':')[0]  # Remove port if present
            
            # Check cache first
            with self.lock:
                if hostname in self.cache:
                    logger.debug(f"DNS cache hit for {hostname}")
                    self.hit_count += 1
                    return self.cache[hostname]
            
            # Cache miss - resolve hostname
            ip_address = self._resolve_hostname(hostname)
            
            # Update cache
            if ip_address:
                with self.lock:
                    self.cache[hostname] = ip_address
                    self.miss_count += 1
                    
            return ip_address
            
        except Exception as e:
            logger.warning(f"Error resolving DNS for {url}: {e}")
            return None
    
    def _resolve_hostname(self, hostname: str) -> Optional[str]:
        """
        Resolve hostname to IP address
        
        Args:
            hostname: Hostname to resolve
            
        Returns:
            IP address or None if resolution fails
        """
        try:
            # First try using dnspython for more control
            answers = self.resolver.resolve(hostname, 'A')
            if answers:
                # Return first IP address
                return str(answers[0])
        except dns.exception.DNSException as e:
            logger.debug(f"dnspython DNS resolution failed for {hostname}: {e}")
            
            # Fall back to socket.gethostbyname
            try:
                return socket.gethostbyname(hostname)
            except socket.gaierror as e:
                logger.warning(f"Socket DNS resolution failed for {hostname}: {e}")
                return None
    
    def bulk_resolve(self, urls: list) -> Dict[str, Optional[str]]:
        """
        Resolve multiple URLs to IP addresses
        
        Args:
            urls: List of URLs to resolve
            
        Returns:
            Dictionary mapping URLs to IP addresses
        """
        results = {}
        for url in urls:
            results[url] = self.resolve(url)
        return results
    
    def clear_cache(self) -> None:
        """Clear the DNS cache"""
        with self.lock:
            self.cache.clear()
            
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the DNS cache
        
        Returns:
            Dictionary with cache statistics
        """
        with self.lock:
            return {
                'size': len(self.cache),
                'max_size': self.cache.maxsize,
                'ttl': self.cache.ttl,
                'hit_count': self.hit_count,
                'miss_count': self.miss_count,
                'hit_ratio': self.hit_count / (self.hit_count + self.miss_count) if (self.hit_count + self.miss_count) > 0 else 0
            } 