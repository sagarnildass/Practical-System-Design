"""
Robots.txt handler for web crawler
"""

import time
import logging
import requests
from urllib.parse import urlparse, urljoin
from typing import Dict, Optional, Tuple
import tldextract
from datetime import datetime, timedelta
from cachetools import TTLCache
import robotexclusionrulesparser

from models import RobotsInfo
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


class RobotsHandler:
    """Handles robots.txt fetching and parsing"""
    
    def __init__(self, user_agent: Optional[str] = None, cache_size: int = 1000, cache_ttl: int = 3600):
        """
        Initialize robots handler
        
        Args:
            user_agent: User agent to use when fetching robots.txt
            cache_size: Maximum number of robots.txt rules to cache
            cache_ttl: Time to live for cache entries in seconds
        """
        self.user_agent = user_agent or config.USER_AGENT
        self.parser = robotexclusionrulesparser.RobotExclusionRulesParser()
        
        # Cache of robots.txt rules for domains
        self.robots_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        
        # Create request session
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
    
    def can_fetch(self, url: str) -> Tuple[bool, Optional[float]]:
        """
        Check if URL can be fetched according to robots.txt
        
        Args:
            url: URL to check
            
        Returns:
            Tuple of (can_fetch, crawl_delay), where crawl_delay is in seconds
        """
        try:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            domain = self._get_domain(url)
            
            # Check if robots info is in cache
            robots_info = self._get_robots_info(base_url, domain)
            
            # Check if allowed
            path = parsed.path or "/"
            allowed = robots_info.allowed
            if allowed:
                allowed = self.parser.is_allowed(self.user_agent, path)
            
            # Get crawl delay
            crawl_delay = robots_info.crawl_delay
            if not crawl_delay and hasattr(self.parser, 'get_crawl_delay'):
                try:
                    crawl_delay = float(self.parser.get_crawl_delay(self.user_agent) or 0)
                except:
                    crawl_delay = 0
            
            return allowed, crawl_delay
            
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            # In case of error, assume allowed
            return True, None
    
    def _get_robots_info(self, base_url: str, domain: str) -> RobotsInfo:
        """
        Get robots.txt info for a domain
        
        Args:
            base_url: Base URL of the domain
            domain: Domain name
            
        Returns:
            RobotsInfo object
        """
        # Check if in cache
        if domain in self.robots_cache:
            return self.robots_cache[domain]
        
        # Fetch robots.txt
        robots_url = urljoin(base_url, "/robots.txt")
        try:
            response = self.session.get(
                robots_url, 
                timeout=config.CRAWL_TIMEOUT,
                allow_redirects=True
            )
            
            status_code = response.status_code
            
            # If robots.txt exists
            if status_code == 200:
                # Parse robots.txt
                self.parser.parse(response.text)
                
                # Create simpler user agents info that doesn't depend on get_user_agents
                user_agents = {}
                # Just store info for our specific user agent
                crawl_delay = None
                if hasattr(self.parser, 'get_crawl_delay'):
                    try:
                        crawl_delay = self.parser.get_crawl_delay(self.user_agent)
                    except:
                        crawl_delay = None
                        
                user_agents[self.user_agent] = {
                    'crawl_delay': crawl_delay
                }
                
                # Create robots info
                robots_info = RobotsInfo(
                    domain=domain,
                    allowed=True,
                    crawl_delay=crawl_delay,
                    last_fetched=datetime.now(),
                    user_agents=user_agents,
                    status_code=status_code
                )
            else:
                # If no robots.txt or error, assume allowed
                self.parser.parse("")  # Parse empty robots.txt
                robots_info = RobotsInfo(
                    domain=domain,
                    allowed=True,
                    crawl_delay=None,
                    last_fetched=datetime.now(),
                    user_agents={},
                    status_code=status_code
                )
                
            # Cache robots info
            self.robots_cache[domain] = robots_info
            return robots_info
            
        except requests.RequestException as e:
            logger.warning(f"Error fetching robots.txt from {robots_url}: {e}")
            
            # In case of error, assume allowed
            self.parser.parse("")  # Parse empty robots.txt
            robots_info = RobotsInfo(
                domain=domain,
                allowed=True,
                crawl_delay=None,
                last_fetched=datetime.now(),
                user_agents={},
                status_code=None
            )
            
            # Cache robots info
            self.robots_cache[domain] = robots_info
            return robots_info
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        parsed = tldextract.extract(url)
        return f"{parsed.domain}.{parsed.suffix}" if parsed.suffix else parsed.domain
    
    def clear_cache(self) -> None:
        """Clear the robots.txt cache"""
        self.robots_cache.clear()

    def update_cache(self, domain: str) -> None:
        """
        Force update of a domain's robots.txt in the cache
        
        Args:
            domain: Domain to update
        """
        if domain in self.robots_cache:
            del self.robots_cache[domain] 