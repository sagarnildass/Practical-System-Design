"""
HTML Parser and URL Extractor component for web crawler
"""

import logging
import re
from typing import Dict, List, Set, Tuple, Optional, Any
from urllib.parse import urlparse, urljoin, unquote
from bs4 import BeautifulSoup
import tldextract
import hashlib

from models import URL, Page, Priority, normalize_url
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class HTMLParser:
    """
    Parses HTML content and extracts URLs and other information
    """
    
    def __init__(self):
        """Initialize HTML parser"""
        # Compile URL filter regex patterns for efficiency
        self.url_filters = [re.compile(pattern) for pattern in config.URL_FILTERS]
    
    def parse(self, page: Page, base_url: Optional[str] = None) -> Tuple[List[str], Dict[str, Any]]:
        """
        Parse HTML content and extract URLs and metadata
        
        Args:
            page: Page object containing HTML content
            base_url: Base URL for resolving relative links (defaults to page URL)
            
        Returns:
            Tuple of (extracted URLs, metadata)
        """
        if not page or not page.content:
            return [], {}
        
        # Use page URL as base URL if not provided
        if not base_url:
            base_url = page.url
        
        # Parse HTML with BeautifulSoup
        try:
            soup = BeautifulSoup(page.content, 'lxml')
        except Exception as e:
            logger.warning(f"Error parsing HTML with lxml: {e}, falling back to html.parser")
            try:
                soup = BeautifulSoup(page.content, 'html.parser')
            except Exception as e:
                logger.error(f"Error parsing HTML: {e}")
                return [], {}
        
        # Extract URLs
        extracted_urls = self._extract_urls(soup, base_url)
        
        # Extract metadata
        metadata = self._extract_metadata(soup)
        
        # Store links in page object
        page.links = extracted_urls
        
        return extracted_urls, metadata
    
    def _extract_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """
        Extract and normalize URLs from HTML content
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative links
            
        Returns:
            List of normalized URLs
        """
        urls = set()
        
        # Extract URLs from <a> tags
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            if href and not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                # Resolve relative URLs
                try:
                    absolute_url = urljoin(base_url, href)
                    # Normalize URL
                    normalized_url = normalize_url(absolute_url)
                    # Apply URL filters
                    if self._should_allow_url(normalized_url):
                        urls.add(normalized_url)
                except Exception as e:
                    logger.debug(f"Error processing URL {href}: {e}")
        
        # Extract URLs from other elements like <iframe>, <frame>, <img>, etc.
        for tag_name, attr in [('frame', 'src'), ('iframe', 'src'), ('img', 'src'),
                               ('link', 'href'), ('script', 'src'), ('area', 'href')]:
            for tag in soup.find_all(tag_name, attrs={attr: True}):
                url = tag[attr].strip()
                if url and not url.startswith(('#', 'javascript:', 'data:', 'mailto:', 'tel:')):
                    try:
                        absolute_url = urljoin(base_url, url)
                        normalized_url = normalize_url(absolute_url)
                        if self._should_allow_url(normalized_url):
                            urls.add(normalized_url)
                    except Exception as e:
                        logger.debug(f"Error processing URL {url}: {e}")
        
        # Return list of unique URLs
        return list(urls)
    
    def _should_allow_url(self, url: str) -> bool:
        """
        Check if URL should be allowed based on filters
        
        Args:
            url: URL to check
            
        Returns:
            True if URL should be allowed, False otherwise
        """
        try:
            parsed = urlparse(url)
            
            # Check scheme
            if parsed.scheme not in config.ALLOWED_SCHEMES:
                return False
            
            # Check domain restrictions
            domain = self._extract_domain(url)
            
            # Check allowed domains if set
            if config.ALLOWED_DOMAINS and domain not in config.ALLOWED_DOMAINS:
                return False
            
            # Check excluded domains
            if domain in config.EXCLUDED_DOMAINS:
                return False
            
            # Check URL filters
            for pattern in self.url_filters:
                if pattern.match(url):
                    return False
            
            return True
            
        except Exception as e:
            logger.debug(f"Error checking URL {url}: {e}")
            return False
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract metadata from HTML content
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary of metadata
        """
        metadata = {}
        
        # Extract title
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            metadata['title'] = title_tag.string.strip()
        
        # Extract meta description
        description_tag = soup.find('meta', attrs={'name': 'description'})
        if description_tag and description_tag.get('content'):
            metadata['description'] = description_tag['content'].strip()
        
        # Extract meta keywords
        keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
        if keywords_tag and keywords_tag.get('content'):
            metadata['keywords'] = [k.strip() for k in keywords_tag['content'].split(',')]
        
        # Extract canonical URL
        canonical_tag = soup.find('link', attrs={'rel': 'canonical'})
        if canonical_tag and canonical_tag.get('href'):
            metadata['canonical_url'] = canonical_tag['href'].strip()
        
        # Extract robots meta
        robots_tag = soup.find('meta', attrs={'name': 'robots'})
        if robots_tag and robots_tag.get('content'):
            metadata['robots'] = robots_tag['content'].strip()
        
        # Extract Open Graph metadata
        og_metadata = {}
        for meta_tag in soup.find_all('meta', attrs={'property': re.compile('^og:')}):
            if meta_tag.get('content'):
                property_name = meta_tag['property'][3:]  # Remove 'og:' prefix
                og_metadata[property_name] = meta_tag['content'].strip()
        
        if og_metadata:
            metadata['open_graph'] = og_metadata
        
        # Extract Twitter Card metadata
        twitter_metadata = {}
        for meta_tag in soup.find_all('meta', attrs={'name': re.compile('^twitter:')}):
            if meta_tag.get('content'):
                property_name = meta_tag['name'][8:]  # Remove 'twitter:' prefix
                twitter_metadata[property_name] = meta_tag['content'].strip()
        
        if twitter_metadata:
            metadata['twitter_card'] = twitter_metadata
        
        # Extract schema.org structured data (JSON-LD)
        schema_metadata = []
        for script in soup.find_all('script', attrs={'type': 'application/ld+json'}):
            if script.string:
                try:
                    import json
                    schema_data = json.loads(script.string)
                    schema_metadata.append(schema_data)
                except Exception as e:
                    logger.debug(f"Error parsing JSON-LD: {e}")
        
        if schema_metadata:
            metadata['structured_data'] = schema_metadata
        
        # Extract text content statistics
        text_content = soup.get_text(separator=' ', strip=True)
        if text_content:
            word_count = len(text_content.split())
            metadata['word_count'] = word_count
            metadata['text_length'] = len(text_content)
        
        return metadata
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        parsed = tldextract.extract(url)
        return f"{parsed.domain}.{parsed.suffix}" if parsed.suffix else parsed.domain
    
    def calculate_priority(self, url: str, metadata: Dict[str, Any]) -> Priority:
        """
        Calculate priority for a URL based on various factors
        
        Args:
            url: URL to calculate priority for
            metadata: Metadata extracted from the page
            
        Returns:
            Priority enum value
        """
        # Default priority
        priority = Priority.MEDIUM
        
        try:
            # Extract path depth
            parsed = urlparse(url)
            path = parsed.path
            depth = len([p for p in path.split('/') if p])
            
            # Prioritize URLs with shorter paths
            if depth <= 1:
                priority = Priority.HIGH
            elif depth <= 3:
                priority = Priority.MEDIUM
            else:
                priority = Priority.LOW
            
            # Prioritize URLs with certain keywords in path
            if re.search(r'(article|blog|news|post)', path, re.IGNORECASE):
                priority = Priority.HIGH
            
            # Deprioritize URLs with pagination patterns
            if re.search(r'(page|p|pg)=\d+', url, re.IGNORECASE):
                priority = Priority.LOW
            
            # Check metadata
            if metadata:
                # Prioritize based on title
                title = metadata.get('title', '')
                if title and len(title) > 10:
                    priority = min(priority, Priority.MEDIUM)  # Raise priority if it's lower
                
                # Prioritize based on description
                description = metadata.get('description', '')
                if description and len(description) > 50:
                    priority = min(priority, Priority.MEDIUM)  # Raise priority if it's lower
                
                # Prioritize based on word count
                word_count = metadata.get('word_count', 0)
                if word_count > 1000:
                    priority = min(priority, Priority.HIGH)  # High priority for content-rich pages
                elif word_count > 500:
                    priority = min(priority, Priority.MEDIUM)
            
            return priority
            
        except Exception as e:
            logger.debug(f"Error calculating priority for URL {url}: {e}")
            return Priority.MEDIUM 