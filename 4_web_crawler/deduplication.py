"""
Content deduplication component for the web crawler.

Provides functionality to detect duplicate pages efficiently

1. Exact content hashing
2. Shingling and MinHash for near-duplicate detection
3. SimHash for fuzzy matching
"""

import hashlib
import logging
import time
from typing import Set, List, Dict, Tuple, Optional, Union
import random
import numpy as np
from collections import defaultdict
import re

import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class ContentDeduplicator:
    """
    Content deduplication using multiple techniques:
    - Exact match (MD5 hash)
    - Near-duplicate detection (MinHash)
    - Fuzzy matching (SimHash)
    """
    
    def __init__(self):
        """Initialize the deduplicator"""
        # Exact content hashing
        self.content_hashes = set()
        
        # MinHash parameters
        self.num_hashes = 100
        self.minhash_signatures = {}  # URL -> MinHash signature
        self.minhash_bands = defaultdict(set)  # band_id -> set of URLs
        self.band_size = 5  # Each band contains 5 signatures
        self.shingle_size = 3  # k-shingles of 3 consecutive tokens
        
        # SimHash parameters
        self.simhash_dim = 64
        self.simhash_values = {}  # URL -> SimHash value
        self.hamming_threshold = 3  # Maximum Hamming distance for similarity
        
        # Cache of previously computed duplicates for quick lookups
        self.duplicate_cache = {}  # URL -> set of duplicate URLs
        
        # Token preprocessing
        self.token_pattern = re.compile(r'\w+')
        self.stop_words = set(['the', 'and', 'a', 'to', 'of', 'in', 'is', 'that', 'for', 'on', 'with'])
        
        # Statistics
        self.stats = {
            'exact_duplicates': 0,
            'near_duplicates': 0,
            'fuzzy_duplicates': 0,
            'processing_time': 0,
            'total_documents': 0,
        }
    
    def is_duplicate(self, url: str, content: str) -> Tuple[bool, Optional[str]]:
        """
        Check if content is a duplicate
        
        Args:
            url: URL of the page
            content: Page content
            
        Returns:
            (is_duplicate, duplicate_url): Tuple indicating if content is duplicate and what it duplicates
        """
        start_time = time.time()
        
        # Check exact match first (fastest)
        content_hash = self._hash_content(content)
        if content_hash in self.content_hashes:
            self.stats['exact_duplicates'] += 1
            processing_time = time.time() - start_time
            self.stats['processing_time'] += processing_time
            
            # Find the URL with the same hash
            for existing_url, existing_hash in self._get_hash_map().items():
                if existing_hash == content_hash and existing_url != url:
                    logger.debug(f"Exact duplicate detected: {url} duplicates {existing_url}")
                    return True, existing_url
            
            return True, None
        
        # Check cache for quick lookup
        if url in self.duplicate_cache:
            duplicate_url = next(iter(self.duplicate_cache[url]))
            logger.debug(f"Duplicate found in cache: {url} duplicates {duplicate_url}")
            return True, duplicate_url
        
        # Only perform more expensive checks if configured to do so
        if config.NEAR_DUPLICATE_DETECTION:
            # Check for near-duplicates using MinHash
            near_duplicate = self._check_minhash(url, content)
            if near_duplicate:
                self.stats['near_duplicates'] += 1
                processing_time = time.time() - start_time
                self.stats['processing_time'] += processing_time
                
                logger.debug(f"Near-duplicate detected: {url} is similar to {near_duplicate}")
                self._add_to_duplicate_cache(url, near_duplicate)
                return True, near_duplicate
        
        if config.FUZZY_DUPLICATE_DETECTION:
            # Check for fuzzy matches using SimHash
            fuzzy_duplicate = self._check_simhash(url, content)
            if fuzzy_duplicate:
                self.stats['fuzzy_duplicates'] += 1
                processing_time = time.time() - start_time
                self.stats['processing_time'] += processing_time
                
                logger.debug(f"Fuzzy duplicate detected: {url} is similar to {fuzzy_duplicate}")
                self._add_to_duplicate_cache(url, fuzzy_duplicate)
                return True, fuzzy_duplicate
        
        # Not a duplicate, add to index
        self._add_to_index(url, content, content_hash)
        
        self.stats['total_documents'] += 1
        processing_time = time.time() - start_time
        self.stats['processing_time'] += processing_time
        
        return False, None
    
    def _add_to_duplicate_cache(self, url: str, duplicate_url: str) -> None:
        """Add URL to duplicate cache for faster lookups"""
        if url not in self.duplicate_cache:
            self.duplicate_cache[url] = set()
        self.duplicate_cache[url].add(duplicate_url)
        
        # Also add reverse relationship
        if duplicate_url not in self.duplicate_cache:
            self.duplicate_cache[duplicate_url] = set()
        self.duplicate_cache[duplicate_url].add(url)
    
    def _get_hash_map(self) -> Dict[str, str]:
        """Get mapping of URLs to their content hashes"""
        return {url: signature for url, signature in self.simhash_values.items()}
    
    def _hash_content(self, content: str) -> str:
        """Create MD5 hash of content"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _preprocess_content(self, content: str) -> List[str]:
        """
        Preprocess content for tokenization:
        1. Convert to lowercase
        2. Remove HTML tags
        3. Extract tokens
        4. Remove stop words
        """
        # Remove HTML tags
        content = re.sub(r'<[^>]+>', ' ', content)
        
        # Tokenize
        tokens = self.token_pattern.findall(content.lower())
        
        # Remove stop words
        tokens = [token for token in tokens if token not in self.stop_words]
        
        return tokens
    
    def _add_to_index(self, url: str, content: str, content_hash: Optional[str] = None) -> None:
        """
        Add content to the deduplication index
        
        Args:
            url: URL of the page
            content: Page content
            content_hash: Optional pre-computed hash
        """
        # Add exact hash
        if content_hash is None:
            content_hash = self._hash_content(content)
        self.content_hashes.add(content_hash)
        
        # Add MinHash signature
        if config.NEAR_DUPLICATE_DETECTION:
            signature = self._compute_minhash(content)
            self.minhash_signatures[url] = signature
            
            # Add to LSH bands
            for i in range(0, self.num_hashes, self.band_size):
                band = tuple(signature[i:i+self.band_size])
                band_id = hash(band)
                self.minhash_bands[band_id].add(url)
        
        # Add SimHash
        if config.FUZZY_DUPLICATE_DETECTION:
            simhash_value = self._compute_simhash(content)
            self.simhash_values[url] = simhash_value
    
    def _create_shingles(self, tokens: List[str], k: int = 3) -> Set[str]:
        """
        Create k-shingles from tokens
        
        Args:
            tokens: List of tokens
            k: Size of shingles
            
        Returns:
            Set of shingles
        """
        return set(' '.join(tokens[i:i+k]) for i in range(len(tokens) - k + 1))
    
    def _compute_minhash(self, content: str) -> List[int]:
        """
        Compute MinHash signature for content
        
        Args:
            content: Page content
            
        Returns:
            MinHash signature (list of hash values)
        """
        tokens = self._preprocess_content(content)
        shingles = self._create_shingles(tokens, self.shingle_size)
        
        # Generate random hash functions
        max_hash = 2**32 - 1
        
        # Create signature
        signature = [max_hash] * self.num_hashes
        
        # For each shingle, compute hashes and keep minimum values
        for shingle in shingles:
            # Use shingle as seed for random hash functions
            shingle_hash = hash(shingle)
            
            for i in range(self.num_hashes):
                # Simple linear hash function: (a*x + b) mod c
                a = i + 1  # Different 'a' for each hash function
                b = i * i  # Different 'b' for each hash function
                hash_value = (a * shingle_hash + b) % max_hash
                
                # Keep the minimum hash value
                signature[i] = min(signature[i], hash_value)
        
        return signature
    
    def _check_minhash(self, url: str, content: str) -> Optional[str]:
        """
        Check for near-duplicates using MinHash and LSH
        
        Args:
            url: URL of the page
            content: Page content
            
        Returns:
            URL of duplicate page if found, None otherwise
        """
        # Compute MinHash signature
        signature = self._compute_minhash(content)
        
        # Check each band for potential matches
        candidate_urls = set()
        for i in range(0, self.num_hashes, self.band_size):
            band = tuple(signature[i:i+self.band_size])
            band_id = hash(band)
            
            # Get URLs that share this band
            if band_id in self.minhash_bands:
                candidate_urls.update(self.minhash_bands[band_id])
        
        # Check Jaccard similarity with candidates
        for candidate_url in candidate_urls:
            if candidate_url == url:
                continue
                
            candidate_signature = self.minhash_signatures[candidate_url]
            similarity = self._jaccard_similarity(signature, candidate_signature)
            
            if similarity >= config.SIMILARITY_THRESHOLD:
                return candidate_url
        
        return None
    
    def _jaccard_similarity(self, sig1: List[int], sig2: List[int]) -> float:
        """
        Estimate Jaccard similarity from MinHash signatures
        
        Args:
            sig1: First signature
            sig2: Second signature
            
        Returns:
            Estimated Jaccard similarity (0-1)
        """
        if len(sig1) != len(sig2):
            raise ValueError("Signatures must have the same length")
            
        # Count matching hash values
        matches = sum(1 for i in range(len(sig1)) if sig1[i] == sig2[i])
        
        # Estimate similarity
        return matches / len(sig1)
    
    def _compute_simhash(self, content: str) -> int:
        """
        Compute SimHash for content
        
        Args:
            content: Page content
            
        Returns:
            SimHash value
        """
        tokens = self._preprocess_content(content)
        
        # Initialize vector
        v = [0] * self.simhash_dim
        
        # For each token, compute hash and update vector
        for token in tokens:
            # Compute hash of token
            token_hash = hashlib.md5(token.encode('utf-8')).digest()
            
            # Convert to binary representation
            token_bits = ''.join(format(byte, '08b') for byte in token_hash)
            
            # Use first self.simhash_dim bits
            token_bits = token_bits[:self.simhash_dim]
            
            # Update vector
            for i, bit in enumerate(token_bits):
                if bit == '1':
                    v[i] += 1
                else:
                    v[i] -= 1
        
        # Create fingerprint
        fingerprint = 0
        for i, val in enumerate(v):
            if val > 0:
                fingerprint |= (1 << i)
        
        return fingerprint
    
    def _check_simhash(self, url: str, content: str) -> Optional[str]:
        """
        Check for fuzzy duplicates using SimHash
        
        Args:
            url: URL of the page
            content: Page content
            
        Returns:
            URL of duplicate page if found, None otherwise
        """
        # Compute SimHash
        simhash_value = self._compute_simhash(content)
        
        # Compare with existing SimHash values
        for existing_url, existing_simhash in self.simhash_values.items():
            if existing_url == url:
                continue
                
            # Calculate Hamming distance
            hamming_distance = bin(simhash_value ^ existing_simhash).count('1')
            
            if hamming_distance <= self.hamming_threshold:
                return existing_url
        
        return None
    
    def clear(self) -> None:
        """Clear all indexes and caches"""
        self.content_hashes.clear()
        self.minhash_signatures.clear()
        self.minhash_bands.clear()
        self.simhash_values.clear()
        self.duplicate_cache.clear()
        
        # Reset statistics
        self.stats = {
            'exact_duplicates': 0,
            'near_duplicates': 0,
            'fuzzy_duplicates': 0,
            'processing_time': 0,
            'total_documents': 0,
        }
    
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """Get deduplication statistics"""
        stats_copy = self.stats.copy()
        
        # Calculate average processing time
        total_docs = self.stats['total_documents']
        if total_docs > 0:
            avg_time = self.stats['processing_time'] / total_docs
            stats_copy['avg_processing_time'] = avg_time
        else:
            stats_copy['avg_processing_time'] = 0
        
        # Calculate total duplicates
        total_duplicates = (self.stats['exact_duplicates'] + 
                            self.stats['near_duplicates'] + 
                            self.stats['fuzzy_duplicates'])
        stats_copy['total_duplicates'] = total_duplicates
        
        # Calculate duplicate percentage
        if total_docs > 0:
            duplicate_percentage = (total_duplicates / total_docs) * 100
            stats_copy['duplicate_percentage'] = duplicate_percentage
        else:
            stats_copy['duplicate_percentage'] = 0
        
        return stats_copy 