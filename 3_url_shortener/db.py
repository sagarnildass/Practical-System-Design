"""
Database layer for the URL shortener service.
Handles connections and operations with SQLite.
"""

import sqlite3
import logging
import time
import os
from datetime import datetime
from typing import Tuple, Optional, Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLite database file path
DB_PATH = os.path.join(os.path.dirname(__file__), 'url_shortener.db')

def get_connection():
    """Get a SQLite connection with row factory enabled"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class URLRepository:
    """Repository class for URL database operations"""
    
    @staticmethod
    def initialize_db():
        """Initialize the database schema if it doesn't exist"""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Create the URLs table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY,
                short_url TEXT NOT NULL,
                long_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(short_url)
            )
            """)
            
            # Create index on long_url
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_long_url ON urls(long_url)
            """)
            
            # Create the analytics table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url_id INTEGER NOT NULL,
                clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                referrer TEXT,
                FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
            )
            """)
            
            conn.commit()
            logger.info("Database schema initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()
    
    @staticmethod
    def save_url(url_id: int, short_url: str, long_url: str) -> bool:
        """
        Save a new URL mapping to the database
        
        Args:
            url_id (int): The unique ID for the URL
            short_url (str): The shortened URL path
            long_url (str): The original long URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO urls (id, short_url, long_url)
            VALUES (?, ?, ?)
            """, (url_id, short_url, long_url))
            
            conn.commit()
            logger.info(f"Saved URL mapping: {short_url} -> {long_url}")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error saving URL: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_url_by_short_url(short_url: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Retrieve the original URL by its short URL
        
        Args:
            short_url (str): The shortened URL path
            
        Returns:
            tuple: (url_id, long_url) or (None, None) if not found
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT id, long_url FROM urls
            WHERE short_url = ?
            """, (short_url,))
            
            result = cursor.fetchone()
            if result:
                return result['id'], result['long_url']
            return None, None
            
        except sqlite3.Error as e:
            logger.error(f"Error retrieving URL: {e}")
            return None, None
        finally:
            conn.close()
    
    @staticmethod
    def get_url_by_long_url(long_url: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Check if a long URL already exists in the database
        
        Args:
            long_url (str): The original long URL
            
        Returns:
            tuple: (url_id, short_url) or (None, None) if not found
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT id, short_url FROM urls
            WHERE long_url = ?
            """, (long_url,))
            
            result = cursor.fetchone()
            if result:
                return result['id'], result['short_url']
            return None, None
            
        except sqlite3.Error as e:
            logger.error(f"Error checking URL existence: {e}")
            return None, None
        finally:
            conn.close()
    
    @staticmethod
    def record_click(url_id: int, ip_address=None, user_agent=None, referrer=None) -> bool:
        """
        Record a click on a shortened URL for analytics
        
        Args:
            url_id (int): The ID of the URL that was clicked
            ip_address (str, optional): The IP address of the client
            user_agent (str, optional): The user agent of the client
            referrer (str, optional): The referrer URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO clicks (url_id, ip_address, user_agent, referrer)
            VALUES (?, ?, ?, ?)
            """, (url_id, ip_address, user_agent, referrer))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error recording click: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    @staticmethod
    def get_click_count(url_id: int) -> int:
        """
        Get the number of clicks for a specific URL
        
        Args:
            url_id (int): The ID of the URL
            
        Returns:
            int: The number of clicks, or 0 if an error occurred
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT COUNT(*) as count FROM clicks
            WHERE url_id = ?
            """, (url_id,))
            
            result = cursor.fetchone()
            return result['count'] if result else 0
            
        except sqlite3.Error as e:
            logger.error(f"Error getting click count: {e}")
            return 0
        finally:
            conn.close()
            
    @staticmethod
    def get_url_created_time(url_id: int) -> Optional[str]:
        """
        Get the creation time of a URL
        
        Args:
            url_id (int): The ID of the URL
            
        Returns:
            str: ISO formatted creation timestamp, or None if an error occurred
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT created_at FROM urls
            WHERE id = ?
            """, (url_id,))
            
            result = cursor.fetchone()
            return result['created_at'] if result else None
            
        except sqlite3.Error as e:
            logger.error(f"Error getting URL creation time: {e}")
            return None
        finally:
            conn.close()
            
    @staticmethod
    def get_last_click_time(url_id: int) -> Optional[str]:
        """
        Get the time of the most recent click
        
        Args:
            url_id (int): The ID of the URL
            
        Returns:
            str: ISO formatted timestamp of the last click, or None if no clicks or error
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT clicked_at FROM clicks
            WHERE url_id = ?
            ORDER BY clicked_at DESC
            LIMIT 1
            """, (url_id,))
            
            result = cursor.fetchone()
            return result['clicked_at'] if result else None
            
        except sqlite3.Error as e:
            logger.error(f"Error getting last click time: {e}")
            return None
        finally:
            conn.close()
            
    @staticmethod
    def get_top_referrers(url_id: int, limit: int = 5) -> Dict[str, int]:
        """
        Get the top referrers for a URL
        
        Args:
            url_id (int): The ID of the URL
            limit (int): Maximum number of referrers to return
            
        Returns:
            dict: Dictionary mapping referrer to count
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT referrer, COUNT(*) as count FROM clicks
            WHERE url_id = ? AND referrer IS NOT NULL AND referrer != ''
            GROUP BY referrer
            ORDER BY count DESC
            LIMIT ?
            """, (url_id, limit))
            
            results = cursor.fetchall()
            referrers = {row['referrer'] or 'Direct': row['count'] for row in results}
            
            return referrers
            
        except sqlite3.Error as e:
            logger.error(f"Error getting top referrers: {e}")
            return {}
        finally:
            conn.close()
            
    @staticmethod
    def get_top_browsers(url_id: int, limit: int = 5) -> Dict[str, int]:
        """
        Get the top browsers used to access a URL
        
        Args:
            url_id (int): The ID of the URL
            limit (int): Maximum number of browsers to return
            
        Returns:
            dict: Dictionary mapping browser to count
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT user_agent, COUNT(*) as count FROM clicks
            WHERE url_id = ? AND user_agent IS NOT NULL AND user_agent != ''
            GROUP BY user_agent
            ORDER BY count DESC
            LIMIT ?
            """, (url_id, limit))
            
            results = cursor.fetchall()
            
            # Simple browser extraction, in a real app would use a proper user-agent parser
            browsers = {}
            for row in results:
                user_agent = row['user_agent']
                browser = "Unknown"
                
                if 'Chrome' in user_agent and 'Edg' in user_agent:
                    browser = 'Edge'
                elif 'Chrome' in user_agent and 'Safari' in user_agent:
                    browser = 'Chrome'
                elif 'Firefox' in user_agent:
                    browser = 'Firefox'
                elif 'Safari' in user_agent:
                    browser = 'Safari'
                    
                browsers[browser] = browsers.get(browser, 0) + row['count']
                
            return browsers
            
        except sqlite3.Error as e:
            logger.error(f"Error getting top browsers: {e}")
            return {}
        finally:
            conn.close()
            
    @staticmethod
    def get_recent_clicks(url_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most recent clicks for a URL
        
        Args:
            url_id (int): The ID of the URL
            limit (int): Maximum number of clicks to return
            
        Returns:
            list: List of dictionaries with click information
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT clicked_at, ip_address, user_agent, referrer FROM clicks
            WHERE url_id = ?
            ORDER BY clicked_at DESC
            LIMIT ?
            """, (url_id, limit))
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            recent_clicks = []
            for row in results:
                recent_clicks.append({
                    'time': row['clicked_at'],
                    'ip': row['ip_address'],
                    'referrer': row['referrer'] or 'Direct',
                    'user_agent': row['user_agent']
                })
                
            return recent_clicks
            
        except sqlite3.Error as e:
            logger.error(f"Error getting recent clicks: {e}")
            return []
        finally:
            conn.close() 