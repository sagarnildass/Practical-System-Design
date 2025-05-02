"""
ID Generator for the URL shortener service.
Generates unique IDs for each new URL.
"""

import time
import random
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IDGenerator:
    """
    A simple distributed ID generator inspired by Twitter's Snowflake.
    
    The ID structure is as follows:
    - 41 bits: Timestamp in milliseconds
    - 10 bits: Machine ID
    - 12 bits: Sequence number
    
    This allows for:
    - 2^41 timestamps (69 years with custom epoch)
    - 2^10 machines (1,024 machines)
    - 2^12 sequence numbers (4,096 per millisecond per machine)
    """
    
    def __init__(self, machine_id=1, epoch=1609459200000):  # epoch: 2021-01-01 00:00:00 UTC
        """
        Initialize the ID generator
        
        Args:
            machine_id (int): A unique identifier for this machine (0-1023)
            epoch (int): Custom epoch in milliseconds
        """
        if machine_id < 0 or machine_id >= 1024:
            raise ValueError("Machine ID must be between 0 and 1023")
            
        self.machine_id = machine_id
        self.epoch = epoch
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()
        
        logger.info(f"Initialized ID generator with machine ID: {machine_id}")
    
    def _current_timestamp(self):
        """Get current timestamp in milliseconds since epoch"""
        return int(time.time() * 1000) - self.epoch
    
    def _wait_next_millisecond(self, last_timestamp):
        """Wait until the next millisecond"""
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp
    
    def generate_id(self):
        """
        Generate a unique ID
        
        Returns:
            int: A unique 63-bit ID
        """
        with self.lock:
            timestamp = self._current_timestamp()
            
            # Clock moved backwards, wait until we're back on track
            if timestamp < self.last_timestamp:
                logger.warning(f"Clock moved backwards. Waiting until {self.last_timestamp}")
                timestamp = self._wait_next_millisecond(self.last_timestamp)
            
            # Same millisecond as last time
            if timestamp == self.last_timestamp:
                # Increment sequence number
                self.sequence = (self.sequence + 1) & 4095  # 4095 is 2^12 - 1
                
                # Sequence overflow in the same millisecond
                if self.sequence == 0:
                    # Wait for the next millisecond
                    timestamp = self._wait_next_millisecond(self.last_timestamp)
            else:
                # Different millisecond, reset sequence
                self.sequence = 0
            
            self.last_timestamp = timestamp
            
            # Compose the ID from timestamp, machine ID, and sequence
            id = ((timestamp << 22) |
                  (self.machine_id << 12) |
                  self.sequence)
            
            logger.debug(f"Generated ID: {id}")
            return id

# Create a default global instance
default_generator = IDGenerator(machine_id=random.randint(0, 1023))

def generate_id():
    """
    Generate a unique ID using the default generator
    
    Returns:
        int: A unique 63-bit ID
    """
    return default_generator.generate_id() 