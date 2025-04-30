import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeIDGenerator:
    """Twitter Snowflake ID Generator
    
    64-bit ID broken down into:
    - 1 bit: sign bit, always 0
    - 41 bits: timestamp (milliseconds since epoch)
    - 5 bits: datacenter ID
    - 5 bits: machine ID
    - 12 bits: sequence number
    """
    
    # Custom epoch (Apr 30, 2025)
    EPOCH = 1714531200000
    
    # Bit lengths for each section
    TIMESTAMP_BITS = 41
    DATACENTER_ID_BITS = 5
    MACHINE_ID_BITS = 5
    SEQUENCE_BITS = 12
    
    # Maximum values for each section
    MAX_DATACENTER_ID = -1 ^ (-1 << DATACENTER_ID_BITS)  # 31
    MAX_MACHINE_ID = -1 ^ (-1 << MACHINE_ID_BITS)        # 31
    MAX_SEQUENCE = -1 ^ (-1 << SEQUENCE_BITS)            # 4095
    
    # Bit shifts for each section
    MACHINE_ID_SHIFT = SEQUENCE_BITS
    DATACENTER_ID_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS
    TIMESTAMP_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS + DATACENTER_ID_BITS
    
    def __init__(self, datacenter_id, machine_id):
        """Initialize the ID generator with datacenter and machine IDs
        
        Args:
            datacenter_id (int): ID of the datacenter (0-31)
            machine_id (int): ID of the machine (0-31)
        """
        # Validate inputs
        if datacenter_id < 0 or datacenter_id > self.MAX_DATACENTER_ID:
            raise ValueError(f"Datacenter ID must be between 0 and {self.MAX_DATACENTER_ID}")
        
        if machine_id < 0 or machine_id > self.MAX_MACHINE_ID:
            raise ValueError(f"Machine ID must be between 0 and {self.MAX_MACHINE_ID}")
        
        self.datacenter_id = datacenter_id
        self.machine_id = machine_id
        self.sequence = 0
        self.last_timestamp = -1
    
    def _wait_next_millis(self, last_timestamp):
        """Wait until the next millisecond
        
        Args:
            last_timestamp (int): The last timestamp used
            
        Returns:
            int: The next timestamp in milliseconds
        """
        timestamp = self._get_current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._get_current_timestamp()
        return timestamp
    
    def _get_current_timestamp(self):
        """Get the current timestamp in milliseconds since the epoch
        
        Returns:
            int: Current timestamp in milliseconds
        """
        return int(time.time() * 1000) - self.EPOCH
    
    def next_id(self):
        """Generate the next unique ID
        
        Returns:
            int: A 64-bit unique ID
        """
        timestamp = self._get_current_timestamp()
        
        # Handle clock moving backwards
        if timestamp < self.last_timestamp:
            logger.error(f"Clock moved backwards. Refusing to generate ID for {self.last_timestamp - timestamp} milliseconds")
            raise RuntimeError(f"Clock moved backwards. Refusing to generate ID for {self.last_timestamp - timestamp} milliseconds")
        
        # Handle multiple requests within the same millisecond
        if timestamp == self.last_timestamp:
            # Increment sequence
            self.sequence = (self.sequence + 1) & self.MAX_SEQUENCE
            
            # If sequence is exhausted, wait for the next millisecond
            if self.sequence == 0:
                timestamp = self._wait_next_millis(self.last_timestamp)
        else:
            # Reset sequence for this new millisecond
            self.sequence = 0
        
        self.last_timestamp = timestamp
        
        # Construct the 64-bit ID
        snowflake_id = (
            (timestamp << self.TIMESTAMP_SHIFT) |
            (self.datacenter_id << self.DATACENTER_ID_SHIFT) |
            (self.machine_id << self.MACHINE_ID_SHIFT) |
            self.sequence
        )
        
        return snowflake_id
    
    @staticmethod
    def parse_id(snowflake_id):
        """Parse a snowflake ID back into its components
        
        Args:
            snowflake_id (int): The snowflake ID to parse
            
        Returns:
            dict: A dictionary with the components of the ID
        """
        binary = bin(snowflake_id)[2:].zfill(64)
        
        timestamp_binary = binary[1:42]  # 41 bits for timestamp
        datacenter_binary = binary[42:47]  # 5 bits for datacenter
        machine_binary = binary[47:52]  # 5 bits for machine
        sequence_binary = binary[52:]  # 12 bits for sequence
        
        timestamp = int(timestamp_binary, 2)
        datacenter_id = int(datacenter_binary, 2)
        machine_id = int(machine_binary, 2)
        sequence = int(sequence_binary, 2)
        
        # Convert timestamp back to a readable time
        readable_time = datetime.fromtimestamp((timestamp + SnowflakeIDGenerator.EPOCH) / 1000)
        
        return {
            "id": snowflake_id,
            "timestamp": timestamp,
            "datacenter_id": datacenter_id,
            "machine_id": machine_id,
            "sequence": sequence,
            "generated_time": readable_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        } 