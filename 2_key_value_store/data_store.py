import time
import threading
from typing import Dict, List, Tuple, Any, Optional, Set
import copy
import json

from vector_clock import VectorClock


class VersionedValue:
    """
    Represents a versioned value in the key-value store.
    Each value is associated with a vector clock for versioning.
    """
    
    def __init__(self, value: Any, vector_clock: Dict[str, int] = None):
        """
        Initialize a new versioned value.
        
        Args:
            value: The value to store
            vector_clock: Optional vector clock to associate with this value
        """
        self.value = value
        self.vector_clock = VectorClock(vector_clock)
        self.timestamp = time.time()
    
    def __str__(self) -> str:
        """
        String representation of the versioned value.
        
        Returns:
            A string representation of the value and its version
        """
        return f"Value: {self.value}, Clock: {self.vector_clock}, Time: {self.timestamp}"


class DataStore:
    """
    Core data store implementation for the key-value store.
    This handles local storage, versioning, and conflict resolution.
    """
    
    def __init__(self, node_id: str):
        """
        Initialize a new data store.
        
        Args:
            node_id: The ID of the node this data store belongs to
        """
        self.node_id = node_id
        self.data: Dict[str, List[VersionedValue]] = {}
        self.lock = threading.RLock()  # Reentrant lock for thread safety
    
    def put(self, key: str, value: Any, context: Dict[str, int] = None) -> Dict[str, int]:
        """
        Store a key-value pair.
        
        Args:
            key: The key to store
            value: The value to store
            context: Optional vector clock context for versioning
            
        Returns:
            The updated vector clock
        """
        with self.lock:
            # Create a new vector clock or use the provided context
            clock = VectorClock(context)
            
            # Increment the counter for this node
            clock.increment(self.node_id)
            
            # Create a versioned value
            versioned_value = VersionedValue(value, clock.clock)
            
            # Check if the key exists
            if key not in self.data:
                self.data[key] = [versioned_value]
            else:
                # Check if we need to handle conflicts
                self._handle_conflicts(key, versioned_value)
            
            return clock.clock
    
    def get(self, key: str) -> Tuple[Optional[Any], Optional[Dict[str, int]]]:
        """
        Retrieve a value by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            A tuple of (value, vector_clock) or (None, None) if key doesn't exist
        """
        with self.lock:
            if key not in self.data or not self.data[key]:
                return None, None
            
            # If there are multiple versions (conflicts), return all values
            if len(self.data[key]) > 1:
                values = [version.value for version in self.data[key]]
                # Use the latest vector clock for context
                latest_version = max(self.data[key], key=lambda v: v.timestamp)
                return values, latest_version.vector_clock.copy()
            
            # Return the single value and its vector clock
            version = self.data[key][0]
            return version.value, version.vector_clock.copy()
    
    def delete(self, key: str, context: Dict[str, int] = None) -> bool:
        """
        Delete a key-value pair.
        
        In distributed systems, a delete is often implemented as a special "tombstone" value
        to handle replication and conflict resolution properly.
        
        Args:
            key: The key to delete
            context: Optional vector clock context for versioning
            
        Returns:
            True if the key was deleted, False otherwise
        """
        with self.lock:
            if key not in self.data:
                return False
            
            # Log the deletion for debugging
            print(f"DataStore({self.node_id}): Deleting key '{key}' with {len(self.data[key])} versions")
            
            # For proper distributed delete, we'd set a tombstone value
            # But for simplicity, we'll just remove the key in this implementation
            del self.data[key]
            return True
    
    def keys(self) -> List[str]:
        """
        Get all keys in the data store.
        
        Returns:
            A list of all keys
        """
        with self.lock:
            return list(self.data.keys())
    
    def _handle_conflicts(self, key: str, new_version: VersionedValue) -> None:
        """
        Handle potential conflicts when adding a new version of a key.
        
        Args:
            key: The key being updated
            new_version: The new versioned value
        """
        non_conflicting_versions = []
        conflicting_versions = []
        
        # Track if the new version is a descendant of any existing version
        is_descendant = False
        # Track if the new version has any conflicts with existing versions
        has_conflicts = False
        
        for existing_version in self.data[key]:
            comparison = new_version.vector_clock.compare(existing_version.vector_clock.clock)
            
            if comparison == 1:
                # New version is causally newer, ignore old version
                is_descendant = True
                continue
            elif comparison == -1:
                # New version is causally older, ignore it
                self.data[key] = [existing_version]
                return
            else:
                # Concurrent modifications, potential conflict
                has_conflicts = True
                conflicting_versions.append(existing_version)
        
        # If the new version is a descendant of some versions but conflicts with others,
        # we need to keep both the conflicting versions and the new version
        if is_descendant and has_conflicts:
            self.data[key] = conflicting_versions + [new_version]
        # If the new version is just a descendant (no conflicts), replace all versions
        elif is_descendant:
            self.data[key] = [new_version]
        # If there are only conflicts (no descent), keep all conflicting versions plus new one
        else:
            self.data[key] = conflicting_versions + [new_version]
    
    def resolve_conflicts(self, key: str, resolved_value: Any) -> Dict[str, int]:
        """
        Manually resolve conflicts for a key by setting a new resolved value.
        
        Args:
            key: The key to resolve conflicts for
            resolved_value: The resolved value to set
            
        Returns:
            The new vector clock after conflict resolution
        """
        with self.lock:
            if key not in self.data or len(self.data[key]) <= 1:
                # No conflicts to resolve
                return self.put(key, resolved_value)
            
            # Create a new vector clock by merging all existing clocks
            merged_clock = VectorClock()
            for version in self.data[key]:
                merged_clock.merge(version.vector_clock.clock)
            
            # Increment this node's counter
            merged_clock.increment(self.node_id)
            
            # Create a new versioned value with the resolved value
            resolved_version = VersionedValue(resolved_value, merged_clock.clock)
            
            # Replace all versions with the resolved version
            self.data[key] = [resolved_version]
            
            return merged_clock.clock 