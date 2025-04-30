from typing import Dict, Optional, List, Tuple
import copy


class VectorClock:
    """
    Implementation of a vector clock for tracking and reconciling
    causality and conflicts between distributed events.
    
    A vector clock is represented as a dictionary of (node_id, counter) pairs.
    """
    
    def __init__(self, clock: Dict[str, int] = None):
        """
        Initialize a new vector clock.
        
        Args:
            clock: Optional initial clock values as a dictionary of node_id to counter
        """
        self.clock = clock or {}
    
    def increment(self, node_id: str) -> Dict[str, int]:
        """
        Increment the counter for a specific node.
        
        Args:
            node_id: The ID of the node to increment
            
        Returns:
            The updated clock dictionary
        """
        if node_id in self.clock:
            self.clock[node_id] += 1
        else:
            self.clock[node_id] = 1
        
        return self.clock
    
    def merge(self, other_clock: Dict[str, int]) -> Dict[str, int]:
        """
        Merge this vector clock with another vector clock by taking
        the maximum value for each node.
        
        Args:
            other_clock: Another vector clock to merge with
            
        Returns:
            The merged clock dictionary
        """
        # Create a new clock with all entries from both clocks
        merged_clock = copy.deepcopy(self.clock)
        
        # Take the maximum value for each node_id
        for node_id, counter in other_clock.items():
            merged_clock[node_id] = max(merged_clock.get(node_id, 0), counter)
        
        self.clock = merged_clock
        return self.clock
    
    def compare(self, other_clock: Dict[str, int]) -> int:
        """
        Compare this vector clock with another vector clock to determine
        their causal relationship.
        
        Args:
            other_clock: Another vector clock to compare with
            
        Returns:
            -1 if this clock < other_clock (happens-before)
            0 if this clock and other_clock are concurrent (conflict)
            1 if this clock > other_clock (happened-after)
        """
        # Check if self happens before other
        self_smaller = False
        
        # Check if other happens before self
        other_smaller = False
        
        # Get all unique node_ids from both clocks
        all_node_ids = set(list(self.clock.keys()) + list(other_clock.keys()))
        
        for node_id in all_node_ids:
            self_counter = self.clock.get(node_id, 0)
            other_counter = other_clock.get(node_id, 0)
            
            if self_counter < other_counter:
                self_smaller = True
            elif self_counter > other_counter:
                other_smaller = True
                
            # If both flags are set, there's a conflict
            if self_smaller and other_smaller:
                return 0  # Concurrent events, conflict
        
        if self_smaller and not other_smaller:
            return -1  # Self happens before other
        if other_smaller and not self_smaller:
            return 1  # Self happens after other
        
        # If we get here, the clocks are identical
        return 0  # No conflict, but also not causally related
    
    def copy(self) -> Dict[str, int]:
        """
        Create a deep copy of this vector clock.
        
        Returns:
            A copy of the vector clock dictionary
        """
        return copy.deepcopy(self.clock)
    
    def __eq__(self, other):
        """
        Check if two vector clocks are equal.
        
        Args:
            other: Another VectorClock instance
            
        Returns:
            True if equal, False otherwise
        """
        if not isinstance(other, VectorClock):
            return False
        return self.clock == other.clock
    
    def __str__(self) -> str:
        """
        String representation of the vector clock.
        
        Returns:
            A string representation of the clock
        """
        return str(self.clock) 