import random
import logging
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Client:
    """
    Client interface for interacting with the distributed key-value store.
    Provides put, get, and delete operations.
    """
    
    def __init__(self, nodes=None):
        """
        Initialize a client for the key-value store.
        
        Args:
            nodes: List of node objects to connect to
        """
        self.nodes = nodes or []
        self.logger = logging.getLogger("KV-Client")
        self.context_cache: Dict[str, Dict[str, int]] = {}  # Cache for vector clock contexts
    
    def add_node(self, node):
        """
        Add a node to the client's connection list.
        
        Args:
            node: The node object to add
        """
        if node not in self.nodes:
            self.nodes.append(node)
    
    def put(self, key: str, value: Any) -> bool:
        """
        Store a key-value pair in the system.
        
        Args:
            key: The key to store
            value: The value to store
            
        Returns:
            True if the operation was successful, False otherwise
        """
        if not self.nodes:
            self.logger.error("No nodes available for put operation")
            return False
        
        # Get the context for this key if we have it
        context = self.context_cache.get(key)
        
        # Try nodes in random order until one succeeds
        random_nodes = random.sample(self.nodes, len(self.nodes))
        
        for node in random_nodes:
            success = node.put(key, value, context)
            if success:
                # Update context cache with the new version
                _, new_context = node.get(key)
                if new_context:
                    self.context_cache[key] = new_context
                return True
        
        self.logger.error(f"Failed to put key '{key}' - no nodes available or quorum not reached")
        return False
    
    def get(self, key: str) -> Any:
        """
        Retrieve a value by key from the system.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value associated with the key, or None if the key doesn't exist
        """
        if not self.nodes:
            self.logger.error("No nodes available for get operation")
            return None
        
        # Try nodes in random order until one succeeds
        random_nodes = random.sample(self.nodes, len(self.nodes))
        
        # We'll collect all values to detect conflicts
        all_results = []
        
        for node in random_nodes:
            try:
                result = node.get(key)
                
                if result[0] is not None:
                    value, context = result
                    # Update context cache
                    if context:
                        self.context_cache[key] = context
                    
                    # We got a successful read, store the result for conflict detection
                    all_results.append(value)
                    # For non-list values, we need at least one successful read
                    if not isinstance(value, list):
                        return value
                    # For list values (conflicts), we'll collect all and return them together
            except Exception as e:
                self.logger.error(f"Error getting key {key} from node {node.node_id}: {e}")
        
        # If we collected any conflicts (list values), return them
        if all_results:
            # Flatten any nested lists
            flat_results = []
            for result in all_results:
                if isinstance(result, list):
                    flat_results.extend(result)
                else:
                    flat_results.append(result)
            
            # Remove duplicates while keeping order
            seen = set()
            unique_results = [x for x in flat_results if not (x in seen or seen.add(x))]
            
            # If there are multiple unique values, return them as a conflict
            if len(unique_results) > 1:
                return unique_results
            # If there's only one unique value, return it directly
            elif len(unique_results) == 1:
                return unique_results[0]
        
        self.logger.error(f"Failed to get key '{key}' - no nodes available or quorum not reached")
        return None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair from the system.
        
        Args:
            key: The key to delete
            
        Returns:
            True if the operation was successful, False otherwise
        """
        if not self.nodes:
            self.logger.error("No nodes available for delete operation")
            return False
        
        # Get the context for this key if we have it
        context = self.context_cache.get(key)
        
        # Try to delete from all nodes to ensure deletion is propagated
        success = False
        deletion_attempts = 0
        deletion_successes = 0
        
        # Try nodes in random order but try all of them to ensure deletion propagates
        random_nodes = random.sample(self.nodes, len(self.nodes))
        
        for node in random_nodes:
            deletion_attempts += 1
            try:
                if node.delete(key, context):
                    deletion_successes += 1
                    success = True
            except Exception as e:
                self.logger.error(f"Error deleting key {key} from node {node.node_id}: {e}")
        
        self.logger.info(f"Delete operation for key '{key}' completed: {deletion_successes}/{deletion_attempts} nodes succeeded")
        
        # Remove from context cache if at least one deletion succeeded
        if success and key in self.context_cache:
            del self.context_cache[key]
            
        # Verify the delete worked by attempting a get - should return None for properly deleted keys
        verify_value = self.get(key)
        if verify_value is not None:
            self.logger.warning(f"Delete verification failed for key '{key}': key still exists with value {verify_value}")
            return False
            
        return success
    
    def resolve_conflict(self, key: str, resolved_value: Any) -> bool:
        """
        Manually resolve a conflict for a key.
        
        Args:
            key: The key to resolve
            resolved_value: The value to use for resolution
            
        Returns:
            True if the resolution was successful, False otherwise
        """
        if not self.nodes:
            self.logger.error("No nodes available for conflict resolution")
            return False
        
        # Try to get all values and contexts
        values_and_contexts = []
        for node in self.nodes:
            result = node.get(key)
            if result[0] is not None:
                values_and_contexts.append(result)
        
        if not values_and_contexts:
            self.logger.error(f"No values found for key '{key}' to resolve conflict")
            return False
        
        # Determine if there's a conflict
        if len(values_and_contexts) == 1 and not isinstance(values_and_contexts[0][0], list):
            self.logger.info(f"No conflict detected for key '{key}'")
            return True
        
        # Create a merged context from all available contexts
        merged_context = {}
        for _, context in values_and_contexts:
            if context:
                for node_id, counter in context.items():
                    merged_context[node_id] = max(merged_context.get(node_id, 0), counter)
        
        # Perform the put with the merged context
        return self.put(key, resolved_value) 