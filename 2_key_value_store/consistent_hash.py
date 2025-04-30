import hashlib
from bisect import bisect_right
from typing import Dict, List, Optional, Tuple, Any


class ConsistentHash:
    """
    Implementation of Consistent Hashing for node distribution in a ring.

    This class manages a hash ring where each node is placed at multiple
    positions on the ring (virtual nodes). This helps ensure even
    distribution of keys across nodes.
    """

    def __init__(self, nodes: List[str] = None, replicas: int = 3):
        """
        Initialize the consistent hash ring.

        Args:
            nodes: Initial list of node names to add to the ring
            replicas: Number of virtual nodes per physical node
        """
        self.replicas = replicas  # Number of virtual nodes per physical node
        self.ring: Dict[int, str] = {}  # Map hash positions to node names
        self.sorted_keys: List[int] = []  # Sorted list of hash positions

        # Add nodes if provided
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        """
        Generate a hash for a given key.

        Args:
            key: The key to hash

        Returns:
            An integer hash value
        """
        key_bytes = key.encode("utf-8")
        return int(hashlib.md5(key_bytes).hexdigest(), 16)

    def add_node(self, node: str):
        """
        Add a node to the hash ring.

        Args:
            node: The node name to add
        """
        # Add virtual nodes (replicas) for better distribution
        for i in range(self.replicas):
            # Create a unique key for each virtual node
            virtual_node_key = f"{node}:{i}"
            # Calculate hash position
            hash_key = self._hash(virtual_node_key)
            # Add to ring
            self.ring[hash_key] = node
            # Update sorted keys
            self.sorted_keys.append(hash_key)

        # Keep keys sorted for efficient lookups
        self.sorted_keys.sort()

    def remove_node(self, node: str):
        """
        Remove a node from the hash ring.

        Args:
            node: The node name to remove
        """
        # Remove all virtual nodes for this physical node
        for i in range(self.replicas):
            virtual_node_key = f"{node}:{i}"
            hash_key = self._hash(virtual_node_key)

            if hash_key in self.ring:
                del self.ring[hash_key]
                self.sorted_keys.remove(hash_key)

    def get_node(self, key: str) -> Optional[str]:
        """
        Get the node responsible for a given key.

        Args:
            key: The key to look up

        Returns:
            The node name responsible for the key, or None if the ring is empty
        """
        if not self.ring:
            return None

        # Get hash for the key
        hash_key = self._hash(key)

        # Find the first node with hash >= hash_key
        idx = bisect_right(self.sorted_keys, hash_key) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[idx]]

    def get_n_replicas(self, key: str, n: int = 3) -> List[str]:
        """
        Get N distinct nodes for replicating a key.

        Args:
            key: The key to replicate
            n: Number of replica nodes to return

        Returns:
            List of node names for replication
        """
        if not self.ring:
            return []

        if n > len(set(self.ring.values())):
            n = len(set(self.ring.values()))

        # Get hash for the key
        hash_key = self._hash(key)

        # Find starting position on the ring
        idx = bisect_right(self.sorted_keys, hash_key) % len(self.sorted_keys)

        # Collect N unique nodes
        replicas = []
        unique_nodes = set()

        for i in range(len(self.sorted_keys)):
            # Move clockwise around the ring
            curr_idx = (idx + i) % len(self.sorted_keys)
            node = self.ring[self.sorted_keys[curr_idx]]

            if node not in unique_nodes:
                replicas.append(node)
                unique_nodes.add(node)

                if len(replicas) == n:
                    break

        return replicas

    def get_node_count(self) -> int:
        """
        Get the number of unique physical nodes in the ring.

        Returns:
            Number of unique nodes
        """
        return len(set(self.ring.values()))
