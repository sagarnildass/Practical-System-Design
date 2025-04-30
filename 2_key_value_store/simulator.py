import time
import random
import logging

from node import Node
from client import Client
from data_store import VersionedValue

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KeyValueStoreSimulator:
    """
    Simulator for the distributed key-value store.
    Demonstrates key functionality including:
    - Data partitioning with consistent hashing
    - Data replication
    - Vector clock versioning
    - Conflict detection and resolution
    - Quorum consensus
    - Gossip protocol for failure detection
    """

    def __init__(self, num_nodes=5, replication_factor=3):
        """
        Initialize the simulator with the specified parameters.

        Args:
            num_nodes: Number of nodes in the cluster
            replication_factor: Number of replicas for each key
        """
        self.num_nodes = num_nodes
        self.replication_factor = replication_factor
        self.nodes = []
        self.client = None

    def setup(self):
        """
        Set up the simulator by creating nodes and a client.
        """
        logger.info(
            "Setting up cluster with %d nodes (replication factor: %d)",
            self.num_nodes,
            self.replication_factor,
        )

        # Create nodes
        for i in range(self.num_nodes):
            node_id = f"node-{i}"

            # First node is standalone, others join through the first node
            if i == 0:
                node = Node(node_id)
            else:
                node = Node(node_id, coordinator=self.nodes[0])

            # Set replication settings
            node.N = self.replication_factor
            node.W = max(1, self.replication_factor // 2 + 1)  # Majority for writes
            node.R = max(1, self.replication_factor // 2 + 1)  # Majority for reads

            self.nodes.append(node)

        # Start all nodes
        for node in self.nodes:
            node.start()

        # Create client
        self.client = Client(self.nodes)

        logger.info("Cluster setup complete. %d nodes running.", len(self.nodes))

    def teardown(self):
        """
        Clean up the simulator by stopping all nodes.
        """
        logger.info("Tearing down cluster...")

        for node in self.nodes:
            node.stop()

        logger.info("Cluster teardown complete.")

    def run_basic_operations(self):
        """
        Demonstrate basic operations (put, get, delete) on the key-value store.
        """
        logger.info("=== Basic Operations Demo ===")

        # Put some key-value pairs
        keys_to_test = ["user:1", "user:2", "product:123", "order:456"]
        values_to_test = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Smartphone", "price": 599.99},
            {"items": ["product:123", "product:456"], "total": 1024.98},
        ]

        logger.info("Putting key-value pairs...")
        for key, value in zip(keys_to_test, values_to_test):
            success = self.client.put(key, value)
            logger.info(
                "PUT %s -> %s: %s", key, value, "Success" if success else "Failed"
            )

        # Get the key-value pairs
        logger.info("Getting values...")
        for key in keys_to_test:
            value = self.client.get(key)
            logger.info("GET %s -> %s", key, value)

        # Delete a key
        key_to_delete = keys_to_test[0]
        logger.info("Deleting key %s...", key_to_delete)
        success = self.client.delete(key_to_delete)
        logger.info("DELETE %s: %s", key_to_delete, "Success" if success else "Failed")

        # Verify deletion
        value = self.client.get(key_to_delete)
        logger.info("GET %s after delete -> %s", key_to_delete, value)

    def demonstrate_conflicts(self):
        """
        Demonstrate conflict detection and resolution.
        """
        logger.info("=== Conflict Detection and Resolution Demo ===")

        # Key to use for conflict demonstration
        conflict_key = "shared:counter"

        # Initial value
        logger.info("Setting initial value for %s...", conflict_key)
        self.client.put(conflict_key, 0)

        # Get the initial context to use for both modifications
        initial_value, initial_context = self.nodes[0].data_store.get(conflict_key)
        logger.info("Initial context: %s", initial_context)

        # Make sure the conflict key is stored on both node-0 and node-1
        # This is a simulation hack to guarantee conflict detection
        value_5 = VersionedValue(5, initial_context.copy() if initial_context else {})
        value_10 = VersionedValue(10, initial_context.copy() if initial_context else {})

        # First modification - directly to data store
        logger.info("Node 0 setting counter to 5...")
        self.nodes[0].data_store.data[conflict_key] = [value_5]

        # Second modification - directly to data store
        logger.info("Node 1 setting counter to 10 (concurrent with Node 0)...")
        self.nodes[1].data_store.data[conflict_key] = [value_10]

        # Try to read the value now - should detect both values
        logger.info("Reading values from all nodes directly...")
        values = []
        for node in self.nodes:
            val, ctx = node.data_store.get(conflict_key)
            if val is not None:
                logger.info(
                    "Node %s has value: %s with context %s", node.node_id, val, ctx
                )
                if val not in values:
                    values.append(val)

        logger.info("Found values across nodes: %s", values)

        # Try through the client
        value = self.client.get(conflict_key)
        logger.info("GET %s after concurrent modifications -> %s", conflict_key, value)

        # If we got a list of values, we have a conflict
        if isinstance(value, list):
            logger.info("Conflict detected! Multiple values: %s", value)

            # Resolve the conflict by taking the max value
            resolved_value = max(value)
            logger.info("Resolving conflict by taking max value: %s", resolved_value)

            success = self.client.resolve_conflict(conflict_key, resolved_value)
            logger.info("Conflict resolution: %s", "Success" if success else "Failed")

            # Verify resolution
            value = self.client.get(conflict_key)
            logger.info("GET %s after resolution -> %s", conflict_key, value)
        else:
            # Simulate conflict resolution even if not automatically detected
            logger.warning(
                "No conflict detected through standard client interface. Performing manual resolution."
            )
            resolved_value = max(values)
            logger.info(
                "Manually resolving conflict by taking max value: %s", resolved_value
            )
            success = self.client.resolve_conflict(conflict_key, resolved_value)
            logger.info(
                "Manual conflict resolution: %s", "Success" if success else "Failed"
            )

    def demonstrate_node_failure(self):
        """
        Demonstrate node failure handling through the gossip protocol.
        """
        logger.info("=== Node Failure Detection Demo ===")

        # Store a key on the cluster
        key = "failure:test"
        value = "Value before failure"

        logger.info("Putting key %s before node failure...", key)
        success = self.client.put(key, value)
        logger.info("PUT %s -> %s: %s", key, value, "Success" if success else "Failed")

        # Choose a node to "fail"
        failed_node_idx = random.randint(0, len(self.nodes) - 1)
        failed_node = self.nodes[failed_node_idx]
        failed_node_id = failed_node.node_id

        logger.info("Simulating failure of node %s...", failed_node_id)

        # Log membership before failure
        logger.info("=== Membership Lists Before Failure ===")
        for node in self.nodes:
            logger.info(
                "Node %s membership: %s",
                node.node_id,
                list(node.get_membership().keys()),
            )

        # Simulate node failure by stopping it
        failed_node.stop()

        # Allow time for failure detection to kick in
        logger.info("Waiting for gossip protocol to detect and propagate failure...")
        time.sleep(4)  # Need time for multiple gossip intervals

        # Check if the failed node was properly removed from all membership lists
        logger.info("=== Membership Lists After Failure ===")
        failure_detected_by = []
        for i, node in enumerate(self.nodes):
            if i != failed_node_idx and node.is_running:
                membership = node.get_membership()
                logger.info(
                    "Node %s membership: %s", node.node_id, list(membership.keys())
                )
                if failed_node_id not in membership:
                    failure_detected_by.append(node.node_id)

        if failure_detected_by:
            logger.info(
                "Failed node %s was properly removed from membership lists by nodes: %s",
                failed_node_id,
                failure_detected_by,
            )
        else:
            logger.warning(
                "Failed node %s was not properly removed from any membership lists",
                failed_node_id,
            )

        # Verify we can still read the key
        value = self.client.get(key)
        logger.info("GET %s after node failure -> %s", key, value)

        # Restart the failed node
        logger.info("Restarting failed node %s...", failed_node_id)
        failed_node.is_running = True
        failed_node.start()

        # Have it rejoin the cluster
        live_node = self.nodes[(failed_node_idx + 1) % len(self.nodes)]
        failed_node.join_cluster(live_node)
        logger.info("Node %s rejoined the cluster", failed_node_id)

        # Wait a moment for the rejoined node to be recognized
        time.sleep(1)

        # Log final membership lists
        logger.info("=== Final Membership Lists ===")
        for node in self.nodes:
            if node.is_running:
                logger.info(
                    "Node %s membership: %s",
                    node.node_id,
                    list(node.get_membership().keys()),
                )

    def run_simulation(self):
        """
        Run the full simulation demonstrating all features.
        """
        try:
            self.setup()

            # Wait for cluster to stabilize
            logger.info("Waiting for cluster to stabilize...")
            time.sleep(2)  # Reduce from 3 to 2 seconds

            # Run basic operations demo
            self.run_basic_operations()

            # Demonstrate conflicts
            self.demonstrate_conflicts()

            # Demonstrate node failure
            self.demonstrate_node_failure()

        finally:
            self.teardown()


if __name__ == "__main__":
    simulator = KeyValueStoreSimulator(num_nodes=5, replication_factor=3)
    simulator.run_simulation()
