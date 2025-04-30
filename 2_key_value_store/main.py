import time
import logging
import argparse

from simulator import KeyValueStoreSimulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for the key-value store simulator.
    """
    parser = argparse.ArgumentParser(description="Distributed Key-Value Store Simulator")
    
    # Add command-line arguments
    parser.add_argument(
        "--nodes", 
        type=int, 
        default=5, 
        help="Number of nodes in the cluster (default: 5)"
    )
    parser.add_argument(
        "--replication", 
        type=int, 
        default=3, 
        help="Replication factor (default: 3)"
    )
    parser.add_argument(
        "--demo", 
        type=str, 
        choices=["all", "basic", "conflict", "failure"],
        default="all",
        help="Demo to run (default: all)"
    )
    
    args = parser.parse_args()
    
    # Create simulator
    simulator = KeyValueStoreSimulator(num_nodes=args.nodes, replication_factor=args.replication)
    
    # Set up the cluster
    simulator.setup()
    
    try:
        # Wait for cluster to stabilize
        logger.info("Waiting for cluster to stabilize...")
        time.sleep(3)
        
        # Run the selected demo
        if args.demo == "all" or args.demo == "basic":
            simulator.run_basic_operations()
        
        if args.demo == "all" or args.demo == "conflict":
            simulator.demonstrate_conflicts()
        
        if args.demo == "all" or args.demo == "failure":
            simulator.demonstrate_node_failure()
        
    finally:
        # Clean up
        simulator.teardown()


if __name__ == "__main__":
    main() 