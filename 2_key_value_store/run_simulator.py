#!/usr/bin/env python3
"""
Command-line script to run the key-value store simulator.
"""
import sys
import os

# Add the current directory to sys.path to make local modules importable
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import directly from the simulator module
from simulator import KeyValueStoreSimulator


def main():
    """Run the key-value store simulator."""
    # Create simulator
    simulator = KeyValueStoreSimulator(num_nodes=5, replication_factor=3)

    # Run the simulation
    simulator.run_simulation()


if __name__ == "__main__":
    main()
