#!/usr/bin/env python3
"""
Unified entry point for the Snowflake ID Generator project.
Allows running all components from a single script.
"""

import sys
import time
import argparse

def print_header(title):
    """Print a section header.
    
    Args:
        title (str): The title to print
    """
    width = 80
    print("\n" + "=" * width)
    print(f"{title:^{width}}")
    print("=" * width + "\n")

def run_id_generator():
    """Run a basic demonstration of the ID generator."""
    from snowflake_id_generator import SnowflakeIDGenerator
    
    print_header("BASIC ID GENERATOR DEMONSTRATION")
    
    print("Creating ID generator with datacenter_id=1, machine_id=1...")
    generator = SnowflakeIDGenerator(1, 1)
    
    print("\nGenerating 5 IDs...")
    for i in range(5):
        id_val = generator.next_id()
        parsed = SnowflakeIDGenerator.parse_id(id_val)
        print(f"ID {i+1}: {id_val}")
        print(f"  Generated at: {parsed['generated_time']}")
        print(f"  Datacenter: {parsed['datacenter_id']}")
        print(f"  Machine: {parsed['machine_id']}")
        print(f"  Sequence: {parsed['sequence']}\n")
    
    print("Basic demonstration complete!")
    
def run_simulator():
    """Run the distributed system simulator."""
    import snowflake_simulator
    
    print_header("DISTRIBUTED SYSTEM SIMULATOR")
    
    # Run the simulator's main function
    try:
        # Create a simulator with 2 datacenters, 3 machines per datacenter
        simulator = snowflake_simulator.DistributedSystemSimulator(num_datacenters=2, num_machines_per_dc=3)
        
        print("Starting ID generation simulation...")
        print(f"Simulating 2 datacenters with 3 machines each")
        
        # Generate 100 IDs per machine (600 total IDs)
        simulator.simulate_load(ids_per_machine=100)
        
        # Display results
        simulator.display_results(limit=5)
        
    except Exception as e:
        print(f"Error in simulation: {e}")

def run_visualizer(id_val=None):
    """Run the ID visualizer.
    
    Args:
        id_val (int, optional): The ID to visualize
    """
    from snowflake_id_generator import SnowflakeIDGenerator
    import snowflake_visualizer
    
    print_header("SNOWFLAKE ID VISUALIZER")
    
    if id_val is None:
        # Generate a new ID
        print("No ID provided. Generating a new ID...")
        generator = SnowflakeIDGenerator(datacenter_id=1, machine_id=1)
        id_val = generator.next_id()
        print(f"Generated ID: {id_val}")
    
    # Visualize the ID
    snowflake_visualizer.visualize_binary(id_val)

def run_real_world_example():
    """Run the real-world example application."""
    import real_world_example
    
    print_header("REAL-WORLD EXAMPLE: SOCIAL MEDIA APPLICATION")
    
    # Run the example's main function
    real_world_example.run_simulation()

def run_verification():
    """Run the requirements verification tests."""
    import verify_requirements
    
    print_header("REQUIREMENTS VERIFICATION")
    
    # Run the verification tests
    verify_requirements.run_all_tests()

def run_all_components():
    """Run all components in sequence."""
    print_header("RUNNING ALL COMPONENTS")
    
    run_id_generator()
    time.sleep(1)
    
    run_visualizer()
    time.sleep(1)
    
    run_real_world_example()
    time.sleep(1)
    
    run_simulator()
    time.sleep(1)
    
    print("\nSkipping verification tests in all-components mode (takes too long).")
    print("Run with --verify to run verification tests separately.")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Snowflake ID Generator Demo Runner")
    
    # Define the subparsers for each component
    subparsers = parser.add_subparsers(dest="component", help="Component to run")
    
    # Basic generator parser
    basic_parser = subparsers.add_parser("basic", help="Run basic ID generator demo")
    
    # Simulator parser
    sim_parser = subparsers.add_parser("simulator", help="Run distributed system simulator")
    
    # Visualizer parser
    vis_parser = subparsers.add_parser("visualizer", help="Run ID visualizer")
    vis_parser.add_argument("--id", type=int, help="Specific ID to visualize")
    
    # Real-world example parser
    example_parser = subparsers.add_parser("example", help="Run real-world example")
    
    # Verification parser
    verify_parser = subparsers.add_parser("verify", help="Run requirements verification")
    
    # All components parser
    all_parser = subparsers.add_parser("all", help="Run all components in sequence")
    
    # Parse args
    args = parser.parse_args()
    
    # Handle component selection
    if args.component == "basic":
        run_id_generator()
    elif args.component == "simulator":
        run_simulator()
    elif args.component == "visualizer":
        run_visualizer(args.id)
    elif args.component == "example":
        run_real_world_example()
    elif args.component == "verify":
        run_verification()
    elif args.component == "all":
        run_all_components()
    else:
        # Default to showing help if no component specified
        parser.print_help()

if __name__ == "__main__":
    main() 