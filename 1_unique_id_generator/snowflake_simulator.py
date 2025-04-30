import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from tabulate import tabulate
from snowflake_id_generator import SnowflakeIDGenerator

class DistributedSystemSimulator:
    """Simulates a distributed system with multiple datacenters and machines."""
    
    def __init__(self, num_datacenters=2, num_machines_per_dc=3):
        """Initialize the simulator with the specified number of datacenters and machines.
        
        Args:
            num_datacenters (int): Number of datacenters to simulate
            num_machines_per_dc (int): Number of machines per datacenter
        """
        self.num_datacenters = num_datacenters
        self.num_machines_per_dc = num_machines_per_dc
        self.generators = {}
        self.generated_ids = []
        self.id_lock = threading.Lock()
        
        # Create ID generators for each datacenter and machine
        for dc_id in range(num_datacenters):
            for machine_id in range(num_machines_per_dc):
                key = (dc_id, machine_id)
                self.generators[key] = SnowflakeIDGenerator(dc_id, machine_id)
    
    def generate_id(self, dc_id, machine_id):
        """Generate a unique ID from a specific datacenter and machine.
        
        Args:
            dc_id (int): Datacenter ID
            machine_id (int): Machine ID
            
        Returns:
            int: A unique ID
        """
        generator = self.generators.get((dc_id, machine_id))
        if not generator:
            raise ValueError(f"No generator found for datacenter {dc_id}, machine {machine_id}")
        
        # Simulate some random processing time (0-10ms)
        time.sleep(random.randint(0, 10) / 1000)
        
        # Generate the ID
        snowflake_id = generator.next_id()
        
        # Store the generated ID with its metadata
        with self.id_lock:
            parsed = SnowflakeIDGenerator.parse_id(snowflake_id)
            self.generated_ids.append(parsed)
        
        return snowflake_id
    
    def _worker(self, work_item):
        """Worker function for thread pool.
        
        Args:
            work_item (tuple): (dc_id, machine_id, count)
            
        Returns:
            list: List of generated IDs
        """
        dc_id, machine_id, count = work_item
        results = []
        for _ in range(count):
            results.append(self.generate_id(dc_id, machine_id))
        return results
    
    def simulate_load(self, ids_per_machine=100, max_workers=None):
        """Simulate load by generating multiple IDs from different machines.
        
        Args:
            ids_per_machine (int): Number of IDs to generate per machine
            max_workers (int, optional): Maximum number of worker threads
            
        Returns:
            list: List of all generated IDs
        """
        work_items = []
        for dc_id in range(self.num_datacenters):
            for machine_id in range(self.num_machines_per_dc):
                work_items.append((dc_id, machine_id, ids_per_machine))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            all_ids = list(executor.map(self._worker, work_items))
        
        # Flatten the list of lists
        return [id_val for sublist in all_ids for id_val in sublist]
    
    def display_results(self, limit=10):
        """Display the results of the simulation.
        
        Args:
            limit (int): Maximum number of IDs to display
        """
        # Sort by ID (which sorts by time)
        sorted_ids = sorted(self.generated_ids, key=lambda x: x['id'])
        
        # Display sample IDs
        sample = sorted_ids[:limit]
        headers = ["ID", "Generated Time", "DC ID", "Machine ID", "Sequence"]
        table_data = [
            [
                id_info['id'],
                id_info['generated_time'],
                id_info['datacenter_id'],
                id_info['machine_id'],
                id_info['sequence']
            ]
            for id_info in sample
        ]
        
        print("\n=== Sample Generated IDs ===")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        # Check for duplicates
        unique_ids = set()
        duplicates = []
        
        for id_info in sorted_ids:
            if id_info['id'] in unique_ids:
                duplicates.append(id_info['id'])
            else:
                unique_ids.add(id_info['id'])
        
        print(f"\n=== Statistics ===")
        print(f"Total IDs generated: {len(sorted_ids)}")
        print(f"Unique IDs: {len(unique_ids)}")
        print(f"Duplicate IDs: {len(duplicates)}")
        
        if duplicates:
            print(f"WARNING: {len(duplicates)} duplicate IDs found!")
        else:
            print("SUCCESS: All IDs are unique!")
        
        # Display timestamp distribution
        timestamp_dist = defaultdict(int)
        for id_info in sorted_ids:
            timestamp_dist[id_info['timestamp']] += 1
        
        # Find timestamps with the most IDs
        busy_timestamps = sorted(timestamp_dist.items(), key=lambda x: x[1], reverse=True)[:5]
        
        print("\n=== Timestamp Distribution ===")
        for ts, count in busy_timestamps:
            readable_time = time.strftime(
                '%Y-%m-%d %H:%M:%S.%f', 
                time.localtime((ts + SnowflakeIDGenerator.EPOCH) / 1000)
            )[:-3]
            print(f"Timestamp {ts} ({readable_time}): {count} IDs")
        
        # Display distribution by datacenter and machine
        dist_by_machine = defaultdict(int)
        for id_info in sorted_ids:
            key = (id_info['datacenter_id'], id_info['machine_id'])
            dist_by_machine[key] += 1
        
        print("\n=== Distribution by Datacenter and Machine ===")
        for (dc_id, machine_id), count in sorted(dist_by_machine.items()):
            print(f"Datacenter {dc_id}, Machine {machine_id}: {count} IDs")
        
        # Display sequence distribution
        sequence_dist = defaultdict(int)
        for id_info in sorted_ids:
            sequence_dist[id_info['sequence']] += 1
        
        print("\n=== Sequence Number Distribution ===")
        seq_table = []
        for seq, count in sorted(sequence_dist.items())[:10]:  # Show first 10 sequence numbers
            seq_table.append([seq, count])
        
        print(tabulate(seq_table, headers=["Sequence", "Count"], tablefmt="grid"))


if __name__ == "__main__":
    try:
        # Create a simulator with 2 datacenters, 3 machines per datacenter
        simulator = DistributedSystemSimulator(num_datacenters=2, num_machines_per_dc=3)
        
        print("Starting ID generation simulation...")
        print(f"Simulating 2 datacenters with 3 machines each")
        
        # Generate 100 IDs per machine (600 total IDs)
        simulator.simulate_load(ids_per_machine=100)
        
        # Display results
        simulator.display_results(limit=10)
        
        print("\nBurst test (high concurrency)...")
        # Generate 2000 IDs per machine (12000 total IDs) in a burst
        simulator.simulate_load(ids_per_machine=2000)
        
        # Display count of generated IDs
        print(f"Total IDs after burst test: {len(simulator.generated_ids)}")
        
        print("\nSimulation complete!")
        
    except Exception as e:
        print(f"Error in simulation: {e}") 