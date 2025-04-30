# Distributed Unique ID Generator Simulation

This project implements a simulation of the Twitter Snowflake algorithm for generating unique IDs in a distributed system.

## Overview

The Twitter Snowflake algorithm generates 64-bit IDs with the following structure:
- 1 bit: Sign bit (always 0)
- 41 bits: Timestamp (milliseconds since custom epoch)
- 5 bits: Datacenter ID (0-31)
- 5 bits: Machine ID (0-31)
- 12 bits: Sequence number (0-4095)

This structure allows for:
- Time-sorted IDs
- Up to 32 datacenters
- Up to 32 machines per datacenter
- Up to 4,096 IDs per machine per millisecond
- Theoretically up to ~4.1 million IDs per millisecond across all machines

## Components

- `snowflake_id_generator.py`: Core implementation of the Snowflake algorithm
- `snowflake_simulator.py`: Simulation of a distributed environment with multiple datacenters and machines
- `snowflake_visualizer.py`: Utility to visualize the binary structure of Snowflake IDs
- `real_world_example.py`: A practical example showing how Snowflake IDs can be used in a social media application
- `verify_requirements.py`: Script to verify that our implementation meets all the requirements
- `run_all.py`: Unified script to run all components with a simple CLI

## Requirements

- Python 3.6+
- tabulate library

## Installation

1. Install the required dependencies:
```
pip install -r requirements.txt
```

## Running the Simulation and Examples

### Unified Runner

The simplest way to run any component is using the unified runner:

```
python run_all.py [component]
```

Available components:
- `basic`: Run a basic ID generator demo
- `simulator`: Run the distributed system simulator
- `visualizer`: Run the ID visualizer
- `example`: Run the real-world example
- `verify`: Run the requirements verification
- `all`: Run all components in sequence

For example:
```
python run_all.py all
```

You can also get help on available options:
```
python run_all.py --help
```

### Distributed System Simulator

Execute the simulator script to see how IDs are generated across multiple datacenters and machines:
```
python snowflake_simulator.py
```

The simulator will:
1. Create a distributed environment with 2 datacenters and 3 machines per datacenter
2. Generate 100 IDs per machine (600 total) in the first run
3. Perform a burst test generating 2000 IDs per machine (12000 total)
4. Display statistics and sample IDs

### ID Visualizer

To visualize the structure of a Snowflake ID:
```
python snowflake_visualizer.py
```

You can also pass a specific ID to visualize:
```
python snowflake_visualizer.py 1234567890123456789
```

### Real-World Example

To see how Snowflake IDs can be used in a real application:
```
python real_world_example.py
```

This simulates a simple social media application with:
- User service generating unique user IDs
- Post service generating unique post IDs
- Time-sortable feeds based on ID ordering

### Requirements Verification

To verify that our implementation meets all the requirements from the problem statement:
```
python verify_requirements.py
```

This script tests:
1. Uniqueness of generated IDs
2. That IDs are numerical values only
3. That IDs fit into 64 bits
4. That IDs are ordered by time
5. That the system can generate more than 10,000 IDs per second

## Customization

You can modify the scripts to change:
- Number of datacenters
- Number of machines per datacenter
- Number of IDs generated per machine
- Custom epoch time

## System Design Considerations

- **Uniqueness**: The combination of timestamp, datacenter ID, machine ID, and sequence number ensures uniqueness
- **Scalability**: The system can theoretically generate up to 4.1 million IDs per millisecond
- **Time-sorting**: IDs are sortable by time due to the timestamp being the most significant bits
- **Clock synchronization**: In a real distributed system, clock synchronization would be important (NTP)
- **No coordination needed**: Each machine can generate IDs independently without coordination with other machines
- **No single point of failure**: Unlike approaches like a ticket server, there's no central point of failure

## Learning Points

This simulation demonstrates several important system design concepts:
1. **Divide and conquer**: Breaking down a problem into manageable parts (timestamp, datacenter, machine, sequence)
2. **Time-based ordering**: Using timestamps to create naturally sortable IDs
3. **Concurrency handling**: Using sequence numbers to handle multiple requests in the same millisecond
4. **Distributed systems**: Creating unique identifiers without central coordination
5. **Bit manipulation**: Efficient packing of information into a 64-bit integer 