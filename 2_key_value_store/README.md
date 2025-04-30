# Distributed Key-Value Store

This is a simulation of a distributed key-value store based on systems like Amazon Dynamo, Cassandra, and Riak. It demonstrates the core concepts and techniques used in modern distributed databases.

## Features

- **Consistent Hashing:** Distributes keys evenly across nodes with minimal redistribution when nodes are added or removed
- **Vector Clocks:** Provides causal ordering and conflict detection for distributed updates
- **Quorum Consensus:** Configurable read/write quorums for tunable consistency
- **Data Partitioning:** Automatically partitions data across multiple nodes
- **Data Replication:** Replicates data across multiple nodes for fault tolerance
- **Gossip Protocol:** Decentralized failure detection
- **Conflict Resolution:** Automatic conflict detection with manual resolution

## Components

The system consists of several key components:

1. **Consistent Hash Ring (`consistent_hash.py`):** Implements consistent hashing for data partitioning and distribution
2. **Vector Clock (`vector_clock.py`):** Provides versioning with causality tracking and conflict detection
3. **Data Store (`data_store.py`):** Handles local data storage with versioning
4. **Node (`node.py`):** Represents a server in the distributed system
5. **Client (`client.py`):** Provides a client interface to interact with the distributed store
6. **Simulator (`simulator.py`):** Demonstrates the key-value store functionality

## Key Design Decisions

### CAP Theorem Trade-offs

This implementation follows an AP (Availability and Partition Tolerance) model similar to Dynamo:

- **Availability:** The system prioritizes availability, allowing reads and writes even during network partitions
- **Partition Tolerance:** The system can continue functioning when network partitions occur
- **Eventual Consistency:** The system guarantees eventual consistency rather than strong consistency

### Quorum Consensus

The system implements configurable quorum consensus:
- N = Number of replicas (default: 3)
- W = Write quorum (default: 2)
- R = Read quorum (default: 2)

With W + R > N, the system provides stronger consistency guarantees. When W + R ≤ N, the system prioritizes availability.

### Versioning and Conflict Resolution

Vector clocks are used to track causality between updates:
- Each update includes a vector clock indicating the version history
- Concurrent updates are detected when vector clocks diverge
- Conflicts are detected on read and can be manually resolved
- The system allows reading multiple conflicting versions

### Architecture

The architecture follows a completely decentralized design with no single point of failure:
- Any node can serve as a coordinator for client requests
- Nodes use consistent hashing to determine data placement
- Gossip protocol is used for failure detection and membership management

## Usage

To run the simulator and see the key-value store in action:

```bash
python simulator.py
```

The simulator demonstrates:
1. Basic operations (put, get, delete)
2. Conflict detection and resolution
3. Node failure handling

## Implementation Notes

This is a simulation of a distributed system running in a single process. In a real implementation:

- Nodes would run on separate machines
- Network communication would be handled via RPC/messaging
- Persistence would be added with commit logs and SSTables
- Read/write paths would include bloom filters and caching
- Anti-entropy would be implemented with Merkle trees

## Example Operations

### Basic Usage

```python
# Create a client connected to the cluster
client = Client(nodes)

# Store a key-value pair
client.put("user:123", {"name": "Alice", "email": "alice@example.com"})

# Retrieve a value
user = client.get("user:123")

# Delete a key-value pair
client.delete("user:123")
```

### Handling Conflicts

```python
# Get a value that might have conflicts
value = client.get("counter")

# Check if we got multiple conflicting values
if isinstance(value, list):
    # Resolve by taking max value
    resolved_value = max(value)
    client.resolve_conflict("counter", resolved_value)
``` 