# Practical System Design

A hands-on repository for learning and implementing practical distributed systems concepts, with a focus on real-world applications and concrete implementations.

## Projects

### 1. Unique ID Generator

An implementation of a distributed unique ID generation system, inspired by designs like Twitter's Snowflake.

#### Features
- Scalable ID generation across multiple servers
- Time-based ordering of IDs
- Worker ID component to identify different generators
- Sequence numbers to handle multiple IDs generated in the same millisecond
- Handles clock drift scenarios
- Compact representation with efficient storage

### 2. Distributed Key-Value Store

A from-scratch implementation of a distributed key-value store demonstrating core distributed systems concepts:

#### Features
- **Data Partitioning**: Uses consistent hashing to distribute data across nodes
- **Replication**: Implements a configurable replication factor for fault tolerance
- **Consensus**: Uses quorum-based reads and writes for data consistency
- **Versioning**: Implements vector clocks for tracking causality
- **Conflict Detection**: Identifies conflicting writes across replicas
- **Gossip Protocol**: Detects node failures and maintains membership

### 3. URL Shortener

A scalable URL shortening service similar to TinyURL or Bit.ly with a focus on high performance and reliability.

#### Features
- **Base62 Encoding**: Efficiently converts IDs to short URL strings using alphanumeric characters
- **Distributed ID Generation**: Snowflake-inspired approach for unique, sortable IDs
- **Caching Layer**: Redis implementation for high-performance URL retrieval
- **Database Persistence**: MySQL storage with connection pooling
- **API Design**: RESTful API with rate limiting for URL operations
- **Analytics**: Click tracking and basic statistics
- **High Read Throughput**: Optimized for the read-heavy workload typical of URL shorteners

## Learning Goals

This repository aims to provide:

- Concrete implementations of distributed systems concepts
- Practical examples that go beyond theory
- Code that can be run, modified, and extended for learning
- Realistic simulations of distributed system behaviors and failure scenarios

## How to Use This Repository

Each numbered directory is a standalone system design implementation with its own documentation:

1. Each project directory contains the complete implementation of a distributed system concept
2. Code is organized to clearly demonstrate architectural patterns and design decisions
3. Implementations are modular and well-documented for learning purposes

## Future Projects

Planned additions to this repository:
- Distributed rate limiter
- Distributed file system
- Distributed cache
- Distributed transaction processing
- Consensus algorithms implementation
- Load balancer design
- Content delivery network (CDN)

## Contributing

To add a new system design to this repository:
1. Create a new numbered directory (e.g., `4_rate_limiter`)
2. Implement your system with clear documentation
3. Update this README to include your implementation

