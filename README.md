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

### 4. Web Crawler

A distributed web crawler system capable of efficiently discovering, downloading, and processing web content at scale.

#### Features
- **URL Frontier**: Advanced URL queue management with priority-based scheduling
- **Politeness Policy**: Rate limiting per domain with robots.txt compliance
- **DNS Caching**: Optimized DNS resolver with TTL-based caching
- **Content Deduplication**: Detects and filters duplicate content
- **Distributed Architecture**: Scales horizontally across multiple crawler instances
- **MongoDB Storage**: Persistent storage of crawled URLs and metadata
- **Redis Queuing**: High-performance distributed queue implementation
- **REST API**: Control and monitor the crawler through RESTful endpoints
- **Domain Filtering**: Ability to restrict crawling to specific domains
- **Metadata Extraction**: Extracts page titles, descriptions, and other metadata

### 5. News Feed System

A highly scalable, real-time social media news feed system similar to platforms like Twitter, Facebook, or Instagram that efficiently processes and displays personalized content.

#### Features
- **Multi-tier Architecture**: Separated data, cache, business logic, and API layers
- **Fanout Service**: Efficient content distribution using push/pull hybrid model
- **Five-tier Caching**: Specialized Redis caches for feeds, content, social graph, actions, and counters
- **MongoDB Storage**: Persistent document storage with efficient indexes
- **Content Personalization**: Customized feeds based on follow relationships
- **Social Interactions**: Support for like, comment, and share actions
- **Optimistic UI Updates**: Immediate feedback with background synchronization
- **FastAPI Backend**: High-performance, async REST API with dependency injection
- **React Frontend**: Modern UI with component-based architecture
- **Celebrity Problem Handling**: Special strategies for high-follower accounts
- **RESTful API Design**: Comprehensive endpoints for all social operations

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
1. Create a new numbered directory (e.g., `6_rate_limiter`)
2. Implement your system with clear documentation
3. Update this README to include your implementation

