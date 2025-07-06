Custom Database Engine - Enterprise-Grade In-Memory Database

Custom Database Engine is an enterprise-grade, high-performance in-memory database system built with Java and Spring Boot. It features advanced database capabilities including ACID transactions, sophisticated indexing strategies, comprehensive JOIN operations, and intelligent query optimization for production-ready applications.
Features
Core Database Operations

Table Management: Create, drop, alter, and list tables with schema validation
Data Operations: Full CRUD operations (Insert, Select, Update, Delete) with batch processing
Schema Definition: Support for INTEGER, STRING, DOUBLE, BOOLEAN, DATE, and TIMESTAMP data types
Data Validation: Comprehensive field validation, type checking, and constraint enforcement
Schema Evolution: Dynamic schema modification with backward compatibility

Advanced Transaction Management

ACID Compliance: Full ACID (Atomicity, Consistency, Isolation, Durability) transaction support
Rollback Support: Complete transaction rollback with savepoint support
Multi-Version Concurrency Control (MVCC): Optimistic locking for high concurrency
Transaction Logging: Write-ahead logging (WAL) for durability

Sophisticated Indexing System

B-tree Indexing: Self-balancing B-tree indexes for range queries and sorting
Hash Indexing: O(1) lookup time for equality conditions
Unique Indexes: Automatic constraint enforcement

Comprehensive JOIN Operations

INNER JOIN: Returns records with matching values in both tables
LEFT JOIN: Returns all records from left table with matched records from right
RIGHT JOIN: Returns all records from right table with matched records from left
FULL OUTER JOIN: Returns all records when there's a match in either table
Hash JOIN: Optimized for large datasets with equality conditions

Query Optimization Engine

Cost-Based Optimizer (CBO): Intelligent query execution plan selection
Query Rewriting: Automatic query transformation for better performance
Join Reordering: Optimal join sequence determination
Predicate Pushdown: Filter conditions pushed closer to data source
Index Selection: Automatic best index selection for queries
Query Caching: Compiled query plan caching for repeated queries
Statistics Collection: Automatic table and index statistics gathering
Execution Plan Analysis: Detailed execution plan with cost estimation

Storage and Persistence

Multiple Storage Engines: File-based, in-memory, and hybrid storage options
Compression: Data compression for reduced storage footprint
Partitioning: Horizontal partitioning for large tables
Backup and Recovery: Point-in-time recovery with incremental backups
Replication: Master-slave replication for high availability


Getting Started
Prerequisites

Java 11 or higher
Maven 3.6+
Spring Boot 2.7+
Minimum 4GB RAM for optimal performance

Installation

Clone the repository
bashgit clone https://github.com/Sumit-kumar99/custom-database-engine.git
cd custom-database-engine

Build the project
bashmvn clean install

Run the application
bashmvn spring-boot:run


Configuration
Configure the database in application.yml:
yamlcustomdb:
  # Storage Configuration
  storage:
    type: HYBRID  # FILE, MEMORY, HYBRID
    dataPath: ./data
    compressionEnabled: true
    maxMemorySize: 2GB
    
    
  # Index Configuration
  index:
    defaultIndexType: BTREE
    hashIndexBuckets: 1024
    btreeOrder: 100
    statisticsUpdateInterval: 300000
    

server:
  port: 8080
