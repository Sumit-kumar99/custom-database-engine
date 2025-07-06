# Custom Database Engine

> An enterprise-grade, high-performance in-memory database system built with Java and Spring Boot

[![Java Version](https://img.shields.io/badge/Java-11%2B-blue.svg)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-2.7%2B-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](https://github.com/Sumit-kumar99/custom-database-engine)

## Overview

Custom Database Engine is an enterprise-grade, high-performance in-memory database system that features advanced database capabilities including ACID transactions, sophisticated indexing strategies, comprehensive JOIN operations, and intelligent query optimization for production-ready applications.

## Features

### 🗃️ Core Database Operations

- **Table Management**: Create, drop, alter, and list tables with schema validation
- **Data Operations**: Full CRUD operations (Insert, Select, Update, Delete) with batch processing
- **Schema Definition**: Support for `INTEGER`, `STRING`, `DOUBLE`, `BOOLEAN`, `DATE`, and `TIMESTAMP` data types
- **Data Validation**: Comprehensive field validation, type checking, and constraint enforcement
- **Schema Evolution**: Dynamic schema modification with backward compatibility

### 🔄 Advanced Transaction Management

- **ACID Compliance**: Full ACID (Atomicity, Consistency, Isolation, Durability) transaction support
- **Rollback Support**: Complete transaction rollback with savepoint support
- **Multi-Version Concurrency Control (MVCC)**: Optimistic locking for high concurrency
- **Transaction Logging**: Write-ahead logging (WAL) for durability

### 🔍 Sophisticated Indexing System

- **B-tree Indexing**: Self-balancing B-tree indexes for range queries and sorting
- **Hash Indexing**: O(1) lookup time for equality conditions
- **Unique Indexes**: Automatic constraint enforcement

### 🔗 Comprehensive JOIN Operations

- **INNER JOIN**: Returns records with matching values in both tables
- **LEFT JOIN**: Returns all records from left table with matched records from right
- **RIGHT JOIN**: Returns all records from right table with matched records from left
- **FULL OUTER JOIN**: Returns all records when there's a match in either table
- **Hash JOIN**: Optimized for large datasets with equality conditions

### ⚡ Query Optimization Engine

- **Cost-Based Optimizer (CBO)**: Intelligent query execution plan selection
- **Query Rewriting**: Automatic query transformation for better performance
- **Join Reordering**: Optimal join sequence determination
- **Predicate Pushdown**: Filter conditions pushed closer to data source
- **Index Selection**: Automatic best index selection for queries
- **Query Caching**: Compiled query plan caching for repeated queries
- **Statistics Collection**: Automatic table and index statistics gathering
- **Execution Plan Analysis**: Detailed execution plan with cost estimation

### 💾 Storage and Persistence

- **Multiple Storage Engines**: File-based, in-memory, and hybrid storage options
- **Compression**: Data compression for reduced storage footprint
- **Partitioning**: Horizontal partitioning for large tables
- **Backup and Recovery**: Point-in-time recovery with incremental backups
- **Replication**: Master-slave replication for high availability

## Getting Started

### Prerequisites

- Java 11 or higher
- Maven 3.6+
- Spring Boot 2.7+
- Minimum 4GB RAM for optimal performance

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Sumit-kumar99/custom-database-engine.git
   cd custom-database-engine
   ```

2. **Build the project**
   ```bash
   mvn clean install
   ```

3. **Run the application**
   ```bash
   mvn spring-boot:run
   ```

### Configuration

Configure the database in `application.yml`:

```yaml
customdb:
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
```

## Usage Examples

### Basic Operations

```java
// Create a table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL,
    email STRING UNIQUE,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

// Insert data
INSERT INTO users (id, name, email, age) VALUES (1, 'John Doe', 'john@example.com', 30);

// Query data
SELECT * FROM users WHERE age > 25;

// Update data
UPDATE users SET age = 31 WHERE id = 1;

// Delete data
DELETE FROM users WHERE id = 1;
```

### Advanced Queries

```java
// JOIN operations
SELECT u.name, o.total 
FROM users u 
INNER JOIN orders o ON u.id = o.user_id 
WHERE o.total > 100;

// Transactions
BEGIN TRANSACTION;
INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane@example.com');
INSERT INTO orders (id, user_id, total) VALUES (1, 2, 150.00);
COMMIT;
```

## API Documentation

### REST Endpoints

- `POST /api/query` - Execute SQL queries
- `GET /api/tables` - List all tables
- `POST /api/tables/{tableName}` - Create table
- `DELETE /api/tables/{tableName}` - Drop table
- `GET /api/stats` - Get database statistics

### Query Parameters

- `includeExecutionPlan` - Include query execution plan in response
- `useCache` - Enable/disable query caching
- `timeout` - Query timeout in seconds


## Support

- 📧 Email: sumitkumariiita@gmail.com
- 📝 Issues: [GitHub Issues](https://github.com/Sumit-kumar99/custom-database-engine/issues)
- 📖 Documentation: [Wiki](https://github.com/Sumit-kumar99/custom-database-engine/wiki)


⭐ **Star this repository if you find it useful!**
