# Kafka-Milvus Integration

A production-ready distributed Milvus vector database setup with Kafka message queue integration for automatic catch-up and fault tolerance. This project provides a complete Docker Compose stack for running Milvus in distributed mode with horizontal scaling capabilities.

## 🏗️ Architecture

This project implements a distributed Milvus architecture with the following components:

### Core Infrastructure
- **Kafka + Zookeeper**: Message queue system for automatic catch-up and fault tolerance
- **etcd**: Metadata storage and coordination
- **MinIO**: Object storage for vector data
- **Milvus Distributed**: Multi-node vector database with automatic replication

### Milvus Components
- **4 Coordinators**: RootCoord, QueryCoord, DataCoord, IndexCoord
- **Multiple Workers**: Proxy, QueryNode, DataNode, IndexNode (scalable)
- **Database-level Replicas**: Automatic REPLICA=2 for all collections

## ✨ Features

- ✅ **Automatic Catch-up**: Nodes automatically sync when recovering from failures
- ✅ **Horizontal Scaling**: Scale QueryNodes, DataNodes, and IndexNodes independently
- ✅ **Fault Tolerance**: No manual sync needed - Kafka handles message persistence
- ✅ **Database-level Replicas**: All collections automatically get REPLICA=2
- ✅ **Production Ready**: Optimized configuration for DetectionSuite workloads
- ✅ **Web UI**: Attu interface for database management
- ✅ **Health Checks**: All services include health monitoring
- ✅ **Robust Timeout Handling**: Thread-safe timeout protection prevents hanging operations
- ✅ **Data Integrity**: Prevents data collision and ensures proper test isolation
- ✅ **Kafka Message Size Optimization**: Configured for large vector operations

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 8GB RAM (16GB recommended)
- Python 3.8+ (for testing)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd kafka-mivlus-integration
   ```

2. **Start the distributed Milvus stack**
   ```bash
   docker-compose up -d
   ```

3. **Wait for all services to be healthy**
   ```bash
   docker-compose ps
   ```

4. **Verify Milvus is running**
   ```bash
   curl http://localhost:9091/healthz
   ```

### Access Points

- **Milvus gRPC**: `localhost:19530`
- **Milvus HTTP/Metrics**: `localhost:9091`
- **Attu Web UI**: `http://localhost:8000`
- **MinIO Console**: `http://localhost:9001` (admin/admin)
- **Kafka**: `localhost:9093`

## 🧪 Testing

The project includes comprehensive test suites for database-level replica configuration and bidirectional failover testing:

```bash
# Install Python dependencies
pip install pymilvus numpy

# Run the replication test
python3 tests/test_replication.py

# Run the bidirectional failover test
python3 tests/bidirectional_test.py

# Run all test suites
python3 tests/test_runner.py

# Run specific test suites
python3 tests/test_runner.py --suites replication_test bidirectional_test

# Run quick essential tests
python3 tests/test_runner.py --quick
```

### Test Features

#### Replication Test (`test_replication.py`)
- Database creation with REPLICA=2 default
- Collection creation and indexing
- Vector insertion and search
- Replica factor verification
- Automatic fallback to collection-level replicas

#### Comprehensive Failover Test (`failover_test.py`)
- **Comprehensive failover testing**: Tests system behavior with individual node failures
- **Database cleanup and setup**: Automatically prepares test data (20 records)
- **Bidirectional testing**: Tests failover in both directions (querynode1 → querynode2)
- **Search functionality verification**: Ensures search works with nodes down
- **Expected failure validation**: Confirms system fails when both nodes are down
- **Full recovery testing**: Verifies complete system recovery after restart
- **Thread-safe timeout protection**: Uses ThreadPoolExecutor for reliable timeout handling
- **Data collision prevention**: Unique prefixes prevent data overwrites during testing
- **Docker integration**: Uses Docker utilities for container management

#### Performance Test (`performance_test.py`)
- **Insert performance**: Tests concurrent insert operations (1000+ vectors/sec)
- **Search performance**: Tests search latency under load
- **Memory usage monitoring**: Tracks memory consumption during operations
- **Concurrent operations**: Tests multi-threaded insert and search operations
- **System metrics**: Monitors CPU, memory, disk, and network usage
- **Resource utilization**: Validates system resource efficiency
- **Optimized batch sizes**: Reduced batch size (20 records) to prevent Kafka message size errors
- **Kafka message size handling**: Configured for large vector operations

#### Chaos Engineering Test (`chaos_engineering_test.py`)
- **Random container restarts**: Tests resilience during random failures
- **Cascading failures**: Tests system behavior with multiple node failures
- **Resource exhaustion**: Tests system under high memory/CPU usage
- **Network partition**: Tests behavior during network connectivity issues
- **Clock skew simulation**: Tests system with timestamp inconsistencies
- **Failure recovery**: Validates automatic recovery mechanisms

#### Consistency Test (`consistency_test.py`)
- **Data integrity**: Verifies data integrity during insert and search operations
- **Replica consistency**: Tests consistency across replica nodes
- **Concurrent consistency**: Tests data consistency under concurrent operations
- **Transaction atomicity**: Validates transaction rollback behavior
- **Data loss detection**: Tests data loss detection and recovery
- **Checksum verification**: Ensures data integrity with checksum validation

#### Comprehensive Test Runner (`test_runner.py`)
- **Unified test execution**: Runs all test suites with a single command
- **Detailed reporting**: Generates comprehensive test reports
- **Prerequisites checking**: Validates system readiness before testing
- **Selective testing**: Allows running specific test suites
- **Quick testing**: Essential tests for rapid validation
- **Report generation**: Saves detailed test results to files

### Bidirectional Test Details

The bidirectional test (`tests/bidirectional_test.py`) is a comprehensive failover testing suite that validates the system's resilience and recovery capabilities:

#### Test Sequence
1. **Database Setup**: Cleans existing data and inserts 10 test records
2. **QueryNode1 Down**: Stops first query node, tests search functionality
3. **QueryNode2 Down**: Restarts first node, stops second node, tests search
4. **Both Nodes Down**: Stops both query nodes, verifies expected failure
5. **Full Recovery**: Restarts all nodes, verifies complete system recovery

#### Key Features
- **Automatic Container Management**: Uses `DockerManager` class for reliable container control
- **Search Timeout Protection**: 60-second timeout prevents hanging operations
- **Comprehensive Status Reporting**: Detailed test results and container status
- **Expected Failure Validation**: Confirms system fails appropriately when all nodes are down
- **Recovery Verification**: Ensures full functionality after node restarts

#### Running the Test
```bash
# Run the complete bidirectional test
python3 tests/bidirectional_test.py

# The test will output:
# - Database setup and data insertion
# - Individual node failure testing
# - Search functionality verification
# - Expected failure confirmation
# - Full recovery validation
# - Comprehensive test summary
```

#### Test Output
The test provides detailed feedback including:
- ✅ **PASS**: Test step completed successfully
- ❌ **FAIL**: Test step failed
- 🎉 **COMPREHENSIVE TEST PASSED**: All tests successful
- ⚠️ **MOSTLY SUCCESSFUL**: Minor issues detected
- ❌ **COMPREHENSIVE TEST FAILED**: Significant issues found

## 📊 Performance Tuning

The configuration is optimized for DetectionSuite workloads:

### Memory Limits
- QueryNode: 8GB
- DataNode: 4GB  
- IndexNode: 4GB

### Buffer Configuration
- DataNode flush buffer: 16MB
- Sync period: 10 minutes
- Retention: 7 days

### Scaling Guidelines

**For Read Performance:**
```bash
# Scale QueryNodes
docker-compose up -d --scale querynode=4
```

**For Write Performance:**
```bash
# Scale DataNodes  
docker-compose up -d --scale datanode=4
```

**For Index Building:**
```bash
# Scale IndexNodes
docker-compose up -d --scale indexnode=4
```

## 🔧 Configuration

### Environment Variables

Key configuration options in `milvus-distributed.env`:

```bash
# Storage
MINIO_ACCESS_KEY_ID=minioadmin
MINIO_SECRET_ACCESS_KEY=minioadmin

# Performance
QUERYNODE_MEMORY_LIMIT=8192  # 8GB
DATANODE_MEMORY_LIMIT=4096   # 4GB

# Kafka Message Size Limits (prevents "Message size too large" errors)
KAFKA_MESSAGE_MAX_BYTES=10485760  # 10MB
KAFKA_REPLICA_FETCH_MAX_BYTES=10485760  # 10MB
KAFKA_SOCKET_SEND_BUFFER_BYTES=10485760  # 10MB
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=10485760  # 10MB

# Retention
RETENTION_DURATION=168  # 7 days
```

### Database Replicas

The system automatically configures REPLICA=2 for all collections:

```python
# Database-level replica configuration
client.create_database(
    db_name="test_db",
    properties={
        "database.replica.number": "2"  # Default for all collections
    }
)
```

## 📈 Monitoring

### Health Checks

All services include health monitoring:

```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f proxy

# Check Milvus health
curl http://localhost:9091/healthz
```

### Metrics

Access metrics at `http://localhost:9091/metrics` for Prometheus integration.

## 🛠️ Development

### Project Structure

```
kafka-mivlus-integration/
├── docker-compose.yml          # Main orchestration
├── milvus-distributed.env      # Environment configuration
├── tests/
│   ├── test_replication.py     # Replica testing
│   ├── bidirectional_test.py   # Bidirectional failover testing
│   ├── performance_test.py    # Performance and load testing
│   ├── chaos_engineering_test.py  # Chaos engineering tests
│   ├── consistency_test.py    # Data consistency testing
│   ├── test_runner.py         # Comprehensive test runner
│   └── docker_utils.py         # Docker container management utilities
└── README.md                   # This file
```

### Adding New Collections

```python
from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")

# Create schema
schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True)
schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2048)

# Create collection (inherits REPLICA=2)
client.create_collection(
    collection_name="my_collection",
    schema=schema
)
```

## 🚨 Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check logs
docker-compose logs [service-name]

# Restart specific service
docker-compose restart [service-name]
```

**Connection refused:**
```bash
# Wait for all services to be healthy
docker-compose ps
# All services should show "healthy" status
```

**Memory issues:**
```bash
# Reduce memory limits in milvus-distributed.env
QUERYNODE_MEMORY_LIMIT=4096  # Reduce from 8192
```

**"Message size too large" errors:**
```bash
# Restart containers after Kafka configuration changes
docker-compose down
docker-compose up -d

# Check Kafka message size limits in milvus-distributed.env
# Default is 10MB, increase if needed for larger batches
```

**Timeout issues in tests:**
```bash
# The failover test now uses ThreadPoolExecutor for reliable timeouts
# If tests still hang, check Milvus client timeout configuration
# Client timeout is set to 30 seconds in database_utils.py
```

**Data collision in tests:**
```bash
# Tests now use unique prefixes to prevent data overwrites
# failover_test uses "failover_record_" and "failover_recovery_record_" prefixes
# This ensures proper test isolation and data integrity
```

### Logs and Debugging

```bash
# View all logs
docker-compose logs

# Follow specific service logs
docker-compose logs -f proxy

# Check Milvus internal logs
docker-compose exec proxy cat /var/lib/milvus/logs/milvus.log
```

## 🔄 Backup and Recovery

### Data Persistence

All data is persisted in Docker volumes:
- Vector data: `minio_data`
- Metadata: `etcd_data`
- Messages: `kafka_data`

### Backup Strategy

```bash
# Backup volumes
docker run --rm -v kafka-mivlus-integration_minio_data:/data -v $(pwd):/backup alpine tar czf /backup/minio_backup.tar.gz -C /data .
```

## 🔄 Recent Improvements

### Version 2.1 Updates

**Enhanced Timeout Handling:**
- ✅ **Thread-safe timeouts**: Replaced signal-based timeouts with ThreadPoolExecutor
- ✅ **Client-level timeouts**: Added 30-second timeout to Milvus client connections
- ✅ **Reliable test execution**: Prevents hanging operations during failover testing

**Data Integrity Improvements:**
- ✅ **Collision prevention**: Unique prefixes prevent data overwrites during testing
- ✅ **Test isolation**: Each test phase uses distinct data identifiers
- ✅ **Proper verification**: Tests can accurately validate data persistence

**Kafka Message Size Optimization:**
- ✅ **Message size limits**: Configured 10MB limits to prevent "Message size too large" errors
- ✅ **Optimized batch sizes**: Reduced performance test batch size from 100 to 20 records
- ✅ **Large vector support**: Handles 2048-dimensional vectors without message size issues

**Configuration Enhancements:**
- ✅ **Kafka configuration**: Added comprehensive message size and buffer settings
- ✅ **Client timeout**: Milvus client now has proper timeout configuration
- ✅ **Performance tuning**: Optimized for large vector operations

## 📚 Additional Resources

- [Milvus Documentation](https://milvus.io/docs)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Compose Reference](https://docs.docker.com/compose/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with the provided test suite
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Note**: This setup is optimized for DetectionSuite workloads with ReID vectors (2048 dimensions). Adjust memory limits and scaling based on your specific use case.
