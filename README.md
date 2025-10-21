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
```

### Test Features

#### Replication Test (`test_replication.py`)
- Database creation with REPLICA=2 default
- Collection creation and indexing
- Vector insertion and search
- Replica factor verification
- Automatic fallback to collection-level replicas

#### Bidirectional Failover Test (`bidirectional_test.py`)
- **Comprehensive failover testing**: Tests system behavior with individual node failures
- **Database cleanup and setup**: Automatically prepares test data (10 records)
- **Bidirectional testing**: Tests failover in both directions (querynode1 → querynode2)
- **Search functionality verification**: Ensures search works with nodes down
- **Expected failure validation**: Confirms system fails when both nodes are down
- **Full recovery testing**: Verifies complete system recovery after restart
- **Timeout protection**: Prevents hanging operations with configurable timeouts
- **Docker integration**: Uses Docker utilities for container management

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

**Bidirectional test issues:**
```bash
# Check container status before running test
python3 tests/docker_utils.py

# Ensure all containers are running
docker-compose ps

# If test hangs, check for timeout issues
# The test has 60-second timeout protection
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
