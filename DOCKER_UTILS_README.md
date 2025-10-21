# Docker Utilities for Milvus Testing

A comprehensive set of utilities for managing Docker containers in Milvus testing scenarios.

## 📁 Files

- `docker_utils.py` - Main utilities module
- `test_with_docker_utils.py` - Example test using the utilities
- `example_usage.py` - Usage examples and patterns

## 🚀 Quick Start

```python
from docker_utils import DockerManager, quick_status_check

# Quick status check
quick_status_check()

# Initialize manager
docker_manager = DockerManager()

# Check if container is running
if docker_manager.is_container_running("milvus-proxy"):
    print("Proxy is running!")
```

## 🔧 Main Features

### Container Management
- **Start/Stop/Restart** containers with verification
- **Status checking** with detailed information
- **Health monitoring** for all Milvus components
- **Log retrieval** with filtering capabilities

### Verification Functions
- **Verify containers are actually stopped**
- **Verify containers are actually running**
- **Wait for containers to be healthy**
- **Monitor container status changes**

### Convenience Functions
- **Quick status checks** for all containers
- **Bulk operations** (stop/start multiple containers)
- **Filtered log retrieval** for debugging
- **Health status monitoring**

## 📊 Usage Examples

### Basic Container Operations

```python
from docker_utils import DockerManager

docker_manager = DockerManager()

# Check container status
status = docker_manager.get_container_status("milvus-proxy")
print(f"Status: {status['status']}, Health: {status['health']}")

# Stop container with verification
if docker_manager.stop_container("milvus-querynode1"):
    print("Container stopped successfully")

# Start container with verification
if docker_manager.start_container("milvus-querynode1"):
    print("Container started successfully")
```

### Status Monitoring

```python
# Get all container statuses
all_status = docker_manager.get_all_containers_status()

# Get specific node types
query_status = docker_manager.get_query_nodes_status()
data_status = docker_manager.get_data_nodes_status()

# Print formatted table
docker_manager.print_container_status_table()
```

### Log Management

```python
# Get recent logs
logs = docker_manager.get_container_logs("milvus-proxy", tail=20)

# Get filtered logs
filtered_logs = docker_manager.get_container_logs_filtered(
    "milvus-querynode1",
    keywords=['sync', 'segment', 'collection'],
    tail=50
)
```

### Health Monitoring

```python
# Wait for containers to be healthy
critical_containers = ['milvus-proxy', 'milvus-rootcoord', 'milvus-etcd']
if docker_manager.wait_for_containers_healthy(critical_containers, timeout=120):
    print("All containers are healthy!")
```

## 🧪 Testing Scenarios

### Failover Testing

```python
def test_failover():
    docker_manager = DockerManager()
    
    # Stop one query node
    if docker_manager.stop_container("milvus-querynode1"):
        print("Query node 1 stopped")
        
        # Test system with one node down
        # ... your test logic here ...
        
        # Restart the node
        if docker_manager.start_container("milvus-querynode1"):
            print("Query node 1 restarted")
```

### Health Checks

```python
def check_system_health():
    docker_manager = DockerManager()
    
    # Check critical containers
    critical = ['milvus-proxy', 'milvus-rootcoord', 'milvus-etcd']
    
    for container in critical:
        if not docker_manager.is_container_running(container):
            print(f"❌ {container} is down!")
            return False
        else:
            print(f"✅ {container} is running")
    
    return True
```

### Log Analysis

```python
def analyze_logs():
    docker_manager = DockerManager()
    
    # Get sync-related logs from query nodes
    for node in ['milvus-querynode1', 'milvus-querynode2']:
        logs = docker_manager.get_container_logs_filtered(
            node,
            keywords=['sync', 'segment', 'version'],
            tail=100
        )
        
        print(f"\n{node} sync activity:")
        for log in logs[-10:]:  # Last 10 sync logs
            print(f"  {log}")
```

## 🛠️ Available Methods

### DockerManager Class

#### Container Operations
- `stop_container(container_name, timeout=30)` - Stop container with verification
- `start_container(container_name, timeout=30)` - Start container with verification  
- `restart_container(container_name, timeout=30)` - Restart container
- `cleanup_containers(containers)` - Stop and remove containers

#### Status Checking
- `get_container_status(container_name)` - Get detailed container status
- `get_all_containers_status()` - Get status of all Milvus containers
- `is_container_running(container_name)` - Check if container is running
- `is_container_stopped(container_name)` - Check if container is stopped

#### Verification
- `verify_container_stopped(container_name, max_attempts=10)` - Verify container is stopped
- `verify_container_running(container_name, max_attempts=15)` - Verify container is running
- `wait_for_containers_healthy(containers, timeout=120)` - Wait for containers to be healthy

#### Log Management
- `get_container_logs(container_name, tail=20)` - Get recent logs
- `get_container_logs_filtered(container_name, keywords, tail=50)` - Get filtered logs

#### Status Display
- `print_container_status_table(containers=None)` - Print formatted status table
- `get_query_nodes_status()` - Get query node statuses
- `get_data_nodes_status()` - Get data node statuses

### Convenience Functions

- `quick_status_check()` - Quick status check of all containers
- `stop_query_nodes()` - Stop both query nodes
- `start_query_nodes()` - Start both query nodes
- `get_milvus_containers()` - Get list of all Milvus container names

## 🎯 Best Practices

### 1. Always Verify Operations
```python
# Good: Verify the operation succeeded
if docker_manager.stop_container("milvus-querynode1"):
    print("Container stopped successfully")
else:
    print("Failed to stop container")

# Bad: Don't assume operations succeed
docker_manager.stop_container("milvus-querynode1")
print("Container stopped")  # Might not be true!
```

### 2. Use Appropriate Timeouts
```python
# For critical operations, use longer timeouts
if docker_manager.start_container("milvus-proxy", timeout=60):
    print("Proxy started")
```

### 3. Check Health Status
```python
# Wait for containers to be healthy after operations
critical_containers = ['milvus-proxy', 'milvus-rootcoord']
if docker_manager.wait_for_containers_healthy(critical_containers):
    print("System is healthy")
```

### 4. Use Filtered Logs for Debugging
```python
# Get relevant logs for debugging
logs = docker_manager.get_container_logs_filtered(
    "milvus-querynode1",
    keywords=['error', 'fail', 'exception'],
    tail=100
)
```

## 🔍 Troubleshooting

### Common Issues

1. **Container not stopping**: Use longer timeout or force stop
2. **Container not starting**: Check if it's already running
3. **Health check failing**: Wait longer or check logs
4. **Logs not found**: Check container name or increase tail size

### Debug Commands

```python
# Check container status
status = docker_manager.get_container_status("container_name")
print(f"Status: {status}")

# Get recent logs
logs = docker_manager.get_container_logs("container_name", tail=50)
for log in logs:
    print(log)

# Check if container is actually running
if docker_manager.is_container_running("container_name"):
    print("Container is running")
else:
    print("Container is not running")
```

## 📝 Example Scripts

See the following files for complete examples:
- `test_with_docker_utils.py` - Complete test using utilities
- `example_usage.py` - Various usage patterns
- `verify_stop_test.py` - Updated to use Docker utilities

## 🚀 Integration

To use in your own scripts:

```python
#!/usr/bin/env python3
from docker_utils import DockerManager, quick_status_check

def my_test():
    docker_manager = DockerManager()
    
    # Your test logic here
    if docker_manager.stop_container("milvus-querynode1"):
        # Test with one node down
        pass
    
    if docker_manager.start_container("milvus-querynode1"):
        # Test recovery
        pass

if __name__ == "__main__":
    my_test()
```

This utilities module provides a robust foundation for all Docker-related operations in your Milvus testing workflows! 🎉
