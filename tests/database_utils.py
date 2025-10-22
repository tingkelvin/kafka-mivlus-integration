#!/usr/bin/env python3
"""
Database Utilities for Milvus Operations
Centralized database operations for reusability across test suites
"""

import time
import numpy as np
import hashlib
import logging
import subprocess
import requests
from typing import List, Dict, Optional, Tuple, Any
from pymilvus import MilvusClient, DataType, Collection, connections

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Centralized database operations manager for Milvus"""
    
    def __init__(self, uri: str = "http://localhost:19530", database_name: str = "test_db", ensure_docker_running: bool = True):
        """Initialize database manager"""
        self.uri = uri
        self.database_name = database_name
        self.client = None
        
        if ensure_docker_running:
            self._ensure_docker_containers_running()
        
        self._connect()
        self._ensure_database()
    
    def _ensure_docker_containers_running(self):
        """Ensure all required Docker containers are running"""
        required_containers = [
            'milvus-etcd',
            'milvus-minio', 
            'milvus-kafka',
            'milvus-zookeeper',
            'milvus-rootcoord',
            'milvus-datacoord',
            'milvus-indexcoord',
            'milvus-querycoord',
            'milvus-proxy',
            'milvus-datanode1',
            'milvus-datanode2',
            'milvus-indexnode1',
            'milvus-indexnode2',
            'milvus-querynode1',
            'milvus-querynode2'
        ]
        
        logger.info("🔍 Checking Docker container status...")
        
        # Get running containers
        try:
            result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], 
                                  capture_output=True, text=True, check=True)
            running_containers = set(result.stdout.strip().split('\n'))
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to check Docker containers: {e}")
            raise
        
        # Check which containers need to be started
        stopped_containers = []
        for container in required_containers:
            if container not in running_containers:
                stopped_containers.append(container)
        
        if stopped_containers:
            logger.warning(f"⚠️ Found {len(stopped_containers)} stopped containers: {stopped_containers}")
            logger.info("🔄 Starting stopped containers...")
            
            # Start stopped containers
            for container in stopped_containers:
                try:
                    logger.info(f"   Starting {container}...")
                    result = subprocess.run(['docker', 'start', container], 
                                          capture_output=True, text=True, check=True)
                    logger.info(f"   ✅ {container} started")
                except subprocess.CalledProcessError as e:
                    logger.error(f"   ❌ Failed to start {container}: {e}")
                    raise
            
            # Wait for containers to be ready
            logger.info("⏱️ Waiting for containers to be ready...")
            time.sleep(10)
            
            # Verify containers are running
            try:
                result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], 
                                      capture_output=True, text=True, check=True)
                running_containers = set(result.stdout.strip().split('\n'))
                
                still_stopped = [c for c in stopped_containers if c not in running_containers]
                if still_stopped:
                    logger.error(f"❌ Some containers failed to start: {still_stopped}")
                    raise Exception(f"Failed to start containers: {still_stopped}")
                
                logger.info("✅ All required containers are now running")
                
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Failed to verify container status: {e}")
                raise
        else:
            logger.info("✅ All required containers are already running")
        
        # Wait for Milvus to be ready
        self._wait_for_milvus_ready()
    
    def _wait_for_milvus_ready(self, max_attempts: int = 30, delay: int = 5):
        """Wait for Milvus to be ready to accept connections"""
        logger.info("⏱️ Waiting for Milvus to be ready...")
        
        for attempt in range(max_attempts):
            try:
                # Try to connect to Milvus
                test_client = MilvusClient(uri=self.uri, timeout=5)
                databases = test_client.list_databases()
                logger.info("✅ Milvus is ready!")
                return True
            except Exception as e:
                if attempt < max_attempts - 1:
                    logger.info(f"   Attempt {attempt + 1}/{max_attempts}: Milvus not ready yet, waiting {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"❌ Milvus failed to become ready after {max_attempts} attempts: {e}")
                    raise
        
    def _connect(self):
        """Connect to Milvus"""
        try:
            # Configure client with reasonable timeout settings
            self.client = MilvusClient(uri=self.uri, timeout=30)
            logger.info(f"Connected to Milvus at {self.uri} with 30s timeout")
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    def _ensure_database(self):
        """Ensure database exists and is selected"""
        try:
            self.client.using_database(self.database_name)
            logger.info(f"Using database: {self.database_name}")
        except Exception as e:
            logger.warning(f"Database {self.database_name} may not exist: {e}")
    
    def create_collection(self, collection_name: str, schema_config: Dict = None, 
                         index_config: Dict = None, replica_number: int = None) -> bool:
        """
        Create a collection with specified schema and index configuration
        
        Args:
            collection_name: Name of the collection
            schema_config: Schema configuration dict
            index_config: Index configuration dict  
            replica_number: Number of replicas (optional)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Drop collection if exists
            if self.client.has_collection(collection_name):
                logger.info(f"Dropping existing collection: {collection_name}")
                self.client.drop_collection(collection_name)
                time.sleep(2)
            
            # Default schema configuration
            if schema_config is None:
                schema_config = {
                    'auto_id': False,
                    'enable_dynamic_field': True,
                    'fields': [
                        {'name': 'id', 'type': DataType.VARCHAR, 'max_length': 100, 'is_primary': True},
                        {'name': 'vector', 'type': DataType.FLOAT_VECTOR, 'dim': 2048},
                        {'name': 'label', 'type': DataType.INT64},
                        {'name': 'timestamp', 'type': DataType.DOUBLE}
                    ]
                }
            
            # Create schema
            schema = self.client.create_schema(
                auto_id=schema_config.get('auto_id', False),
                enable_dynamic_field=schema_config.get('enable_dynamic_field', True)
            )
            
            # Add fields to schema
            for field_config in schema_config.get('fields', []):
                # Extract parameters with defaults
                field_name = field_config['name']
                datatype = field_config['type']
                max_length = field_config.get('max_length', None)
                is_primary = field_config.get('is_primary', False)
                dim = field_config.get('dim', None)
                
                # Add field with positional arguments
                if max_length is not None:
                    schema.add_field(field_name, datatype, max_length=max_length, is_primary=is_primary)
                elif dim is not None:
                    schema.add_field(field_name, datatype, dim=dim)
                else:
                    schema.add_field(field_name, datatype)
            
            # Default index configuration
            if index_config is None:
                index_config = {
                    'field_name': 'vector',
                    'index_type': 'IVF_FLAT',
                    'metric_type': 'L2',
                    'params': {'nlist': 1024}
                }
            
            # Create index parameters
            index_params = self.client.prepare_index_params()
            index_params.add_index(
                field_name=index_config['field_name'],
                index_type=index_config['index_type'],
                metric_type=index_config['metric_type'],
                params=index_config['params']
            )
            
            # Create collection
            self.client.create_collection(
                collection_name=collection_name,
                schema=schema,
                index_params=index_params
            )
            
            # Load collection
            self.client.load_collection(collection_name)
            
            # Set replica number if specified
            if replica_number:
                try:
                    self.client.load_collection(collection_name, replica_number=replica_number)
                    logger.info(f"Collection loaded with replica_number={replica_number}")
                except Exception as e:
                    logger.warning(f"Failed to set replica_number: {e}")
            
            time.sleep(3)  # Wait for collection to be ready
            logger.info(f"✅ Collection '{collection_name}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create collection: {e}")
            return False
    
    def create_consistency_collection(self, collection_name: str = "consistency_test") -> bool:
        """Create collection specifically for consistency testing"""
        schema_config = {
            'auto_id': False,
            'enable_dynamic_field': True,
            'fields': [
                {'name': 'id', 'type': DataType.VARCHAR, 'max_length': 100, 'is_primary': True},
                {'name': 'vector', 'type': DataType.FLOAT_VECTOR, 'dim': 2048},
                {'name': 'label', 'type': DataType.INT64},
                {'name': 'timestamp', 'type': DataType.DOUBLE},
                {'name': 'checksum', 'type': DataType.VARCHAR, 'max_length': 100}
            ]
        }
        
        index_config = {
            'field_name': 'vector',
            'index_type': 'IVF_FLAT',
            'metric_type': 'L2',
            'params': {'nlist': 1024}
        }
        
        return self.create_collection(collection_name, schema_config, index_config)
    
    def create_performance_collection(self, collection_name: str = "perf_test") -> bool:
        """Create collection specifically for performance testing"""
        return self.create_collection(collection_name)
    
    def create_chaos_collection(self, collection_name: str = "chaos_test") -> bool:
        """Create collection specifically for chaos engineering testing"""
        return self.create_collection(collection_name)
    
    def create_reid_collection(self, collection_name: str = "test_collection") -> bool:
        """Create collection specifically for ReID testing"""
        schema_config = {
            'auto_id': False,
            'enable_dynamic_field': True,
            'fields': [
                {'name': 'detection_uuid', 'type': DataType.VARCHAR, 'max_length': 100, 'is_primary': True},
                {'name': 'reid_matrix', 'type': DataType.FLOAT_VECTOR, 'dim': 2048},
                {'name': 'reid', 'type': DataType.INT64},
                {'name': 'source_id', 'type': DataType.VARCHAR, 'max_length': 50},
                {'name': 'timestamp', 'type': DataType.DOUBLE}
            ]
        }
        
        index_config = {
            'field_name': 'reid_matrix',
            'index_type': 'IVF_FLAT',
            'metric_type': 'L2',
            'params': {'nlist': 1024}
        }
        
        return self.create_collection(collection_name, schema_config, index_config)
    
    def insert_data(self, collection_name: str, data: List[Dict]) -> bool:
        """
        Insert data into collection
        
        Args:
            collection_name: Name of the collection
            data: List of dictionaries containing data to insert
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not data:
            return True
            
        try:
            self.client.insert(collection_name=collection_name, data=data)
            logger.debug(f"✅ Inserted {len(data)} records into {collection_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Insert failed: {e}")
            return False
    
    def insert_batch_data(self, collection_name: str, data: List[Dict], batch_size: int = 100) -> bool:
        """
        Insert data in batches
        
        Args:
            collection_name: Name of the collection
            data: List of dictionaries containing data to insert
            batch_size: Size of each batch
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                if not self.insert_data(collection_name, batch):
                    return False
                time.sleep(0.1)  # Small delay between batches
            return True
        except Exception as e:
            logger.error(f"❌ Batch insert failed: {e}")
            return False
    
    def search_vectors(self, collection_name: str, query_vectors: List[List[float]], 
                      limit: int = 10, filter_expr: Optional[str] = None,
                      output_fields: Optional[List[str]] = None) -> List[Dict]:
        """
        Search for similar vectors
        
        Args:
            collection_name: Name of the collection
            query_vectors: List of query vectors
            limit: Maximum number of results per query
            filter_expr: Filter expression (optional)
            output_fields: Fields to return (optional)
        
        Returns:
            List of search results
        """
        try:
            if output_fields is None:
                output_fields = ["id", "label", "timestamp"]
            
            results = self.client.search(
                collection_name=collection_name,
                data=query_vectors,
                filter=filter_expr or "",
                limit=limit,
                output_fields=output_fields
            )
            
            matches = []
            for hits in results:
                for hit in hits:
                    # Extract entity data
                    entity = hit.get('entity', {})
                    
                    # Build match dictionary with all requested output fields
                    match = {'similarity': hit.get('distance', 0.0)}
                    
                    # Add all requested output fields
                    for field in output_fields:
                        if field in entity:
                            match[field] = entity[field]
                        elif field in hit:
                            match[field] = hit[field]
                    
                    matches.append(match)
            
            return matches
            
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []
    
    def query_data(self, collection_name: str, filter_expr: str = "", 
                   output_fields: Optional[List[str]] = None, limit: int = 1000) -> List[Dict]:
        """
        Query data from collection
        
        Args:
            collection_name: Name of the collection
            filter_expr: Filter expression
            output_fields: Fields to return (optional)
            limit: Maximum number of results
        
        Returns:
            List of query results
        """
        try:
            if output_fields is None:
                output_fields = ["id", "label", "timestamp"]
            
            results = self.client.query(
                collection_name=collection_name,
                filter=filter_expr,
                output_fields=output_fields,
                limit=limit
            )
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            return []
    
    def get_collection_stats(self, collection_name: str) -> Dict:
        """
        Get collection statistics
        
        Args:
            collection_name: Name of the collection
        
        Returns:
            Dictionary containing collection statistics
        """
        try:
            stats = self.client.get_collection_stats(collection_name)
            return stats
        except Exception as e:
            logger.error(f"❌ Failed to get collection stats: {e}")
            return {}
    
    def calculate_checksum(self, data: Dict) -> str:
        """
        Calculate checksum for data integrity verification
        
        Args:
            data: Dictionary containing data to checksum
        
        Returns:
            MD5 checksum string
        """
        content = f"{data.get('id', '')}_{data.get('label', '')}_{data.get('timestamp', '')}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def verify_data_integrity(self, collection_name: str, expected_data: List[Dict]) -> Tuple[int, int]:
        """
        Verify data integrity by comparing checksums
        
        Args:
            collection_name: Name of the collection
            expected_data: List of expected data with checksums
        
        Returns:
            Tuple of (total_records, integrity_errors)
        """
        try:
            # Query all data
            all_data = self.query_data(collection_name, output_fields=["id", "label", "timestamp", "checksum"])
            
            integrity_errors = 0
            for record in all_data:
                # Recalculate checksum
                expected_checksum = self.calculate_checksum(record)
                actual_checksum = record.get('checksum', '')
                
                if expected_checksum != actual_checksum:
                    integrity_errors += 1
                    logger.warning(f"Checksum mismatch for {record['id']}")
            
            return len(all_data), integrity_errors
            
        except Exception as e:
            logger.error(f"❌ Data integrity verification failed: {e}")
            return 0, 0
    
    def generate_test_data(self, num_records: int, vector_dim: int = 2048, 
                          prefix: str = "test") -> List[Dict]:
        """
        Generate test data for testing purposes
        
        Args:
            num_records: Number of records to generate
            vector_dim: Dimension of vectors
            prefix: Prefix for record IDs
        
        Returns:
            List of test data dictionaries
        """
        test_data = []
        for i in range(num_records):
            data = {
                "id": f"{prefix}_{i}",
                "vector": np.random.rand(vector_dim).tolist(),
                "label": i,
                "timestamp": time.time()
            }
            test_data.append(data)
        
        return test_data
    
    def generate_reid_test_data(self, num_records: int, vector_dim: int = 2048) -> List[Dict]:
        """
        Generate ReID test data
        
        Args:
            num_records: Number of records to generate
            vector_dim: Dimension of vectors
        
        Returns:
            List of ReID test data dictionaries
        """
        test_data = []
        for i in range(num_records):
            data = {
                "detection_uuid": f"test_reid_{i}",
                "reid_matrix": np.random.rand(vector_dim).tolist(),
                "reid": i,
                "source_id": f"camera{i % 3 + 1}",
                "timestamp": time.time()
            }
            test_data.append(data)
        
        return test_data
    
    def generate_consistency_test_data(self, num_records: int, vector_dim: int = 2048, prefix: str = "consistency_test") -> List[Dict]:
        """
        Generate test data with checksums for consistency testing
        
        Args:
            num_records: Number of records to generate
            vector_dim: Dimension of vectors
            prefix: Prefix for record IDs
        
        Returns:
            List of test data dictionaries with checksums
        """
        # Use a fixed timestamp for all records to ensure checksum consistency
        fixed_timestamp = time.time()
        
        test_data = []
        for i in range(num_records):
            data = {
                "id": f"{prefix}_{i}",
                "vector": np.random.rand(vector_dim).tolist(),
                "label": i,
                "timestamp": fixed_timestamp,
                "checksum": ""
            }
            data["checksum"] = self.calculate_checksum(data)
            test_data.append(data)
        
        return test_data
    
    def drop_collection(self, collection_name: str) -> bool:
        """
        Drop a collection
        
        Args:
            collection_name: Name of the collection to drop
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.client.has_collection(collection_name):
                self.client.drop_collection(collection_name)
                logger.info(f"✅ Dropped collection: {collection_name}")
                time.sleep(2)  # Wait for cleanup
                return True
            else:
                logger.info(f"Collection {collection_name} does not exist")
                return True
        except Exception as e:
            logger.error(f"❌ Failed to drop collection: {e}")
            return False
    
    def cleanup_all_collections(self) -> bool:
        """Clean up all test collections"""
        collections_to_clean = [
            "consistency_test", "perf_test", "chaos_test", 
            "test_collection", "reid_test"
        ]
        
        success = True
        for collection_name in collections_to_clean:
            if not self.drop_collection(collection_name):
                success = False
        
        return success
    
    def get_replica_info(self, collection_name: str) -> Dict:
        """
        Get replica information for a collection
        
        Args:
            collection_name: Name of the collection
        
        Returns:
            Dictionary containing replica information
        """
        try:
            # Use pymilvus Collection for replica info
            connections.connect(alias='replica_check', host='localhost', port='19530')
            connections.get_connection(alias='replica_check').set_database(self.database_name)
            
            collection = Collection(collection_name, using='replica_check')
            replicas = collection.get_replicas()
            
            connections.disconnect(alias='replica_check')
            
            return {
                'num_groups': len(replicas.groups),
                'groups': replicas.groups
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get replica info: {e}")
            return {'num_groups': 0, 'groups': []}


# Convenience functions for common operations
def create_test_database(uri: str = "http://localhost:19530", database_name: str = "test_db") -> DatabaseManager:
    """Create and return a database manager instance"""
    return DatabaseManager(uri, database_name)

def quick_search_test(collection_name: str = "test_collection", 
                     query_vector: Optional[List[float]] = None) -> bool:
    """
    Quick search test for basic functionality verification
    
    Args:
        collection_name: Name of the collection to search
        query_vector: Query vector (optional, generates random if not provided)
    
    Returns:
        bool: True if search successful, False otherwise
    """
    try:
        db_manager = DatabaseManager()
        
        if query_vector is None:
            query_vector = [0.1] * 2048
        
        results = db_manager.search_vectors(collection_name, [query_vector], limit=5)
        
        if results:
            logger.info(f"✅ Quick search successful: {len(results)} results")
            return True
        else:
            logger.warning("⚠️ Quick search returned no results")
            return False
            
    except Exception as e:
        logger.error(f"❌ Quick search test failed: {e}")
        return False

def quick_query_test(collection_name: str = "test_collection") -> bool:
    """
    Quick query test for basic functionality verification
    
    Args:
        collection_name: Name of the collection to query
    
    Returns:
        bool: True if query successful, False otherwise
    """
    try:
        db_manager = DatabaseManager()
        results = db_manager.query_data(collection_name, limit=10)
        
        if results is not None:
            logger.info(f"✅ Quick query successful: {len(results)} results")
            return True
        else:
            logger.warning("⚠️ Quick query returned no results")
            return False
            
    except Exception as e:
        logger.error(f"❌ Quick query test failed: {e}")
        return False

def check_docker_status() -> Dict[str, bool]:
    """
    Check the status of all required Docker containers
    
    Returns:
        Dictionary with container names as keys and running status as values
    """
    required_containers = [
        'milvus-etcd',
        'milvus-minio', 
        'milvus-kafka',
        'milvus-zookeeper',
        'milvus-rootcoord',
        'milvus-datacoord',
        'milvus-indexcoord',
        'milvus-querycoord',
        'milvus-proxy',
        'milvus-datanode1',
        'milvus-datanode2',
        'milvus-indexnode1',
        'milvus-indexnode2',
        'milvus-querynode1',
        'milvus-querynode2'
    ]
    
    try:
        result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], 
                              capture_output=True, text=True, check=True)
        running_containers = set(result.stdout.strip().split('\n'))
        
        status = {}
        for container in required_containers:
            status[container] = container in running_containers
        
        return status
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to check Docker containers: {e}")
        return {container: False for container in required_containers}

def ensure_all_containers_running() -> bool:
    """
    Ensure all required Docker containers are running
    
    Returns:
        bool: True if all containers are running, False otherwise
    """
    try:
        db_manager = DatabaseManager(ensure_docker_running=True)
        logger.info("✅ All containers are running and Milvus is ready")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to ensure containers are running: {e}")
        return False


if __name__ == "__main__":
    # Test the database utilities
    print("Testing Database Utilities...")
    
    db_manager = DatabaseManager()
    
    # Test collection creation
    print("\n1. Testing collection creation...")
    success = db_manager.create_collection("test_utils_collection")
    print(f"Collection creation: {'✅ Success' if success else '❌ Failed'}")
    
    # Test data generation and insertion
    print("\n2. Testing data generation and insertion...")
    test_data = db_manager.generate_test_data(5)
    success = db_manager.insert_data("test_utils_collection", test_data)
    print(f"Data insertion: {'✅ Success' if success else '❌ Failed'}")
    
    # Test search
    print("\n3. Testing search...")
    results = db_manager.search_vectors("test_utils_collection", [[0.1] * 2048], limit=5)
    print(f"Search: {'✅ Success' if results else '❌ Failed'} ({len(results)} results)")
    
    # Test query
    print("\n4. Testing query...")
    results = db_manager.query_data("test_utils_collection", limit=10)
    print(f"Query: {'✅ Success' if results else '❌ Failed'} ({len(results)} results)")
    
    # Test collection stats
    print("\n5. Testing collection stats...")
    stats = db_manager.get_collection_stats("test_utils_collection")
    print(f"Stats: {'✅ Success' if stats else '❌ Failed'}")
    
    # Cleanup
    print("\n6. Cleaning up...")
    success = db_manager.drop_collection("test_utils_collection")
    print(f"Cleanup: {'✅ Success' if success else '❌ Failed'}")
    
    print("\nDatabase utilities test completed!")
