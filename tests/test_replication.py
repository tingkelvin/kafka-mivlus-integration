#!/usr/bin/env python3
"""
Milvus Distributed Manager with Database-Level Replicas

This version uses database-level replica configuration,
which automatically applies REPLICA=2 to all collections!

Simpler and more reliable than collection-level approach.

Run: python3 milvus_with_database_replicas.py
"""

import os
import time
import logging
from typing import List, Dict, Optional
from pymilvus import MilvusClient, DataType
from database_utils import DatabaseManager

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
MILVUS_URI = os.getenv("MILVUS_URI", "http://localhost:19530")
DATABASE_NAME = "test_db"
COLLECTION_NAME = "test_collection"
REID_DIM = 2048


class MilvusDistributedManagerV2:
    """
    Milvus manager using database-level replica configuration.
    All collections automatically get REPLICA=2!
    """
    
    def __init__(self, uri: str = MILVUS_URI):
        """Initialize with database-level replicas"""
        self.uri = uri
        # Ensure Docker containers are running before starting tests
        self.db_manager = DatabaseManager(uri, DATABASE_NAME, ensure_docker_running=True)
        self._ensure_collection()
        logger.info(f"✅ Connected to Milvus with DATABASE-LEVEL REPLICA=2")
    
    
    def _ensure_collection(self):
        """Create collection (will automatically use REPLICA=2 from database!)"""
        try:
            success = self.db_manager.create_reid_collection(COLLECTION_NAME)
            if success:
                logger.info(f"✅ Created collection (will inherit REPLICA=2 from database)")
            else:
                logger.error("Failed to create collection")
                raise Exception("Collection creation failed")
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
    
    def insert_reid(self, data: List[Dict]) -> bool:
        """Insert ReID data"""
        return self.db_manager.insert_data(COLLECTION_NAME, data)
    
    def search_reid(self, query_matrix: List[float], limit: int = 10, 
                    filter_expr: Optional[str] = None) -> List[Dict]:
        """Search for similar vectors"""
        return self.db_manager.search_vectors(
            collection_name=COLLECTION_NAME,
            query_vectors=[query_matrix],
            filter_expr=filter_expr,
            limit=limit,
            output_fields=["detection_uuid", "source_id", "timestamp", "reid"]
        )


if __name__ == "__main__":
    import numpy as np
    
    print("="*60)
    print("Testing Database-Level Replica Configuration")
    print("="*60)
    
    try:
        manager = MilvusDistributedManagerV2()
        
        # Insert test
        print("\nInserting test data...")
        test_data = [{
            "detection_uuid": f"db_test_{i}",
            "reid_matrix": np.random.rand(2048).tolist(),
            "reid": i,
            "source_id": f"camera{i % 3 + 1}",
            "timestamp": time.time()
        } for i in range(10)]
        
        manager.insert_reid(test_data)
        print("✅ Data inserted")
        
        # Search test
        print("\nSearching...")
        results = manager.search_reid(np.random.rand(2048).tolist(), limit=5)
        print(f"✅ Found {len(results)} results")
        
        # Check replica factor
        print("\nChecking replica factor...")
        from pymilvus import connections, Collection
        
        connections.connect(alias='check', host='localhost', port='19530')
        connections.get_connection(alias='check').set_database(DATABASE_NAME)
        
        collection = Collection(COLLECTION_NAME, using='check')
        replicas = collection.get_replicas()
        
        print(f"✅ Replica groups: {len(replicas.groups)}")
        
        if len(replicas.groups) >= 2:
            print("🎉 Database-level REPLICA=2 is working!")
        else:
            print("⚠️  May need manual load with replica_number=2")
        
        connections.disconnect(alias='check')
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nNote: Database-level replicas may not be supported in this Milvus version")
        print("Fall back to collection.load(replica_number=2) approach")
    
    print("\n" + "="*60)

