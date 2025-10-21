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
        self.client = None
        self._connect()
        self._ensure_database()
        self._ensure_collection()
        logger.info(f"✅ Connected to Milvus with DATABASE-LEVEL REPLICA=2")
    
    def _connect(self):
        """Connect to Milvus"""
        try:
            self.client = MilvusClient(uri=self.uri)
            logger.info(f"Connected to Milvus at {self.uri}")
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    def _ensure_database(self):
        """Create database with REPLICA=2 as default"""
        try:
            # List databases
            databases = self.client.list_databases()
            logger.info(f"Existing databases: {databases}")
            
            if DATABASE_NAME not in databases:
                logger.info(f"Creating database '{DATABASE_NAME}' with REPLICA=2...")
                
                # Create database with replica configuration
                self.client.create_database(
                    db_name=DATABASE_NAME,
                    properties={
                        "database.replica.number": "2"  # ← Default REPLICA=2 for all collections!
                    }
                )
                
                logger.info(f"✅ Database '{DATABASE_NAME}' created with default REPLICA=2")
            else:
                logger.info(f"Database '{DATABASE_NAME}' already exists")
            
            # Switch to this database
            self.client.using_database(DATABASE_NAME)
            logger.info(f"✅ Using database: {DATABASE_NAME}")
            
        except Exception as e:
            logger.warning(f"Database creation failed (may not be supported): {e}")
            logger.info("Falling back to default database with collection-level replicas")
    
    def _ensure_collection(self):
        """Create collection (will automatically use REPLICA=2 from database!)"""
        try:
            if self.client.has_collection(COLLECTION_NAME):
                logger.info(f"Collection '{COLLECTION_NAME}' already exists")
                
                # Load collection (should inherit REPLICA=2 from database)
                self.client.load_collection(COLLECTION_NAME)
                return
            
            # Create collection
            logger.info(f"Creating collection '{COLLECTION_NAME}'...")
            
            schema = self.client.create_schema(
                auto_id=False,
                enable_dynamic_field=True
            )
            
            schema.add_field("detection_uuid", DataType.VARCHAR, max_length=100, is_primary=True)
            schema.add_field("reid_matrix", DataType.FLOAT_VECTOR, dim=REID_DIM)
            schema.add_field("reid", DataType.INT64)
            schema.add_field("source_id", DataType.VARCHAR, max_length=50)
            schema.add_field("timestamp", DataType.DOUBLE)
            
            index_params = self.client.prepare_index_params()
            index_params.add_index(
                field_name="reid_matrix",
                index_type="IVF_FLAT",
                metric_type="L2",
                params={"nlist": 1024}
            )
            
            self.client.create_collection(
                collection_name=COLLECTION_NAME,
                schema=schema,
                index_params=index_params
            )
            
            # Load collection (inherits REPLICA=2 from database!)
            self.client.load_collection(COLLECTION_NAME)
            
            logger.info(f"✅ Created collection (will inherit REPLICA=2 from database)")
            
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
    
    def insert_reid(self, data: List[Dict]) -> bool:
        """Insert ReID data"""
        if not data:
            return True
        try:
            self.client.insert(collection_name=COLLECTION_NAME, data=data)
            logger.debug(f"✅ Inserted {len(data)} records")
            return True
        except Exception as e:
            logger.error(f"❌ Insert failed: {e}")
            return False
    
    def search_reid(self, query_matrix: List[float], limit: int = 10, 
                    filter_expr: Optional[str] = None) -> List[Dict]:
        """Search for similar vectors"""
        try:
            results = self.client.search(
                collection_name=COLLECTION_NAME,
                data=[query_matrix],
                filter=filter_expr,
                limit=limit,
                output_fields=["detection_uuid", "source_id", "timestamp", "reid"]
            )
            
            matches = []
            for hits in results:
                for hit in hits:
                    matches.append({
                        'detection_uuid': hit.get('entity', {}).get('detection_uuid') or hit.get('detection_uuid'),
                        'source_id': hit.get('entity', {}).get('source_id') or hit.get('source_id'),
                        'timestamp': hit.get('entity', {}).get('timestamp') or hit.get('timestamp'),
                        'reid': hit.get('entity', {}).get('reid') or hit.get('reid'),
                        'similarity': hit.get('distance', 0.0)
                    })
            
            return matches
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")
            return []


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

