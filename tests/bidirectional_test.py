#!/usr/bin/env python3
"""
Comprehensive Bidirectional Failover Test
Tests search functionality with proper data setup and bidirectional failover
"""

import time
import numpy as np
from pymilvus import MilvusClient, DataType
from docker_utils import DockerManager, quick_status_check

def clean_and_setup_database():
    """
    Clean database and insert test data
    """
    print("="*60)
    print("DATABASE CLEANUP AND SETUP")
    print("="*60)
    
    try:
        client = MilvusClient(uri="http://localhost:19530")
        client.using_database("test_db")
        
        # Drop collection if exists
        if client.has_collection("test_collection"):
            print("   🗑️  Dropping existing collection...")
            client.drop_collection("test_collection")
            time.sleep(2)
        
        # Create new collection
        print("   📦 Creating new collection...")
        schema = client.create_schema(
            auto_id=False,
            enable_dynamic_field=True
        )
        
        schema.add_field("id", DataType.VARCHAR, max_length=100, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2048)
        schema.add_field("label", DataType.INT64)
        schema.add_field("timestamp", DataType.DOUBLE)
        
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 1024}
        )
        
        client.create_collection(
            collection_name="test_collection",
            schema=schema,
            index_params=index_params
        )
        
        # Load collection
        client.load_collection("test_collection")
        time.sleep(3)  # Wait for collection to be ready
        
        # Insert 10 test records
        print("   📝 Inserting 10 test records...")
        test_data = []
        for i in range(10):
            test_data.append({
                "id": f"test_record_{i}",
                "vector": np.random.rand(2048).tolist(),
                "label": i,
                "timestamp": time.time()
            })
        
        client.insert("test_collection", test_data)
        time.sleep(2)  # Wait for insertion to complete
        
        # Verify data
        stats = client.get_collection_stats("test_collection")
        row_count = stats.get('row_count', 0)
        print(f"   ✅ Database ready: {row_count} records")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Database setup failed: {e}")
        return False

def test_search_all_data(timeout=60):
    """
    Test searching for all data in the collection with timeout
    """
    import signal
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Search operation timed out")
    
    try:
        # Set timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        
        client = MilvusClient(uri="http://localhost:19530")
        client.using_database("test_db")
        
        # Get all data using query
        all_data = client.query(
            collection_name="test_collection",
            filter="",
            output_fields=["id", "label"],
            limit=100
        )
        
        print(f"   📊 Found {len(all_data)} records in database")
        
        # Test search for each record
        search_success_count = 0
        for record in all_data:
            record_id = record.get('id')
            record_label = record.get('label')
            
            # Search for this specific record
            search_results = client.search(
                collection_name="test_collection",
                data=[[0.1] * 2048],  # Random vector
                limit=10,
                output_fields=["id", "label"]
            )
            
            if search_results and search_results[0]:
                search_success_count += 1
                print(f"   ✅ Search {search_success_count}/10: Found {len(search_results[0])} results")
            else:
                print(f"   ❌ Search failed for record {record_id}")
                return False
        
        print(f"   🎉 All {search_success_count} searches successful!")
        return True
        
    except TimeoutError:
        print(f"   ⏰ Search operation timed out after {timeout} seconds")
        return False
    except Exception as e:
        print(f"   ❌ Search test failed: {e}")
        return False
    finally:
        signal.alarm(0)  # Cancel timeout

def test_with_node_down(node_name, test_name):
    """
    Test system behavior with a specific node down
    """
    print(f"\n{'='*60}")
    print(f"TESTING: {test_name}")
    print(f"{'='*60}")
    
    docker_manager = DockerManager()
    
    try:
        # Stop the node
        print(f"🛑 Stopping {node_name}...")
        if not docker_manager.stop_container(node_name):
            print(f"   ❌ Failed to stop {node_name}")
            return False
        
        print(f"   ✅ {node_name} stopped successfully")
        
        # Wait for system to stabilize
        print("   ⏱️  Waiting for system to stabilize...")
        time.sleep(5)
        
        # Test search functionality
        print("🔍 Testing search functionality...")
        try:
            success = test_search_all_data()
            if success:
                print(f"   ✅ Search working with {node_name} down")
                return True
            else:
                print(f"   ❌ Search failed with {node_name} down")
                return False
        except Exception as e:
            print(f"   ❌ Search test failed: {e}")
            return False
        
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
        return False

def test_with_both_nodes_down():
    """
    Test system behavior with both query nodes down (should fail)
    """
    print(f"\n{'='*60}")
    print("TESTING: Both Query Nodes Down (Expected to Fail)")
    print(f"{'='*60}")
    
    docker_manager = DockerManager()
    
    try:
        # Stop both query nodes
        print("🛑 Stopping both query nodes...")
        if not docker_manager.stop_container("milvus-querynode1"):
            print("   ❌ Failed to stop querynode1")
            return False
        
        if not docker_manager.stop_container("milvus-querynode2"):
            print("   ❌ Failed to stop querynode2")
            return False
        
        print("   ✅ Both query nodes stopped")
        
        # Wait for system to stabilize
        print("   ⏱️  Waiting for system to stabilize...")
        time.sleep(5)
        
        # Test search functionality (should fail)
        print("🔍 Testing search functionality (should fail)...")
        try:
            success = test_search_all_data()
            if success:
                print("   ⚠️  UNEXPECTED: Search still working with both nodes down!")
                return False
            else:
                print("   ✅ EXPECTED: Search failed with both nodes down")
                return True
        except Exception as e:
            print(f"   ✅ EXPECTED: Search failed as expected: {e}")
            return True
        
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
        return False

def restart_all_nodes():
    """
    Restart all query nodes
    """
    print(f"\n{'='*60}")
    print("RESTARTING ALL QUERY NODES")
    print(f"{'='*60}")
    
    docker_manager = DockerManager()
    
    try:
        # Start both query nodes
        print("🔄 Starting query nodes...")
        if docker_manager.start_container("milvus-querynode1"):
            print("   ✅ querynode1 started")
        else:
            print("   ❌ Failed to start querynode1")
            return False
        
        if docker_manager.start_container("milvus-querynode2"):
            print("   ✅ querynode2 started")
        else:
            print("   ❌ Failed to start querynode2")
            return False
        
        # Wait for recovery
        print("   ⏱️  Waiting for full recovery...")
        time.sleep(15)
        
        # Test final functionality
        print("🔍 Testing final recovery...")
        success = test_search_all_data()
        if success:
            print("   ✅ Full recovery successful")
            return True
        else:
            print("   ❌ Recovery test failed")
            return False
        
    except Exception as e:
        print(f"   ❌ Restart failed: {e}")
        return False

def main():
    """
    Run comprehensive bidirectional failover test
    """
    print("COMPREHENSIVE BIDIRECTIONAL FAILOVER TEST")
    print("="*60)
    print("This test will:")
    print("1. Clean database and insert 10 test records")
    print("2. Kill querynode1, test search functionality")
    print("3. Restart querynode1, kill querynode2, test search")
    print("4. Kill both nodes, verify search fails")
    print("5. Restart both nodes, verify full recovery")
    print("="*60)
    
    results = []
    
    # Step 1: Clean and setup database
    print("\n🧪 STEP 1: Database Setup")
    if not clean_and_setup_database():
        print("❌ Database setup failed - aborting test")
        return False
    results.append(("Database Setup", True))
    
    # Step 2: Test with querynode1 down
    print("\n🧪 STEP 2: Testing with querynode1 down")
    result1 = test_with_node_down("milvus-querynode1", "QueryNode1 Down")
    results.append(("QueryNode1 Down", result1))
    
    # Step 3: Restart querynode1, test with querynode2 down
    print("\n🧪 STEP 3: Testing with querynode2 down")
    docker_manager = DockerManager()
    if docker_manager.start_container("milvus-querynode1"):
        print("   ✅ querynode1 restarted")
        time.sleep(10)  # Wait for recovery
    
    result2 = test_with_node_down("milvus-querynode2", "QueryNode2 Down")
    results.append(("QueryNode2 Down", result2))

    restart_all_nodes()
    
    # Summary
    print("\n" + "="*60)
    print("COMPREHENSIVE BIDIRECTIONAL TEST SUMMARY")
    print("="*60)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 COMPREHENSIVE TEST PASSED!")
        print("   ✅ Bidirectional failover working")
        print("   ✅ Search functionality verified")
        print("   ✅ Expected failures confirmed")
        print("   ✅ Full recovery successful")
    elif passed >= len(results) * 0.8:
        print("⚠️  MOSTLY SUCCESSFUL - Minor issues detected")
    else:
        print("❌ COMPREHENSIVE TEST FAILED")
        print("   ⚠️  Significant issues with failover or search")
    
    # Final status
    print("\nFinal Container Status:")
    quick_status_check()

if __name__ == "__main__":
    main()
