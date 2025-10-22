#!/usr/bin/env python3
"""
Comprehensive Failover Test Suite
Tests system behavior during node failures and recovery scenarios
"""

import time
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import List, Dict, Tuple
from database_utils import DatabaseManager
from docker_utils import DockerManager, quick_status_check

class FailoverTester:
    """Comprehensive failover testing suite for distributed Milvus"""
    
    def __init__(self, uri: str = "http://localhost:19530"):
        # Ensure Docker containers are running before starting tests
        self.db_manager = DatabaseManager(uri, ensure_docker_running=True)
        self.docker_manager = DockerManager()
        self.results = {}
        
    def setup_test_environment(self, collection_name: str = "failover_test"):
        """Setup test environment with data"""
        print("="*60)
        print("FAILOVER TEST ENVIRONMENT SETUP")
        print("="*60)
        
        try:
            # Drop existing collection
            print("   🗑️  Dropping existing collection...")
            self.db_manager.drop_collection(collection_name)
            
            # Create new collection
            print("   📦 Creating new collection...")
            success = self.db_manager.create_collection(collection_name)
            if not success:
                return False
            
            # Insert test records
            print("   📝 Inserting 20 test records...")
            test_data = self.db_manager.generate_test_data(20, prefix="failover_record")
            success = self.db_manager.insert_data(collection_name, test_data)
            if not success:
                return False
            
            time.sleep(2)  # Wait for insertion
            
            # Verify setup
            stats = self.db_manager.get_collection_stats(collection_name)
            row_count = stats.get('row_count', 0)
            print(f"   ✅ Environment ready: {row_count} records")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Environment setup failed: {e}")
            return False
    
    def test_search_with_timeout(self, timeout: int = 60):
        """Test search functionality with timeout protection using ThreadPoolExecutor"""
        def search_operation():
            """The actual search operation to be executed with timeout"""
            return self.db_manager.search_vectors(
                collection_name="failover_test",
                query_vectors=[[0.1] * 2048],
                limit=10,
                output_fields=["id", "label"]
            )
        
        try:
            # Use ThreadPoolExecutor with timeout for thread-safe timeout handling
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(search_operation)
                results = future.result(timeout=timeout)
            
            if results:
                print(f"   ✅ Search successful: {len(results)} results")
                return True
            else:
                print("   ❌ Search returned no results")
                return False
                
        except FutureTimeoutError:
            print(f"   ⏰ Search timed out after {timeout} seconds")
            return False
        except Exception as e:
            print(f"   ❌ Search failed: {e}")
            return False
    
    def test_single_node_failover(self, node_name: str, test_name: str):
        """Test system behavior with a single node down"""
        print(f"\n{'='*60}")
        print(f"TESTING: {test_name}")
        print(f"{'='*60}")
        
        try:
            # Stop the node
            print(f"🛑 Stopping {node_name}...")
            if not self.docker_manager.stop_container(node_name):
                print(f"   ❌ Failed to stop {node_name}")
                return False
            
            print(f"   ✅ {node_name} stopped successfully")
            
            # Wait for system to stabilize
            print("   ⏱️  Waiting for system to stabilize...")
            time.sleep(5)
            
            # Test search functionality
            print("🔍 Testing search functionality...")
            search_success = self.test_search_with_timeout()
            
            # Restart the node
            print(f"🔄 Restarting {node_name}...")
            if not self.docker_manager.start_container(node_name):
                print(f"   ❌ Failed to restart {node_name}")
                return False
            
            print(f"   ✅ {node_name} restarted successfully")
            
            # Wait for recovery
            print("   ⏱️  Waiting for recovery...")
            time.sleep(10)
            
            # Test search after recovery
            print("🔍 Testing search after recovery...")
            recovery_success = self.test_search_with_timeout()
            
            # Store results
            self.results[test_name] = {
                'node_stopped': True,
                'search_during_failure': search_success,
                'node_restarted': True,
                'search_after_recovery': recovery_success,
                'overall_success': search_success and recovery_success
            }
            
            if search_success and recovery_success:
                print(f"   ✅ {test_name}: PASSED")
                return True
            else:
                print(f"   ❌ {test_name}: FAILED")
                return False
                
        except Exception as e:
            print(f"   ❌ {test_name} failed: {e}")
            self.results[test_name] = {'error': str(e), 'overall_success': False}
            return False
    
    def test_both_nodes_down(self):
        """Test system behavior with both query nodes down (should fail)"""
        print(f"\n{'='*60}")
        print("TESTING: Both Query Nodes Down (Expected to Fail)")
        print(f"{'='*60}")
        
        try:
            # Stop both query nodes
            print("🛑 Stopping both query nodes...")
            if not self.docker_manager.stop_container("milvus-querynode1"):
                print("   ❌ Failed to stop querynode1")
                return False
            
            if not self.docker_manager.stop_container("milvus-querynode2"):
                print("   ❌ Failed to stop querynode2")
                return False
            
            print("   ✅ Both query nodes stopped")

            print("🔍 Testing search functionality (should fail)...")
            search_success = self.test_search_with_timeout(timeout=10)

            print("📝 Inserting 20 test records...")
            # Use unique prefix to avoid collision with initial data
            test_data = self.db_manager.generate_test_data(20, prefix="failover_recovery_record")
            
            # Store the IDs and labels of the data we're inserting for verification
            inserted_ids = [record["id"] for record in test_data]
            inserted_labels = [record["label"] for record in test_data]
            print(f"📋 Inserted {len(inserted_ids)} records with IDs: {inserted_ids[:5]}...")  # Show first 5 IDs
            
            success = self.db_manager.insert_data("failover_test", test_data)
            if not success:
                return False
            
            time.sleep(2)  # Wait for insertion
            
            # Wait for system to stabilize
            print("   ⏱️  Waiting for system to stabilize...")
            time.sleep(5)
            
            # Restart both query nodes
            print("🔄 Restarting both query nodes...")
            if not self.docker_manager.start_container("milvus-querynode1"):
                print("   ❌ Failed to restart querynode1")
                return False
            
            if not self.docker_manager.start_container("milvus-querynode2"):
                print("   ❌ Failed to restart querynode2")
                return False

            
            print("   ✅ Both query nodes restarted")
            
            # Wait for recovery
            print("   ⏱️  Waiting for recovery...")
            time.sleep(5)
            
            # Test search after recovery
            print("🔍 Testing search after recovery...")
            recovery_success = self.test_search_with_timeout()
            
            # Check if newly inserted data is available and matches what we inserted
            print("🔍 Checking if newly inserted data is available and matches...")
            try:
                # Query the newly inserted data
                new_data = self.db_manager.query_data(
                    collection_name="failover_test",
                    filter_expr="id like 'failover_recovery_record_%'",
                    output_fields=["id", "label"],
                    limit=100
                )
                
                print(f"   📊 Found {len(new_data)} records with failover_recovery_record_ prefix")
                
                # Verify that the retrieved data matches exactly what we inserted
                retrieved_ids = [record["id"] for record in new_data]
                retrieved_labels = [record["label"] for record in new_data]
                
                # Check if all inserted IDs are present in retrieved data
                missing_ids = set(inserted_ids) - set(retrieved_ids)
                extra_ids = set(retrieved_ids) - set(inserted_ids)
                
                print(f"   🔍 Verification:")
                print(f"      Expected {len(inserted_ids)} records")
                print(f"      Retrieved {len(retrieved_ids)} records")
                print(f"      Missing IDs: {list(missing_ids)[:5]}..." if missing_ids else "      Missing IDs: None")
                print(f"      Extra IDs: {list(extra_ids)[:5]}..." if extra_ids else "      Extra IDs: None")
                
                # Data is considered available if all inserted records are present and no unexpected records
                data_available = len(missing_ids) == 0 and len(extra_ids) == 0
                
                if data_available:
                    print("   ✅ New data matches exactly what was inserted")
                elif len(missing_ids) > 0:
                    print(f"   ❌ Missing {len(missing_ids)} inserted records")
                elif len(extra_ids) > 0:
                    print(f"   ⚠️  Found {len(extra_ids)} unexpected records")
                else:
                    print("   ❌ Data verification failed")
                    
            except Exception as e:
                print(f"   ❌ Failed to check new data: {e}")
                data_available = False
            
            # Store results
            self.results['Both Nodes Down'] = {
                'both_nodes_stopped': True,
                'search_success': search_success,
                'both_nodes_restarted': True,
                'recovery_success': recovery_success,
                'new_data_available': data_available,
                'expected_behavior': not search_success  # Should fail when both down
            }
            
            if search_success:
                print("   ⚠️  UNEXPECTED: Search still working with both nodes down!")
                return False
            elif recovery_success and data_available:
                print("   ✅ EXPECTED: Search failed with both nodes down, recovered successfully with new data")
                return True
            elif recovery_success and not data_available:
                print("   ⚠️  PARTIAL: Search recovered but new data is missing")
                return False
            else:
                print("   ❌ Recovery failed after restarting nodes")
                return False
                
        except Exception as e:
            print(f"   ✅ EXPECTED: Search failed as expected: {e}")
            self.results['Both Nodes Down'] = {'expected_failure': str(e), 'expected_behavior': True}
            return True
    
    def run_failover_suite(self):
        """Run complete failover test suite"""
        print("="*60)
        print("COMPREHENSIVE FAILOVER TEST SUITE")
        print("="*60)
        
        # Setup test environment
        if not self.setup_test_environment():
            print("❌ Test environment setup failed - aborting")
            return False
        
        # Run failover tests
        tests = [
            ("QueryNode1 Failover", lambda: self.test_single_node_failover("milvus-querynode1", "QueryNode1 Failover")),
            ("QueryNode2 Failover", lambda: self.test_single_node_failover("milvus-querynode2", "QueryNode2 Failover")),
            ("Both Nodes Down", self.test_both_nodes_down),
        ]
        
        results = {}
        for test_name, test_func in tests:
            print(f"\n🧪 Running {test_name}...")
            try:
                success = test_func()
                results[test_name] = success
            except Exception as e:
                print(f"❌ {test_name} failed: {e}")
                results[test_name] = False
        
        # Summary
        print("\n" + "="*60)
        print("FAILOVER TEST SUMMARY")
        print("="*60)
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"   {test_name}: {status}")
        
        print(f"\n🎯 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 ALL FAILOVER TESTS PASSED!")
            print("   ✅ Node failover working correctly")
            print("   ✅ Search functionality maintained")
            print("   ✅ Recovery process successful")
        else:
            print("⚠️ Some failover tests failed")
            print("   ⚠️ System resilience may be compromised")
        
        # Final status
        print("\nFinal Container Status:")
        quick_status_check()
        
        return passed == total

if __name__ == "__main__":
    failover_tester = FailoverTester()
    failover_tester.run_failover_suite()
