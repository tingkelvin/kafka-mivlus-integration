#!/usr/bin/env python3
"""
Chaos Engineering Tests for Distributed Milvus
Tests system resilience under various failure conditions
"""

import time
import random
import numpy as np
from typing import List, Dict, Tuple
from pymilvus import MilvusClient, DataType
from docker_utils import DockerManager
from database_utils import DatabaseManager
import threading
import concurrent.futures

class ChaosEngineer:
    """Chaos engineering test suite for distributed Milvus"""
    
    def __init__(self, uri: str = "http://localhost:19530"):
        # Ensure Docker containers are running before starting tests
        self.db_manager = DatabaseManager(uri, ensure_docker_running=True)
        self.docker_manager = DockerManager()
        self.results = {}
        
    def setup_chaos_collection(self, collection_name: str = "chaos_test"):
        """Setup collection for chaos testing"""
        return self.db_manager.create_chaos_collection(collection_name)
    
    def test_random_container_restarts(self, duration_minutes: int = 5):
        """Test system resilience with random container restarts"""
        print(f"\n🧪 Testing Random Container Restarts ({duration_minutes} minutes)")
        
        # List of containers to restart
        restartable_containers = [
            'milvus-querynode1', 'milvus-querynode2',
            'milvus-datanode1', 'milvus-datanode2',
            'milvus-indexnode1', 'milvus-indexnode2'
        ]
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        restart_count = 0
        successful_operations = 0
        failed_operations = 0
        
        def perform_operations():
            nonlocal successful_operations, failed_operations
            try:
                # Insert operation
                data = self.db_manager.generate_test_data(1, prefix=f"chaos_insert_{int(time.time())}")
                self.db_manager.insert_data("chaos_test", data)
                
                # Search operation
                query_vector = np.random.rand(2048).tolist()
                results = self.db_manager.search_vectors(
                    collection_name="chaos_test",
                    query_vectors=[query_vector],
                    limit=5
                )
                
                successful_operations += 1
                return True
                
            except Exception as e:
                failed_operations += 1
                return False
        
        try:
            while time.time() < end_time:
                # Perform operations
                success = perform_operations()
                
                # Randomly restart a container
                if random.random() < 0.3:  # 30% chance
                    container = random.choice(restartable_containers)
                    print(f"   🔄 Restarting {container}...")
                    
                    if self.docker_manager.restart_container(container):
                        restart_count += 1
                        print(f"   ✅ {container} restarted successfully")
                    else:
                        print(f"   ❌ Failed to restart {container}")
                    
                    # Wait for system to stabilize
                    time.sleep(10)
                
                # Small delay between operations
                time.sleep(1)
            
            self.results['random_restarts'] = {
                'duration_minutes': duration_minutes,
                'restarts': restart_count,
                'successful_ops': successful_operations,
                'failed_ops': failed_operations,
                'success_rate': successful_operations / (successful_operations + failed_operations) if (successful_operations + failed_operations) > 0 else 0
            }
            
            success_rate = self.results['random_restarts']['success_rate']
            print(f"✅ Random Restarts: {restart_count} restarts, {success_rate:.1%} success rate")
            return success_rate > 0.8  # 80% success rate threshold
            
        except Exception as e:
            print(f"❌ Random restart test failed: {e}")
            return False
    
    def test_cascading_failures(self):
        """Test system behavior with cascading failures"""
        print(f"\n🧪 Testing Cascading Failures")
        
        # Step 1: Stop one query node
        print("   🛑 Step 1: Stopping querynode1...")
        if not self.docker_manager.stop_container("milvus-querynode1"):
            print("   ❌ Failed to stop querynode1")
            return False
        
        time.sleep(5)
        
        # Test operations with one node down
        print("   🔍 Testing operations with querynode1 down...")
        try:
            data = self.db_manager.generate_test_data(1, prefix="cascade_test_1")
            self.db_manager.insert_data("chaos_test", data)
            
            query_vector = np.random.rand(2048).tolist()
            results = self.db_manager.search_vectors(
                collection_name="chaos_test",
                query_vectors=[query_vector],
                limit=5
            )
            print("   ✅ Operations working with querynode1 down")
            
        except Exception as e:
            print(f"   ❌ Operations failed with querynode1 down: {e}")
            return False
        
        # Step 2: Stop second query node (should fail)
        print("   🛑 Step 2: Stopping querynode2...")
        if not self.docker_manager.stop_container("milvus-querynode2"):
            print("   ❌ Failed to stop querynode2")
            return False
        
        time.sleep(5)
        
        # Test operations with both nodes down (should fail)
        print("   🔍 Testing operations with both nodes down...")
        try:
            data = self.db_manager.generate_test_data(1, prefix="cascade_test_2")
            self.db_manager.insert_data("chaos_test", data)
            print("   ⚠️ UNEXPECTED: Operations still working with both nodes down!")
            return False
            
        except Exception as e:
            print(f"   ✅ EXPECTED: Operations failed as expected: {e}")
        
        # Step 3: Restart both nodes
        print("   🔄 Step 3: Restarting both query nodes...")
        if not self.docker_manager.start_container("milvus-querynode1"):
            print("   ❌ Failed to restart querynode1")
            return False
        
        if not self.docker_manager.start_container("milvus-querynode2"):
            print("   ❌ Failed to restart querynode2")
            return False
        
        time.sleep(15)  # Wait for recovery
        
        # Test operations after recovery
        print("   🔍 Testing operations after recovery...")
        try:
            data = self.db_manager.generate_test_data(1, prefix="cascade_test_3")
            self.db_manager.insert_data("chaos_test", data)
            
            query_vector = np.random.rand(2048).tolist()
            results = self.db_manager.search_vectors(
                collection_name="chaos_test",
                query_vectors=[query_vector],
                limit=5
            )
            print("   ✅ Operations working after recovery")
            
            self.results['cascading_failures'] = {
                'single_node_failure': True,
                'dual_node_failure': True,
                'recovery': True
            }
            return True
            
        except Exception as e:
            print(f"   ❌ Operations failed after recovery: {e}")
            return False
    
    def test_resource_exhaustion(self):
        """Test system behavior under resource exhaustion"""
        print(f"\n🧪 Testing Resource Exhaustion")
        
        try:
            # Test with high memory usage
            print("   💾 Testing high memory usage...")
            # Insert large batches
            for i in range(10):  # Create 10 large batches
                batch_data = self.db_manager.generate_test_data(100, prefix=f"memory_test_{i}")
                self.db_manager.insert_data("chaos_test", batch_data)
                print(f"   📊 Inserted batch {i+1}/10")
                time.sleep(1)  # Small delay
            
            print("   ✅ High memory usage test completed")
            
            # Test search under memory pressure
            query_vector = np.random.rand(2048).tolist()
            results = self.db_manager.search_vectors(
                collection_name="chaos_test",
                query_vectors=[query_vector],
                limit=10
            )
            print("   ✅ Search working under memory pressure")
            
            self.results['resource_exhaustion'] = {
                'memory_test': True,
                'search_under_pressure': True
            }
            return True
            
        except Exception as e:
            print(f"   ❌ Resource exhaustion test failed: {e}")
            return False
    
    def test_network_partition(self):
        """Test system behavior with network partitions"""
        print(f"\n🧪 Testing Network Partition")
        
        # This is a simplified test - in a real scenario, you'd use network tools
        # to create actual network partitions
        
        print("   🌐 Simulating network partition by stopping network-dependent services...")
        
        # Stop Kafka (simulates network partition)
        print("   🛑 Stopping Kafka...")
        if not self.docker_manager.stop_container("milvus-kafka"):
            print("   ❌ Failed to stop Kafka")
            return False
        
        time.sleep(5)
        
        # Test operations during network partition
        print("   🔍 Testing operations during network partition...")
        try:
            data = self.db_manager.generate_test_data(1, prefix="network_test_1")
            self.db_manager.insert_data("chaos_test", data)
            print("   ⚠️ UNEXPECTED: Operations still working during network partition!")
            
        except Exception as e:
            print(f"   ✅ EXPECTED: Operations failed during network partition: {e}")
        
        # Restart Kafka
        print("   🔄 Restarting Kafka...")
        if not self.docker_manager.start_container("milvus-kafka"):
            print("   ❌ Failed to restart Kafka")
            return False
        
        time.sleep(10)  # Wait for recovery
        
        # Test operations after network recovery
        print("   🔍 Testing operations after network recovery...")
        try:
            data = self.db_manager.generate_test_data(1, prefix="network_test_2")
            self.db_manager.insert_data("chaos_test", data)
            print("   ✅ Operations working after network recovery")
            
            self.results['network_partition'] = {
                'partition_simulation': True,
                'recovery': True
            }
            return True
            
        except Exception as e:
            print(f"   ❌ Operations failed after network recovery: {e}")
            return False
    
    def test_clock_skew(self):
        """Test system behavior with clock skew"""
        print(f"\n🧪 Testing Clock Skew")
        
        # This is a simplified test - in a real scenario, you'd use tools
        # to actually change system time
        
        print("   ⏰ Simulating clock skew by manipulating timestamps...")
        
        try:
            # Insert data with different timestamps
            current_time = time.time()
            skewed_times = [
                current_time - 3600,  # 1 hour ago
                current_time + 3600,  # 1 hour in future
                current_time - 86400, # 1 day ago
                current_time + 86400  # 1 day in future
            ]
            
            for i, skewed_time in enumerate(skewed_times):
                data = self.db_manager.generate_test_data(1, prefix=f"clock_skew_{i}")
                # Override timestamp
                data[0]["timestamp"] = skewed_time
                self.db_manager.insert_data("chaos_test", data)
                print(f"   📊 Inserted record with timestamp: {skewed_time}")
            
            # Test search with clock skew
            query_vector = np.random.rand(2048).tolist()
            results = self.db_manager.search_vectors(
                collection_name="chaos_test",
                query_vectors=[query_vector],
                limit=10
            )
            print("   ✅ Search working with clock skew")
            
            self.results['clock_skew'] = {
                'timestamp_manipulation': True,
                'search_functionality': True
            }
            return True
            
        except Exception as e:
            print(f"   ❌ Clock skew test failed: {e}")
            return False
    
    def run_chaos_suite(self):
        """Run complete chaos engineering test suite"""
        print("="*60)
        print("CHAOS ENGINEERING TEST SUITE")
        print("="*60)
        
        # Setup
        if not self.setup_chaos_collection():
            return False
        
        # Run tests
        tests = [
            ("Random Container Restarts", self.test_random_container_restarts),
            ("Cascading Failures", self.test_cascading_failures),
            ("Resource Exhaustion", self.test_resource_exhaustion),
            ("Network Partition", self.test_network_partition),
            ("Clock Skew", self.test_clock_skew)
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
        print("CHAOS ENGINEERING TEST SUMMARY")
        print("="*60)
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"   {test_name}: {status}")
        
        print(f"\n🎯 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 ALL CHAOS TESTS PASSED!")
            print("   ✅ System is resilient to failures")
        else:
            print("⚠️ Some chaos tests failed")
            print("   ⚠️ System may not be fully resilient")
        
        return passed == total

if __name__ == "__main__":
    chaos = ChaosEngineer()
    chaos.run_chaos_suite()
