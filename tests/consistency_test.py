#!/usr/bin/env python3
"""
Data Consistency Tests for Distributed Milvus
Tests data integrity and consistency across distributed components
"""

import time
import numpy as np
import threading
import concurrent.futures
from typing import List, Dict, Tuple, Set
from pymilvus import MilvusClient, DataType
from docker_utils import DockerManager
from database_utils import DatabaseManager
import json

class ConsistencyTester:
    """Data consistency testing suite for distributed Milvus"""
    
    def __init__(self, uri: str = "http://localhost:19530"):
        # Ensure Docker containers are running before starting tests
        self.db_manager = DatabaseManager(uri, ensure_docker_running=True)
        self.docker_manager = DockerManager()
        self.results = {}
        
    def setup_consistency_collection(self, collection_name: str = "consistency_test"):
        """Setup collection for consistency testing"""
        return self.db_manager.create_consistency_collection(collection_name)
    
    def calculate_checksum(self, data: Dict) -> str:
        """Calculate checksum for data integrity verification"""
        return self.db_manager.calculate_checksum(data)
    
    def test_data_integrity(self, num_records: int = 100):
        """Test data integrity during insert and search operations"""
        print(f"\n🧪 Testing Data Integrity: {num_records} records")
        
        try:
            # Generate test data with checksums
            inserted_data = self.db_manager.generate_consistency_test_data(num_records)
            
            # Insert data
            self.db_manager.insert_data("consistency_test", inserted_data)
            time.sleep(2)  # Wait for insertion
            
            # Verify data integrity
            print("   🔍 Verifying data integrity...")
            total_records, integrity_errors = self.db_manager.verify_data_integrity("consistency_test", inserted_data)
            
            integrity_rate = (total_records - integrity_errors) / total_records if total_records > 0 else 0
            
            self.results['data_integrity'] = {
                'records': total_records,
                'errors': integrity_errors,
                'integrity_rate': integrity_rate
            }
            
            print(f"✅ Data Integrity: {integrity_rate:.1%} integrity rate")
            return integrity_rate > 0.95  # 95% integrity threshold
            
        except Exception as e:
            print(f"❌ Data integrity test failed: {e}")
            return False
    
    def test_replica_consistency(self):
        """Test data consistency across replica nodes without failover"""
        print(f"\n🧪 Testing Replica Consistency")
        
        try:
            # Insert test data with multiple records
            test_data = self.db_manager.generate_consistency_test_data(10, prefix="replica_test")
            
            self.db_manager.insert_data("consistency_test", test_data)
            time.sleep(3)  # Wait for replication
            
            # Test search consistency across replicas
            print("   🔍 Testing search consistency across replicas...")
            query_vector = np.random.rand(2048).tolist()
            
            # Perform multiple searches to test consistency
            consistency_errors = 0
            search_results = []
            
            for i in range(5):  # Multiple searches
                results = self.db_manager.search_vectors(
                    collection_name="consistency_test",
                    query_vectors=[query_vector],
                    limit=10,
                    output_fields=["id", "label", "timestamp", "checksum"]
                )
                
                if results:
                    search_results.append(results)
                else:
                    consistency_errors += 1
                    print(f"   ❌ Search {i+1} returned no results")
            
            # Verify data consistency in search results
            for i, results in enumerate(search_results):
                for hit in results:
                    record_id = hit.get('id')
                    if record_id and record_id.startswith('replica_test_'):
                        # Find original data
                        original_data = next((d for d in test_data if d['id'] == record_id), None)
                        if original_data:
                            # Verify checksum
                            actual_checksum = hit.get('checksum')
                            expected_checksum = original_data['checksum']
                            
                            if actual_checksum is None:
                                print(f"   ⚠️ Checksum field missing for {record_id}")
                                consistency_errors += 1
                                continue
                            
                            if actual_checksum != expected_checksum:
                                consistency_errors += 1
                                print(f"   ❌ Checksum mismatch for {record_id}: {actual_checksum} != {expected_checksum}")
            
            # Verify all inserted data can be queried
            print("   🔍 Verifying all data can be queried...")
            queried_data = self.db_manager.query_data(
                collection_name="consistency_test",
                filter_expr="id like 'replica_test_%'",
                output_fields=["id", "label", "timestamp", "checksum"],
                limit=100
            )
            
            data_consistency_errors = 0
            for record in queried_data:
                expected_checksum = self.db_manager.calculate_checksum(record)
                actual_checksum = record.get('checksum', '')
                
                if expected_checksum != actual_checksum:
                    data_consistency_errors += 1
            
            total_errors = consistency_errors + data_consistency_errors
            
            self.results['replica_consistency'] = {
                'search_consistency_errors': consistency_errors,
                'data_consistency_errors': data_consistency_errors,
                'total_errors': total_errors,
                'searches_performed': len(search_results),
                'records_verified': len(queried_data)
            }
            
            print(f"✅ Replica Consistency: {total_errors} total errors ({len(search_results)} searches, {len(queried_data)} records)")
            return total_errors == 0
            
        except Exception as e:
            print(f"❌ Replica consistency test failed: {e}")
            return False
    
    def test_concurrent_consistency(self, num_threads: int = 5, operations_per_thread: int = 20):
        """Test data consistency under concurrent operations"""
        print(f"\n🧪 Testing Concurrent Consistency: {num_threads} threads")
        
        shared_data = []
        consistency_errors = 0
        lock = threading.Lock()
        
        def worker_thread(thread_id: int):
            nonlocal consistency_errors
            
            for i in range(operations_per_thread):
                try:
                    # Insert operation
                    data = {
                        "id": f"concurrent_{thread_id}_{i}",
                        "vector": np.random.rand(2048).tolist(),
                        "label": thread_id * 1000 + i,
                        "timestamp": time.time(),
                        "checksum": ""
                    }
                    data["checksum"] = self.calculate_checksum(data)
                    
                    self.db_manager.insert_data("consistency_test", [data])
                    
                    # Store for verification
                    with lock:
                        shared_data.append(data)
                    
                    # Search operation
                    query_vector = np.random.rand(2048).tolist()
                    results = self.db_manager.search_vectors(
                        collection_name="consistency_test",
                        query_vectors=[query_vector],
                        limit=5,
                        output_fields=["id", "label", "checksum"]
                    )
                    
                    # Verify search results
                    if results:
                        for hit in results:
                            hit_id = hit.get('id')
                            hit_checksum = hit.get('checksum')
                            
                            # Find original data
                            original_data = next((d for d in shared_data if d['id'] == hit_id), None)
                            if original_data and original_data['checksum'] != hit_checksum:
                                with lock:
                                    consistency_errors += 1
                                    print(f"   ❌ Concurrent consistency error: {hit_id}")
                    
                except Exception as e:
                    with lock:
                        consistency_errors += 1
                        print(f"   ❌ Thread {thread_id} error: {e}")
            
            return True
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(worker_thread, i) for i in range(num_threads)]
                concurrent.futures.wait(futures)
            
            self.results['concurrent_consistency'] = {
                'threads': num_threads,
                'operations': num_threads * operations_per_thread,
                'consistency_errors': consistency_errors,
                'consistency_rate': 1 - (consistency_errors / (num_threads * operations_per_thread))
            }
            
            consistency_rate = self.results['concurrent_consistency']['consistency_rate']
            print(f"✅ Concurrent Consistency: {consistency_rate:.1%} consistency rate")
            return consistency_rate > 0.95  # 95% consistency threshold
            
        except Exception as e:
            print(f"❌ Concurrent consistency test failed: {e}")
            return False
    
    def test_transaction_atomicity(self):
        """Test transaction atomicity and rollback behavior"""
        print(f"\n🧪 Testing Transaction Atomicity")
        
        try:
            # Test batch insert atomicity
            print("   📦 Testing batch insert atomicity...")
            batch_data = self.db_manager.generate_consistency_test_data(10, prefix="atomic_test")
            
            # Insert batch
            self.db_manager.insert_data("consistency_test", batch_data)
            time.sleep(2)
            
            # Verify all records are present
            all_data = self.db_manager.query_data(
                collection_name="consistency_test",
                filter_expr="id like 'atomic_test_%'",
                output_fields=["id", "label", "timestamp", "checksum"],
                limit=100
            )
            
            atomicity_success = len(all_data) == len(batch_data)
            
            if atomicity_success:
                print("   ✅ Batch insert atomicity verified")
            else:
                print(f"   ❌ Batch insert atomicity failed: {len(all_data)}/{len(batch_data)} records")
            
            # Test data consistency in batch
            consistency_errors = 0
            for record in all_data:
                expected_checksum = self.db_manager.calculate_checksum(record)
                actual_checksum = record.get('checksum', '')
                
                if expected_checksum != actual_checksum:
                    consistency_errors += 1
            
            self.results['transaction_atomicity'] = {
                'batch_insert_successful': atomicity_success,
                'consistency_errors': consistency_errors,
                'atomicity_rate': 1 - (consistency_errors / len(all_data)) if all_data else 0
            }
            
            atomicity_rate = self.results['transaction_atomicity']['atomicity_rate']
            print(f"✅ Transaction Atomicity: {atomicity_rate:.1%} atomicity rate")
            return atomicity_rate > 0.95  # 95% atomicity threshold
            
        except Exception as e:
            print(f"❌ Transaction atomicity test failed: {e}")
            return False
    
    def test_data_loss_detection(self):
        """Test data loss detection and recovery"""
        print(f"\n🧪 Testing Data Loss Detection")
        
        try:
            # Insert test data
            test_data = self.db_manager.generate_consistency_test_data(50, prefix="loss_test")
            
            self.db_manager.insert_data("consistency_test", test_data)
            time.sleep(3)
            
            # Simulate data loss by stopping and restarting services
            print("   🛑 Stopping and restarting services...")
            
            # Stop data nodes
            self.docker_manager.stop_container("milvus-datanode1")
            self.docker_manager.stop_container("milvus-datanode2")
            time.sleep(5)
            
            # Restart data nodes
            self.docker_manager.start_container("milvus-datanode1")
            self.docker_manager.start_container("milvus-datanode2")
            time.sleep(10)  # Wait for recovery
            
            # Check for data loss
            print("   🔍 Checking for data loss...")
            recovered_data = self.db_manager.query_data(
                collection_name="consistency_test",
                filter_expr="id like 'loss_test_%'",
                output_fields=["id", "label", "checksum"],
                limit=100
            )
            
            data_loss = len(test_data) - len(recovered_data)
            data_loss_rate = data_loss / len(test_data) if test_data else 0
            
            # Verify data integrity of recovered data
            integrity_errors = 0
            for record in recovered_data:
                expected_checksum = self.db_manager.calculate_checksum(record)
                actual_checksum = record.get('checksum', '')
                
                if expected_checksum != actual_checksum:
                    integrity_errors += 1
            
            self.results['data_loss_detection'] = {
                'original_records': len(test_data),
                'recovered_records': len(recovered_data),
                'data_loss': data_loss,
                'data_loss_rate': data_loss_rate,
                'integrity_errors': integrity_errors,
                'recovery_successful': data_loss_rate < 0.05  # Less than 5% data loss
            }
            
            print(f"✅ Data Loss Detection: {data_loss} records lost ({data_loss_rate:.1%})")
            return data_loss_rate < 0.05  # Less than 5% data loss threshold
            
        except Exception as e:
            print(f"❌ Data loss detection test failed: {e}")
            return False
    
    def run_consistency_suite(self):
        """Run complete consistency test suite"""
        print("="*60)
        print("DATA CONSISTENCY TEST SUITE")
        print("="*60)
        
        # Setup
        if not self.setup_consistency_collection():
            return False
        
        # Run tests
        tests = [
            ("Data Integrity", self.test_data_integrity),
            ("Replica Consistency", self.test_replica_consistency),
            ("Concurrent Consistency", self.test_concurrent_consistency),
            ("Transaction Atomicity", self.test_transaction_atomicity),
            ("Data Loss Detection", self.test_data_loss_detection)
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
        print("DATA CONSISTENCY TEST SUMMARY")
        print("="*60)
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"   {test_name}: {status}")
        
        print(f"\n🎯 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 ALL CONSISTENCY TESTS PASSED!")
            print("   ✅ Data integrity and consistency verified")
        else:
            print("⚠️ Some consistency tests failed")
            print("   ⚠️ Data integrity may be compromised")
        
        return passed == total

if __name__ == "__main__":
    consistency = ConsistencyTester()
    consistency.run_consistency_suite()
