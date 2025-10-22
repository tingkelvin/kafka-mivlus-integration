#!/usr/bin/env python3
"""
Performance and Load Testing for Distributed Milvus
Tests system performance under various load conditions
"""

import time
import numpy as np
import threading
import concurrent.futures
from typing import List, Dict, Tuple
from pymilvus import MilvusClient, DataType
from docker_utils import DockerManager
from database_utils import DatabaseManager
import psutil
import requests

class PerformanceTester:
    """Performance testing suite for distributed Milvus"""
    
    def __init__(self, uri: str = "http://localhost:19530"):
        # Ensure Docker containers are running before starting tests
        self.db_manager = DatabaseManager(uri, ensure_docker_running=True)
        self.docker_manager = DockerManager()
        self.results = {}
        
    def setup_test_collection(self, collection_name: str = "perf_test"):
        """Setup collection for performance testing"""
        return self.db_manager.create_performance_collection(collection_name)
    
    def test_insert_performance(self, num_records: int = 1000, batch_size: int = 20):
        """Test insert performance with concurrent operations"""
        print(f"\n🧪 Testing Insert Performance: {num_records} records")
        
        start_time = time.time()
        records_inserted = 0
        
        try:
            for i in range(0, num_records, batch_size):
                batch_data = self.db_manager.generate_test_data(
                    min(batch_size, num_records - i), 
                    prefix=f"perf_test_{i}"
                )
                
                self.db_manager.insert_data("perf_test", batch_data)
                records_inserted += len(batch_data)
                
                if i % (batch_size * 5) == 0:
                    elapsed = time.time() - start_time
                    rate = records_inserted / elapsed
                    print(f"   📊 Progress: {records_inserted}/{num_records} ({rate:.1f} records/sec)")
            
            total_time = time.time() - start_time
            rate = records_inserted / total_time
            
            self.results['insert_performance'] = {
                'records': records_inserted,
                'time': total_time,
                'rate': rate
            }
            
            print(f"✅ Insert Performance: {rate:.1f} records/sec")
            return True
            
        except Exception as e:
            print(f"❌ Insert performance test failed: {e}")
            return False
    
    def test_search_performance(self, num_searches: int = 100, concurrent_searches: int = 10):
        """Test search performance with concurrent operations"""
        print(f"\n🧪 Testing Search Performance: {num_searches} searches")
        
        def single_search():
            query_vector = np.random.rand(2048).tolist()
            start = time.time()
            results = self.db_manager.search_vectors(
                collection_name="perf_test",
                query_vectors=[query_vector],
                limit=10
            )
            return time.time() - start, len(results) if results else 0
        
        start_time = time.time()
        search_times = []
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_searches) as executor:
                futures = [executor.submit(single_search) for _ in range(num_searches)]
                
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    search_time, results_count = future.result()
                    search_times.append(search_time)
                    
                    if i % 20 == 0:
                        avg_time = sum(search_times) / len(search_times)
                        print(f"   📊 Progress: {i+1}/{num_searches} (avg: {avg_time:.3f}s)")
            
            total_time = time.time() - start_time
            avg_search_time = sum(search_times) / len(search_times)
            searches_per_sec = num_searches / total_time
            
            self.results['search_performance'] = {
                'searches': num_searches,
                'total_time': total_time,
                'avg_search_time': avg_search_time,
                'searches_per_sec': searches_per_sec
            }
            
            print(f"✅ Search Performance: {searches_per_sec:.1f} searches/sec (avg: {avg_search_time:.3f}s)")
            return True
            
        except Exception as e:
            print(f"❌ Search performance test failed: {e}")
            return False
    
    def test_memory_usage(self):
        """Monitor memory usage during operations"""
        print(f"\n🧪 Testing Memory Usage")
        
        try:
            # Get initial memory usage
            initial_memory = psutil.virtual_memory().used / (1024**3)  # GB
            
            # Perform memory-intensive operations
            large_batch = self.db_manager.generate_test_data(1000, prefix="memory_test")
            
            # Insert large batch
            self.db_manager.insert_data("perf_test", large_batch)
            
            # Check memory usage
            current_memory = psutil.virtual_memory().used / (1024**3)  # GB
            memory_increase = current_memory - initial_memory
            
            self.results['memory_usage'] = {
                'initial_gb': initial_memory,
                'current_gb': current_memory,
                'increase_gb': memory_increase
            }
            
            print(f"✅ Memory Usage: {memory_increase:.2f}GB increase")
            return True
            
        except Exception as e:
            print(f"❌ Memory usage test failed: {e}")
            return False
    
    def test_concurrent_operations(self, num_threads: int = 5, operations_per_thread: int = 20):
        """Test concurrent insert and search operations"""
        print(f"\n🧪 Testing Concurrent Operations: {num_threads} threads")
        
        def worker_thread(thread_id: int):
            results = {'inserts': 0, 'searches': 0, 'errors': 0}
            
            for i in range(operations_per_thread):
                try:
                    # Insert operation
                    data = self.db_manager.generate_test_data(1, prefix=f"concurrent_{thread_id}_{i}")
                    self.db_manager.insert_data("perf_test", data)
                    results['inserts'] += 1
                    
                    # Search operation
                    query_vector = np.random.rand(2048).tolist()
                    self.db_manager.search_vectors(
                        collection_name="perf_test",
                        query_vectors=[query_vector],
                        limit=5
                    )
                    results['searches'] += 1
                    
                except Exception as e:
                    results['errors'] += 1
                    print(f"   ⚠️ Thread {thread_id} error: {e}")
            
            return results
        
        start_time = time.time()
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(worker_thread, i) for i in range(num_threads)]
                thread_results = [future.result() for future in concurrent.futures.as_completed(futures)]
            
            total_time = time.time() - start_time
            total_inserts = sum(r['inserts'] for r in thread_results)
            total_searches = sum(r['searches'] for r in thread_results)
            total_errors = sum(r['errors'] for r in thread_results)
            
            self.results['concurrent_operations'] = {
                'threads': num_threads,
                'total_time': total_time,
                'inserts': total_inserts,
                'searches': total_searches,
                'errors': total_errors,
                'ops_per_sec': (total_inserts + total_searches) / total_time
            }
            
            print(f"✅ Concurrent Operations: {total_inserts} inserts, {total_searches} searches, {total_errors} errors")
            return True
            
        except Exception as e:
            print(f"❌ Concurrent operations test failed: {e}")
            return False
    
    def test_system_metrics(self):
        """Test system resource utilization"""
        print(f"\n🧪 Testing System Metrics")
        
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Network I/O
            network = psutil.net_io_counters()
            
            self.results['system_metrics'] = {
                'cpu_percent': cpu_percent,
                'memory_total_gb': memory.total / (1024**3),
                'memory_used_gb': memory.used / (1024**3),
                'memory_percent': memory.percent,
                'disk_total_gb': disk.total / (1024**3),
                'disk_used_gb': disk.used / (1024**3),
                'disk_percent': (disk.used / disk.total) * 100,
                'network_bytes_sent': network.bytes_sent,
                'network_bytes_recv': network.bytes_recv
            }
            
            print(f"✅ System Metrics: CPU {cpu_percent}%, Memory {memory.percent}%, Disk {self.results['system_metrics']['disk_percent']:.1f}%")
            return True
            
        except Exception as e:
            print(f"❌ System metrics test failed: {e}")
            return False
    
    def run_performance_suite(self):
        """Run complete performance test suite"""
        print("="*60)
        print("PERFORMANCE TEST SUITE")
        print("="*60)
        
        # Setup
        if not self.setup_test_collection():
            return False
        
        # Run tests
        tests = [
            ("Insert Performance", self.test_insert_performance),
            ("Search Performance", self.test_search_performance),
            ("Memory Usage", self.test_memory_usage),
            ("Concurrent Operations", self.test_concurrent_operations),
            ("System Metrics", self.test_system_metrics)
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
        print("PERFORMANCE TEST SUMMARY")
        print("="*60)
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"   {test_name}: {status}")
        
        print(f"\n🎯 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 ALL PERFORMANCE TESTS PASSED!")
        else:
            print("⚠️ Some performance tests failed")
        
        return passed == total

if __name__ == "__main__":
    tester = PerformanceTester()
    tester.run_performance_suite()
