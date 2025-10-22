#!/usr/bin/env python3
"""
Comprehensive Test Runner for Distributed Milvus
Runs all test suites and provides detailed reporting
"""

import time
import sys
import os
from typing import Dict, List, Tuple
from docker_utils import DockerManager, quick_status_check

# Import test modules
try:
    from test_replication import MilvusDistributedManagerV2
    from performance_test import PerformanceTester
    from chaos_engineering_test import ChaosEngineer
    from consistency_test import ConsistencyTester
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all test modules are available")
    sys.exit(1)

class TestRunner:
    """Comprehensive test runner for distributed Milvus"""
    
    def __init__(self):
        self.docker_manager = DockerManager()
        self.results = {}
        self.start_time = time.time()
        
    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met"""
        print("🔍 Checking Prerequisites...")
        
        # Check if Docker is running
        try:
            import subprocess
            result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
            if result.returncode != 0:
                print("❌ Docker is not running")
                return False
        except FileNotFoundError:
            print("❌ Docker is not installed")
            return False
        
        # Check and ensure all Milvus containers are running
        print("   📊 Checking and starting Milvus services...")
        try:
            from database_utils import ensure_all_containers_running
            success = ensure_all_containers_running()
            if not success:
                print("   ❌ Failed to start all required containers")
                return False
        except Exception as e:
            print(f"   ❌ Failed to check/start containers: {e}")
            return False
        
        print("✅ Prerequisites check passed")
        return True
    
    def run_replication_test(self) -> bool:
        """Run replication test"""
        print("\n" + "="*60)
        print("RUNNING REPLICATION TEST")
        print("="*60)
        
        try:
            manager = MilvusDistributedManagerV2()
            
            # Test basic functionality
            test_data = [{
                "detection_uuid": f"runner_test_{i}",
                "reid_matrix": [0.1] * 2048,
                "reid": i,
                "source_id": f"camera{i % 3 + 1}",
                "timestamp": time.time()
            } for i in range(5)]
            
            success = manager.insert_reid(test_data)
            if success:
                results = manager.search_reid([0.1] * 2048, limit=5)
                success = len(results) > 0
            
            self.results['replication_test'] = success
            print(f"✅ Replication test: {'PASS' if success else 'FAIL'}")
            return success
            
        except Exception as e:
            print(f"❌ Replication test failed: {e}")
            self.results['replication_test'] = False
            return False
    
    def run_failover_test(self) -> bool:
        """Run comprehensive failover test"""
        print("\n" + "="*60)
        print("RUNNING FAILOVER TEST")
        print("="*60)
        
        try:
            from failover_test import FailoverTester
            failover_tester = FailoverTester()
            success = failover_tester.run_failover_suite()
            self.results['failover_test'] = success
            print(f"✅ Failover test: {'PASS' if success else 'FAIL'}")
            return success
            
        except Exception as e:
            print(f"❌ Failover test failed: {e}")
            self.results['failover_test'] = False
            return False
    
    def run_performance_test(self) -> bool:
        """Run performance test"""
        print("\n" + "="*60)
        print("RUNNING PERFORMANCE TEST")
        print("="*60)
        
        try:
            tester = PerformanceTester()
            success = tester.run_performance_suite()
            self.results['performance_test'] = success
            print(f"✅ Performance test: {'PASS' if success else 'FAIL'}")
            return success
            
        except Exception as e:
            print(f"❌ Performance test failed: {e}")
            self.results['performance_test'] = False
            return False
    
    def run_chaos_test(self) -> bool:
        """Run chaos engineering test"""
        print("\n" + "="*60)
        print("RUNNING CHAOS ENGINEERING TEST")
        print("="*60)
        
        try:
            chaos = ChaosEngineer()
            success = chaos.run_chaos_suite()
            self.results['chaos_test'] = success
            print(f"✅ Chaos test: {'PASS' if success else 'FAIL'}")
            return success
            
        except Exception as e:
            print(f"❌ Chaos test failed: {e}")
            self.results['chaos_test'] = False
            return False
    
    def run_consistency_test(self) -> bool:
        """Run consistency test"""
        print("\n" + "="*60)
        print("RUNNING CONSISTENCY TEST")
        print("="*60)
        
        try:
            consistency = ConsistencyTester()
            success = consistency.run_consistency_suite()
            self.results['consistency_test'] = success
            print(f"✅ Consistency test: {'PASS' if success else 'FAIL'}")
            return success
            
        except Exception as e:
            print(f"❌ Consistency test failed: {e}")
            self.results['consistency_test'] = False
            return False
    
    def run_all_tests(self, test_suites: List[str] = None) -> Dict[str, bool]:
        """Run all test suites"""
        if test_suites is None:
            test_suites = [
                'replication_test',
                'failover_test', 
                'performance_test',
                'chaos_test',
                'consistency_test'
            ]
        
        print("="*80)
        print("COMPREHENSIVE TEST SUITE FOR DISTRIBUTED MILVUS")
        print("="*80)
        print(f"Running test suites: {', '.join(test_suites)}")
        print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check prerequisites
        if not self.check_prerequisites():
            print("❌ Prerequisites check failed - aborting tests")
            return {}
        
        # Run tests
        for test_suite in test_suites:
            if test_suite == 'replication_test':
                self.run_replication_test()
            elif test_suite == 'failover_test':
                self.run_failover_test()
            elif test_suite == 'performance_test':
                self.run_performance_test()
            elif test_suite == 'chaos_test':
                self.run_chaos_test()
            elif test_suite == 'consistency_test':
                self.run_consistency_test()
            else:
                print(f"⚠️ Unknown test suite: {test_suite}")
        
        return self.results
    
    def generate_report(self) -> str:
        """Generate comprehensive test report"""
        total_time = time.time() - self.start_time
        
        report = f"""
{'='*80}
COMPREHENSIVE TEST REPORT
{'='*80}
Test Date: {time.strftime('%Y-%m-%d %H:%M:%S')}
Total Duration: {total_time:.1f} seconds

TEST RESULTS:
"""
        
        passed = 0
        total = len(self.results)
        
        for test_name, result in self.results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            report += f"   {test_name.replace('_', ' ').title()}: {status}\n"
            if result:
                passed += 1
        
        report += f"""
SUMMARY:
   Total Tests: {total}
   Passed: {passed}
   Failed: {total - passed}
   Success Rate: {(passed/total)*100:.1f}%

OVERALL STATUS: """
        
        if passed == total:
            report += "🎉 ALL TESTS PASSED! System is fully operational.\n"
        elif passed >= total * 0.8:
            report += "⚠️ MOSTLY SUCCESSFUL - Minor issues detected.\n"
        else:
            report += "❌ SIGNIFICANT ISSUES - System may not be fully operational.\n"
        
        report += f"""
RECOMMENDATIONS:
"""
        
        if self.results.get('replication_test', False):
            report += "   ✅ Replication: Database-level replicas working correctly\n"
        else:
            report += "   ❌ Replication: Check replica configuration\n"
        
        if self.results.get('failover_test', False):
            report += "   ✅ Failover: Node failover and recovery working correctly\n"
        else:
            report += "   ❌ Failover: Check failover configuration\n"
        
        if self.results.get('performance_test', False):
            report += "   ✅ Performance: System performance is acceptable\n"
        else:
            report += "   ❌ Performance: Consider performance optimization\n"
        
        if self.results.get('chaos_test', False):
            report += "   ✅ Resilience: System is resilient to failures\n"
        else:
            report += "   ❌ Resilience: System may not be fully resilient\n"
        
        if self.results.get('consistency_test', False):
            report += "   ✅ Consistency: Data integrity is maintained\n"
        else:
            report += "   ❌ Consistency: Data integrity may be compromised\n"
        
        report += f"""
{'='*80}
"""
        
        return report
    
    def save_report(self, filename: str = "test_report.txt"):
        """Save test report to file"""
        report = self.generate_report()
        
        try:
            with open(filename, 'w') as f:
                f.write(report)
            print(f"📄 Test report saved to: {filename}")
        except Exception as e:
            print(f"❌ Failed to save report: {e}")

def main():
    """Main function to run all tests"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run comprehensive tests for distributed Milvus')
    parser.add_argument('--suites', nargs='+', 
                       choices=['replication_test', 'failover_test', 'performance_test', 'chaos_test', 'consistency_test'],
                       help='Specific test suites to run (default: all)')
    parser.add_argument('--report', default='test_report.txt',
                       help='Output file for test report (default: test_report.txt)')
    parser.add_argument('--quick', action='store_true',
                       help='Run only essential tests (replication and failover)')
    
    args = parser.parse_args()
    
    # Determine test suites to run
    if args.quick:
        test_suites = ['replication_test', 'failover_test']
    elif args.suites:
        test_suites = args.suites
    else:
        test_suites = None  # Run all
    
    # Run tests
    runner = TestRunner()
    results = runner.run_all_tests(test_suites)
    
    # Generate and save report
    print(runner.generate_report())
    runner.save_report(args.report)
    
    # Exit with appropriate code
    if all(results.values()):
        sys.exit(0)  # All tests passed
    else:
        sys.exit(1)  # Some tests failed

if __name__ == "__main__":
    main()
