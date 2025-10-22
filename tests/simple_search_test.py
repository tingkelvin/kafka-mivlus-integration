#!/usr/bin/env python3
"""
Simple Search Test
Stimulates a search to see what happens
"""

import time
from docker_utils import DockerManager
from database_utils import quick_search_test, quick_query_test

def test_search():
    """
    Test a simple search operation
    """
    print("   🔍 Attempting search...")
    return quick_search_test("test_collection")

def test_query():
    """
    Test a simple query operation
    """
    print("   🔍 Attempting query...")
    return quick_query_test("test_collection")

def main():
    """
    Run simple search test
    """
    print("SIMPLE SEARCH TEST")
    print("="*60)
    print("This test stimulates search operations to see what happens")
    print("="*60)
    
    # Test 1: Basic search
    print("\n📊 Test 1: Basic Search")
    search_success = test_search()
    
    # Test 2: Basic query
    print("\n📊 Test 2: Basic Query")
    query_success = test_query()
    
    # Test 3: Multiple searches
    print("\n📊 Test 3: Multiple Searches")
    for i in range(3):
        print(f"   Search {i+1}/3:")
        test_search()
        time.sleep(1)
    
    # Test 4: Multiple queries
    print("\n📊 Test 4: Multiple Queries")
    for i in range(3):
        print(f"   Query {i+1}/3:")
        test_query()
        time.sleep(1)
    
    # Summary
    print("\n" + "="*60)
    print("SIMPLE SEARCH TEST SUMMARY")
    print("="*60)
    print(f"   Basic search: {'✅ PASS' if search_success else '❌ FAIL'}")
    print(f"   Basic query: {'✅ PASS' if query_success else '❌ FAIL'}")
    print("   Multiple operations: See results above")
    print("="*60)

if __name__ == "__main__":
    main()
