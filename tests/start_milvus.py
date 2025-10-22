#!/usr/bin/env python3
"""
Milvus Docker Startup Script
Ensures all required Docker containers are running before tests
"""

import sys
import time
from database_utils import check_docker_status, ensure_all_containers_running

def main():
    """Main function to start all Milvus containers"""
    print("="*60)
    print("MILVUS DOCKER STARTUP SCRIPT")
    print("="*60)
    
    # Check current status
    print("\n🔍 Checking current Docker container status...")
    status = check_docker_status()
    
    running_count = sum(status.values())
    total_count = len(status)
    
    print(f"📊 Status: {running_count}/{total_count} containers running")
    
    # Show detailed status
    print("\n📋 Container Status:")
    for container, is_running in status.items():
        status_icon = "✅" if is_running else "❌"
        print(f"   {status_icon} {container}")
    
    if running_count == total_count:
        print("\n🎉 All containers are already running!")
        return True
    
    # Start missing containers
    print(f"\n🔄 Starting {total_count - running_count} stopped containers...")
    try:
        success = ensure_all_containers_running()
        if success:
            print("\n🎉 All containers are now running and Milvus is ready!")
            
            # Show final status
            print("\n📋 Final Container Status:")
            final_status = check_docker_status()
            for container, is_running in final_status.items():
                status_icon = "✅" if is_running else "❌"
                print(f"   {status_icon} {container}")
            
            return True
        else:
            print("\n❌ Failed to start all containers")
            return False
            
    except Exception as e:
        print(f"\n❌ Error starting containers: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
