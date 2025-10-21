#!/usr/bin/env python3
"""
Docker Utilities for Milvus Testing
Common functions for Docker container management and verification
"""

import subprocess
import time
import logging
from typing import List, Dict, Optional, Tuple

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DockerManager:
    """
    Docker container management utilities for Milvus testing
    """
    
    def __init__(self):
        self.milvus_containers = [
            'milvus-querynode1',
            'milvus-querynode2', 
            'milvus-datanode1',
            'milvus-datanode2',
            'milvus-indexnode1',
            'milvus-indexnode2',
            'milvus-proxy',
            'milvus-rootcoord',
            'milvus-datacoord',
            'milvus-querycoord',
            'milvus-indexcoord',
            'milvus-etcd',
            'milvus-kafka',
            'milvus-minio',
            'milvus-attu'
        ]
    
    def get_container_status(self, container_name: str) -> Dict[str, str]:
        """
        Get detailed status of a specific container
        
        Args:
            container_name: Name of the container
            
        Returns:
            Dict with container status information
        """
        try:
            # First check if container exists and is running
            result = subprocess.run([
                'docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}'
            ], capture_output=True, text=True)
            
            if container_name not in result.stdout:
                # Container not running, check if it exists
                result_all = subprocess.run([
                    'docker', 'ps', '-a', '--filter', f'name={container_name}', '--format', '{{.Names}}'
                ], capture_output=True, text=True)
                
                if container_name not in result_all.stdout:
                    return {
                        'status': 'not_found',
                        'health': 'unknown',
                        'started_at': 'unknown',
                        'image': 'unknown'
                    }
                else:
                    # Container exists but not running
                    return {
                        'status': 'stopped',
                        'health': 'unknown',
                        'started_at': 'unknown',
                        'image': 'unknown'
                    }
            
            # Container is running, get detailed info
            inspect_result = subprocess.run([
                'docker', 'inspect', container_name,
                '--format', '{{.State.Status}}|{{.State.Health.Status}}|{{.State.StartedAt}}|{{.Config.Image}}'
            ], capture_output=True, text=True)
            
            if inspect_result.returncode != 0:
                return {
                    'status': 'running',
                    'health': 'unknown',
                    'started_at': 'unknown',
                    'image': 'unknown'
                }
            
            parts = inspect_result.stdout.strip().split('|')
            return {
                'status': parts[0] if len(parts) > 0 else 'running',
                'health': parts[1] if len(parts) > 1 else 'unknown',
                'started_at': parts[2] if len(parts) > 2 else 'unknown',
                'image': parts[3] if len(parts) > 3 else 'unknown'
            }
        except Exception as e:
            logger.error(f"Error getting status for {container_name}: {e}")
            return {'status': 'error', 'health': 'unknown', 'started_at': 'unknown', 'image': 'unknown'}
    
    def get_all_containers_status(self) -> Dict[str, Dict[str, str]]:
        """
        Get status of all Milvus containers
        
        Returns:
            Dict mapping container names to their status
        """
        status = {}
        for container in self.milvus_containers:
            status[container] = self.get_container_status(container)
        return status
    
    def is_container_running(self, container_name: str) -> bool:
        """
        Check if a container is running
        
        Args:
            container_name: Name of the container
            
        Returns:
            True if container is running, False otherwise
        """
        try:
            result = subprocess.run([
                'docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}'
            ], capture_output=True, text=True)
            
            return container_name in result.stdout
        except Exception as e:
            logger.error(f"Error checking if {container_name} is running: {e}")
            return False
    
    def is_container_stopped(self, container_name: str) -> bool:
        """
        Check if a container is stopped
        
        Args:
            container_name: Name of the container
            
        Returns:
            True if container is stopped, False otherwise
        """
        return not self.is_container_running(container_name)
    
    def stop_container(self, container_name: str, timeout: int = 30) -> bool:
        """
        Stop a container and verify it's stopped
        
        Args:
            container_name: Name of the container to stop
            timeout: Maximum time to wait for stop (seconds)
            
        Returns:
            True if container stopped successfully, False otherwise
        """
        try:
            logger.info(f"Stopping {container_name}...")
            
            # Stop the container
            result = subprocess.run(['docker', 'stop', container_name], 
                                  capture_output=True, text=True, timeout=timeout)
            
            if result.returncode != 0:
                logger.error(f"Failed to stop {container_name}: {result.stderr}")
                return False
            
            # Verify it's actually stopped
            return self.verify_container_stopped(container_name, max_attempts=10)
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout stopping {container_name}")
            return False
        except Exception as e:
            logger.error(f"Error stopping {container_name}: {e}")
            return False
    
    def start_container(self, container_name: str, timeout: int = 30) -> bool:
        """
        Start a container and verify it's running
        
        Args:
            container_name: Name of the container to start
            timeout: Maximum time to wait for start (seconds)
            
        Returns:
            True if container started successfully, False otherwise
        """
        try:
            logger.info(f"Starting {container_name}...")
            
            # Start the container
            result = subprocess.run(['docker', 'start', container_name], 
                                  capture_output=True, text=True, timeout=timeout)
            
            if result.returncode != 0:
                logger.error(f"Failed to start {container_name}: {result.stderr}")
                return False
            
            # Wait for it to be running
            return self.verify_container_running(container_name, max_attempts=15)
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout starting {container_name}")
            return False
        except Exception as e:
            logger.error(f"Error starting {container_name}: {e}")
            return False
    
    def restart_container(self, container_name: str, timeout: int = 30) -> bool:
        """
        Restart a container
        
        Args:
            container_name: Name of the container to restart
            timeout: Maximum time to wait for restart (seconds)
            
        Returns:
            True if container restarted successfully, False otherwise
        """
        try:
            logger.info(f"Restarting {container_name}...")
            
            # Restart the container
            result = subprocess.run(['docker', 'restart', container_name], 
                                  capture_output=True, text=True, timeout=timeout)
            
            if result.returncode != 0:
                logger.error(f"Failed to restart {container_name}: {result.stderr}")
                return False
            
            # Wait for it to be running
            return self.verify_container_running(container_name, max_attempts=15)
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout restarting {container_name}")
            return False
        except Exception as e:
            logger.error(f"Error restarting {container_name}: {e}")
            return False
    
    def verify_container_stopped(self, container_name: str, max_attempts: int = 10) -> bool:
        """
        Verify that a container is actually stopped
        
        Args:
            container_name: Name of the container
            max_attempts: Maximum number of attempts to check
            
        Returns:
            True if container is confirmed stopped, False otherwise
        """
        for attempt in range(max_attempts):
            if self.is_container_stopped(container_name):
                logger.info(f"✅ {container_name} is confirmed STOPPED (attempt {attempt + 1})")
                return True
            else:
                logger.info(f"⏳ {container_name} still running (attempt {attempt + 1})")
                time.sleep(2)
        
        logger.error(f"❌ {container_name} failed to stop after {max_attempts} attempts")
        return False
    
    def verify_container_running(self, container_name: str, max_attempts: int = 15) -> bool:
        """
        Verify that a container is actually running
        
        Args:
            container_name: Name of the container
            max_attempts: Maximum number of attempts to check
            
        Returns:
            True if container is confirmed running, False otherwise
        """
        for attempt in range(max_attempts):
            if self.is_container_running(container_name):
                logger.info(f"✅ {container_name} is confirmed RUNNING (attempt {attempt + 1})")
                return True
            else:
                logger.info(f"⏳ {container_name} still starting (attempt {attempt + 1})")
                time.sleep(2)
        
        logger.error(f"❌ {container_name} failed to start after {max_attempts} attempts")
        return False
    
    def get_container_logs(self, container_name: str, tail: int = 20) -> List[str]:
        """
        Get recent logs from a container
        
        Args:
            container_name: Name of the container
            tail: Number of lines to get
            
        Returns:
            List of log lines
        """
        try:
            result = subprocess.run([
                'docker', 'logs', '--tail', str(tail), container_name
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Failed to get logs for {container_name}: {result.stderr}")
                return []
            
            return result.stdout.strip().split('\n')
        except Exception as e:
            logger.error(f"Error getting logs for {container_name}: {e}")
            return []
    
    def get_container_logs_filtered(self, container_name: str, keywords: List[str], tail: int = 50) -> List[str]:
        """
        Get filtered logs from a container
        
        Args:
            container_name: Name of the container
            keywords: Keywords to filter for
            tail: Number of lines to get before filtering
            
        Returns:
            List of filtered log lines
        """
        try:
            logs = self.get_container_logs(container_name, tail)
            filtered = []
            
            for line in logs:
                if any(keyword.lower() in line.lower() for keyword in keywords):
                    filtered.append(line)
            
            return filtered
        except Exception as e:
            logger.error(f"Error getting filtered logs for {container_name}: {e}")
            return []
    
    def get_query_nodes_status(self) -> Dict[str, Dict[str, str]]:
        """
        Get status of all query nodes
        
        Returns:
            Dict with query node statuses
        """
        query_nodes = ['milvus-querynode1', 'milvus-querynode2']
        status = {}
        
        for node in query_nodes:
            status[node] = self.get_container_status(node)
        
        return status
    
    def get_data_nodes_status(self) -> Dict[str, Dict[str, str]]:
        """
        Get status of all data nodes
        
        Returns:
            Dict with data node statuses
        """
        data_nodes = ['milvus-datanode1', 'milvus-datanode2']
        status = {}
        
        for node in data_nodes:
            status[node] = self.get_container_status(node)
        
        return status
    
    def print_container_status_table(self, containers: Optional[List[str]] = None):
        """
        Print a formatted table of container statuses
        
        Args:
            containers: List of container names to check (default: all Milvus containers)
        """
        if containers is None:
            containers = self.milvus_containers
        
        print("\n📊 Container Status:")
        print("=" * 80)
        print(f"{'Container':<20} {'Status':<12} {'Health':<12} {'Image':<20}")
        print("-" * 80)
        
        for container in containers:
            status = self.get_container_status(container)
            print(f"{container:<20} {status['status']:<12} {status['health']:<12} {status['image']:<20}")
    
    def wait_for_containers_healthy(self, containers: List[str], timeout: int = 120) -> bool:
        """
        Wait for containers to be healthy
        
        Args:
            containers: List of container names to wait for
            timeout: Maximum time to wait (seconds)
            
        Returns:
            True if all containers are healthy, False otherwise
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_healthy = True
            
            for container in containers:
                status = self.get_container_status(container)
                if status['status'] != 'running' or status['health'] not in ['healthy', 'unknown']:
                    all_healthy = False
                    break
            
            if all_healthy:
                logger.info("✅ All containers are healthy")
                return True
            
            elapsed = int(time.time() - start_time)
            logger.info(f"⏳ Waiting for containers to be healthy... ({elapsed}s)")
            time.sleep(5)
        
        logger.error(f"❌ Timeout waiting for containers to be healthy after {timeout}s")
        return False
    
    def cleanup_containers(self, containers: List[str]) -> bool:
        """
        Stop and remove containers
        
        Args:
            containers: List of container names to cleanup
            
        Returns:
            True if cleanup successful, False otherwise
        """
        success = True
        
        for container in containers:
            try:
                # Stop container
                if self.is_container_running(container):
                    logger.info(f"Stopping {container}...")
                    subprocess.run(['docker', 'stop', container], capture_output=True)
                
                # Remove container
                logger.info(f"Removing {container}...")
                result = subprocess.run(['docker', 'rm', container], capture_output=True, text=True)
                
                if result.returncode != 0:
                    logger.error(f"Failed to remove {container}: {result.stderr}")
                    success = False
                else:
                    logger.info(f"✅ {container} removed successfully")
                    
            except Exception as e:
                logger.error(f"Error cleaning up {container}: {e}")
                success = False
        
        return success

# Convenience functions for common operations
def get_milvus_containers() -> List[str]:
    """Get list of all Milvus container names"""
    return [
        'milvus-querynode1', 'milvus-querynode2',
        'milvus-datanode1', 'milvus-datanode2',
        'milvus-indexnode1', 'milvus-indexnode2',
        'milvus-proxy', 'milvus-rootcoord',
        'milvus-datacoord', 'milvus-querycoord',
        'milvus-indexcoord', 'milvus-etcd',
        'milvus-kafka', 'milvus-minio', 'milvus-attu'
    ]

def quick_status_check() -> None:
    """Quick status check of all Milvus containers"""
    docker_manager = DockerManager()
    docker_manager.print_container_status_table()

def stop_query_nodes() -> bool:
    """Stop both query nodes"""
    docker_manager = DockerManager()
    success = True
    
    for node in ['milvus-querynode1', 'milvus-querynode2']:
        if not docker_manager.stop_container(node):
            success = False
    
    return success

def start_query_nodes() -> bool:
    """Start both query nodes"""
    docker_manager = DockerManager()
    success = True
    
    for node in ['milvus-querynode1', 'milvus-querynode2']:
        if not docker_manager.start_container(node):
            success = False
    
    return success

if __name__ == "__main__":
    # Example usage
    print("Docker Utilities for Milvus Testing")
    print("=" * 50)
    
    # Quick status check
    quick_status_check()
    
    # Example: Get query nodes status
    docker_manager = DockerManager()
    query_status = docker_manager.get_query_nodes_status()
    print(f"\nQuery Nodes Status: {query_status}")
