#!/usr/bin/env python3
###############################################
#
# Purpose: Monitor and recover quorums for broker and discovery services
#
###############################################
import time
import logging
import argparse
import json
import os
import sys
import subprocess
import threading
import signal
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NoNodeError

class QuorumMonitor:
    """
    Monitors discovery and broker services to maintain quorum of at least 2 nodes.
    Automatically recovers failed nodes by restarting them on their designated VMs.
    """
    def __init__(self, zk_addr, logger=None):
        self.zk_addr = zk_addr
        self.logger = logger or logging.getLogger("QuorumMonitor")
        self.zk = None
        self.discovery_path = "/discovery"
        self.broker_path = "/brokers"
        self.discovery_recovery_lock = threading.Lock()
        self.broker_recovery_lock = threading.Lock()
        self.discovery_recovery_in_progress = False
        self.broker_recovery_in_progress = False
        self.running = True
        
        # VM configuration for recovery
        self.vm_config = {
            "discovery": {
                "vm2": {
                    "ip": "192.168.5.139", 
                    "command": "python3 ~/CS6381/PA2/DiscoveryAppln.py --addr 192.168.5.139 --port 5555 --zk_addr 192.168.5.17:2181 --loglevel 20"
                },
                "vm3": {
                    "ip": "192.168.5.175", 
                    "command": "python3 ~/CS6381/PA2/DiscoveryAppln.py --addr 192.168.5.175 --port 5555 --zk_addr 192.168.5.17:2181 --loglevel 20"
                }
            },
            "broker": {
                "vm4": {
                    "ip": "192.168.5.126", 
                    "command": "python3 ~/CS6381/PA2/BrokerAppln.py --name broker-distsys-team3-vm4 --addr 192.168.5.126 --port 5566 --zk_addr 192.168.5.17:2181 --discovery 192.168.5.139:5555 --loglevel 20"
                },
                "vm5": {
                    "ip": "192.168.5.178", 
                    "command": "python3 ~/CS6381/PA2/BrokerAppln.py --name broker-distsys-team3-vm5 --addr 192.168.5.178 --port 5566 --zk_addr 192.168.5.17:2181 --discovery 192.168.5.139:5555 --loglevel 20"
                }
            }
        }
    
    def connect(self):
        """Connect to ZooKeeper and set up connection state handling"""
        self.logger.info(f"Connecting to ZooKeeper at {self.zk_addr}")
        self.zk = KazooClient(hosts=self.zk_addr)
        
        # Register connection state handler
        @self.zk.add_listener
        def connection_listener(state):
            if state == KazooState.LOST:
                self.logger.warning("ZooKeeper connection lost")
            elif state == KazooState.SUSPENDED:
                self.logger.warning("ZooKeeper connection suspended")
            else:
                self.logger.info("ZooKeeper connected")
                # Re-establish watches when reconnected
                self.setup_watches()
        
        self.zk.start()
        
        # Ensure paths exist
        self._ensure_paths_exist()
    
    def _ensure_paths_exist(self):
        """Ensure necessary ZooKeeper paths exist"""
        paths = [self.discovery_path, self.broker_path]
        for path in paths:
            if not self.zk.exists(path):
                self.logger.info(f"Creating path: {path}")
                self.zk.create(path, makepath=True)
    
    def setup_watches(self):
        """Setup watches on discovery and broker nodes"""
        self.logger.info("Setting up watches on discovery and broker nodes")
        
        # Use ChildrenWatch for continuous monitoring
        self.zk.ChildrenWatch(self.discovery_path, self._handle_discovery_change)
        self.zk.ChildrenWatch(self.broker_path, self._handle_broker_change)
        
        # Initial check
        self._check_discovery_quorum()
        self._check_broker_quorum()
    
    def _handle_discovery_change(self, children):
        """Handle changes in discovery nodes"""
        self.logger.info(f"Discovery nodes changed: {children}")
        self._check_discovery_quorum()
        return True  # Keep the watch active
    
    def _handle_broker_change(self, children):
        """Handle changes in broker nodes"""
        self.logger.info(f"Broker nodes changed: {children}")
        self._check_broker_quorum()
        return True  # Keep the watch active
    
    def _check_discovery_quorum(self):
        """Check if discovery quorum is maintained and initiate recovery if needed"""
        try:
            children = self.zk.get_children(self.discovery_path)
            self.logger.info(f"Current discovery nodes: {children}")
            
            if len(children) < 2:
                with self.discovery_recovery_lock:
                    if not self.discovery_recovery_in_progress:
                        self.logger.warning("Discovery quorum lost! Starting recovery...")
                        self.discovery_recovery_in_progress = True
                        threading.Thread(target=self._recover_discovery_node, args=(children,)).start()
        except Exception as e:
            self.logger.error(f"Error checking discovery quorum: {str(e)}")
    
    def _check_broker_quorum(self):
        """Check if broker quorum is maintained and initiate recovery if needed"""
        try:
            children = self.zk.get_children(self.broker_path)
            self.logger.info(f"Current broker nodes: {children}")
            
            if len(children) < 2:
                with self.broker_recovery_lock:
                    if not self.broker_recovery_in_progress:
                        self.logger.warning("Broker quorum lost! Starting recovery...")
                        self.broker_recovery_in_progress = True
                        threading.Thread(target=self._recover_broker_node, args=(children,)).start()
        except Exception as e:
            self.logger.error(f"Error checking broker quorum: {str(e)}")
    
    def _recover_discovery_node(self, active_nodes):
        """Recover a discovery node by identifying which one is missing and restarting it"""
        try:
            self.logger.info(f"Starting discovery node recovery. Active nodes: {active_nodes}")
            
            # Determine which VM's service is down
            vm2_active = any("vm2" in node for node in active_nodes)
            vm3_active = any("vm3" in node for node in active_nodes)
            
            if not vm2_active and not vm3_active:
                # Both are down, start with VM2
                target_vm = "vm2"
            elif vm2_active and not vm3_active:
                target_vm = "vm3"
            elif not vm2_active and vm3_active:
                target_vm = "vm2"
            else:
                # Both are active, no recovery needed
                self.logger.info("All discovery nodes appear to be active, no recovery needed")
                with self.discovery_recovery_lock:
                    self.discovery_recovery_in_progress = False
                return
            
            self.logger.info(f"Recovering discovery node on {target_vm}")
            
            # Get the configuration for the target VM
            vm_config = self.vm_config["discovery"][target_vm]
            
            # Execute the command to restart the service on the target VM
            self._execute_remote_command(vm_config["ip"], vm_config["command"])
            
            # Wait for the node to register with ZooKeeper
            self._wait_for_discovery_quorum()
            
        except Exception as e:
            self.logger.error(f"Error during discovery recovery: {str(e)}")
        finally:
            with self.discovery_recovery_lock:
                self.discovery_recovery_in_progress = False
    
    def _recover_broker_node(self, active_nodes):
        """Recover a broker node by identifying which one is missing and restarting it"""
        try:
            self.logger.info(f"Starting broker node recovery. Active nodes: {active_nodes}")
            
            # Determine which VM's service is down
            vm4_active = any("vm4" in node for node in active_nodes)
            vm5_active = any("vm5" in node for node in active_nodes)
            
            if not vm4_active and not vm5_active:
                # Both are down, start with VM4
                target_vm = "vm4"
            elif vm4_active and not vm5_active:
                target_vm = "vm5"
            elif not vm4_active and vm5_active:
                target_vm = "vm4"
            else:
                # Both are active, no recovery needed
                self.logger.info("All broker nodes appear to be active, no recovery needed")
                with self.broker_recovery_lock:
                    self.broker_recovery_in_progress = False
                return
            
            self.logger.info(f"Recovering broker node on {target_vm}")
            
            # Get the configuration for the target VM
            vm_config = self.vm_config["broker"][target_vm]
            
            # Execute the command to restart the service on the target VM
            self._execute_remote_command(vm_config["ip"], vm_config["command"])
            
            # Wait for the node to register with ZooKeeper
            self._wait_for_broker_quorum()
            
        except Exception as e:
            self.logger.error(f"Error during broker recovery: {str(e)}")
        finally:
            with self.broker_recovery_lock:
                self.broker_recovery_in_progress = False
    
    def _execute_remote_command(self, ip, command):
        """Execute a command on a remote VM using SSH"""
        try:
            # Make the command run in the background on the remote machine
            background_command = f"nohup {command} > recovery.log 2>&1 &"
            
            # Use SSH to execute the command
            ssh_command = f"ssh {ip} '{background_command}'"
            self.logger.info(f"Executing: {ssh_command}")
            
            # Execute the command
            process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=10)
            
            if process.returncode != 0:
                self.logger.error(f"Command failed with return code {process.returncode}")
                self.logger.error(f"Stderr: {stderr.decode('utf-8')}")
            else:
                self.logger.info(f"Command executed successfully")
                
        except subprocess.TimeoutExpired:
            self.logger.warning("SSH command timed out, but may still be running on remote host")
        except Exception as e:
            self.logger.error(f"Error executing remote command: {str(e)}")
    
    def _wait_for_discovery_quorum(self):
        """Wait for discovery quorum to be restored, with timeout"""
        timeout = 60  # seconds
        start_time = time.time()
        
        self.logger.info("Waiting for discovery quorum to be restored...")
        
        while time.time() - start_time < timeout:
            try:
                children = self.zk.get_children(self.discovery_path)
                if len(children) >= 2:
                    self.logger.info(f"Discovery quorum restored! Current nodes: {children}")
                    return True
            except Exception as e:
                self.logger.error(f"Error while waiting for discovery quorum: {str(e)}")
            
            time.sleep(2)
        
        self.logger.warning(f"Timed out waiting for discovery quorum to be restored")
        return False
    
    def _wait_for_broker_quorum(self):
        """Wait for broker quorum to be restored, with timeout"""
        timeout = 60  # seconds
        start_time = time.time()
        
        self.logger.info("Waiting for broker quorum to be restored...")
        
        while time.time() - start_time < timeout:
            try:
                children = self.zk.get_children(self.broker_path)
                if len(children) >= 2:
                    self.logger.info(f"Broker quorum restored! Current nodes: {children}")
                    return True
            except Exception as e:
                self.logger.error(f"Error while waiting for broker quorum: {str(e)}")
            
            time.sleep(2)
        
        self.logger.warning(f"Timed out waiting for broker quorum to be restored")
        return False
    
    def run(self):
        """Main loop for the quorum monitor"""
        try:
            self.connect()
            self.setup_watches()
            
            self.logger.info("Quorum monitor started")
            
            # Keep running until signaled to stop
            while self.running:
                try:
                    time.sleep(1)
                except KeyboardInterrupt:
                    self.logger.info("KeyboardInterrupt received, exiting")
                    self.running = False
                    break
                
        except KeyboardInterrupt:
            self.logger.info("Monitor service stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in monitor service: {str(e)}")
        finally:
            self.cleanup()
            # Ensure we exit
            self.logger.info("Exiting program")
            sys.exit(0)
    
    def cleanup(self):
        """Clean up resources"""
        try:
            # Stop any running threads
            for thread in threading.enumerate():
                if thread is not threading.main_thread():
                    self.logger.info(f"Terminating thread: {thread.name}")
            
            # Close ZooKeeper connection
            if self.zk and self.zk.connected:
                self.zk.stop()
                self.zk.close()
                self.logger.info("ZooKeeper connection closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            # Force exit if cleanup fails
            os._exit(1)
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {sig}, shutting down")
        self.running = False
        self.cleanup()
        # Force exit after a brief delay
        threading.Timer(3.0, lambda: os._exit(0)).start()

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Monitor and maintain quorum for discovery and broker services")
    parser.add_argument("--zk_addr", default="192.168.5.17:2181", help="ZooKeeper address")
    parser.add_argument("--loglevel", type=int, default=logging.INFO, help="Logging level")
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=args.loglevel,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("quorum_monitor.log")
        ]
    )
    
    logger = logging.getLogger("QuorumMonitor")
    
    # Create and run the monitor
    monitor = QuorumMonitor(args.zk_addr, logger)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, monitor.signal_handler)
    signal.signal(signal.SIGTERM, monitor.signal_handler)
    
    # Start the monitor
    monitor.run() 