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
import socket
import traceback
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NoNodeError

class QuorumMonitor:
    """
    Monitors discovery and broker services to maintain quorum of at least 2 nodes.
    Automatically recovers failed nodes by restarting them on their designated VMs.
    """
    def __init__(self, zk_addr, logger=None):
        self.zk_addr = zk_addr
        self.zk = None
        self.running = True
        self.shutting_down = False
        
        # Configure logger
        if logger is None:
            self.logger = logging.getLogger("QuorumMonitor")
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
            
        # ZooKeeper paths
        self.discovery_path = "/discovery"
        self.broker_path = "/brokers"
        
        # Recovery status tracking
        self.broker_recovery_in_progress = False
        self.discovery_recovery_in_progress = False
        self.broker_recovery_lock = threading.Lock()
        self.discovery_recovery_lock = threading.Lock()
        
        # Flap detection and stabilization
        self.broker_state_history = []
        self.discovery_state_history = []
        self.flap_window_seconds = 10
        self.verification_count = 3
        self.verification_delay_seconds = 2
        self.stabilization_period = 30  # Wait 30 seconds after detecting a flap before attempting recovery
        self.last_broker_flap_time = 0
        self.last_discovery_flap_time = 0
        
        # Get hostname for logging
        self.hostname = socket.gethostname()
        self.logger.info(f"Monitor starting on host: {self.hostname}")
        
        # Register signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
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
                if not self.shutting_down:  # Don't set up watches during shutdown
                    threading.Thread(target=self.setup_watches, name="SetupWatchesThread").start()
        
        self.zk.start()
        self.logger.info(f"ZooKeeper connected with session ID: {self.zk._session_id}")
        
        # Ensure paths exist
        self._ensure_paths_exist()
    
    def _ensure_paths_exist(self):
        """Ensure necessary ZooKeeper paths exist"""
        paths = [self.discovery_path, self.broker_path]
        for path in paths:
            try:
                if not self.zk.exists(path):
                    self.logger.info(f"Creating path: {path}")
                    self.zk.create(path, makepath=True)
                else:
                    children = self.zk.get_children(path)
                    self.logger.info(f"Path {path} exists with children: {children}")
            except Exception as e:
                self.logger.error(f"Error ensuring path {path} exists: {str(e)}")
    
    def _list_zk_root(self):
        """Non-recursive simple listing of ZooKeeper root paths for debugging"""
        self.logger.info("==== ZooKeeper Root Paths ====")
        try:
            root_children = self.zk.get_children('/')
            self.logger.info(f"Root paths: {root_children}")
            
            # Check broker path specifically
            if 'brokers' in root_children:
                broker_children = self.zk.get_children('/brokers')
                self.logger.info(f"Broker children: {broker_children}")
            
            # Check discovery path specifically
            if 'discovery' in root_children:
                discovery_children = self.zk.get_children('/discovery')
                self.logger.info(f"Discovery children: {discovery_children}")
                
        except Exception as e:
            self.logger.error(f"Error listing ZK root: {str(e)}", exc_info=True)
        self.logger.info("==============================")
    
    def setup_watches(self):
        """Set up watches on discovery and broker nodes"""
        self.logger.info("Setting up watches on discovery and broker nodes")
        
        # Print ZooKeeper structure for debugging
        self._list_zk_root()
        
        try:
            # Discovery path
            if self.zk.exists(self.discovery_path):
                children = self.zk.get_children(self.discovery_path)
                self.logger.info(f"Path {self.discovery_path} exists with children: {children}")
                
                # Set up watch on discovery path
                def discovery_watch_callback(children):
                    # Only check quorum if not already in recovery and not shutting down
                    self.logger.info(f"*** DISCOVERY WATCH TRIGGERED *** Current nodes: {children}")
                    if not self.discovery_recovery_in_progress and not self.shutting_down:
                        threading.Timer(1.0, self._check_discovery_quorum).start()  # Slight delay to allow stabilization
                    return True  # Keep the watch active
                
                # Set up ChildrenWatch on discovery path
                self.discovery_watch = self.zk.ChildrenWatch(self.discovery_path, discovery_watch_callback)
                self.logger.info(f"Successfully set up discovery watch")
            else:
                self.logger.error(f"Discovery path {self.discovery_path} does not exist")
                
            # Broker path
            if self.zk.exists(self.broker_path):
                children = self.zk.get_children(self.broker_path)
                self.logger.info(f"Path {self.broker_path} exists with children: {children}")
                
                # Set up watch on broker path
                def broker_watch_callback(children):
                    # Only check quorum if not already in recovery and not shutting down
                    self.logger.info(f"*** BROKER WATCH TRIGGERED *** Current nodes: {children}")
                    if not self.broker_recovery_in_progress and not self.shutting_down:
                        threading.Timer(1.0, self._check_broker_quorum).start()  # Slight delay to allow stabilization
                    return True  # Keep the watch active
                
                # Set up ChildrenWatch on broker path
                self.broker_watch = self.zk.ChildrenWatch(self.broker_path, broker_watch_callback)
                self.logger.info(f"Successfully set up broker watch")
            else:
                self.logger.error(f"Broker path {self.broker_path} does not exist")
                
        except Exception as e:
            self.logger.error(f"Error setting up watches: {str(e)}", exc_info=True)
    
    def _check_discovery_quorum(self):
        """Check if discovery quorum is maintained and initiate recovery if needed"""
        if self.shutting_down:
            self.logger.info("Skipping discovery quorum check during shutdown")
            return
            
        try:
            children = self.zk.get_children(self.discovery_path)
            self.logger.info(f"Checking discovery quorum: Current nodes ({len(children)}): {children}")
            
            # Record state for flap detection
            current_time = time.time()
            self.discovery_state_history.append((current_time, len(children), children))
            
            # Remove old history entries
            self.discovery_state_history = [entry for entry in self.discovery_state_history 
                                           if current_time - entry[0] <= self.flap_window_seconds]
            
            # Check for flapping (rapid changes in node count)
            if self._detect_flapping(self.discovery_state_history):
                self.last_discovery_flap_time = current_time
                self.logger.warning(f"Discovery node flapping detected! Waiting for stabilization.")
                return
                
            # Don't attempt recovery if too soon after flapping
            if current_time - self.last_discovery_flap_time < self.stabilization_period:
                self.logger.info(f"In stabilization period after flapping. Skipping recovery check.")
                return
            
            if len(children) < 2:
                self.logger.warning(f"Potential discovery quorum loss! Only {len(children)} nodes active")
                
                # Perform multiple verification checks with delay
                verified_loss = True
                for i in range(self.verification_count):
                    if self.shutting_down:
                        return
                        
                    time.sleep(self.verification_delay_seconds)
                    verify_children = self.zk.get_children(self.discovery_path)
                    self.logger.info(f"Verification check {i+1}/{self.verification_count}: {len(verify_children)} discovery nodes")
                    
                    if len(verify_children) >= 2:
                        self.logger.info(f"Discovery quorum restored during verification. No recovery needed.")
                        verified_loss = False
                        break
                
                if not verified_loss:
                    return
                    
                # Still below quorum after all verification checks
                self.logger.warning(f"Confirmed discovery quorum loss after {self.verification_count} verification checks")
                
                with self.discovery_recovery_lock:
                    if not self.discovery_recovery_in_progress and not self.shutting_down:
                        self.logger.warning(f"Starting discovery recovery process")
                        self.discovery_recovery_in_progress = True
                        recovery_thread = threading.Thread(
                            target=self._recover_discovery_node, 
                            args=(children,),
                            name="DiscoveryRecovery"
                        )
                        recovery_thread.daemon = True
                        recovery_thread.start()
                        self.logger.info(f"Started discovery recovery thread: {recovery_thread.name}")
                    else:
                        self.logger.info("Skipping discovery recovery: already in progress or shutting down")
            else:
                self.logger.info(f"Discovery quorum OK: {len(children)} nodes active")
        except Exception as e:
            self.logger.error(f"Error checking discovery quorum: {str(e)}", exc_info=True)
    
    def _check_broker_quorum(self):
        """Check if broker quorum is maintained and initiate recovery if needed"""
        if self.shutting_down:
            self.logger.info("Skipping broker quorum check during shutdown")
            return
            
        try:
            self.logger.info(f"Checking broker quorum on path: {self.broker_path}")
            children = self.zk.get_children(self.broker_path)
            self.logger.info(f"Current broker nodes ({len(children)}): {children}")
            
            # Record state for flap detection
            current_time = time.time()
            self.broker_state_history.append((current_time, len(children), children))
            
            # Remove old history entries
            self.broker_state_history = [entry for entry in self.broker_state_history 
                                        if current_time - entry[0] <= self.flap_window_seconds]
            
            # Check for flapping (rapid changes in node count)
            if self._detect_flapping(self.broker_state_history):
                self.last_broker_flap_time = current_time
                self.logger.warning(f"Broker node flapping detected! Waiting for stabilization.")
                return
                
            # Don't attempt recovery if too soon after flapping
            if current_time - self.last_broker_flap_time < self.stabilization_period:
                self.logger.info(f"In stabilization period after flapping. Skipping recovery check.")
                return
            
            if len(children) < 2:
                self.logger.warning(f"Potential broker quorum loss! Only {len(children)} nodes active")
                
                # Perform multiple verification checks with delay
                verified_loss = True
                for i in range(self.verification_count):
                    if self.shutting_down:
                        return
                        
                    time.sleep(self.verification_delay_seconds)
                    verify_children = self.zk.get_children(self.broker_path)
                    self.logger.info(f"Verification check {i+1}/{self.verification_count}: {len(verify_children)} broker nodes")
                    
                    if len(verify_children) >= 2:
                        self.logger.info(f"Broker quorum restored during verification. No recovery needed.")
                        verified_loss = False
                        break
                
                if not verified_loss:
                    return
                    
                # Still below quorum after all verification checks
                self.logger.warning(f"Confirmed broker quorum loss after {self.verification_count} verification checks")
                
                with self.broker_recovery_lock:
                    if not self.broker_recovery_in_progress and not self.shutting_down:
                        self.logger.warning(f"Starting broker recovery process")
                        self.broker_recovery_in_progress = True
                        recovery_thread = threading.Thread(
                            target=self._recover_broker_node, 
                            args=(children,),
                            name="BrokerRecovery"
                        )
                        recovery_thread.daemon = True
                        recovery_thread.start()
                        self.logger.info(f"Started broker recovery thread: {recovery_thread.name}")
                    else:
                        self.logger.info("Skipping broker recovery: already in progress or shutting down")
            else:
                self.logger.info(f"Broker quorum OK: {len(children)} nodes active")
        except Exception as e:
            self.logger.error(f"Error checking broker quorum: {str(e)}", exc_info=True)
    
    def _detect_flapping(self, state_history):
        """Detect if nodes are flapping (rapidly changing state)"""
        if len(state_history) < 3:
            return False
            
        # Count state changes within our window
        changes = 0
        previous_count = state_history[0][1]
        
        for entry in state_history[1:]:
            if entry[1] != previous_count:
                changes += 1
                previous_count = entry[1]
                
        # If we've seen more than 2 changes in our window, that's flapping
        if changes >= 2:
            self.logger.warning(f"Detected {changes} state changes in the last {self.flap_window_seconds} seconds")
            return True
            
        return False
    
    def _recover_discovery_node(self, active_nodes):
        """Recover a discovery node by identifying which one is missing and restarting it"""
        # Don't proceed with recovery if shutting down
        if self.shutting_down:
            self.logger.info("Skipping discovery recovery during shutdown")
            with self.discovery_recovery_lock:
                self.discovery_recovery_in_progress = False
            return
            
        try:
            self.logger.info(f"Starting discovery node recovery. Active nodes: {active_nodes}")
            
            # Double-check quorum before proceeding
            current_nodes = self.zk.get_children(self.discovery_path)
            if len(current_nodes) >= 2:
                self.logger.info(f"Discovery quorum already restored to {len(current_nodes)} nodes: {current_nodes}")
                with self.discovery_recovery_lock:
                    self.discovery_recovery_in_progress = False
                return
            
            # Determine which VM's service is down
            vm2_active = any("vm2" in node for node in active_nodes)
            vm3_active = any("vm3" in node for node in active_nodes)
            
            if not vm2_active and not vm3_active:
                # Both are down, start with VM2
                target_vm = "vm2"
                self.logger.info("Both discovery nodes are down. Starting with VM2.")
            elif vm2_active and not vm3_active:
                target_vm = "vm3"
                self.logger.info("Discovery node on VM3 is down, recovering it.")
            elif not vm2_active and vm3_active:
                target_vm = "vm2"
                self.logger.info("Discovery node on VM2 is down, recovering it.")
            else:
                # Both are active, no recovery needed
                self.logger.info("All discovery nodes appear to be active, no recovery needed")
                with self.discovery_recovery_lock:
                    self.discovery_recovery_in_progress = False
                return
            
            # Check if we're shutting down before proceeding with recovery
            if self.shutting_down:
                self.logger.info("Aborting recovery because monitor is shutting down")
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
            self.logger.error(f"Error during discovery recovery: {str(e)}", exc_info=True)
        finally:
            if not self.shutting_down:
                with self.discovery_recovery_lock:
                    self.discovery_recovery_in_progress = False
                    self.logger.info("Discovery recovery process completed")
    
    def _recover_broker_node(self, active_nodes):
        """Recover a broker node by identifying which one is missing and restarting it"""
        # Don't proceed with recovery if shutting down
        if self.shutting_down:
            self.logger.info("Skipping broker recovery during shutdown")
            with self.broker_recovery_lock:
                self.broker_recovery_in_progress = False
            return
            
        try:
            self.logger.info(f"Starting broker node recovery. Active nodes: {active_nodes}")
            
            # Double-check quorum before proceeding
            current_nodes = self.zk.get_children(self.broker_path)
            if len(current_nodes) >= 2:
                self.logger.info(f"Broker quorum already restored to {len(current_nodes)} nodes: {current_nodes}")
                with self.broker_recovery_lock:
                    self.broker_recovery_in_progress = False
                return
            
            # Determine which VM's service is down by looking for vm4/vm5 in node names
            vm4_active = any("vm4" in node for node in active_nodes)
            vm5_active = any("vm5" in node for node in active_nodes)
            
            self.logger.info(f"Broker status - VM4: {'ACTIVE' if vm4_active else 'DOWN'}, VM5: {'ACTIVE' if vm5_active else 'DOWN'}")
            
            if not vm4_active and not vm5_active:
                # Both are down, start with VM4
                target_vm = "vm4"
                self.logger.info("Both broker nodes are down. Starting with VM4.")
            elif vm4_active and not vm5_active:
                target_vm = "vm5"
                self.logger.info("Broker node on VM5 is down, recovering it.")
            elif not vm4_active and vm5_active:
                target_vm = "vm4"
                self.logger.info("Broker node on VM4 is down, recovering it.")
            else:
                # Both are active, no recovery needed
                self.logger.info("All broker nodes appear to be active, no recovery needed")
                with self.broker_recovery_lock:
                    self.broker_recovery_in_progress = False
                return
            
            # Check if we're shutting down before proceeding with recovery
            if self.shutting_down:
                self.logger.info("Aborting recovery because monitor is shutting down")
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
            self.logger.error(f"Error during broker recovery: {str(e)}", exc_info=True)
        finally:
            if not self.shutting_down:
                with self.broker_recovery_lock:
                    self.broker_recovery_in_progress = False
                    self.logger.info("Broker recovery process completed")
    
    def _execute_remote_command(self, ip, command):
        """Execute a command on a remote VM using SSH"""
        # Don't execute commands if shutting down
        if self.shutting_down:
            self.logger.info(f"Skipping remote command execution during shutdown")
            return
            
        try:
            # Add SSH options to avoid host key checking
            background_command = f"nohup {command} > recovery.log 2>&1 &"
            
            # Use SSH with options to avoid the host key verification prompt
            ssh_command = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {ip} '{background_command}'"
            self.logger.info(f"Executing recovery command on {ip}: {ssh_command}")
            
            # Execute the command
            process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=10)
            
            if process.returncode != 0:
                self.logger.error(f"Command failed with return code {process.returncode}")
                self.logger.error(f"Stderr: {stderr.decode('utf-8')}")
            else:
                self.logger.info(f"Command executed successfully on {ip}")
                
        except subprocess.TimeoutExpired:
            self.logger.warning("SSH command timed out, but may still be running on remote host")
        except Exception as e:
            self.logger.error(f"Error executing remote command: {str(e)}", exc_info=True)
    
    def _wait_for_discovery_quorum(self):
        """Wait for discovery quorum to be restored, with timeout"""
        # Don't wait if shutting down
        if self.shutting_down:
            self.logger.info("Skipping discovery quorum wait during shutdown")
            return False
            
        timeout = 60  # seconds
        start_time = time.time()
        
        self.logger.info("Waiting for discovery quorum to be restored...")
        
        while time.time() - start_time < timeout and self.running and not self.shutting_down:
            try:
                children = self.zk.get_children(self.discovery_path)
                self.logger.info(f"Discovery recovery - current node count: {len(children)}, nodes: {children}")
                if len(children) >= 2:
                    self.logger.info(f"Discovery quorum restored! Current nodes: {children}")
                    return True
            except Exception as e:
                self.logger.error(f"Error while waiting for discovery quorum: {str(e)}")
            
            time.sleep(2)
        
        if not self.running or self.shutting_down:
            self.logger.info("Recovery wait interrupted because monitor is shutting down")
        else:
            self.logger.warning(f"Timed out waiting for discovery quorum to be restored")
        return False
    
    def _wait_for_broker_quorum(self):
        """Wait for broker quorum to be restored, with timeout"""
        # Don't wait if shutting down
        if self.shutting_down:
            self.logger.info("Skipping broker quorum wait during shutdown")
            return False
            
        timeout = 60  # seconds
        start_time = time.time()
        
        self.logger.info("Waiting for broker quorum to be restored...")
        
        while time.time() - start_time < timeout and self.running and not self.shutting_down:
            try:
                children = self.zk.get_children(self.broker_path)
                self.logger.info(f"Broker recovery - current node count: {len(children)}, nodes: {children}")
                if len(children) >= 2:
                    self.logger.info(f"Broker quorum restored! Current nodes: {children}")
                    return True
            except Exception as e:
                self.logger.error(f"Error while waiting for broker quorum: {str(e)}")
            
            time.sleep(2)
        
        if not self.running or self.shutting_down:
            self.logger.info("Recovery wait interrupted because monitor is shutting down")
        else:
            self.logger.warning(f"Timed out waiting for broker quorum to be restored")
        return False
    
    def run(self):
        """Main loop for the quorum monitor"""
        try:
            self.connect()
            self.setup_watches()
            
            self.logger.info("Quorum monitor started and running...")
            
            # Keep running until signaled to stop
            check_counter = 0
            while self.running:
                try:
                    time.sleep(5)
                    check_counter += 1
                    
                    # Every 30 seconds, do a manual check of quorums
                    if check_counter >= 6 and not self.shutting_down:
                        self.logger.info("Performing periodic quorum check")
                        self._check_discovery_quorum()
                        self._check_broker_quorum()
                        check_counter = 0
                        
                except KeyboardInterrupt:
                    self.logger.info("KeyboardInterrupt received in run loop, exiting")
                    self.shutting_down = True
                    self.running = False
                    break
                
        except KeyboardInterrupt:
            self.logger.info("Monitor service stopped by user via KeyboardInterrupt")
            self.shutting_down = True
        except Exception as e:
            self.logger.error(f"Unexpected error in monitor service: {str(e)}", exc_info=True)
            # Log the full traceback for unhandled exceptions
            tb = traceback.format_exc()
            self.logger.error(f"Traceback: {tb}")
        finally:
            self.logger.info("Cleaning up before exit")
            self.cleanup()
            # Ensure we exit
            self.logger.info("Exiting program")
            sys.exit(0)
    
    def cleanup(self):
        """Clean up resources"""
        self.logger.info("Starting cleanup process")
        try:
            # Set shutdown flags to prevent new operations
            self.shutting_down = True
            self.running = False
            
            # Log all running threads
            active_threads = threading.enumerate()
            self.logger.info(f"Active threads during cleanup: {[t.name for t in active_threads]}")
            
            # Close ZooKeeper connection
            if self.zk:
                if self.zk.connected:
                    self.logger.info("Stopping ZooKeeper connection")
                    self.zk.stop()
                    self.zk.close()
                    self.logger.info("ZooKeeper connection closed")
                else:
                    self.logger.info("ZooKeeper was not connected")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
            # Don't force exit here - let the normal exit happen
    
    def signal_handler(self, sig, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {sig}, shutting down")
        
        # Set shutdown flags to prevent new operations
        self.shutting_down = True
        self.running = False
        
        # Set recovery flags to prevent new recovery attempts during shutdown
        with self.broker_recovery_lock:
            self.broker_recovery_in_progress = True  # Prevent new recovery during shutdown
        
        with self.discovery_recovery_lock:
            self.discovery_recovery_in_progress = True  # Prevent new recovery during shutdown

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
    logger.info(f"Starting Quorum Monitor with loglevel {args.loglevel}")
    
    # Create and run the monitor
    monitor = QuorumMonitor(args.zk_addr, logger)
    
    # Start the monitor
    monitor.run() 