#!/usr/bin/env python3
"""
Test script to verify the pub/sub system functionality using Mininet
"""

import os
import sys
import time
import subprocess
import signal
import argparse
import logging
from mininet.net import Mininet
from mininet.topo import SingleSwitchTopo
from mininet.cli import CLI
from mininet.log import setLogLevel
from typing import List, Dict

class TestHarness:
    def __init__(self, logger, topo=None):
        self.logger = logger
        self.processes: Dict[str, subprocess.Popen] = {}
        if not topo:
          self.topo = SingleSwitchTopo(5)
        else:
            self.topo = topo
        self.net = Mininet(topo=self.topo)
        self.net.start()
        self.hosts = self.net.hosts
        # add external ip addresses to hosts for debugging purposes.
        for i, host in enumerate(self.hosts):
          host.cmd(f"ifconfig {host.name}-eth0 10.0.0.{i+1}/24")


    def cleanup(self):
        """Cleanup all running processes and stop Mininet"""
        for name, proc in self.processes.items():
            self.logger.info(f"Terminating {name}")
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=2)  # Give each process 2 seconds to cleanup
            except subprocess.TimeoutExpired:
                proc.kill()  # Force kill if it doesn't terminate
                proc.wait()
        if self.net:
            self.net.stop()

    def start_discovery(self, host_id: int, port: int, num_pubs: int, num_subs: int):
        """Start the discovery service on a Mininet host"""
        host = self.hosts[host_id]
        cmd = [
            "python3",
            "DiscoveryAppln.py",
            "-P", str(num_pubs),
            "-S", str(num_subs),
            "-p", str(port),
            "-a", f"{host.IP()}",
            "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Discovery Service on {host.name}: {' '.join(cmd)}")
        proc = host.popen(cmd)
        self.processes["discovery"] = proc
        time.sleep(2)  # Give discovery time to start

    def start_publisher(self, host_id: int, name: str, port: int, discovery_addr: str, topics: int = 1):
        """Start a publisher on a Mininet host"""
        host = self.hosts[host_id]
        cmd = [
            "python3",
            "PublisherAppln.py",
            "-n", name,
            "-p", str(port),
            "-a", f"{host.IP()}",
            "-d", discovery_addr,
            "-T", str(topics),
            "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Publisher {name} on {host.name}: {' '.join(cmd)}")
        proc = host.popen(cmd)
        self.processes[f"pub_{name}"] = proc
        time.sleep(1)  # Give publisher time to start

    def start_subscriber(self, host_id: int, name: str, discovery_addr: str, topics: int = 1):
        """Start a subscriber on a Mininet host"""
        host = self.hosts[host_id]
        cmd = [
            "python3",
            "SubscriberAppln.py",
            "-n", name,
            "-d", discovery_addr,
            "-T", str(topics),
             "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Subscriber {name} on {host.name}: {' '.join(cmd)}")
        proc = host.popen(cmd)
        self.processes[f"sub_{name}"] = proc
        time.sleep(1)  # Give subscriber time to start

    def start_broker(self, host_id: int, discovery_addr: str, broker_port: int):
        """Start the broker on a Mininet host"""
        host = self.hosts[host_id]
        cmd = [
            "python3",
            "BrokerAppln.py",
            "-n", "broker",
             "-d", discovery_addr,  # Discovery service address
             "-c", "config.ini",      # Config file (which should have dissemination strategy, etc.)
             "-a", f"{host.IP()}",         # Broker's bind address
             "-p", str(broker_port),            # Broker's publish port 
             "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Broker on {host.name}: {' '.join(cmd)}")
        proc = host.popen(cmd)
        self.processes["broker"] = proc
        time.sleep(2)  # Give broker time to start
    

def test_direct_dissemination(logger):
    """Test the direct dissemination strategy"""

    # Create test harness (mininet)
    harness = TestHarness(logger)
    try:
        # Update config to use direct
        with open("config.ini", "w") as f:
            f.write("[Discovery]\n")
            f.write("Strategy=Centralized\n\n")
            f.write("[Dissemination]\n")
            f.write("Strategy=Direct\n")
        
        # Start discovery service on h1
        harness.start_discovery(0, 5555, 2, 2)  # Host ID 0, 2 pubs, 2 subs
        
        discovery_address = f"{harness.hosts[0].IP()}:5555"

        # Start publishers on h2 and h3
        harness.start_publisher(1, "pub1", 5556, discovery_address, topics=2)  # Host ID 1
        harness.start_publisher(2, "pub2", 5557, discovery_address, topics=2)  # Host ID 2

        # Start subscribers on h4 and h5
        harness.start_subscriber(3, "sub1", discovery_address, topics=2)  # Host ID 3
        harness.start_subscriber(4, "sub2", discovery_address, topics=2) # Host ID 4

        # Let the system run for a while
        logger.info("System running - waiting 30 seconds")
        time.sleep(30)

    except Exception as e:
        logger.error(f"Exception in test: {str(e)}")
    finally:
        harness.cleanup()


def test_broker_dissemination(logger):
    """Test the broker-based dissemination strategy"""
    # Create test harness (mininet)
    harness = TestHarness(logger)
    try:
        # Update config to use broker
        with open("config.ini", "w") as f:
            f.write("[Discovery]\n")
            f.write("Strategy=Centralized\n\n")
            f.write("[Dissemination]\n")
            f.write("Strategy=ViaBroker\n")


        
        # Start discovery service on h1
        harness.start_discovery(0, 5555, 2, 2)  # Host ID 0, 2 pubs, 2 subs
        
        discovery_address = f"{harness.hosts[0].IP()}:5555"
        
        # Start broker on h2
        harness.start_broker(1, discovery_address, 5560) # Host ID 1

        # Start publishers on h3 and h4
        harness.start_publisher(2, "pub1", 5556, discovery_address, topics=2)  # Host ID 2
        harness.start_publisher(3, "pub2", 5557, discovery_address, topics=2)  # Host ID 3

        # Start subscribers on h5 and h6
        harness.start_subscriber(4, "sub1", discovery_address, topics=2)  # Host ID 4
        harness.start_subscriber(4, "sub2", discovery_address, topics=2)  # Host ID 4

        # Let the system run for a while
        logger.info("System running - waiting 30 seconds")
        time.sleep(30)

    except Exception as e:
        logger.error(f"Exception in test: {str(e)}")
    finally:
        harness.cleanup()



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", choices=["Direct", "Broker"],
                      default="Direct", help="Dissemination strategy to test")
    parser.add_argument("--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING,
                                 logging.ERROR, logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=args.loglevel,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("TestHarness")

    if args.strategy == "Direct":
      test_direct_dissemination(logger)
    else:
      test_broker_dissemination(logger)


if __name__ == "__main__":
    setLogLevel('info')
    main()