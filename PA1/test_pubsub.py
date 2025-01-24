#!/usr/bin/env python3
"""
Test script to verify the pub/sub system functionality
"""

import os
import sys
import time
import subprocess
import signal
import argparse
import logging
from typing import List, Dict

class TestHarness:
    def __init__(self, logger):
        self.logger = logger
        self.processes: Dict[str, subprocess.Popen] = {}

    def cleanup(self):
        """Cleanup all running processes"""
        for name, proc in self.processes.items():
            self.logger.info(f"Terminating {name}")
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=2)  # Give each process 2 seconds to cleanup
            except subprocess.TimeoutExpired:
                proc.kill()  # Force kill if it doesn't terminate
                proc.wait()

    def start_discovery(self, port: int, num_pubs: int, num_subs: int):
        """Start the discovery service"""
        cmd = [
            "python3",
            "DiscoveryAppln.py",
            "-P", str(num_pubs),
            "-S", str(num_subs),
            "-p", str(port),
            "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Discovery Service: {' '.join(cmd)}")
        proc = subprocess.Popen(cmd)
        self.processes["discovery"] = proc
        time.sleep(2)  # Give discovery time to start

    def start_publisher(self, name: str, port: int, topics: int = 1):
        """Start a publisher"""
        cmd = [
            "python3",
            "PublisherAppln.py",
            "-n", name,
            "-p", str(port),
            "-T", str(topics),
            "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Publisher {name}: {' '.join(cmd)}")
        proc = subprocess.Popen(cmd)
        self.processes[f"pub_{name}"] = proc
        time.sleep(1)  # Give publisher time to start

    def start_subscriber(self, name: str, topics: int = 1):
        """Start a subscriber"""
        cmd = [
            "python3",
            "SubscriberAppln.py",
            "-n", name,
            "-T", str(topics),
            "-l", str(logging.DEBUG)
        ]
        self.logger.info(f"Starting Subscriber {name}: {' '.join(cmd)}")
        proc = subprocess.Popen(cmd)
        self.processes[f"sub_{name}"] = proc
        time.sleep(1)  # Give subscriber time to start

def test_direct_dissemination(logger):
    """Test the direct dissemination strategy"""
    try:
        # Create test harness
        harness = TestHarness(logger)

        # Start discovery service
        harness.start_discovery(5555, 2, 2)  # 2 pubs, 2 subs

        # Start publishers
        harness.start_publisher("pub1", 5556, topics=2)
        harness.start_publisher("pub2", 5557, topics=2)

        # Start subscribers
        harness.start_subscriber("sub1", topics=2)
        harness.start_subscriber("sub2", topics=2)

        # Let the system run for a while
        logger.info("System running - waiting 30 seconds")
        time.sleep(30)

    except Exception as e:
        logger.error(f"Exception in test: {str(e)}")
    finally:
        harness.cleanup()

def test_broker_dissemination(logger):
    """Test the broker-based dissemination strategy"""
    try:
        # Create test harness
        harness = TestHarness(logger)

        # Update config to use broker
        with open("config.ini", "w") as f:
            f.write("[Discovery]\n")
            f.write("Strategy=Centralized\n\n")
            f.write("[Dissemination]\n")
            f.write("Strategy=ViaBroker\n")

        # Start discovery service
        harness.start_discovery(5555, 2, 2)  # 2 pubs, 2 subs

        # Start broker (when implemented)
        # harness.start_broker()

        # Start publishers
        harness.start_publisher("pub1", 5556, topics=2)
        harness.start_publisher("pub2", 5557, topics=2)

        # Start subscribers
        harness.start_subscriber("sub1", topics=2)
        harness.start_subscriber("sub2", topics=2)

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
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("TestHarness")

    if args.strategy == "Direct":
        test_direct_dissemination(logger)
    else:
        test_broker_dissemination(logger)

if __name__ == "__main__":
    main()
