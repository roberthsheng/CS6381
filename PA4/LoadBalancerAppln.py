#!/usr/bin/env python3
import json
import zmq
import logging
import argparse
from kazoo.client import KazooClient, NodeExistsError

class LoadBalancer:
    def __init__(self, zk_addr, bind_addr, rep_port, logger):
        self.zk_addr = zk_addr
        self.bind_addr = bind_addr
        self.rep_port = rep_port
        self.logger = logger
        self.zk = None
        self.context = zmq.Context()
        self.rep_socket = None

    def start_zk(self):
        self.logger.info(f"Connecting to ZooKeeper at {self.zk_addr}")
        self.zk = KazooClient(hosts=self.zk_addr)
        self.zk.start(timeout=10)
        # Ensure the /brokers path exists
        if not self.zk.exists("/brokers"):
            self.zk.create("/brokers", b"", makepath=True)
            self.logger.info("Created /brokers path in ZooKeeper.")

    def setup_socket(self):
        self.rep_socket = self.context.socket(zmq.REP)
        bind_str = f"tcp://{self.bind_addr}:{self.rep_port}"
        self.rep_socket.bind(bind_str)
        self.logger.info(f"Load balancer REP socket bound to {bind_str}")

    def select_broker(self):
        """
        Retrieves broker znodes from ZooKeeper and selects the one with the lowest
        subscriber count. Broker znode data is assumed to be a JSON object containing
        at least 'addr', 'port', and 'subscribers'.
        """
        children = self.zk.get_children("/brokers")
        if not children:
            self.logger.error("No brokers available in ZooKeeper!")
            return None, None

        best_broker = None
        best_count = float("inf")
        best_znode = None
        for child in children:
            broker_path = f"/brokers/{child}"
            data, stat = self.zk.get(broker_path)
            try:
                broker_info = json.loads(data.decode("utf-8"))
                count = broker_info.get("subscribers", 0)
                if count < best_count:
                    best_count = count
                    best_broker = broker_info
                    best_znode = broker_path
            except Exception as e:
                self.logger.error(f"Error parsing broker data at {broker_path}: {e}")
        return best_broker, best_znode

    def increment_subscriber_count(self, broker_znode):
        """
        Increments the subscriber count stored in the broker znode.
        """
        data, stat = self.zk.get(broker_znode)
        broker_info = json.loads(data.decode("utf-8"))
        broker_info["subscribers"] = broker_info.get("subscribers", 0) + 1
        new_data = json.dumps(broker_info).encode("utf-8")
        self.zk.set(broker_znode, new_data)
        self.logger.info(f"Incremented subscriber count for {broker_znode} to {broker_info['subscribers']}")

    def run(self):
        self.start_zk()
        self.setup_socket()
        self.logger.info("Load balancer started and waiting for subscriber requests.")
        while True:
            # Wait for a request from a subscriber.
            # For example, the subscriber can simply send "connect" as a request.
            message = self.rep_socket.recv_string()
            self.logger.info(f"Received request: {message}")

            # Select a broker based on the current load.
            broker_info, broker_znode = self.select_broker()
            if broker_info is None:
                # Send an error response if no brokers are available.
                self.rep_socket.send_string("No brokers available")
                continue

            # Increment the subscriber count for the selected broker.
            self.increment_subscriber_count(broker_znode)

            # Return the broker's connection information as JSON.
            response = json.dumps(broker_info)
            self.rep_socket.send_string(response)
            self.logger.info(f"Sent broker info: {response}")

def parse_args():
    parser = argparse.ArgumentParser(description="Load Balancer Application")
    parser.add_argument("-z", "--zk_addr", default="localhost:2181", help="ZooKeeper address (default: localhost:2181)")
    parser.add_argument("-a", "--addr", default="0.0.0.0", help="Load balancer bind address (default: 0.0.0.0)")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Load balancer REP socket port (default: 6000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="Logging level")
    return parser.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(level=args.loglevel,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("LoadBalancer")
    lb = LoadBalancer(args.zk_addr, args.addr, args.port, logger)
    try:
        lb.run()
    except KeyboardInterrupt:
        logger.info("Load balancer interrupted, shutting down.")

if __name__ == "__main__":
    main()
