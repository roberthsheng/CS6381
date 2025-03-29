###############################################
#
# Purpose: Publisher middleware implementation
#
###############################################
import pdb
from kazoo.client import KazooClient, NodeExistsError, NoNodeError
import json 
import os
import sys
import time
import logging
import zmq
import socket
import configparser
from CS6381_MW import discovery_pb2, topic_pb2

class PublisherMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # ZMQ REQ socket for discovery service
        self.pub = None  # ZMQ PUB socket for dissemination
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we publish
        self.upcall_obj = None  # handle to appln obj
        self.handle_events = True  # event loop control

    def configure(self, args, topiclist):
        try:
            self.logger.info("PublisherMW::configure")
            self.topiclist = topiclist
            # Initialize our variables
            self.port = args.port
            self.addr = socket.gethostbyname(socket.gethostname())

            self.publisher_path = "/publishers"
            self.zk_addr = args.zk_addr
            self._init_zk()
            self.create_publisher_znodes()

            # Get the ZMQ context
            context = zmq.Context()

            # Acquire the REQ and PUB sockets
            self.pub = context.socket(zmq.PUB)

            # Bind the PUB socket
            self.logger.debug("PublisherMW::configure - bind to pub socket")
            bind_string = f"tcp://*:{self.port}"
            self.bind_string = bind_string
            self.pub.bind(bind_string)

            self.logger.info("PublisherMW::configure completed")

        except Exception as e:
            raise e

    def _init_zk(self):
        """Initialize ZooKeeper connection and ensure paths exist."""
        try:
            self.logger.info("PublisherMW::_init_zk - Connecting to ZooKeeper at {}".format(self.zk_addr))
            self.zk = KazooClient(hosts=self.zk_addr)
            self.zk.start(timeout=10) # Increased timeout for robustness

            if not self.zk.exists(self.publisher_path):
                try:
                    self.zk.create(self.publisher_path, makepath=True)
                    self.logger.info(f"Created election path: {self.publisher_path}")
                except NodeExistsError:
                    self.logger.warning(f"Election path {self.publisher_path} already exists, likely created concurrently.")

            for topic in self.topiclist:
                path = "topics/" + topic
                if not self.zk.exists(path):
                    try:
                        self.zk.create(path, makepath=True)
                        self.logger.info(f"Created election path: {path}")
                    except NodeExistsError:
                        self.logger.warning(f"Election path {path} already exists, likely created concurrently.")
                    
        except Exception as e:
            self.logger.error(f"DiscoveryMW::_init_zk - ZooKeeper initialization failed: {e}")
            # Implement a zk error handler (e.g., retry, exit)
            raise e

    def create_publisher_znodes(self):
        """
        Create the publisher znode and, for each topic, a sequential,
        ephemeral znode under the topic path. The content of each znode is
        the publisher's "addr:port", which can later be used for ranking.
        """
        try:
            self.logger.info("PublisherMW::create_publisher_znodes")

            # Ensure the parent publisher path exists
            if not self.zk.exists(self.publisher_path):
                self.zk.create(self.publisher_path, b"", makepath=True)
                self.logger.info(f"Created publisher path: {self.publisher_path}")

            # New code: include a unique publisher id (for example, provided as args.name)
            publisher_info = {
                "addr": self.addr,
                "port": self.port,
                "publisher_id":  f"{self.addr}:{self.port}" # or any other unique identifier
            }
            publisher_data = json.dumps(publisher_info).encode("utf-8")
            self.publisher_id = f"{self.addr}:{self.port}"

            # Create an ephemeral sequential node for the publisher
            publisher_node = self.zk.create(
                f"{self.publisher_path}/node_",
                publisher_data,
                ephemeral=True,
                sequence=True,
                makepath=True,
            )
            self.logger.info(
                f"Created publisher znode: {publisher_node} with data {publisher_data.decode()}"
            )

            # For each topic, create a sequential, ephemeral node under the topic path.
            for topic in self.topiclist:
                topic_path = f"topics/{topic}"
                # Ensure the topic path exists
                if not self.zk.exists(topic_path):
                    self.zk.create(topic_path, b"", makepath=True)
                    self.logger.info(f"Created topic path: {topic_path}")

                # Create the ephemeral sequential node under the topic path.
                topic_node = self.zk.create(
                    f"{topic_path}/node_",
                    publisher_data,
                    ephemeral=True,
                    sequence=True,
                    makepath=True,
                )
                self.logger.info(
                    f"Created topic znode for {topic}: {topic_node} with data {publisher_data.decode()}"
                )

        except Exception as e:
            self.logger.error(f"Exception in PublisherMW::create_publisher_znodes: {e}")
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - run the event loop")

            while self.handle_events:

                    # timeout occurred, let application handle
                timeout = self.upcall_obj.invoke_operation()


            self.logger.info("PublisherMW::event_loop - out of the event loop")

        except Exception as e:
            raise e

    def disseminate(self, id, topic, data):
        try:
            self.logger.debug("PublisherMW::disseminate")
            pub_msg = topic_pb2.Publication()
            pub_msg.publisher_id = self.publisher_id  # Ensure 'id' here is set to args.name or your unique id
            pub_msg.topic = topic
            pub_msg.data = data
            pub_msg.timestamp = int(time.time() * 1000)  # Time in milliseconds
            buf2send = pub_msg.SerializeToString()
            self.pub.send_multipart([topic.encode('utf-8'), buf2send])

        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
