###############################################
#
# Purpose: Publisher middleware implementation
#
###############################################
import pdb
import os
import sys
import time
import logging
import zmq
import socket
import configparser
from CS6381_MW import discovery_pb2, topic_pb2
from kazoo.client import KazooClient
import queue

class PublisherMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None        # ZMQ REQ socket for discovery service
        self.pub = None        # ZMQ PUB socket for dissemination
        self.poller = None     # used to wait on incoming replies
        self.addr = None       # our advertised IP address
        self.port = None       # port number where we publish
        self.upcall_obj = None # handle to application object
        self.handle_events = True  # event loop control

        # For broker availability monitoring via ZooKeeper
        self.zk = None
        self.zk_event_queue = None

    def configure(self, args):
        try:
            self.logger.info("PublisherMW::configure")

            # Initialize our variables
            self.port = args.port
            self.addr = socket.gethostbyname(socket.gethostname())
            self.name = args.name

            # Get the ZMQ context and poller
            context = zmq.Context()
            self.poller = zmq.Poller()

            # Acquire the REQ and PUB sockets
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)

            # Register the REQ socket to receive replies
            self.poller.register(self.req, zmq.POLLIN)

            # Connect to the discovery service
            self.logger.debug("PublisherMW::configure - connecting to Discovery service")
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Bind the PUB socket for dissemination
            self.logger.debug("PublisherMW::configure - binding to PUB socket")
            bind_string = f"tcp://*:{self.port}"
            self.pub.bind(bind_string)

            # Set up ZooKeeper to watch for the broker’s znode.
            self.logger.info("PublisherMW::configure - connecting to ZooKeeper")
            self.zk = KazooClient(hosts=args.zk_addr)
            self.zk.start()
            self.zk_event_queue = queue.Queue()

            self.logger.info("PublisherMW::configure completed")

        except Exception as e:
            raise e

    def watch_for_broker(self):
        """
        Sets a ZooKeeper data watch on the /broker/primary znode.
        The callback pushes an event onto a queue so that the event loop
        can update our broker availability flag.
        """
        self.logger.info("PublisherMW::watch_for_broker - setting up watch on /broker/primary")
        broker_node = "/broker/primary"

        def broker_watch(data, stat, event):
            if event is None:
                if stat is None:
                    self.logger.info("Broker znode /broker/primary does not exist initially.")
                    self.zk_event_queue.put(("broker_not_available", None))
                else:
                    self.logger.info("Broker znode /broker/primary exists initially.")
                    self.zk_event_queue.put(("broker_available", data))
            else:
                self.logger.info("Broker watch event: %s", event)
                if event.type == "DELETED":
                    self.logger.info("Broker znode /broker/primary has been deleted.")
                    self.zk_event_queue.put(("broker_not_available", None))
                elif event.type in ["CREATED", "CHANGED"]:
                    self.logger.info("Broker znode /broker/primary has been created/changed.")
                    self.zk_event_queue.put(("broker_available", data))
            return True  # keep the watch active

        if not self.zk.exists("/broker"):
            self.zk.create("/broker", b"", makepath=True)
        self.zk.DataWatch(broker_node, broker_watch)

    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - run the event loop")
            default_timeout = 500  # milliseconds

            while self.handle_events:
                effective_timeout = timeout if timeout is not None else default_timeout
                events = dict(self.poller.poll(timeout=effective_timeout))

                # Process any ZooKeeper events from the watch.
                while not self.zk_event_queue.empty():
                    event_type, data = self.zk_event_queue.get_nowait()
                    self.logger.info(f"Dequeued zk event: {event_type} with data: {data}")
                    if event_type == "broker_available":
                        if self.upcall_obj:
                            self.upcall_obj.handle_broker_available()
                    elif event_type == "broker_not_available":
                        if self.upcall_obj:
                            self.upcall_obj.handle_broker_unavailable()

                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                else:
                    raise Exception("Unknown event after poll")

            self.logger.info("PublisherMW::event_loop - out of the event loop")

        except Exception as e:
            raise e

    def handle_reply(self):
        try:
            self.logger.info("PublisherMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            else:
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("PublisherMW::register")

            # Build the registrant info
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port

            # Build the register request
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_PUBLISHER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist.extend(topiclist)

            # Build the outer discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            # Serialize and send
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)

        except Exception as e:
            raise e

    def disseminate(self, id, topic, data):
        try:
            # Only send if the broker is available
            self.logger.info("PublisherMW::disseminate")
            pub_msg = topic_pb2.Publication()
            pub_msg.publisher_id = id
            pub_msg.topic = topic
            pub_msg.data = data
            pub_msg.timestamp = int(time.time() * 1000)  # Time in milliseconds

            buf2send = pub_msg.SerializeToString()
            self.pub.send_multipart([topic.encode('utf-8'), buf2send])

        except Exception as e:
            raise e

    def create_publisher_znode(self):
        """Create the /broker/primary znode if it does not exist."""
        try:
            self.logger.info("PublisherMW::create_publisher_znode")
            if not self.zk.exists(f"/publisher/{self.name}"):
                self.zk.create(f"/publisher/{self.name}", value=self.addr.encode(), ephemeral=True, makepath=True)
        except Exception as e:
            self.logger.error("Exception in PublisherMW::create_publisher_znode: " + str(e))
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def cleanup(self):
        try:
            self.logger.info("PublisherMW::cleanup")
            if self.pub:
                self.pub.close()
            if self.req:
                self.req.close()
            if self.zk:
                self.zk.stop()
        except Exception as e:
            self.logger.error("Error during cleanup: " + str(e))
            raise e
