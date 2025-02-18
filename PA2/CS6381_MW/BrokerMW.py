###############################################
#
# Purpose: Broker middleware implementation
#
###############################################
import threading
import json
import os
import sys
import time
import logging
import zmq
import socket
from kazoo.client import KazooClient
import queue
from CS6381_MW import discovery_pb2, topic_pb2

class BrokerMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None         # REQ socket for discovery service
        self.sub = None         # SUB socket to receive publications from publishers
        self.pub = None         # PUB socket to disseminate messages to subscribers
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.zk = None
        self.zk_event_queue = queue.Queue()  # For ZooKeeper events (e.g. publisher changes)
        self.connected_publishers = set()      # Track already connected publishers
        self.addr = None   # Broker's advertised address
        self.port = None   # Broker's publish port
        self.context = zmq.Context()
        self.election_path = None
        self.current_connect_str = None

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")
            # Set broker's bind address and publish port from command-line args.
            self.addr = args.addr
            self.port = args.port
            self.election_path = "/discovery_election"

            # Setup ZooKeeper connection using provided zk_addr.
            self.zk = KazooClient(hosts=args.zk_addr)
            self.zk.start()

            # Setup ZMQ sockets.
            self.req = self.context.socket(zmq.REQ)
            leader_znode_path = self.wait_for_leader()
            self.connect_to_leader(leader_znode_path)

            # Create a SUB socket to receive publications from publishers.
            self.sub = self.context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics

            # Create a PUB socket to disseminate messages to subscribers.
            self.pub = self.context.socket(zmq.PUB)
            self.pub.bind(f"tcp://{self.addr}:{self.port}")

            # Setup poller for the REQ and SUB sockets.
            self.poller = zmq.Poller()
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.logger.info("BrokerMW::configure - watching for discovery changes")
            self.watch_leader()
            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            self.logger.error("Exception in BrokerMW::configure: " + str(e))
            raise e

    def wait_for_leader(self, check_interval=1):
        """
        Blocks until at least one child exists in the election path.
        Returns the leader's full znode path (the one with the smallest sequence number).
        """
        leader_found = threading.Event()

        def leader_watch(event):
            # This callback will be invoked when a change happens
            leader_found.set()

        while True:
            try:
                children = self.zk.get_children(self.election_path, watch=leader_watch)
                if children:
                    # Sort children to determine the leader
                    children.sort()
                    leader_znode = self.election_path + "/" + children[0]
                    return leader_znode
                else:
                    # No leader yet; wait for the event to trigger a change
                    leader_found.wait(timeout=check_interval)
                    # Clear the event and loop again to check for a leader
                    leader_found.clear()
            except Exception as e:
                # Handle connection errors or other issues as needed
                print(f"Error checking election path: {e}")
                time.sleep(check_interval)

    def watch_leader(self):
        """
        Sets a watch on the leader's znode by watching the children of the election path.
        This is non-blocking; ZooKeeper will call the provided callback when a change occurs.
        """
        try:
            children = self.zk.get_children(self.election_path, watch=self._leader_watch_callback)
            if children:
                children.sort()  # Leader is the one with the smallest sequence number
                leader_znode = self.election_path + "/" + children[0]
                self.logger.info(f"Current leader: {leader_znode}")
                self.connect_to_leader(leader_znode)
            else:
                self.logger.info("No leader found. Waiting for leader to be elected...")
        except Exception as e:
            self.logger.error(f"Error setting watch on leader: {e}")

    def read_discovery_znode(self, leader_znode_path):
            leader_data, leader_stat = self.zk.get(leader_znode_path)
            if leader_data:
                # Decode and parse the JSON data
                leader_info = json.loads(leader_data.decode('utf-8'))
                leader_address = leader_info.get("address")
                leader_port = leader_info.get("port")
                return leader_address, leader_port
            raise ValueError("No data found in discovery leader znode")

    def connect_to_leader(self, leader_znode_path):
        """
        Connects the REQ socket to the leader's discovery address.
        """
        try:
            discovery_address, discovery_port = self.read_discovery_znode(leader_znode_path)
            new_connect_str = f"tcp://{discovery_address}:{discovery_port}"
            self.logger.info(f"Connecting to leader at {new_connect_str}")
            self.req.connect(new_connect_str)
            self.current_connect_str = new_connect_str
        except Exception as e:
            self.logger.error(f"Failed to connect to leader: {e}")

    def update_leader_connection(self, new_leader_znode):
        """
        Disconnects from the old leader (if connected) and connects to the new leader.
        """
        try:
            new_address, new_port = self.read_discovery_znode(new_leader_znode)
            new_connect_str = f"tcp://{new_address}:{new_port}"

            # Only change if it's a different endpoint than the current one
            if self.current_connect_str and self.current_connect_str != new_connect_str:
                self.logger.info(f"Disconnecting from old leader at {self.current_connect_str}")
                self.req.disconnect(self.current_connect_str)
                self.logger.info(f"Connecting to new leader at {new_connect_str}")
                self.req.connect(new_connect_str)
                self.current_connect_str = new_connect_str
            elif not self.current_connect_str:
                # First time connecting.
                self.logger.info(f"Connecting to leader at {new_connect_str}")
                self.req.connect(new_connect_str)
                self.current_connect_str = new_connect_str
            else:
                self.logger.info("Already connected to the correct leader.")
        except Exception as e:
            self.logger.error(f"Error updating leader connection: {e}")

    def _leader_watch_callback(self, event):
        """
        This callback is invoked when the children of the election path change.
        It will disconnect from the current leader (if any) and connect to the new leader.
        """
        self.logger.info(f"ZooKeeper watch triggered: {event}")
        # When a watch is triggered, we need to re-read the election path.
        # (Note: watches are one-shot, so we must set a new one.)
        try:
            children = self.zk.get_children(self.election_path, watch=self._leader_watch_callback)
            if not children:
                self.logger.info("No candidates in election path yet.")
                return

            children.sort()
            new_leader_znode = self.election_path + "/" + children[0]
            self.logger.info(f"New leader detected: {new_leader_znode}")
            self.update_leader_connection(new_leader_znode)
        except Exception as e:
            self.logger.error(f"Error in leader watch callback: {e}")

    def start_publisher_watch(self):
        """Set a ZooKeeper ChildrenWatch on /publisher and add events to the queue."""
        self.logger.info("BrokerMW::start_publisher_watch - setting watch on /publisher")
        def publisher_watch(children):
            self.logger.info(f"BrokerMW::publisher_watch - publisher nodes changed: {children}")
            self.zk_event_queue.put(("publishers_changed", children))
            return True  # keep the watch active
        # Ensure the /publisher node exists.
        if not self.zk.exists("/publisher"):
            self.zk.create("/publisher", b"", makepath=True)
        self.zk.ChildrenWatch("/publisher", publisher_watch)

    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW::event_loop - starting")
            default_timeout = 500  # milliseconds
            while self.handle_events:
                effective_timeout = timeout if timeout is not None else default_timeout
                events = dict(self.poller.poll(timeout=effective_timeout))

                # Process ZooKeeper events from our internal queue.
                while not self.zk_event_queue.empty():
                    event_type, data = self.zk_event_queue.get_nowait()
                    self.logger.info(f"BrokerMW::event_loop - ZK event: {event_type} with data: {data}")
                    if event_type == "publishers_changed":
                        if self.upcall_obj:
                            self.upcall_obj.handle_publishers_update()

                if self.req in events:
                    # Handle reply from the discovery service.
                    reply_bytes = self.req.recv()
                    disc_resp = discovery_pb2.DiscoveryResp()
                    disc_resp.ParseFromString(reply_bytes)
                    if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                        timeout = self.upcall_obj.register_response(disc_resp.register_resp)
                    elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                        timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
                    else:
                        self.logger.error("BrokerMW::event_loop - Unrecognized message type")
                        timeout = None
                elif self.sub in events:
                    # Handle incoming publication from a publisher.
                    topic, msg_bytes = self.sub.recv_multipart()
                    pub_msg = topic_pb2.Publication()
                    pub_msg.ParseFromString(msg_bytes)
                    send_time = pub_msg.timestamp
                    recv_time = int(time.time() * 1000)
                    timeout = self.upcall_obj.handle_publication(topic.decode('utf-8'), pub_msg.data, send_time, pub_msg.publisher_id)
                else:
                    timeout = self.upcall_obj.invoke_operation()
            self.logger.info("BrokerMW::event_loop - exiting")
        except Exception as e:
            self.logger.error("Exception in BrokerMW::event_loop: " + str(e))
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def register(self, name):
        try:
            self.logger.info("BrokerMW::register")
            reg_req = discovery_pb2.RegisterReq()
            reg_req.role = discovery_pb2.ROLE_BOTH
            info = discovery_pb2.RegistrantInfo()
            info.id = name
            info.addr = self.addr
            info.port = self.port
            reg_req.info.CopyFrom(info)
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(reg_req)
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error("Exception in BrokerMW::register: " + str(e))
            raise e

    def create_broker_znode(self):
        """Create the /broker/primary znode if it does not exist."""
        try:
            self.logger.info("BrokerMW::create_broker_znode")
            if not self.zk.exists("/broker/primary"):
                self.zk.create("/broker/primary", value=self.addr.encode(), ephemeral=True, makepath=True)
        except Exception as e:
            self.logger.error("Exception in BrokerMW::create_broker_znode: " + str(e))
            raise e

    def lookup_all_publishers(self):
        try:
            self.logger.info("BrokerMW::lookup_all_publishers")
            # Instead of a separate lookup-all request, we use the lookup-by-topic request.
            # By passing an empty topic list, the discovery service returns all publishers.
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            # No topics added: implies "all publishers"
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_req.lookup_req.CopyFrom(lookup_req)
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error("Exception in BrokerMW::lookup_all_publishers: " + str(e))
            raise e

    def connect_to_publishers(self, publishers):
        try:
            self.logger.info("BrokerMW::connect_to_publishers")
            for pub in publishers:
                if pub.id in self.connected_publishers:
                    continue
                self.connected_publishers.add(pub.id)
                endpoint = f"tcp://{pub.addr}:{pub.port}"
                self.sub.connect(endpoint)
                self.logger.debug(f"BrokerMW::connect_to_publishers - Connected to publisher {pub.id} at {endpoint}")
        except Exception as e:
            self.logger.error("Exception in BrokerMW::connect_to_publishers: " + str(e))
            raise e

    def disseminate(self, topic, data, old_timestamp, publisher_id):
        try:
            self.logger.debug("BrokerMW::disseminate")

            pub_msg = topic_pb2.Publication()
            pub_msg.publisher_id = "broker"
            pub_msg.topic = topic
            pub_msg.data = data
            pub_msg.timestamp = old_timestamp 
            pub_msg.publisher_id = publisher_id

            buf2send = pub_msg.SerializeToString()
            self.pub.send_multipart([topic.encode("utf-8"), buf2send])

        except Exception as e:
            raise e

    def cleanup(self):
        try:
            self.logger.info("BrokerMW::cleanup")
            if self.sub:
                self.sub.close()
            if self.pub:
                self.pub.close()
            if self.req:
                self.req.close()
            if self.zk:
                self.zk.stop()
            self.context.term()
        except Exception as e:
            self.logger.error("Exception in BrokerMW::cleanup: " + str(e))
            raise e
