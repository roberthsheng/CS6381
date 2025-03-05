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
from kazoo.client import KazooClient, NodeExistsError, NoNodeError
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
        self.broker_election_path = "/broker_election"
        self.discovery_election_path = "/discovery_election"
        self.current_connect_str = None
        self.my_election_znode = None # which znode i made for election
        self.is_leader = False
        self.leader_event = threading.Event()
        self.quorum_size = 2  # Quorum size for replicas (set for demo)
        self.replica_count = 0  # Current number of replicas

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")
            self.addr = args.addr
            self.port = args.port
            self.zk_addr = args.zk_addr

            self._init_zk()

            leader_znode_path = self.wait_for_leader()
            self.connect_to_leader(leader_znode_path)

            self.sub = self.context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

            self.pub = self.context.socket(zmq.PUB)
            self.pub.bind(f"tcp://{self.addr}:{self.port}")

            self.poller = zmq.Poller()
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.logger.info("BrokerMW::configure - watching for discovery changes")
            self.watch_leader()
            self.logger.info("BrokerMW::configure completed")

        except Exception as e:
            self.logger.error("Exception in BrokerMW::configure: " + str(e))
            raise e

    def _init_zk(self):
        """Initialize ZooKeeper connection and participate in leader election."""
        try:
            self.logger.info("BrokerMW::_init_zk - Connecting to ZooKeeper at {}".format(self.zk_addr))
            self.zk = KazooClient(hosts=self.zk_addr)
            self.zk.start(timeout=10)

            if not self.zk.exists(self.broker_election_path):
                try:
                    self.zk.create(self.broker_election_path, makepath=True)
                    self.logger.info(f"Created election path: {self.broker_election_path}")
                except NodeExistsError:
                    self.logger.warning(f"Election path {self.broker_election_path} already exists, likely created concurrently.")

            self._attempt_leader_election()
            self._monitor_replicas()  # Start monitoring replicas

        except Exception as e:
            self.logger.error(f"BrokerMW::_init_zk - ZooKeeper initialization failed: {e}")
            raise e

    def _attempt_leader_election(self):
        """Attempt to become the leader by creating an ephemeral sequential znode."""
        try:
            self.logger.info("BrokerMW::_attempt_leader_election - Attempting to become leader")
            if not self.my_election_znode:
                my_znode_path = self.zk.create(self.broker_election_path + "/n_", ephemeral=True, sequence=True)
                self.my_election_znode = my_znode_path
                self.logger.debug(f"Created election znode: {my_znode_path}")

            self._check_leadership()

        except Exception as e:
            self.logger.error(f"BrokerMW::_attempt_leader_election - Leader election attempt failed: {e}")
            raise e

    def _check_leadership(self):
        """Determine if this instance is the leader and set up watcher if not."""
        try:
            children = self.zk.get_children(self.broker_election_path)
            children.sort()

            self.leader_sequence_num = int(self.my_election_znode.split("_")[-1])

            if self.my_election_znode == self.broker_election_path + "/" + children[0]:
                self.logger.info("BrokerMW::_check_leadership - I am the leader!")
                self.become_leader()
            else:
                leader_znode_name = children[0]
                self.logger.info(f"BrokerMW::_check_leadership - I am a replica. Current leader: {leader_znode_name}")
                leader_seq_num = int(leader_znode_name.split("_")[-1])

                my_index = children.index(os.path.basename(self.my_election_znode))
                if my_index > 0:
                    prev_znode_name = children[my_index - 1]
                    self.next_leader_candidate_path = self.broker_election_path + "/" + prev_znode_name
                    self.logger.debug(f"Watching for deletion of: {self.next_leader_candidate_path}")
                    self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)
                else:
                    self.next_leader_candidate_path = self.broker_election_path + "/" + leader_znode_name
                    self.logger.debug(f"Watching for deletion of leader: {self.next_leader_candidate_path}")
                    self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)

        except Exception as e:
            self.logger.error(f"BrokerMW::_check_leadership - Error checking leadership: {e}")
            raise e

    def become_leader(self):
        """Actions to perform when this instance becomes the leader."""
        self.upcall_obj.began_running = time.time() 
        if not self.is_leader:
            self.logger.info("BrokerMW::become_leader - Transitioning to leader state.")
            self.is_leader = True
            self.leader_event.set()

    def _leader_watcher(self, event):
        """ZooKeeper watcher callback for leadership changes."""
        try:
            if event and event.type == "DELETED":
                self.logger.info(f"BrokerMW::_leader_watcher - Watched znode {event.path} deleted. Previous leader might have failed.")
                if not self.is_leader:
                    self._attempt_leader_election()

            elif event is None:
                exists = self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)
                if not exists:
                    self.logger.info(f"BrokerMW::_leader_watcher - Watched znode {self.next_leader_candidate_path} disappeared before callback, attempting leadership.")
                    if not self.is_leader:
                        self._attempt_leader_election()

            else:
                self.logger.debug(f"BrokerMW::_leader_watcher - Event: {event}")

        except NoNodeError:
            self.logger.warning("BrokerMW::_leader_watcher - Watched znode disappeared quickly, attempting leadership.")
            if not self.is_leader:
                self._attempt_leader_election()
        except Exception as e:
            self.logger.error(f"BrokerMW::_leader_watcher - Exception in watcher callback: {e}")
            self._handle_zk_error()

    def _monitor_replicas(self):
        """Monitor the number of replicas and revive if necessary."""
        def replica_watcher(event):
            try:
                children = self.zk.get_children(self.broker_election_path)
                self.replica_count = len(children)
                self.logger.info(f"BrokerMW::_monitor_replicas - Current replica count: {self.replica_count}")
                if self.replica_count < self.quorum_size:
                    self.logger.warning(f"Quorum not met: {self.replica_count} replicas. Reviving replica.")
                    self.revive_replica()
            except Exception as e:
                self.logger.error(f"Error in replica watcher: {e}")

        self.zk.get_children(self.broker_election_path, watch=replica_watcher)

    def revive_replica(self):
        """Revive a new replica to maintain quorum."""
        try:
            import subprocess
            # Launch a new Broker instance with a port offset of +10
            subprocess.Popen(["python3", "BrokerAppln.py", "--addr", "localhost", "--port", str(int(self.port) + 10), "--zk_addr", self.zk_addr, "--loglevel", "20"])
            self.logger.info("Revived a new Broker replica.")
        except Exception as e:
            self.logger.error(f"Failed to revive replica: {e}")

    def check_quorum(self):
        """Check if quorum is met before performing operations."""
        children = self.zk.get_children(self.broker_election_path)
        self.replica_count = len(children)
        return self.replica_count >= self.quorum_size

    def wait_for_leader(self, check_interval=1):
        """Blocks until a Discovery leader is elected."""
        leader_found = threading.Event()

        def leader_watch(event):
            leader_found.set()

        while True:
            try:
                children = self.zk.get_children(self.discovery_election_path, watch=leader_watch)
                if children:
                    children.sort()
                    leader_znode = self.discovery_election_path + "/" + children[0]
                    return leader_znode
                else:
                    leader_found.wait(timeout=check_interval)
                    leader_found.clear()
            except Exception as e:
                self.logger.error(f"Error checking election path: {e}")
                time.sleep(check_interval)

    def watch_leader(self):
        """Sets a watch on the Discovery leader."""
        try:
            children = self.zk.get_children(self.discovery_election_path, watch=self._leader_watch_callback)
            if children:
                children.sort()
                leader_znode = self.discovery_election_path + "/" + children[0]
                self.logger.info(f"Current Discovery leader: {leader_znode}")
                self.connect_to_leader(leader_znode)
            else:
                self.logger.info("No Discovery leader found. Waiting...")
        except Exception as e:
            self.logger.error(f"Error setting watch on leader: {e}")

    def read_discovery_znode(self, leader_znode_path):
        leader_data, _ = self.zk.get(leader_znode_path)
        if leader_data:
            leader_info = json.loads(leader_data.decode('utf-8'))
            return leader_info.get("address"), leader_info.get("port")
        raise ValueError("No data found in Discovery leader znode")

    def connect_to_leader(self, leader_znode_path):
        """Connects to the Discovery leader."""
        try:
            discovery_address, discovery_port = self.read_discovery_znode(leader_znode_path)
            new_connect_str = f"tcp://{discovery_address}:{discovery_port}"
            self.logger.info(f"Connecting to Discovery leader at {new_connect_str}")
            self.req = self.context.socket(zmq.REQ)
            self.req.connect(new_connect_str)
            self.current_connect_str = new_connect_str
            self.poller.register(self.req, zmq.POLLIN)
        except Exception as e:
            self.logger.error(f"Failed to connect to leader: {e}")

    def _leader_watch_callback(self, event):
        """Callback for Discovery leader changes."""
        self.logger.info(f"ZooKeeper watch triggered: {event}")
        try:
            children = self.zk.get_children(self.discovery_election_path, watch=self._leader_watch_callback)
            if not children:
                self.logger.info("No Discovery candidates yet.")
                return
            children.sort()
            new_leader_znode = self.discovery_election_path + "/" + children[0]
            self.logger.info(f"New Discovery leader detected: {new_leader_znode}")
            self.update_leader_connection(new_leader_znode)
        except Exception as e:
            self.logger.error(f"Error in leader watch callback: {e}")

    def update_leader_connection(self, new_leader_znode):
        """Updates connection to new Discovery leader."""
        try:
            new_address, new_port = self.read_discovery_znode(new_leader_znode)
            new_connect_str = f"tcp://{new_address}:{new_port}"
            if self.current_connect_str and self.current_connect_str != new_connect_str:
                self.logger.info(f"Disconnecting from old leader at {self.current_connect_str}")
                self.poller.unregister(self.req)
                self.req.disconnect(self.current_connect_str)
                self.req.connect(new_connect_str)
                self.poller.register(self.req, zmq.POLLIN)
                self.current_connect_str = new_connect_str
            elif not self.current_connect_str:
                self.logger.info(f"Connecting to leader at {new_connect_str}")
                self.req.connect(new_connect_str)
                self.poller.register(self.req, zmq.POLLIN)
                self.current_connect_str = new_connect_str
            else:
                self.logger.info("Already connected to the correct leader.")
        except Exception as e:
            self.logger.error(f"Error updating leader connection: {e}")

    def start_publisher_watch(self):
        """Set a ZooKeeper watch on /publisher."""
        self.logger.info("BrokerMW::start_publisher_watch - setting watch on /publisher")
        def publisher_watch(children):
            self.logger.info(f"BrokerMW::publisher_watch - publisher nodes changed: {children}")
            self.zk_event_queue.put(("publishers_changed", children))
            return True
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

                while not self.zk_event_queue.empty():
                    event_type, data = self.zk_event_queue.get_nowait()
                    self.logger.info(f"BrokerMW::event_loop - ZK event: {event_type} with data: {data}")
                    if event_type == "publishers_changed":
                        if self.upcall_obj:
                            self.upcall_obj.handle_publishers_update()

                if self.req in events:
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
                    # Check quorum before disseminating messages
                    if not self.check_quorum():
                        self.logger.warning("Quorum not met. Pausing dissemination.")
                        continue  # Skip processing this event
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

    def _handle_zk_error(self):
        """Handles ZooKeeper related errors."""
        self.logger.error("BrokerMW::_handle_zk_error - A ZooKeeper error occurred.")
        # Placeholder for retry/exit strategy
        pass

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
            lookup_req = discovery_pb2.LookupPubByTopicReq()
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
            
            if hasattr(self, "context") and self.context:
                self.context.term()
                self.logger.info("BrokerMW::cleanup - ZMQ context terminated.")
                self.context = None

        except Exception as e:
            self.logger.error("Error during cleanup: " + str(e))
            raise e