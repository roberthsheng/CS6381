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
        self.broker_election_path = None
        self.discovery_election_path = None
        self.current_connect_str = None
        self.my_election_znode = None # which znode i made for election
        self.is_leader = False
        self.leader_event = threading.Event()

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")
            self.addr = args.addr
            self.port = args.port
            self.zk_addr = args.zk_addr
            self.discovery_election_path = "/discovery_election"  # Keep this too
            self.broker_election_path = "/broker_election"  # Match original intent
            self.logger.debug(f"BrokerMW::configure - Paths set: broker={self.broker_election_path}, discovery={self.discovery_election_path}")
            self._init_zk()
            self.req = self.context.socket(zmq.REQ)
            leader_znode_path = self.wait_for_leader()
            self.connect_to_leader(leader_znode_path)

            self.sub = self.context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
            self.sub.connect("tcp://10.0.0.5:5570")
            self.logger.info("BrokerMW::configure - SUB connected to tcp://10.0.0.5:5570")

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
        try:
            self.logger.info("BrokerMW::_init_zk - Connecting to ZooKeeper at {}".format(self.zk_addr))
            self.zk = KazooClient(hosts=self.zk_addr)
            self.logger.info("BrokerMW::_init_zk - Starting ZooKeeper client")
            self.zk.start(timeout=10)
            self.logger.info(f"BrokerMW::_init_zk - Checking path: {self.broker_election_path}, type: {type(self.broker_election_path)}")
            if not self.zk.exists(self.broker_election_path):
                self.logger.info("BrokerMW::_init_zk - Creating election path")
                self.zk.create(self.broker_election_path, value=b"", makepath=True)
                self.logger.info(f"BrokerMW::_init_zk - Created election path: {self.broker_election_path}")
            else:
                self.logger.info(f"BrokerMW::_init_zk - Path {self.broker_election_path} already exists")
            self.logger.info("BrokerMW::_init_zk - Attempting leader election")
            self._attempt_leader_election()
        except Exception as e:
            self.logger.error(f"BrokerMW::_init_zk - ZooKeeper initialization failed: {e}")
            raise e

    def _attempt_leader_election(self):
        """Attempt to become the leader by creating an ephemeral sequential znode."""
        try:
            self.logger.info("DiscoveryMW::_attempt_leader_election - Attempting to become leader")
            # Create an ephemeral sequential znode
            if not self.my_election_znode:
                my_znode_path = self.zk.create(self.broker_election_path + "/n_", ephemeral=True, sequence=True)
                self.my_election_znode = my_znode_path
                self.logger.debug(f"Created election znode: {my_znode_path}")

            self._check_leadership()

        except Exception as e:
            self.logger.error(f"DiscoveryMW::_attempt_leader_election - Leader election attempt failed: {e}")
            self._handle_zk_error()
            # can handle here if u want
            raise e

    def _check_leadership(self):
        """Determine if this instance is the leader and set up watcher if not."""
        try:
            children = self.zk.get_children(self.broker_election_path)
            children.sort() # Sort to find the lowest sequence number

            self.leader_sequence_num = int(self.my_election_znode.split("_")[-1])

            # pdb.set_trace()
            if self.my_election_znode == self.broker_election_path + "/" + children[0]: # Check if our znode is the first one
                self.logger.info("DiscoveryMW::_check_leadership - I am the leader!")
                self.become_leader()
            else:
                leader_znode_name = children[0]
                self.logger.info(f"DiscoveryMW::_check_leadership - I am a replica. Current leader: {leader_znode_name}")
                leader_seq_num = int(leader_znode_name.split("_")[-1])

                # Find the znode just before ours in sequence
                my_index = children.index(os.path.basename(self.my_election_znode))
                if my_index > 0:
                    prev_znode_name = children[my_index - 1]
                    self.next_leader_candidate_path = self.broker_election_path + "/" + prev_znode_name
                    self.logger.debug(f"Watching for deletion of: {self.next_leader_candidate_path}")
                    self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher) # Set watch on the previous znode
                else: # We are the second in line, watching the current leader
                    self.next_leader_candidate_path = self.broker_election_path + "/" + leader_znode_name
                    self.logger.debug(f"Watching for deletion of leader: {self.next_leader_candidate_path}")
                    self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)

        except Exception as e:
            self.logger.error(f"DiscoveryMW::_check_leadership - Error checking leadership: {e}")
            raise e

    def become_leader(self):
        """Actions to perform when this instance becomes the leader."""
        self.upcall_obj.began_running = time.time() 
        if not self.is_leader:
            self.logger.info("DiscoveryMW::become_leader - Transitioning to leader state.")
            self.is_leader = True
            self.leader_event.set()
            # Optionally, perform any other leader-specific initialization here

    def resign_leadership(self): # Optional - for graceful shutdown if needed
        """Actions to perform when resigning leadership (e.g., during shutdown)."""
        if self.is_leader:
            self.logger.info("DiscoveryMW::resign_leadership - Resigning leadership.")
            self.is_leader = False
            if self.my_election_znode and self.zk.exists(self.my_election_znode):
                self.zk.delete(self.my_election_znode)
                self.logger.info(f"deleted ephemeral election node")

            if self.zk.exists("/broker/primary"):
                self.zk.delete("/broker/primary")
                self.logger.info(f"deleted primary node")

    def _leader_watcher(self, event):
        """ZooKeeper watcher callback for leadership changes."""
        try:
            if event and event.type == "DELETED":
                self.logger.info(f"DiscoveryMW::_leader_watcher - Watched znode {event.path} deleted.")
                self.logger.info("Attempting to become the new leader...")
                # Force a re-check of all children to ensure proper election
                children = self.zk.get_children(self.broker_election_path)
                children.sort()
                
                # Check if we might be the new leader based on current nodes
                if children and os.path.basename(self.my_election_znode) == children[0]:
                    self.logger.info("I might be the new leader - attempting election")
                
                if not self.is_leader:
                    self._attempt_leader_election()
                    # Verify success after attempt
                    time.sleep(0.5)  # Small delay to let ZK propagate changes
                    self._check_leadership()  # Double-check leadership status

            elif event is None: # Initial call of watcher, or node exists
                exists = self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)
                if not exists: # If the node disappeared between setting the watch and the callback
                    self.logger.info(f"DiscoveryMW::_leader_watcher - Watched znode {self.next_leader_candidate_path} disappeared before callback, attempting leadership.")
                    if not self.is_leader:
                        self._attempt_leader_election()

            else:
                self.logger.debug(f"DiscoveryMW::_leader_watcher - Event: {event}") # Log other events if needed

        except NoNodeError: # Possible race condition where the watched node is deleted very quickly
            self.logger.warning("DiscoveryMW::_leader_watcher - Watched znode disappeared quickly, attempting leadership.")
            if not self.is_leader:
                self._attempt_leader_election()
        except Exception as e:
            self.logger.error(f"DiscoveryMW::_leader_watcher - Exception in watcher callback: {e}")
            self._handle_zk_error() # Handle ZK errors in watcher too


            # Optionally, clean up leader-specific resources

    def _handle_zk_error(self):
        """Handles ZooKeeper related errors with basic retry logic."""
        self.logger.error("DiscoveryMW::_handle_zk_error - A ZooKeeper error occurred.")
        
        # Check ZooKeeper connection
        if self.zk.state != 'CONNECTED':
            self.logger.info("ZooKeeper disconnected. Attempting to reconnect...")
            try:
                self.zk.stop()
                self.zk.close()
                time.sleep(1)
                self.zk = KazooClient(hosts=self.zk_addr)
                self.zk.start(timeout=10)
                
                # Reinitialize election participation
                self.my_election_znode = None  # Reset so we create a new one
                self._attempt_leader_election()
                self.logger.info("Successfully reconnected to ZooKeeper")
            except Exception as e:
                self.logger.error(f"Failed to reconnect to ZooKeeper: {e}")
                # In a real system, you might want to exit here if critical


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
                children = self.zk.get_children(self.discovery_election_path, watch=leader_watch)
                if children:
                    # Sort children to determine the leader
                    children.sort()
                    leader_znode = self.discovery_election_path + "/" + children[0]
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
            children = self.zk.get_children(self.discovery_election_path, watch=self._leader_watch_callback)
            if children:
                children.sort()  # Leader is the one with the smallest sequence number
                leader_znode = self.discovery_election_path + "/" + children[0]
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
            # Retry logic for reading znode data
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    new_address, new_port = self.read_discovery_znode(new_leader_znode)
                    new_connect_str = f"tcp://{new_address}:{new_port}"
                    
                    self.logger.info(f"Reconnecting to new leader at {new_connect_str}, attempt {attempt+1}")
                    
                    # Disconnect from old leader if needed
                    if self.current_connect_str and self.current_connect_str != new_connect_str:
                        try:
                            self.req.disconnect(self.current_connect_str)
                        except Exception as e:
                            self.logger.warning(f"Error disconnecting from old leader: {e}")
                            
                        # Create a new socket to ensure clean connection
                        self.req.close()
                        self.req = self.context.socket(zmq.REQ)
                    
                    # Connect to new leader
                    self.req.connect(new_connect_str)
                    self.current_connect_str = new_connect_str
                    self.logger.info(f"Successfully connected to new leader at {new_connect_str}")
                    
                    # Try a simple message to verify connection
                    # self.register(self.id)  # Re-register with new leader
                    
                    return  # Success
                    
                except Exception as e:
                    self.logger.error(f"Error connecting to leader, attempt {attempt+1}: {e}")
                    time.sleep(1)  # Backoff before retry
                    
            self.logger.error("Failed to connect to new leader after multiple attempts")
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
            children = self.zk.get_children(self.discovery_election_path, watch=self._leader_watch_callback)
            if not children:
                self.logger.info("No candidates in election path yet.")
                return

            children.sort()
            new_leader_znode = self.discovery_election_path + "/" + children[0]
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
            self.resign_leadership()
            if self.sub:
                self.sub.close()
            if self.pub:
                self.pub.close()
            if self.req:
                self.req.close()
            if self.zk:
                self.zk.stop()
            
            # 3. Terminate the ZMQ context (make sure you store it during configuration)
            if hasattr(self, "context") and self.context:
                self.context.term()
                self.logger.info("DiscoveryMW::cleanup - ZMQ context terminated.")
                self.context = None

            # 4. Clean up the poller (unregister sockets if needed)
            if self.poller:
                # Unregister any sockets; poller cleanup is not as critical since it's managed in Python.
                try:
                    self.poller.unregister(self.rep)
                except Exception:
                    pass  # Socket may already be unregistered/closed.
                self.poller = None



            # 5. Clean up the ZooKeeper connection.
            self.zk.stop()
            self.zk.close()
            self.logger.info("Closed ZooKeeper connection.")

            # At this point, all znodes and local middleware state are cleaned up.
            # To rejoin the election, you can simply call your configuration method again,
            # which will create a new ephemeral sequential node and reinitialize the state.

        except Exception as e:
            self.logger.error("Exception in BrokerMW::cleanup: " + str(e))
            raise e

    def disable_event_loop(self):
        self.handle_events = False
