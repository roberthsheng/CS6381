###############################################
#
# Purpose: Discovery service middleware implementation
#
###############################################
import pdb
import os
import socket
import sys
import time
import logging
import threading
import zmq
from kazoo.client import KazooClient, NoNodeError, NodeExistsError
import json # For state serialization
from CS6381_MW import discovery_pb2


class DiscoveryMW:
    """Middleware class for the discovery service"""

    def __init__(self, logger):
        self.logger = logger
        self.rep = None  # ZMQ REP socket for handling requests
        self.poller = None  # ZMQ poller for event handling
        self.handle_events = True  # Event loop control
        self.upcall_obj = None  # Reference to application layer
        self.zk = None 
        self.zk_addr = None
        self.election_path = "/discovery_election"
        self.state_path = "/discovery_state"
        self.my_election_znode = None
        self.leader_sequence_num = None
        self.next_leader_candidate_path = None
        self.is_leader = False
        self.address_info = None
        self.leader_event = threading.Event()

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")
            self.addr = args.addr
            self.port = args.port
            self.discovery_election_path = "/discovery_election"
            self.broker_election_path = "/broker_election"  # Set before _init_zk
            self.zk_addr = args.zk_addr
            self.logger.debug(f"BrokerMW::configure - Paths set: broker={self.broker_election_path}, discovery={self.discovery_election_path}")
            self._init_zk()

            self.req = self.context.socket(zmq.REQ)
            leader_znode_path = self.wait_for_leader()
            self.connect_to_leader(leader_znode_path)

            self.sub = self.context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
            self.sub.connect("tcp://10.0.0.5:5570")  # From your read branch
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
            self.logger.debug("BrokerMW::_init_zk - Starting ZooKeeper client")
            self.zk.start(timeout=10)
            self.logger.debug(f"BrokerMW::_init_zk - Checking path: {self.broker_election_path}, type: {type(self.broker_election_path)}")
            if not self.zk.exists(self.broker_election_path):
                self.logger.debug("BrokerMW::_init_zk - Creating election path")
                self.zk.create(self.broker_election_path, value=b"", makepath=True)  # Fixed with value
                self.logger.info(f"BrokerMW::_init_zk - Created election path: {self.broker_election_path}")
            else:
                self.logger.debug(f"BrokerMW::_init_zk - Path {self.broker_election_path} already exists")
            self.logger.debug("BrokerMW::_init_zk - Attempting leader election")
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
                address_data = json.dumps(self.address_info).encode()
                my_znode_path = self.zk.create(self.election_path + "/n_", address_data, ephemeral=True, sequence=True)
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
            children = self.zk.get_children(self.election_path)
            children.sort() # Sort to find the lowest sequence number

            self.leader_sequence_num = int(self.my_election_znode.split("_")[-1])

            # pdb.set_trace()
            if self.my_election_znode == self.election_path + "/" + children[0]: # Check if our znode is the first one
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
                    self.next_leader_candidate_path = self.election_path + "/" + prev_znode_name
                    self.logger.debug(f"Watching for deletion of: {self.next_leader_candidate_path}")
                    self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher) # Set watch on the previous znode
                else: # We are the second in line, watching the current leader
                    self.next_leader_candidate_path = self.election_path + "/" + leader_znode_name
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
            self.restore_state_from_zk() # Restore state from ZooKeeper
            # Optionally, perform any other leader-specific initialization here

    def resign_leadership(self): # Optional - for graceful shutdown if needed
        """Actions to perform when resigning leadership (e.g., during shutdown)."""
        if self.is_leader:
            self.logger.info("DiscoveryMW::resign_leadership - Resigning leadership.")
            self.is_leader = False
            # Optionally, clean up leader-specific resources

    def _leader_watcher(self, event):
        """ZooKeeper watcher callback for leadership changes."""
        try:
            if event and event.type == "DELETED":
                self.logger.info(f"DiscoveryMW::_leader_watcher - Watched znode {event.path} deleted. Previous leader might have failed.")
                if not self.is_leader: # Avoid re-election if we are already leader (e.g., due to session timeout)
                    self._attempt_leader_election() # Try to become the leader again

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

    def _handle_zk_error(self):
        """Handles ZooKeeper related errors. Implement retry/exit logic."""
        self.logger.error("DiscoveryMW::_handle_zk_error - A ZooKeeper error occurred. System might be unstable.")
        # Implement your error handling strategy. Options:
        # 1. Retry connection/election after a delay
        # 2. Disable leader features and run in a degraded mode
        # 3. Terminate the application
        # For now, let's just log and maybe implement retry later.
        pass # Placeholder for error handling logic - TODO: Implement retry/exit strategy


    def event_loop(self, timeout=None):
        try:
            self.logger.info("DiscoveryMW::event_loop - start")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=50))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()

                elif self.rep in events:
                    timeout = self.handle_request()

                else:
                    self.logger.warning(
                        "DiscoveryMW::event_loop - Unknown event encountered"
                    )

            self.logger.info("DiscoveryMW::event_loop - done")

        except Exception as e:
            self.logger.error(f"DiscoveryMW::event_loop - Exception: {str(e)}")
            raise e

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request - Waiting for request...")

            # Receive the request (non-blocking mode for debugging)
            try:
                request_bytes = self.rep.recv()
            except zmq.Again:
                self.logger.warning(
                    "DiscoveryMW::handle_request - No message received (ZMQ Again)"
                )
                return None  # No message, return to event loop

            # Log raw bytes received
            self.logger.info(
                f"DiscoveryMW::handle_request - Received raw bytes: {request_bytes}"
            )

            # Deserialize using protobuf
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(request_bytes)

            # Log parsed request type
            self.logger.info(
                f"DiscoveryMW::handle_request - Parsed request type: {disc_req.msg_type}"
            )
            timeout = 0

            # Handle different message types
            if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info(
                    "DiscoveryMW::handle_request - Handling TYPE_REGISTER request"
                )
                timeout = self.upcall_obj.handle_register(disc_req.register_req)

                # FIX: Send response back to publisher/subscriber
                # self.send_register_response(status=discovery_pb2.STATUS_SUCCESS)


            elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info(
                    "DiscoveryMW::handle_request - Handling TYPE_LOOKUP_PUB_BY_TOPIC request"
                )
                timeout = self.upcall_obj.handle_lookup(disc_req.lookup_req)

            else:
                self.logger.warning(
                    f"DiscoveryMW::handle_request - Unknown request type: {disc_req.msg_type}"
                )

            return timeout

        except Exception as e:
            self.logger.error(f"DiscoveryMW::handle_request - Exception: {str(e)}")
            raise e

    def send_register_response(self, status, reason=None):
        try:
            self.logger.info("DiscoveryMW::send_register_response")

            # Create register response
            register_resp = discovery_pb2.RegisterResp()
            register_resp.status = status
            if reason:
                register_resp.reason = reason

            # Create discovery response
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.msg_type = discovery_pb2.TYPE_REGISTER
            disc_resp.register_resp.CopyFrom(register_resp)

            # Serialize and send response back to Publisher
            buf2send = disc_resp.SerializeToString()
            self.rep.send(buf2send)  # ✅ Ensure this actually sends data

            self.logger.info(
                f"DiscoveryMW::send_register_response - Sent response: {disc_resp}"
            )

        except Exception as e:
            self.logger.error(
                f"DiscoveryMW::send_register_response - Exception: {str(e)}"
            )
            raise e


    def send_lookup_response(self, publisher_list):
        try:
            self.logger.info("DiscoveryMW::send_lookup_response")

            # Create lookup response
            lookup_resp = discovery_pb2.LookupPubByTopicResp()

            # Add each publisher (which are already RegistrantInfo objects)
            for pub in publisher_list:
                lookup_resp.publishers.append(pub)

            # Create discovery response
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_resp.lookup_resp.CopyFrom(lookup_resp)

            # Serialize and send
            buf2send = disc_resp.SerializeToString()
            self.rep.send(buf2send)
            self.logger.info(
                "DiscoveryMW::send_lookup_response - Response sent successfully"
            )

        except Exception as e:
            self.logger.error(
                f"DiscoveryMW::send_lookup_response - Exception: {str(e)}"
            )
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def restore_state_from_zk(self):
        """Restore state from ZooKeeper. To be implemented."""
        try:
            self.logger.info("DiscoveryMW::restore_state_from_zk - Restoring state from ZooKeeper...")
            data, stat = self.zk.get(self.state_path)
            # pdb.set_trace()
            if data:
                state = json.loads(data.decode()) # Assuming state is stored as JSON
                self.upcall_obj.publishers = state.get('publishers', {}) # Restore publishers dict in Appln
                self.upcall_obj.subscribers = state.get('subscribers', {}) # Restore subscribers dict in Appln
                self.upcall_obj.broker_info = state.get('broker', None) # restore broker info if it's there
                self.upcall_obj.registered_publishers = state.get('registered_publishers', 0) # Restore registered publishers count
                self.upcall_obj.registered_subscribers = state.get('registered_subscribers', 0) # Restore registered subscribers count
                self.upcall_obj.lookup_count = state.get('lookup_count', 0) # Restore lookup count
                self.logger.info("DiscoveryMW::restore_state_from_zk - State restored successfully.")
            else:
                self.logger.info("DiscoveryMW::restore_state_from_zk - No state found in ZooKeeper, starting fresh.")
                # If no state, the application will start with empty publishers/subscribers as initialized in DiscoveryAppln

        except NoNodeError:
            self.logger.info("DiscoveryMW::restore_state_from_zk - State znode not found, starting fresh.")
            # No state znode, start fresh (application will initialize empty dicts)
        except Exception as e:
            self.logger.error(f"DiscoveryMW::restore_state_from_zk - Error restoring state from ZooKeeper: {e}")
            self._handle_zk_error() # Handle ZK errors during restore
            # In case of restore failure, it's critical - consider app termination or degraded mode.

    def persist_state_to_zk(self):
        """Persist current state to ZooKeeper. To be implemented."""
        if self.is_leader: # Only leader persists state
            try:
                self.logger.debug("DiscoveryMW::persist_state_to_zk - Persisting state to ZooKeeper...")
                state = { # Gather state from the application layer
                    'publishers': self.upcall_obj.publishers,
                    'subscribers': self.upcall_obj.subscribers,
                    'broker': self.upcall_obj.broker_info,
                    'registered_publishers': self.upcall_obj.registered_publishers,
                    'registered_subscribers': self.upcall_obj.registered_subscribers,
                    'lookup_count': self.upcall_obj.lookup_count
                }
                state_json = json.dumps(state)
                self.zk.set(self.state_path, state_json.encode()) # Update state znode
                self.logger.debug("DiscoveryMW::persist_state_to_zk - State persisted successfully.")
            except Exception as e:
                self.logger.error(f"DiscoveryMW::persist_state_to_zk - Error persisting state to ZooKeeper: {e}")
                self._handle_zk_error() # Handle ZK errors during persist
        else:
            self.logger.debug("DiscoveryMW::persist_state_to_zk - Not leader, skipping state persistence.")

    def get_local_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Connect to an external IP. The connection doesn't have to succeed.
            s.connect(('8.8.8.8', 80))
            ip_address = s.getsockname()[0]
        except Exception:
            ip_address = '127.0.0.1'
        finally:
            s.close()
        return ip_address

    def cleanup(self):
        """Clean up middleware state by deleting znodes and resetting the election state."""
        try:
            self.logger.info("DiscoveryMW::cleanup - Cleaning up all ZooKeeper state.")

            # 1. Delete the ephemeral election znode (if it exists).
            if self.my_election_znode and self.zk.exists(self.my_election_znode):
                self.zk.delete(self.my_election_znode)
                self.logger.info(f"Deleted ephemeral election znode: {self.my_election_znode}")
                self.my_election_znode = None

            #  Stop the event loop
            self.handle_events = False

            # 2. Close the REP socket if it exists.
            if self.rep:
                # Close immediately (linger=0 ensures pending messages are discarded)
                self.rep.close(linger=0)
                self.logger.info("DiscoveryMW::cleanup - REP socket closed.")
                self.rep = None

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
            self.logger.error(f"DiscoveryMW::cleanup - Exception during cleanup: {e}")
