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
import json  # For state serialization
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
        self.quorum_size = 2  # Quorum size for replicas (set for demo)
        self.replica_count = 0  # Current number of replicas

    def configure(self, args):
        try:
            self.logger.info("DiscoveryMW::configure")

            # Store ZooKeeper address
            self.zk_addr = args.zk_addr

            # Get ZMQ context
            context = zmq.Context()

            # Create REP socket for handling requests
            self.rep = context.socket(zmq.REP)

            # Get the poller object
            self.poller = zmq.Poller()
            self.poller.register(self.rep, zmq.POLLIN)

            # Decide the binding string for the REP socket
            bind_string = f"tcp://{args.addr}:{args.port}"
            self.rep.bind(bind_string)

            # Record address info for other parties
            self.address_info = {
                "address": args.addr, 
                "port": args.port
            }

            self.logger.info(
                f"DiscoveryMW::configure - Listening on {bind_string}, connecting to ZK at {self.zk_addr}"
            )

            # Initialize ZooKeeper connection and leader election
            self._init_zk()
            self.logger.info(
                f"DiscoveryMW::configure completed. Listening on {bind_string}"
            )

        except Exception as e:
            self.logger.error(f"DiscoveryMW::configure - Exception: {str(e)}")
            raise e

    def _init_zk(self):
        """Initialize ZooKeeper connection and participate in leader election."""
        try:
            self.logger.info("DiscoveryMW::_init_zk - Connecting to ZooKeeper at {}".format(self.zk_addr))
            self.zk = KazooClient(hosts=self.zk_addr)
            self.zk.start(timeout=10)  # Increased timeout for robustness

            # Ensure election path exists
            if not self.zk.exists(self.election_path):
                try:
                    self.zk.create(self.election_path, makepath=True)
                    self.logger.info(f"Created election path: {self.election_path}")
                except NodeExistsError:
                    self.logger.warning(f"Election path {self.election_path} already exists, likely created concurrently.")

            # Ensure state path exists
            if not self.zk.exists(self.state_path):
                try:
                    self.zk.create(self.state_path, b'{}', makepath=True)  # Initialize with empty JSON object
                    self.logger.info(f"Created state path: {self.state_path}")
                except NodeExistsError:
                    self.logger.warning(f"State path {self.state_path} already exists, likely created concurrently.")

            self._attempt_leader_election()
            self._monitor_replicas()  # Start monitoring replicas

        except Exception as e:
            self.logger.error(f"DiscoveryMW::_init_zk - ZooKeeper initialization failed: {e}")
            raise e

    def _attempt_leader_election(self):
        """Attempt to become the leader by creating an ephemeral sequential znode."""
        try:
            self.logger.info("DiscoveryMW::_attempt_leader_election - Attempting to become leader")
            if not self.my_election_znode:
                address_data = json.dumps(self.address_info).encode()
                my_znode_path = self.zk.create(self.election_path + "/n_", address_data, ephemeral=True, sequence=True)
                self.my_election_znode = my_znode_path
                self.logger.debug(f"Created election znode: {my_znode_path}")

            self._check_leadership()

        except Exception as e:
            self.logger.error(f"DiscoveryMW::_attempt_leader_election - Leader election attempt failed: {e}")
            raise e

    def _check_leadership(self):
        """Determine if this instance is the leader and set up watcher if not."""
        try:
            children = self.zk.get_children(self.election_path)
            children.sort()  # Sort to find the lowest sequence number

            self.leader_sequence_num = int(self.my_election_znode.split("_")[-1])

            if self.my_election_znode == self.election_path + "/" + children[0]:
                self.logger.info("DiscoveryMW::_check_leadership - I am the leader!")
                self.become_leader()
            else:
                leader_znode_name = children[0]
                self.logger.info(f"DiscoveryMW::_check_leadership - I am a replica. Current leader: {leader_znode_name}")
                leader_seq_num = int(leader_znode_name.split("_")[-1])

                my_index = children.index(os.path.basename(self.my_election_znode))
                if my_index > 0:
                    prev_znode_name = children[my_index - 1]
                    self.next_leader_candidate_path = self.election_path + "/" + prev_znode_name
                    self.logger.debug(f"Watching for deletion of: {self.next_leader_candidate_path}")
                    self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)
                else:
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
            self.restore_state_from_zk()

    def _leader_watcher(self, event):
        """ZooKeeper watcher callback for leadership changes."""
        try:
            if event and event.type == "DELETED":
                self.logger.info(f"DiscoveryMW::_leader_watcher - Watched znode {event.path} deleted. Previous leader might have failed.")
                if not self.is_leader:
                    self._attempt_leader_election()

            elif event is None:
                exists = self.zk.exists(self.next_leader_candidate_path, watch=self._leader_watcher)
                if not exists:
                    self.logger.info(f"DiscoveryMW::_leader_watcher - Watched znode {self.next_leader_candidate_path} disappeared before callback, attempting leadership.")
                    if not self.is_leader:
                        self._attempt_leader_election()

            else:
                self.logger.debug(f"DiscoveryMW::_leader_watcher - Event: {event}")

        except NoNodeError:
            self.logger.warning("DiscoveryMW::_leader_watcher - Watched znode disappeared quickly, attempting leadership.")
            if not self.is_leader:
                self._attempt_leader_election()
        except Exception as e:
            self.logger.error(f"DiscoveryMW::_leader_watcher - Exception in watcher callback: {e}")
            self._handle_zk_error()

    def _monitor_replicas(self):
        """Monitor the number of replicas and revive if necessary."""
        def replica_watcher(event):
            try:
                children = self.zk.get_children(self.election_path)
                self.replica_count = len(children)
                self.logger.info(f"DiscoveryMW::_monitor_replicas - Current replica count: {self.replica_count}")
                if self.replica_count < self.quorum_size:
                    self.logger.warning(f"Quorum not met: {self.replica_count} replicas. Reviving replica.")
                    self.revive_replica()
            except Exception as e:
                self.logger.error(f"Error in replica watcher: {e}")

        self.zk.get_children(self.election_path, watch=replica_watcher)

    def revive_replica(self):
        """Revive a new replica to maintain quorum."""
        try:
            import subprocess
            # Launch a new Discovery instance with a port offset of +10
            subprocess.Popen(["python3", "DiscoveryAppln.py", "--addr", "localhost", "--port", str(int(self.address_info["port"]) + 10), "--zk_addr", self.zk_addr, "--loglevel", "20"])
            self.logger.info("Revived a new Discovery replica.")
        except Exception as e:
            self.logger.error(f"Failed to revive replica: {e}")

    def check_quorum(self):
        """Check if quorum is met before performing operations."""
        children = self.zk.get_children(self.election_path)
        self.replica_count = len(children)
        return self.replica_count >= self.quorum_size

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
                    self.logger.warning("DiscoveryMW::event_loop - Unknown event encountered")

            self.logger.info("DiscoveryMW::event_loop - done")

        except Exception as e:
            self.logger.error(f"DiscoveryMW::event_loop - Exception: {str(e)}")
            raise e

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request - Waiting for request...")

            request_bytes = self.rep.recv()
            self.logger.info(f"DiscoveryMW::handle_request - Received raw bytes: {request_bytes}")

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(request_bytes)
            self.logger.info(f"DiscoveryMW::handle_request - Parsed request type: {disc_req.msg_type}")
            timeout = 0

            # Check quorum before processing requests
            if not self.check_quorum():
                self.logger.warning("Quorum not met. Pausing operations.")
                self.send_register_response(discovery_pb2.STATUS_FAILURE, "Quorum not met")
                return timeout

            if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("DiscoveryMW::handle_request - Handling TYPE_REGISTER request")
                timeout = self.upcall_obj.handle_register(disc_req.register_req)
            elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info("DiscoveryMW::handle_request - Handling TYPE_LOOKUP_PUB_BY_TOPIC request")
                timeout = self.upcall_obj.handle_lookup(disc_req.lookup_req)
            else:
                self.logger.warning(f"DiscoveryMW::handle_request - Unknown request type: {disc_req.msg_type}")

            return timeout

        except Exception as e:
            self.logger.error(f"DiscoveryMW::handle_request - Exception: {str(e)}")
            raise e

    def send_register_response(self, status, reason=None):
        try:
            self.logger.info("DiscoveryMW::send_register_response")

            register_resp = discovery_pb2.RegisterResp()
            register_resp.status = status
            if reason:
                register_resp.reason = reason

            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.msg_type = discovery_pb2.TYPE_REGISTER
            disc_resp.register_resp.CopyFrom(register_resp)

            buf2send = disc_resp.SerializeToString()
            self.rep.send(buf2send)
            self.logger.info(f"DiscoveryMW::send_register_response - Sent response: {disc_resp}")

        except Exception as e:
            self.logger.error(f"DiscoveryMW::send_register_response - Exception: {str(e)}")
            raise e

    def send_lookup_response(self, publisher_list):
        try:
            self.logger.info("DiscoveryMW::send_lookup_response")

            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            for pub in publisher_list:
                lookup_resp.publishers.append(pub)

            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_resp.lookup_resp.CopyFrom(lookup_resp)

            buf2send = disc_resp.SerializeToString()
            self.rep.send(buf2send)
            self.logger.info("DiscoveryMW::send_lookup_response - Response sent successfully")

        except Exception as e:
            self.logger.error(f"DiscoveryMW::send_lookup_response - Exception: {str(e)}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def restore_state_from_zk(self):
        """Restore state from ZooKeeper."""
        try:
            self.logger.info("DiscoveryMW::restore_state_from_zk - Restoring state from ZooKeeper...")
            data, stat = self.zk.get(self.state_path)
            if data:
                state = json.loads(data.decode())
                self.upcall_obj.publishers = state.get('publishers', {})
                self.upcall_obj.subscribers = state.get('subscribers', {})
                self.upcall_obj.broker_info = state.get('broker', None)
                self.upcall_obj.registered_publishers = state.get('registered_publishers', 0)
                self.upcall_obj.registered_subscribers = state.get('registered_subscribers', 0)
                self.upcall_obj.lookup_count = state.get('lookup_count', 0)
                self.logger.info("DiscoveryMW::restore_state_from_zk - State restored successfully.")
            else:
                self.logger.info("DiscoveryMW::restore_state_from_zk - No state found in ZooKeeper, starting fresh.")

        except NoNodeError:
            self.logger.info("DiscoveryMW::restore_state_from_zk - State znode not found, starting fresh.")
        except Exception as e:
            self.logger.error(f"DiscoveryMW::restore_state_from_zk - Error restoring state from ZooKeeper: {e}")
            self._handle_zk_error()

    def persist_state_to_zk(self):
        """Persist current state to ZooKeeper."""
        if self.is_leader:
            try:
                self.logger.debug("DiscoveryMW::persist_state_to_zk - Persisting state to ZooKeeper...")
                state = {
                    'publishers': self.upcall_obj.publishers,
                    'subscribers': self.upcall_obj.subscribers,
                    'broker': self.upcall_obj.broker_info,
                    'registered_publishers': self.upcall_obj.registered_publishers,
                    'registered_subscribers': self.upcall_obj.registered_subscribers,
                    'lookup_count': self.upcall_obj.lookup_count
                }
                state_json = json.dumps(state)
                self.zk.set(self.state_path, state_json.encode())
                self.logger.debug("DiscoveryMW::persist_state_to_zk - State persisted successfully.")
            except Exception as e:
                self.logger.error(f"DiscoveryMW::persist_state_to_zk - Error persisting state to ZooKeeper: {e}")
                self._handle_zk_error()

    def _handle_zk_error(self):
        """Handles ZooKeeper related errors."""
        self.logger.error("DiscoveryMW::_handle_zk_error - A ZooKeeper error occurred.")
        # Placeholder for retry/exit strategy
        pass

    def cleanup(self):
        """Clean up middleware state."""
        try:
            self.logger.info("DiscoveryMW::cleanup - Cleaning up all ZooKeeper state.")

            if self.my_election_znode and self.zk.exists(self.my_election_znode):
                self.zk.delete(self.my_election_znode)
                self.logger.info(f"Deleted ephemeral election znode: {self.my_election_znode}")
                self.my_election_znode = None

            self.handle_events = False

            if self.rep:
                self.rep.close(linger=0)
                self.logger.info("DiscoveryMW::cleanup - REP socket closed.")
                self.rep = None

            if hasattr(self, "context") and self.context:
                self.context.term()
                self.logger.info("DiscoveryMW::cleanup - ZMQ context terminated.")
                self.context = None

            if self.poller:
                try:
                    self.poller.unregister(self.rep)
                except Exception:
                    pass
                self.poller = None

            self.zk.stop()
            self.zk.close()
            self.logger.info("Closed ZooKeeper connection.")

        except Exception as e:
            self.logger.error(f"DiscoveryMW::cleanup - Exception during cleanup: {e}")