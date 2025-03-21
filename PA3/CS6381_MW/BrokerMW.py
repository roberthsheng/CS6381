import time
import pdb
import zmq
import configparser
import socket
from CS6381_MW import discovery_pb2, topic_pb2
from kazoo.client import KazooClient, NodeExistsError, NoNodeError
import json 

class BrokerMW:
    def __init__(self, logger):
        self.logger = logger
        self.sub = None  # ZMQ SUB socket to receive publications
        self.pub = None  # ZMQ PUB socket to send to subscribers
        self.req = None  # ZMQ REQ socket to talk to discovery
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.broker_binding = None  # Used when broker based dissemination is used.
        self.subscribers = {}  # track the subscribers

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")

            self.port = args.port
            self.addr = socket.gethostbyname(socket.gethostname())

            self.broker_path = "/brokers"
            self.publisher_path = "/publishers"
            self.zk_addr = args.zk_addr
            self._init_zk()
            self.create_broker_znode()
            self.connected_publishers = set()

            # 

            # Get ZMQ context
            context = zmq.Context()

            # create sockets, connect to discovery, etc here.
            # Create SUB socket for receiving publications
            self.sub = context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

            # Create PUB socket for sending to subscribers
            self.pub = context.socket(zmq.PUB)
            bind_str = f"tcp://{args.addr}:{args.port}"  # you will need to read this from config.ini
            self.pub.bind(bind_str)

            # Create poller
            self.poller = zmq.Poller()
            self.poller.register(self.sub, zmq.POLLIN)

            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            raise e

    def _init_zk(self):
        """Initialize ZooKeeper connection and ensure paths exist."""
        try:
            self.logger.info("DiscoveryMW::_init_zk - Connecting to ZooKeeper at {}".format(self.zk_addr))
            self.zk = KazooClient(hosts=self.zk_addr)
            self.zk.start(timeout=10) # Increased timeout for robustness

            # Ensure election path exists
            if not self.zk.exists(self.broker_path):
                try:
                    self.zk.create(self.broker_path, makepath=True)
                    self.logger.info(f"Created election path: {self.broker_path}")
                except NodeExistsError:
                    self.logger.warning(f"Election path {self.broker_path} already exists, likely created concurrently.")

            if not self.zk.exists(self.publisher_path):
                try:
                    self.zk.create(self.publisher_path, makepath=True)
                    self.logger.info(f"Created election path: {self.publisher_path}")
                except NodeExistsError:
                    self.logger.warning(f"Election path {self.publisher_path} already exists, likely created concurrently.")
        except Exception as e:
            self.logger.error(f"DiscoveryMW::_init_zk - ZooKeeper initialization failed: {e}")
            # Implement a zk error handler (e.g., retry, exit)
            raise e

    def create_broker_znode(self):
        """Create the broker znode for this broker instance"""
        try:
            self.logger.info("BrokerMW::create_broker_znode")
            
            # Ensure parent path exists
            if not self.zk.exists(self.broker_path):
                self.zk.create(self.broker_path, b"", makepath=True)
                self.logger.info(f"Created broker path: {self.broker_path}")
            
            broker_id = f"broker-{socket.gethostname()}"
            # Include a subscriber count initialized to 0
            broker_info = {
                "addr": self.addr,
                "port": self.port,
                "subscribers": 0
            }
            broker_data = json.dumps(broker_info).encode("utf-8")
            broker_znode = f"node_"
            
            if not self.zk.exists(broker_znode):
                self.zk.create(broker_znode, broker_data, ephemeral=True, sequence=True)
                self.logger.info(f"Registered broker znode: {broker_znode}")

        except Exception as e:
            self.logger.error("Exception in BrokerMW::create_broker_znode")
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW::event_loop - start")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    # Timeout occurred, let application decide what to do
                    timeout = self.upcall_obj.invoke_operation()

                elif self.sub in events:
                    # Handle incoming publication
                    timeout = self.handle_publication()

                else:
                    raise Exception("Unknown event")

            self.logger.info("BrokerMW::event_loop - done")

        except Exception as e:
            raise e

    def connect_to_publishers(self):
        try:
            self.logger.info("BrokerMW::connect_to_publishers using ZooKeeper")
                        
            def connect_new_node(child):
                node_path = f"{self.publisher_path}/{child}"
                data, stat = self.zk.get(node_path)
                try:
                    # Parse JSON data to retrieve connection details
                    publisher_info = json.loads(data.decode("utf-8"))
                    addr = publisher_info["addr"]
                    port = publisher_info["port"]
                    connect_str = f"tcp://{addr}:{port}"
                    self.sub.connect(connect_str)
                    self.logger.debug(f"Connected to publisher {child} at {connect_str}")
                    self.connected_publishers.add(child)
                except Exception as e:
                    self.logger.error(f"Error parsing publisher data for {child}: {e}")

            # Get current publishers under the publisher path and connect to them.
            children = self.zk.get_children(self.publisher_path)
            for child in children:
                if child not in self.connected_publishers:
                    connect_new_node(child)
            
            # Watch for changes in the publisher path. When new children appear,
            # the watch callback is invoked, and we connect to any new publishers.
            @self.zk.ChildrenWatch(self.publisher_path)
            def watch_publishers(children):
                for child in children:
                    if child not in self.connected_publishers:
                        self.logger.info(f"New publisher detected: {child}")
                        connect_new_node(child)
                        
            # (Optional) You might also want to handle publisher removals by watching for deletions.
        
                        
        except Exception as e:
            self.logger.error(f"Error in connect_to_publishers: {str(e)}")
            raise e

    def handle_publication(self):
        try:
            self.logger.info("BrokerMW::handle_publication")

            # Receive the serialized message
            topic, msg_bytes = self.sub.recv_multipart()

            # Deserialize using protobuf
            pub_msg = topic_pb2.Publication()
            pub_msg.ParseFromString(msg_bytes)

            # Log the message with timestamps (for debugging)
            send_time = pub_msg.timestamp
            publisher_id = pub_msg.publisher_id
            recv_time = int(time.time() * 1000)
            latency = recv_time - send_time
            self.logger.debug(
                f"Broker received publication on topic {pub_msg.topic}, latency: {latency} ms"
            )

            # Pass topic and data to the upcall
            timeout = self.upcall_obj.handle_publication(pub_msg.topic, pub_msg.data, send_time, publisher_id)

            return timeout

        except Exception as e:
            raise e

    def disseminate(self, topic, data, old_timestamp, publisher_id):
        try:
            # pdb.set_trace()
            leader_id = self.get_topic_leader(topic)
            if leader_id != publisher_id:
                self.logger.debug(f"Publisher {publisher_id} is not the leader for topic {topic}. Skipping dissemination.")
                return

            self.logger.debug("BrokerMW::disseminate")
            pub_msg = topic_pb2.Publication()
            pub_msg.publisher_id = publisher_id  # ensure the same id is used
            pub_msg.topic = topic
            pub_msg.data = data
            pub_msg.timestamp = old_timestamp
            buf2send = pub_msg.SerializeToString()
            self.pub.send_multipart([topic.encode("utf-8"), buf2send])
        except Exception as e:
            raise e

    def get_topic_leader(self, topic):
        topic_path = f"/topics/{topic}"
        children = self.zk.get_children(topic_path)
        if not children:
            return None  # Or handle the no-publisher case appropriately
        # The node with the smallest sequence is typically the leader
        leader_node = sorted(children)[0]
        full_path = f"{topic_path}/{leader_node}"
        data, _ = self.zk.get(full_path)
        leader_info = json.loads(data.decode("utf-8"))
        return leader_info.get("publisher_id")


    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def cleanup(self):
        try:
            self.logger.info("BrokerMW::cleanup")
            if self.sub:
                self.sub.close()
            if self.pub:
                self.pub.close()
            if self.req:
                self.req.close()
            if self.poller:
                self.poller.unregister(self.pub)
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise e
