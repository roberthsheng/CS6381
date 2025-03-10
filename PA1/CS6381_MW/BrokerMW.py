import time
import zmq
import configparser
import socket
from CS6381_MW import discovery_pb2, topic_pb2


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

            # Get ZMQ context
            context = zmq.Context()

            # create sockets, connect to discovery, etc here.
            self.req = context.socket(zmq.REQ)
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Create SUB socket for receiving publications
            self.sub = context.socket(zmq.SUB)

            # Create PUB socket for sending to subscribers
            self.pub = context.socket(zmq.PUB)
            bind_str = f"tcp://{args.addr}:{args.port}"  # you will need to read this from config.ini
            self.pub.bind(bind_str)

            # Create poller
            self.poller = zmq.Poller()
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW::event_loop - start")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    # Timeout occurred, let application decide what to do
                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:
                    # Handle reply from discovery service
                    timeout = self.handle_discovery_reply()

                elif self.sub in events:
                    # Handle incoming publication
                    timeout = self.handle_publication()

                else:
                    raise Exception("Unknown event")

            self.logger.info("BrokerMW::event_loop - done")

        except Exception as e:
            raise e

    def handle_discovery_reply(self):
        try:
            self.logger.info("BrokerMW::handle_discovery_reply")
            # Receive and deserialize reply
            reply_bytes = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(reply_bytes)

            # Handle different response types
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            else:
                raise ValueError("Unknown response type")

            return timeout

        except Exception as e:
            raise e

    def lookup_all_publishers(self):
        try:
            self.logger.info("Broker::lookup_all_publishers")

            # Create lookup request
            lookup_req = discovery_pb2.LookupPubByTopicReq()

            # Create discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_req.lookup_req.CopyFrom(lookup_req)

            # Serialize and send
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)

        except Exception as e:
            raise e

    def connect_to_publishers(self, publishers):
        try:
            self.logger.info("BrokerMW::connect_to_publishers")

            # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            dissemination = config["Dissemination"]["Strategy"]

            # Connect to each publisher
            for pub in publishers:
                # Access fields through protobuf getters
                connect_str = f"tcp://{pub.addr}:{pub.port}"
                self.sub.connect(connect_str)
                self.logger.debug(
                    f"Connected to publisher {pub.id} at {connect_str}"
                )

            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        except Exception as e:
            self.logger.error(f"Error in connect_to_publishers: {str(e)}")
            raise e

    def is_ready(self):
        try:
            self.logger.info("BrokerMW::is_ready")
            # Create isready request
            isready_req = discovery_pb2.IsReadyReq()

            # Create discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)

            # Serialize and send the request
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
        except Exception as e:
            self.logger.error(f"BrokerMW::is_ready - Exception: {str(e)}")
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

    def register(self, name):
        try:
            self.logger.info("BrokerMW::register")
            # Create register request
            register_req = discovery_pb2.RegisterReq()
            register_req.role = (
                discovery_pb2.ROLE_BOTH
            )  # Broker is both publisher and subscriber

            # Add registrant info
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            register_req.info.CopyFrom(reg_info)

            # Create discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            # Serialize and send
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
        except Exception as e:
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
                self.poller.unregister(self.sub)
                self.poller.unregister(self.pub)
                self.poller.unregister(self.req)
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise e
