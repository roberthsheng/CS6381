###############################################
#
# Purpose: Subscriber middleware implementation
#
###############################################

import os
import sys
import time
import logging
import zmq
import configparser
from CS6381_MW import discovery_pb2, topic_pb2


class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # ZMQ REQ socket for discovery service
        self.sub = None  # ZMQ SUB socket for receiving publications
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.broker_binding = None  # Used in broker-based dissemination

    def configure(self, args):
        try:
            self.logger.info("SubscriberMW::configure")

            # Get ZMQ context
            context = zmq.Context()

            # Create REQ socket for discovery service
            self.req = context.socket(zmq.REQ)
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Create SUB socket for receiving publications
            self.sub = context.socket(zmq.SUB)

            # Create poller
            self.poller = zmq.Poller()
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.logger.info("SubscriberMW::configure completed")

        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("SubscriberMW::event_loop - start")

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

            self.logger.info("SubscriberMW::event_loop - done")

        except Exception as e:
            raise e

    def register(self, name, topics):
        try:
            self.logger.info("SubscriberMW::register")

            # Create register request
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER

            # Add registrant info
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist.extend(topics)

            # Create discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            # Serialize and send
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)

        except Exception as e:
            raise e

    def lookup_publishers(self, topics):
        try:
            self.logger.info("SubscriberMW::lookup_publishers")

            # Create lookup request
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist.extend(topics)

            # Create discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_req.lookup_req.CopyFrom(lookup_req)

            # Serialize and send
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)

        except Exception as e:
            raise e

    def handle_discovery_reply(self):
        try:
            self.logger.info("SubscriberMW::handle_discovery_reply")

            # Receive and deserialize reply
            reply_bytes = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(reply_bytes)

            # Handle different response types
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            else:
                raise ValueError("Unknown response type")

            return timeout

        except Exception as e:
            raise e

    # def handle_publication(self):
    #     try:
    #         self.logger.info("SubscriberMW::handle_publication")

    #         # Receive publication
    #         data = self.sub.recv_string()

    #         # Parse topic and value (assuming "topic:value" format)
    #         topic, value = data.split(":", 1)

    #         # Let application handle the data
    #         timeout = self.upcall_obj.handle_publication(topic, value)

    #         return timeout

    #     except Exception as e:
    #         raise e


    def handle_publication(self):
        try:
            self.logger.info("SubscriberMW::handle_publication")

            # Receive the serialized message
            msg_bytes = self.sub.recv()

            # Deserialize using protobuf
            pub_msg = topic_pb2.Publication()
            pub_msg.ParseFromString(msg_bytes)

            # Log the message with timestamps (for debugging)
            send_time = pub_msg.timestamp
            recv_time = int(time.time() * 1000)
            latency = recv_time - send_time
            self.logger.debug(f"Received publication on topic {pub_msg.topic}, latency: {latency} ms")

            # Pass topic and data to the upcall
            timeout = self.upcall_obj.handle_publication(pub_msg.topic, pub_msg.data)

            return timeout

        except Exception as e:
            raise e

    def connect_to_publishers(self, publishers, topics):
        try:
            self.logger.info("SubscriberMW::connect_to_publishers")

            # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            dissemination = config["Dissemination"]["Strategy"]

            if dissemination == "Direct":
                # Connect to each publisher
                for pub in publishers:
                    # Access fields through protobuf getters
                    connect_str = f"tcp://{pub.addr}:{pub.port}"
                    self.sub.connect(connect_str)
                    self.logger.debug(
                        f"Connected to publisher {pub.id} at {connect_str}"
                    )

                # Subscribe to all topics
                for topic in topics:
                    self.logger.debug(f"Subscribing to topic: {topic}")
                    self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

            elif dissemination == "ViaBroker":
                # Connect to broker
                if not self.broker_binding:
                    raise Exception("Broker binding not set")
                self.sub.connect(self.broker_binding)

                # Subscribe to topics
                for topic in topics:
                    self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

            else:
                raise ValueError(f"Unknown dissemination strategy: {dissemination}")

            self.logger.info(
                "Successfully connected to publishers and subscribed to topics"
            )

        except Exception as e:
            self.logger.error(f"Error in connect_to_publishers: {str(e)}")
            raise e

    def set_broker_binding(self, binding):
        self.broker_binding = binding

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
