###############################################
#
# Purpose: Publisher middleware implementation
#
###############################################

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

    def configure(self, args):
        try:
            self.logger.info("PublisherMW::configure")

            # Initialize our variables
            self.port = args.port
            self.addr = socket.gethostbyname(socket.gethostname())

            # Get the ZMQ context
            context = zmq.Context()

            # Get the ZMQ poller object
            self.poller = zmq.Poller()

            # Acquire the REQ and PUB sockets
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)

            # Register the REQ socket for incoming events
            self.poller.register(self.req, zmq.POLLIN)

            # Connect to discovery service
            self.logger.debug("PublisherMW::configure - connect to Discovery service")
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Bind the PUB socket
            self.logger.debug("PublisherMW::configure - bind to pub socket")
            bind_string = f"tcp://*:{self.port}"
            self.pub.bind(bind_string)

            self.logger.info("PublisherMW::configure completed")

        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - run the event loop")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    # timeout occurred, let application handle
                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:
                    # handle response from discovery
                    timeout = self.handle_reply()

                else:
                    raise Exception("Unknown event after poll")

            self.logger.info("PublisherMW::event_loop - out of the event loop")

        except Exception as e:
            raise e

    def handle_reply(self):
        try:
            self.logger.info("PublisherMW::handle_reply")

            # Receive the reply
            bytesRcvd = self.req.recv()

            # Deserialize using protobuf
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
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

            # Build register request
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

    def is_ready(self):
        try:
            self.logger.info("PublisherMW::is_ready")

            # Build isready request
            isready_req = discovery_pb2.IsReadyReq()

            # Build discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)

            # Serialize and send
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)

        except Exception as e:
            raise e

    def disseminate(self, id, topic, data):
        try:
            self.logger.debug("PublisherMW::disseminate")

            pub_msg = topic_pb2.Publication()
            pub_msg.publisher_id = id
            pub_msg.topic = topic
            pub_msg.data = data
            pub_msg.timestamp = int(time.time() * 1000) # Time in milliseconds

            buf2send = pub_msg.SerializeToString()
            self.pub.send_multipart([topic.encode('utf-8'), buf2send])

        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
