###############################################
#
# Purpose: Discovery service middleware implementation
#
###############################################

import os
import sys
import time
import logging
import zmq
from CS6381_MW import discovery_pb2


class DiscoveryMW:
    """Middleware class for the discovery service"""

    def __init__(self, logger):
        self.logger = logger
        self.rep = None  # ZMQ REP socket for handling requests
        self.poller = None  # ZMQ poller for event handling
        self.handle_events = True  # Event loop control
        self.upcall_obj = None  # Reference to application layer

    def configure(self, args):
        try:
            self.logger.info("DiscoveryMW::configure")

            # Get ZMQ context
            context = zmq.Context()

            # Create REP socket for handling requests
            self.rep = context.socket(zmq.REP)

            # Get the poller object
            self.poller = zmq.Poller()
            self.poller.register(self.rep, zmq.POLLIN)

            # Decide the binding string for the REP socket
            bind_string = f"tcp://*:{args.port}"
            self.rep.bind(bind_string)

            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        try:
            self.logger.info("DiscoveryMW::event_loop - start")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()

                elif self.rep in events:
                    timeout = self.handle_request()

                else:
                    raise Exception("Unknown event")

            self.logger.info("DiscoveryMW::event_loop - done")

        except Exception as e:
            raise e

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request")

            # Receive the request
            request_bytes = self.rep.recv()

            # Deserialize using protobuf
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(request_bytes)

            # Handle different message types
            if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.handle_register(disc_req.register_req)
            elif disc_req.msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.handle_isready(disc_req.isready_req)
            elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.handle_lookup(disc_req.lookup_req)
            else:
                raise ValueError("Unhandled message type")

            return timeout

        except Exception as e:
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

            # Serialize and send
            buf2send = disc_resp.SerializeToString()
            self.rep.send(buf2send)

        except Exception as e:
            self.logger.error(f"Error in send_register_response: {str(e)}")
            raise e

    def send_isready_response(self, status):
        try:
            self.logger.info("DiscoveryMW::send_isready_response")

            # Create isready response
            isready_resp = discovery_pb2.IsReadyResp()
            isready_resp.status = status

            # Create discovery response
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.msg_type = discovery_pb2.TYPE_ISREADY
            disc_resp.isready_resp.CopyFrom(isready_resp)

            # Serialize and send
            buf2send = disc_resp.SerializeToString()
            self.rep.send(buf2send)

        except Exception as e:
            self.logger.error(f"Error in send_isready_response: {str(e)}")
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

        except Exception as e:
            self.logger.error(f"Error in send_lookup_response: {str(e)}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
