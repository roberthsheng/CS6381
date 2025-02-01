# import zmq
# import logging
# from CS6381_MW import discovery_pb2
# import configparser

# class BrokerMW:
#     def __init__(self, logger):
#         self.logger = logger
#         self.sub = None  # ZMQ SUB socket to receive publications
#         self.pub = None  # ZMQ PUB socket to send to subscribers
#         self.req = None  # ZMQ REQ socket to talk to discovery
#         self.poller = None
#         self.upcall_obj = None
#         self.handle_events = True
#         self.broker_binding = None  # Used when broker based dissemination is used.
#         # Add any other variables

#     def configure(self, args):
#         try:
#             self.logger.info("BrokerMW::configure")

#             # Get ZMQ context
#             context = zmq.Context()

#             # create sockets, connect to discovery, etc here.
#             self.req = context.socket(zmq.REQ)
#             connect_str = f"tcp://{args.discovery}"
#             self.req.connect(connect_str)

#             # Create SUB socket for receiving publications
#             self.sub = context.socket(zmq.SUB)

#             # Create PUB socket for sending to subscribers
#             self.pub = context.socket(zmq.PUB)

#             # Bind PUB socket for subscribers to connect to us
#             bind_string = f"tcp://*:{args.port}"
#             self.pub.bind(bind_string)
#             self.logger.debug(f"BrokerMW::configure - bind to pub socket {bind_string}")

#             # Create poller
#             self.poller = zmq.Poller()
#             self.poller.register(self.req, zmq.POLLIN)
#             self.poller.register(self.sub, zmq.POLLIN)

#             self.logger.info("BrokerMW::configure completed")
#         except Exception as e:
#             raise e

#     def event_loop(self, timeout=None):
#         try:
#             self.logger.info("BrokerMW::event_loop - start")

#             while self.handle_events:
#                 events = dict(self.poller.poll(timeout=timeout))

#                 if not events:
#                     # Timeout occurred, let application decide what to do
#                     timeout = self.upcall_obj.invoke_operation()

#                 elif self.req in events:
#                     # Handle reply from discovery service
#                     timeout = self.handle_discovery_reply()

#                 elif self.sub in events:
#                     # Handle incoming publication
#                     timeout = self.handle_publication()

#                 else:
#                     raise Exception("Unknown event")

#             self.logger.info("BrokerMW::event_loop - done")

#         except Exception as e:
#             raise e

#     def handle_discovery_reply(self):
#         try:
#             self.logger.info("BrokerMW::handle_discovery_reply")
#             # Receive and deserialize reply
#             reply_bytes = self.req.recv()
#             disc_resp = discovery_pb2.DiscoveryResp()
#             disc_resp.ParseFromString(reply_bytes)

#             # Handle different response types
#             if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
#                 timeout = self.upcall_obj.register_response(disc_resp.register_resp)
#             elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
#                 timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
#             else:
#                 raise ValueError("Unknown response type")

#             return timeout

#         except Exception as e:
#             raise e

#     def handle_publication(self):
#         try:
#             self.logger.info("BrokerMW::handle_publication")
#             # Receive the data from publisher
#             data = self.sub.recv_string()

#             # Parse topic and value (assuming "topic:value" format)
#             topic, value = data.split(":", 1)

#             # upcall to application to determine who to send data to
#             timeout = self.upcall_obj.handle_publication(topic, value)

#             return timeout

#         except Exception as e:
#             raise e

#     def disseminate (self, topic, data):
#       try:
#           self.logger.debug(f"BrokerMW::disseminate for topic: {topic} data: {data}")
#           # For now, simply format as "topic:data"
#           # In future, use protobuf for more complex data
#           send_str = f"{topic}:{data}"
#           self.logger.debug(f"BrokerMW::disseminate - {send_str}")

#           # Send as bytes with utf-8 encoding
#           self.pub.send(bytes(send_str, "utf-8"))
        
#           self.logger.debug("BrokerMW::disseminate complete")
#       except Exception as e:
#         raise e
          

#     def register (self, name, topiclist):
#       try:
#           self.logger.info ("BrokerMW::register")
#           # Create register request
#           register_req = discovery_pb2.RegisterReq()
#           register_req.role = discovery_pb2.ROLE_BOTH # Broker is both publisher and subscriber
    
#           # Add registrant info
#           reg_info = discovery_pb2.RegistrantInfo()
#           reg_info.id = name
#           register_req.info.CopyFrom(reg_info)
#           register_req.topiclist.extend(topiclist)

#           # Create discovery request
#           disc_req = discovery_pb2.DiscoveryReq()
#           disc_req.msg_type = discovery_pb2.TYPE_REGISTER
#           disc_req.register_req.CopyFrom(register_req)

#           # Serialize and send
#           buf2send = disc_req.SerializeToString()
#           self.req.send(buf2send)
#       except Exception as e:
#         raise e
    
#     def set_upcall_handle(self, upcall_obj):
#         self.upcall_obj = upcall_obj

#     def disable_event_loop(self):
#         self.handle_events = False

###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

import os
import sys
import time
import logging
import zmq
import configparser
from CS6381_MW import discovery_pb2

class BrokerMW:
    def __init__(self, logger):
        self.logger = logger
        self.sub = None  # ZMQ SUB socket to receive publications
        self.pub = None  # ZMQ PUB socket to send to subscribers
        self.req = None # ZMQ REQ socket to talk to discovery
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.broker_binding = None # Used when broker based dissemination is used.
        self.subscribers = {} # track the subscribers

    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")

            # Get ZMQ context
            context = zmq.Context()

            # create sockets, connect to discovery, etc here.
            self.req = context.socket(zmq.REQ)
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Create SUB socket for receiving publications
            self.sub = context.socket(zmq.SUB)
            bind_str = "tcp://*:5559" # you will need to read this from config.ini
            self.sub.bind (bind_str)

            # Create PUB socket for sending to subscribers
            self.pub = context.socket(zmq.PUB)
            bind_str = "tcp://*:5560" # you will need to read this from config.ini
            self.pub.bind (bind_str)


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
          self.logger.info ("BrokerMW::handle_discovery_reply")
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


    def handle_publication(self):
      try:
        self.logger.info("BrokerMW::handle_publication")
        # Receive the data from publisher
        data = self.sub.recv_string()
        
        # Parse topic and value (assuming "topic:value" format)
        topic, value = data.split(":", 1)
       
        # upcall to application to determine who to send data to
        timeout = self.upcall_obj.handle_publication (topic, value)

        return timeout

      except Exception as e:
        raise e

    def register (self, name, topiclist):
      try:
          self.logger.info ("BrokerMW::register")
          # Create register request
          register_req = discovery_pb2.RegisterReq()
          register_req.role = discovery_pb2.ROLE_BOTH # Broker is both publisher and subscriber
    
          # Add registrant info
          reg_info = discovery_pb2.RegistrantInfo()
          reg_info.id = name
          register_req.info.CopyFrom(reg_info)
          register_req.topiclist.extend(topiclist)

          # Create discovery request
          disc_req = discovery_pb2.DiscoveryReq()
          disc_req.msg_type = discovery_pb2.TYPE_REGISTER
          disc_req.register_req.CopyFrom(register_req)

          # Serialize and send
          buf2send = disc_req.SerializeToString()
          self.req.send(buf2send)
      except Exception as e:
        raise e

    def disseminate (self, topic, data):
       try:
            self.logger.debug ("BrokerMW::disseminate")
            # For now, simply format as "topic:data"
            # In future, use protobuf for more complex data
            send_str = f"{topic}:{data}"
            self.logger.debug(f"BrokerMW::disseminate - {send_str}")

            # Send as bytes with utf-8 encoding
            self.pub.send(bytes(send_str, "utf-8"))

       except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def cleanup (self):
      try:
        self.logger.info ("BrokerMW::cleanup")
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