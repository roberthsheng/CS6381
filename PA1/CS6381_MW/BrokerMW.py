import zmq
import logging
from CS6381_MW import discovery_pb2
import configparser

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
        # Add any other variables

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

            # Create PUB socket for sending to subscribers
            self.pub = context.socket(zmq.PUB)

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

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False