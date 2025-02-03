###############################################
#
# Purpose: Subscriber middleware implementation
#
###############################################

import os
import sys
import time
import logging
import csv
import zmq
import configparser
from dataclasses import dataclass
from CS6381_MW import discovery_pb2, topic_pb2

@dataclass
class Record:
    publisher_id: str
    subscriber_id: str
    topic: str
    send_time: int
    recv_time: int
    dissemination: str 

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # ZMQ REQ socket for discovery service
        self.sub = None  # ZMQ SUB socket for receiving publications
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.broker_binding = None  # Used in broker-based dissemination
        self.name = None
        self.records = []
        self.csv_file = None
        self.csv_writer = None
        self.dissemination = None

    def configure(self, args):
        try:
            self.logger.info("SubscriberMW::configure")
            self.name = args.name

            # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            dissemination = config["Dissemination"]["Strategy"] 
            self.dissemination = dissemination

            # Setup the CSV file for data logging
            filename = f"{args.name}_data.csv"
            self.csv_file = open(filename, 'w', newline='')
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(["publisher_id", "subscriber_id", "topic", "send_time", "recv_time", "broker_recv_time", "broker_send_time", "dissemination"])

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
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            else:
                raise ValueError("Unknown response type")

            return timeout

        except Exception as e:
            raise e


    def handle_publication(self):
        try:
            self.logger.info("SubscriberMW::handle_publication")

            # Receive the serialized message
            topic, msg_bytes = self.sub.recv_multipart()

            # Deserialize using protobuf
            pub_msg = topic_pb2.Publication()
            pub_msg.ParseFromString(msg_bytes)

            # Log the message with timestamps (for debugging)
            send_time = pub_msg.timestamp
            recv_time = int(time.time() * 1000)
            latency = recv_time - send_time
            record = Record(
                    publisher_id = pub_msg.publisher_id,
                    subscriber_id = self.name,
                    topic = topic,
                    send_time = send_time,
                    recv_time = recv_time,
                    dissemination = self.dissemination
            )
            self.records.append(record)
            self.logger.debug(f"Received publication on topic {pub_msg.topic}, latency: {latency} ms")

            # Pass topic and data to the upcall
            timeout = self.upcall_obj.handle_publication(pub_msg.topic, pub_msg.data)

            return timeout

        except Exception as e:
            raise e

    def is_ready(self):
        try:
            self.logger.info("SubscriberMW::is_ready")
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
            self.logger.error(f"SubscriberMW::is_ready - Exception: {str(e)}")
            raise e


    def connect_to_publishers(self, publishers, topics):
        try:
            self.logger.info("SubscriberMW::connect_to_publishers")
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



        except Exception as e:
            self.logger.error(f"Error in connect_to_publishers: {str(e)}")
            raise e

    def set_broker_binding(self, binding):
        self.broker_binding = binding

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def cleanup(self):
        try:
            self.logger.info("SubscriberMW::cleanup")
            
            # Write the data log to a csv file.
            if self.records:
                # Convert Record objects to list for CSV writing
                rows = [
                   [
                    rec.publisher_id,
                    rec.subscriber_id,
                    rec.topic,
                    rec.send_time,
                    rec.recv_time,
                    rec.dissemination
                ] for rec in self.records]
                self.csv_writer.writerows(rows)

                self.logger.info(f"Wrote collected data to {self.csv_file.name}")

            if self.sub:
                self.sub.close()
            if self.req:
                self.req.close()
            if self.poller:
                self.poller.unregister(self.sub)
                self.poller.unregister(self.req)
            if self.csv_file:
                self.csv_file.close()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise e
