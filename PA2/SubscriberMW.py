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
from datetime import datetime
import configparser
from dataclasses import dataclass, asdict
from influxdb_client_3 import InfluxDBClient3, Point
from kazoo.client import KazooClient
import threading
import queue
from CS6381_MW import discovery_pb2, topic_pb2

@dataclass
class Record:
    publisher_id: str
    subscriber_id: str
    topic: str
    send_time: int
    recv_time: int
    dissemination: str 

def convert_record_to_point(record: Record) -> Point:
    # Choose a measurement name for the data. Here we use "record".
    point = Point("record")
    
    # Map fields to tags. Tags are indexed, so use them for identifiers.
    point.tag("publisher_id", record.publisher_id)
    point.tag("subscriber_id", record.subscriber_id)
    point.tag("topic", record.topic)
    
    # Map other data as fields.
    point.field("recv_time", record.recv_time)
    point.field("publish_time", record.send_time)
    point.field("dissemination", record.dissemination)
    
    return point

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
        self.publishers = None
        self.zk = None

    def configure(self, args):
        try:
            self.logger.info("SubscriberMW::configure")
            self.name = args.name
            self.publishers = set()

            # set up influxdb client
            self.logger.info("SubscriberMW - Connecting to influxdb")
            token =  "5EapoOGtPyx4TEPCDEMPiPN5n5KroFNwHfHneEfZC5HKDSwrJA1zgrThvmPJXEGJ_LXqOUMq0e0hrsANTuBxRQ=="
            org = "CS6381"
            host = "https://us-east-1-1.aws.cloud2.influxdata.com"

            self.influx_client = InfluxDBClient3(host=host, token=token, org=org)
            self.database = "CS6381"
            self.write_queue = queue.Queue()

            # connect to zk
            self.zk = KazooClient(hosts=args.zk_addr)
            self.zk.start()

            # Set up the queue for ZooKeeper events
            self.zk_event_queue = queue.Queue()

            # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            dissemination = config["Dissemination"]["Strategy"] 
            self.dissemination = dissemination

            self.write_interval = args.time

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

    def watch_for_broker(self):
        """
        Sets a ZooKeeper data watch on the /broker/primary znode.
        If the node exists, we signal that the broker is available;
        if it is missing or later deleted, we signal that the broker is unavailable.
        """
        self.logger.info("SubscriberMW::watch_for_broker - setting up watch on /broker/primary")
        broker_node = "/broker/primary"

        def broker_watch(data, stat, event):
            # The initial callback is invoked with event==None.
            if event is None:
                if stat is None:
                    self.logger.info("Broker znode /broker/primary does not exist initially.")
                    self.zk_event_queue.put(("broker_not_available", None))
                else:
                    self.logger.info("Broker znode /broker/primary exists initially.")
                    self.zk_event_queue.put(("broker_available", data))
            else:
                self.logger.info("Broker watch event: %s", event)
                if event.type == "DELETED":
                    self.logger.info("Broker znode /broker/primary has been deleted.")
                    self.zk_event_queue.put(("broker_not_available", None))
                elif event.type in ["CREATED", "CHANGED"]:
                    self.logger.info("Broker znode /broker/primary has been created/changed.")
                    self.zk_event_queue.put(("broker_available", data))
            return True  # keep the watch active

        self.zk.DataWatch(broker_node, broker_watch)

    def disconnect_from_publishers(self):
        """
        When the broker is not available, we want to stop receiving publications.
        Here we unsubscribe from all topics and clear the publisher list.
        """
        try:
            self.logger.info("SubscriberMW::disconnect_from_publishers - disconnecting from publishers")
            if self.upcall_obj and hasattr(self.upcall_obj, "topiclist"):
                for topic in self.upcall_obj.topiclist:
                    self.sub.setsockopt_string(zmq.UNSUBSCRIBE, topic)
            # Clear publisher list so that on next lookup, we connect to fresh publishers.
            self.publishers.clear()
        except Exception as e:
            self.logger.error(f"Exception in disconnect_from_publishers: {str(e)}")

    def event_loop(self, timeout=None):
        try:
            last_write_time = time.time()
            self.logger.info("SubscriberMW::event_loop - start")
            # Set a default timeout if None is provided
            default_timeout = 500  # in milliseconds

            while self.handle_events:
                # Use a default timeout if none is provided
                effective_timeout = timeout if timeout is not None else default_timeout
                events = dict(self.poller.poll(timeout=effective_timeout))

                # Process any ZooKeeper events from the watch.
                while not self.zk_event_queue.empty():
                    event_type, data = self.zk_event_queue.get_nowait()
                    self.logger.info("Dequeued zk event: %s", event_type)
                    if event_type == "broker_available":
                        self.logger.info("Broker is now available. Initiating lookup...")
                        self.upcall_obj.handle_broker_available()  # Trigger lookup.
                    elif event_type == "broker_not_available":
                        self.logger.info("Broker is no longer available. Transitioning to wait state.")
                        self.upcall_obj.handle_broker_unavailable()

                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_discovery_reply()
                elif self.sub in events:
                    timeout = self.handle_publication()
                else:
                    raise Exception("Unknown event")

                if time.time() - last_write_time >= self.write_interval:
                    self.flush_write_queue()
                    last_write_time = time.time()

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
            self.write_queue.put(record)
            self.logger.debug(f"Received publication on topic {pub_msg.topic}, latency: {latency} ms")

            # Pass topic and data to the upcall
            timeout = self.upcall_obj.handle_publication(pub_msg.topic, pub_msg.data)

            return timeout

        except Exception as e:
            raise e

    def connect_to_publishers(self, publishers, topics):
        try:
            self.logger.info("SubscriberMW::connect_to_publishers")
            # Connect to each publisher if not already connected.
            for pub in publishers:
                if pub.id in self.publishers:
                    continue 
                self.publishers.add(pub.id)
                connect_str = f"tcp://{pub.addr}:{pub.port}"
                self.sub.connect(connect_str)
                self.logger.debug(f"Connected to publisher {pub.id} at {connect_str}")

            # Subscribe to all topics.
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

    def flush_write_queue(self):
        """
        Non-blocking function that flushes all items currently in the queue
        to InfluxDB by spawning a background thread to perform the write.
        """
        self.logger.info("SubscriberMW::flush_write_queue - writing to InfluxDB")
        points = []

        while True:
            try:
                datapoint = self.write_queue.get_nowait()
            except queue.Empty:
                break

            point = convert_record_to_point(datapoint)
            points.append(point)
            self.write_queue.task_done()

        if points:
            thread = threading.Thread(target=self._write_points, args=(points,), daemon=True)
            thread.start()

    def _write_points(self, points):
        try:
            self.influx_client.write(database=self.database, record=points)
        except Exception as e:
            print("Error writing to InfluxDB:", e)

    def cleanup(self):
        try:
            self.logger.info("SubscriberMW::cleanup")
            
            if self.records:
                rows = [
                   [rec.publisher_id, rec.subscriber_id, rec.topic, rec.send_time, rec.recv_time, rec.dissemination]
                   for rec in self.records
                ]
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
            if self.zk:
                self.zk.stop()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise e
