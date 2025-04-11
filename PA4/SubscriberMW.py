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
    
    # Determine the timestamp. For example, convert send_time from epoch seconds to a datetime.
    # Adjust the conversion if send_time is in milliseconds.
    # timestamp = datetime.utcfromtimestamp(time.time() / 1000.0)
    # point.time(timestamp)
    
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
        self.max_latency = None

    def configure(self, args):
        try:
            self.logger.info("SubscriberMW::configure")
            self.name = args.name
            self.max_latency = args.max_latency

            # set up influxdb client
            token =  "5EapoOGtPyx4TEPCDEMPiPN5n5KroFNwHfHneEfZC5HKDSwrJA1zgrThvmPJXEGJ_LXqOUMq0e0hrsANTuBxRQ=="
            org = "CS6381"
            host = "https://us-east-1-1.aws.cloud2.influxdata.com"

            self.influx_client = InfluxDBClient3(host=host, token=token, org=org)
            self.database = "CS6381"
            self.write_queue = queue.Queue()

            # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            dissemination = config["Dissemination"]["Strategy"] 
            self.dissemination = dissemination

            self.write_interval = args.time

            # Setup the CSV file for data logging
            # filename = f"./data/{args.name}_data.csv"
            # self.csv_file = open(filename, 'w', newline='')
            # self.csv_writer = csv.writer(self.csv_file)
            # self.csv_writer.writerow(["publisher_id", "subscriber_id", "topic", "send_time", "recv_time", "broker_recv_time", "broker_send_time", "dissemination"])

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
            last_write_time = time.time()
            self.logger.info("SubscriberMW::event_loop - start")

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

                if time.time() - last_write_time >= self.write_interval:
                    self.flush_write_queue()
                    last_write_time = time.time()

            self.logger.info("SubscriberMW::event_loop - done")

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
            
            # Log latency for every publication
            self.logger.info(f"Publication received on topic {topic} with latency: {latency} ms")
            
            # Check if latency exceeds max_latency
            if self.max_latency and latency > self.max_latency:
                self.logger.warning(f"Publication was received {latency} ms late (max allowed: {self.max_latency} ms), disconnecting and joining another load balanced group")
                self.handle_events = False  # Signal to stop event loop
                return None  # Return None to signal reconnection needed
            
            record = Record(
                    publisher_id = pub_msg.publisher_id,
                    subscriber_id = self.name,
                    topic = topic,
                    send_time = send_time,
                    recv_time = recv_time,
                    dissemination = self.dissemination
            )
            self.write_queue.put(record)

            # Pass topic and data to the upcall
            timeout = self.upcall_obj.handle_publication(pub_msg.topic, pub_msg.data)

            return timeout

        except Exception as e:
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

    def flush_write_queue(self):
        """
        Non-blocking function that flushes all items currently in the queue
        to InfluxDB by spawning a background thread to perform the write.
        """
        self.logger.info("SubscriberMW::flush_write_queue - writing to InfluxDB")
        points = []

        # Drain all items currently in the queue.
        while True:
            try:
                datapoint = self.write_queue.get_nowait()
            except queue.Empty:
                break

            point = convert_record_to_point(datapoint)

            points.append(point)
            self.write_queue.task_done()

        # If there are points to write, spawn a background thread.
        if points:
            thread = threading.Thread(target=self._write_points, args=(points,), daemon=True)
            thread.start()

    def _write_points(self, points):
        """
        Worker function that writes a list of points to InfluxDB.
        This runs in its own thread.
        """
        try:
            self.influx_client.write(database=self.database, record=points)
        except Exception as e:
            # Handle exceptions as needed (e.g., log the error, retry, etc.)
            print("Error writing to InfluxDB:", e)

    
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
