###############################################
#
# Updated SubscriberMW (middleware) implementation
#
###############################################
import pdb
import os
import sys
import time
import logging
import csv
import zmq
from datetime import datetime
import configparser
from dataclasses import dataclass, asdict
import json
import threading
import queue
from CS6381_MW import discovery_pb2, topic_pb2
from influxdb_client_3 import InfluxDBClient3, Point

@dataclass
class Record:
    publisher_id: str
    subscriber_id: str
    topic: str
    send_time: int
    recv_time: int
    dissemination: str 

def convert_record_to_point(record: Record) -> Point:
    point = Point("record")
    point.tag("publisher_id", record.publisher_id)
    point.tag("subscriber_id", record.subscriber_id)
    point.tag("topic", record.topic)
    point.field("recv_time", record.recv_time)
    point.field("publish_time", record.send_time)
    point.field("dissemination", record.dissemination)
    return point

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None   # REQ socket for load balancer (instead of discovery)
        self.sub = None   # SUB socket for receiving publications from the broker
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.broker_binding = None
        self.name = None
        self.records = []
        self.write_queue = queue.Queue()
        self.dissemination = None

    def configure(self, args):
        try:
            self.logger.info("SubscriberMW::configure")
            self.name = args.name

            # Set up InfluxDB client (credentials unchanged)
            token =  "5EapoOGtPyx4TEPCDEMPiPN5n5KroFNwHfHneEfZC5HKDSwrJA1zgrThvmPJXEGJ_LXqOUMq0e0hrsANTuBxRQ=="
            org = "CS6381"
            host = "https://us-east-1-1.aws.cloud2.influxdata.com"
            self.influx_client = InfluxDBClient3(host=host, token=token, org=org)
            self.database = "CS6381"
            self.write_interval = args.time

            # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            self.dissemination = config["Dissemination"]["Strategy"] 

            # Get ZMQ context and create sockets
            context = zmq.Context()
            # REQ socket to contact the load balancer
            self.req = context.socket(zmq.REQ)
            # Connect to the load balancer address (supplied as --discovery argument)
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)
            self.logger.info(f"SubscriberMW::configure - Connected REQ socket to load balancer at {connect_str}")

            # Create SUB socket; we will connect it to the broker once we get broker info.
            self.sub = context.socket(zmq.SUB)
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
                    # No events: invoke the application upcall for the next operation
                    timeout = self.upcall_obj.invoke_operation()
                elif self.sub in events:
                    # Handle incoming publication from broker
                    timeout = self.handle_publication()
                else:
                    raise Exception("Unknown event")
                # Periodically flush the write queue to InfluxDB
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
            pub_msg = topic_pb2.Publication()
            pub_msg.ParseFromString(msg_bytes)
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
            timeout = self.upcall_obj.handle_publication(pub_msg.topic, pub_msg.data)
            return timeout
        except Exception as e:
            raise e

    def flush_write_queue(self):
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

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False

    def cleanup(self):
        try:
            self.logger.info("SubscriberMW::cleanup")
            if self.sub:
                self.sub.close()
            if self.req:
                self.req.close()
            if self.poller:
                self.poller.unregister(self.sub)
                self.poller.unregister(self.req)
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise e

###############################################
#
# Updated SubscriberAppln (application) implementation
#
###############################################
import os
import sys
import time
import logging
import argparse
from enum import Enum
from SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2, topic_pb2
from topic_selector import TopicSelector
import json

class SubscriberAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2      # New state: register with load balancer to get broker info
        LISTENING = 3
        COMPLETED = 4

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.mw_obj = None
        self.logger = logger
        self.received_publications = {}  # Track received publications per topic

    def configure(self, args):
        try:
            self.logger.info("SubscriberAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name
            ts = TopicSelector()
            self.topiclist = ts.interest(args.num_topics)
            self.logger.info(f"SubscriberAppln::configure - Selected topics: {self.topiclist}")
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args)
            # Initialize publication count tracking
            for topic in self.topiclist:
                self.received_publications[topic] = 0
            # After configuration, move to REGISTER state to get broker info from load balancer.
            self.state = self.State.REGISTER
            self.logger.info("SubscriberAppln::configure completed")
        except Exception as e:
            self.logger.error(f"Exception in configure: {str(e)}")
            raise e

    def driver(self):
        try:
            self.logger.info("SubscriberAppln::driver")
            # Set up the upcall handle so that middleware can call back to us.
            self.mw_obj.set_upcall_handle(self)
            # Enter event loop. The flow will be: REGISTER (contact load balancer) -> LISTENING.
            self.logger.debug("SubscriberAppln::driver - Entering event loop")
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("SubscriberAppln::driver completed")
        except Exception as e:
            self.logger.error(f"Exception in driver: {str(e)}")
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("SubscriberAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.debug("SubscriberAppln::invoke_operation - Registering with load balancer")
                # Send request to load balancer to get the broker connection info.
                self.mw_obj.req.send_string("connect")
                response = self.mw_obj.req.recv_string()
                self.logger.info(f"Received broker info from load balancer: {response}")
                if response == 'No brokers available':
                    self.logger.info(f"No brokers available. Waiting 1 second")
                    time.sleep(2)
                    return 0
                broker_info = json.loads(response)
                # Connect the SUB socket to the broker returned by the load balancer.
                broker_connect_str = f"tcp://{broker_info['addr']}:{broker_info['port']}"
                self.mw_obj.sub.connect(broker_connect_str)
                self.logger.info(f"Subscriber connected to broker at {broker_connect_str}")
                # Subscribe to each topic.
                for topic in self.topiclist:
                    self.mw_obj.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
                    self.logger.debug(f"Subscribed to topic: {topic}")
                self.state = self.State.LISTENING
                return 0
            elif self.state == self.State.LISTENING:
                # In listening state, simply continue waiting for publications.
                return None
            else:
                raise ValueError(f"Undefined state: {self.state}")
        except Exception as e:
            self.logger.error(f"Exception in invoke_operation: {str(e)}")
            raise e

    def handle_publication(self, topic, value):
        try:
            self.logger.info(f"SubscriberAppln::handle_publication - {topic}: {value}")
            if topic in self.received_publications:
                self.received_publications[topic] += 1
            return 0  # Continue listening
        except Exception as e:
            self.logger.error(f"Exception in handle_publication: {str(e)}")
            raise e

    def cleanup(self):
        try:
            self.logger.info("SubscriberAppln::cleanup")
            self.logger.info("Publication statistics:")
            for topic, count in self.received_publications.items():
                self.logger.info(f"  {topic}: {count} publications received")
            if self.mw_obj:
                self.mw_obj.cleanup()
            self.state = self.State.COMPLETED
        except Exception as e:
            self.logger.error(f"Exception in cleanup: {str(e)}")
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument("-n", "--name", default="sub", help="Name of the subscriber")
    # Use the --discovery argument to specify the load balancer’s address (e.g., localhost:6000)
    parser.add_argument("-d", "--discovery", default="localhost:6000", help="Load balancer address")
    parser.add_argument("-T", "--num_topics", type=int, default=1, help="Number of topics to subscribe")
    parser.add_argument("-t", "--time", type=int, default=15, help="Amount of time to run for (seconds)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="Logging level")
    return parser.parse_args()

###################################
#
# Main program
#
###################################
def main():
    try:
        logging.info("Subscriber Application - Main")
        logger = logging.getLogger("SubscriberAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug(f"Main: resetting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)
        logger.debug("Main: obtain the subscriber appln object")
        sub_app = SubscriberAppln(logger)
        logger.debug("Main: configure the subscriber appln object")
        sub_app.configure(args)
        logger.debug("Main: invoke the subscriber appln driver")
        sub_app.driver() 
    except KeyboardInterrupt:
        logger.info("Subscriber Application - Interrupted by user")
        try:
            if "sub_app" in locals():
                sub_app.cleanup()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    except Exception as e:
        logger.error(f"Exception in main: {str(e)}")
        return

###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
