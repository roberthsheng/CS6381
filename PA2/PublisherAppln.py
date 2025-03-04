###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Publisher Application with broker wait state
#
###############################################
import pdb
import os
import sys
import time
import argparse
import configparser
import logging
from topic_selector import TopicSelector
from CS6381_MW.PublisherMW import PublisherMW
from CS6381_MW import discovery_pb2
from enum import Enum

class PublisherAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        REGISTER_WAIT = 3
        BROKER_WAIT = 4
        DISSEMINATE = 5
        COMPLETED = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.iters = None
        self.frequency = None
        self.num_topics = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.logger = logger
        self.watch_started = False

    def configure(self, args):
        try:
            self.logger.info("PublisherAppln::configure")
            self.logger.info(f"I am here!!")
            self.state = self.State.CONFIGURE
            self.name = args.name
            self.iters = args.iters
            self.frequency = args.frequency
            self.num_topics = args.num_topics

            self.logger.info("PublisherAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            self.logger.info("PublisherAppln::configure - selecting our topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)

            self.logger.info("PublisherAppln::configure - initializing the middleware object")
            self.mw_obj = PublisherMW(self.logger)
            self.mw_obj.set_upcall_handle(self)
            self.logger.info(f"PublisherAppln::configure - using ZK: {args.zk_addr}, Discovery: {args.discovery}")
            self.mw_obj.configure(args)
            self.logger.info("PublisherAppln::configure - configuration complete")
        except Exception as e:
            self.logger.error(f"PublisherAppln::configure - exception: {str(e)}")
            raise e

    def driver(self):
        try:
            self.logger.info("PublisherAppln::driver")
            self.dump()
            self.state = self.State.REGISTER
            # Start the middleware event loop.
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("PublisherAppln::driver completed")
        except Exception as e: #TODO: don't need to re-register but should connect to the new correct discovery service address when it goes down
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("PublisherAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.info("PublisherAppln::invoke_operation - registering with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                # After registration, transition to waiting for broker
                self.state = self.State.REGISTER_WAIT
                return None

            elif self.state == self.State.REGISTER_WAIT:
                self.logger.debug("PublisherAppln::invoke_operation - Waiting for registration")
                return None

            elif self.state == self.State.BROKER_WAIT:
                self.logger.info("PublisherAppln::invoke_operation - waiting for broker")
                # Simply return to let the event loop poll the sockets and ZooKeeper queue
                return None

            elif self.state == self.State.DISSEMINATE:
                self.logger.info("PublisherAppln::invoke_operation - starting dissemination")
                ts = TopicSelector()
                # Instead of a blocking loop, send one round of messages and then return control.
                for topic in self.topiclist:
                    dissemination_data = ts.gen_publication(topic)
                    self.mw_obj.disseminate(self.name, topic, dissemination_data)
                # Sleep a bit and then check if we should continue
                time.sleep(1 / float(self.frequency))
                # Instead of looping here, return a timeout so that the event loop is invoked again.
                return 0  # or appropriate timeout value


            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state of the application object")
        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        try:
            self.logger.info("PublisherAppln::register_response")
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.debug("PublisherAppln::register_response - registration successful")
                # After registration, wait for the broker
                self.state = self.State.BROKER_WAIT
                if not self.watch_started:
                    self.mw_obj.watch_for_broker()
                self.mw_obj.create_publisher_znode()
                return 0
            else:
                self.logger.debug("PublisherAppln::register_response - registration failed")
                raise ValueError("Publisher needs to have a unique id")
        except Exception as e:
            raise e

    # Upcalls from the middleware regarding broker status.
    def handle_broker_available(self):
        try:
            self.logger.info("PublisherAppln::handle_broker_available - broker is available")
            # Transition to dissemination when the broker becomes available.
            if self.state != self.State.DISSEMINATE:
                self.state = self.State.DISSEMINATE
        except Exception as e:
            self.logger.error(f"Exception in broker update: {str(e)}")
            raise e

    def handle_broker_unavailable(self):
        try:
            self.logger.info("PublisherAppln::handle_broker_unavailable - broker is unavailable, transitioning to BROKER_WAIT")
            if self.state != self.State.BROKER_WAIT:
                self.state = self.State.BROKER_WAIT
        except Exception as e:
            self.logger.error(f"Exception in broker update: {str(e)}")
            raise e            

    def dump(self):
        try:
            self.logger.info("**********************************")
            self.logger.info("PublisherAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info("     Dissemination: {}".format(self.dissemination))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("     Iterations: {}".format(self.iters))
            self.logger.info("     Frequency: {}".format(self.frequency))
            self.logger.info("**********************************")
        except Exception as e:
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Publisher Application")
    parser.add_argument("-n", "--name", default="pub", help="Unique name for the publisher")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Port number for the publisher's ZMQ service")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service address")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Frequency of dissemination (messages per second)")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="Number of dissemination iterations")
    parser.add_argument("-z", "--zk_addr", default="localhost:2181", help="ZooKeeper address for broker watch")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="Logging level")
    return parser.parse_args()

def main():
    try:
        logging.info("Main - starting Publisher Application")
        logger = logging.getLogger("PublisherAppln")
        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)
        pub_app = PublisherAppln(logger)
        pub_app.configure(args)
        pub_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
