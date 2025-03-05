###############################################
#
# Purpose: Subscriber application implementation
#
###############################################

import os
import sys
import time
import logging
import argparse
from enum import Enum
from SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2
from topic_selector import TopicSelector
import pdb

class SubscriberAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        REGISTER_WAIT = 3
        BROKER_WAIT = 4
        LOOKUP = 5
        LOOKUP_WAIT = 6
        LISTENING = 7
        COMPLETED = 8

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.mw_obj = None
        self.logger = logger
        self.lookup_attempts = 0
        self.max_lookup_attempts = 30
        self.received_publications = {}  # Track received publications per topic
        self.watch_started = False
        self.broker_available = False

    def configure(self, args):
        try:
            self.logger.info("SubscriberAppln::configure")

            self.state = self.State.CONFIGURE
            self.name = args.name

            # Initialize topic selector and get topics
            ts = TopicSelector()
            self.topiclist = ts.interest(args.num_topics)
            self.logger.info(f"SubscriberAppln::configure - Selected topics: {self.topiclist}")

            # Initialize the middleware
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.configure(args)

            # Initialize received publications tracking
            for topic in self.topiclist:
                self.received_publications[topic] = 0

            self.logger.info("SubscriberAppln::configure completed")

        except Exception as e:
            self.logger.error(f"Exception in configure: {str(e)}")
            raise e

    def driver(self):
        try:
            self.logger.info("SubscriberAppln::driver")
            self.state = self.State.REGISTER
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
                self.logger.debug("SubscriberAppln::invoke_operation - Registering")
                self.mw_obj.register(self.name, self.topiclist)
                self.state = self.State.REGISTER_WAIT
                return None

            if self.state == self.State.REGISTER_WAIT:
                self.logger.debug("SubscriberAppln::invoke_operation - waiting for register response")
                return None

            if self.state == self.State.BROKER_WAIT:
                self.logger.debug("SubscriberAppln::invoke_operation - waiting for broker")
                # The middleware’s watch is already active.
                return None

            elif self.state == self.State.LOOKUP:
                self.logger.debug("SubscriberAppln::invoke_operation - Looking up publishers")
                self.lookup_attempts += 1
                self.mw_obj.lookup_publishers(self.topiclist)
                self.state = self.State.LOOKUP_WAIT
                return None

            elif self.state == self.State.LOOKUP_WAIT:
                self.logger.debug("SubscriberAppln::invoke_operation - waiting for lookup response")
                return None
            

            elif self.state == self.State.LISTENING:
                # continue listening
                return None

            else:
                raise ValueError(f"Undefined state: {self.state}")

        except Exception as e:
            self.logger.error(f"Exception in invoke_operation: {str(e)}")
            raise e

    def register_response(self, register_resp):
        try:
            self.logger.info("SubscriberAppln::register_response")

            if register_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.debug("SubscriberAppln::register_response - Registration successful")
                self.state = self.State.BROKER_WAIT
                if not self.watch_started:
                    self.mw_obj.watch_for_broker()
                return 0
            else:
                raise Exception(f"Registration failed: {register_resp.reason}")

        except Exception as e:
            self.logger.error(f"Exception in register_response: {str(e)}")
            raise e
        
    def handle_broker_available(self):
        self.logger.debug("SubscriberAppln::handle_broker_available - switching to lookup mode")
        self.broker_available = True
        self.state = self.State.LOOKUP
        return 0

    def handle_broker_unavailable(self):
        self.broker_available = False
        self.logger.debug("SubscriberAppln::handle_broker_unavailable - broker died, back to waiting")
        # Stop listening to publishers.
        self.mw_obj.disconnect_from_publishers()
        self.state = self.State.BROKER_WAIT 
        return 0

    def lookup_response(self, lookup_resp):
        try:
            self.logger.info("SubscriberAppln::lookup_response")

            if not hasattr(lookup_resp, "publishers") or not lookup_resp.publishers:
                self.logger.debug("No publishers found in response")
                if self.lookup_attempts < self.max_lookup_attempts:
                    self.logger.info(f"Retry {self.lookup_attempts}/{self.max_lookup_attempts}")
                    time.sleep(1)
                    self.state = self.State.LOOKUP
                    return 0
                else:
                    self.logger.warning("Max lookup attempts reached - continuing without publishers")

            self.mw_obj.connect_to_publishers(list(lookup_resp.publishers), self.topiclist)
            self.state = self.State.LISTENING
            return 0

        except Exception as e:
            self.logger.error(f"Exception in lookup_response: {str(e)}")
            raise e

    def handle_publication(self, topic, value):
        try:
            self.logger.info(f"SubscriberAppln::handle_publication - {topic}: {value}")

            if topic in self.received_publications:
                self.received_publications[topic] += 1

            # Process or store the publication as needed.
            return 0

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
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")  # Add this
    parser.add_argument("-z", "--zk_addr", default="localhost:2181", help="ZooKeeper address")
    parser.add_argument("-T", "--num_topics", type=int, default=1, help="Number of topics to subscribe")
    parser.add_argument("-t", "--time", type=int, default=15, help="Amount of time to run for")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="Logging level, choices 10,20,30,40,50")
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
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    main()
