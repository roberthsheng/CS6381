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
        ISREADY = 3
        LOOKUP = 4
        LISTENING = 5
        COMPLETED = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.mw_obj = None
        self.logger = logger
        self.lookup_attempts = 0
        self.max_lookup_attempts = 30
        self.received_publications = {}  # Track received publications per topic

    def configure(self, args):
        try:
            self.logger.info("SubscriberAppln::configure")

            self.state = self.State.CONFIGURE
            self.name = args.name

            # Initialize topic selector and get topics
            ts = TopicSelector()
            self.topiclist = ts.interest(args.num_topics)
            self.logger.info(
                f"SubscriberAppln::configure - Selected topics: {self.topiclist}"
            )

            # Initialize the middleware
            self.mw_obj = SubscriberMW(self.logger)
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

            # Set up middleware upcall handle
            self.mw_obj.set_upcall_handle(self)

            # Start in REGISTER state
            self.state = self.State.REGISTER

            # Enter event loop
            self.logger.debug("SubscriberAppln::driver - Entering event loop")
            self.mw_obj.event_loop(timeout=0)

            self.logger.info("SubscriberAppln::driver completed")

        except Exception as e:
            self.logger.error(f"Exception in driver: {str(e)}")
            raise e

    def isready_response(self, isready_resp):
        try:
            self.logger.info("SubscriberAppln::isready_response")
            if isready_resp.status:  # Assuming status is a boolean or equivalent (True means ready)
                self.logger.info("System is ready; moving to lookup state.")
                self.state = self.State.LOOKUP
            else:
                self.logger.info("System not ready yet; will retry isready query.")
                time.sleep(1)  # Wait a bit before retrying
            return 0
        except Exception as e:
            self.logger.error(f"Exception in isready_response: {str(e)}")
            raise e
 

    def invoke_operation(self):
        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            if self.state == self.State.REGISTER:
                # Register with discovery service
                self.logger.debug("SubscriberAppln::invoke_operation - Registering")
                self.mw_obj.register(self.name, self.topiclist)
                return None

            elif self.state == self.State.ISREADY:
                self.logger.debug("SubscriberAppln::invoke_operation - Querying isready")
                self.mw_obj.is_ready()
                return None

            elif self.state == self.State.LOOKUP:
                # Lookup publishers
                self.logger.debug(
                    "SubscriberAppln::invoke_operation - Looking up publishers"
                )
                self.lookup_attempts += 1
                self.mw_obj.lookup_publishers(self.topiclist)
                return None

            elif self.state == self.State.LISTENING:
                # In listening state, just continue listening
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
                # Move to lookup state
                self.logger.debug(
                    "SubscriberAppln::register_response - Registration successful"
                )
                self.state = self.State.ISREADY
                return 0
            else:
                raise Exception(f"Registration failed: {register_resp.reason}")

        except Exception as e:
            self.logger.error(f"Exception in register_response: {str(e)}")
            raise e

    def lookup_response(self, lookup_resp):
        try:
            self.logger.info("SubscriberAppln::lookup_response")

            # Check if we received any publishers
            if not hasattr(lookup_resp, "publishers") or not lookup_resp.publishers:
                self.logger.debug("No publishers found in response")

                # Check if we should retry
                if self.lookup_attempts < self.max_lookup_attempts:
                    self.logger.info(
                        f"Retry {self.lookup_attempts}/{self.max_lookup_attempts}"
                    )
                    time.sleep(1)  # Wait before retry
                    return 0  # Will trigger another lookup
                else:
                    self.logger.warning(
                        "Max lookup attempts reached - continuing without publishers"
                    )

            # Connect to the publishers we received
            self.mw_obj.connect_to_publishers(
                list(lookup_resp.publishers), self.topiclist
            )

            # Move to listening state
            self.state = self.State.LISTENING
            return 0

        except Exception as e:
            self.logger.error(f"Exception in lookup_response: {str(e)}")
            raise e

    def handle_publication(self, topic, value):
        try:
            self.logger.info(f"SubscriberAppln::handle_publication - {topic}: {value}")

            # Increment received count for this topic
            if topic in self.received_publications:
                self.received_publications[topic] += 1

            # Here you might want to:
            # - Record timestamps for latency measurements
            # - Store data for analytics
            # - Process the value based on topic
            # - Check for experiment completion

            return 0  # Keep listening

        except Exception as e:
            self.logger.error(f"Exception in handle_publication: {str(e)}")
            raise e

    def cleanup(self):
        try:
            self.logger.info("SubscriberAppln::cleanup")

            # Print final statistics
            self.logger.info("Publication statistics:")
            for topic, count in self.received_publications.items():
                self.logger.info(f"  {topic}: {count} publications received")

            # Cleanup middleware
            if self.mw_obj:
                self.mw_obj.cleanup()

            self.state = self.State.COMPLETED

        except Exception as e:
            self.logger.error(f"Exception in cleanup: {str(e)}")
            raise e


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")

    parser.add_argument("-n", "--name", default="sub", help="Name of the subscriber")

    parser.add_argument(
        "-d", "--discovery", default="localhost:5555", help="Discovery service address"
    )

    parser.add_argument(
        "-T", "--num_topics", type=int, default=1, help="Number of topics to subscribe"
    )

    parser.add_argument(
        "-l",
        "--loglevel",
        type=int,
        default=logging.INFO,
        choices=[
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ],
        help="logging level, choices 10,20,30,40,50",
    )

    return parser.parse_args()


###################################
#
# Main program
#
###################################
def main():
    try:
        # Obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Subscriber Application - Main")
        logger = logging.getLogger("SubscriberAppln")

        # First parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # Reset the log level to as specified
        logger.debug(f"Main: resetting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)

        # Obtain subscriber application
        logger.debug("Main: obtain the subscriber appln object")
        sub_app = SubscriberAppln(logger)

        # Configure the application
        logger.debug("Main: configure the subscriber appln object")
        sub_app.configure(args)

        # Now invoke the driver program
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
    # Set underlying default logging capabilities
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
