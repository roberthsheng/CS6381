###############################################
#
# Purpose: Broker application implementation
#
###############################################

import os
import sys
import time
import logging
import argparse
import configparser
from enum import Enum
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2
# TODO: implement leader election and resigning here
class BrokerAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE  = 1
        REGISTER   = 2
        REGISTER_WAIT = 3
        LOOKUP     = 4
        RUNNING    = 5
        COMPLETED  = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.mw_obj = None
        self.logger = logger
        self.max_lookup_attempts = 1
        self.lookup_attempts = 0

    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")
            self.state = self.State.CONFIGURE

            self.name = args.name
            # Parse configuration file
            self.logger.debug("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]
            if self.dissemination == "Direct":
                self.logger.info("BrokerAppln::configure - Dissemination strategy set to Direct. Broker not required. Exiting.")
                sys.exit(0)

            # Initialize the middleware
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args)
            
            self.logger.info("BrokerAppln::configure completed")
        
        except Exception as e:
            self.logger.error(f"Exception in configure: {str(e)}")
            raise e

    def driver(self):
        try:
            self.logger.info("BrokerAppln::driver")
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("BrokerAppln::driver completed")
        except Exception as e:
            self.logger.error(f"Exception in driver: {str(e)}")
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("BrokerAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.debug("BrokerAppln::invoke_operation - Registering")
                self.mw_obj.register(self.name)
                self.state = self.State.REGISTER_WAIT
                return None
            elif self.state == self.State.REGISTER_WAIT:
                self.logger.debug("BrokerAppln::invoke_operation - Waiting for register response")
                return None
            elif self.state == self.State.LOOKUP:
                self.logger.debug("BrokerAppln::invoke_operation - Looking up publishers")
                self.lookup_attempts += 1
                self.mw_obj.lookup_all_publishers()
                return None
            elif self.state == self.State.RUNNING:
                # Broker is running normally; nothing to do.
                return None
            else:
                raise ValueError(f"Undefined state: {self.state}")
        except Exception as e:
            self.logger.error(f"Exception in invoke_operation: {str(e)}")
            raise e

    def register_response(self, register_resp):
        try:
            self.logger.info("BrokerAppln::register_response")
            if register_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.info("BrokerAppln::register_response - Registration successful")
                # Create the /broker/primary znode so that subscribers/publishers know the broker is up.
                self.mw_obj.create_broker_znode()  # Implemented in BrokerMW
                self.mw_obj.start_publisher_watch()
                # The middleware already starts its publisher watch.
                # Immediately transition to LOOKUP so that one lookup is performed.
                self.state = self.State.LOOKUP
                return 0
            else:
                raise Exception(f"Registration failed: {register_resp.reason}")
        except Exception as e:
            self.logger.error(f"Exception in register_response: {str(e)}")
            # may want to handle gracefully and return to registser state to try again
            raise e

    def lookup_response(self, lookup_resp):
        try:
            self.logger.info("BrokerAppln::lookup_response")
            if not hasattr(lookup_resp, "publishers") or not lookup_resp.publishers:
                self.logger.debug("No publishers found in response")
                if self.lookup_attempts < self.max_lookup_attempts:
                    self.logger.info(f"Retry {self.lookup_attempts}/{self.max_lookup_attempts}")
                    time.sleep(1)
                    return 0
                else:
                    self.logger.warning("Max lookup attempts reached - continuing without publishers")
            # Connect to the publishers received.
            self.mw_obj.connect_to_publishers(list(lookup_resp.publishers))
            # Transition to RUNNING state.
            self.state = self.State.RUNNING
            return 0
        except Exception as e:
            self.logger.error(f"Exception in lookup_response: {str(e)}")
            raise e

    def handle_publication(self, topic, value, old_timestamp, publisher_id):
        try:
            self.logger.debug(f"BrokerAppln::handle_publication: Topic: {topic}, Value: {value}")
            self.mw_obj.disseminate(topic, value, old_timestamp, publisher_id)
            return 0
        except Exception as e:
            self.logger.error(f"Exception in handle_publication: {str(e)}")
            raise e

    def handle_publishers_update(self):
        """
        Upcall from the middleware when the watch on /publisher detects a change.
        Switch the broker's state to LOOKUP so that a new lookup is performed.
        """
        try:
            self.logger.info("BrokerAppln::handle_publishers_update - publisher change detected, switching to LOOKUP")
            if self.state != self.State.LOOKUP:
                self.state = self.State.LOOKUP
            return 0
        except Exception as e:
            self.logger.error(f"Exception in handle_publishers_update: {str(e)}")
            raise e

    def cleanup(self):
        try:
            self.logger.info("BrokerAppln::cleanup")
            if self.mw_obj:
                self.mw_obj.cleanup()
            self.state = self.State.COMPLETED
        except Exception as e:
            self.logger.error(f"Exception in cleanup: {str(e)}")
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")
    
    parser.add_argument("-n", "--name", default="broker",
                        help="Name of the broker")
    
    parser.add_argument("-d", "--discovery", default="localhost:5555",
                        help="Discovery service address")
    
    parser.add_argument("-c", "--config", default="config.ini",
                        help="Configuration file (default: config.ini)")
    
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING,
                                 logging.ERROR, logging.CRITICAL],
                        help="Logging level, choices 10,20,30,40,50")
    
    parser.add_argument("-a", "--addr", default="0.0.0.0",
                        help="Broker's bind address (default: 0.0.0.0)")
    parser.add_argument("-p", "--port", type=int, default=5560,
                        help="Broker's publish port (default: 5560)")
    parser.add_argument("-z", "--zk_addr", default="localhost:2181",
                        help="ZooKeeper address for broker watch")
    return parser.parse_args()

###################################
#
# Main program
#
###################################
def main():
    try:
        logging.info("Broker Application - Main")
        logger = logging.getLogger("BrokerAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug(f"Main: resetting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)
        logger.debug("Main: obtain the broker appln object")
        broker_app = BrokerAppln(logger)
        logger.debug("Main: configure the broker appln object")
        broker_app.configure(args)
        logger.debug("Main: invoke the broker appln driver")
        broker_app.driver()
    except KeyboardInterrupt:
        logger.info("Broker Application - Interrupted by user")
        try:
            if 'broker_app' in locals():
                broker_app.cleanup()
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
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
