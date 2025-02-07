import os
import sys
import time
import logging
import argparse
import configparser
from enum import Enum
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2


class BrokerAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        ISREADY = 3
        LOOKUP = 4
        RUNNING = 5
        COMPLETED = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.mw_obj = None
        self.logger = logger
        self.max_lookup_attempts = 10
        self.lookup_attempts = 0
        self.subscribers = {}  # {topic: [subscriber_info]}

    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")
            self.state = self.State.CONFIGURE

            self.name = args.name
             # Now, get the configuration object
            self.logger.debug ("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)
            self.dissemination = config["Dissemination"]["Strategy"]
            if self.dissemination == "Direct":
                self.logger.info("BrokerAppln::configure - Dissemination strategy set to Direct. Broker not required. Exiting.")
                sys.exit(0)  # Exit gracefully if broker is not needed.                


            # Initialize the middleware
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args)
            
            self.logger.info("BrokerAppln::configure completed")
        
        except Exception as e:
            self.logger.error(f"Exception in configure: {str(e)}")
            raise e
            
    def isready_response(self, isready_resp):
        try:
            self.logger.info("BrokerAppln::isready_response")
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
                # Register with discovery service
                self.logger.debug("BrokerAppln::invoke_operation - Registering")
                self.mw_obj.register(self.name)
                return None
            elif self.state == self.State.ISREADY:
                self.logger.debug("BrokerAppln::invoke_operation - Querying isready")
                self.mw_obj.is_ready()
                return None
            elif self.state == self.State.LOOKUP:
                self.logger.debug(
                    "BrokerAppln::invoke_operation - Looking up publishers"
                )
                self.lookup_attempts += 1
                self.mw_obj.lookup_all_publishers()
                return None
            elif self.state == self.State.RUNNING:
                # The broker should continue running and routing data.
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
                self.state = self.State.ISREADY
                # now begin waiting until all parties are ready
                return 0
            else:
                raise Exception(f"Registration failed: {register_resp.reason}")
        except Exception as e:
            self.logger.error(f"Exception in register_response: {str(e)}")
            raise e

    def lookup_response(self, lookup_resp):
        try:
            self.logger.info("BrokerAppln::lookup_response")

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
                list(lookup_resp.publishers) 
            )

            # Move to listening state
            self.state = self.State.RUNNING
            return 0

        except Exception as e:
            self.logger.error(f"Exception in lookup_response: {str(e)}")
            raise e


    def handle_publication(self, topic, value, old_timestamp, publisher_id):
        try:
           self.logger.debug(f"BrokerAppln::handle_publication: Topic: {topic}, Value: {value}")
           self.mw_obj.disseminate(topic, value, old_timestamp, publisher_id)
           return 0 # Continue listening

        except Exception as e:
            self.logger.error(f"Exception in handle_publication: {str(e)}")
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
    
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG,logging.INFO,logging.WARNING,
                                logging.ERROR,logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50")

    parser.add_argument("-a", "--addr", default="0.0.0.0",
                        help="Broker's bind address (default: 0.0.0.0)")
    parser.add_argument("-p", "--port", type=int, default=5560,
                        help="Broker's publish port (default: 5560)") 
    return parser.parse_args()

###################################
#
# Main program
#
###################################
def main():
    try:
        # Obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Broker Application - Main")
        logger = logging.getLogger("BrokerAppln")

        # First parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # Reset the log level to as specified
        logger.debug(f"Main: resetting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)
        
        # Obtain broker application
        logger.debug("Main: obtain the broker appln object")
        broker_app = BrokerAppln(logger)

        # Configure the application
        logger.debug("Main: configure the broker appln object")
        broker_app.configure(args)

        # Now invoke the driver program
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
    # Set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()