import os
import sys
import time
import logging
import argparse
import configparser
from enum import Enum
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2
from topic_selector import TopicSelector

class BrokerAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        LOOKUP = 3
        RUNNING = 4
        COMPLETED = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.mw_obj = None
        self.logger = logger
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

            # Get topics of interest
            ts = TopicSelector()
            self.topiclist = ts.interest()
            self.logger.info(f"BrokerAppln::configure - Topics {self.topiclist}")

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
                # Register with discovery service
                self.logger.debug("BrokerAppln::invoke_operation - Registering")
                self.mw_obj.register(self.name, self.topiclist)
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
                self.state = self.State.RUNNING
                # Here we don't call lookup. The broker doesn't need any publisher info,
                # and we rely on the subscribers to connect to this broker
                # Therefore, after registration, we can immediately proceed to routing data.
                return 0
            else:
                raise Exception(f"Registration failed: {register_resp.reason}")
        except Exception as e:
            self.logger.error(f"Exception in register_response: {str(e)}")
            raise e


    def handle_publication(self, topic, value):
        try:
           self.logger.debug(f"BrokerAppln::handle_publication: Topic: {topic}, Value: {value}")
           # Since we are not doing protobof, simply forward the message.
           self.mw_obj.disseminate(topic, value)
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