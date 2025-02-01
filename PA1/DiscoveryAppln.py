###############################################
#
# Purpose: Discovery application implementation
#
###############################################

import os
import sys
import time
import logging
import configparser
import argparse
from enum import Enum
from DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2

class DiscoveryAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        RUNNING = 2
        COMPLETED = 3

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.mw_obj = None
        self.logger = logger
        self.publishers = {}  # {topic: [publisher_info]}
        self.subscribers = {}  # {topic: [subscriber_info]}
        self.expected_publishers = 0
        self.expected_subscribers = 0
        self.registered_publishers = 0
        self.registered_subscribers = 0
        self.lookup_count = 0  # Track number of lookup requests

    def configure(self, args):
        try:
            self.logger.info("DiscoveryAppln::configure")
            
            self.state = self.State.CONFIGURE
            
            # Initialize discovery middleware
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)
            
            # Set expected counts
            self.expected_publishers = args.publishers
            self.expected_subscribers = args.subscribers
            
            self.logger.info(f"DiscoveryAppln::Expected publishers: {self.expected_publishers}, subscribers: {self.expected_subscribers}")
            
            self.logger.info("DiscoveryAppln::configure completed")
            
        except Exception as e:
            self.logger.error(f"Exception in configure: {str(e)}")
            raise e

    def driver(self):
        try:
            self.logger.info("DiscoveryAppln::driver")
            
            # Set upcall handle
            self.mw_obj.set_upcall_handle(self)
            
            # Set to running state
            self.state = self.State.RUNNING
            
            # Start event loop
            self.mw_obj.event_loop()
            
            self.logger.info("DiscoveryAppln::driver completed")
            
        except Exception as e:
            self.logger.error(f"Exception in driver: {str(e)}")
            raise e

    # def handle_register(self, register_req):
    #     try:
    #         self.logger.info("DiscoveryAppln::handle_register")
            
    #         # Get registrant info
    #         info = register_req.info
    #         role = register_req.role
    #         topics = register_req.topiclist
            
    #         if role == discovery_pb2.ROLE_PUBLISHER:
    #             # Store publisher info
    #             for topic in topics:
    #                 if topic not in self.publishers:
    #                     self.publishers[topic] = []
    #                 # Check for duplicate registration
    #                 if not any(p["id"] == info.id for p in self.publishers[topic]):
    #                     self.publishers[topic].append({
    #                         "id": info.id,
    #                         "addr": info.addr,
    #                         "port": info.port
    #                     })
    #             self.registered_publishers += 1
    #             self.logger.info(f"Registered publisher {info.id} for topics {topics}")
                
    #         elif role == discovery_pb2.ROLE_SUBSCRIBER:
    #             # Store subscriber info
    #             for topic in topics:
    #                 if topic not in self.subscribers:
    #                     self.subscribers[topic] = []
    #                 # Check for duplicate registration
    #                 if not any(s["id"] == info.id for s in self.subscribers[topic]):
    #                     self.subscribers[topic].append({
    #                         "id": info.id
    #                     })
    #             self.registered_subscribers += 1
    #             self.logger.info(f"Registered subscriber {info.id} for topics {topics}")

    #         elif role == discovery_pb2.ROLE_BOTH:
    #           # it's a broker
    #             self.broker_info = {
    #               "id": info.id,
    #                "addr": info.addr,
    #                "port": info.port
    #             }
    #             self.logger.info(f"Registered broker {info.id}") 
            
    #         self.logger.info(f"Current registration status: {self.registered_publishers}/{self.expected_publishers} publishers, {self.registered_subscribers}/{self.expected_subscribers} subscribers")
            
    #         # Send success response
    #         self.mw_obj.send_register_response(discovery_pb2.STATUS_SUCCESS)
            
    #         return 0
            
    #     except Exception as e:
    #         self.logger.error(f"Exception in handle_register: {str(e)}")
    #         self.mw_obj.send_register_response(
    #             discovery_pb2.STATUS_FAILURE,
    #             str(e)
    #         )
    #         raise e

    def handle_register(self, register_req):
        self.logger.info("DiscoveryAppln::handle_register")
        
        # Get registrant info
        info = register_req.info
        role = register_req.role
        topics = register_req.topiclist
        
        if role == discovery_pb2.ROLE_PUBLISHER:
            # Store publisher info
            for topic in topics:
                if topic not in self.publishers:
                    self.publishers[topic] = []
                # Check for duplicate registration
                if not any(p["id"] == info.id for p in self.publishers[topic]):
                    self.publishers[topic].append({
                        "id": info.id,
                        "addr": info.addr,
                        "port": info.port
                    })
            self.registered_publishers += 1
            self.logger.info(f"Registered publisher {info.id} for topics {topics}")
            
        elif role == discovery_pb2.ROLE_SUBSCRIBER:
            # Store subscriber info
            for topic in topics:
                if topic not in self.subscribers:
                    self.subscribers[topic] = []
                # Check for duplicate registration
                if not any(s["id"] == info.id for s in self.subscribers[topic]):
                    self.subscribers[topic].append({
                        "id": info.id
                    })
            self.registered_subscribers += 1
            self.logger.info(f"Registered subscriber {info.id} for topics {topics}")
        elif role == discovery_pb2.ROLE_BOTH:
            # it's a broker
            self.broker_info = {
                "id": info.id,
                "addr": info.addr,
                "port": info.port
            }
            self.logger.info(f"Registered broker {info.id}")

        
        self.logger.info(f"Current registration status: {self.registered_publishers}/{self.expected_publishers} publishers, {self.registered_subscribers}/{self.expected_subscribers} subscribers")
        
        # Send success response
        self.mw_obj.send_register_response(discovery_pb2.STATUS_SUCCESS)
        
        return 0

    def handle_lookup(self, lookup_req):
            self.logger.info("DiscoveryAppln::handle_lookup")
            self.lookup_count += 1
            
             # Read dissemination strategy from config
            config = configparser.ConfigParser()
            config.read("config.ini")
            dissemination = config["Dissemination"]["Strategy"]

            # Convert publisher info to RegistrantInfo objects
            matching_publishers = []
            if dissemination == "Direct":
              for topic in lookup_req.topiclist:
                if topic in self.publishers:
                    for pub_info in self.publishers[topic]:
                        reg_info = discovery_pb2.RegistrantInfo()
                        reg_info.id = pub_info["id"]
                        reg_info.addr = pub_info["addr"]
                        reg_info.port = pub_info["port"]
                        # Only add if not already in the list
                        if not any(p.id == reg_info.id for p in matching_publishers):
                            matching_publishers.append(reg_info)
            elif dissemination == "ViaBroker":
                 if hasattr(self, 'broker_info') and self.broker_info:
                    reg_info = discovery_pb2.RegistrantInfo()
                    reg_info.id = self.broker_info["id"]
                    reg_info.addr = self.broker_info["addr"]
                    reg_info.port = self.broker_info["port"]
                    matching_publishers.append(reg_info)
                    self.logger.debug ("Broker address found and sent")
                 else:
                   self.logger.warning ("Broker not registered, no lookup done")

            
            self.logger.info(f"Lookup request {self.lookup_count}: Found {len(matching_publishers)} unique publishers for topics {lookup_req.topiclist}")
            
            # Send response with the list of matching publishers
            self.mw_obj.send_lookup_response(matching_publishers)
            
            return 0

    def handle_isready(self, isready_req):
        try:
            self.logger.info("DiscoveryAppln::handle_isready")
            
            # Check if all expected entities are registered
            is_ready = (self.registered_publishers == self.expected_publishers and 
                       self.registered_subscribers == self.expected_subscribers)
            
            if is_ready:
                self.logger.info("System is ready - all publishers and subscribers registered")
            else:
                self.logger.info(f"System not ready - waiting for {self.expected_publishers - self.registered_publishers} publishers and {self.expected_subscribers - self.registered_subscribers} subscribers")
            
            self.mw_obj.send_isready_response(is_ready)
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Exception in handle_isready: {str(e)}")
            raise e

    # def handle_lookup(self, lookup_req):
    #     try:
    #         self.logger.info("DiscoveryAppln::handle_lookup")
    #         self.lookup_count += 1
            
    #         # Convert publisher info to RegistrantInfo objects
    #         matching_publishers = []
    #         for topic in lookup_req.topiclist:
    #             if topic in self.publishers:
    #                 for pub_info in self.publishers[topic]:
    #                     reg_info = discovery_pb2.RegistrantInfo()
    #                     reg_info.id = pub_info["id"]
    #                     reg_info.addr = pub_info["addr"]
    #                     reg_info.port = pub_info["port"]
    #                     # Only add if not already in the list
    #                     if not any(p.id == reg_info.id for p in matching_publishers):
    #                         matching_publishers.append(reg_info)
            
    #         self.logger.info(f"Lookup request {self.lookup_count}: Found {len(matching_publishers)} unique publishers for topics {lookup_req.topiclist}")
            
    #         # Send response with the list of matching publishers
    #         self.mw_obj.send_lookup_response(matching_publishers)
            
    #         return 0
            
    #     except Exception as e:
    #         self.logger.error(f"Exception in handle_lookup: {str(e)}")
    #         raise e

    def invoke_operation(self):
        """Handle any periodic operations - in Discovery's case, just return None"""
        try:
            self.logger.debug("DiscoveryAppln::invoke_operation")
            return None  # No periodic actions needed for discovery service
            
        except Exception as e:
            self.logger.error(f"Exception in invoke_operation: {str(e)}")
            raise e

    def cleanup(self):
        try:
            self.logger.info("DiscoveryAppln::cleanup")
            
            # Reset all tracking variables
            self.publishers.clear()
            self.subscribers.clear()
            self.registered_publishers = 0
            self.registered_subscribers = 0
            self.lookup_count = 0
            
            # Clean up middleware
            if self.mw_obj:
                self.mw_obj.cleanup()
            
            self.state = self.State.COMPLETED
            
        except Exception as e:
            self.logger.error(f"Exception in cleanup: {str(e)}")
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Application")
    
    parser.add_argument("-P", "--publishers", type=int, default=1,
                        help="Expected number of publishers")
                        
    parser.add_argument("-S", "--subscribers", type=int, default=1,
                        help="Expected number of subscribers")
                        
    parser.add_argument("-p", "--port", type=int, default=5555,
                        help="Port number on which our discovery service runs")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG,logging.INFO,logging.WARNING,
                                logging.ERROR,logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    
    args = parser.parse_args()
    return args

def main():
    try:
        # Set up logging
        logging.info("Discovery Application - Main")
        logger = logging.getLogger("DiscoveryAppln")

        # Parse command line arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        logger.debug(f"Main: setting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(args.loglevel)
        logger.addHandler(console_handler)

        # Create discovery application instance
        logger.debug("Main: create discovery application object")
        discovery_app = DiscoveryAppln(logger)

        # Configure the application
        logger.debug("Main: configure discovery application object")
        discovery_app.configure(args)

        # Start the driver
        logger.debug("Main: invoke the discovery application driver")
        discovery_app.driver()

    except KeyboardInterrupt:
        logger.info("Discovery Application - Interrupted by user")
        try:
            if 'discovery_app' in locals():
                discovery_app.cleanup()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    except Exception as e:
        logger.error(f"Exception in main: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Discovery Application - Completed")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
