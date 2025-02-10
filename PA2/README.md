# Distributed Pub/Sub System with Centralized Discovery (Milestone 1)

This project implements a distributed publish-subscribe system with a centralized discovery service using ZeroMQ (ZMQ) middleware.

## System Architecture

The system consists of three main components:

1. **Discovery Service**
   - Centralized registry for publishers and subscribers
   - Tracks registration status of all entities
   - Provides readiness status when system is fully registered

2. **Publishers**
   - Register with discovery service
   - Publish on specific topics
   - Support direct dissemination to subscribers

3. **Subscribers** 
   - Register with discovery service
   - Subscribe to topics of interest
   - Receive publications from matching publishers

## Directory Structure

```
.
├── CS6381_MW/                 # Middleware implementation
│   ├── __init__.py
│   ├── discovery_pb2.py      # Generated protobuf code
│   ├── PublisherMW.py        # Publisher middleware
│   └── topic_pb2.py          # Topic protobuf code
├── config.ini                # System configuration
├── DiscoveryAppln.py        # Discovery service application
├── DiscoveryMW.py           # Discovery middleware
├── discovery.proto          # Discovery service protocol
├── PublisherAppln.py        # Publisher application
├── SubscriberAppln.py       # Subscriber application  
├── SubscriberMW.py          # Subscriber middleware
├── test_pubsub.py          # Test harness
├── topic.proto             # Topic protocol
└── topic_selector.py       # Topic selection utility
```

## Installation Requirements

```bash
# Install ZMQ
sudo pip3 install pyzmq

# Install protobuf
sudo pip3 install protobuf

# Install Mininet (if not already installed)
sudo apt-get install mininet
```

## Testing with Mininet

Start a simple topology with 5 hosts:
```bash
sudo mn --topo=single,5
```

In the Mininet CLI, start components:
```bash
# Start Discovery Service on h1
h1 python3 DiscoveryAppln.py -P 2 -S 2 -p 5555 -l 20 &

# Start Publishers on h2 and h3
h2 python3 PublisherAppln.py -n pub1 -d h1:5555 -p 5556 -T 2 -l 20 &
h3 python3 PublisherAppln.py -n pub2 -d h1:5555 -p 5557 -T 2 -l 20 &

# Start Subscribers on h4 and h5
h4 python3 SubscriberAppln.py -n sub1 -d h1:5555 -T 2 -l 20 &
h5 python3 SubscriberAppln.py -n sub2 -d h1:5555 -T 2 -l 20 &
```

## Command Line Arguments

### Discovery Application
- `-P, --publishers`: Expected number of publishers
- `-S, --subscribers`: Expected number of subscribers  
- `-p, --port`: Port number for discovery service
- `-l, --loglevel`: Logging level (10=DEBUG, 20=INFO)

### Publisher Application
- `-n, --name`: Publisher name
- `-d, --discovery`: Discovery service address
- `-p, --port`: Publisher port number
- `-T, --num_topics`: Number of topics to publish
- `-l, --loglevel`: Logging level

### Subscriber Application  
- `-n, --name`: Subscriber name
- `-d, --discovery`: Discovery service address
- `-T, --num_topics`: Number of topics to subscribe
- `-l, --loglevel`: Logging level

## Configuration

The system behavior is configured through `config.ini`:

```ini
[Discovery]
Strategy=Centralized

[Dissemination]
Strategy=Direct
```

## Current Status (Milestone 1)

Completed features:
- ✅ Discovery application and middleware implementation
- ✅ Subscriber application and middleware implementation
- ✅ Centralized registration system
- ✅ Publisher/Subscriber registration with discovery service
- ✅ System readiness status from discovery service
- ✅ Testing on Mininet network topology

Todo:
- Direct dissemination implementation (Milestone 2)
- Broker-based dissemination (Milestone 3)
- Performance measurements
- Analytics and visualization
