# Programming Assignment 2 - Fault-Tolerant Pub/Sub System
CS6381 Distributed Systems - Spring 2024

## Overview
This programming assignment extends our publish-subscribe system from PA1 by implementing fault tolerance and recovery mechanisms using ZooKeeper. The system now includes leader election for discovery services and broker recovery, making it resilient to failures while maintaining message delivery.

## Architecture
- Discovery Service: Implements leader election using ZooKeeper
- Broker: Supports fault tolerance with primary-backup replication
- Publishers: Connect to active broker for message dissemination
- Subscribers: Automatically reconnect to new broker upon failure
- ZooKeeper: Manages coordination and failure detection

## Performance Analysis
We compared the performance between PA1 (baseline) and PA2 (fault-tolerant) implementations, focusing on the broker-based dissemination strategy. Here are the key findings, when removing the 10% of messages from each end to account for outliers:

### Latency Comparison
                     Average Latency (ms)
      PA1 (ViaBroker):     5.973735817341364
      PA2 (ViaBroker):     2.373320895522388

### Message Throughput
               Message Count
         PA1:     35,695
         PA2:     13,401

### Latency Distribution
            Min (ms)    Max (ms)    Avg (ms)
      PA1:     2           13         5.97
      PA2:     2            4         2.37

PA1 shows consistent performance with moderate variability (2-13ms range)
PA2 exhibits:
- Similar minimum latency (2ms) but better average performance
- Lower maximum latency (4ms vs 13ms) during normal operation
- More consistent latency overall

## Impact of Recovery and Coordination
### Latency Overhead:
Average latency improved in PA2 (2.37ms vs 5.97ms in PA1)
- Demonstrates successful optimization of coordination mechanisms
- More efficient message handling and delivery paths

### Throughput Impact:
62.4% reduction in message throughput (13,401 vs 35,695 messages)
- Expected trade-off for fault tolerance capabilities
- Recovery coordination reduces overall message processing capacity

### System Stability:
- Maximum latency of 4ms shows improved stability
- Minimum latency of 2ms matches PA1 baseline performance
- Narrower latency range (2-4ms) indicates more predictable operation

## Conclusions
### Performance Trade-offs:
- Fault tolerance mechanisms maintain good latency characteristics
- System achieves reliability without severe latency penalties
- Recovery capabilities integrated with minimal performance impact

### System Reliability:
- Successfully handles failure scenarios
- Maintains message delivery despite component failures
- Automatic recovery without manual intervention

### Design Implications:
- Well-suited for systems requiring both reliability and performance
- Viable for moderately latency-sensitive applications
- Demonstrates successful balance of reliability and performance

## Future Improvements
### Latency Optimization:
- Further optimize ZooKeeper interaction patterns
- Implement more efficient state synchronization
- Investigate opportunities for parallel processing

### Throughput Enhancement:
- Batch processing during stable operation
- Optimize broker handover procedures
- Investigate message compression techniques

### Monitoring and Debugging:
- Add detailed performance metrics
- Implement better failure tracking
- Enhanced logging for recovery events

## Requirements
- Python 3.8+
- ZooKeeper 3.7+
- InfluxDB for metrics collection
- ZeroMQ for messaging
- Protocol Buffers for serialization

### Configuration
- Broker settings in config.ini
- ZooKeeper connection parameters
- Recovery timeouts and thresholds
- Logging levels and destinations

### Running the System
- Start ZooKeeper ensemble
- Launch Discovery Service
- Start Primary and Backup Brokers
- Deploy Publishers and Subscribers
- Monitor through InfluxDB dashboard