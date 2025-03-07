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
We compared the performance between PA1 (baseline) and PA2 (fault-tolerant) implementations, focusing on the broker-based dissemination strategy. Here are the key findings:

### Latency Comparison
                     Average Latency (ms)
      PA1 (ViaBroker):     6.99
      PA2 (ViaBroker):     5,829.47

PA2 shows significantly higher average latency due to:
- ZooKeeper coordination overhead
- Leader election processes
- State synchronization between primary and backup brokers
The ~5.8 second average latency in PA2 reflects the cost of ensuring fault tolerance

### Message Throughput
               Message Count
         PA1:     35,695
         PA2:     13,401

PA2 processed ~37.5% of PA1's message volume
Lower throughput is attributed to:
- Additional coordination overhead
- Time spent in recovery procedures
- State synchronization between replicas

### Latency Distribution
       Min (ms)    Max (ms)    Avg (ms)
       PA1:     0           130         6.99
       PA2:     1           6,006,554   5,829.47

PA1 shows consistent performance with low variability
PA2 exhibits:
- Higher minimum latency due to coordination overhead
- Extremely high maximum latency (6 seconds) during recovery events
- Greater latency variability overall

## Impact of Recovery and Coordination
### Latency Overhead:
~5.8 seconds average latency in PA2 vs 7ms in PA1
- Represents the cost of ensuring message delivery during failures
- Recovery procedures significantly impact individual message latencies

### Throughput Impact:
62.5% reduction in message throughput
- Trade-off between reliability and performance
- Recovery periods reduce effective transmission time

### System Stability:
- Maximum latency of 6 seconds indicates recovery duration
- Minimum latency of 1ms shows baseline performance during stable operation
- Wide latency range demonstrates system adaptability during failures

## Conclusions
### Performance Trade-offs:
- Fault tolerance mechanisms introduce significant overhead
- System prioritizes reliability over raw performance
- Recovery capabilities come at the cost of increased latency

### System Reliability:
- Successfully handles failure scenarios
- Maintains message delivery despite component failures
- Automatic recovery without manual intervention

### Design Implications:
- Suitable for systems requiring strong reliability guarantees
- May need optimization for latency-sensitive applications
- Consider hybrid approaches for balancing performance and reliability

## Future Improvements
### Latency Optimization:
- Optimize ZooKeeper interaction patterns
- Implement more efficient state synchronization
- Reduce recovery time through better failure detection

### Throughput Enhancement:
- Batch processing during stable operation
- Optimize broker handover procedures
- Implement parallel recovery mechanisms

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