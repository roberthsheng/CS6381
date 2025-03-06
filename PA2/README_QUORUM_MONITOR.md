# Quorum Monitor for Distributed System

This document describes how to use the Quorum Monitor for maintaining quorums of 2 replicas for both Discovery and Broker nodes in our distributed system.

## Overview

The Quorum Monitor is responsible for:

1. Monitoring the status of Discovery and Broker nodes
2. Detecting when a node fails (not just due to lease expiry)
3. Automatically restarting the failed node on its original VM
4. Maintaining a quorum of at least 2 nodes for each service type

During recovery, updates are paused (as required by the quorum enforcement in the broker middleware) until the quorum is restored.

## Installation

No additional installation is required beyond the base dependencies for the project:
- Python 3.x
- kazoo (ZooKeeper client)
- subprocess, threading, and other standard libraries

## Usage

Deploy the Quorum Monitor on VM1 (alongside ZooKeeper) for the most reliable operation:

```bash
# On VM1
python3 ~/CS6381/PA2/quorum_monitor.py --zk_addr 192.168.5.17:2181 --loglevel 20
```

Command-line options:
- `--zk_addr`: ZooKeeper address (default: 192.168.5.17:2181)
- `--loglevel`: Logging level (default: 20 for INFO, use 10 for DEBUG)

## How It Works

1. The monitor connects to ZooKeeper and sets up watches on the `/discovery` and `/brokers` paths
2. When a node fails, its ephemeral znode is removed from ZooKeeper
3. The monitor detects this change and:
   - Determines which node is missing
   - Uses SSH to restart the service on the appropriate VM
   - Waits for the quorum to be restored

4. The broker's quorum enforcement logic will automatically pause updates when either quorum is lost
5. When the quorum is restored, updates resume automatically

## Customization

The VM configurations and commands are defined in the `vm_config` structure at the beginning of the class. Modify these if your VM addresses or application paths change.

## Logs

The monitor creates a log file `quorum_monitor.log` in the current directory, which can be used to monitor its activity.

When services are restarted, a `recovery.log` file is created on the target VM with output from the restarted service.

## Testing

To test the monitor:
1. Start the system normally with all services running
2. Kill a Discovery or Broker node process (e.g., using `Ctrl+C` or `kill`)
3. Watch the quorum monitor logs to see it detect and recover the failed node
4. Verify that the system pauses updates during recovery and resumes once quorum is restored

## Limitations

- The monitor requires SSH access between VMs without password prompts (using key-based authentication)
- It focuses on node failure recovery, not handling network partitions
- If both nodes of the same type fail simultaneously, recovery starts with the primary node first 