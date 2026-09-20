# Jetski
Jetski is an experimental, performance-aware Raft consensus implementation built from scratch based on the original [Raft paper](https://raft.github.io/raft.pdf), with no dependencies other than the C++ STL and glibc. The implementation is available as a header-only C++20 library.
# Table of Contents
- [Overview](#overview)
  - [Design](#design)
- [Get Started](#get-started)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Usage](#usage)

# Overview
This implementation currently supports the following features:
- Leader election
- Log replication
- Log compaction
- Crash recovery
- Snapshot installations for lagging followers
- Dynamic cluster configurations
- Automatic client request forwarding to the leader
- Retries for failed messages

## Design
For a more complete design walkthrough, see [DESIGN.md](DESIGN.md).

This implementation is intended to incur lower latency by applying various architectural optimizations, including:
- Multithreaded event loop architecture, with no cross-thread shared state for minimal cache coherency overhead

- Event-driven network I/O with epoll

- Nonblocking sockets

- Custom lock-free ring buffers for fast message passing (no traditional locking/mutexes used anywhere)

- RPCs sent over UDP (can opt for TCP in config file)

- Periodic fflush/fsync as opposed to calling on every disk write

- Lazy log entry application and state materialization (log entries are only applied to the state machine when the state is requested by the client)

- A custom zero-allocation byte-based network protocol rolled from the ground up

# Get Started
## Prerequisites
- Linux >= 2.5.45
- a compiler that supports C++20 (i.e. GCC >= 10 / Clang >= 10)

Jetski is not currently supported on macOS or Windows.

## Configuration
A configuration file template is defined in config.hpp.example. For the library to work, there must be a `config.hpp` file on every node in the cluster. The value of a particular configuration variable doesn't need to be consistent across every node unless otherwise stated.
```
cp config.hpp.example config.hpp
```
### Raft Parameters
The following parameters must be explicitly configured:
- `BASE_CLUSTER_SIZE` - the number of nodes in the initial cluster configuration, including the node this is running on. Should be the same across every node's config file. Must be >= 1.

- `MY_ID` - the logical ID of the node this is being ran on. Must be unique across every node, and ideally should be dense (i.e. 0,1,... BASE_CLUSTER_SIZE-1) and not sparse. Must be >= 0.

- `CMD_SIZE` - Each log entry contains a term number and a byte-array command of fixed size `CMD_SIZE`. Adjust this based on your application state and the log entries that are used to modify it.

- `setup_peers()` - Initializes the base cluster configuration by mapping node IDs to their IP addresses. The number of mappings should correspond to `BASE_CLUSTER_SIZE`. Should be consistent across all nodes when starting the cluster for the first time.

### Other Parameters
The following parameters can optionally be tuned for your system or for potentially better performance:
- `SERVER_PORT` - the port that each node's listening socket binds onto.
 
- `SOCKET_TYPE` - TCP or UDP. Should be consistent across all nodes.

- `MAX_ENTRIES` - the maximum number of log entries that can be included in a single AppendEntries RPC. Must be >= 1.

- `SNAPSHOT_CHUNK_SIZE` - the byte size of each snapshot chunk that leaders send to lagging followers. If this value is too small, excessive network bandwidth will be used due to the high volume of packets being sent. If this value is too large, buffer memory ends up wasted. Recommended to be 2-8 MB for practical workloads.

- `EVENT_LOOP_THREADS` - number of threads for handling network I/O. Must be >= 1. Setting this to a power of 2 allows for a slight optimization involving bitwise operations instead of modulo.

- `SERVER_BACKLOG` - maximum number of incoming connections that can wait on the listening socket's queue before they get dropped. Tune this based on your cluster size and how much it may change during runtime.

- `MAX_SERVER_CONNS` - maximum number of connections to "clients" (nodes on the sending side of an RPC) in the cluster. Tune this based on your cluster size and how much it may change during runtime.

- `LOG_COMPACT_THRESHOLD` - the maximum number of log entries that can be held in memory and stored in the log file before they get compacted. Must be >= 1.

- `EPOLL_BATCH` - the maximum number of file descriptors that can be processed per network thread on each event loop iteration. Must be >= 1.

- `UDP_RECV_BATCH` - the maximum number of datagrams that can be extracted from the UDP listen-side socket on each `recvmmsg` call. This is unused if TCP is configured as the `SOCKET_TYPE`. Must be >= 1.

- `NODE_EVENT_LOOP_INBOX_RING_CAP` - the byte capacity of the ring buffer used to pass messages from a network thread to the main thread. Can be optimized by setting to a power of 2. Must be >= 1.

- `NODE_CLIENT_INBOX_RING_CAP` - the byte capacity of the ring buffer used to pass messages from the client to the main thread. Can be optimized by setting to a power of 2. Must be >= 1.

- `EVENT_LOOP_INBOX_RING_CAP` - the byte capacity of the ring buffer used to pass messages from the main thread to a network thread. Can be optimized by setting to a power of 2. Must be >= 1.

- `MIN_ELECTION_TIMEOUT_MS` - the lower bound (in milliseconds) from which to calculate the election timeout, which is re-randomized after each timeout. The default is 150 ms as specified in the Raft paper. Must be >= 1.

- `MAX_ELECTION_TIMEOUT_MS` - the upper bound (in milliseconds) from which to calculate the election timeout, which is re-randomized after each timeout. The default is 300 ms as specified in the Raft paper. Must be >= 1.

- `FLUSH_INTERVAL_MS` - the period (in milliseconds) for which the log file and snapshot file should be fflushed and fsynced to disk. Longer intervals incur less performance overhead at the cost of potential data loss in the event that a node crashes. Shorter intervals incur more latency, but have less potential for data loss. Recommended to be >= 1000.

- `{LOG, SNAPSHOT, SNAPSHOT_TMP, STATE_MACHINE, STATE_MACHINE_TMP}_FILE_PATH` - file paths for the relevant files for persistence and replication. This includes temporary files that are used for atomic renames.

## Usage
