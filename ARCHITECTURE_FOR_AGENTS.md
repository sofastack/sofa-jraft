# Architecture Reference — SOFAJRaft

> This is the reference companion to [`AGENTS.md`](./AGENTS.md).
> Read AGENTS.md first for hard constraints and workflow. This file is background knowledge.

---

## jraft-core Package Map

```
com.alipay.sofa.jraft/
├── (root)              # Top-level interfaces: Node, StateMachine, Closure, FSMCaller,
│                       # Lifecycle, ReplicatorGroup, CliService, RouteTable, JRaftUtils
├── core/               # Implementations: NodeImpl, Replicator, FSMCallerImpl, BallotBox,
│                       # ReadOnlyServiceImpl, CliServiceImpl, State, NodeMetrics
├── conf/               # Configuration, ConfigurationEntry, ConfigurationManager
├── entity/             # Data: LogEntry, LogId, PeerId, NodeId, Task, Ballot
│   └── codec/          # Log serialization (V1 legacy, V2 current, AutoDetect)
├── storage/            # Storage interfaces: LogStorage, LogManager, SnapshotStorage,
│   │                   # RaftMetaStorage, SnapshotExecutor, FileService
│   ├── impl/           # LogManagerImpl, LocalRaftMetaStorage
│   ├── log/            # RocksDBSegmentLogStorage, SegmentFile
│   ├── snapshot/       # SnapshotExecutorImpl
│   │   ├── local/      # LocalSnapshotStorage, LocalSnapshotWriter/Reader
│   │   └── remote/     # RemoteFileCopier, SnapshotCopier
│   └── io/             # ProtoBufFile, LocalDirReader, FileReader
├── rpc/                # RPC abstractions: RpcClient, RpcServer, RaftClientService,
│   │                   # RaftServerService, RpcRequests (generated), ProtobufMsgFactory
│   └── impl/           # AbstractClientService, BoltRpcClient, BoltRpcServer
│       ├── core/       # Request processors: AppendEntries, RequestVote, InstallSnapshot,
│       │               # ReadIndex, TimeoutNow, GetFile, DefaultRaftClientService
│       └── cli/        # CLI processors: AddPeer, RemovePeer, TransferLeader, etc.
├── closure/            # Callback types: ReadIndexClosure, SaveSnapshotClosure,
│                       # ClosureQueue, SynchronizedClosure, CatchUpClosure
├── option/             # All configuration: NodeOptions, RaftOptions, RpcOptions,
│                       # ReplicatorOptions, LogManagerOptions, ReadOnlyOption (enum)
├── error/              # RaftError (enum), RaftException, and specific exceptions
│                       # (LogNotFound, InvokeTimeout, Overload, etc.)
└── util/               # Threading, buffers, timers, metrics, SPI
    ├── concurrent/     # LongHeldDetectingReadWriteLock, MpscSingleThreadExecutor,
    │                   # FixedThreadsExecutorGroup, AdjustableSemaphore
    ├── internal/       # Unsafe field updaters, reflection fallbacks
    └── timer/          # HashedWheelTimer, DefaultTimer, RaftTimerFactory (SPI)
```

---

## Critical Files

These files contain the core Raft algorithm. Read and understand them before modifying anything in `core/`:

| File | Responsibility |
|------|----------------|
| **`core/NodeImpl.java`** | Central Raft node state machine. Leader election, log application, configuration changes, all RPC handling. All state transitions flow through here. |
| **`core/Replicator.java`** | Log replication to a single follower. Manages probe → replicate → snapshot states. Heartbeats. One instance per follower per leader term. |
| **`storage/impl/LogManagerImpl.java`** | Log management. Disruptor-based write pipeline. Committed index tracking. Configuration log entries. |
| **`core/FSMCallerImpl.java`** | State machine event dispatcher. Disruptor-based. Calls user's `StateMachine.onApply()`. Single-threaded guarantee for FSM safety. |
| **`core/BallotBox.java`** | Quorum tracking. Advances commitIndex when majority acknowledges. StampedLock for concurrent access. |

---

## Threading Model

```
                    ┌─────────────────────────┐
  User Thread       │  Node.apply(Task)       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
  Disruptor #1      │  NodeImpl               │  Log append + dispatch to replicators
  (Node events)     │  (event-driven loop)    │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                   │
  ┌───────────▼──────┐ ┌────────▼────────┐ ┌───────▼────────┐
  │ Replicator (F1)  │ │ Replicator (F2) │ │ Replicator (F3)│  Per-follower
  │ (ThreadId-based) │ │                 │ │                │  logical handles
  └───────────┬──────┘ └────────┬────────┘ └───────┬────────┘
              │                  │                   │
              └──────────────────┼──────────────────┘
                                 │ quorum reached
                    ┌────────────▼────────────┐
  Disruptor #2      │  FSMCallerImpl          │  Batched apply to state machine
  (FSM events)      │  (single consumer)      │  SINGLE-THREADED
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  StateMachine.onApply() │  User code runs here
                    └─────────────────────────┘

  Other threads:
  ├── HashedWheelTimer — election timeouts, heartbeats, replication retries
  ├── Disruptor #3 (LogManager) — async log persistence
  ├── Disruptor #4 (ReadOnlyServiceImpl) — batched read-index requests
  └── ThreadPoolsFactory — closure executor pools (global + per-group isolation)
```

### Thread summary

| Component | Thread type | Concurrency |
|-----------|-------------|-------------|
| NodeImpl event loop | Disruptor single consumer | 1 per node |
| FSMCaller | Disruptor single consumer | 1 per node (FSM safety guarantee) |
| LogManager persistence | Disruptor single consumer | 1 per node |
| ReadOnlyService | Disruptor single consumer | 1 per node |
| Replicator (per follower) | ThreadId (logical handle, not a dedicated thread). RPC callbacks run on RPC framework threads; heartbeats/probes scheduled by HashedWheelTimer. | 1 logical instance per follower per leader |
| Election/Heartbeat timers | HashedWheelTimer | Shared timer wheel |
| Closure execution | ThreadPoolsFactory | Configurable pool per group |
| RPC I/O | Bolt/gRPC thread pool | Framework-managed |

### Locking

`NodeImpl` uses `LongHeldDetectingReadWriteLock` — read lock for queries, write lock for state mutations. When `-Djraft.node.detecting.lock.max_blocking_ms_to_report` is set to a positive value, the lock logs warnings if acquisition blocks longer than the threshold. Disabled by default (`-1`).

`ReplicatorGroupImpl` has **no group-level lock**. It uses a `ConcurrentHashMap<PeerId, ThreadId>` for replicator lookup. Individual Replicators are guarded by `ThreadId.lock()` (see `ThreadId.java`).

Some paths in `ReplicatorGroupImpl` (e.g., `checkReplicator`) optionally acquire `node.writeLock`.

---

## Data Flows

### Log Replication (Leader → Follower → Commit)

```
User: Node.apply(Task{data, closure})
  → NodeImpl: validate leadership, encode LogEntry, queue to Disruptor
    → LogManager: append to LogStorage (RocksDBLogStorage by default)
    → Replicator[]: send AppendEntriesRequest via RPC to each follower
      → Follower: AppendEntriesRequestProcessor validates, appends locally
      → Follower: responds with AppendEntriesResponse(success, lastLogIndex)
    → Leader Replicator: on response, BallotBox.commitAt(index)
      → BallotBox: count votes, if quorum → advance commitIndex
        → FSMCaller: doCommitted(committedIndex)
          → StateMachine.onApply(Iterator over committed entries)
          → Invoke each Task's Closure with Status.OK()
```

### Leader Election

```
ElectionTimer fires (no heartbeat received within timeout)
  → NodeImpl: preVote to all peers (prevents disruption from partitioned nodes)
    → If preVote quorum: increment term, persist term+votedFor to RaftMetaStorage
      → Send RequestVoteRequest to all peers
        → Peers: grant vote if candidate's log is at least as up-to-date
      → If quorum votes: transition to LEADER
        → FSMCaller.onLeaderStart(term)
        → Create Replicator for each follower
        → Send initial heartbeats (empty AppendEntries)
```

---

## Design Patterns

### Closure/Callback

Every async operation completes via `Closure.run(Status)`. No futures, no promises. Closures support custom executors via `Closure.getExecutor()`. The `ClosureQueue` maintains FIFO ordering for committed entry callbacks.

### ThreadId

Replicators are identified by `ThreadId`, not thread references. This enables lifecycle management (start/stop/error) across thread pools without thread pinning.

### Disruptor Ring Buffers

Four components use LMAX Disruptor for lock-free event processing: NodeImpl, FSMCallerImpl, LogManagerImpl, and ReadOnlyServiceImpl. Look for `publishEvent` / `onEvent` patterns.

### SPI Extension

Pluggable implementations via `JRaftServiceLoader`:
- `RaftRpcFactory` — transport layer (Bolt default, gRPC extension)
- `LogEntryCodecFactory` — log serialization (V1 legacy, V2 current)
- `RaftTimerFactory` — timer implementation

---

## Storage Architecture

```
LogStorage (interface)
  ├── RocksDBLogStorage             # Default (returned by DefaultJRaftServiceFactory)
  │     └── RocksDB                 # Stores log entries directly in RocksDB
  └── RocksDBSegmentLogStorage      # Alternative segment-based implementation
        ├── RocksDB (index only)    # Maps logIndex → file position
        └── SegmentFile[]           # Actual log data (append-only files)

RaftMetaStorage (interface)
  └── LocalRaftMetaStorage        # File-based: stores {term, votedFor} as ProtoBuf

SnapshotStorage (interface)
  └── LocalSnapshotStorage        # Directory-based snapshots
        ├── snapshot_XXXXX/       # One dir per snapshot (XXXXX = lastIncludedIndex)
        │   ├── __raft_snapshot_meta  # ProtoBuf metadata
        │   └── (user files)      # User's snapshot data
        └── temp/                 # In-progress snapshot
```

### Log entry codecs

- **V1** (legacy): ProtoBuf-based encoding. Still supported for reading old data.
- **V2** (current): More efficient binary format. Default for writes.
- `AutoDetectDecoder` handles both versions transparently.

---

## RPC Architecture

```
RPC Layer (pluggable via SPI)
├── Bolt (default) — SOFA's RPC framework
│   ├── BoltRpcClient / BoltRpcServer
│   └── Uses Hessian serialization for some types
└── gRPC (extension) — jraft-extension/rpc-grpc-impl
    ├── GrpcClient / GrpcServer
    └── Uses ProtoBuf marshalling

Request Processors (server-side handlers):
├── Core Raft:
│   ├── AppendEntriesRequestProcessor  — log replication
│   ├── RequestVoteRequestProcessor    — leader election
│   ├── InstallSnapshotRequestProcessor — snapshot transfer
│   ├── ReadIndexRequestProcessor      — linearizable reads
│   ├── TimeoutNowRequestProcessor     — forced leader transfer
│   └── GetFileRequestProcessor        — snapshot file chunks
└── CLI/Admin:
    ├── AddPeer / RemovePeer / ChangePeersRequestProcessor
    ├── TransferLeaderRequestProcessor
    ├── GetLeader / GetPeersRequestProcessor
    └── SnapshotRequestProcessor
```

---

## Configuration

Two main config objects:

**`NodeOptions`** — Everything to start a node:
- `electionTimeoutMs`, `snapshotIntervalSecs`, `catchupMargin`
- `initialConf` — initial cluster membership
- `fsm` — user's StateMachine implementation
- `logUri`, `raftMetaUri`, `snapshotUri` — storage paths
- `raftOptions` — protocol-level tuning (see below)

**`RaftOptions`** — Protocol-level tuning:
- `maxByteCountPerRpc` — max RPC payload size
- `maxEntriesSize` / `maxBodySize` — batch limits for AppendEntries
- `disruptorBufferSize` — ring buffer size for Disruptors
- `replicatorPipeline` — enable/disable pipeline replication
- `maxReplicatorInflightMsgs` — pipeline depth
- `enableLogEntryChecksum` — data integrity verification

---

## Linearizable Reads

Two modes (configured via `ReadOnlyOption`):

| Mode | Mechanism | Tradeoff |
|------|-----------|----------|
| `ReadOnlySafe` | Leader sends heartbeat to quorum before responding | Correct under all conditions. Extra RTT. |
| `ReadOnlyLeaseBased` | Leader trusts its lease hasn't expired | Lower latency. Requires clock synchronization. |

Implementation: `ReadOnlyServiceImpl` batches read-index requests using a Disruptor, then either confirms via heartbeat quorum or lease check.

---

## Error Handling

- `Status` is the universal result type. Check `status.isOk()` before proceeding.
- `RaftError` enum defines all error codes (ESTALE, EBUSY, EAGAIN, EIO, etc.).
- Never silently swallow exceptions. If an exception in FSM goes unhandled, the apply pipeline stalls permanently.

---

## Protobuf Files

Proto definitions in `jraft-core/src/main/resources/`:

| File | Purpose |
|------|---------|
| `rpc.proto` | Core Raft RPC messages (AppendEntries, RequestVote, InstallSnapshot, ReadIndex) |
| `cli.proto` | Admin/CLI operations (AddPeer, RemovePeer, TransferLeader, GetLeader) |
| `raft.proto` | Snapshot metadata |
| `enum.proto` | Shared enums (EntryType: DATA, CONF, NO_OP) |
| `log.proto` | V2 log entry format |
| `local_storage.proto` | Raft metadata (term, votedFor) |
| `local_file_meta.proto` | Snapshot file metadata |

---

## Other Modules

### jraft-extension/rpc-grpc-impl
Alternative RPC transport using gRPC. Drop-in replacement via SPI.
Entry point: `GrpcRaftRpcFactory`.

### jraft-extension/bdb-log-storage-impl
Alternative log storage using Berkeley DB (Sleepycat JE).
Entry point: `BDBLogStorage`, `BDBLogStorageJRaftServiceFactory`.

### jraft-rheakv/rheakv-core
Distributed KV store built on jraft. `StoreEngine` / `RegionEngine` for region management. `DefaultRegionKVService` for KV operations. Storage backend: RocksDB.

### jraft-rheakv/rheakv-pd
Placement Driver for cluster coordination (similar to TiKV's PD). Region splitting, leader balancing, stats collection. Pipeline-based handler architecture.

### jraft-test
Test infrastructure + atomic KV reference implementation. `AtomicStateMachine` is a complete StateMachine example with snapshot support.

### jraft-example
Working examples: leader election, priority election, RheaKV operations (CRUD, distributed lock), benchmark.
