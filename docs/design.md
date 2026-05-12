# Design

## Summary

The solution is organized into five layers:

1. `core`: Raft protocol rules and node modeling as an `rxnet` FSM.
2. `transport`: message and client-request delivery.
3. `storage`: persistence for Raft metadata, log, and applied state.
4. `state_machine`: application logic; in this project, a key-value store.
5. `telemetry`: optional structured event emission for an external shell.

The C and Python implementations share the same baseline semantics, although
not the same internal layout. The `joint consensus` reconfiguration described
in this document is implemented in the Python and C references.

## Node Model

Each node maintains:

- `node_id`
- `config_state`
- `peers`
- `current_term`
- `voted_for`
- `log`
- `commit_index`
- `last_applied`
- `role`
- `leader_id`
- `election_deadline_ms`
- `heartbeat_deadline_ms`
- `next_index[peer]`
- `match_index[peer]`

The `rxnet` FSM models the role and the main Raft events:

- `FOLLOWER`
- `CANDIDATE`
- `LEADER`

The rest of the state lives in the node's user structure and is updated in
phase callbacks.

In addition, the Python implementation models two secondary FSMs:

- `CompactionFSM`: `IDLE -> SNAPSHOT_PENDING -> COMPACTING -> IDLE`
- `MembershipFSM`: `STABLE -> JOINT_PENDING -> JOINT -> FINALIZING -> STABLE`

Membership is no longer a simple list. It is represented as an explicit
configuration:

- `old_members`
- `new_members | None`
- `configuration_index`

When `new_members is None`, the configuration is stable. When `new_members`
exists, the node is in `joint consensus`.

The main transition table is:

- `FOLLOWER -- timeout_expired --> CANDIDATE / become_candidate`
- `FOLLOWER -- has_append_entries --> FOLLOWER / handle_append_entries`
- `FOLLOWER -- has_vote_request --> FOLLOWER / handle_vote_request`
- `FOLLOWER -- has_vote --> FOLLOWER / ignore_vote`
- `CANDIDATE -- timeout_expired --> FOLLOWER / back_to_follower_due_to_timeout`
- `CANDIDATE -- received_majority_votes --> LEADER / become_leader`
- `CANDIDATE -- has_append_entries --> FOLLOWER / handle_append_entries`
- `CANDIDATE -- has_vote_request --> CANDIDATE / handle_vote_request`
- `CANDIDATE -- has_vote --> CANDIDATE / handle_vote`
- `LEADER -- has_append_entries --> FOLLOWER / handle_append_entries`
- `LEADER -- has_vote_request --> LEADER / ignore_vote_request`
- `LEADER -- has_vote --> LEADER / ignore_vote`
- `LEADER -- time_for_heartbeat --> LEADER / send_heartbeat`

Event queues are drained in `latch_inputs`, and the FSM guards check whether
there are pending events of each type.

## Mapping onto `rxnet` Phases

### Latch inputs

In `latch_inputs`, the node:

- reads the current time from an abstract clock,
- drains incoming messages from the transport,
- classifies RPCs and responses into pending queues,
- detects election or heartbeat timeouts,
- accumulates pending client commands,
- computes transition flags for the FSM.

This phase also prepares output buffers and marks pending persistent writes.

### Evaluate

`rxnet` evaluates the first enabled transition according to the order in the
table above. This enforces clear semantics based on Raft events rather than
side-effect callbacks.

### Commit

The FSM publishes the new role and executes a deferred action associated with
the transition:

- `become_candidate`: increments the term, votes for itself, and sends
  `RequestVote`,
- `become_leader`: initializes replication indexes and sends an immediate
  heartbeat,
- `back_to_follower_due_to_timeout`: abandons the current candidacy and resets
  the timeout,
- `handle_append_entries`: processes the leader RPC,
- `handle_vote_request`: decides whether to grant the vote,
- `handle_vote`: accumulates votes for an active election,
- `send_heartbeat`: sends an empty `AppendEntries`,
- `ignore_vote` and `ignore_vote_request`: consume events that are not
  relevant to that role.

### Deferred actions

Deferred actions are used only for role changes and the initial sends
associated with the transition. This keeps the decision about state separate
from side effects.

### Dump outputs

In `dump_outputs`, the node:

- persists dirty metadata and log data,
- sends pending messages,
- advances leader replication,
- applies committed entries to the state machine,
- emits optional telemetry.

## Message Types

Four basic types are defined:

- `REQUEST_VOTE`
- `REQUEST_VOTE_RESPONSE`
- `APPEND_ENTRIES`
- `APPEND_ENTRIES_RESPONSE`

And one internal auxiliary type for clients:

- `CLIENT_COMMAND`

Each message contains:

- `term`
- `source`
- `target`
- RPC-specific fields

`APPEND_ENTRIES` carries zero or more entries. Zero entries is equivalent to a
heartbeat.

## Log and Commit

Each log entry contains:

- `term`
- `index`
- `leader_id`
- `command`

The leader:

- accepts client commands,
- appends them to its local log,
- replicates them with `AppendEntries`,
- updates `commit_index` when the entry is replicated by a majority in the
  current term.

Followers:

- validate `prev_log_index` and `prev_log_term`,
- correct conflicts by truncating the incompatible suffix,
- append valid new entries,
- update `commit_index` according to `leader_commit`.

### Cluster reconfiguration

Membership changes are not applied with a single entry. They are modeled as
two log entries:

- `cluster.enter_joint(C_new)`
- `cluster.leave_joint(C_new)`

The shell exposes high-level operations such as `addnode` and `rmnode`, but
internally the leader activates the `MembershipFSM`, which materializes that
sequence.

During `joint consensus`, commit calculation requires a double majority:

- majority over `old_members`
- majority over `new_members`

The transition only finishes when the leader considers the members of `C_new`
sufficiently caught up.

### Current reconfiguration scope

The current implementation covers:

- persistent representation of stable or joint configuration,
- double quorum for commit in joint mode,
- two-log-entry transition,
- operational shell support for `members`, `addnode`, and `rmnode`,
- local provisioning of new nodes in the simulation cluster.

It does not yet cover:

- `InstallSnapshot` for accelerated onboarding of lagging nodes,
- additional leader-eligibility restrictions during reconfiguration.

## Persistence

Two abstractions are used:

- `RaftStorage`: persistent protocol state.
- `StateMachineStorage`: persistence for applied application state.

On host systems, the reference persistence is implemented with per-node files:

- `meta.json` or `meta.txt`
- `log.jsonl` or `log.txt`
- `kv.json` or `kv.txt`

`meta.json` also persists:

- `old_members`
- `new_members`
- `configuration_index`
- `compaction_threshold`
- snapshot metadata

Metadata and state-machine writes use a temporary file followed by an atomic
replace. In this first version, the log may be rewritten in full to simplify
robustness and traceability.

## Transport

The transport interface offers:

- `send(message)`
- `recv_for(node_id) -> messages`
- `submit_client_command(node_id, command)`
- `recv_client_commands(node_id)`

The reference implementation is an in-memory bus, deterministic and
thread-free, useful for:

- tests,
- simulation,
- a single-process distributed-database example.

In addition to the in-memory transport, the repository includes demo
transports for multi-process execution:

- C: `raft_tcp_transport_t`, exposed in `raft/raft_tcp_transport.h`, with a
  TCP listener, persistent peer table, and `join`/`merge` protocol.
- Python: `HttpTransport` inside `python/examples/demo_app`, with REST
  endpoints for Raft messages, client commands, and membership changes.

The API leaves room for future transports over UDP, Unix sockets, or RTOS
queues.

## Clock

Time is not read directly from the system inside the core. It is abstracted
through a clock:

- Python: `Clock.now_ms()`
- C: callback or integrator-updated field

This enables deterministic tests and embedded deployments with custom timers.

## Observability

In Python, observability is implemented with an optional `TelemetrySink`. If it
is `NoOp`, there is no external dependency cost.

Events are emitted as lightweight structures and may be serialized as JSON
Lines for consumption by an external shell.

The generic Python shell:

- is an independent process,
- reads events from JSONL files,
- presents an aggregated cluster view,
- is not part of the node runtime.

In C, the available observability is optional `rxnet` tracing
(`RX_TRACE_ENABLE`) and traced examples that export `trace.bin`.

## Proposed Public API

### Python

- `raft_rx.RaftApplication`
- `raft_rx.Command`, `LogEntry`, `Message`, `MessageKind`
- `raft_rx.ManualClock`, `SystemClock`
- `raft_rx.MemoryTransport`
- `raft_rx.JsonFileStorage`
- `raft_rx.JsonlTelemetrySink`, `NullTelemetrySink`
- `raft_rx.NodeConfig`, `RaftNode`, `Role`
- `raft_rx.RaftCluster`
- `raft_rx.RaftShell`

### C

- `include/raft/raft.h`
- `include/raft/raft_kv_app.h`
- `include/raft/raft_tcp_transport.h`

`libraft_rx.a` contains the core, in-memory transport, file persistence, TCP
transport, and sample KV state machine.

## Deliberate Differences Between C and Python

Python prioritizes:

- ergonomics,
- simulation,
- external shell,
- easy state inspection.

C prioritizes:

- explicit API,
- portability to constrained environments,
- reasonable fixed buffers and capacities,
- no additional dependencies.

Protocol semantics must remain aligned, even when the internal representation
is not identical.
