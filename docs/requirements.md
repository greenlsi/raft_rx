# Requirements

## Goal

Build a Raft library on top of `../rxnet` in C and Python, aimed at:

- distributed embedded systems with dependency constraints,
- terminal applications on macOS and Linux,
- optional integration with external observability tools,
- deterministic execution based on ticks and state machines.

The result must include a working example of a persistent distributed
key-value database.

## Functional Scope

The library must implement the core Raft algorithm:

- leader election,
- heartbeats and leadership maintenance,
- log replication,
- majority-based commit,
- ordered application of entries to a state machine,
- persistence of `current_term`, `voted_for`, and the log,
- recovery after restart from persistent storage,
- explicit redirection or rejection of client requests when the node is not
  the leader.

## Architecture Requirements

### R1. Reactive model over `rxnet`

Each Raft node must be modeled as an `rxnet` state machine with at least these
roles:

- `FOLLOWER`
- `CANDIDATE`
- `LEADER`

Role transitions must be governed by events latched in the current tick:

- election timeout expiration,
- heartbeat or `AppendEntries` reception,
- vote reception,
- discovery of a higher term.

### R2. Layer separation

The solution must clearly separate:

- the Raft protocol core,
- transport,
- persistence,
- application state machine,
- observability.

The core must not depend on a concrete network, shell, or UI implementation.

### R3. C and Python compatibility

There must be a C implementation and a Python implementation with aligned
semantics for:

- message types,
- term and vote transition rules,
- log validation rules,
- commit semantics,
- transport interface,
- persistence interface,
- application state-machine integration API.

Sharing binaries or an ABI is not required, but the conceptual model and
observable behavior must match.

## Transport Requirements

### R4. Abstract transport

The library must define an abstract transport interface capable of:

- sending Raft messages to a peer,
- receiving all pending messages for a node in one tick,
- injecting client requests,
- avoiding dynamic allocation on critical C runtime paths whenever possible.

### R5. Reference transport

At least one deterministic and portable reference transport must be included:

- an in-memory transport for simulation, tests, and local examples.

In addition, the independent-process demos must demonstrate that the core does
not depend on the in-memory bus:

- C: pluggable TCP transport with a persistent peer table.
- Python: HTTP transport in the external demo, implemented with the standard
  library.

Host transports must be addable without modifying the Raft core.

## Persistence Requirements

### R6. Minimum persistent state

Each node must durably persist:

- `current_term`,
- `voted_for`,
- the Raft log,
- the applied state of the key-value example state machine.

### R7. Safe writes

Host persistence must use reasonable atomic or near-atomic writes:

- temporary file plus `rename`/`replace`,
- readable and auditable format,
- robust recovery after clean restarts.

For embedded targets, the API must allow this persistence layer to be replaced
with one adapted to flash, NVRAM, or platform-specific storage.

## Application State-Machine Requirements

### R8. State-machine API

There must be an application interface with these capabilities:

- validate or accept serialized commands,
- apply committed entries in order,
- query client-visible state,
- load and save persistent state.

### R9. Key-value example

A sample state machine must be delivered with these operations:

- `set(key, value)`
- `delete(key)`
- `get(key)`

`get` may be resolved locally against the applied state. `set` and `delete`
must enter the leader's log.

## Observability Requirements

### R10. Optional and external observability

Observability must be fully optional. When it is not enabled, the Raft core
must not depend on shell, TUI, or visual libraries.

### R11. Structured events

When enabled, each node must be able to emit external structured events for at
least:

- role changes,
- term changes,
- votes sent or received,
- RPC reception and transmission,
- entry append and commit,
- command application to the state machine,
- persistence or transport errors.

### R12. External shell

A basic, comfortable, extensible external shell must be included for Python.
Its minimum responsibilities are:

- display nodes, roles, and terms,
- show log length and `commit_index`,
- show recent events,
- query the key-value state of the example cluster.

The independent-process demos may include a local CLI as an FSM in the same
runtime, as long as the core remains separate from that UI.

## Non-Functional Requirements

### R13. Portability

The C code must compile at least on:

- macOS,
- Linux.

The architecture must remain suitable for embedded-platform ports.

### R14. Dependencies

- C: no mandatory external dependencies beyond `rxnet` and the standard C
  library.
- Python: only the standard library plus local `rxnet`.
- The external shell must not introduce dependencies into the embedded runtime.

### R15. Determinism and testability

The implementation must be executable deterministically in tests:

- controllable clock,
- in-memory transport,
- explicit timeout injection,
- cluster and log state verification.

### R16. Production quality

The deliverable must include:

- requirements, design, and task documentation,
- automated tests in C and Python,
- executable examples,
- explicit error handling,
- clear public API.

## Acceptance Criteria

The work is considered complete when:

1. `docs/requirements.md`, `docs/design.md`, and `docs/tasks.md` describe the
   system coherently.
2. A usable and tested Python library exists.
3. A usable and compilable C library exists.
4. A 3-node in-memory cluster elects a leader and replicates key-value
   operations.
5. State persists and can be recovered after restart.
6. Observability can be enabled or disabled without affecting the core.
7. Independent C and Python demos start nodes in separate processes and allow
   membership reconfiguration.
