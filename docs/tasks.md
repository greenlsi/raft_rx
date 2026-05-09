# Tasks

## Documentation

- [x] Write system requirements.
- [x] Write the layered design and mapping onto `rxnet`.
- [x] Define the scope of the first production-ready version.

## Project Structure

- [x] Create the Python code tree.
- [x] Create the C code tree.
- [x] Add the main README and build/test commands.

## Conceptually Shared Raft Core

- [x] Define message types and log entries.
- [x] Define term, vote, and commit semantics.
- [x] Implement log validation rules.
- [x] Implement recovery from persistent storage.

## Python

- [x] Create the `raft_rx` package.
- [x] Implement a deterministic clock for tests.
- [x] Implement the in-memory transport.
- [x] Implement file-based persistence.
- [x] Implement no-op and JSONL `TelemetrySink`.
- [x] Implement the persistent key-value state machine.
- [x] Implement the Raft node over `rxnet.fsm`.
- [x] Implement a simulation cluster.
- [x] Create an executable distributed-database example.
- [x] Create an external shell for cluster operation and inspection.
- [x] Add tests for election, replication, and recovery.
- [x] Implement stable/joint cluster configuration and the secondary membership
  FSM.
- [x] Expose reconfiguration in the shell with `members`, `addnode`, and
  `rmnode`.
- [x] Add an independent demo with HTTP transport, local CLI, `join`, and
  `merge`.

## C

- [x] Create the `raft` library.
- [x] Implement public types and limits.
- [x] Implement the in-memory transport.
- [x] Implement reference file-based persistence.
- [x] Add optional tracing through `rxnet` (`RX_TRACE_ENABLE`) and an
  exportable example.
- [x] Implement the persistent key-value state machine.
- [x] Implement the Raft node over `rxnet/fsm.h`.
- [x] Create a 3-node cluster example.
- [x] Add tests for election, replication, and restart.
- [x] Port `joint consensus` reconfiguration to the C library.
- [x] Add an independent demo with TCP transport, local CLI, `join`, and
  `merge`.

## Verification

- [x] Run Python tests.
- [x] Compile C.
- [x] Run C tests.
- [x] Verify the key-value example in both languages.
- [x] Update the documentation with the final state of the deliverable.
