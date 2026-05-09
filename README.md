# raft_rx

`raft_rx` is a Raft consensus implementation built on top of `rxnet`, with
parallel C and Python implementations.

The project provides:

- a Raft core modeled as `rxnet` finite state machines,
- leader election, heartbeats, log replication, commit, and recovery,
- persistent term, vote, log, snapshot, and membership configuration,
- stable and joint-consensus cluster membership changes,
- pluggable transports,
- a replicated key-value example,
- standalone multi-process demo applications in C and Python.

The C and Python implementations are intentionally not ABI-compatible. They
share the same protocol model, public concepts, and observable behavior while
using idioms appropriate for each language.

## Repository Layout

| Path | Purpose |
|---|---|
| `c/` | C library, tests, local examples, and TCP demo application |
| `python/` | Python package, tests, local examples, shell, and HTTP demo application |
| `docs/` | Requirements, design notes, tasks, and user guides |
| `rxnet/` | Expected sibling dependency: `../rxnet` |

## Documentation

- [C user guide](docs/user-c.md): C integration, storage, transports, event
  loops, membership changes, and the C demo application.
- [Python user guide](docs/user-python.md): Python integration, simulation
  clusters, shell, observability, HTTP demo application, and API reference.
- [Requirements](docs/requirements.md)
- [Design](docs/design.md)
- [Tasks](docs/tasks.md)

PDF versions are generated under `docs/` by the documentation Makefile.

```bash
make -C docs
```

## Prerequisites

The repository expects `rxnet` to be checked out next to it:

```text
parent/
├── raft_rx/
└── rxnet/
```

For Python workflows, use `uv`. For C workflows, use a C99 compiler and
`make`.

## Build and Test

Run the full project test suite from the repository root:

```bash
make test
```

That command runs the Python tests and then the C tests.

### Python

Run the Python library tests:

```bash
uv run --project python --extra dev pytest -q
```

Build the Python package:

```bash
uv build python
```

### C

Build the C library, trace variant, tests, and examples:

```bash
make -C c
```

Run the C tests:

```bash
make -C c test
```

Build only the standard C library artifact:

```bash
make -C c build/libraft_rx.a
```

Build the trace-enabled variant:

```bash
make -C c build/libraft_rx_trace.a
```

## Local Examples

These examples run multiple Raft nodes inside one process using deterministic
or local transports. They are useful for quick verification and for studying
the core APIs without operating several processes.

### Python Key-Value Cluster

```bash
uv run --project python python/examples/kv_cluster.py
```

Interactive key-value shell:

```bash
uv run --project python python/examples/kv_shell.py
```

Generic cluster shell:

```bash
uv run --project python python/tools/raftsh.py
```

Trace-producing reconfiguration example:

```bash
uv run --project python python/examples/kv_reconfigure_trace.py
```

This writes:

- `python/var/python-reconfigure-trace/trace.bin`
- `python/var/python-reconfigure-trace/trace.html`

### C Key-Value Cluster

```bash
make -C c build/raft_kv_cluster
./c/build/raft_kv_cluster
```

Trace-producing C example:

```bash
make -C c build/raft_kv_cluster_trace
./c/build/raft_kv_cluster_trace
```

This writes:

- `c/var/c-trace-example/trace.bin`

## Standalone Demo Applications

The standalone demos run one Raft node per process and demonstrate how the core
library can be embedded in an application with networking, a local CLI,
persistence, and dynamic membership.

### Python HTTP Demo

The Python demo lives in `python/examples/demo_app`. It uses HTTP as the Raft
transport and exposes both an interactive CLI and REST endpoints. Each node ID
is its advertised `HOST:PORT`.

Build/package the demo:

```bash
uv build python/examples/demo_app
```

Run its tests:

```bash
uv run --project python/examples/demo_app --extra dev pytest -q
```

Start a first node:

```bash
uv run --project python/examples/demo_app raft-rx-demo --bind 127.0.0.1:7400
```

Start additional nodes in separate terminals:

```bash
uv run --project python/examples/demo_app raft-rx-demo \
  --join 127.0.0.1:7400 \
  --bind 127.0.0.1:7401

uv run --project python/examples/demo_app raft-rx-demo \
  --join 127.0.0.1:7400 \
  --bind 127.0.0.1:7402
```

Useful CLI commands inside a node are `status`, `members`, `log [LIMIT]`,
`set KEY VALUE`, `get KEY`, `delete KEY`, `addnode HOST:PORT`,
`rmnode HOST:PORT`, `join HOST:PORT`, `merge HOST:PORT`, `stop`, `start`, and
`quit`.

### C TCP Demo

The C demo lives in `c/examples/demo_app`. It uses the pluggable TCP transport,
a replicated key-value state machine, and a local CLI running in the same
`rxnet` runtime as the Raft node.

Build the demo:

```bash
make -C c/examples/demo_app
```

Start a fresh 3-node cluster in three terminals:

```bash
./c/examples/demo_app/build/raft_node --id n1 --port 5001 \
  --member n1 --member n2 --member n3 \
  --peer n2=127.0.0.1:5002 --peer n3=127.0.0.1:5003
```

```bash
./c/examples/demo_app/build/raft_node --id n2 --port 5002 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n3=127.0.0.1:5003
```

```bash
./c/examples/demo_app/build/raft_node --id n3 --port 5003 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n2=127.0.0.1:5002
```

Add a fourth node from another terminal:

```bash
./c/examples/demo_app/build/raft_node --id n4 --port 5004 --join 127.0.0.1:5001
```

Useful CLI commands inside a node are `status`, `leader`, `members`, `log`,
`set KEY VALUE`, `get KEY`, `delete KEY`, `addnode ID`, `rmnode ID`, `leave`,
`join HOST:PORT`, `merge HOST:PORT`, `stop`, `start`, and `quit`.

After the first run, node state is stored under `var/raft/<id>/`. A node can be
restarted with only its ID:

```bash
./c/examples/demo_app/build/raft_node --id n1
```

## License

This project is distributed under the GNU General Public License v3.0. See
[LICENSE](LICENSE) for the full text.
