# raft_rx - Python User Guide

---

# Part I - The Library

---

## 1. Overview

`raft_rx` is a Python package that implements the Raft consensus algorithm on
top of the `rxnet` FSM runtime. It provides:

- **Replicated state machine** - commands are appended to a distributed log,
  committed by majority quorum, and applied to your application in order.
- **Leader election** - deterministic tests via `ManualClock`, or wall-clock
  operation via `SystemClock`.
- **Membership changes** - joint-consensus reconfiguration through
  `request_membership_change`.
- **Persistent storage** - term, vote, log, snapshot and membership
  configuration are stored as JSON files.
- **Pluggable transport** - the package includes deterministic in-memory
  transport; the standalone demo shows an HTTP transport.
- **Pluggable application** - implement a small protocol to attach your own
  state machine.
- **Optional observability** - JSONL telemetry and `rxnet` trace export can be
  enabled per cluster.

---

## 2. Architecture

```mermaid
graph TD
    APP["Your application<br/>RaftApplication<br/>apply / reload / snapshot / restore_snapshot"]
    RAFT["raft_rx consensus engine<br/>RaftCluster / RaftNode<br/>election / replication / membership"]
    TRANSPORT["Transport<br/>MemoryTransport / custom<br/>send / recv_for / client commands"]
    STORAGE["Storage<br/>JsonFileStorage<br/>meta.json / log.json / snapshot.json"]
    RUNTIME["rxnet runtime<br/>fsm.Runtime / CoopExecutive<br/>drives node FSMs"]
    TELEMETRY["Optional telemetry<br/>JsonlTelemetrySink / rxnet Tracer"]

    APP --> RAFT
    RAFT --> TRANSPORT
    RAFT --> STORAGE
    RAFT --> RUNTIME
    RAFT --> TELEMETRY
```

| Layer | Type | Responsibility |
|---|---|---|
| Application | `RaftApplication` | Your state machine: what commands mean |
| Consensus engine | `RaftCluster` / `RaftNode` | Raft protocol, log replication and membership |
| Transport | `MemoryTransport` or compatible object | Deliver Raft messages and client commands |
| Storage | `JsonFileStorage` | Durable protocol state and snapshots |
| Runtime | `rxnet.fsm.Runtime` | Ticks the Raft FSMs |
| Telemetry | `JsonlTelemetrySink` / `NullTelemetrySink` | Optional structured events |

---

## 3. Key Concepts

### Roles

Every running node is in one of three roles:

| Role | Description |
|---|---|
| `FOLLOWER` | Accepts log entries and votes in elections |
| `CANDIDATE` | Requests votes after its election timeout expires |
| `LEADER` | Accepts client commands, replicates and commits them |

### Log, Terms and Commit

- Each client command becomes a `LogEntry(index, term, command, leader_id)`.
- The leader replicates entries to a quorum, then advances `commit_index`.
- Committed entries are applied through your application's `apply` method.
- `term` is a monotonically increasing election epoch.

### Membership Configuration

Membership is represented by `ClusterConfiguration`:

- `old_members`: current stable set, or leaving set during joint consensus.
- `new_members`: joining set during joint consensus, otherwise `None`.
- `index`: log index where the configuration was written.

`mode()` returns `"stable"` or `"joint"`. During joint consensus, commit
requires a majority in both `old_members` and `new_members`.

---

## 4. Implementing an Application

Your application must implement the `RaftApplication` protocol:

```python
from raft_rx import Command


class RaftApplication:
    def apply(self, command: Command) -> None: ...
    def reload(self) -> None: ...
    def snapshot(self) -> object: ...
    def restore_snapshot(self, snapshot: object) -> None: ...
```

Commands are simple dataclasses:

```python
from raft_rx import Command

Command(op="set", key="color", value="blue")
Command(op="delete", key="color")
```

| Method | When it is called | What it should do |
|---|---|---|
| `apply` | Once per committed log entry, in index order | Mutate application state |
| `reload` | Restart with no snapshot | Reset or reload the empty/base state |
| `snapshot` | Log compaction | Return a JSON-serializable snapshot object |
| `restore_snapshot` | Restart from snapshot | Restore state from the snapshot object |

Correctness rule: mutate replicated state only from `apply`. Reads can inspect
the local applied state, but writes must enter the leader's log.

### Example: Key-Value Application

```python
import json
from pathlib import Path

from raft_rx import Command


class KVApp:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, str] = {}
        self.reload()

    def reload(self) -> None:
        if self.path.exists():
            self.data = {
                str(k): str(v)
                for k, v in json.loads(self.path.read_text()).items()
            }
        else:
            self.data = {}

    def apply(self, command: Command) -> None:
        if command.op == "set":
            if command.value is None:
                raise ValueError("set requires value")
            self.data[command.key] = command.value
        elif command.op == "delete":
            self.data.pop(command.key, None)
        else:
            raise ValueError(f"unsupported op: {command.op}")
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def snapshot(self) -> object:
        return dict(self.data)

    def restore_snapshot(self, snapshot: object) -> None:
        self.data = {str(k): str(v) for k, v in dict(snapshot).items()}
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))
```

The repository version in `python/examples/kv_app.py` also writes via a
temporary file and atomic replace.

---

## 5. Building a Cluster

Use `RaftCluster` for local deterministic simulations and tests.

```python
from pathlib import Path

from raft_rx import ManualClock, NodeConfig, RaftCluster
from raft_rx.cluster import ClusterNodePaths


root = Path("var/my-cluster")
cluster = RaftCluster(clock=ManualClock())
node_ids = ["n1", "n2", "n3"]

for i, node_id in enumerate(node_ids):
    peers = [peer for peer in node_ids if peer != node_id]
    cluster.add_node(
        NodeConfig(
            node_id=node_id,
            peers=peers,
            election_timeout_ms=150 + i * 100,
            heartbeat_interval_ms=50,
        ),
        ClusterNodePaths(
            storage_dir=root / "storage",
            telemetry_path=root / "telemetry" / f"{node_id}.jsonl",
        ),
        application=KVApp(root / "kv" / f"{node_id}.json"),
    )
```

`NodeConfig.initial_members` is optional. If omitted, the node starts with
`[node_id, *peers]`. Use an explicit list when you need all nodes to share a
particular initial ordering.

### Running Ticks

With `ManualClock`, each tick advances time explicitly:

```python
cluster.run(40, advance_ms=10)
leader = cluster.leader()
```

With `SystemClock`, use an `rxnet` executive, as shown by the standalone demo:

```python
from rxnet.coop import CoopExecutive

executive = CoopExecutive()
executive.add(cluster.runtime)
executive.run()
```

---

## 6. Submitting Commands

Commands must reach the leader to be committed.

```python
from raft_rx import Command

leader = cluster.leader()
if leader is None:
    raise RuntimeError("no leader")

leader.submit_command(Command(op="set", key="color", value="blue"))
cluster.run(30, advance_ms=10)
```

`RaftCluster.submit_to_leader` is a convenience wrapper:

```python
ok = cluster.submit_to_leader(Command(op="delete", key="color"))
```

If a command is submitted to a follower in the core in-memory transport, that
follower drains and rejects it. Production-style forwarding belongs in the
application or transport layer; the standalone HTTP demo implements that.

---

## 7. Membership Changes

Call `request_membership_change` on the current leader with the full target
member list, not a delta.

```python
leader = cluster.leader()
if leader is None:
    raise RuntimeError("no leader")

cluster.provision_node("n4", members=["n1", "n2", "n3", "n4"], running=True)
leader.request_membership_change(["n1", "n2", "n3", "n4"])
cluster.run(120, advance_ms=10)
```

To remove a node, omit it from the target list:

```python
leader.request_membership_change(["n1", "n2", "n3"])
```

Internally the membership FSM writes:

1. `cluster.enter_joint`
2. `cluster.leave_joint`

During the joint phase, quorum requires a majority from both old and new
configurations.

---

## 8. Storage Format

`JsonFileStorage(root, node_id)` stores files under `{root}/{node_id}/`:

```text
var/my-cluster/storage/
└── n1/
    ├── meta.json
    ├── log.json
    └── snapshot.json
```

### `meta.json`

Contains protocol metadata:

```json
{
  "commit_index": 2,
  "compaction_threshold": 32,
  "configuration_index": 0,
  "current_term": 1,
  "new_members": null,
  "old_members": ["n1", "n2", "n3"],
  "snapshot_last_included_index": 0,
  "snapshot_last_included_term": 0,
  "voted_for": "n1"
}
```

### `log.json`

Contains un-compacted log entries:

```json
[
  {
    "index": 1,
    "term": 1,
    "leader_id": "n1",
    "command": {"op": "set", "key": "color", "value": "blue"}
  }
]
```

### `snapshot.json`

Contains compaction metadata plus your application snapshot:

```json
{
  "application": {"color": "blue"},
  "last_included_index": 32,
  "last_included_term": 4
}
```

`JsonFileStorage` writes JSON via temp file plus replace.

---

## 9. Observability and Tracing

### JSONL Telemetry

Pass a telemetry path when adding each node:

```python
ClusterNodePaths(
    storage_dir=root / "storage",
    telemetry_path=root / "telemetry" / f"{node_id}.jsonl",
)
```

Each line is a JSON object with time, role, term, event and payload. The shell
command `events [NODE ...]` displays the last events.

### `rxnet` Trace

Enable tracing on the cluster:

```python
cluster = RaftCluster(clock=ManualClock(), trace_enabled=True)
```

Export the trace:

```python
cluster.export_trace(root / "trace.bin")
cluster.report_trace(root / "trace.html")
```

The example `python/examples/kv_reconfigure_trace.py` generates:

- `python/var/python-reconfigure-trace/trace.bin`
- `python/var/python-reconfigure-trace/trace.html`

---

## 10. Shell

Run the generic shell:

```bash
uv run --project python python/tools/raftsh.py
```

Run the KV shell:

```bash
uv run --project python python/examples/kv_shell.py
```

### Generic Commands

| Command | Description |
|---|---|
| `status` | Cluster status and per-node summary |
| `leader` | Current leader |
| `tick [N] [MS]` | Advance the simulation |
| `autotick [MS]` | Configure background ticking; `0` disables it |
| `stop NODE` | Stop a node |
| `start NODE` | Start a node |
| `restart NODE` | Stop and start a node |
| `members [NODE]` | Show stable or joint membership |
| `addnode NODE` | Provision and add a node via joint consensus |
| `rmnode NODE` | Remove a node via joint consensus |
| `log NODE` | Print local log entries and commit status |
| `maxlog [ENTRIES]` | Read or update compaction threshold |
| `dump` | Print summaries as JSON |
| `trace [PATH]` | Export `trace.bin` if tracing is enabled |
| `trace_report [PATH]` | Export an HTML trace report |
| `events [NODE ...]` | Show recent JSONL telemetry |
| `quit` / `exit` | Leave the shell |

### KV Commands

The KV shell registers additional commands:

| Command | Description |
|---|---|
| `set KEY VALUE` | Submit a replicated write to the leader |
| `get KEY [NODE]` | Read local applied state from a node |
| `delete KEY` | Submit a replicated delete to the leader |

---

## 11. Standalone HTTP Demo

`python/examples/demo_app` is a separate application that consumes `raft-rx` as
an external dependency. Each process is one Raft node and uses `HOST:PORT` as
its node ID.

### Run

From `python/`:

```bash
uv run --project examples/demo_app raft-rx-demo --bind 127.0.0.1:7400
uv run --project examples/demo_app raft-rx-demo --join 127.0.0.1:7400 --bind 127.0.0.1:7402
```

Useful flags:

| Flag | Description |
|---|---|
| `--bind HOST:PORT` | Address to bind and advertise as node ID |
| `--join HOST:PORT` | Existing node to join at startup |
| `--data-dir DIR` | Persistent state directory |
| `--tick-ms MS` | Runtime tick period; default `25` |

### CLI Commands

| Command | Description |
|---|---|
| `status` | Local node status |
| `members` | Membership configuration |
| `log [LIMIT]` | Local log entries |
| `set KEY VALUE` | Replicated write; forwarded to the leader if needed |
| `get KEY` | Local read |
| `delete KEY` | Replicated delete |
| `addnode HOST:PORT` | Add a node ID/address to membership |
| `rmnode HOST:PORT` | Remove a node ID/address from membership |
| `join HOST:PORT` | Join the target cluster |
| `merge HOST:PORT` | Merge this cluster with another cluster |
| `stop` / `start` | Stop or resume local Raft processing |
| `quit` | Stop the process |

### REST API

| Endpoint | Method | Purpose |
|---|---|---|
| `/status` | GET | Local node summary |
| `/cluster/config` | GET | Membership configuration |
| `/log?limit=N` | GET | Local log entries |
| `/kv/<KEY>` | GET | Local KV read |
| `/kv/set` | POST | Replicated `set`; body `{"key": "...", "value": "..."}` |
| `/kv/delete` | POST | Replicated delete; body `{"key": "..."}` |
| `/raft/message` | POST | Incoming Raft protocol message |
| `/raft/client-command` | POST | Forwarded client command |
| `/cluster/add-node` | POST | Add a node; body `{"node": "HOST:PORT"}` |
| `/cluster/remove-node` | POST | Remove a node; body `{"node": "HOST:PORT"}` |
| `/cluster/join` | POST | Add caller node to the cluster |
| `/cluster/membership-request` | POST | Request a target member list |
| `/cluster/merge` | POST | Merge membership with another cluster |
| `/node/stop` | POST | Stop local Raft processing |
| `/node/start` | POST | Resume local Raft processing |

---

## 12. Public API Reference

### Package Exports

```python
from raft_rx import (
    ClusterConfiguration,
    Command,
    JsonFileStorage,
    JsonlTelemetrySink,
    LogEntry,
    ManualClock,
    MemoryTransport,
    Message,
    MessageKind,
    NodeConfig,
    NullTelemetrySink,
    RaftApplication,
    RaftCluster,
    RaftNode,
    RaftShell,
    Role,
    SystemClock,
)
```

### `NodeConfig`

| Field | Type | Description |
|---|---|---|
| `node_id` | `str` | Unique node identifier |
| `peers` | `list[str]` | Other nodes the node can talk to |
| `election_timeout_ms` | `int` | Base election timeout |
| `heartbeat_interval_ms` | `int` | Leader heartbeat interval; default `50` |
| `initial_members` | `list[str] \| None` | Explicit initial membership; defaults to node plus peers |

### `RaftCluster`

| Method | Description |
|---|---|
| `add_node(config, paths, application, period_us=0)` | Add a node and register its FSMs |
| `provision_node(node_id, members, running=False, election_timeout_ms=450)` | Create a node during reconfiguration |
| `tick(advance_ms=10)` | Advance one runtime tick |
| `run(ticks, advance_ms=10)` | Advance several ticks |
| `leader()` | Return the current leader or `None` |
| `submit_to_leader(command)` | Submit to the current leader if one exists |
| `summaries()` | Return per-node status dictionaries |
| `stop_node(node_id)` / `start_node(node_id)` / `restart_node(node_id)` | Lifecycle helpers |
| `export_trace(path)` / `report_trace(path)` / `serve_trace(...)` | Trace helpers |

### `RaftNode`

| Method / property | Description |
|---|---|
| `role` | Current `Role` |
| `submit_command(command)` | Enqueue a client command for this node |
| `request_membership_change(members)` | Request full target membership |
| `summary()` | Return a status dictionary |
| `last_committed_command()` | Return the last applied command if available |
| `stop()` / `start()` | Local node lifecycle |

---

## 13. Building and Testing

Run the library tests:

```bash
uv run --project python --extra dev pytest -q
```

Run the standalone demo tests:

```bash
uv run --project python/examples/demo_app --extra dev pytest -q
```

Build the standalone demo package:

```bash
cd python/examples/demo_app
uv build
```

---

## 14. Troubleshooting

### No leader is elected

- Ensure every node has the same initial membership.
- Use staggered election timeouts in deterministic examples.
- Advance enough simulated time with `cluster.run(...)`.
- Check whether nodes were stopped through `stop_node` or `node.stop()`.

### Commands are not committed

- Submit writes to the leader, or use a layer that forwards to the leader.
- Run enough ticks after submission.
- Check `status`, `leader`, and `log NODE` in the shell.

### A joining node does not participate

- In in-memory clusters, call `cluster.provision_node(...)` before requesting
  the membership change.
- Pass the full target member list to `request_membership_change`.
- Run until the cluster returns to stable membership.

### Snapshot restore misses state

- Ensure `snapshot()` returns the complete application state.
- Ensure the returned object is JSON-serializable when using `JsonFileStorage`.
- Keep all state mutation inside `apply` and `restore_snapshot`.
