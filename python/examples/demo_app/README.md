# raft-rx-demo

Standalone demo application that uses `raft-rx` as an external dependency.

## Structure

| Module | Responsibility |
|--------|----------------|
| `demo.py` | Process wiring: Raft node, HTTP server, runtime, REST API, and entry point |
| `app.py` | Replicated key-value application |
| `transport.py` | HTTP Raft transport and REST client helpers |
| `cli.py` | Interactive rxnet CLI FSM |

## Run

From `python/`:

```bash
uv run --project examples/demo_app raft-rx-demo --bind 127.0.0.1:7400
uv run --project examples/demo_app raft-rx-demo --join 127.0.0.1:7400 --bind 127.0.0.1:7402
```

Each process uses its bind address as the Raft node ID. By default, persistent
state is stored under `var/demo/<host>_<port>`; override it with `--data-dir`.
The cooperative runtime ticks every 25 ms by default and can be adjusted with
`--tick-ms`.

## CLI

The process starts a local rxnet CLI:

| Command | Description |
|---------|-------------|
| `status` | Local node status, role, term, log and commit index |
| `members` | Current stable or joint membership configuration |
| `log [LIMIT]` | Local Raft log, optionally limited to the last entries |
| `set KEY VALUE` | Replicated write, queued locally or forwarded to the leader |
| `get KEY` | Local read from the demo KV state |
| `delete KEY` | Replicated delete |
| `addnode HOST:PORT` | Add a node ID/address to membership |
| `rmnode HOST:PORT` | Remove a node ID/address from membership |
| `join HOST:PORT` | Join the cluster reachable at that address |
| `merge HOST:PORT` | Merge this cluster with the target cluster |
| `stop` / `start` | Stop or resume the local Raft node |
| `quit` | Stop the process |

## REST API

The demo also exposes HTTP endpoints on `--bind`:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/status` | GET | Local node summary |
| `/cluster/config` | GET | Membership configuration |
| `/log?limit=N` | GET | Local log entries |
| `/kv/<KEY>` | GET | Local KV read |
| `/kv/set` | POST | Replicated `set`; body `{"key": "...", "value": "..."}` |
| `/kv/delete` | POST | Replicated delete; body `{"key": "..."}` |
| `/cluster/join` | POST | Add the caller node to this cluster |
| `/cluster/merge` | POST | Merge membership with another cluster |

## Test

From `python/`:

```bash
uv run --project examples/demo_app --extra dev pytest -q
```
