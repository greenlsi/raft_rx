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

## Test

From `python/`:

```bash
uv run --project examples/demo_app --extra dev pytest -q
```
