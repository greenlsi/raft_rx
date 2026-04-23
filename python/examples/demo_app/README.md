# raft-rx-demo

Standalone demo application that uses `raft-rx` as an external dependency.

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

