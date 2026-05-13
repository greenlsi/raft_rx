# Raft counter example

This is a deliberately tiny replicated counter built with Raft and the TCP
transport.

Each process starts as a one-node cluster. To build a larger cluster, start the
nodes first and then use `join HOST:PORT` from the CLI of each node that should
join an existing cluster.

## Build

```bash
make -C c/examples/counter
```

## Start three nodes

Open three terminals:

```bash
# Terminal 1
./build/counter_node --id n1 --port 5001

# Terminal 2
./build/counter_node --id n2 --port 5002

# Terminal 3
./build/counter_node --id n3 --port 5003
```

Then join `n2` and `n3` to `n1`'s cluster:

```text
n2> join 127.0.0.1:5001
n3> join 127.0.0.1:5001
```

## CLI

| Command | Description |
|---------|-------------|
| `inc` | Synchronized operation: increment the counter by 1 |
| `reset` | Synchronized operation: set the counter to 0 |
| `get` | Local, unsynchronized read |
| `join HOST:PORT` | Join the cluster reachable through that node |
| `leader` | Show the known leader |
| `status` | Show local Raft state |
| `quit` | Exit the process |

## Important design bug

This example intentionally contains a serious application-level design bug:
`inc` is not idempotent.

Raft replicates log entries, but it does not automatically make client
operations safe to retry. If a client sends `inc`, loses the response, and then
tries `inc` again, the cluster may apply two different committed increments.
The user-visible command did not change, but the state did.

This is exactly the kind of bug that can hide behind a correct consensus
implementation: the replicated state machine still needs a client operation
protocol that defines what "the same request" means.

Exercise: how would you fix the application so that retrying `inc` is safe,
without changing the user interface at all?

Constraints:

- Keep the CLI commands exactly as they are: `inc`, `reset`, and `get`.
- Do not ask the user to type request IDs, sequence numbers, or tokens.
- Preserve `get` as a local, unsynchronized read.
- Fix the application/protocol boundary so the system can recognize duplicate
  client requests internally.
