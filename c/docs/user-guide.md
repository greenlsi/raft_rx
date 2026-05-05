# raft_rx C — User Guide

This guide explains how to use the `raft_rx` C library to build a replicated,
fault-tolerant application.  It covers the architecture, the integration API,
the TCP transport, cluster lifecycle, and all configuration knobs.

---

## Table of contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Key concepts](#3-key-concepts)
4. [Quick start — the demo app](#4-quick-start--the-demo-app)
5. [Integrating raft_rx into your application](#5-integrating-raft_rx-into-your-application)
   - 5.1 [Implement `raft_application_t`](#51-implement-raft_application_t)
   - 5.2 [Bootstrap the cluster](#52-bootstrap-the-cluster)
   - 5.3 [Attach the transport](#53-attach-the-transport)
   - 5.4 [Run the event loop](#54-run-the-event-loop)
   - 5.5 [Submit commands](#55-submit-commands)
   - 5.6 [Complete minimal example](#56-complete-minimal-example)
6. [TCP transport](#6-tcp-transport)
   - 6.1 [Initialization](#61-initialization)
   - 6.2 [Peer persistence](#62-peer-persistence)
   - 6.3 [Self persistence](#63-self-persistence)
   - 6.4 [The listener thread](#64-the-listener-thread)
7. [Cluster lifecycle](#7-cluster-lifecycle)
   - 7.1 [Bootstrap a fresh cluster](#71-bootstrap-a-fresh-cluster)
   - 7.2 [Restarting a node](#72-restarting-a-node)
   - 7.3 [Adding a node (`--join`)](#73-adding-a-node---join)
   - 7.4 [Removing a node](#74-removing-a-node)
8. [Membership changes internals](#8-membership-changes-internals)
9. [Persistence and storage format](#9-persistence-and-storage-format)
10. [Configuration reference](#10-configuration-reference)
11. [Limits reference](#11-limits-reference)
12. [Building](#12-building)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Overview

`raft_rx` is a C implementation of the
[Raft consensus algorithm](https://raft.github.io/) built on top of the
`rxnet` reactive-synchronous runtime.  It provides:

- **Replicated state machine** — commands are appended to a distributed log,
  committed by majority, and applied to your application in order.
- **Leader election** — automatic election with randomised timeouts; the
  cluster self-heals after a leader failure.
- **Membership changes** — safe joint-consensus reconfiguration; nodes can be
  added or removed without stopping the cluster.
- **Persistent storage** — term, log, snapshot, and membership configuration
  survive process restarts.
- **Pluggable transport** — swap the default in-memory transport for TCP (or
  any other medium) by implementing five function pointers.
- **Pluggable application** — implement four callbacks to connect your state
  machine; the built-in key-value store is one example.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Your application                       │
│   raft_application_t  { apply, reload, snapshot, ... }  │
└────────────────────────┬────────────────────────────────┘
                         │ committed commands
┌────────────────────────▼────────────────────────────────┐
│               raft_rx consensus engine                   │
│  raft_cluster_t  ╌╌  raft_node_t  (rx_fsm_machine)      │
│  leader election · log replication · membership changes  │
└──────────┬─────────────────────────────┬────────────────┘
           │ raft_transport_t            │ raft_file_storage_t
┌──────────▼───────────┐    ┌───────────▼──────────────────┐
│   Network transport  │    │   Disk storage               │
│  raft_tcp_transport  │    │  meta.txt  log.txt           │
│  (or memory / BLE …) │    │  snapshot.bin  peers.txt     │
└──────────────────────┘    └──────────────────────────────┘
           │
┌──────────▼──────────────────┐
│   rxnet cooperative runtime │
│   rx_coop_exec              │
└─────────────────────────────┘
```

### Layer responsibilities

| Layer | Type | Responsibility |
|---|---|---|
| Application | `raft_application_t` | Your state machine — what commands *mean* |
| Consensus engine | `raft_cluster_t` / `raft_node_t` | Raft protocol, leader election, log replication |
| Transport | `raft_transport_t` | Send/receive messages between nodes |
| Storage | `raft_file_storage_t` | Durable persistence of Raft state |
| Runtime | `rx_coop_exec` | Cooperative execution of all FSMs in one thread |

---

## 3. Key concepts

### Roles

Every node is always in one of three roles:

| Role | Description |
|---|---|
| **Follower** | Passive; applies log entries sent by the leader |
| **Candidate** | Running for election after election timeout expires |
| **Leader** | Accepts client commands, replicates them to followers |

### Log, terms, and commit

- Each command submitted by a client becomes a **log entry** `(index, term, command)`.
- The leader replicates entries to a majority of nodes, then **commits** them.
- Once committed, the leader calls `apply` on each node's application in index order.
- A **term** is a monotonically increasing counter that advances with each
  election.  Terms are the Raft equivalent of a logical clock.

### Membership configuration

Raft tracks which nodes are authoritative members of the cluster.  `raft_rx`
stores this as `raft_cluster_configuration_t`:

```c
typedef struct {
    char   old_members[RAFT_MAX_NODES][RAFT_MAX_ID]; /* stable or leaving set */
    size_t old_count;
    char   new_members[RAFT_MAX_NODES][RAFT_MAX_ID]; /* joining set (joint only) */
    size_t new_count;
    int    index;      /* log index where this config was committed */
} raft_cluster_configuration_t;
```

`old_count > 0, new_count == 0` → **stable** configuration  
`old_count > 0, new_count > 0` → **joint** configuration (transition in progress)

### The application interface

Your application plugs into the consensus engine through four callbacks:

```c
typedef struct {
    void   (*apply)            (void *user, const raft_command_t *command);
    void   (*reload)           (void *user);
    size_t (*snapshot)         (void *user, void *buf, size_t buf_size);
    void   (*restore_snapshot) (void *user, const void *buf, size_t size);
    void  *user;
} raft_application_t;
```

| Callback | When called | What to do |
|---|---|---|
| `apply` | Each time a command is committed | Mutate your state machine |
| `reload` | Node restart with no snapshot on disk | Re-apply log from scratch (start from zero) |
| `snapshot` | Log compaction requested | Serialise current state into `buf`; return bytes written |
| `restore_snapshot` | Node restart with a snapshot on disk | Deserialise `buf` into your state machine |

---

## 4. Quick start — the demo app

The demo app ships a ready-to-run multi-process cluster.  Each process is a
Raft node with a built-in key-value store and a readline CLI.

### Build

```bash
# From the repo root
make -C c/examples/demo_app
# Result: c/examples/demo_app/build/raft_node
```

### Start a 3-node cluster

Open three terminals in `c/examples/demo_app/`:

```bash
# Terminal 1
./build/raft_node --id n1 --port 5001 \
  --member n1 --member n2 --member n3 \
  --peer n2=127.0.0.1:5002 --peer n3=127.0.0.1:5003

# Terminal 2
./build/raft_node --id n2 --port 5002 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n3=127.0.0.1:5003

# Terminal 3
./build/raft_node --id n3 --port 5003 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n2=127.0.0.1:5002
```

After ~1 s a leader is elected.  Try these CLI commands on any node:

```
n1> set foo bar        # write (only accepted on the leader)
n2> get foo            # read local state: bar
n1> status             # role, term, leader, commit index
n1> members            # current cluster membership
n1> log                # Raft log entries
```

### Restart a node

Once nodes have run at least once their state is persisted under `var/raft/`.
Restarting is simple — only `--id` is required:

```bash
./build/raft_node --id n1
```

Port, peers, and membership are recovered automatically from disk.

---

## 5. Integrating raft_rx into your application

This section shows how to embed `raft_rx` into your own C program step by step.

### 5.1 Implement `raft_application_t`

Define what a command means for your state machine.  A command is:

```c
typedef struct {
    char op   [32];              /* operation name, e.g. "set", "del" */
    char key  [RAFT_MAX_KEY];    /* primary key     (max 63 chars)    */
    char value[RAFT_MAX_VALUE];  /* payload / value (max 127 chars)   */
} raft_command_t;
```

You choose how to interpret `op`, `key`, and `value`; the consensus engine
treats them as opaque strings.

```c
/* Example: a simple counter replicated across nodes */

typedef struct { int counter; } my_state_t;

static void my_apply(void *user, const raft_command_t *cmd) {
    my_state_t *s = (my_state_t *)user;
    if (strcmp(cmd->op, "inc") == 0)
        s->counter += atoi(cmd->value);
    else if (strcmp(cmd->op, "reset") == 0)
        s->counter = 0;
}

static void my_reload(void *user) {
    /* Called on restart when there is no snapshot.
     * The engine will re-apply every committed log entry through my_apply,
     * so here you only need to reset to the "empty" state. */
    my_state_t *s = (my_state_t *)user;
    s->counter = 0;
}

static size_t my_snapshot(void *user, void *buf, size_t buf_size) {
    my_state_t *s = (my_state_t *)user;
    int n = snprintf((char *)buf, buf_size, "%d\n", s->counter);
    return (n > 0 && (size_t)n < buf_size) ? (size_t)n : 0;
}

static void my_restore_snapshot(void *user, const void *buf, size_t size) {
    my_state_t *s = (my_state_t *)user;
    (void)size;
    s->counter = atoi((const char *)buf);
}

raft_application_t my_make_app(my_state_t *state) {
    raft_application_t app;
    app.apply            = my_apply;
    app.reload           = my_reload;
    app.snapshot         = my_snapshot;
    app.restore_snapshot = my_restore_snapshot;
    app.user             = state;
    return app;
}
```

> **Correctness rule**: `apply` is called exactly once per committed log entry
> in index order, and always on the rxnet thread (single-threaded by design).
> Do **not** spawn threads inside `apply`.

> **Snapshot size**: The snapshot buffer is
> `RAFT_MAX_SNAPSHOT_SIZE` bytes (default 4096).  Override it before including
> `raft.h` if your state is larger:
> ```c
> #define RAFT_MAX_SNAPSHOT_SIZE (64 * 1024)
> #include "raft/raft.h"
> ```

### 5.2 Bootstrap the cluster

Create the runtime, cluster, and one node per process:

```c
rx_fsm_runtime  runtime;
raft_cluster_t  cluster;
raft_node_t    *node;
my_state_t      state = {0};

/* 1. Runtime — capacity = number of rx_fsm_machines you will register
 *    (one per Raft node + one per auxiliary FSM, e.g. a CLI). */
rx_fsm_runtime_init(&runtime, 2);
raft_cluster_init(&cluster, &runtime);
raft_cluster_enable_realtime_clock(&cluster);  /* wall-clock ticks */

/* 2. Node configuration */
raft_node_config_t cfg;
memset(&cfg, 0, sizeof(cfg));
strncpy(cfg.node_id, "n1", RAFT_MAX_ID - 1);
cfg.election_timeout_ms   = 500;   /* ms before follower starts election */
cfg.heartbeat_interval_ms = 100;   /* ms between leader heartbeats      */

/* Initial membership — list every node that starts together.
 * All nodes in the initial cluster must declare the same set.
 * Leave this empty (member_count = 0) for nodes joining via --join. */
strncpy(cfg.initial_members[0], "n1", RAFT_MAX_ID - 1);
strncpy(cfg.initial_members[1], "n2", RAFT_MAX_ID - 1);
strncpy(cfg.initial_members[2], "n3", RAFT_MAX_ID - 1);
cfg.initial_member_count = 3;

/* Peers — IDs of other nodes this node will talk to.
 * The transport uses these IDs to route messages. */
strncpy(cfg.peers[0], "n2", RAFT_MAX_ID - 1);
strncpy(cfg.peers[1], "n3", RAFT_MAX_ID - 1);
cfg.peer_count = 2;

/* 3. Build the application */
raft_application_t app = my_make_app(&state);

/* 4. Add the node to the cluster.
 *    "var/myapp" is the root data directory; the engine stores state under
 *    var/myapp/n1/ for a node with id "n1". */
node = raft_cluster_add_node(&cluster, &cfg, "var/myapp", &app, 10000 /* tick µs */);
```

### 5.3 Attach the transport

#### Option A — memory transport (in-process, for tests)

```c
/* All nodes must share the same raft_cluster_t. */
node->transport = raft_mem_transport_make(node);
```

#### Option B — TCP transport (multi-process)

```c
raft_tcp_transport_t tcp;
raft_tcp_transport_init(&tcp, 5001);             /* listen port */
raft_tcp_transport_set_self(&tcp, "n1", "127.0.0.1");
raft_tcp_transport_set_data_dir(&tcp, "var/myapp/n1"); /* loads persisted peers */

/* Add known peers (only needed on first run; saved automatically after that) */
raft_tcp_transport_add_peer(&tcp, "n2", "127.0.0.1", 5002);
raft_tcp_transport_add_peer(&tcp, "n3", "127.0.0.1", 5003);

raft_tcp_transport_start(&tcp);  /* starts the listener thread */

node->transport = raft_tcp_transport_make(&tcp);
```

#### Option C — custom transport

Implement the five-function vtable:

```c
typedef struct {
    int    (*send)           (void *ctx, const raft_message_t *msg);
    size_t (*recv)           (void *ctx, raft_message_t *out, size_t capacity);
    int    (*submit_command) (void *ctx, const raft_command_t *cmd);
    size_t (*recv_commands)  (void *ctx, raft_command_t *out, size_t capacity);
    void   (*set_enabled)    (void *ctx, int enabled); /* may be NULL */
    void  *ctx;
} raft_transport_t;
```

| Function | Called by | Purpose |
|---|---|---|
| `send` | Consensus engine | Send a Raft message to another node (by `msg->target`) |
| `recv` | Consensus engine | Drain incoming Raft messages from the network |
| `submit_command` | Your client code | Enqueue a client command for the leader to apply |
| `recv_commands` | Consensus engine (leader) | Drain pending client commands |
| `set_enabled` | `raft_node_stop/start` | Gate the transport on simulated failures (optional) |

`send` and `recv` carry Raft protocol messages (`raft_message_t`).  
`submit_command` / `recv_commands` carry application-level commands (`raft_command_t`).

### 5.4 Run the event loop

`raft_rx` runs inside the `rxnet` cooperative executor — a single-threaded
poll loop that ticks all registered FSMs in turn:

```c
rx_coop_exec ce;
rx_coop_exec_init(&ce);
rx_coop_exec_add(&ce, &runtime.runtime);

rx_coop_exec_run(&ce);  /* blocks until all machines exit */
```

The executor calls each FSM at the configured tick interval (`period_us` in
`raft_cluster_add_node`).  The default is 10 000 µs (10 ms); tune it to be
≤ `heartbeat_interval_ms / 2` in practice.

> **Threading model**: There is exactly one compute thread (the one calling
> `rx_coop_exec_run`).  The TCP listener runs in a separate OS thread, but it
> only appends to mutex-protected queues.  All Raft logic, `apply`, and your
> application callbacks execute on the single compute thread.

### 5.5 Submit commands

Commands must reach the **leader** to be committed.  With the memory transport
(single process) you can find the leader directly:

```c
raft_node_t *leader = raft_cluster_leader(&cluster);
if (leader) {
    raft_command_t cmd;
    memset(&cmd, 0, sizeof(cmd));
    strncpy(cmd.op,    "inc",  sizeof(cmd.op)    - 1);
    strncpy(cmd.value, "1",    sizeof(cmd.value)  - 1);
    raft_node_submit_command(leader, &cmd);
}
```

With the TCP transport the command is submitted to the transport's command
queue and drained by the leader's FSM on the next tick:

```c
/* On the leader node (or any node that forwards to the leader): */
raft_command_t cmd;
memset(&cmd, 0, sizeof(cmd));
strncpy(cmd.op, "inc", sizeof(cmd.op) - 1);
strncpy(cmd.value, "1", sizeof(cmd.value) - 1);
node->transport.submit_command(node->transport.ctx, &cmd);
```

### 5.6 Complete minimal example

The snippet below is a self-contained in-process 3-node cluster:

```c
#include "raft/raft.h"
#include "rxnet/fsm.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ── Application ─────────────────────────────────────────────────────────── */

typedef struct { int counter; } counter_state_t;

static void on_apply(void *u, const raft_command_t *cmd) {
    counter_state_t *s = u;
    if (strcmp(cmd->op, "inc") == 0) s->counter += atoi(cmd->value);
}
static void on_reload(void *u)                              { ((counter_state_t *)u)->counter = 0; }
static size_t on_snapshot(void *u, void *buf, size_t sz)   {
    return (size_t)snprintf(buf, sz, "%d\n", ((counter_state_t *)u)->counter);
}
static void on_restore(void *u, const void *buf, size_t sz) {
    (void)sz; ((counter_state_t *)u)->counter = atoi(buf);
}

static raft_application_t make_app(counter_state_t *s) {
    raft_application_t a = { on_apply, on_reload, on_snapshot, on_restore, s };
    return a;
}

/* ── Bootstrap helper ────────────────────────────────────────────────────── */

static raft_node_config_t node_cfg(const char *id, const char *p1, const char *p2) {
    raft_node_config_t c;
    memset(&c, 0, sizeof(c));
    strncpy(c.node_id, id, RAFT_MAX_ID - 1);
    strncpy(c.peers[0], p1, RAFT_MAX_ID - 1);
    strncpy(c.peers[1], p2, RAFT_MAX_ID - 1);
    c.peer_count = 2;
    strncpy(c.initial_members[0], "n1", RAFT_MAX_ID - 1);
    strncpy(c.initial_members[1], "n2", RAFT_MAX_ID - 1);
    strncpy(c.initial_members[2], "n3", RAFT_MAX_ID - 1);
    c.initial_member_count  = 3;
    c.election_timeout_ms   = 150;
    c.heartbeat_interval_ms = 50;
    return c;
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void) {
    rx_fsm_runtime   runtime;
    raft_cluster_t   cluster;
    counter_state_t  state[3] = {{0}, {0}, {0}};
    raft_application_t apps[3];
    raft_node_config_t cfgs[3];
    raft_node_t *leader;
    raft_command_t cmd;
    size_t i;

    rx_fsm_runtime_init(&runtime, 3);
    raft_cluster_init(&cluster, &runtime);

    cfgs[0] = node_cfg("n1", "n2", "n3");
    cfgs[1] = node_cfg("n2", "n1", "n3");
    cfgs[2] = node_cfg("n3", "n1", "n2");

    for (i = 0; i < 3; ++i) {
        apps[i] = make_app(&state[i]);
        raft_node_t *n = raft_cluster_add_node(&cluster, &cfgs[i],
                                               NULL /*no persistence*/, &apps[i], 0);
        n->transport = raft_mem_transport_make(n);
    }

    /* Simulate 800 ms of wall time in 10 ms ticks */
    for (i = 0; i < 80; ++i)
        raft_cluster_tick(&cluster, 10);

    leader = raft_cluster_leader(&cluster);
    if (!leader) { fputs("no leader\n", stderr); return 1; }
    printf("leader: %s\n", leader->config.node_id);

    /* Submit a command */
    memset(&cmd, 0, sizeof(cmd));
    strncpy(cmd.op, "inc", sizeof(cmd.op) - 1);
    strncpy(cmd.value, "42", sizeof(cmd.value) - 1);
    raft_node_submit_command(leader, &cmd);

    for (i = 0; i < 20; ++i)
        raft_cluster_tick(&cluster, 10);

    printf("counter on n1: %d\n", state[0].counter);  /* → 42 */

    raft_cluster_destroy(&cluster);
    rx_fsm_runtime_free(&runtime);
    return 0;
}
```

Compile with:

```bash
cc -std=c99 -Ic/include -Irxnet/c/include \
   my_app.c c/build/libraft_rx.a rxnet/c/build/librxnet.a -o my_app
```

---

## 6. TCP transport

`raft_tcp_transport_t` provides a production-grade TCP transport for
multi-process clusters.  Include `raft/raft_tcp_transport.h` and link with
`raft_tcp_transport.c` and `-lpthread`.

### 6.1 Initialization

```c
raft_tcp_transport_t tcp;

/* 1. Init with listen port (0 = don't listen yet) */
raft_tcp_transport_init(&tcp, 5001);

/* 2. Identify this node (used in self-announcements during join) */
raft_tcp_transport_set_self(&tcp, "n1", "127.0.0.1");

/* 3. Set data directory — loads persisted peers.txt and self.txt from disk */
raft_tcp_transport_set_data_dir(&tcp, "var/myapp/n1");

/* 4. Add initial peers (skip on restart; they are loaded from peers.txt) */
raft_tcp_transport_add_peer(&tcp, "n2", "127.0.0.1", 5002);
raft_tcp_transport_add_peer(&tcp, "n3", "127.0.0.1", 5003);

/* 5. Start the listener thread */
raft_tcp_transport_start(&tcp);

/* 6. Create the vtable that raft_node_t will use */
node->transport = raft_tcp_transport_make(&tcp);
```

Call order matters: `set_self` and `set_data_dir` must come before
`start`.

### 6.2 Peer persistence

Every time a peer is added (via `raft_tcp_transport_add_peer` or through the
join protocol), the full peer table is written atomically to
`{data_dir}/peers.txt`.  Format:

```
n2 127.0.0.1 5002
n3 127.0.0.1 5003
```

On the next startup `raft_tcp_transport_set_data_dir` reloads the table, so
`--peer` flags are not needed after the first run.

`add_peer` deduplicates by node ID: if the same ID is added again with a
different address, the entry is updated and the file is rewritten.

### 6.3 Self persistence

`raft_tcp_transport_start` writes `{data_dir}/self.txt`:

```
127.0.0.1 5001
```

On restart, `set_data_dir` reads this file and fills in `own_host` and
`listen_port` if they are not already set.  This means `--port` and `--host`
flags are also optional after the first run.

Priority (highest to lowest):

1. Explicit `--port` / `--host` command-line argument
2. Values in `self.txt`
3. Defaults (`127.0.0.1`, no port)

### 6.4 The listener thread

`raft_tcp_transport_start` spawns a single background thread that accepts
connections, reads framed messages, and appends them to mutex-protected
in-memory queues (`in_queue`, `join_queue`, `cmd_queue`, `fwd_cmd_queue`).

The rxnet compute thread drains these queues each tick by calling the
transport vtable's `recv` and `recv_commands` functions — no callbacks or
locking is needed in application code.

Tear down with `raft_tcp_transport_stop(&tcp)` after the event loop exits.

---

## 7. Cluster lifecycle

### 7.1 Bootstrap a fresh cluster

A **fresh** cluster starts with no data on disk.  Every node needs:

- `--id NAME` — unique node identifier (max 15 chars)
- `--port PORT` — TCP listen port
- `--member ID` × N — the IDs of **all** initial members (same on every node)
- `--peer ID=HOST:PORT` × N-1 — address of each other node

All nodes that are part of the initial cluster must declare **identical**
`--member` lists.  Once enough nodes are running (≥ ⌊N/2⌋ + 1), a leader is
elected and the cluster is operational.

Example — 3-node cluster:

```bash
./raft_node --id n1 --port 5001 \
  --member n1 --member n2 --member n3 \
  --peer n2=127.0.0.1:5002 --peer n3=127.0.0.1:5003

./raft_node --id n2 --port 5002 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n3=127.0.0.1:5003

./raft_node --id n3 --port 5003 \
  --member n1 --member n2 --member n3 \
  --peer n1=127.0.0.1:5001 --peer n2=127.0.0.1:5002
```

### 7.2 Restarting a node

After the first run, all state lives on disk.  Restart with only:

```bash
./raft_node --id n1
```

The node loads term, log, snapshot, membership, peers, port, and host from
`{data_dir}/n1/`.  No other flags are needed.

**What happens internally**:

1. `raft_storage_node_load` restores term, log, snapshot, and membership.
2. `raft_tcp_transport_set_data_dir` reloads `peers.txt` and `self.txt`.
3. The node starts as a follower with an **extended** initial election timeout
   (`election_timeout + random + 2 × election_timeout`).  This gives the
   existing leader time to send heartbeats and prevents a spurious election.
4. On receiving the first heartbeat the node rejoins the cluster normally.

> The cluster **does not need to be stopped** to restart a single node.  A
> 3-node cluster tolerates one failed node while remaining fully operational.

### 7.3 Adding a node (`--join`)

A new node does not need `--member` or `--peer` — it only needs to contact
one existing member (the *introducer*):

```bash
./raft_node --id n4 --port 5004 --join 127.0.0.1:5001
```

**Join protocol**:

```
n4 ──(join-request)──► n1 (introducer)
n1 ──(flood: join-request, forwarded)──► n2, n3
n1, n2, n3 ──(announce-self)──► n4
n4 adds n1, n2, n3 as TCP peers
leader applies membership change via Raft log
```

1. n4 sends its ID, host, and port to the introducer (n1).
2. n1 floods the request to all its peers with `forwarded=1` (prevents loops).
3. Every existing node adds n4 as a TCP peer and sends n4 a self-announcement.
4. n4 now knows the address of every member.
5. The current leader appends a joint-consensus entry to the log; once it
   commits, n4 is a full voting member.

Existing nodes **do not need reconfiguration** to accept a new member.

### 7.4 Removing a node

Run `rmnode ID` on the leader's CLI, or call the API directly:

```c
char new_members[3][RAFT_MAX_ID] = {"n1", "n2", "n3"};
raft_node_request_membership_change(leader, new_members, 3);
```

Pass the **new full membership list** (the target state, not the delta).
The engine runs joint consensus automatically and shuts down the removed node
once the final configuration commits.

To remove the current node itself, use `leave` on the CLI or call
`raft_node_request_membership_change` with a list that excludes `node_id`.

---

## 8. Membership changes internals

`raft_rx` uses the **joint consensus** approach from the Raft paper.  A
membership change proceeds in two log-appended phases:

### Phase 1 — enter joint

The leader appends a `cluster.enter_joint` entry encoding both the old and new
member sets.  While this entry is not yet committed, quorum requires a majority
of **both** the old and new sets.

```
log: … | enter_joint [old={n1,n2,n3}, new={n1,n2,n3,n4}] | …
```

### Phase 2 — leave joint

Once every new member has caught up (replicated the full log) and the
`enter_joint` entry is committed, the leader appends `cluster.leave_joint`.
This finalises the configuration to just `new_members`.

```
log: … | enter_joint [...] | … | leave_joint [new={n1,n2,n3,n4}] | …
```

The configuration is stored in `meta.txt` and replayed on restart — no
external state is needed.

---

## 9. Persistence and storage format

Each node stores its state under `{root_dir}/{node_id}/`:

```
var/myapp/
└── n1/
    ├── meta.txt        Raft durable state
    ├── log.txt         Uncommitted / recent log entries
    ├── snapshot.bin    Compacted application snapshot
    ├── peers.txt       Known TCP peer addresses  (TCP transport only)
    └── self.txt        Own host and port         (TCP transport only)
```

### `meta.txt`

Plain text, one field per line:

```
current_term 7
voted_for n2
compaction_threshold 128
commit_index 14
snapshot_last_included_index 10
snapshot_last_included_term 4
configuration_index 3
old_count 3
old_member n1
old_member n2
old_member n3
new_count 0
```

### `log.txt`

One entry per line, pipe-delimited:

```
index|term|op|key|value
11|4|set|foo|bar
12|4|del|old|-
13|7|cluster.enter_joint|members|n1,n2,n3,n4
```

### `snapshot.bin`

Binary: `[int32 index][int32 term][int32 size][<size> bytes of app data]`

### `peers.txt` / `self.txt` (TCP transport)

```
# peers.txt
n2 127.0.0.1 5002
n3 127.0.0.1 5003

# self.txt
127.0.0.1 5001
```

Writes are **atomic** (write to `.tmp`, then `rename`) so a crash mid-write
never corrupts the current file.

---

## 10. Configuration reference

### `raft_node_config_t`

| Field | Type | Description |
|---|---|---|
| `node_id` | `char[RAFT_MAX_ID]` | Unique node name (max 15 chars) |
| `peers` | `char[RAFT_MAX_PEERS][RAFT_MAX_ID]` | IDs of other nodes |
| `peer_count` | `size_t` | Number of entries in `peers` |
| `initial_members` | `char[RAFT_MAX_NODES][RAFT_MAX_ID]` | All nodes in the initial cluster |
| `initial_member_count` | `size_t` | Number of initial members |
| `learner` | `int` | `1` for a joining node (skips auto-bootstrap) |
| `election_timeout_ms` | `int` | Base election timeout in ms |
| `heartbeat_interval_ms` | `int` | Leader heartbeat interval in ms |

### Timing guidelines

| Parameter | Recommended | Minimum | Notes |
|---|---|---|---|
| `election_timeout_ms` | 500 ms | ~3 × RTT | Too small → spurious elections; too large → slow failover |
| `heartbeat_interval_ms` | 100 ms | ~RTT | Must be much less than `election_timeout_ms`; rule of thumb: ≤ 1/5 |
| tick interval (`period_us`) | 10 000 µs | — | Should be ≤ `heartbeat_interval_ms × 1000 / 2` |

### Restarting node election delay

When a node with `current_term > 0` on disk restarts it gets an extra
`2 × election_timeout_ms` added to its first election deadline.  This gives
the existing leader time to establish heartbeats before the restarting node
launches an election.

---

## 11. Limits reference

| Constant | Default | Meaning |
|---|---|---|
| `RAFT_MAX_NODES` | 8 | Maximum nodes per cluster |
| `RAFT_MAX_PEERS` | 7 | Maximum peers per node (`RAFT_MAX_NODES - 1`) |
| `RAFT_MAX_ID` | 16 | Maximum length of a node ID (including `\0`) |
| `RAFT_MAX_LOG` | 128 | Maximum log entries before compaction is mandatory |
| `RAFT_MAX_QUEUE` | 256 | Message queue size (per node, per direction) |
| `RAFT_MAX_BATCH` | 16 | Maximum entries per `AppendEntries` RPC |
| `RAFT_MAX_KEY` | 64 | Maximum key length in `raft_command_t` |
| `RAFT_MAX_VALUE` | 128 | Maximum value length in `raft_command_t` |
| `RAFT_MAX_SNAPSHOT_SIZE` | 4096 | Snapshot buffer size; override before `#include "raft/raft.h"` |
| `RAFT_KV_MAX_PAIRS` | 128 | Maximum key-value pairs in the built-in KV app |

All constants can be overridden at compile time:

```c
#define RAFT_MAX_NODES 16
#define RAFT_MAX_SNAPSHOT_SIZE (256 * 1024)
#include "raft/raft.h"
```

---

## 12. Building

### Library only

```bash
make -C c build/libraft_rx.a
```

### With rxnet tracing enabled

```bash
make -C c build/libraft_rx_trace.a
```

The trace variant compiles with `-DRX_TRACE_ENABLE` and produces a
`trace.bin` readable by the rxnet trace viewer.

### Linking your application

```makefile
INCLUDES := -Ic/include -Irxnet/c/include
LIBS     := c/build/libraft_rx.a rxnet/c/build/librxnet.a -lpthread

$(TARGET): $(SRC) $(LIBS)
	$(CC) -std=c99 -O2 $(INCLUDES) -o $@ $(SRC) $(LIBS)
```

The TCP transport (`raft_tcp_transport.c`) and the KV application
(`raft_kv_app.c`) are **not** compiled into `libraft_rx.a`; include their
`.c` files directly in your build if you use them (see the demo app
`Makefile` for a complete example).

---

## 13. Troubleshooting

### No leader elected after startup

- Check that all nodes declare the **same** `--member` list.
- Check that `--peer` addresses are reachable and the ports are open.
- Verify that `election_timeout_ms` is at least 3–5× the round-trip latency.

### Node restarts with `running=no` (does not rejoin)

This can happen if a node that originally joined via `--join` is restarted
with an incorrect membership override.  The fix is already baked in: a node
with log entries on disk **never** applies the `initial_members` fallback; it
always replays membership from the log.  If you see this symptom, check that
you are not accidentally passing `--member` flags on restart.

### Leader lost after restarting two nodes sequentially

A restarting node with a stale term on disk may start a new election before the
existing leader can send a heartbeat, disrupting the cluster.  The extended
election timeout (§10) mitigates this.  If it still occurs, increase
`election_timeout_ms` or reduce the time between restarts.

### `raft_cluster_add_node` returns NULL

- Cluster is already at `RAFT_MAX_NODES` capacity.
- The data directory cannot be created (check permissions).

### Log grows without compaction

Compaction triggers automatically when `log_count >= compaction_threshold`
(default: `RAFT_MAX_LOG = 128`).  The engine calls `snapshot` on your
application, writes `snapshot.bin`, and truncates `log.txt`.  Ensure your
`snapshot` callback serialises all state correctly, otherwise a restart will
lose data.

### Custom snapshot size

If your application state exceeds 4096 bytes, override the constant:

```c
#define RAFT_MAX_SNAPSHOT_SIZE (64 * 1024)
#include "raft/raft.h"
```

Do this consistently in every translation unit that includes `raft.h`.
