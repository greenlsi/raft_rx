// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: MIT

/*
 * raft_node — single-node Raft/KV process.  The CLI is itself an FSM
 * registered on the same rxnet cooperative runtime as the Raft node, so
 * main() only creates machines and calls rx_coop_exec_run().
 *
 * Usage:
 *   raft_node --id NAME --port PORT [--host HOST] [--data DIR]
 *             [--member ID] ...          initial cluster members
 *             [--peer ID=HOST:PORT] ...  known peer addresses
 *             [--join HOST:PORT]         join existing cluster via introducer
 *
 * 3-node bootstrap example:
 *   raft_node --id n1 --port 5001 --member n1 --member n2 --member n3 \
 *             --peer n2=127.0.0.1:5002 --peer n3=127.0.0.1:5003
 *
 *   raft_node --id n2 --port 5002 --member n1 --member n2 --member n3 \
 *             --peer n1=127.0.0.1:5001 --peer n3=127.0.0.1:5003
 *
 *   raft_node --id n3 --port 5003 --member n1 --member n2 --member n3 \
 *             --peer n1=127.0.0.1:5001 --peer n2=127.0.0.1:5002
 *
 * Adding a 4th node (no --member or --peer needed for the joining node):
 *   raft_node --id n4 --port 5004 --join 127.0.0.1:5001
 *
 * The introducer (n1) floods the join request to all its peers and announces
 * itself back to the joiner; each peer also announces itself.  The current
 * leader then applies the membership change.  Existing nodes do NOT need
 * reconfiguration.
 *
 * CLI commands: set, get, delete, status, leader, stop, start,
 *               members, log, addnode, rmnode, leave, help, quit
 */

#include "cli_fsm.h"
#include "raft/raft.h"
#include "raft/raft_kv_app.h"
#include "raft/raft_tcp_transport.h"
#include "rxnet/coop.h"
#include "rxnet/fsm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TICK_US            10000L
#define ELECTION_TIMEOUT   500
#define HEARTBEAT_INTERVAL 100
#define DEFAULT_DATA_DIR   "var/raft"
#define DEFAULT_HOST       "127.0.0.1"

/* ── Argument parsing ────────────────────────────────────────────────────── */

typedef struct {
    char node_id[RAFT_MAX_ID];
    int  port;
    int  port_supplied;
    char host[256];
    char data_dir[256];

    char initial_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    int  member_count;

    char peer_ids  [RAFT_MAX_PEERS][RAFT_MAX_ID];
    char peer_hosts[RAFT_MAX_PEERS][256];
    int  peer_ports[RAFT_MAX_PEERS];
    int  peer_count;

    char join_host[256];
    int  join_port;      /* 0 = no --join */
} node_args_t;

static void usage(const char *argv0) {
    fprintf(stderr,
        "usage: %s --id NAME [--port PORT] [--host HOST] [--data DIR]\n"
        "          [--member ID] ... [--peer ID=HOST:PORT] ...\n"
        "          [--join HOST:PORT]\n",
        argv0);
    exit(1);
}

static int parse_host_port(const char *arg, char *host, size_t hostsz, int *port) {
    const char *colon = strrchr(arg, ':');
    if (!colon) return -1;
    size_t hlen = (size_t)(colon - arg);
    if (hlen == 0 || hlen >= hostsz) return -1;
    strncpy(host, arg, hlen); host[hlen] = '\0';
    *port = atoi(colon + 1);
    return (*port > 0) ? 0 : -1;
}

static int parse_peer(const char *arg, char *id, char *host, int *port) {
    const char *eq = strchr(arg, '=');
    if (!eq) return -1;
    size_t id_len = (size_t)(eq - arg);
    if (id_len >= RAFT_MAX_ID) return -1;
    strncpy(id, arg, id_len); id[id_len] = '\0';
    return parse_host_port(eq + 1, host, 256, port);
}

static node_args_t parse_args(int argc, char **argv) {
    node_args_t a;
    int i;
    memset(&a, 0, sizeof(a));
    strncpy(a.data_dir, DEFAULT_DATA_DIR, sizeof(a.data_dir) - 1);
    strncpy(a.host,     DEFAULT_HOST,     sizeof(a.host) - 1);

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            strncpy(a.node_id, argv[++i], RAFT_MAX_ID - 1);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            a.port = atoi(argv[++i]);
            a.port_supplied = 1;
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            strncpy(a.host, argv[++i], sizeof(a.host) - 1);
        } else if (strcmp(argv[i], "--data") == 0 && i + 1 < argc) {
            strncpy(a.data_dir, argv[++i], sizeof(a.data_dir) - 1);
        } else if (strcmp(argv[i], "--member") == 0 && i + 1 < argc) {
            if (a.member_count < RAFT_MAX_NODES)
                strncpy(a.initial_members[a.member_count++], argv[++i], RAFT_MAX_ID - 1);
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            if (a.peer_count < RAFT_MAX_PEERS) {
                int idx = a.peer_count;
                if (parse_peer(argv[++i], a.peer_ids[idx],
                               a.peer_hosts[idx], &a.peer_ports[idx]) == 0)
                    a.peer_count++;
                else { fprintf(stderr, "bad --peer: %s\n", argv[i]); exit(1); }
            }
        } else if (strcmp(argv[i], "--join") == 0 && i + 1 < argc) {
            if (parse_host_port(argv[++i], a.join_host, sizeof(a.join_host),
                                &a.join_port) < 0) {
                fprintf(stderr, "bad --join: %s (expected HOST:PORT)\n", argv[i]);
                exit(1);
            }
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            usage(argv[0]);
        }
    }

    if (!a.node_id[0]) {
        fprintf(stderr, "--id is required\n");
        usage(argv[0]);
    }
    if (a.join_port > 0 && (!a.port_supplied || a.port <= 0)) {
        fprintf(stderr, "--join requires --port so peers can reach this node\n");
        usage(argv[0]);
    }
    return a;
}

/* ── Global state ────────────────────────────────────────────────────────── */

static rx_fsm_runtime       g_runtime;
static raft_cluster_t       g_cluster;
static raft_kv_state_t      g_kv;
static raft_tcp_transport_t g_tcp;
static rx_coop_exec         g_ce;
static rx_fsm_machine       g_cli_machine;
static raft_node_t         *g_node = NULL;
static cli_user_t           g_cli;

typedef struct {
    raft_application_t kv_app;
    raft_kv_state_t *kv;
    raft_tcp_transport_t *tcp;
} demo_app_state_t;

static demo_app_state_t g_app_state;

static void demo_apply(void *user, const raft_command_t *command) {
    demo_app_state_t *state = (demo_app_state_t *)user;
    if (strcmp(command->op, "cluster.peer_set") == 0) {
        char host[256];
        int port;
        if (parse_host_port(command->value, host, sizeof(host), &port) == 0 &&
            strcmp(command->key, state->tcp->own_id) != 0) {
            raft_tcp_transport_add_peer(state->tcp, command->key, host, port);
        }
        return;
    }
    if (state->kv_app.apply)
        state->kv_app.apply(state->kv_app.user, command);
}

static void demo_reload(void *user) {
    demo_app_state_t *state = (demo_app_state_t *)user;
    if (state->kv_app.reload)
        state->kv_app.reload(state->kv_app.user);
}

static size_t demo_snapshot(void *user, void *buf, size_t buf_size) {
    demo_app_state_t *state = (demo_app_state_t *)user;
    if (!state->kv_app.snapshot) return 0;
    return state->kv_app.snapshot(state->kv_app.user, buf, buf_size);
}

static void demo_restore_snapshot(void *user, const void *buf, size_t size) {
    demo_app_state_t *state = (demo_app_state_t *)user;
    if (state->kv_app.restore_snapshot)
        state->kv_app.restore_snapshot(state->kv_app.user, buf, size);
}

static raft_application_t demo_make_application(demo_app_state_t *state,
                                                raft_kv_state_t *kv,
                                                raft_tcp_transport_t *tcp) {
    raft_application_t app;
    state->kv = kv;
    state->tcp = tcp;
    state->kv_app = raft_kv_make_application(kv);
    memset(&app, 0, sizeof(app));
    app.apply = demo_apply;
    app.reload = demo_reload;
    app.snapshot = demo_snapshot;
    app.restore_snapshot = demo_restore_snapshot;
    app.user = state;
    return app;
}

/* ── Setup ───────────────────────────────────────────────────────────────── */

static void setup(const node_args_t *a) {
    raft_node_config_t cfg;
    raft_application_t app;
    int i;

    raft_tcp_transport_init(&g_tcp, a->port);
    raft_tcp_transport_set_self(&g_tcp, a->node_id, a->host);
    {
        char node_dir[512];
        snprintf(node_dir, sizeof(node_dir), "%s/%s", a->data_dir, a->node_id);
        raft_tcp_transport_set_data_dir(&g_tcp, node_dir);  /* loads saved peers */
    }
    for (i = 0; i < a->peer_count; ++i)
        raft_tcp_transport_add_peer(&g_tcp, a->peer_ids[i],
                                    a->peer_hosts[i], a->peer_ports[i]);
    if (g_tcp.listen_port > 0 && raft_tcp_transport_start(&g_tcp) != 0) {
        fprintf(stderr, "failed to listen on port %d\n", g_tcp.listen_port);
        exit(1);
    }

    rx_fsm_runtime_init(&g_runtime, 2);  /* capacity: Raft node + CLI */
    raft_cluster_init(&g_cluster, &g_runtime);
    raft_cluster_enable_realtime_clock(&g_cluster);

    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.node_id, a->node_id, RAFT_MAX_ID - 1);
    cfg.election_timeout_ms   = ELECTION_TIMEOUT;
    cfg.heartbeat_interval_ms = HEARTBEAT_INTERVAL;
    if (a->join_port > 0)
        cfg.learner = 1;  /* joining node: don't auto-bootstrap a solo cluster */
    for (i = 0; i < a->peer_count; ++i)
        strncpy(cfg.peers[cfg.peer_count++], a->peer_ids[i], RAFT_MAX_ID - 1);
    for (i = 0; i < a->member_count; ++i)
        strncpy(cfg.initial_members[cfg.initial_member_count++],
                a->initial_members[i], RAFT_MAX_ID - 1);

    raft_kv_state_init(&g_kv);
    app = demo_make_application(&g_app_state, &g_kv, &g_tcp);

    g_node = raft_cluster_add_node(&g_cluster, &cfg, a->data_dir, &app, TICK_US);
    if (!g_node) { fputs("raft_cluster_add_node failed\n", stderr); exit(1); }

    g_node->transport = raft_tcp_transport_make(&g_tcp);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    node_args_t args = parse_args(argc, argv);

    setup(&args);

    /*
     * --join HOST:PORT: send a join request to one introducer.
     * The introducer floods the request to all its peers (so every existing
     * node adds us as a TCP peer) and announces itself back to us.  Each
     * peer also announces itself.  The current leader then applies the
     * membership change.
     */
    if (args.join_port > 0)
        raft_tcp_transport_request_join(&g_tcp,
                                         args.node_id, args.host, args.port,
                                         args.join_host, args.join_port);

    memset(&g_cli, 0, sizeof(g_cli));
    strncpy(g_cli.node_id, args.node_id, RAFT_MAX_ID - 1);
    g_cli.node          = g_node;
    g_cli.tcp           = &g_tcp;
    g_cli.kv            = &g_kv;
    g_cli.coop          = &g_ce;
    g_cli.prompt_needed = 1;
    g_cli.last_role      = g_node->machine.state;
    g_cli.last_term      = g_node->current_term;
    strncpy(g_cli.last_voted_for, g_node->voted_for, RAFT_MAX_ID - 1);
    g_cli.last_vote_count = 0;

    cli_machine_init(&g_cli_machine, &g_cli);
    if (rx_fsm_runtime_add_machine(&g_runtime, &g_cli_machine, TICK_US, 0) != 0) {
        fputs("failed to add CLI machine\n", stderr); exit(1);
    }

    rx_coop_exec_init(&g_ce);
    if (rx_coop_exec_add(&g_ce, &g_runtime.runtime) != 0) {
        fputs("rx_coop_exec_add failed\n", stderr); exit(1);
    }

    if (raft_tcp_transport_is_listening(&g_tcp))
        printf("[%s] listening on :%d  (type 'help')\n", args.node_id, g_tcp.listen_port);
    else
        printf("[%s] not listening  (use 'port PORT'; type 'help')\n", args.node_id);

    rx_coop_exec_run(&g_ce);

    raft_tcp_transport_stop(&g_tcp);
    raft_cluster_destroy(&g_cluster);
    rx_fsm_runtime_free(&g_runtime);
    return 0;
}
