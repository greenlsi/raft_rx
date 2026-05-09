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

#include "demo_args.h"
#include "demo_kv_app.h"
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

typedef struct {
    rx_fsm_runtime       runtime;
    raft_cluster_t       cluster;
    raft_kv_state_t      kv;
    raft_tcp_transport_t tcp;
    rx_coop_exec         coop;
    rx_fsm_machine       cli_machine;
    raft_node_t         *node;
    cli_user_t           cli;
    demo_kv_app_t        app_state;
} demo_process_t;

static int parse_host_port(const char *arg, char *host, size_t hostsz, int *port) {
    const char *colon = strrchr(arg, ':');
    if (!colon) return -1;
    {
        size_t hlen = (size_t)(colon - arg);
        if (hlen == 0 || hlen >= hostsz) return -1;
        strncpy(host, arg, hlen);
        host[hlen] = '\0';
    }
    *port = atoi(colon + 1);
    return (*port > 0) ? 0 : -1;
}

static void register_peer_from_app(void *user,
                                   const char *node_id,
                                   const char *address) {
    demo_process_t *demo = (demo_process_t *)user;
    char host[256];
    int port;
    if (strcmp(node_id, demo->tcp.own_id) == 0) return;
    if (parse_host_port(address, host, sizeof(host), &port) == 0)
        raft_tcp_transport_add_peer(&demo->tcp, node_id, host, port);
}

static void configure_transport(demo_process_t *demo, const demo_args_t *args) {
    int i;
    raft_tcp_transport_init(&demo->tcp, args->port);
    raft_tcp_transport_set_self(&demo->tcp, args->node_id, args->host);
    {
        char node_dir[512];
        snprintf(node_dir, sizeof(node_dir), "%s/%s", args->data_dir, args->node_id);
        raft_tcp_transport_set_data_dir(&demo->tcp, node_dir);
    }
    for (i = 0; i < args->peer_count; ++i)
        raft_tcp_transport_add_peer(&demo->tcp, args->peer_ids[i],
                                    args->peer_hosts[i], args->peer_ports[i]);
}

static void make_raft_config(raft_node_config_t *cfg, const demo_args_t *args) {
    int i;
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->node_id, args->node_id, RAFT_MAX_ID - 1);
    cfg->election_timeout_ms   = ELECTION_TIMEOUT;
    cfg->heartbeat_interval_ms = HEARTBEAT_INTERVAL;
    if (args->join_port > 0)
        cfg->learner = 1;
    for (i = 0; i < args->peer_count; ++i)
        strncpy(cfg->peers[cfg->peer_count++], args->peer_ids[i], RAFT_MAX_ID - 1);
    for (i = 0; i < args->member_count; ++i)
        strncpy(cfg->initial_members[cfg->initial_member_count++],
                args->initial_members[i], RAFT_MAX_ID - 1);
}

static void configure_cli(demo_process_t *demo, const demo_args_t *args) {
    memset(&demo->cli, 0, sizeof(demo->cli));
    strncpy(demo->cli.node_id, args->node_id, RAFT_MAX_ID - 1);
    demo->cli.node          = demo->node;
    demo->cli.tcp           = &demo->tcp;
    demo->cli.kv            = &demo->kv;
    demo->cli.coop          = &demo->coop;
    demo->cli.prompt_needed = 1;
    demo->cli.last_role      = demo->node->machine.state;
    demo->cli.last_term      = demo->node->current_term;
    strncpy(demo->cli.last_voted_for, demo->node->voted_for, RAFT_MAX_ID - 1);
    demo->cli.last_vote_count = 0;
}

static int setup(demo_process_t *demo, const demo_args_t *args) {
    raft_node_config_t cfg;
    raft_application_t app;

    memset(demo, 0, sizeof(*demo));
    configure_transport(demo, args);
    if (demo->tcp.listen_port > 0 && raft_tcp_transport_start(&demo->tcp) != 0) {
        fprintf(stderr, "failed to listen on port %d\n", demo->tcp.listen_port);
        return -1;
    }

    rx_fsm_runtime_init(&demo->runtime, 2);
    raft_cluster_init(&demo->cluster, &demo->runtime);
    raft_cluster_enable_realtime_clock(&demo->cluster);

    make_raft_config(&cfg, args);
    raft_kv_state_init(&demo->kv);
    app = demo_kv_app_make(&demo->app_state, &demo->kv,
                           register_peer_from_app, demo);

    demo->node = raft_cluster_add_node(&demo->cluster, &cfg,
                                       args->data_dir, &app, TICK_US);
    if (!demo->node) {
        fputs("raft_cluster_add_node failed\n", stderr);
        return -1;
    }
    demo->node->transport = raft_tcp_transport_make(&demo->tcp);

    configure_cli(demo, args);
    cli_machine_init(&demo->cli_machine, &demo->cli);
    if (rx_fsm_runtime_add_machine(&demo->runtime, &demo->cli_machine, TICK_US, 0) != 0) {
        fputs("failed to add CLI machine\n", stderr);
        return -1;
    }

    rx_coop_exec_init(&demo->coop);
    if (rx_coop_exec_add(&demo->coop, &demo->runtime.runtime) != 0) {
        fputs("rx_coop_exec_add failed\n", stderr);
        return -1;
    }
    return 0;
}

static void request_join(demo_process_t *demo, const demo_args_t *args) {
    if (args->join_port <= 0) return;
    raft_tcp_transport_request_join(&demo->tcp,
                                    args->node_id, args->host, args->port,
                                    args->join_host, args->join_port);
}

static void run(demo_process_t *demo) {
    if (raft_tcp_transport_is_listening(&demo->tcp))
        printf("[%s] listening on :%d  (type 'help')\n",
               demo->node->config.node_id, demo->tcp.listen_port);
    else
        printf("[%s] not listening  (use 'port PORT'; type 'help')\n",
               demo->node->config.node_id);

    rx_coop_exec_run(&demo->coop);
}

static void teardown(demo_process_t *demo) {
    raft_tcp_transport_stop(&demo->tcp);
    raft_cluster_destroy(&demo->cluster);
    rx_fsm_runtime_free(&demo->runtime);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    demo_args_t args = demo_args_parse(argc, argv);
    static demo_process_t demo;

    if (setup(&demo, &args) != 0)
        return 1;

    /*
     * --join HOST:PORT: send a join request to one introducer.
     * The introducer floods the request to all its peers (so every existing
     * node adds us as a TCP peer) and announces itself back to us.  Each
     * peer also announces itself.  The current leader then applies the
     * membership change.
     */
    request_join(&demo, &args);
    run(&demo);
    teardown(&demo);
    return 0;
}
