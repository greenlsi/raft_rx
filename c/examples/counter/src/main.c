// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

/*
 * counter_node — ejemplo minimo de contador replicado.
 *
 * Uso:
 *   counter_node --id NAME --port PORT [--host HOST] [--data DIR]
 *
 * Cada nodo arranca como cluster de un solo miembro. Desde la CLI:
 *   join HOST:PORT   une este nodo al cluster indicado
 *   inc              operacion Raft sincronizada: value += 1
 *   reset            operacion Raft sincronizada: value = 0
 *   get              lectura local no sincronizada
 */

#include "counter_cli.h"

#include "raft/raft.h"
#include "raft/raft_tcp_transport.h"
#include "rxnet/coop.h"
#include "rxnet/fsm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_DATA_DIR   "var/counter"
#define DEFAULT_HOST       "127.0.0.1"
#define TICK_US            10000L
#define ELECTION_TIMEOUT   500
#define HEARTBEAT_INTERVAL 100

typedef struct {
    char node_id[RAFT_MAX_ID];
    int  port;
    char host[256];
    char data_dir[256];
} counter_args_t;

typedef struct {
    rx_fsm_runtime       runtime;
    raft_cluster_t       cluster;
    counter_state_t      counter;
    raft_tcp_transport_t tcp;
    rx_coop_exec         coop;
    rx_fsm_machine       cli_machine;
    raft_node_t         *node;
    counter_cli_t        cli;
} counter_process_t;

static void usage(const char *argv0) {
    fprintf(stderr, "usage: %s --id NAME --port PORT [--host HOST] [--data DIR]\n", argv0);
    exit(1);
}

static counter_args_t parse_args(int argc, char **argv) {
    counter_args_t args;
    int i;
    memset(&args, 0, sizeof(args));
    strncpy(args.host, DEFAULT_HOST, sizeof(args.host) - 1);
    strncpy(args.data_dir, DEFAULT_DATA_DIR, sizeof(args.data_dir) - 1);

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            strncpy(args.node_id, argv[++i], RAFT_MAX_ID - 1);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            args.port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            strncpy(args.host, argv[++i], sizeof(args.host) - 1);
        } else if (strcmp(argv[i], "--data") == 0 && i + 1 < argc) {
            strncpy(args.data_dir, argv[++i], sizeof(args.data_dir) - 1);
        } else {
            usage(argv[0]);
        }
    }
    if (!args.node_id[0] || args.port <= 0) usage(argv[0]);
    return args;
}

static void counter_apply(void *user, const raft_command_t *command) {
    counter_state_t *counter = (counter_state_t *)user;
    if (strcmp(command->op, "inc") == 0) {
        counter->value += 1;
    } else if (strcmp(command->op, "reset") == 0) {
        counter->value = 0;
    }
}

static void counter_reload(void *user) {
    counter_state_t *counter = (counter_state_t *)user;
    counter->value = 0;
}

static size_t counter_snapshot(void *user, void *buf, size_t buf_size) {
    counter_state_t *counter = (counter_state_t *)user;
    int n = snprintf((char *)buf, buf_size, "%d\n", counter->value);
    if (n <= 0 || (size_t)n >= buf_size) return 0;
    return (size_t)n;
}

static void counter_restore_snapshot(void *user, const void *buf, size_t size) {
    counter_state_t *counter = (counter_state_t *)user;
    char tmp[32];
    size_t n = size < sizeof(tmp) - 1 ? size : sizeof(tmp) - 1;
    memcpy(tmp, buf, n);
    tmp[n] = '\0';
    counter->value = atoi(tmp);
}

static raft_application_t counter_make_application(counter_state_t *counter) {
    raft_application_t app;
    memset(&app, 0, sizeof(app));
    app.apply = counter_apply;
    app.reload = counter_reload;
    app.snapshot = counter_snapshot;
    app.restore_snapshot = counter_restore_snapshot;
    app.user = counter;
    return app;
}

static void make_raft_config(raft_node_config_t *cfg, const counter_args_t *args) {
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->node_id, args->node_id, RAFT_MAX_ID - 1);
    cfg->election_timeout_ms = ELECTION_TIMEOUT;
    cfg->heartbeat_interval_ms = HEARTBEAT_INTERVAL;
}

static int setup(counter_process_t *proc, const counter_args_t *args) {
    raft_node_config_t cfg;
    raft_application_t app;
    char node_dir[512];

    memset(proc, 0, sizeof(*proc));
    raft_tcp_transport_init(&proc->tcp, args->port);
    raft_tcp_transport_set_self(&proc->tcp, args->node_id, args->host);
    snprintf(node_dir, sizeof(node_dir), "%s/%s", args->data_dir, args->node_id);
    raft_tcp_transport_set_data_dir(&proc->tcp, node_dir);
    if (raft_tcp_transport_start(&proc->tcp) != 0) {
        fprintf(stderr, "failed to listen on port %d\n", proc->tcp.listen_port);
        return -1;
    }

    if (rx_fsm_runtime_init(&proc->runtime, 2) != 0) return -1;
    raft_cluster_init(&proc->cluster, &proc->runtime);
    raft_cluster_enable_realtime_clock(&proc->cluster);

    make_raft_config(&cfg, args);
    proc->counter.value = 0;
    app = counter_make_application(&proc->counter);
    proc->node = raft_cluster_add_node(&proc->cluster, &cfg, args->data_dir, &app, TICK_US);
    if (!proc->node) return -1;
    proc->node->transport = raft_tcp_transport_make(&proc->tcp);

    strncpy(proc->cli.node_id, args->node_id, RAFT_MAX_ID - 1);
    proc->cli.node = proc->node;
    proc->cli.tcp = &proc->tcp;
    proc->cli.counter = &proc->counter;
    proc->cli.coop = &proc->coop;
    proc->cli.prompt_needed = 1;
    proc->cli.last_role = proc->node->machine.state;
    proc->cli.last_term = proc->node->current_term;
    counter_cli_machine_init(&proc->cli_machine, &proc->cli);
    if (rx_fsm_runtime_add_machine(&proc->runtime, &proc->cli_machine, TICK_US, 0) != 0)
        return -1;

    rx_coop_exec_init(&proc->coop);
    if (rx_coop_exec_add(&proc->coop, &proc->runtime.runtime) != 0) return -1;
    return 0;
}

static void teardown(counter_process_t *proc) {
    raft_tcp_transport_stop(&proc->tcp);
    raft_cluster_destroy(&proc->cluster);
    rx_fsm_runtime_free(&proc->runtime);
}

int main(int argc, char **argv) {
    counter_args_t args = parse_args(argc, argv);
    static counter_process_t proc;
    if (setup(&proc, &args) != 0) return 1;
    printf("[%s] listening on %s:%d (type 'help')\n",
           args.node_id, proc.tcp.own_host, proc.tcp.listen_port);
    rx_coop_exec_run(&proc.coop);
    teardown(&proc);
    return 0;
}
