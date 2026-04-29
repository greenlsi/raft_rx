#include "raft/raft.h"
#include "raft/raft_kv_app.h"
#include "rxnet/fsm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static raft_node_config_t make_config(const char *node_id, const char *peer_a, const char *peer_b,
                                      int election_timeout_ms) {
    raft_node_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.node_id, node_id, RAFT_MAX_ID - 1);
    strncpy(config.peers[0], peer_a, RAFT_MAX_ID - 1);
    strncpy(config.peers[1], peer_b, RAFT_MAX_ID - 1);
    config.peer_count = 2;
    strncpy(config.initial_members[0], node_id, RAFT_MAX_ID - 1);
    strncpy(config.initial_members[1], peer_a,  RAFT_MAX_ID - 1);
    strncpy(config.initial_members[2], peer_b,  RAFT_MAX_ID - 1);
    config.initial_member_count  = 3;
    config.election_timeout_ms   = election_timeout_ms;
    config.heartbeat_interval_ms = 50;
    return config;
}

static raft_node_config_t make_joiner_config(const char *node_id, const char *peer_a, const char *peer_b,
                                             const char *peer_c, int election_timeout_ms) {
    raft_node_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.node_id, node_id, RAFT_MAX_ID - 1);
    strncpy(config.peers[0], peer_a, RAFT_MAX_ID - 1);
    strncpy(config.peers[1], peer_b, RAFT_MAX_ID - 1);
    strncpy(config.peers[2], peer_c, RAFT_MAX_ID - 1);
    config.peer_count = 3;
    strncpy(config.initial_members[0], peer_a, RAFT_MAX_ID - 1);
    strncpy(config.initial_members[1], peer_b, RAFT_MAX_ID - 1);
    strncpy(config.initial_members[2], peer_c, RAFT_MAX_ID - 1);
    config.initial_member_count  = 3;
    config.election_timeout_ms   = election_timeout_ms;
    config.heartbeat_interval_ms = 50;
    return config;
}

int main(void) {
    raft_cluster_t *cluster;
    rx_fsm_runtime  runtime;
    raft_command_t command;
    raft_node_t *leader;
    raft_node_config_t n1, n2, n3, n4;
    raft_kv_state_t kv[RAFT_MAX_NODES];
    raft_application_t apps[RAFT_MAX_NODES];
    rx_trace_buf_t trace;
    char add_members[4][RAFT_MAX_ID] = {"n1", "n2", "n3", "n4"};
    size_t i;

    cluster = calloc(1, sizeof(*cluster));
    if (cluster == NULL) return 1;

    if (rx_fsm_runtime_init(&runtime, RAFT_MAX_NODES) != 0) return 1;
    if (raft_cluster_init(cluster, &runtime) != 0) return 1;

    rx_trace_init(&trace, 1);
    if (raft_cluster_attach_trace(cluster, &trace) != 0) return 1;

    n1 = make_config("n1", "n2", "n3", 150);
    n2 = make_config("n2", "n1", "n3", 250);
    n3 = make_config("n3", "n1", "n2", 350);
    n4 = make_joiner_config("n4", "n1", "n2", "n3", 450);

    for (i = 0; i < RAFT_MAX_NODES; ++i) {
        raft_kv_state_init(&kv[i]);
        apps[i] = raft_kv_make_application(&kv[i]);
    }

    raft_cluster_add_node(cluster, &n1, "var/c-trace-example", &apps[0], 0);
    raft_cluster_add_node(cluster, &n2, "var/c-trace-example", &apps[1], 0);
    raft_cluster_add_node(cluster, &n3, "var/c-trace-example", &apps[2], 0);

    for (i = 0; i < 80; ++i)
        raft_cluster_tick(cluster, 10);

    leader = raft_cluster_leader(cluster);
    if (leader == NULL) {
        fprintf(stderr, "no leader elected\n");
        return 1;
    }

    memset(&command, 0, sizeof(command));
    strcpy(command.op,    "set");
    strcpy(command.key,   "color");
    strcpy(command.value, "blue");
    raft_node_submit_command(leader, &command);

    memset(&command, 0, sizeof(command));
    strcpy(command.op,    "set");
    strcpy(command.key,   "mode");
    strcpy(command.value, "active");
    raft_node_submit_command(leader, &command);

    for (i = 0; i < 80; ++i)
        raft_cluster_tick(cluster, 10);

    raft_cluster_add_node(cluster, &n4, "var/c-trace-example", &apps[3], 0);
    raft_node_request_membership_change(leader, add_members, 4);

    for (i = 0; i < 160; ++i)
        raft_cluster_tick(cluster, 10);

    for (i = 0; i < cluster->node_count; ++i) {
        raft_node_t *node = &cluster->nodes[i];
        printf("%s role=%d term=%d cfg_members=%zu color=%s mode=%s\n",
               node->config.node_id,
               node->machine.state,
               node->current_term,
               node->config_state.old_count,
               raft_kv_get(&kv[i], "color"),
               raft_kv_get(&kv[i], "mode"));
    }

    if (rx_trace_export(&trace, "var/c-trace-example/trace.bin") != 0) {
        fprintf(stderr, "could not export trace\n");
        return 1;
    }
    printf("trace=var/c-trace-example/trace.bin\n");

    raft_cluster_destroy(cluster);
    rx_fsm_runtime_free(&runtime);
    free(cluster);
    return 0;
}
