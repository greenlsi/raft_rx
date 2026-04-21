#include "raft/raft.h"

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
    config.election_timeout_ms = election_timeout_ms;
    config.heartbeat_interval_ms = 50;
    return config;
}

int main(void) {
    raft_cluster_t *cluster;
    raft_command_t command;
    raft_node_t *leader;
    raft_node_config_t n1;
    raft_node_config_t n2;
    raft_node_config_t n3;
    size_t i;

    cluster = calloc(1, sizeof(*cluster));
    if (cluster == NULL) {
        return 1;
    }
    raft_cluster_init(cluster);
    n1 = make_config("n1", "n2", "n3", 150);
    n2 = make_config("n2", "n1", "n3", 250);
    n3 = make_config("n3", "n1", "n2", 350);
    raft_cluster_add_node(cluster, &n1, "var/c-example");
    raft_cluster_add_node(cluster, &n2, "var/c-example");
    raft_cluster_add_node(cluster, &n3, "var/c-example");

    for (i = 0; i < 80; ++i) {
        raft_cluster_tick(cluster, 10);
    }

    leader = raft_cluster_leader(cluster);
    if (leader == NULL) {
        fprintf(stderr, "no leader elected\n");
        return 1;
    }

    memset(&command, 0, sizeof(command));
    strcpy(command.op, "set");
    strcpy(command.key, "color");
    strcpy(command.value, "blue");
    raft_node_submit_command(leader, &command);

    for (i = 0; i < 80; ++i) {
        raft_cluster_tick(cluster, 10);
    }

    for (i = 0; i < cluster->node_count; ++i) {
        raft_node_t *node = &cluster->nodes[i];
        printf("%s role=%d term=%d color=%s\n",
               node->config.node_id,
               node->machine.state,
               node->current_term,
               raft_node_get(node, "color"));
    }

    raft_cluster_destroy(cluster);
    free(cluster);
    return 0;
}
