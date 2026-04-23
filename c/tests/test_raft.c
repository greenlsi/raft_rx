#include "raft/raft.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static raft_node_config_t make_config(const char *node_id, const char *peer_a, const char *peer_b,
                                      int election_timeout_ms) {
    raft_node_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.node_id, node_id, RAFT_MAX_ID - 1);
    strncpy(config.peers[0], peer_a, RAFT_MAX_ID - 1);
    strncpy(config.peers[1], peer_b, RAFT_MAX_ID - 1);
    config.peer_count = 2;
    strncpy(config.initial_members[0], node_id, RAFT_MAX_ID - 1);
    strncpy(config.initial_members[1], peer_a, RAFT_MAX_ID - 1);
    strncpy(config.initial_members[2], peer_b, RAFT_MAX_ID - 1);
    config.initial_member_count = 3;
    config.election_timeout_ms = election_timeout_ms;
    config.heartbeat_interval_ms = 50;
    return config;
}

static raft_node_config_t make_config4(const char *node_id, const char *peer_a, const char *peer_b,
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
    config.initial_member_count = 3;
    config.election_timeout_ms = election_timeout_ms;
    config.heartbeat_interval_ms = 50;
    return config;
}

static void build_cluster(raft_cluster_t *cluster, const char *root) {
    raft_node_config_t n1;
    raft_node_config_t n2;
    raft_node_config_t n3;
    assert(raft_cluster_init(cluster) == 0);
    n1 = make_config("n1", "n2", "n3", 150);
    n2 = make_config("n2", "n1", "n3", 250);
    n3 = make_config("n3", "n1", "n2", 350);
    assert(raft_cluster_add_node(cluster, &n1, root) != NULL);
    assert(raft_cluster_add_node(cluster, &n2, root) != NULL);
    assert(raft_cluster_add_node(cluster, &n3, root) != NULL);
}

int main(void) {
    char root[] = "/tmp/raft-c-test-XXXXXX";
    raft_cluster_t *cluster;
    raft_cluster_t *restarted;
    raft_node_t *leader;
    raft_node_t *n4;
    raft_command_t command;
    size_t i;
    char add_members[4][RAFT_MAX_ID] = {"n1", "n2", "n3", "n4"};
    raft_node_config_t n4_config;

    assert(mkdtemp(root) != NULL);
    cluster = calloc(1, sizeof(*cluster));
    restarted = calloc(1, sizeof(*restarted));
    assert(cluster != NULL);
    assert(restarted != NULL);
    build_cluster(cluster, root);

    for (i = 0; i < 80; ++i) {
        assert(raft_cluster_tick(cluster, 10) == 0);
    }
    leader = raft_cluster_leader(cluster);
    assert(leader != NULL);
    assert(strcmp(leader->config.node_id, "n1") == 0);

    memset(&command, 0, sizeof(command));
    strcpy(command.op, "set");
    strcpy(command.key, "alpha");
    strcpy(command.value, "1");
    assert(raft_node_submit_command(leader, &command) == 0);

    for (i = 0; i < 80; ++i) {
        assert(raft_cluster_tick(cluster, 10) == 0);
    }
    for (i = 0; i < cluster->node_count; ++i) {
        assert(strcmp(raft_node_get(&cluster->nodes[i], "alpha"), "1") == 0);
    }

    n4_config = make_config4("n4", "n1", "n2", "n3", 450);
    n4 = raft_cluster_add_node(cluster, &n4_config, root);
    assert(n4 != NULL);
    assert(raft_node_request_membership_change(leader, add_members, 4) == 0);

    for (i = 0; i < 160; ++i) {
        assert(raft_cluster_tick(cluster, 10) == 0);
    }

    for (i = 0; i < cluster->node_count; ++i) {
        raft_node_t *node = &cluster->nodes[i];
        assert(node->config_state.old_count == 4);
        assert(node->config_state.new_count == 0);
        assert(strcmp(raft_node_get(node, "alpha"), "1") == 0);
    }

    raft_cluster_destroy(cluster);

    build_cluster(restarted, root);
    n4 = raft_cluster_add_node(restarted, &n4_config, root);
    assert(n4 != NULL);
    for (i = 0; i < restarted->node_count; ++i) {
        assert(strcmp(raft_node_get(&restarted->nodes[i], "alpha"), "1") == 0);
        assert(restarted->nodes[i].config_state.old_count == 4);
        assert(restarted->nodes[i].config_state.new_count == 0);
    }
    raft_cluster_destroy(restarted);
    free(cluster);
    free(restarted);
    printf("ok\n");
    return 0;
}
