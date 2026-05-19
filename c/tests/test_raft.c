// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "raft/raft.h"
#include "raft/raft_kv_app.h"
#include "rxnet/fsm.h"

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
    strncpy(config.initial_members[1], peer_a,  RAFT_MAX_ID - 1);
    strncpy(config.initial_members[2], peer_b,  RAFT_MAX_ID - 1);
    config.initial_member_count   = 3;
    config.election_timeout_ms    = election_timeout_ms;
    config.heartbeat_interval_ms  = 50;
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
    config.initial_member_count   = 3;
    config.election_timeout_ms    = election_timeout_ms;
    config.heartbeat_interval_ms  = 50;
    return config;
}

static raft_node_config_t make_single_config(const char *node_id, int election_timeout_ms) {
    raft_node_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.node_id, node_id, RAFT_MAX_ID - 1);
    strncpy(config.initial_members[0], node_id, RAFT_MAX_ID - 1);
    config.initial_member_count   = 1;
    config.election_timeout_ms    = election_timeout_ms;
    config.heartbeat_interval_ms  = 50;
    return config;
}

static raft_node_config_t make_learner_config(const char *node_id, int election_timeout_ms) {
    raft_node_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.node_id, node_id, RAFT_MAX_ID - 1);
    config.learner                = 1;
    config.election_timeout_ms    = election_timeout_ms;
    config.heartbeat_interval_ms  = 50;
    return config;
}

typedef struct {
    int value;
} test_counter_t;

static void test_counter_apply(void *user, const raft_command_t *command) {
    test_counter_t *counter = (test_counter_t *)user;
    if (strcmp(command->op, "inc") == 0)
        counter->value++;
}

static void test_counter_reload(void *user) {
    ((test_counter_t *)user)->value = 0;
}

static size_t test_counter_snapshot(void *user, void *buf, size_t buf_size) {
    test_counter_t *counter = (test_counter_t *)user;
    int n = snprintf((char *)buf, buf_size, "%d\n", counter->value);
    return (n > 0 && (size_t)n < buf_size) ? (size_t)n : 0;
}

static void test_counter_restore_snapshot(void *user, const void *buf, size_t size) {
    test_counter_t *counter = (test_counter_t *)user;
    char tmp[32];
    size_t n = size < sizeof(tmp) - 1 ? size : sizeof(tmp) - 1;
    memcpy(tmp, buf, n);
    tmp[n] = '\0';
    counter->value = atoi(tmp);
}

static raft_application_t test_counter_make_application(test_counter_t *counter) {
    raft_application_t app;
    memset(&app, 0, sizeof(app));
    app.apply = test_counter_apply;
    app.reload = test_counter_reload;
    app.snapshot = test_counter_snapshot;
    app.restore_snapshot = test_counter_restore_snapshot;
    app.user = counter;
    return app;
}

/* Adds 3 initial nodes to cluster; kv[] must have room for at least 3 entries. */
static void build_cluster(raft_cluster_t *cluster, rx_fsm_runtime *rt,
                          const char *root, raft_kv_state_t kv[]) {
    raft_node_config_t n1 = make_config("n1", "n2", "n3", 150);
    raft_node_config_t n2 = make_config("n2", "n1", "n3", 250);
    raft_node_config_t n3 = make_config("n3", "n1", "n2", 350);
    raft_application_t a[3];
    int i;

    assert(rx_fsm_runtime_init(rt, RAFT_MAX_NODES) == 0);
    assert(raft_cluster_init(cluster, rt) == 0);
    for (i = 0; i < 3; ++i) {
        raft_kv_state_init(&kv[i]);
        a[i] = raft_kv_make_application(&kv[i]);
    }
    assert(raft_cluster_add_node(cluster, &n1, root, &a[0], 0) != NULL);
    assert(raft_cluster_add_node(cluster, &n2, root, &a[1], 0) != NULL);
    assert(raft_cluster_add_node(cluster, &n3, root, &a[2], 0) != NULL);
}

int main(void) {
    char root[] = "/tmp/raft-c-test-XXXXXX";
    raft_cluster_t *cluster;
    raft_cluster_t *restarted;
    raft_cluster_t *sequential;
    raft_cluster_t *sequential_restarted;
    rx_fsm_runtime  rt1, rt2;
    rx_fsm_runtime  rt3, rt4;
    raft_node_t *leader;
    raft_node_t *n4;
    raft_command_t command;
    raft_kv_state_t kv1[RAFT_MAX_NODES];
    raft_kv_state_t kv2[RAFT_MAX_NODES];
    raft_application_t app_n4;
    size_t i;
    char add_members[4][RAFT_MAX_ID] = {"n1", "n2", "n3", "n4"};
    raft_node_config_t n4_config;
    char root_seq[] = "/tmp/raft-c-seq-test-XXXXXX";
    char root_counter[] = "/tmp/raft-c-counter-test-XXXXXX";

    assert(mkdtemp(root) != NULL);
    assert(mkdtemp(root_seq) != NULL);
    assert(mkdtemp(root_counter) != NULL);
    cluster   = calloc(1, sizeof(*cluster));
    restarted = calloc(1, sizeof(*restarted));
    sequential = calloc(1, sizeof(*sequential));
    sequential_restarted = calloc(1, sizeof(*sequential_restarted));
    assert(cluster != NULL);
    assert(restarted != NULL);
    assert(sequential != NULL);
    assert(sequential_restarted != NULL);

    build_cluster(cluster, &rt1, root, kv1);

    for (i = 0; i < 80; ++i)
        assert(raft_cluster_tick(cluster, 10) == 0);

    leader = raft_cluster_leader(cluster);
    assert(leader != NULL);
    assert(strcmp(leader->config.node_id, "n1") == 0);

    memset(&command, 0, sizeof(command));
    strcpy(command.op,    "set");
    strcpy(command.key,   "alpha");
    strcpy(command.value, "1");
    assert(raft_node_submit_command(leader, &command) == 0);

    for (i = 0; i < 80; ++i)
        assert(raft_cluster_tick(cluster, 10) == 0);

    for (i = 0; i < cluster->node_count; ++i)
        assert(strcmp(raft_kv_get(&kv1[i], "alpha"), "1") == 0);

    n4_config = make_config4("n4", "n1", "n2", "n3", 450);
    raft_kv_state_init(&kv1[3]);
    app_n4 = raft_kv_make_application(&kv1[3]);
    n4 = raft_cluster_add_node(cluster, &n4_config, root, &app_n4, 0);
    assert(n4 != NULL);
    assert(raft_node_request_membership_change(leader, add_members, 4) == 0);

    for (i = 0; i < 160; ++i)
        assert(raft_cluster_tick(cluster, 10) == 0);

    for (i = 0; i < cluster->node_count; ++i) {
        raft_node_t *node = &cluster->nodes[i];
        assert(node->config_state.old_count == 4);
        assert(node->config_state.new_count == 0);
        assert(strcmp(raft_kv_get(&kv1[i], "alpha"), "1") == 0);
    }

    {
        raft_node_t *rejoining = &cluster->nodes[2];
        raft_command_t second;

        raft_node_stop(rejoining);

        memset(&second, 0, sizeof(second));
        strcpy(second.op,    "set");
        strcpy(second.key,   "beta");
        strcpy(second.value, "2");
        assert(raft_node_submit_command(leader, &second) == 0);

        for (i = 0; i < 80; ++i)
            assert(raft_cluster_tick(cluster, 10) == 0);

        assert(leader->commit_index > rejoining->commit_index);
        assert(strcmp(raft_kv_get(&kv1[0], "beta"), "2") == 0);
        assert(strcmp(raft_kv_get(&kv1[1], "beta"), "2") == 0);
        assert(strcmp(raft_kv_get(&kv1[3], "beta"), "2") == 0);

        raft_node_reset_for_join(rejoining);
        raft_kv_state_init(&kv1[2]);

        for (i = 0; i < 240; ++i)
            assert(raft_cluster_tick(cluster, 10) == 0);

        assert(rejoining->commit_index == leader->commit_index);
        assert(strcmp(raft_kv_get(&kv1[2], "alpha"), "1") == 0);
        assert(strcmp(raft_kv_get(&kv1[2], "beta"), "2") == 0);
    }

    raft_cluster_destroy(cluster);
    rx_fsm_runtime_free(&rt1);

    /* Restart: reload state from disk and verify persistence */
    build_cluster(restarted, &rt2, root, kv2);
    raft_kv_state_init(&kv2[3]);
    app_n4 = raft_kv_make_application(&kv2[3]);
    n4 = raft_cluster_add_node(restarted, &n4_config, root, &app_n4, 0);
    assert(n4 != NULL);

    /* One tick lets each node apply committed entries from the restored log */
    assert(raft_cluster_tick(restarted, 1) == 0);

    for (i = 0; i < restarted->node_count; ++i) {
        assert(strcmp(raft_kv_get(&kv2[i], "alpha"), "1") == 0);
        assert(restarted->nodes[i].config_state.old_count == 4);
        assert(restarted->nodes[i].config_state.new_count == 0);
        assert(restarted->nodes[i].log_count > 0);
        assert(restarted->nodes[i].log[0].index == 1);
    }

    {
        raft_node_t *node = &restarted->nodes[0];
        size_t before_count = node->log_count;
        assert(before_count > 0);

        raft_node_stop(node);
        raft_node_start(node);

        assert(node->log_count == before_count);
        for (i = 0; i < node->log_count; ++i)
            assert(node->log[i].index == (int)i + 1);
    }

    raft_cluster_destroy(restarted);
    rx_fsm_runtime_free(&rt2);

    {
        raft_node_config_t s1 = make_single_config("n1", 150);
        raft_node_config_t s2 = make_learner_config("n2", 250);
        raft_node_config_t s3 = make_learner_config("n3", 350);
        raft_node_config_t r1 = make_single_config("n1", 150);
        raft_node_config_t r2 = make_learner_config("n2", 250);
        raft_node_config_t r3 = make_learner_config("n3", 350);
        raft_application_t seq_apps[3];
        raft_application_t restart_apps[3];
        raft_kv_state_t seq_kv[3];
        raft_kv_state_t restart_kv[3];
        char add_n2[2][RAFT_MAX_ID] = {"n1", "n2"};
        char add_n3[3][RAFT_MAX_ID] = {"n1", "n2", "n3"};
        raft_node_t *seq_leader;

        assert(rx_fsm_runtime_init(&rt3, RAFT_MAX_NODES) == 0);
        assert(raft_cluster_init(sequential, &rt3) == 0);
        for (i = 0; i < 3; ++i) {
            raft_kv_state_init(&seq_kv[i]);
            seq_apps[i] = raft_kv_make_application(&seq_kv[i]);
        }
        assert(raft_cluster_add_node(sequential, &s1, root_seq, &seq_apps[0], 0) != NULL);
        for (i = 0; i < 80; ++i)
            assert(raft_cluster_tick(sequential, 10) == 0);
        seq_leader = raft_cluster_leader(sequential);
        assert(seq_leader != NULL);

        assert(raft_cluster_add_node(sequential, &s2, root_seq, &seq_apps[1], 0) != NULL);
        assert(raft_node_request_membership_change(seq_leader, add_n2, 2) == 0);
        for (i = 0; i < 160; ++i)
            assert(raft_cluster_tick(sequential, 10) == 0);

        assert(raft_cluster_add_node(sequential, &s3, root_seq, &seq_apps[2], 0) != NULL);
        assert(raft_node_request_membership_change(seq_leader, add_n3, 3) == 0);
        for (i = 0; i < 160; ++i)
            assert(raft_cluster_tick(sequential, 10) == 0);

        raft_cluster_destroy(sequential);
        rx_fsm_runtime_free(&rt3);

        assert(rx_fsm_runtime_init(&rt4, RAFT_MAX_NODES) == 0);
        assert(raft_cluster_init(sequential_restarted, &rt4) == 0);
        for (i = 0; i < 3; ++i) {
            raft_kv_state_init(&restart_kv[i]);
            restart_apps[i] = raft_kv_make_application(&restart_kv[i]);
        }
        r2.learner = 0;
        r3.learner = 0;
        assert(raft_cluster_add_node(sequential_restarted, &r1, root_seq, &restart_apps[0], 0) != NULL);
        assert(raft_cluster_add_node(sequential_restarted, &r2, root_seq, &restart_apps[1], 0) != NULL);
        assert(raft_cluster_add_node(sequential_restarted, &r3, root_seq, &restart_apps[2], 0) != NULL);

        assert(raft_cluster_tick(sequential_restarted, 1) == 0);

        assert(sequential_restarted->nodes[2].running);
        assert(sequential_restarted->nodes[2].config_state.old_count == 3);

        raft_cluster_destroy(sequential_restarted);
        rx_fsm_runtime_free(&rt4);
    }

    {
        raft_cluster_t *counter_cluster = calloc(1, sizeof(*counter_cluster));
        raft_cluster_t *counter_restarted = calloc(1, sizeof(*counter_restarted));
        rx_fsm_runtime rt_counter;
        rx_fsm_runtime rt_counter_restart;
        raft_node_config_t c1 = make_single_config("n1", 150);
        test_counter_t counter_state;
        test_counter_t restarted_counter_state;
        raft_application_t counter_app;
        raft_application_t restarted_counter_app;
        raft_node_t *counter_leader;
        raft_command_t inc;

        assert(counter_cluster != NULL);
        assert(counter_restarted != NULL);
        assert(rx_fsm_runtime_init(&rt_counter, RAFT_MAX_NODES) == 0);
        assert(raft_cluster_init(counter_cluster, &rt_counter) == 0);
        counter_app = test_counter_make_application(&counter_state);
        assert(raft_cluster_add_node(counter_cluster, &c1, root_counter, &counter_app, 0) != NULL);
        for (i = 0; i < 80; ++i)
            assert(raft_cluster_tick(counter_cluster, 10) == 0);
        counter_leader = raft_cluster_leader(counter_cluster);
        assert(counter_leader != NULL);

        memset(&inc, 0, sizeof(inc));
        strcpy(inc.op, "inc");
        assert(raft_node_submit_command(counter_leader, &inc) == 0);
        assert(raft_node_submit_command(counter_leader, &inc) == 0);
        for (i = 0; i < 80; ++i)
            assert(raft_cluster_tick(counter_cluster, 10) == 0);
        assert(counter_state.value == 2);

        raft_cluster_destroy(counter_cluster);
        rx_fsm_runtime_free(&rt_counter);
        free(counter_cluster);

        assert(rx_fsm_runtime_init(&rt_counter_restart, RAFT_MAX_NODES) == 0);
        assert(raft_cluster_init(counter_restarted, &rt_counter_restart) == 0);
        restarted_counter_app = test_counter_make_application(&restarted_counter_state);
        assert(raft_cluster_add_node(counter_restarted, &c1, root_counter,
                                     &restarted_counter_app, 0) != NULL);
        assert(raft_cluster_tick(counter_restarted, 1) == 0);
        assert(restarted_counter_state.value == 2);

        raft_cluster_destroy(counter_restarted);
        rx_fsm_runtime_free(&rt_counter_restart);
        free(counter_restarted);
    }

    {
        char root_merge[] = "/tmp/raft-c-merge-test-XXXXXX";
        raft_cluster_t *merge_cluster = calloc(1, sizeof(*merge_cluster));
        rx_fsm_runtime rt_merge;
        raft_node_config_t m1 = make_single_config("n1", 150);
        raft_kv_state_t merge_kv;
        raft_application_t merge_app;
        raft_node_t *follower;
        raft_command_t old_a, old_b, winning_c;
        raft_message_t append;

        assert(merge_cluster != NULL);
        assert(mkdtemp(root_merge) != NULL);
        assert(rx_fsm_runtime_init(&rt_merge, RAFT_MAX_NODES) == 0);
        assert(raft_cluster_init(merge_cluster, &rt_merge) == 0);
        raft_kv_state_init(&merge_kv);
        merge_app = raft_kv_make_application(&merge_kv);
        follower = raft_cluster_add_node(merge_cluster, &m1, root_merge, &merge_app, 0);
        assert(follower != NULL);

        memset(&old_a, 0, sizeof(old_a));
        strcpy(old_a.op, "set");
        strcpy(old_a.key, "a");
        strcpy(old_a.value, "1");
        memset(&old_b, 0, sizeof(old_b));
        strcpy(old_b.op, "set");
        strcpy(old_b.key, "b");
        strcpy(old_b.value, "2");

        follower->log_count = 2;
        follower->log[0].index = 1;
        follower->log[0].term = 1;
        strcpy(follower->log[0].leader_id, "n1");
        follower->log[0].command = old_a;
        follower->log[1].index = 2;
        follower->log[1].term = 1;
        strcpy(follower->log[1].leader_id, "n1");
        follower->log[1].command = old_b;
        follower->commit_index = 2;
        follower->last_applied = 2;
        merge_app.apply(merge_app.user, &old_a);
        merge_app.apply(merge_app.user, &old_b);

        memset(&winning_c, 0, sizeof(winning_c));
        strcpy(winning_c.op, "set");
        strcpy(winning_c.key, "c");
        strcpy(winning_c.value, "3");
        memset(&append, 0, sizeof(append));
        append.kind = RAFT_MSG_APPEND_ENTRIES;
        append.term = 2;
        strcpy(append.source, "n3");
        strcpy(append.target, "n1");
        append.prev_log_index = 0;
        append.prev_log_term = 0;
        append.leader_commit = 1;
        append.entry_count = 1;
        append.entries[0].index = 1;
        append.entries[0].term = 1;
        strcpy(append.entries[0].leader_id, "n3");
        append.entries[0].command = winning_c;
        assert(raft_memory_transport_send(&merge_cluster->transport, &append) == 0);

        assert(raft_cluster_tick(merge_cluster, 10) == 0);
        assert(raft_cluster_tick(merge_cluster, 10) == 0);

        assert(follower->commit_index == 1);
        assert(follower->last_applied == 1);
        assert(strcmp(raft_kv_get(&merge_kv, "a"), "") == 0);
        assert(strcmp(raft_kv_get(&merge_kv, "b"), "") == 0);
        assert(strcmp(raft_kv_get(&merge_kv, "c"), "3") == 0);

        raft_cluster_destroy(merge_cluster);
        rx_fsm_runtime_free(&rt_merge);
        free(merge_cluster);
    }

    free(cluster);
    free(restarted);
    free(sequential);
    free(sequential_restarted);
    printf("ok\n");
    return 0;
}
