#include "raft/raft.h"
#include "raft/raft_kv_app.h"
#include "rxnet/coop.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PERIOD_US 10000L  /* 10 ms tick */

/* ── Orchestrator FSM ────────────────────────────────────────────────────────
 *
 * Drives the demo sequence as a state machine running alongside the raft
 * nodes in the same rx_coop_exec loop.  All four nodes are pre-registered
 * before the loop starts; n4 begins as a non-member and is admitted via a
 * membership-change request.
 */

typedef enum {
    DEMO_WAITING_LEADER = 0,
    DEMO_WAITING_COMMIT,
    DEMO_WAITING_MEMBERSHIP,
} demo_state_t;

typedef struct {
    raft_cluster_t *cluster;
    raft_kv_state_t *kv;
    rx_coop_exec   *ce;
} demo_user_t;

/* ── Guards ──────────────────────────────────────────────────────────────── */

static int guard_leader_ready(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    return raft_cluster_leader(((demo_user_t *)user)->cluster) != NULL;
}

static int guard_color_committed(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    const char *v = raft_kv_get(&((demo_user_t *)user)->kv[0], "color");
    return v && strcmp(v, "blue") == 0;
}

static int guard_membership_stable(const rx_fsm_context *ctx, void *user) {
    demo_user_t *d = (demo_user_t *)user;
    size_t i;
    (void)ctx;
    if (d->cluster->node_count < 4) return 0;
    for (i = 0; i < d->cluster->node_count; ++i) {
        const raft_node_t *n = &d->cluster->nodes[i];
        if (n->config_state.old_count != 4 || n->config_state.new_count != 0)
            return 0;
    }
    return 1;
}

/* ── Actions ─────────────────────────────────────────────────────────────── */

static void action_submit_command(rx_fsm_context *ctx, void *user) {
    raft_command_t cmd;
    raft_node_t *leader = raft_cluster_leader(((demo_user_t *)user)->cluster);
    (void)ctx;
    memset(&cmd, 0, sizeof(cmd));
    strcpy(cmd.op,    "set");
    strcpy(cmd.key,   "color");
    strcpy(cmd.value, "blue");
    raft_node_submit_command(leader, &cmd);
}

static void action_request_membership(rx_fsm_context *ctx, void *user) {
    demo_user_t *d = (demo_user_t *)user;
    raft_node_t *leader = raft_cluster_leader(d->cluster);
    char members[4][RAFT_MAX_ID] = {"n1", "n2", "n3", "n4"};
    (void)ctx;
    raft_node_request_membership_change(leader, members, 4);
}

static void action_print_and_stop(rx_fsm_context *ctx, void *user) {
    demo_user_t *d = (demo_user_t *)user;
    size_t i;
    (void)ctx;
    for (i = 0; i < d->cluster->node_count; ++i) {
        const raft_node_t *n = &d->cluster->nodes[i];
        printf("%s role=%d term=%d cfg_members=%zu color=%s\n",
               n->config.node_id,
               n->machine.state,
               n->current_term,
               n->config_state.old_count,
               raft_kv_get(&d->kv[i], "color"));
    }
    rx_coop_exec_stop(d->ce);
}

/* ── Transition table ────────────────────────────────────────────────────── */

static const rx_fsm_transition DEMO_TRANSITIONS[] = {
    {DEMO_WAITING_LEADER,     DEMO_WAITING_COMMIT,      guard_leader_ready,        action_submit_command},
    {DEMO_WAITING_COMMIT,     DEMO_WAITING_MEMBERSHIP,  guard_color_committed,     action_request_membership},
    {DEMO_WAITING_MEMBERSHIP, DEMO_WAITING_MEMBERSHIP,  guard_membership_stable,   action_print_and_stop},
};

/* ── Config helpers ──────────────────────────────────────────────────────── */

static raft_node_config_t make_config(const char *id, const char *pa, const char *pb,
                                      int election_timeout_ms) {
    raft_node_config_t c;
    memset(&c, 0, sizeof(c));
    strncpy(c.node_id,          id, RAFT_MAX_ID - 1);
    strncpy(c.peers[0],         pa, RAFT_MAX_ID - 1);
    strncpy(c.peers[1],         pb, RAFT_MAX_ID - 1);
    c.peer_count = 2;
    strncpy(c.initial_members[0], id, RAFT_MAX_ID - 1);
    strncpy(c.initial_members[1], pa, RAFT_MAX_ID - 1);
    strncpy(c.initial_members[2], pb, RAFT_MAX_ID - 1);
    c.initial_member_count   = 3;
    c.election_timeout_ms    = election_timeout_ms;
    c.heartbeat_interval_ms  = 50;
    return c;
}

static raft_node_config_t make_joiner_config(const char *id, const char *pa, const char *pb,
                                             const char *pc, int election_timeout_ms) {
    raft_node_config_t c;
    memset(&c, 0, sizeof(c));
    strncpy(c.node_id,  id, RAFT_MAX_ID - 1);
    strncpy(c.peers[0], pa, RAFT_MAX_ID - 1);
    strncpy(c.peers[1], pb, RAFT_MAX_ID - 1);
    strncpy(c.peers[2], pc, RAFT_MAX_ID - 1);
    c.peer_count = 3;
    strncpy(c.initial_members[0], pa, RAFT_MAX_ID - 1);
    strncpy(c.initial_members[1], pb, RAFT_MAX_ID - 1);
    strncpy(c.initial_members[2], pc, RAFT_MAX_ID - 1);
    c.initial_member_count   = 3;
    c.election_timeout_ms    = election_timeout_ms;
    c.heartbeat_interval_ms  = 50;
    return c;
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void) {
    raft_cluster_t  *cluster;
    raft_kv_state_t  kv[RAFT_MAX_NODES];
    raft_application_t apps[RAFT_MAX_NODES];
    raft_node_config_t n1, n2, n3, n4;
    rx_fsm_runtime   runtime;
    rx_fsm_machine   demo_machine;
    rx_coop_exec     ce;
    demo_user_t      demo_user;
    size_t i;

    cluster = calloc(1, sizeof(*cluster));
    if (cluster == NULL) return 1;

    /* All raft nodes + orchestrator share one runtime. */
    if (rx_fsm_runtime_init(&runtime, RAFT_MAX_NODES + 1) != 0) return 1;

    raft_cluster_init(cluster, &runtime);

    /* Use real wall-clock time for timeouts/heartbeats — must be set BEFORE
     * adding nodes so election deadlines are relative to real time. */
    raft_cluster_enable_realtime_clock(cluster);

    n1 = make_config("n1", "n2", "n3", 150);
    n2 = make_config("n2", "n1", "n3", 250);
    n3 = make_config("n3", "n1", "n2", 350);
    n4 = make_joiner_config("n4", "n1", "n2", "n3", 450);

    for (i = 0; i < RAFT_MAX_NODES; ++i) {
        raft_kv_state_init(&kv[i]);
        apps[i] = raft_kv_make_application(&kv[i]);
    }

    /* Pre-register all nodes.  n4 starts as a non-member joiner and is
     * admitted to the cluster when the orchestrator requests it. */
    raft_cluster_add_node(cluster, &n1, "var/demo_app", &apps[0], PERIOD_US);
    raft_cluster_add_node(cluster, &n2, "var/demo_app", &apps[1], PERIOD_US);
    raft_cluster_add_node(cluster, &n3, "var/demo_app", &apps[2], PERIOD_US);
    raft_cluster_add_node(cluster, &n4, "var/demo_app", &apps[3], PERIOD_US);

    demo_user.cluster = cluster;
    demo_user.kv      = kv;
    demo_user.ce      = &ce;

    rx_fsm_machine_init(&demo_machine, "demo",
                        DEMO_WAITING_LEADER,
                        DEMO_TRANSITIONS,
                        sizeof(DEMO_TRANSITIONS) / sizeof(DEMO_TRANSITIONS[0]),
                        &demo_user, NULL, NULL);
    rx_fsm_runtime_add_machine(&runtime, &demo_machine, PERIOD_US, 0);

    rx_coop_exec_init(&ce);
    rx_coop_exec_add(&ce, &runtime.runtime);
    rx_coop_exec_run(&ce);

    raft_cluster_destroy(cluster);
    rx_fsm_runtime_free(&runtime);
    free(cluster);
    return 0;
}
