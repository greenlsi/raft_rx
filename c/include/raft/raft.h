// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "rxnet/fsm.h"
#include "rxnet/trace.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RAFT_MAX_NODES 8
#define RAFT_MAX_PEERS 7
#define RAFT_MAX_ID 16
#define RAFT_MAX_LOG 128
#define RAFT_MAX_QUEUE 256
#define RAFT_MAX_BATCH 16
#define RAFT_MAX_KEY 64
#define RAFT_MAX_VALUE 128

#ifndef RAFT_MAX_SNAPSHOT_SIZE
#define RAFT_MAX_SNAPSHOT_SIZE 4096
#endif

typedef enum {
    RAFT_ROLE_FOLLOWER = 0,
    RAFT_ROLE_CANDIDATE = 1,
    RAFT_ROLE_LEADER = 2,
} raft_role_t;

typedef enum {
    RAFT_MEMBERSHIP_STABLE = 0,
    RAFT_MEMBERSHIP_JOINT_PENDING = 1,
    RAFT_MEMBERSHIP_JOINT = 2,
    RAFT_MEMBERSHIP_FINALIZING = 3,
} raft_membership_state_t;

typedef enum {
    RAFT_MSG_REQUEST_VOTE = 0,
    RAFT_MSG_REQUEST_VOTE_RESPONSE = 1,
    RAFT_MSG_APPEND_ENTRIES = 2,
    RAFT_MSG_APPEND_ENTRIES_RESPONSE = 3,
} raft_message_kind_t;

typedef struct {
    char op[32];
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
} raft_command_t;

typedef struct {
    int index;
    int term;
    char leader_id[RAFT_MAX_ID]; /* node that created this entry */
    raft_command_t command;
} raft_log_entry_t;

typedef struct {
    raft_message_kind_t kind;
    int term;
    char source[RAFT_MAX_ID];
    char target[RAFT_MAX_ID];
    int granted;
    int success;
    int prev_log_index;
    int prev_log_term;
    char prev_log_leader_id[RAFT_MAX_ID]; /* leader_id of the prev entry */
    int leader_commit;
    int match_index;
    int last_log_index;
    int last_log_term;
    raft_log_entry_t entries[RAFT_MAX_BATCH];
    size_t entry_count;
} raft_message_t;

/*
 * Application callbacks — implement these four functions to connect your
 * state machine to the consensus engine.
 *
 *  apply            Called once per committed log entry (in index order) on
 *                   the rxnet thread.  Mutate your state here.
 *
 *  reload           Called on restart when no snapshot exists on disk.
 *                   Reset your state to "empty"; the engine will re-apply
 *                   every committed log entry through apply().
 *
 *  snapshot         Called during log compaction.  Serialise your full state
 *                   into buf (max buf_size bytes).  Return bytes written.
 *                   Override RAFT_MAX_SNAPSHOT_SIZE before including this
 *                   header if your state exceeds the default 4096 bytes.
 *
 *  restore_snapshot Called on restart when a snapshot exists on disk.
 *                   Deserialise buf into your state machine; the engine will
 *                   then re-apply any log entries that follow the snapshot.
 */
typedef struct {
    void   (*apply)            (void *user, const raft_command_t *command);
    void   (*reload)           (void *user);
    size_t (*snapshot)         (void *user, void *buf, size_t buf_size);
    void   (*restore_snapshot) (void *user, const void *buf, size_t size);
    void  *user;
} raft_application_t;

typedef struct {
    char node_id[RAFT_MAX_ID];
    char root_dir[256];
    uint8_t snapshot_buf[RAFT_MAX_SNAPSHOT_SIZE];
    size_t snapshot_buf_size;
} raft_file_storage_t;

typedef struct {
    raft_message_t queues[RAFT_MAX_NODES][RAFT_MAX_QUEUE];
    size_t queue_count[RAFT_MAX_NODES];
    raft_command_t client_queues[RAFT_MAX_NODES][RAFT_MAX_QUEUE];
    size_t client_count[RAFT_MAX_NODES];
    char node_ids[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t node_count;
} raft_memory_transport_t;

/*
 * Pluggable transport vtable — implement to support TCP, BLE, etc.
 *
 *  send            Send a Raft protocol message to msg->target.
 *  recv            Drain incoming Raft messages into out[0..capacity-1];
 *                  return the number of messages written.
 *  submit_command  Enqueue a client command for the leader to process.
 *  recv_commands   Drain pending client commands (called on the leader each
 *                  tick); return the number of commands written.
 *  set_enabled     Gate the transport on simulated node stop/start (optional,
 *                  may be NULL).
 *
 * send/recv carry raft_message_t (Raft protocol).
 * submit_command/recv_commands carry raft_command_t (application level).
 */
typedef struct {
    int    (*send)           (void *ctx, const raft_message_t *msg);
    size_t (*recv)           (void *ctx, raft_message_t *out, size_t capacity);
    int    (*submit_command) (void *ctx, const raft_command_t *cmd);
    size_t (*recv_commands)  (void *ctx, raft_command_t *out, size_t capacity);
    void   (*set_enabled)    (void *ctx, int enabled);
    void  *ctx;
} raft_transport_t;

typedef struct raft_node    raft_node_t;
typedef struct raft_cluster raft_cluster_t;

/*
 * Per-node configuration.  Zero-initialise with memset before filling.
 *
 *  node_id               Unique identifier for this node (max RAFT_MAX_ID-1 chars).
 *  peers / peer_count    IDs of the other nodes this node talks to.  The
 *                        transport uses these to route outgoing messages.
 *  initial_members /     IDs of ALL nodes in the initial cluster.  Must be
 *  initial_member_count  identical on every node that bootstraps together.
 *                        Leave at zero for nodes joining via --join / learner=1.
 *  learner               Set to 1 for a joining node.  Prevents the node from
 *                        auto-bootstrapping a solo cluster on first start.
 *  election_timeout_ms   Follower fires an election if no heartbeat is received
 *                        within [timeout, 2×timeout) ms.  Guideline: ≥ 5×RTT.
 *  heartbeat_interval_ms Leader sends a heartbeat every this many ms.
 *                        Guideline: ≤ election_timeout_ms / 5.
 */
typedef struct {
    char   node_id[RAFT_MAX_ID];
    char   peers[RAFT_MAX_PEERS][RAFT_MAX_ID];
    size_t peer_count;
    char   initial_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t initial_member_count;
    int    learner;
    int    election_timeout_ms;
    int    heartbeat_interval_ms;
} raft_node_config_t;

typedef struct {
    char old_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t old_count;
    char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t new_count;
    int index;
} raft_cluster_configuration_t;

struct raft_node {
    raft_cluster_t    *cluster;
    raft_node_config_t config;
    long *clock_ms;
    raft_transport_t transport;
    raft_file_storage_t storage;
    raft_application_t application;

    rx_fsm_machine machine;

    int current_term;
    char voted_for[RAFT_MAX_ID];
    raft_log_entry_t log[RAFT_MAX_LOG];
    size_t log_count;
    int snapshot_last_included_index;
    int snapshot_last_included_term;
    int commit_index;
    int last_applied;
    char leader_id[RAFT_MAX_ID];
    int compaction_threshold;
    int running;

    raft_cluster_configuration_t config_state;
    raft_membership_state_t membership_state;
    char requested_membership[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t requested_membership_count;
    int requested_membership_pending;
    int joint_config_index;
    int final_config_index;

    char votes_received[RAFT_MAX_PEERS + 1][RAFT_MAX_ID];
    size_t vote_count;
    int next_index[RAFT_MAX_PEERS];
    int match_index[RAFT_MAX_PEERS];

    raft_message_t outbox[RAFT_MAX_QUEUE];
    size_t outbox_count;
    raft_message_t pending_append_entries[RAFT_MAX_QUEUE];
    size_t pending_append_count;
    raft_message_t pending_vote_requests[RAFT_MAX_QUEUE];
    size_t pending_vote_request_count;
    raft_message_t pending_votes[RAFT_MAX_QUEUE];
    size_t pending_vote_count;

    int persist_dirty;
    long election_deadline_ms;
    long heartbeat_deadline_ms;
};

struct raft_cluster {
    rx_fsm_runtime *runtime;
    int realtime_clock;
    long clock_ms;
    raft_memory_transport_t transport;
    raft_node_t nodes[RAFT_MAX_NODES];
    size_t node_count;
#ifdef RX_TRACE_ENABLE
    rx_trace_buf_t *trace;
    int trace_labels_registered;
#endif
};

/* Transport — memory (in-process) adapter */
raft_transport_t raft_mem_transport_make(raft_node_t *node);

void raft_memory_transport_init(raft_memory_transport_t *transport);
int raft_memory_transport_register_node(raft_memory_transport_t *transport, const char *node_id);
int raft_memory_transport_send(raft_memory_transport_t *transport, const raft_message_t *message);
size_t raft_memory_transport_recv_for(raft_memory_transport_t *transport, const char *node_id,
                                      raft_message_t *out, size_t capacity);
int raft_memory_transport_submit_client_command(raft_memory_transport_t *transport, const char *node_id,
                                                const raft_command_t *command);
size_t raft_memory_transport_recv_client_commands(raft_memory_transport_t *transport, const char *node_id,
                                                  raft_command_t *out, size_t capacity);

/* Storage */
void raft_file_storage_init(raft_file_storage_t *storage, const char *root_dir, const char *node_id);

/* Cluster */
int raft_cluster_init(raft_cluster_t *cluster, rx_fsm_runtime *runtime);
void raft_cluster_destroy(raft_cluster_t *cluster);
void raft_cluster_enable_realtime_clock(raft_cluster_t *cluster);
raft_node_t *raft_cluster_add_node(raft_cluster_t *cluster, const raft_node_config_t *config,
                                   const char *root_dir, const raft_application_t *application,
                                   long period_us);
int raft_cluster_tick(raft_cluster_t *cluster, int advance_ms);
raft_node_t *raft_cluster_leader(raft_cluster_t *cluster);

#ifdef RX_TRACE_ENABLE
int raft_cluster_attach_trace(raft_cluster_t *cluster, rx_trace_buf_t *trace);
#endif

/* Node */
int  raft_node_submit_command(raft_node_t *node, const raft_command_t *command);
int  raft_node_request_membership_change(raft_node_t *node, const char members[][RAFT_MAX_ID],
                                         size_t member_count);
void raft_node_stop(raft_node_t *node);
void raft_node_start(raft_node_t *node);
void raft_node_reset_for_join(raft_node_t *node);

#ifdef __cplusplus
}
#endif
