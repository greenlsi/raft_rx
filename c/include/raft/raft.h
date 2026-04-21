#pragma once

#include <stddef.h>

#include "rxnet/fsm.h"

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
#define RAFT_MAX_KV 128

typedef enum {
    RAFT_ROLE_FOLLOWER = 0,
    RAFT_ROLE_CANDIDATE = 1,
    RAFT_ROLE_LEADER = 2,
} raft_role_t;

typedef enum {
    RAFT_MSG_REQUEST_VOTE = 0,
    RAFT_MSG_REQUEST_VOTE_RESPONSE = 1,
    RAFT_MSG_APPEND_ENTRIES = 2,
    RAFT_MSG_APPEND_ENTRIES_RESPONSE = 3,
} raft_message_kind_t;

typedef struct {
    char op[16];
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
} raft_command_t;

typedef struct {
    int index;
    int term;
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
    int leader_commit;
    int match_index;
    int last_log_index;
    int last_log_term;
    raft_log_entry_t entries[RAFT_MAX_BATCH];
    size_t entry_count;
} raft_message_t;

typedef struct {
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
    int used;
} raft_kv_pair_t;

typedef struct {
    char node_id[RAFT_MAX_ID];
    char root_dir[256];
} raft_file_storage_t;

typedef struct {
    raft_message_t queues[RAFT_MAX_NODES][RAFT_MAX_QUEUE];
    size_t queue_count[RAFT_MAX_NODES];
    raft_command_t client_queues[RAFT_MAX_NODES][RAFT_MAX_QUEUE];
    size_t client_count[RAFT_MAX_NODES];
    char node_ids[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t node_count;
} raft_memory_transport_t;

typedef struct raft_node raft_node_t;

typedef struct {
    char node_id[RAFT_MAX_ID];
    char peers[RAFT_MAX_PEERS][RAFT_MAX_ID];
    size_t peer_count;
    int election_timeout_ms;
    int heartbeat_interval_ms;
} raft_node_config_t;

struct raft_node {
    raft_node_config_t config;
    long *clock_ms;
    raft_memory_transport_t *transport;
    raft_file_storage_t storage;

    rx_fsm_machine machine;

    int current_term;
    char voted_for[RAFT_MAX_ID];
    raft_log_entry_t log[RAFT_MAX_LOG];
    size_t log_count;
    int commit_index;
    int last_applied;
    char leader_id[RAFT_MAX_ID];

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

    raft_kv_pair_t kv[RAFT_MAX_KV];
    size_t kv_count;

    int persist_dirty;
    long election_deadline_ms;
    long heartbeat_deadline_ms;
};

typedef struct {
    rx_fsm_runtime runtime;
    long clock_ms;
    raft_memory_transport_t transport;
    raft_node_t nodes[RAFT_MAX_NODES];
    size_t node_count;
} raft_cluster_t;

void raft_memory_transport_init(raft_memory_transport_t *transport);
int raft_memory_transport_register_node(raft_memory_transport_t *transport, const char *node_id);
int raft_memory_transport_send(raft_memory_transport_t *transport, const raft_message_t *message);
size_t raft_memory_transport_recv_for(raft_memory_transport_t *transport, const char *node_id,
                                      raft_message_t *out, size_t capacity);
int raft_memory_transport_submit_client_command(raft_memory_transport_t *transport, const char *node_id,
                                                const raft_command_t *command);
size_t raft_memory_transport_recv_client_commands(raft_memory_transport_t *transport, const char *node_id,
                                                  raft_command_t *out, size_t capacity);

void raft_file_storage_init(raft_file_storage_t *storage, const char *root_dir, const char *node_id);

int raft_cluster_init(raft_cluster_t *cluster);
void raft_cluster_destroy(raft_cluster_t *cluster);
raft_node_t *raft_cluster_add_node(raft_cluster_t *cluster, const raft_node_config_t *config,
                                   const char *root_dir);
int raft_cluster_tick(raft_cluster_t *cluster, int advance_ms);
raft_node_t *raft_cluster_leader(raft_cluster_t *cluster);

int raft_node_submit_command(raft_node_t *node, const raft_command_t *command);
const char *raft_node_get(raft_node_t *node, const char *key);

#ifdef __cplusplus
}
#endif
