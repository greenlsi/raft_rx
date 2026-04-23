#include "raft/raft.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int guard_timeout_expired(const rx_fsm_context *ctx, void *user);
static int guard_has_append_entries(const rx_fsm_context *ctx, void *user);
static int guard_has_vote_request(const rx_fsm_context *ctx, void *user);
static int guard_has_vote(const rx_fsm_context *ctx, void *user);
static int guard_received_majority_votes(const rx_fsm_context *ctx, void *user);
static int guard_time_for_heartbeat(const rx_fsm_context *ctx, void *user);

static void action_become_candidate(rx_fsm_context *ctx, void *user);
static void action_handle_append_entries(rx_fsm_context *ctx, void *user);
static void action_handle_vote_request(rx_fsm_context *ctx, void *user);
static void action_ignore_vote(rx_fsm_context *ctx, void *user);
static void action_back_to_follower_due_to_timeout(rx_fsm_context *ctx, void *user);
static void action_become_leader(rx_fsm_context *ctx, void *user);
static void action_handle_vote(rx_fsm_context *ctx, void *user);
static void action_ignore_vote_request(rx_fsm_context *ctx, void *user);
static void action_send_heartbeat(rx_fsm_context *ctx, void *user);

static void raft_node_latch_inputs(rx_fsm_context *ctx, void *user);
static void raft_node_dump_outputs(rx_fsm_context *ctx, void *user);

static int configuration_is_joint(const raft_cluster_configuration_t *config);
static void configuration_set_stable(raft_cluster_configuration_t *config,
                                     const char members[][RAFT_MAX_ID], size_t member_count, int index);
static void configuration_set_joint(raft_cluster_configuration_t *config,
                                    const char old_members[][RAFT_MAX_ID], size_t old_count,
                                    const char new_members[][RAFT_MAX_ID], size_t new_count, int index);
static size_t normalize_members(const char members[][RAFT_MAX_ID], size_t member_count,
                                char out[][RAFT_MAX_ID], size_t capacity);
static int member_list_contains(const char members[][RAFT_MAX_ID], size_t member_count, const char *node_id);
static void node_refresh_peers(raft_node_t *node);
static int node_self_is_member(const raft_node_t *node);
static int node_has_vote_quorum(const raft_node_t *node, const char members[][RAFT_MAX_ID], size_t member_count);
static int node_replicated_on_members(const raft_node_t *node, int candidate_index,
                                      const char members[][RAFT_MAX_ID], size_t member_count);
static int node_is_committed_under_current_configuration(const raft_node_t *node, int candidate_index);
static int node_joint_members_are_caught_up(const raft_node_t *node);
static int node_append_internal_command(raft_node_t *node, const char *op, const char *key, const char *value);
static void node_maybe_drive_membership(raft_node_t *node);
static void node_apply_internal_command(raft_node_t *node, const raft_command_t *command);
static void encode_member_list(const char members[][RAFT_MAX_ID], size_t member_count,
                               char *out, size_t out_size);
static size_t decode_member_list(const char *value, char out[][RAFT_MAX_ID], size_t capacity);
#ifdef RX_TRACE_ENABLE
static void cluster_register_trace_labels(raft_cluster_t *cluster);
static void node_register_trace_metadata(raft_cluster_t *cluster, raft_node_t *node);
static void node_trace_user(raft_node_t *node, uint8_t label, uint16_t value);
#endif

#ifdef RX_TRACE_ENABLE
enum {
    RAFT_TRACE_REPL_SEND = 0,
    RAFT_TRACE_REPL_MATCH_ADVANCED = 1,
    RAFT_TRACE_COMMIT = 2,
    RAFT_TRACE_APPLY = 3,
    RAFT_TRACE_MEMBERSHIP_REQUESTED = 4,
    RAFT_TRACE_MEMBERSHIP_JOINT_COMMITTED = 5,
    RAFT_TRACE_MEMBERSHIP_CATCHUP_READY = 6,
    RAFT_TRACE_MEMBERSHIP_STABLE_COMMITTED = 7,
};
#endif

static const rx_fsm_transition RAFT_TRANSITIONS[] = {
    {RAFT_ROLE_FOLLOWER, RAFT_ROLE_CANDIDATE, guard_timeout_expired, action_become_candidate},
    {RAFT_ROLE_FOLLOWER, RAFT_ROLE_FOLLOWER, guard_has_append_entries, action_handle_append_entries},
    {RAFT_ROLE_FOLLOWER, RAFT_ROLE_FOLLOWER, guard_has_vote_request, action_handle_vote_request},
    {RAFT_ROLE_FOLLOWER, RAFT_ROLE_FOLLOWER, guard_has_vote, action_ignore_vote},
    {RAFT_ROLE_CANDIDATE, RAFT_ROLE_FOLLOWER, guard_timeout_expired, action_back_to_follower_due_to_timeout},
    {RAFT_ROLE_CANDIDATE, RAFT_ROLE_LEADER, guard_received_majority_votes, action_become_leader},
    {RAFT_ROLE_CANDIDATE, RAFT_ROLE_FOLLOWER, guard_has_append_entries, action_handle_append_entries},
    {RAFT_ROLE_CANDIDATE, RAFT_ROLE_CANDIDATE, guard_has_vote_request, action_handle_vote_request},
    {RAFT_ROLE_CANDIDATE, RAFT_ROLE_CANDIDATE, guard_has_vote, action_handle_vote},
    {RAFT_ROLE_LEADER, RAFT_ROLE_FOLLOWER, guard_has_append_entries, action_handle_append_entries},
    {RAFT_ROLE_LEADER, RAFT_ROLE_LEADER, guard_has_vote_request, action_ignore_vote_request},
    {RAFT_ROLE_LEADER, RAFT_ROLE_LEADER, guard_has_vote, action_ignore_vote},
    {RAFT_ROLE_LEADER, RAFT_ROLE_LEADER, guard_time_for_heartbeat, action_send_heartbeat},
};

static int transport_find_node(raft_memory_transport_t *transport, const char *node_id) {
    size_t i;
    for (i = 0; i < transport->node_count; ++i) {
        if (strcmp(transport->node_ids[i], node_id) == 0) {
            return (int)i;
        }
    }
    return -1;
}

void raft_memory_transport_init(raft_memory_transport_t *transport) {
    memset(transport, 0, sizeof(*transport));
}

int raft_memory_transport_register_node(raft_memory_transport_t *transport, const char *node_id) {
    if (transport->node_count >= RAFT_MAX_NODES) {
        return -1;
    }
    strncpy(transport->node_ids[transport->node_count], node_id, RAFT_MAX_ID - 1);
    transport->node_count++;
    return 0;
}

int raft_memory_transport_send(raft_memory_transport_t *transport, const raft_message_t *message) {
    int idx = transport_find_node(transport, message->target);
    if (idx < 0 || transport->queue_count[idx] >= RAFT_MAX_QUEUE) {
        return -1;
    }
    transport->queues[idx][transport->queue_count[idx]++] = *message;
    return 0;
}

size_t raft_memory_transport_recv_for(raft_memory_transport_t *transport, const char *node_id,
                                      raft_message_t *out, size_t capacity) {
    int idx = transport_find_node(transport, node_id);
    size_t count, i;
    if (idx < 0) {
        return 0;
    }
    count = transport->queue_count[idx];
    if (count > capacity) {
        count = capacity;
    }
    for (i = 0; i < count; ++i) {
        out[i] = transport->queues[idx][i];
    }
    transport->queue_count[idx] = 0;
    return count;
}

int raft_memory_transport_submit_client_command(raft_memory_transport_t *transport, const char *node_id,
                                                const raft_command_t *command) {
    int idx = transport_find_node(transport, node_id);
    if (idx < 0 || transport->client_count[idx] >= RAFT_MAX_QUEUE) {
        return -1;
    }
    transport->client_queues[idx][transport->client_count[idx]++] = *command;
    return 0;
}

size_t raft_memory_transport_recv_client_commands(raft_memory_transport_t *transport, const char *node_id,
                                                  raft_command_t *out, size_t capacity) {
    int idx = transport_find_node(transport, node_id);
    size_t count, i;
    if (idx < 0) {
        return 0;
    }
    count = transport->client_count[idx];
    if (count > capacity) {
        count = capacity;
    }
    for (i = 0; i < count; ++i) {
        out[i] = transport->client_queues[idx][i];
    }
    transport->client_count[idx] = 0;
    return count;
}

#ifdef RX_TRACE_ENABLE
static void cluster_register_trace_labels(raft_cluster_t *cluster) {
    rx_trace_buf_t *trace;

    if (!cluster || !cluster->trace || cluster->trace_labels_registered) {
        return;
    }
    trace = cluster->trace;
    rx_trace_set_label_name(trace, RAFT_TRACE_REPL_SEND, "raft.repl.send");
    rx_trace_set_label_name(trace, RAFT_TRACE_REPL_MATCH_ADVANCED, "raft.repl.match_advanced");
    rx_trace_set_label_name(trace, RAFT_TRACE_COMMIT, "raft.commit");
    rx_trace_set_label_name(trace, RAFT_TRACE_APPLY, "raft.apply");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_REQUESTED, "raft.membership.requested");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_JOINT_COMMITTED, "raft.membership.joint_committed");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_CATCHUP_READY, "raft.membership.catchup_ready");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_STABLE_COMMITTED, "raft.membership.stable_committed");
    cluster->trace_labels_registered = 1;
}

static void node_register_trace_metadata(raft_cluster_t *cluster, raft_node_t *node) {
    if (!cluster || !cluster->trace || !node || node->machine.node.trace != cluster->trace) {
        return;
    }
    rx_trace_set_node_name(cluster->trace, node->machine.node.trace_nid, node->config.node_id);
    rx_trace_set_state_name(cluster->trace, node->machine.node.trace_nid, RAFT_ROLE_FOLLOWER, "FOLLOWER");
    rx_trace_set_state_name(cluster->trace, node->machine.node.trace_nid, RAFT_ROLE_CANDIDATE, "CANDIDATE");
    rx_trace_set_state_name(cluster->trace, node->machine.node.trace_nid, RAFT_ROLE_LEADER, "LEADER");
}

static void node_trace_user(raft_node_t *node, uint8_t label, uint16_t value) {
    if (!node || !node->machine.node.trace) {
        return;
    }
    rx_trace_user(node->machine.node.trace, label, value);
}
#endif

static int configuration_is_joint(const raft_cluster_configuration_t *config) {
    return config->new_count > 0;
}

static size_t normalize_members(const char members[][RAFT_MAX_ID], size_t member_count,
                                char out[][RAFT_MAX_ID], size_t capacity) {
    size_t i;
    size_t out_count = 0;
    for (i = 0; i < member_count && out_count < capacity; ++i) {
        size_t j;
        if (members[i][0] == '\0') {
            continue;
        }
        for (j = 0; j < out_count; ++j) {
            if (strcmp(out[j], members[i]) == 0) {
                break;
            }
        }
        if (j == out_count) {
            strncpy(out[out_count], members[i], RAFT_MAX_ID - 1);
            out[out_count][RAFT_MAX_ID - 1] = '\0';
            out_count++;
        }
    }
    return out_count;
}

static void configuration_set_stable(raft_cluster_configuration_t *config,
                                     const char members[][RAFT_MAX_ID], size_t member_count, int index) {
    char normalized[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t normalized_count = normalize_members(members, member_count, normalized, RAFT_MAX_NODES);
    memset(config, 0, sizeof(*config));
    memcpy(config->old_members, normalized, sizeof(normalized));
    config->old_count = normalized_count;
    config->index = index;
}

static void configuration_set_joint(raft_cluster_configuration_t *config,
                                    const char old_members[][RAFT_MAX_ID], size_t old_count,
                                    const char new_members[][RAFT_MAX_ID], size_t new_count, int index) {
    char normalized_old[RAFT_MAX_NODES][RAFT_MAX_ID];
    char normalized_new[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t normalized_old_count = normalize_members(old_members, old_count, normalized_old, RAFT_MAX_NODES);
    size_t normalized_new_count = normalize_members(new_members, new_count, normalized_new, RAFT_MAX_NODES);
    memset(config, 0, sizeof(*config));
    memcpy(config->old_members, normalized_old, sizeof(normalized_old));
    memcpy(config->new_members, normalized_new, sizeof(normalized_new));
    config->old_count = normalized_old_count;
    config->new_count = normalized_new_count;
    config->index = index;
}

static int member_list_contains(const char members[][RAFT_MAX_ID], size_t member_count, const char *node_id) {
    size_t i;
    for (i = 0; i < member_count; ++i) {
        if (strcmp(members[i], node_id) == 0) {
            return 1;
        }
    }
    return 0;
}

static void node_refresh_peers(raft_node_t *node) {
    size_t i;
    size_t peer_count = 0;
    memset(node->config.peers, 0, sizeof(node->config.peers));
    for (i = 0; i < node->config_state.old_count && peer_count < RAFT_MAX_PEERS; ++i) {
        if (strcmp(node->config_state.old_members[i], node->config.node_id) != 0) {
            strncpy(node->config.peers[peer_count], node->config_state.old_members[i], RAFT_MAX_ID - 1);
            peer_count++;
        }
    }
    for (i = 0; i < node->config_state.new_count && peer_count < RAFT_MAX_PEERS; ++i) {
        if (strcmp(node->config_state.new_members[i], node->config.node_id) != 0 &&
            !member_list_contains((const char (*)[RAFT_MAX_ID])node->config.peers, peer_count,
                                  node->config_state.new_members[i])) {
            strncpy(node->config.peers[peer_count], node->config_state.new_members[i], RAFT_MAX_ID - 1);
            peer_count++;
        }
    }
    node->config.peer_count = peer_count;
}

static int node_self_is_member(const raft_node_t *node) {
    if (member_list_contains(node->config_state.old_members, node->config_state.old_count, node->config.node_id)) {
        return 1;
    }
    return member_list_contains(node->config_state.new_members, node->config_state.new_count, node->config.node_id);
}

static void mkdir_if_needed(const char *path) {
    char tmp[600];
    size_t i, len;
    memset(tmp, 0, sizeof(tmp));
    strncpy(tmp, path, sizeof(tmp) - 1);
    len = strlen(tmp);
    for (i = 1; i < len; ++i) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
                perror("mkdir");
                exit(1);
            }
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
        perror("mkdir");
        exit(1);
    }
}

void raft_file_storage_init(raft_file_storage_t *storage, const char *root_dir, const char *node_id) {
    memset(storage, 0, sizeof(*storage));
    strncpy(storage->root_dir, root_dir, sizeof(storage->root_dir) - 1);
    strncpy(storage->node_id, node_id, sizeof(storage->node_id) - 1);
}

static void storage_node_dir(const raft_file_storage_t *storage, char *out, size_t out_size) {
    snprintf(out, out_size, "%s/%s", storage->root_dir, storage->node_id);
}

static void storage_meta_path(const raft_file_storage_t *storage, char *out, size_t out_size) {
    char node_dir[512];
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    snprintf(out, out_size, "%s/meta.txt", node_dir);
}

static void storage_log_path(const raft_file_storage_t *storage, char *out, size_t out_size) {
    char node_dir[512];
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    snprintf(out, out_size, "%s/log.txt", node_dir);
}

static void storage_kv_path(const raft_file_storage_t *storage, char *out, size_t out_size) {
    char node_dir[512];
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    snprintf(out, out_size, "%s/kv.txt", node_dir);
}

static void storage_ensure_dirs(const raft_file_storage_t *storage) {
    char node_dir[512];
    mkdir_if_needed(storage->root_dir);
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    mkdir_if_needed(node_dir);
}

static void atomic_replace_path(const char *path, void (*writer)(FILE *, void *), void *arg) {
    char tmp[600];
    FILE *fp;
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    fp = fopen(tmp, "w");
    if (fp == NULL) {
        perror("fopen");
        exit(1);
    }
    writer(fp, arg);
    fclose(fp);
    if (rename(tmp, path) != 0) {
        perror("rename");
        exit(1);
    }
}

typedef struct {
    int current_term;
    const char *voted_for;
    int compaction_threshold;
    const raft_cluster_configuration_t *config_state;
} meta_writer_arg_t;

static void write_meta(FILE *fp, void *arg) {
    meta_writer_arg_t *meta = (meta_writer_arg_t *)arg;
    fprintf(fp, "current_term %d\n", meta->current_term);
    fprintf(fp, "voted_for %s\n", meta->voted_for[0] ? meta->voted_for : "-");
    fprintf(fp, "compaction_threshold %d\n", meta->compaction_threshold);
    fprintf(fp, "configuration_index %d\n", meta->config_state->index);
    fprintf(fp, "old_count %zu\n", meta->config_state->old_count);
    for (size_t i = 0; i < meta->config_state->old_count; ++i) {
        fprintf(fp, "old_member %s\n", meta->config_state->old_members[i]);
    }
    fprintf(fp, "new_count %zu\n", meta->config_state->new_count);
    for (size_t i = 0; i < meta->config_state->new_count; ++i) {
        fprintf(fp, "new_member %s\n", meta->config_state->new_members[i]);
    }
}

typedef struct {
    raft_node_t *node;
} log_writer_arg_t;

static void write_log(FILE *fp, void *arg) {
    size_t i;
    raft_node_t *node = ((log_writer_arg_t *)arg)->node;
    for (i = 0; i < node->log_count; ++i) {
        raft_log_entry_t *entry = &node->log[i];
        fprintf(fp, "%d|%d|%s|%s|%s\n",
                entry->index, entry->term, entry->command.op, entry->command.key, entry->command.value);
    }
}

static void write_kv(FILE *fp, void *arg) {
    size_t i;
    raft_node_t *node = ((log_writer_arg_t *)arg)->node;
    for (i = 0; i < node->kv_count; ++i) {
        if (node->kv[i].used) {
            fprintf(fp, "%s=%s\n", node->kv[i].key, node->kv[i].value);
        }
    }
}

static void node_save_persistent(raft_node_t *node) {
    char meta_path[600];
    char log_path[600];
    char kv_path[600];
    meta_writer_arg_t meta;
    log_writer_arg_t log_arg;
    storage_ensure_dirs(&node->storage);
    storage_meta_path(&node->storage, meta_path, sizeof(meta_path));
    storage_log_path(&node->storage, log_path, sizeof(log_path));
    storage_kv_path(&node->storage, kv_path, sizeof(kv_path));
    meta.current_term = node->current_term;
    meta.voted_for = node->voted_for;
    meta.compaction_threshold = node->compaction_threshold;
    meta.config_state = &node->config_state;
    log_arg.node = node;
    atomic_replace_path(meta_path, write_meta, &meta);
    atomic_replace_path(log_path, write_log, &log_arg);
    atomic_replace_path(kv_path, write_kv, &log_arg);
}

static void node_load_persistent(raft_node_t *node) {
    char meta_path[600];
    char log_path[600];
    char kv_path[600];
    FILE *fp;
    char line[512];
    storage_ensure_dirs(&node->storage);
    storage_meta_path(&node->storage, meta_path, sizeof(meta_path));
    storage_log_path(&node->storage, log_path, sizeof(log_path));
    storage_kv_path(&node->storage, kv_path, sizeof(kv_path));

    fp = fopen(meta_path, "r");
    if (fp != NULL) {
        while (fgets(line, sizeof(line), fp) != NULL) {
            if (strncmp(line, "current_term ", 13) == 0) {
                node->current_term = atoi(line + 13);
            } else if (strncmp(line, "voted_for ", 10) == 0) {
                sscanf(line + 10, "%15s", node->voted_for);
                if (strcmp(node->voted_for, "-") == 0) {
                    node->voted_for[0] = '\0';
                }
            } else if (strncmp(line, "compaction_threshold ", 21) == 0) {
                node->compaction_threshold = atoi(line + 21);
            } else if (strncmp(line, "configuration_index ", 20) == 0) {
                node->config_state.index = atoi(line + 20);
            } else if (strncmp(line, "old_count ", 10) == 0) {
                node->config_state.old_count = (size_t)atoi(line + 10);
                if (node->config_state.old_count > RAFT_MAX_NODES) {
                    node->config_state.old_count = RAFT_MAX_NODES;
                }
            } else if (strncmp(line, "old_member ", 11) == 0) {
                size_t idx = 0;
                while (idx < node->config_state.old_count && node->config_state.old_members[idx][0] != '\0') {
                    idx++;
                }
                if (idx < RAFT_MAX_NODES) {
                    sscanf(line + 11, "%15s", node->config_state.old_members[idx]);
                }
            } else if (strncmp(line, "new_count ", 10) == 0) {
                node->config_state.new_count = (size_t)atoi(line + 10);
                if (node->config_state.new_count > RAFT_MAX_NODES) {
                    node->config_state.new_count = RAFT_MAX_NODES;
                }
            } else if (strncmp(line, "new_member ", 11) == 0) {
                size_t idx = 0;
                while (idx < node->config_state.new_count && node->config_state.new_members[idx][0] != '\0') {
                    idx++;
                }
                if (idx < RAFT_MAX_NODES) {
                    sscanf(line + 11, "%15s", node->config_state.new_members[idx]);
                }
            }
        }
        fclose(fp);
    }

    fp = fopen(log_path, "r");
    if (fp != NULL) {
        while (fgets(line, sizeof(line), fp) != NULL && node->log_count < RAFT_MAX_LOG) {
            raft_log_entry_t *entry = &node->log[node->log_count];
            memset(entry, 0, sizeof(*entry));
            if (sscanf(line, "%d|%d|%15[^|]|%63[^|]|%127[^\n]",
                       &entry->index, &entry->term, entry->command.op, entry->command.key,
                       entry->command.value) == 5) {
                node->log_count++;
            }
        }
        fclose(fp);
    }

    fp = fopen(kv_path, "r");
    if (fp != NULL) {
        while (fgets(line, sizeof(line), fp) != NULL && node->kv_count < RAFT_MAX_KV) {
            char *eq = strchr(line, '=');
            if (eq == NULL) {
                continue;
            }
            *eq = '\0';
            eq++;
            line[strcspn(line, "\n")] = '\0';
            eq[strcspn(eq, "\n")] = '\0';
            strncpy(node->kv[node->kv_count].key, line, RAFT_MAX_KEY - 1);
            strncpy(node->kv[node->kv_count].value, eq, RAFT_MAX_VALUE - 1);
            node->kv[node->kv_count].used = 1;
            node->kv_count++;
        }
        fclose(fp);
    }
}

static int node_is_leader(const raft_node_t *node) {
    return node->running && node_self_is_member(node) && node->machine.state == RAFT_ROLE_LEADER;
}

static int node_last_log_index(const raft_node_t *node) {
    return (int)node->log_count;
}

static int node_last_log_term(const raft_node_t *node) {
    return node->log_count == 0 ? 0 : node->log[node->log_count - 1].term;
}

static int member_majority(size_t member_count) {
    return (int)(member_count / 2) + 1;
}

static int node_peer_index(const raft_node_t *node, const char *peer_id) {
    size_t i;
    for (i = 0; i < node->config.peer_count; ++i) {
        if (strcmp(node->config.peers[i], peer_id) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static void node_record_vote(raft_node_t *node, const char *peer_id) {
    size_t i;
    for (i = 0; i < node->vote_count; ++i) {
        if (strcmp(node->votes_received[i], peer_id) == 0) {
            return;
        }
    }
    if (node->vote_count < RAFT_MAX_PEERS + 1) {
        strncpy(node->votes_received[node->vote_count], peer_id, RAFT_MAX_ID - 1);
        node->vote_count++;
    }
}

static int node_has_vote_quorum(const raft_node_t *node, const char members[][RAFT_MAX_ID], size_t member_count) {
    size_t i;
    int votes = 0;
    for (i = 0; i < member_count; ++i) {
        if (member_list_contains((const char (*)[RAFT_MAX_ID])node->votes_received, node->vote_count, members[i])) {
            votes++;
        }
    }
    return votes >= member_majority(member_count);
}

static void node_queue_message(raft_node_t *node, const raft_message_t *message) {
    if (node->outbox_count < RAFT_MAX_QUEUE) {
        node->outbox[node->outbox_count++] = *message;
    }
}

static void node_send_append_entries_to(raft_node_t *node, const char *peer_id) {
    raft_message_t message;
    int peer_idx = node_peer_index(node, peer_id);
    int next_index = peer_idx >= 0 ? node->next_index[peer_idx] : node_last_log_index(node) + 1;
    int prev_index = next_index - 1;
    int prev_term = prev_index == 0 ? 0 : node->log[prev_index - 1].term;
    size_t i;
    memset(&message, 0, sizeof(message));
    message.kind = RAFT_MSG_APPEND_ENTRIES;
    message.term = node->current_term;
    strncpy(message.source, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(message.target, peer_id, RAFT_MAX_ID - 1);
    message.prev_log_index = prev_index;
    message.prev_log_term = prev_term;
    message.leader_commit = node->commit_index;
    for (i = (size_t)(next_index - 1); i < node->log_count && message.entry_count < RAFT_MAX_BATCH; ++i) {
        message.entries[message.entry_count++] = node->log[i];
    }
    node_queue_message(node, &message);
#ifdef RX_TRACE_ENABLE
    node_trace_user(node, RAFT_TRACE_REPL_SEND, (uint16_t)message.entry_count);
#endif
}

static void node_broadcast_append_entries(raft_node_t *node) {
    size_t i;
    for (i = 0; i < node->config.peer_count; ++i) {
        node_send_append_entries_to(node, node->config.peers[i]);
    }
}

static int node_candidate_is_up_to_date(const raft_node_t *node, const raft_message_t *message) {
    if (message->last_log_term != node_last_log_term(node)) {
        return message->last_log_term > node_last_log_term(node);
    }
    return message->last_log_index >= node_last_log_index(node);
}

static void node_apply_command(raft_node_t *node, const raft_command_t *command) {
    size_t i;
    if (strcmp(command->op, "cluster.set_maxlog") == 0 ||
        strcmp(command->op, "cluster.enter_joint") == 0 ||
        strcmp(command->op, "cluster.leave_joint") == 0) {
        node_apply_internal_command(node, command);
        return;
    }
    for (i = 0; i < node->kv_count; ++i) {
        if (node->kv[i].used && strcmp(node->kv[i].key, command->key) == 0) {
            if (strcmp(command->op, "delete") == 0) {
                node->kv[i].used = 0;
                node->kv[i].key[0] = '\0';
                node->kv[i].value[0] = '\0';
            } else {
                strncpy(node->kv[i].value, command->value, RAFT_MAX_VALUE - 1);
            }
            return;
        }
    }
    if (strcmp(command->op, "set") == 0 && node->kv_count < RAFT_MAX_KV) {
        strncpy(node->kv[node->kv_count].key, command->key, RAFT_MAX_KEY - 1);
        strncpy(node->kv[node->kv_count].value, command->value, RAFT_MAX_VALUE - 1);
        node->kv[node->kv_count].used = 1;
        node->kv_count++;
    }
}

static int node_replicated_on_members(const raft_node_t *node, int candidate_index,
                                      const char members[][RAFT_MAX_ID], size_t member_count) {
    size_t i;
    int replicated = 0;
    for (i = 0; i < member_count; ++i) {
        if (strcmp(members[i], node->config.node_id) == 0) {
            if (node_last_log_index(node) >= candidate_index) {
                replicated++;
            }
            continue;
        }
        {
            int peer_idx = node_peer_index(node, members[i]);
            if (peer_idx >= 0 && node->match_index[peer_idx] >= candidate_index) {
                replicated++;
            }
        }
    }
    return replicated >= member_majority(member_count);
}

static int node_is_committed_under_current_configuration(const raft_node_t *node, int candidate_index) {
    if (!node_replicated_on_members(node, candidate_index,
                                    node->config_state.old_members, node->config_state.old_count)) {
        return 0;
    }
    if (!configuration_is_joint(&node->config_state)) {
        return 1;
    }
    return node_replicated_on_members(node, candidate_index,
                                      node->config_state.new_members, node->config_state.new_count);
}

static int node_joint_members_are_caught_up(const raft_node_t *node) {
    size_t i;
    int target_index = node_last_log_index(node);
    if (!configuration_is_joint(&node->config_state)) {
        return 0;
    }
    for (i = 0; i < node->config_state.new_count; ++i) {
        if (strcmp(node->config_state.new_members[i], node->config.node_id) == 0) {
            continue;
        }
        {
            int peer_idx = node_peer_index(node, node->config_state.new_members[i]);
            if (peer_idx < 0 || node->match_index[peer_idx] < target_index) {
                return 0;
            }
        }
    }
    return 1;
}

static void encode_member_list(const char members[][RAFT_MAX_ID], size_t member_count,
                               char *out, size_t out_size) {
    size_t i;
    size_t used = 0;
    if (out_size == 0) {
        return;
    }
    out[0] = '\0';
    for (i = 0; i < member_count; ++i) {
        int written = snprintf(out + used, out_size - used, "%s%s", i == 0 ? "" : ",", members[i]);
        if (written < 0 || (size_t)written >= out_size - used) {
            out[out_size - 1] = '\0';
            return;
        }
        used += (size_t)written;
    }
}

static size_t decode_member_list(const char *value, char out[][RAFT_MAX_ID], size_t capacity) {
    char buffer[RAFT_MAX_VALUE];
    char *token;
    size_t count = 0;
    if (value == NULL || value[0] == '\0') {
        return 0;
    }
    strncpy(buffer, value, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';
    token = strtok(buffer, ",");
    while (token != NULL && count < capacity) {
        if (token[0] != '\0' && !member_list_contains((const char (*)[RAFT_MAX_ID])out, count, token)) {
            strncpy(out[count], token, RAFT_MAX_ID - 1);
            out[count][RAFT_MAX_ID - 1] = '\0';
            count++;
        }
        token = strtok(NULL, ",");
    }
    return count;
}

static void node_apply_internal_command(raft_node_t *node, const raft_command_t *command) {
    if (strcmp(command->op, "cluster.set_maxlog") == 0) {
        int threshold = atoi(command->value);
        if (threshold > 0) {
            node->compaction_threshold = threshold;
        }
        return;
    }
    if (strcmp(command->op, "cluster.enter_joint") == 0) {
        char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
        size_t new_count = decode_member_list(command->value, new_members, RAFT_MAX_NODES);
        if (new_count > 0) {
            configuration_set_joint(&node->config_state,
                                    node->config_state.old_members, node->config_state.old_count,
                                    new_members, new_count, node->last_applied);
            node->membership_state = RAFT_MEMBERSHIP_JOINT;
            node->joint_config_index = node->last_applied;
            node_refresh_peers(node);
#ifdef RX_TRACE_ENABLE
            node_trace_user(node, RAFT_TRACE_MEMBERSHIP_JOINT_COMMITTED, (uint16_t)node->config_state.index);
#endif
        }
        return;
    }
    if (strcmp(command->op, "cluster.leave_joint") == 0) {
        char members[RAFT_MAX_NODES][RAFT_MAX_ID];
        size_t member_count = decode_member_list(command->value, members, RAFT_MAX_NODES);
        if (member_count > 0) {
            configuration_set_stable(&node->config_state, members, member_count, node->last_applied);
            node->membership_state = RAFT_MEMBERSHIP_STABLE;
            node->final_config_index = node->last_applied;
            node->joint_config_index = 0;
            node->requested_membership_pending = 0;
            node->requested_membership_count = 0;
            node_refresh_peers(node);
            if (!node_self_is_member(node)) {
                node->leader_id[0] = '\0';
                node->vote_count = 0;
                node->machine.state = RAFT_ROLE_FOLLOWER;
            }
#ifdef RX_TRACE_ENABLE
            node_trace_user(node, RAFT_TRACE_MEMBERSHIP_STABLE_COMMITTED, (uint16_t)node->config_state.index);
#endif
        }
    }
}

static void node_advance_commit_index(raft_node_t *node) {
    int idx;
    for (idx = node_last_log_index(node); idx > node->commit_index; --idx) {
        if (node->log[idx - 1].term != node->current_term) {
            continue;
        }
        if (node_is_committed_under_current_configuration(node, idx)) {
            node->commit_index = idx;
#ifdef RX_TRACE_ENABLE
            node_trace_user(node, RAFT_TRACE_COMMIT, (uint16_t)idx);
#endif
            return;
        }
    }
}

static void node_handle_term_update(raft_node_t *node, const raft_message_t *message) {
    if (message->term > node->current_term) {
        node->current_term = message->term;
        node->voted_for[0] = '\0';
        node->leader_id[0] = '\0';
        node->persist_dirty = 1;
    }
}

static void node_handle_vote_request(raft_node_t *node, const raft_message_t *message) {
    raft_message_t response;
    int granted = 0;
    long now = *node->clock_ms;
    node_handle_term_update(node, message);
    if (node_self_is_member(node) && message->term == node->current_term) {
        if ((node->voted_for[0] == '\0' || strcmp(node->voted_for, message->source) == 0) &&
            node_candidate_is_up_to_date(node, message)) {
            strncpy(node->voted_for, message->source, RAFT_MAX_ID - 1);
            node->election_deadline_ms = now + node->config.election_timeout_ms;
            node->persist_dirty = 1;
            granted = 1;
        }
    }
    memset(&response, 0, sizeof(response));
    response.kind = RAFT_MSG_REQUEST_VOTE_RESPONSE;
    response.term = node->current_term;
    strncpy(response.source, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(response.target, message->source, RAFT_MAX_ID - 1);
    response.granted = granted;
    node_queue_message(node, &response);
}

static int node_append_entries_from_leader(raft_node_t *node, const raft_message_t *message) {
    size_t i;
    if (message->prev_log_index > node_last_log_index(node)) {
        return 0;
    }
    if (message->prev_log_index > 0 && node->log[message->prev_log_index - 1].term != message->prev_log_term) {
        return 0;
    }
    for (i = 0; i < message->entry_count; ++i) {
        raft_log_entry_t *entry = (raft_log_entry_t *)&message->entries[i];
        if ((size_t)entry->index <= node->log_count) {
            if (node->log[entry->index - 1].term != entry->term) {
                node->log_count = (size_t)(entry->index - 1);
                node->persist_dirty = 1;
            } else {
                continue;
            }
        }
        if ((size_t)entry->index == node->log_count + 1 && node->log_count < RAFT_MAX_LOG) {
            node->log[node->log_count++] = *entry;
            node->persist_dirty = 1;
        }
    }
    if (message->leader_commit > node->commit_index) {
        int last = node_last_log_index(node);
        node->commit_index = message->leader_commit < last ? message->leader_commit : last;
    }
    return 1;
}

static void node_handle_append_entries(raft_node_t *node, const raft_message_t *message) {
    raft_message_t response;
    int success = 0;
    long now = *node->clock_ms;
    node_handle_term_update(node, message);
    if (message->term == node->current_term) {
        strncpy(node->leader_id, message->source, RAFT_MAX_ID - 1);
        node->election_deadline_ms = now + node->config.election_timeout_ms;
        success = node_append_entries_from_leader(node, message);
    }
    memset(&response, 0, sizeof(response));
    response.kind = RAFT_MSG_APPEND_ENTRIES_RESPONSE;
    response.term = node->current_term;
    strncpy(response.source, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(response.target, message->source, RAFT_MAX_ID - 1);
    response.success = success;
    response.match_index = node_last_log_index(node);
    node_queue_message(node, &response);
}

static void node_handle_vote_response(raft_node_t *node, const raft_message_t *message) {
    node_handle_term_update(node, message);
    if (!node_self_is_member(node) ||
        node->machine.state != RAFT_ROLE_CANDIDATE || message->term != node->current_term) {
        return;
    }
    if (message->granted) {
        node_record_vote(node, message->source);
    }
}

static void node_handle_append_entries_response(raft_node_t *node, const raft_message_t *message) {
    int peer_idx;
    node_handle_term_update(node, message);
    if (node->machine.state != RAFT_ROLE_LEADER || message->term != node->current_term) {
        return;
    }
    peer_idx = node_peer_index(node, message->source);
    if (peer_idx < 0) {
        return;
    }
    if (message->success) {
        node->match_index[peer_idx] = message->match_index;
        node->next_index[peer_idx] = message->match_index + 1;
#ifdef RX_TRACE_ENABLE
        node_trace_user(node, RAFT_TRACE_REPL_MATCH_ADVANCED, (uint16_t)message->match_index);
#endif
        node_advance_commit_index(node);
        return;
    }
    if (node->next_index[peer_idx] > 1) {
        node->next_index[peer_idx]--;
    }
    node_send_append_entries_to(node, message->source);
}

static void raft_node_init(raft_node_t *node, const raft_node_config_t *config, long *clock_ms,
                           raft_memory_transport_t *transport, const char *root_dir) {
    char initial_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t initial_count = 0;
    memset(node, 0, sizeof(*node));
    node->config = *config;
    node->clock_ms = clock_ms;
    node->transport = transport;
    node->running = 1;
    node->compaction_threshold = RAFT_MAX_LOG;
    raft_file_storage_init(&node->storage, root_dir, config->node_id);
    node_load_persistent(node);
    if (node->compaction_threshold <= 0) {
        node->compaction_threshold = RAFT_MAX_LOG;
    }
    if (node->config_state.old_count == 0) {
        if (config->initial_member_count > 0) {
            configuration_set_stable(&node->config_state, config->initial_members, config->initial_member_count, 0);
        } else {
            strncpy(initial_members[initial_count++], config->node_id, RAFT_MAX_ID - 1);
            for (size_t i = 0; i < config->peer_count && initial_count < RAFT_MAX_NODES; ++i) {
                strncpy(initial_members[initial_count++], config->peers[i], RAFT_MAX_ID - 1);
            }
            configuration_set_stable(&node->config_state, initial_members, initial_count, 0);
        }
    }
    node->membership_state = configuration_is_joint(&node->config_state) ? RAFT_MEMBERSHIP_JOINT : RAFT_MEMBERSHIP_STABLE;
    node_refresh_peers(node);
    node->election_deadline_ms = *clock_ms + config->election_timeout_ms;
    node->heartbeat_deadline_ms = *clock_ms + config->heartbeat_interval_ms;
    rx_fsm_machine_init(&node->machine, config->node_id, RAFT_ROLE_FOLLOWER,
                        RAFT_TRANSITIONS, sizeof(RAFT_TRANSITIONS) / sizeof(RAFT_TRANSITIONS[0]),
                        node, raft_node_latch_inputs, raft_node_dump_outputs);
}

int raft_cluster_init(raft_cluster_t *cluster) {
    memset(cluster, 0, sizeof(*cluster));
    raft_memory_transport_init(&cluster->transport);
    return rx_fsm_runtime_init(&cluster->runtime, RAFT_MAX_NODES);
}

void raft_cluster_destroy(raft_cluster_t *cluster) {
    rx_fsm_runtime_free(&cluster->runtime);
}

#ifdef RX_TRACE_ENABLE
int raft_cluster_attach_trace(raft_cluster_t *cluster, rx_trace_buf_t *trace) {
    if (!cluster || !trace) {
        return -1;
    }
    cluster->trace = trace;
    cluster_register_trace_labels(cluster);
    if (rx_trace_attach_runtime(trace, &cluster->runtime.runtime) != 0) {
        return -1;
    }
    for (size_t i = 0; i < cluster->node_count; ++i) {
        node_register_trace_metadata(cluster, &cluster->nodes[i]);
    }
    return 0;
}
#endif

raft_node_t *raft_cluster_add_node(raft_cluster_t *cluster, const raft_node_config_t *config,
                                   const char *root_dir) {
    raft_node_t *node;
    if (cluster->node_count >= RAFT_MAX_NODES) {
        return NULL;
    }
    node = &cluster->nodes[cluster->node_count++];
    raft_memory_transport_register_node(&cluster->transport, config->node_id);
    raft_node_init(node, config, &cluster->clock_ms, &cluster->transport, root_dir);
    if (rx_fsm_runtime_add_machine(&cluster->runtime, &node->machine, 0, 0) != 0) {
        return NULL;
    }
#ifdef RX_TRACE_ENABLE
    if (cluster->trace) {
        cluster_register_trace_labels(cluster);
        if (rx_trace_attach_runtime(cluster->trace, &cluster->runtime.runtime) != 0) {
            return NULL;
        }
        node_register_trace_metadata(cluster, node);
    }
#endif
    return node;
}

int raft_cluster_tick(raft_cluster_t *cluster, int advance_ms) {
    cluster->clock_ms += advance_ms;
    return rx_fsm_tick(&cluster->runtime);
}

raft_node_t *raft_cluster_leader(raft_cluster_t *cluster) {
    size_t i;
    for (i = 0; i < cluster->node_count; ++i) {
        if (node_is_leader(&cluster->nodes[i])) {
            return &cluster->nodes[i];
        }
    }
    return NULL;
}

int raft_node_submit_command(raft_node_t *node, const raft_command_t *command) {
    if (!node->running || !node_self_is_member(node)) {
        return -1;
    }
    return raft_memory_transport_submit_client_command(node->transport, node->config.node_id, command);
}

int raft_node_request_membership_change(raft_node_t *node, const char members[][RAFT_MAX_ID], size_t member_count) {
    if (!node_is_leader(node)) {
        return -1;
    }
    node->requested_membership_count = normalize_members(members, member_count,
                                                         node->requested_membership, RAFT_MAX_NODES);
    if (node->requested_membership_count == 0) {
        return -1;
    }
    node->requested_membership_pending = 1;
#ifdef RX_TRACE_ENABLE
    node_trace_user(node, RAFT_TRACE_MEMBERSHIP_REQUESTED, (uint16_t)node->requested_membership_count);
#endif
    return 0;
}

const char *raft_node_get(raft_node_t *node, const char *key) {
    size_t i;
    for (i = 0; i < node->kv_count; ++i) {
        if (node->kv[i].used && strcmp(node->kv[i].key, key) == 0) {
            return node->kv[i].value;
        }
    }
    return NULL;
}

static int node_append_internal_command(raft_node_t *node, const char *op, const char *key, const char *value) {
    raft_log_entry_t *entry;
    if (!node_is_leader(node) || node->log_count >= RAFT_MAX_LOG) {
        return -1;
    }
    entry = &node->log[node->log_count];
    memset(entry, 0, sizeof(*entry));
    entry->index = (int)node->log_count + 1;
    entry->term = node->current_term;
    strncpy(entry->command.op, op, sizeof(entry->command.op) - 1);
    strncpy(entry->command.key, key, sizeof(entry->command.key) - 1);
    strncpy(entry->command.value, value, sizeof(entry->command.value) - 1);
    node->log_count++;
    node->persist_dirty = 1;
    node_broadcast_append_entries(node);
    return entry->index;
}

static void node_maybe_drive_membership(raft_node_t *node) {
    char encoded[RAFT_MAX_VALUE];
    if (!node_is_leader(node) || !node->running) {
        return;
    }
    if (node->membership_state == RAFT_MEMBERSHIP_STABLE &&
        node->requested_membership_pending &&
        !configuration_is_joint(&node->config_state)) {
        encode_member_list((const char (*)[RAFT_MAX_ID])node->requested_membership,
                           node->requested_membership_count, encoded, sizeof(encoded));
        node->joint_config_index = node_append_internal_command(node, "cluster.enter_joint",
                                                                "members", encoded);
        if (node->joint_config_index > 0) {
            node->membership_state = RAFT_MEMBERSHIP_JOINT_PENDING;
        }
        return;
    }
    if (node->membership_state == RAFT_MEMBERSHIP_JOINT_PENDING &&
        configuration_is_joint(&node->config_state) &&
        node->config_state.index == node->joint_config_index) {
        node->membership_state = RAFT_MEMBERSHIP_JOINT;
        return;
    }
    if (node->membership_state == RAFT_MEMBERSHIP_JOINT &&
        configuration_is_joint(&node->config_state) &&
        node_joint_members_are_caught_up(node)) {
#ifdef RX_TRACE_ENABLE
        node_trace_user(node, RAFT_TRACE_MEMBERSHIP_CATCHUP_READY, (uint16_t)node_last_log_index(node));
#endif
        encode_member_list((const char (*)[RAFT_MAX_ID])node->requested_membership,
                           node->requested_membership_count, encoded, sizeof(encoded));
        node->final_config_index = node_append_internal_command(node, "cluster.leave_joint",
                                                                "members", encoded);
        if (node->final_config_index > 0) {
            node->membership_state = RAFT_MEMBERSHIP_FINALIZING;
        }
        return;
    }
    if (node->membership_state == RAFT_MEMBERSHIP_FINALIZING &&
        !configuration_is_joint(&node->config_state) &&
        node->config_state.index == node->final_config_index) {
        node->membership_state = RAFT_MEMBERSHIP_STABLE;
    }
}

static void raft_node_latch_inputs(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t messages[RAFT_MAX_QUEUE];
    raft_command_t commands[RAFT_MAX_QUEUE];
    size_t i, nmsg, ncmd;
    (void)ctx;
    if (!node->running) {
        node->pending_append_count = 0;
        node->pending_vote_request_count = 0;
        node->pending_vote_count = 0;
        node->outbox_count = 0;
        return;
    }
    node->pending_append_count = 0;
    node->pending_vote_request_count = 0;
    node->pending_vote_count = 0;
    nmsg = raft_memory_transport_recv_for(node->transport, node->config.node_id, messages, RAFT_MAX_QUEUE);
    for (i = 0; i < nmsg; ++i) {
        raft_message_t *message = &messages[i];
        if (message->kind == RAFT_MSG_APPEND_ENTRIES) {
            node->pending_append_entries[node->pending_append_count++] = *message;
        } else if (message->kind == RAFT_MSG_REQUEST_VOTE) {
            node->pending_vote_requests[node->pending_vote_request_count++] = *message;
        } else if (message->kind == RAFT_MSG_REQUEST_VOTE_RESPONSE) {
            node->pending_votes[node->pending_vote_count++] = *message;
        } else if (message->kind == RAFT_MSG_APPEND_ENTRIES_RESPONSE) {
            node_handle_append_entries_response(node, message);
        }
    }
    if (node_is_leader(node)) {
        ncmd = raft_memory_transport_recv_client_commands(node->transport, node->config.node_id,
                                                          commands, RAFT_MAX_QUEUE);
        for (i = 0; i < ncmd; ++i) {
            raft_log_entry_t *entry;
            if (node->log_count >= RAFT_MAX_LOG) {
                break;
            }
            entry = &node->log[node->log_count];
            memset(entry, 0, sizeof(*entry));
            entry->index = (int)node->log_count + 1;
            entry->term = node->current_term;
            entry->command = commands[i];
            node->log_count++;
            node->persist_dirty = 1;
            node_broadcast_append_entries(node);
        }
    } else {
        raft_memory_transport_recv_client_commands(node->transport, node->config.node_id, commands, RAFT_MAX_QUEUE);
    }
}

static void raft_node_dump_outputs(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    size_t i;
    (void)ctx;
    if (!node->running) {
        node->outbox_count = 0;
        return;
    }
    while (node->last_applied < node->commit_index) {
        node->last_applied++;
        node_apply_command(node, &node->log[node->last_applied - 1].command);
#ifdef RX_TRACE_ENABLE
        node_trace_user(node, RAFT_TRACE_APPLY, (uint16_t)node->last_applied);
#endif
        node->persist_dirty = 1;
    }
    node_maybe_drive_membership(node);
    if (node->persist_dirty) {
        node_save_persistent(node);
        node->persist_dirty = 0;
    }
    for (i = 0; i < node->outbox_count; ++i) {
        raft_memory_transport_send(node->transport, &node->outbox[i]);
    }
    node->outbox_count = 0;
}

static int guard_timeout_expired(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return node->running && node_self_is_member(node) && *node->clock_ms >= node->election_deadline_ms;
}

static int guard_has_append_entries(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return node->pending_append_count > 0;
}

static int guard_has_vote_request(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return node->pending_vote_request_count > 0;
}

static int guard_has_vote(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return node->pending_vote_count > 0;
}

static int guard_received_majority_votes(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    if (!node_has_vote_quorum(node, node->config_state.old_members, node->config_state.old_count)) {
        return 0;
    }
    if (!configuration_is_joint(&node->config_state)) {
        return 1;
    }
    return node_has_vote_quorum(node, node->config_state.new_members, node->config_state.new_count);
}

static int guard_time_for_heartbeat(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return *node->clock_ms >= node->heartbeat_deadline_ms;
}

static void action_become_candidate(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    size_t i;
    (void)ctx;
    node->current_term++;
    strncpy(node->voted_for, node->config.node_id, RAFT_MAX_ID - 1);
    node->vote_count = 0;
    node_record_vote(node, node->config.node_id);
    node->persist_dirty = 1;
    node->election_deadline_ms = *node->clock_ms + node->config.election_timeout_ms;
    node->leader_id[0] = '\0';
    for (i = 0; i < node->config.peer_count; ++i) {
        memset(&message, 0, sizeof(message));
        message.kind = RAFT_MSG_REQUEST_VOTE;
        message.term = node->current_term;
        strncpy(message.source, node->config.node_id, RAFT_MAX_ID - 1);
        strncpy(message.target, node->config.peers[i], RAFT_MAX_ID - 1);
        message.last_log_index = node_last_log_index(node);
        message.last_log_term = node_last_log_term(node);
        node_queue_message(node, &message);
    }
}

static void action_handle_append_entries(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    (void)ctx;
    if (node->pending_append_count == 0) {
        return;
    }
    message = node->pending_append_entries[0];
    memmove(&node->pending_append_entries[0], &node->pending_append_entries[1],
            sizeof(node->pending_append_entries[0]) * (node->pending_append_count - 1));
    node->pending_append_count--;
    node_handle_append_entries(node, &message);
}

static void action_handle_vote_request(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    (void)ctx;
    if (node->pending_vote_request_count == 0) {
        return;
    }
    message = node->pending_vote_requests[0];
    memmove(&node->pending_vote_requests[0], &node->pending_vote_requests[1],
            sizeof(node->pending_vote_requests[0]) * (node->pending_vote_request_count - 1));
    node->pending_vote_request_count--;
    node_handle_vote_request(node, &message);
}

static void action_ignore_vote(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    if (node->pending_vote_count > 0) {
        memmove(&node->pending_votes[0], &node->pending_votes[1],
                sizeof(node->pending_votes[0]) * (node->pending_vote_count - 1));
        node->pending_vote_count--;
    }
}

static void action_back_to_follower_due_to_timeout(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    node->vote_count = 0;
    node->leader_id[0] = '\0';
    node->election_deadline_ms = *node->clock_ms + node->config.election_timeout_ms;
}

static void action_become_leader(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    size_t i;
    (void)ctx;
    strncpy(node->leader_id, node->config.node_id, RAFT_MAX_ID - 1);
    for (i = 0; i < node->config.peer_count; ++i) {
        node->next_index[i] = node_last_log_index(node) + 1;
        node->match_index[i] = 0;
    }
    node_broadcast_append_entries(node);
    node->heartbeat_deadline_ms = *node->clock_ms + node->config.heartbeat_interval_ms;
}

static void action_handle_vote(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    (void)ctx;
    if (node->pending_vote_count == 0) {
        return;
    }
    message = node->pending_votes[0];
    memmove(&node->pending_votes[0], &node->pending_votes[1],
            sizeof(node->pending_votes[0]) * (node->pending_vote_count - 1));
    node->pending_vote_count--;
    node_handle_vote_response(node, &message);
}

static void action_ignore_vote_request(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    if (node->pending_vote_request_count > 0) {
        memmove(&node->pending_vote_requests[0], &node->pending_vote_requests[1],
                sizeof(node->pending_vote_requests[0]) * (node->pending_vote_request_count - 1));
        node->pending_vote_request_count--;
    }
}

static void action_send_heartbeat(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    node_broadcast_append_entries(node);
    node->heartbeat_deadline_ms = *node->clock_ms + node->config.heartbeat_interval_ms;
}
