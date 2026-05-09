// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: MIT

#include "raft/raft.h"
#include "raft_storage_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Guards ──────────────────────────────────────────────────────────────── */
static int guard_timeout_expired(const rx_fsm_context *ctx, void *user);
static int guard_has_append_entries(const rx_fsm_context *ctx, void *user);
static int guard_has_vote_request(const rx_fsm_context *ctx, void *user);
static int guard_has_higher_term_vote_request(const rx_fsm_context *ctx, void *user);
static int guard_has_vote(const rx_fsm_context *ctx, void *user);
static int guard_received_majority_votes(const rx_fsm_context *ctx, void *user);
static int guard_time_for_heartbeat(const rx_fsm_context *ctx, void *user);

/* ── Actions ─────────────────────────────────────────────────────────────── */
static void action_become_candidate(rx_fsm_context *ctx, void *user);
static void action_handle_append_entries(rx_fsm_context *ctx, void *user);
static void action_handle_vote_request(rx_fsm_context *ctx, void *user);
static void action_ignore_vote(rx_fsm_context *ctx, void *user);
static void action_back_to_follower_due_to_timeout(rx_fsm_context *ctx, void *user);
static void action_become_leader(rx_fsm_context *ctx, void *user);
static void action_handle_vote(rx_fsm_context *ctx, void *user);
static void action_ignore_vote_request(rx_fsm_context *ctx, void *user);
static void action_send_heartbeat(rx_fsm_context *ctx, void *user);

/* ── Phase callbacks ─────────────────────────────────────────────────────── */
static void raft_node_latch_inputs(rx_fsm_context *ctx, void *user);
static void raft_node_dump_outputs(rx_fsm_context *ctx, void *user);

/* ── Internal helpers ────────────────────────────────────────────────────── */
static int  configuration_is_joint(const raft_cluster_configuration_t *config);
static void configuration_set_stable(raft_cluster_configuration_t *config,
                                     const char members[][RAFT_MAX_ID], size_t member_count, int index);
static void configuration_set_joint(raft_cluster_configuration_t *config,
                                    const char old_members[][RAFT_MAX_ID], size_t old_count,
                                    const char new_members[][RAFT_MAX_ID], size_t new_count, int index);
static size_t normalize_members(const char members[][RAFT_MAX_ID], size_t member_count,
                                char out[][RAFT_MAX_ID], size_t capacity);
static int  member_list_contains(const char members[][RAFT_MAX_ID], size_t member_count,
                                  const char *node_id);
static void node_refresh_peers(raft_node_t *node);
static int  node_self_is_member(const raft_node_t *node);
static int  node_has_vote_quorum(const raft_node_t *node,
                                  const char members[][RAFT_MAX_ID], size_t member_count);
static int  node_replicated_on_members(const raft_node_t *node, int candidate_index,
                                        const char members[][RAFT_MAX_ID], size_t member_count);
static int  node_is_committed_under_current_configuration(const raft_node_t *node, int candidate_index);
static int  node_joint_members_are_caught_up(const raft_node_t *node);
static int  node_append_internal_command(raft_node_t *node,
                                          const char *op, const char *key, const char *value);
static void node_maybe_drive_membership(raft_node_t *node);
static void node_apply_internal_command(raft_node_t *node, const raft_command_t *command);
static void node_do_compact(raft_node_t *node);
static void encode_member_list(const char members[][RAFT_MAX_ID], size_t member_count,
                               char *out, size_t out_size);
static size_t decode_member_list(const char *value, char out[][RAFT_MAX_ID], size_t capacity);
static long node_next_election_timeout(const raft_node_t *node);

/* ── Log-index helpers (compaction-aware) ────────────────────────────────── */

static int node_last_log_index(const raft_node_t *node) {
    return node->snapshot_last_included_index + (int)node->log_count;
}

static int node_last_log_term(const raft_node_t *node) {
    if (node->log_count == 0) return node->snapshot_last_included_term;
    return node->log[node->log_count - 1].term;
}

/* First logical index stored in node->log[0]. */
static int node_log_base_index(const raft_node_t *node) {
    return node->snapshot_last_included_index + 1;
}

/* Converts a logical log index to a physical array index. */
static int node_physical_index(const raft_node_t *node, int index) {
    return index - node->snapshot_last_included_index - 1;
}

static int node_has_log_index(const raft_node_t *node, int index) {
    return index >= node_log_base_index(node) && index <= node_last_log_index(node);
}

static int node_term_at_index(const raft_node_t *node, int index) {
    if (index == 0) return 0;
    if (index == node->snapshot_last_included_index) return node->snapshot_last_included_term;
    return node->log[node_physical_index(node, index)].term;
}

static const char *node_leader_at_index(const raft_node_t *node, int index) {
    if (index <= 0 || !node_has_log_index(node, index)) return "";
    return node->log[node_physical_index(node, index)].leader_id;
}

/* ── Trace labels ────────────────────────────────────────────────────────── */
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
    RAFT_TRACE_COMPACTION = 8,
};

static void cluster_register_trace_labels(raft_cluster_t *cluster);
static void node_register_trace_metadata(raft_cluster_t *cluster, raft_node_t *node);
static void node_trace_user(raft_node_t *node, uint8_t label, uint16_t value);
#endif

/* ── FSM transition table ────────────────────────────────────────────────── */
static const rx_fsm_transition RAFT_TRANSITIONS[] = {
    /* FOLLOWER transitions */
    {RAFT_ROLE_FOLLOWER,   RAFT_ROLE_CANDIDATE, guard_timeout_expired,              action_become_candidate},
    {RAFT_ROLE_FOLLOWER,   RAFT_ROLE_FOLLOWER,  guard_has_append_entries,           action_handle_append_entries},
    {RAFT_ROLE_FOLLOWER,   RAFT_ROLE_FOLLOWER,  guard_has_vote_request,             action_handle_vote_request},
    {RAFT_ROLE_FOLLOWER,   RAFT_ROLE_FOLLOWER,  guard_has_vote,                     action_ignore_vote},
    /* CANDIDATE transitions */
    {RAFT_ROLE_CANDIDATE,  RAFT_ROLE_FOLLOWER,  guard_timeout_expired,              action_back_to_follower_due_to_timeout},
    {RAFT_ROLE_CANDIDATE,  RAFT_ROLE_LEADER,    guard_received_majority_votes,      action_become_leader},
    {RAFT_ROLE_CANDIDATE,  RAFT_ROLE_FOLLOWER,  guard_has_append_entries,           action_handle_append_entries},
    /* Higher-term vote request causes immediate step-down */
    {RAFT_ROLE_CANDIDATE,  RAFT_ROLE_FOLLOWER,  guard_has_higher_term_vote_request, action_handle_vote_request},
    {RAFT_ROLE_CANDIDATE,  RAFT_ROLE_CANDIDATE, guard_has_vote_request,             action_handle_vote_request},
    {RAFT_ROLE_CANDIDATE,  RAFT_ROLE_CANDIDATE, guard_has_vote,                     action_handle_vote},
    /* LEADER transitions */
    {RAFT_ROLE_LEADER,     RAFT_ROLE_FOLLOWER,  guard_has_append_entries,           action_handle_append_entries},
    /* Higher-term vote request causes immediate step-down */
    {RAFT_ROLE_LEADER,     RAFT_ROLE_FOLLOWER,  guard_has_higher_term_vote_request, action_handle_vote_request},
    {RAFT_ROLE_LEADER,     RAFT_ROLE_LEADER,    guard_has_vote_request,             action_ignore_vote_request},
    {RAFT_ROLE_LEADER,     RAFT_ROLE_LEADER,    guard_has_vote,                     action_ignore_vote},
    {RAFT_ROLE_LEADER,     RAFT_ROLE_LEADER,    guard_time_for_heartbeat,           action_send_heartbeat},
};

/* ──────────────────────────────────────────────────────────────────────────
   Trace helpers
   ────────────────────────────────────────────────────────────────────────── */
#ifdef RX_TRACE_ENABLE
static void cluster_register_trace_labels(raft_cluster_t *cluster) {
    rx_trace_buf_t *trace;
    if (!cluster || !cluster->trace || cluster->trace_labels_registered) return;
    trace = cluster->trace;
    rx_trace_set_label_name(trace, RAFT_TRACE_REPL_SEND,              "raft.repl.send");
    rx_trace_set_label_name(trace, RAFT_TRACE_REPL_MATCH_ADVANCED,    "raft.repl.match_advanced");
    rx_trace_set_label_name(trace, RAFT_TRACE_COMMIT,                 "raft.commit");
    rx_trace_set_label_name(trace, RAFT_TRACE_APPLY,                  "raft.apply");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_REQUESTED,   "raft.membership.requested");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_JOINT_COMMITTED,  "raft.membership.joint_committed");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_CATCHUP_READY,    "raft.membership.catchup_ready");
    rx_trace_set_label_name(trace, RAFT_TRACE_MEMBERSHIP_STABLE_COMMITTED, "raft.membership.stable_committed");
    rx_trace_set_label_name(trace, RAFT_TRACE_COMPACTION,             "raft.compaction");
    cluster->trace_labels_registered = 1;
}

static void node_register_trace_metadata(raft_cluster_t *cluster, raft_node_t *node) {
    if (!cluster || !cluster->trace || !node || node->machine.node.trace != cluster->trace) return;
    rx_trace_set_node_name(cluster->trace, node->machine.node.trace_nid, node->config.node_id);
    rx_trace_set_state_name(cluster->trace, node->machine.node.trace_nid, RAFT_ROLE_FOLLOWER,  "FOLLOWER");
    rx_trace_set_state_name(cluster->trace, node->machine.node.trace_nid, RAFT_ROLE_CANDIDATE, "CANDIDATE");
    rx_trace_set_state_name(cluster->trace, node->machine.node.trace_nid, RAFT_ROLE_LEADER,    "LEADER");
}

static void node_trace_user(raft_node_t *node, uint8_t label, uint16_t value) {
    if (!node || !node->machine.node.trace) return;
    rx_trace_user(node->machine.node.trace, label, value);
}
#endif

/* ──────────────────────────────────────────────────────────────────────────
   Election timeout — pseudo-random offset via FNV-1a hash to reduce
   split-vote probability (mirrors Python's crc32-based jitter).
   ────────────────────────────────────────────────────────────────────────── */
static long node_next_election_timeout(const raft_node_t *node) {
    uint32_t h = 2166136261u;
    const char *p = node->config.node_id;
    int base = node->config.election_timeout_ms;
    if (base <= 1) return base;
    while (*p) { h ^= (uint8_t)*p++; h *= 16777619u; }
    h ^= (uint32_t)node->current_term;    h *= 16777619u;
    h ^= (uint32_t)(*node->clock_ms & 0xFFFFFFFF); h *= 16777619u;
    return base + (long)(h % (uint32_t)base);
}

/* ──────────────────────────────────────────────────────────────────────────
   Configuration helpers
   ────────────────────────────────────────────────────────────────────────── */
static int configuration_is_joint(const raft_cluster_configuration_t *config) {
    return config->new_count > 0;
}

static size_t normalize_members(const char members[][RAFT_MAX_ID], size_t member_count,
                                char out[][RAFT_MAX_ID], size_t capacity) {
    size_t i, j, out_count = 0;
    for (i = 0; i < member_count && out_count < capacity; ++i) {
        if (members[i][0] == '\0') continue;
        for (j = 0; j < out_count; ++j) {
            if (strcmp(out[j], members[i]) == 0) break;
        }
        if (j == out_count) {
            strncpy(out[out_count], members[i], RAFT_MAX_ID - 1);
            out[out_count][RAFT_MAX_ID - 1] = '\0';
            out_count++;
        }
    }
    for (i = 1; i < out_count; ++i) {
        char tmp[RAFT_MAX_ID];
        j = i;
        while (j > 0 && strcmp(out[j - 1], out[j]) > 0) {
            strncpy(tmp, out[j - 1], RAFT_MAX_ID - 1);
            tmp[RAFT_MAX_ID - 1] = '\0';
            strncpy(out[j - 1], out[j], RAFT_MAX_ID - 1);
            out[j - 1][RAFT_MAX_ID - 1] = '\0';
            strncpy(out[j], tmp, RAFT_MAX_ID - 1);
            out[j][RAFT_MAX_ID - 1] = '\0';
            j--;
        }
    }
    return out_count;
}

static void configuration_set_stable(raft_cluster_configuration_t *config,
                                     const char members[][RAFT_MAX_ID], size_t member_count,
                                     int index) {
    char normalized[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t norm_count = normalize_members(members, member_count, normalized, RAFT_MAX_NODES);
    memset(config, 0, sizeof(*config));
    memcpy(config->old_members, normalized, sizeof(normalized));
    config->old_count = norm_count;
    config->index = index;
}

static void configuration_set_joint(raft_cluster_configuration_t *config,
                                    const char old_members[][RAFT_MAX_ID], size_t old_count,
                                    const char new_members[][RAFT_MAX_ID], size_t new_count,
                                    int index) {
    char norm_old[RAFT_MAX_NODES][RAFT_MAX_ID];
    char norm_new[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t nold = normalize_members(old_members, old_count, norm_old, RAFT_MAX_NODES);
    size_t nnew = normalize_members(new_members, new_count, norm_new, RAFT_MAX_NODES);
    memset(config, 0, sizeof(*config));
    memcpy(config->old_members, norm_old, sizeof(norm_old));
    memcpy(config->new_members, norm_new, sizeof(norm_new));
    config->old_count = nold;
    config->new_count = nnew;
    config->index = index;
}

static int member_list_contains(const char members[][RAFT_MAX_ID], size_t member_count,
                                 const char *node_id) {
    size_t i;
    for (i = 0; i < member_count; ++i) {
        if (strcmp(members[i], node_id) == 0) return 1;
    }
    return 0;
}

static void node_refresh_peers(raft_node_t *node) {
    size_t i, peer_count = 0;
    memset(node->config.peers, 0, sizeof(node->config.peers));
    for (i = 0; i < node->config_state.old_count && peer_count < RAFT_MAX_PEERS; ++i) {
        if (strcmp(node->config_state.old_members[i], node->config.node_id) != 0) {
            strncpy(node->config.peers[peer_count], node->config_state.old_members[i], RAFT_MAX_ID - 1);
            peer_count++;
        }
    }
    for (i = 0; i < node->config_state.new_count && peer_count < RAFT_MAX_PEERS; ++i) {
        const char *m = node->config_state.new_members[i];
        if (strcmp(m, node->config.node_id) != 0 &&
            !member_list_contains((const char (*)[RAFT_MAX_ID])node->config.peers, peer_count, m)) {
            strncpy(node->config.peers[peer_count], m, RAFT_MAX_ID - 1);
            peer_count++;
        }
    }
    node->config.peer_count = peer_count;
}

static int node_self_is_member(const raft_node_t *node) {
    if (member_list_contains(node->config_state.old_members,
                              node->config_state.old_count, node->config.node_id)) return 1;
    return member_list_contains(node->config_state.new_members,
                                 node->config_state.new_count, node->config.node_id);
}

static int node_is_leader(const raft_node_t *node) {
    return node->running && node_self_is_member(node) && node->machine.state == RAFT_ROLE_LEADER;
}

static int member_majority(size_t member_count) {
    return (int)(member_count / 2) + 1;
}

static int node_peer_index(const raft_node_t *node, const char *peer_id) {
    size_t i;
    for (i = 0; i < node->config.peer_count; ++i) {
        if (strcmp(node->config.peers[i], peer_id) == 0) return (int)i;
    }
    return -1;
}

static void node_record_vote(raft_node_t *node, const char *peer_id) {
    size_t i;
    for (i = 0; i < node->vote_count; ++i) {
        if (strcmp(node->votes_received[i], peer_id) == 0) return;
    }
    if (node->vote_count < RAFT_MAX_PEERS + 1) {
        strncpy(node->votes_received[node->vote_count], peer_id, RAFT_MAX_ID - 1);
        node->vote_count++;
    }
}

static int node_has_vote_quorum(const raft_node_t *node,
                                 const char members[][RAFT_MAX_ID], size_t member_count) {
    size_t i;
    int votes = 0;
    for (i = 0; i < member_count; ++i) {
        if (member_list_contains((const char (*)[RAFT_MAX_ID])node->votes_received,
                                  node->vote_count, members[i]))
            votes++;
    }
    return votes >= member_majority(member_count);
}

static void node_queue_message(raft_node_t *node, const raft_message_t *message) {
    if (node->outbox_count < RAFT_MAX_QUEUE)
        node->outbox[node->outbox_count++] = *message;
}

/* ──────────────────────────────────────────────────────────────────────────
   Replication
   ────────────────────────────────────────────────────────────────────────── */
static void node_send_append_entries_to(raft_node_t *node, const char *peer_id) {
    raft_message_t message;
    int peer_idx = node_peer_index(node, peer_id);
    int next_idx = peer_idx >= 0 ? node->next_index[peer_idx] : node_last_log_index(node) + 1;
    int log_base = node_log_base_index(node);
    int i, prev_index, prev_term;
    size_t count = 0;

    if (next_idx < log_base) next_idx = log_base;
    prev_index = next_idx - 1;
    prev_term  = node_term_at_index(node, prev_index);

    memset(&message, 0, sizeof(message));
    message.kind          = RAFT_MSG_APPEND_ENTRIES;
    message.term          = node->current_term;
    message.prev_log_index = prev_index;
    message.prev_log_term  = prev_term;
    strncpy(message.prev_log_leader_id, node_leader_at_index(node, prev_index), RAFT_MAX_ID - 1);
    message.leader_commit  = node->commit_index;
    strncpy(message.source, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(message.target, peer_id, RAFT_MAX_ID - 1);

    for (i = next_idx; i <= node_last_log_index(node) && count < RAFT_MAX_BATCH; ++i) {
        message.entries[count++] = node->log[node_physical_index(node, i)];
    }
    message.entry_count = count;
    node_queue_message(node, &message);
#ifdef RX_TRACE_ENABLE
    node_trace_user(node, RAFT_TRACE_REPL_SEND, (uint16_t)count);
#endif
}

static void node_broadcast_append_entries(raft_node_t *node) {
    size_t i;
    for (i = 0; i < node->config.peer_count; ++i)
        node_send_append_entries_to(node, node->config.peers[i]);
}

static int node_candidate_is_up_to_date(const raft_node_t *node, const raft_message_t *message) {
    int my_term  = node_last_log_term(node);
    int my_index = node_last_log_index(node);
    if (message->last_log_term != my_term) return message->last_log_term > my_term;
    return message->last_log_index >= my_index;
}

static int node_replicated_on_members(const raft_node_t *node, int candidate_index,
                                       const char members[][RAFT_MAX_ID], size_t member_count) {
    size_t i;
    int replicated = 0;
    for (i = 0; i < member_count; ++i) {
        if (strcmp(members[i], node->config.node_id) == 0) {
            if (node_last_log_index(node) >= candidate_index) replicated++;
        } else {
            int peer_idx = node_peer_index(node, members[i]);
            if (peer_idx >= 0 && node->match_index[peer_idx] >= candidate_index) replicated++;
        }
    }
    return replicated >= member_majority(member_count);
}

static int node_is_committed_under_current_configuration(const raft_node_t *node, int candidate_index) {
    if (!node_replicated_on_members(node, candidate_index,
                                    node->config_state.old_members, node->config_state.old_count))
        return 0;
    if (!configuration_is_joint(&node->config_state)) return 1;
    return node_replicated_on_members(node, candidate_index,
                                      node->config_state.new_members, node->config_state.new_count);
}

static int node_joint_members_are_caught_up(const raft_node_t *node) {
    size_t i;
    int target = node_last_log_index(node);
    if (!configuration_is_joint(&node->config_state)) return 0;
    for (i = 0; i < node->config_state.new_count; ++i) {
        const char *m = node->config_state.new_members[i];
        if (strcmp(m, node->config.node_id) == 0) continue;
        int peer_idx = node_peer_index(node, m);
        if (peer_idx < 0 || node->match_index[peer_idx] < target) return 0;
    }
    return 1;
}

static void node_advance_commit_index(raft_node_t *node) {
    int idx;
    for (idx = node_last_log_index(node); idx > node->commit_index; --idx) {
        if (node_term_at_index(node, idx) != node->current_term) continue;
        if (node_is_committed_under_current_configuration(node, idx)) {
            node->commit_index = idx;
            node->persist_dirty = 1;
#ifdef RX_TRACE_ENABLE
            node_trace_user(node, RAFT_TRACE_COMMIT, (uint16_t)idx);
#endif
            return;
        }
    }
}

/* ──────────────────────────────────────────────────────────────────────────
   Message handling
   ────────────────────────────────────────────────────────────────────────── */
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
            node->election_deadline_ms = now + node_next_election_timeout(node);
            node->persist_dirty = 1;
            granted = 1;
        }
    }
    memset(&response, 0, sizeof(response));
    response.kind   = RAFT_MSG_REQUEST_VOTE_RESPONSE;
    response.term   = node->current_term;
    response.granted = granted;
    strncpy(response.source, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(response.target, message->source, RAFT_MAX_ID - 1);
    node_queue_message(node, &response);
}

static int node_append_entries_from_leader(raft_node_t *node, const raft_message_t *message) {
    size_t i;

    /* prev_index must not be before our snapshot boundary */
    if (message->prev_log_index < node->snapshot_last_included_index) return 0;
    if (message->prev_log_index > node_last_log_index(node)) return 0;
    if (message->prev_log_index > 0 &&
        node_term_at_index(node, message->prev_log_index) != message->prev_log_term) return 0;
    /* Detect cross-cluster log conflict: same term but different creator.
     * This happens when two independently-bootstrapped clusters are merged
     * and share the same term numbers. */
    if (message->prev_log_index > 0 && message->prev_log_leader_id[0] != '\0' &&
        node_has_log_index(node, message->prev_log_index) &&
        strcmp(node_leader_at_index(node, message->prev_log_index),
               message->prev_log_leader_id) != 0) return 0;

    for (i = 0; i < message->entry_count; ++i) {
        const raft_log_entry_t *entry = &message->entries[i];

        /* Skip entries already covered by our snapshot */
        if (entry->index <= node->snapshot_last_included_index) continue;

        if (node_has_log_index(node, entry->index)) {
            int conflict = node_term_at_index(node, entry->index) != entry->term;
            if (!conflict && entry->leader_id[0] != '\0') {
                conflict = strcmp(node_leader_at_index(node, entry->index),
                                  entry->leader_id) != 0;
            }
            if (conflict) {
                /* Truncate conflicting suffix */
                node->log_count = (size_t)node_physical_index(node, entry->index);
                node->persist_dirty = 1;
            } else {
                continue;
            }
        }

        if (entry->index == node_last_log_index(node) + 1 && node->log_count < RAFT_MAX_LOG) {
            node->log[node->log_count++] = *entry;
            node->persist_dirty = 1;
        }
    }

    if (message->leader_commit > node->commit_index) {
        int last = node_last_log_index(node);
        node->commit_index = message->leader_commit < last ? message->leader_commit : last;
        node->persist_dirty = 1;
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
        node->election_deadline_ms = now + node_next_election_timeout(node);
        success = node_append_entries_from_leader(node, message);
    }
    memset(&response, 0, sizeof(response));
    response.kind        = RAFT_MSG_APPEND_ENTRIES_RESPONSE;
    response.term        = node->current_term;
    response.success     = success;
    response.match_index = node_last_log_index(node);
    strncpy(response.source, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(response.target, message->source, RAFT_MAX_ID - 1);
    node_queue_message(node, &response);
}

static void node_handle_vote_response(raft_node_t *node, const raft_message_t *message) {
    node_handle_term_update(node, message);
    if (!node_self_is_member(node) ||
        node->machine.state != RAFT_ROLE_CANDIDATE ||
        message->term != node->current_term) return;
    if (message->granted) node_record_vote(node, message->source);
}

static void node_handle_append_entries_response(raft_node_t *node, const raft_message_t *message) {
    int peer_idx;
    node_handle_term_update(node, message);
    if (node->machine.state != RAFT_ROLE_LEADER || message->term != node->current_term) return;
    peer_idx = node_peer_index(node, message->source);
    if (peer_idx < 0) return;
    if (message->success) {
        node->match_index[peer_idx] = message->match_index;
        node->next_index[peer_idx]  = message->match_index + 1;
#ifdef RX_TRACE_ENABLE
        node_trace_user(node, RAFT_TRACE_REPL_MATCH_ADVANCED, (uint16_t)message->match_index);
#endif
        node_advance_commit_index(node);
        return;
    }
    if (node->next_index[peer_idx] > node_log_base_index(node))
        node->next_index[peer_idx]--;
    node_send_append_entries_to(node, message->source);
}

/* ──────────────────────────────────────────────────────────────────────────
   Membership helpers
   ────────────────────────────────────────────────────────────────────────── */
static void encode_member_list(const char members[][RAFT_MAX_ID], size_t member_count,
                               char *out, size_t out_size) {
    size_t i, used = 0;
    if (out_size == 0) return;
    out[0] = '\0';
    for (i = 0; i < member_count; ++i) {
        int written = snprintf(out + used, out_size - used, "%s%s",
                               i == 0 ? "" : ",", members[i]);
        if (written < 0 || (size_t)written >= out_size - used) { out[out_size - 1] = '\0'; return; }
        used += (size_t)written;
    }
}

static size_t decode_member_list(const char *value, char out[][RAFT_MAX_ID], size_t capacity) {
    char buffer[RAFT_MAX_VALUE];
    char *token;
    size_t count = 0;
    if (!value || value[0] == '\0') return 0;
    strncpy(buffer, value, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';
    token = strtok(buffer, ",");
    while (token && count < capacity) {
        if (token[0] != '\0' &&
            !member_list_contains((const char (*)[RAFT_MAX_ID])out, count, token)) {
            strncpy(out[count], token, RAFT_MAX_ID - 1);
            out[count][RAFT_MAX_ID - 1] = '\0';
            count++;
        }
        token = strtok(NULL, ",");
    }
    return count;
}

static int node_append_internal_command(raft_node_t *node,
                                         const char *op, const char *key, const char *value) {
    raft_log_entry_t *entry;
    if (!node_is_leader(node) || node->log_count >= RAFT_MAX_LOG) return -1;
    entry = &node->log[node->log_count];
    memset(entry, 0, sizeof(*entry));
    entry->index = node_last_log_index(node) + 1;
    entry->term  = node->current_term;
    strncpy(entry->leader_id, node->config.node_id, RAFT_MAX_ID - 1);
    strncpy(entry->command.op,    op,    sizeof(entry->command.op) - 1);
    strncpy(entry->command.key,   key,   sizeof(entry->command.key) - 1);
    strncpy(entry->command.value, value, sizeof(entry->command.value) - 1);
    node->log_count++;
    node->persist_dirty = 1;
    node_broadcast_append_entries(node);
    return entry->index;
}

static void node_apply_internal_command(raft_node_t *node, const raft_command_t *command) {
    if (strcmp(command->op, "cluster.set_maxlog") == 0) {
        int threshold = atoi(command->value);
        if (threshold > 0) {
            node->compaction_threshold = threshold;
            node->persist_dirty = 1;
        }
        return;
    }
    if (strcmp(command->op, "cluster.enter_joint") == 0) {
        char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
        size_t new_count = decode_member_list(command->value, new_members, RAFT_MAX_NODES);
        if (node->last_applied <= node->config_state.index) return;
        if (new_count > 0) {
            configuration_set_joint(&node->config_state,
                                    node->config_state.old_members, node->config_state.old_count,
                                    new_members, new_count, node->last_applied);
            node->membership_state  = RAFT_MEMBERSHIP_JOINT;
            node->joint_config_index = node->last_applied;
            /* Keep requested_membership in sync so a follower that becomes
             * leader can drive the leave_joint with the correct member list. */
            memcpy(node->requested_membership, new_members,
                   new_count * sizeof(new_members[0]));
            node->requested_membership_count   = new_count;
            node->requested_membership_pending = 1;
            node_refresh_peers(node);
            node->persist_dirty = 1;
#ifdef RX_TRACE_ENABLE
            node_trace_user(node, RAFT_TRACE_MEMBERSHIP_JOINT_COMMITTED,
                            (uint16_t)node->config_state.index);
#endif
        }
        return;
    }
    if (strcmp(command->op, "cluster.leave_joint") == 0) {
        char members[RAFT_MAX_NODES][RAFT_MAX_ID];
        size_t member_count = decode_member_list(command->value, members, RAFT_MAX_NODES);
        if (node->last_applied <= node->config_state.index) return;
        if (member_count > 0) {
            int was_member = node_self_is_member(node);
            configuration_set_stable(&node->config_state, members, member_count, node->last_applied);
            node->membership_state   = RAFT_MEMBERSHIP_STABLE;
            node->final_config_index = node->last_applied;
            node->joint_config_index = 0;
            node->requested_membership_pending = 0;
            node->requested_membership_count   = 0;
            node_refresh_peers(node);
            node->persist_dirty = 1;
            /* Only stop a node that was actively participating and is now
             * expelled.  A learner (was_member==0) receiving a leave_joint
             * that doesn't include it yet must keep running so it can
             * continue to receive subsequent AppendEntries that will add it. */
            if (was_member && !node_self_is_member(node)) {
                node->running = 0;
                node->leader_id[0] = '\0';
                node->vote_count   = 0;
                node->machine.state = RAFT_ROLE_FOLLOWER;
            }
#ifdef RX_TRACE_ENABLE
            node_trace_user(node, RAFT_TRACE_MEMBERSHIP_STABLE_COMMITTED,
                            (uint16_t)node->config_state.index);
#endif
        }
    }
}

static void node_apply_command(raft_node_t *node, const raft_command_t *command) {
    if (strcmp(command->op, "cluster.set_maxlog") == 0 ||
        strcmp(command->op, "cluster.enter_joint") == 0 ||
        strcmp(command->op, "cluster.leave_joint") == 0) {
        node_apply_internal_command(node, command);
        return;
    }
    if (node->application.apply)
        node->application.apply(node->application.user, command);
}

/* ──────────────────────────────────────────────────────────────────────────
   Log compaction
   ────────────────────────────────────────────────────────────────────────── */

/*
 * Safe compaction boundary: the minimum acknowledged index across all peers
 * (for leader) or simply last_applied (for followers). This is never applied
 * beyond last_applied, so the application state is always consistent.
 */
static int node_compactible_index(const raft_node_t *node) {
    size_t i;
    int min_idx = node->last_applied;
    if (node->machine.state != RAFT_ROLE_LEADER) return node->last_applied;
    for (i = 0; i < node->config.peer_count; ++i) {
        if (node->match_index[i] < min_idx) min_idx = node->match_index[i];
    }
    return min_idx;
}

static int node_should_compact(const raft_node_t *node) {
    int compactible = node_compactible_index(node);
    return node->compaction_threshold > 0 &&
           (compactible - node->snapshot_last_included_index) >= node->compaction_threshold;
}

static void node_do_compact(raft_node_t *node) {
    int target_index = node_compactible_index(node);
    int target_term;
    size_t keep_from, entries_to_keep;

    if (target_index <= node->snapshot_last_included_index) return;
    if (target_index > node_last_log_index(node)) return;

    target_term = node_term_at_index(node, target_index);

    /* Serialize application state into the storage buffer */
    if (node->application.snapshot)
        node->storage.snapshot_buf_size = node->application.snapshot(
            node->application.user, node->storage.snapshot_buf, RAFT_MAX_SNAPSHOT_SIZE);
    else
        node->storage.snapshot_buf_size = 0;

    /* Persist snapshot data before mutating in-memory state */
    raft_storage_save_snapshot(node, target_index, target_term);

    /* Truncate log prefix through target_index */
    keep_from = (size_t)(target_index - node->snapshot_last_included_index);
    if (keep_from >= node->log_count) {
        node->log_count = 0;
    } else {
        entries_to_keep = node->log_count - keep_from;
        memmove(node->log, node->log + keep_from, sizeof(node->log[0]) * entries_to_keep);
        node->log_count = entries_to_keep;
    }

    node->snapshot_last_included_index = target_index;
    node->snapshot_last_included_term  = target_term;
    node->persist_dirty = 1;

#ifdef RX_TRACE_ENABLE
    node_trace_user(node, RAFT_TRACE_COMPACTION, (uint16_t)target_index);
#endif
}

/* ──────────────────────────────────────────────────────────────────────────
   Membership driver (called every dump_outputs from the leader)
   ────────────────────────────────────────────────────────────────────────── */
static void node_maybe_drive_membership(raft_node_t *node) {
    char encoded[RAFT_MAX_VALUE];
    if (!node_is_leader(node) || !node->running) return;

    if (node->membership_state == RAFT_MEMBERSHIP_STABLE &&
        node->requested_membership_pending &&
        !configuration_is_joint(&node->config_state)) {
        encode_member_list((const char (*)[RAFT_MAX_ID])node->requested_membership,
                           node->requested_membership_count, encoded, sizeof(encoded));
        node->joint_config_index = node_append_internal_command(node, "cluster.enter_joint",
                                                                 "members", encoded);
        if (node->joint_config_index > 0)
            node->membership_state = RAFT_MEMBERSHIP_JOINT_PENDING;
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
        node_trace_user(node, RAFT_TRACE_MEMBERSHIP_CATCHUP_READY,
                        (uint16_t)node_last_log_index(node));
#endif
        encode_member_list((const char (*)[RAFT_MAX_ID])node->requested_membership,
                           node->requested_membership_count, encoded, sizeof(encoded));
        node->final_config_index = node_append_internal_command(node, "cluster.leave_joint",
                                                                 "members", encoded);
        if (node->final_config_index > 0)
            node->membership_state = RAFT_MEMBERSHIP_FINALIZING;
        return;
    }

    if (node->membership_state == RAFT_MEMBERSHIP_FINALIZING &&
        !configuration_is_joint(&node->config_state) &&
        node->config_state.index == node->final_config_index) {
        node->membership_state = RAFT_MEMBERSHIP_STABLE;
    }
}

/* ──────────────────────────────────────────────────────────────────────────
   rxnet phase callbacks
   ────────────────────────────────────────────────────────────────────────── */
static void raft_node_latch_inputs(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t messages[RAFT_MAX_QUEUE];
    raft_command_t commands[RAFT_MAX_QUEUE];
    size_t i, nmsg, ncmd;
    (void)ctx;

    if (node->cluster && node->cluster->realtime_clock) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        node->cluster->clock_ms = ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
    }

    if (!node->running) {
        node->pending_append_count      = 0;
        node->pending_vote_request_count = 0;
        node->pending_vote_count        = 0;
        node->outbox_count              = 0;
        return;
    }

    node->pending_append_count      = 0;
    node->pending_vote_request_count = 0;
    node->pending_vote_count        = 0;

    nmsg = node->transport.recv(node->transport.ctx, messages, RAFT_MAX_QUEUE);
    for (i = 0; i < nmsg; ++i) {
        raft_message_t *m = &messages[i];
        switch (m->kind) {
        case RAFT_MSG_APPEND_ENTRIES:
            if (node->pending_append_count < RAFT_MAX_QUEUE)
                node->pending_append_entries[node->pending_append_count++] = *m;
            break;
        case RAFT_MSG_REQUEST_VOTE:
            if (node->pending_vote_request_count < RAFT_MAX_QUEUE)
                node->pending_vote_requests[node->pending_vote_request_count++] = *m;
            break;
        case RAFT_MSG_REQUEST_VOTE_RESPONSE:
            if (node->pending_vote_count < RAFT_MAX_QUEUE)
                node->pending_votes[node->pending_vote_count++] = *m;
            break;
        case RAFT_MSG_APPEND_ENTRIES_RESPONSE:
            node_handle_append_entries_response(node, m);
            break;
        }
    }

    if (node_is_leader(node)) {
        ncmd = node->transport.recv_commands(node->transport.ctx, commands, RAFT_MAX_QUEUE);
        for (i = 0; i < ncmd && node->log_count < RAFT_MAX_LOG; ++i) {
            raft_log_entry_t *entry = &node->log[node->log_count];
            memset(entry, 0, sizeof(*entry));
            entry->index = node_last_log_index(node) + 1;
            entry->term  = node->current_term;
            strncpy(entry->leader_id, node->config.node_id, RAFT_MAX_ID - 1);
            entry->command = commands[i];
            node->log_count++;
            node->persist_dirty = 1;
            node_broadcast_append_entries(node);
        }
    } else {
        node->transport.recv_commands(node->transport.ctx, commands, RAFT_MAX_QUEUE);
    }
}

static void raft_node_dump_outputs(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    size_t i;
    (void)ctx;

    if (!node->running) { node->outbox_count = 0; return; }

    /*
     * Advance the leader's commit index proactively on every tick.
     * This covers single-node clusters (no peer responses ever arrive) and
     * reduces commit latency when the quorum was already met before the
     * current tick's AppendEntries responses are processed.
     */
    if (node_is_leader(node))
        node_advance_commit_index(node);

    /* Apply committed entries */
    while (node->last_applied < node->commit_index) {
        node->last_applied++;
        node_apply_command(node, &node->log[node_physical_index(node, node->last_applied)].command);
        node->persist_dirty = 1;
#ifdef RX_TRACE_ENABLE
        node_trace_user(node, RAFT_TRACE_APPLY, (uint16_t)node->last_applied);
#endif
    }

    node_maybe_drive_membership(node);

    /* Compact log if threshold reached */
    if (node_should_compact(node)) node_do_compact(node);

    if (node->persist_dirty) {
        raft_storage_node_save(node);
        node->persist_dirty = 0;
    }

    for (i = 0; i < node->outbox_count; ++i)
        node->transport.send(node->transport.ctx, &node->outbox[i]);
    node->outbox_count = 0;
}

/* ──────────────────────────────────────────────────────────────────────────
   Guards
   ────────────────────────────────────────────────────────────────────────── */
static int guard_timeout_expired(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return node->running && node_self_is_member(node) &&
           *node->clock_ms >= node->election_deadline_ms;
}

static int guard_has_append_entries(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    return ((raft_node_t *)user)->pending_append_count > 0;
}

static int guard_has_vote_request(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    return ((raft_node_t *)user)->pending_vote_request_count > 0;
}

static int guard_has_higher_term_vote_request(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return node->pending_vote_request_count > 0 &&
           node->pending_vote_requests[0].term > node->current_term;
}

static int guard_has_vote(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    return ((raft_node_t *)user)->pending_vote_count > 0;
}

static int guard_received_majority_votes(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    if (!node_has_vote_quorum(node, node->config_state.old_members, node->config_state.old_count))
        return 0;
    if (!configuration_is_joint(&node->config_state)) return 1;
    return node_has_vote_quorum(node, node->config_state.new_members, node->config_state.new_count);
}

static int guard_time_for_heartbeat(const rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    (void)ctx;
    return *node->clock_ms >= node->heartbeat_deadline_ms;
}

/* ──────────────────────────────────────────────────────────────────────────
   Actions
   ────────────────────────────────────────────────────────────────────────── */
static void action_become_candidate(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    size_t i;
    (void)ctx;
    node->current_term++;
    strncpy(node->voted_for, node->config.node_id, RAFT_MAX_ID - 1);
    node->vote_count = 0;
    node_record_vote(node, node->config.node_id);
    node->leader_id[0] = '\0';
    node->persist_dirty = 1;
    node->election_deadline_ms = *node->clock_ms + node_next_election_timeout(node);
    for (i = 0; i < node->config.peer_count; ++i) {
        memset(&message, 0, sizeof(message));
        message.kind          = RAFT_MSG_REQUEST_VOTE;
        message.term          = node->current_term;
        message.last_log_index = node_last_log_index(node);
        message.last_log_term  = node_last_log_term(node);
        strncpy(message.source, node->config.node_id, RAFT_MAX_ID - 1);
        strncpy(message.target, node->config.peers[i], RAFT_MAX_ID - 1);
        node_queue_message(node, &message);
    }
}

static void action_handle_append_entries(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    (void)ctx;
    if (node->pending_append_count == 0) return;
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
    if (node->pending_vote_request_count == 0) return;
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
    node->election_deadline_ms = *node->clock_ms + node_next_election_timeout(node);
}

static void action_become_leader(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    size_t i;
    int log_base = node_log_base_index(node);
    (void)ctx;
    strncpy(node->leader_id, node->config.node_id, RAFT_MAX_ID - 1);
    for (i = 0; i < node->config.peer_count; ++i) {
        node->next_index[i]  = node_last_log_index(node) + 1;
        node->match_index[i] = 0;
        /* Avoid slow one-by-one backoff for nodes that haven't caught up */
        if (node->next_index[i] < log_base) node->next_index[i] = log_base;
    }
    node_broadcast_append_entries(node);
    node->heartbeat_deadline_ms = *node->clock_ms + node->config.heartbeat_interval_ms;
}

static void action_handle_vote(rx_fsm_context *ctx, void *user) {
    raft_node_t *node = (raft_node_t *)user;
    raft_message_t message;
    (void)ctx;
    if (node->pending_vote_count == 0) return;
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

/* ──────────────────────────────────────────────────────────────────────────
   Node initialisation
   ────────────────────────────────────────────────────────────────────────── */
static void raft_node_init(raft_node_t *node, const raft_node_config_t *config,
                           long *clock_ms, raft_transport_t transport,
                           const char *root_dir, const raft_application_t *application,
                           raft_cluster_t *cluster) {
    char initial_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t initial_count = 0;

    memset(node, 0, sizeof(*node));
    node->cluster   = cluster;
    node->config    = *config;
    node->clock_ms   = clock_ms;
    node->transport  = transport;
    node->running    = 1;
    node->compaction_threshold = RAFT_MAX_LOG;

    if (application) node->application = *application;

    raft_file_storage_init(&node->storage, root_dir, config->node_id);
    raft_storage_node_load(node); /* populates term/log/config/snapshot; calls reload or restore */

    if (node->compaction_threshold <= 0) node->compaction_threshold = RAFT_MAX_LOG;

    /*
     * Learner (--join): discard any stale persisted state from a previous run.
     * The node will receive all log entries from the leader and rebuild its
     * cluster config from AppendEntries.  This covers the common case where
     * the data directory has state left over from a previous (possibly solo)
     * cluster run.
     */
    if (config->learner) {
        memset(&node->config_state, 0, sizeof(node->config_state));
        node->log_count                    = 0;
        node->commit_index                 = 0;
        node->last_applied                 = 0;
        node->snapshot_last_included_index = 0;
        node->snapshot_last_included_term  = 0;
        node->current_term                 = 0;
        node->voted_for[0]                 = '\0';
        node->persist_dirty                = 1;  /* overwrite stale files on next save */
    }

    /* Set stable configuration from storage or from initial config.
     * Skip when log_count > 0: the node was previously a learner whose config
     * was zeroed on disk; replaying the log from old_count=0 correctly rebuilds
     * membership through the enter/leave_joint entries.  Applying a synthetic
     * config before replay causes leave_joint to wrongly mark the node expelled.
     */
    if (node->config_state.old_count == 0 && !config->learner && node->log_count == 0) {
        if (config->initial_member_count > 0) {
            configuration_set_stable(&node->config_state,
                                     config->initial_members, config->initial_member_count, 0);
        } else {
            strncpy(initial_members[initial_count++], config->node_id, RAFT_MAX_ID - 1);
            for (size_t i = 0; i < config->peer_count && initial_count < RAFT_MAX_NODES; ++i)
                strncpy(initial_members[initial_count++], config->peers[i], RAFT_MAX_ID - 1);
            configuration_set_stable(&node->config_state, initial_members, initial_count, 0);
        }
    }

    node->membership_state = configuration_is_joint(&node->config_state)
                             ? RAFT_MEMBERSHIP_JOINT : RAFT_MEMBERSHIP_STABLE;
    node_refresh_peers(node);

    /* Restarting node: use 2x election timeout as initial deadline so an
     * existing leader has time to send heartbeats before a new election
     * fires and disrupts the cluster. */
    node->election_deadline_ms  = *clock_ms + node_next_election_timeout(node)
                                  + (node->current_term > 0
                                     ? 2 * config->election_timeout_ms : 0);
    node->heartbeat_deadline_ms = *clock_ms + config->heartbeat_interval_ms;

    rx_fsm_machine_init(&node->machine, config->node_id, RAFT_ROLE_FOLLOWER,
                        RAFT_TRANSITIONS, sizeof(RAFT_TRANSITIONS) / sizeof(RAFT_TRANSITIONS[0]),
                        node, raft_node_latch_inputs, raft_node_dump_outputs);
}

/* ──────────────────────────────────────────────────────────────────────────
   Cluster API
   ────────────────────────────────────────────────────────────────────────── */
int raft_cluster_init(raft_cluster_t *cluster, rx_fsm_runtime *runtime) {
    memset(cluster, 0, sizeof(*cluster));
    cluster->runtime = runtime;
    raft_memory_transport_init(&cluster->transport);
    return 0;
}

void raft_cluster_destroy(raft_cluster_t *cluster) {
    (void)cluster;
}

void raft_cluster_enable_realtime_clock(raft_cluster_t *cluster) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    cluster->clock_ms = ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
    cluster->realtime_clock = 1;
}

#ifdef RX_TRACE_ENABLE
int raft_cluster_attach_trace(raft_cluster_t *cluster, rx_trace_buf_t *trace) {
    if (!cluster || !trace) return -1;
    cluster->trace = trace;
    cluster_register_trace_labels(cluster);
    if (rx_trace_attach_runtime(trace, &cluster->runtime->runtime) != 0) return -1;
    for (size_t i = 0; i < cluster->node_count; ++i)
        node_register_trace_metadata(cluster, &cluster->nodes[i]);
    return 0;
}
#endif

raft_node_t *raft_cluster_add_node(raft_cluster_t *cluster, const raft_node_config_t *config,
                                   const char *root_dir, const raft_application_t *application,
                                   long period_us) {
    raft_node_t *node;
    if (cluster->node_count >= RAFT_MAX_NODES) return NULL;
    node = &cluster->nodes[cluster->node_count++];
    raft_memory_transport_register_node(&cluster->transport, config->node_id);
    raft_node_init(node, config, &cluster->clock_ms, raft_mem_transport_make(node),
                   root_dir, application, cluster);
    if (rx_fsm_runtime_add_machine(cluster->runtime, &node->machine, period_us, 0) != 0) return NULL;
#ifdef RX_TRACE_ENABLE
    if (cluster->trace) {
        cluster_register_trace_labels(cluster);
        if (rx_trace_attach_runtime(cluster->trace, &cluster->runtime->runtime) != 0) return NULL;
        node_register_trace_metadata(cluster, node);
    }
#endif
    return node;
}

int raft_cluster_tick(raft_cluster_t *cluster, int advance_ms) {
    cluster->clock_ms += advance_ms;
    return rx_fsm_tick(cluster->runtime);
}

raft_node_t *raft_cluster_leader(raft_cluster_t *cluster) {
    size_t i;
    for (i = 0; i < cluster->node_count; ++i)
        if (node_is_leader(&cluster->nodes[i])) return &cluster->nodes[i];
    return NULL;
}

/* ──────────────────────────────────────────────────────────────────────────
   Node API
   ────────────────────────────────────────────────────────────────────────── */
int raft_node_submit_command(raft_node_t *node, const raft_command_t *command) {
    if (!node->running || !node_self_is_member(node)) return -1;
    return node->transport.submit_command(node->transport.ctx, command);
}

void raft_node_stop(raft_node_t *node) {
    node->running = 0;
    if (node->transport.set_enabled)
        node->transport.set_enabled(node->transport.ctx, 0);
    node->outbox_count              = 0;
    node->pending_append_count      = 0;
    node->pending_vote_request_count = 0;
    node->pending_vote_count        = 0;
    node->machine.state             = RAFT_ROLE_FOLLOWER;
    /* Keep leader_id so status shows the last known leader right after restart.
     * It is corrected on the first heartbeat received after start(). */
}

void raft_node_start(raft_node_t *node) {
    if (node->transport.set_enabled)
        node->transport.set_enabled(node->transport.ctx, 1);
    raft_storage_node_load(node);
    /* Mirror the raft_node_init fallback, with the same log_count guard. */
    if (node->config_state.old_count == 0 && !node->config.learner && node->log_count == 0) {
        if (node->config.initial_member_count > 0) {
            configuration_set_stable(&node->config_state,
                                     node->config.initial_members,
                                     node->config.initial_member_count, 0);
        } else {
            char initial[RAFT_MAX_NODES][RAFT_MAX_ID];
            size_t count = 0, i;
            strncpy(initial[count++], node->config.node_id, RAFT_MAX_ID - 1);
            for (i = 0; i < node->config.peer_count && count < RAFT_MAX_NODES; ++i)
                strncpy(initial[count++], node->config.peers[i], RAFT_MAX_ID - 1);
            configuration_set_stable(&node->config_state, initial, count, 0);
        }
        node_refresh_peers(node);
    }
    node->running       = 1;
    node->machine.state = RAFT_ROLE_FOLLOWER;
    node->election_deadline_ms = *node->clock_ms + node_next_election_timeout(node)
                                 + 2 * node->config.election_timeout_ms;
}

void raft_node_reset_for_join(raft_node_t *node) {
    memset(&node->config_state, 0, sizeof(node->config_state));
    memset(node->log, 0, sizeof(node->log));
    node->log_count                    = 0;
    node->commit_index                 = 0;
    node->last_applied                 = 0;
    node->snapshot_last_included_index = 0;
    node->snapshot_last_included_term  = 0;
    node->current_term                 = 0;
    node->voted_for[0]                 = '\0';
    node->leader_id[0]                 = '\0';
    node->vote_count                   = 0;
    node->pending_append_count         = 0;
    node->pending_vote_request_count   = 0;
    node->pending_vote_count           = 0;
    node->outbox_count                 = 0;
    node->config.learner               = 1;
    node->membership_state             = RAFT_MEMBERSHIP_STABLE;
    node_refresh_peers(node);
    node->running       = 1;
    node->machine.state = RAFT_ROLE_FOLLOWER;
    node->election_deadline_ms = *node->clock_ms + node_next_election_timeout(node)
                                 + 2 * node->config.election_timeout_ms;
    node->heartbeat_deadline_ms = *node->clock_ms + node->config.heartbeat_interval_ms;
    node->persist_dirty = 1;
    raft_storage_node_save(node);
    node->persist_dirty = 0;
}

int raft_node_request_membership_change(raft_node_t *node,
                                         const char members[][RAFT_MAX_ID], size_t member_count) {
    if (!node_is_leader(node)) return -1;
    node->requested_membership_count =
        normalize_members(members, member_count, node->requested_membership, RAFT_MAX_NODES);
    if (node->requested_membership_count == 0) return -1;
    node->requested_membership_pending = 1;
#ifdef RX_TRACE_ENABLE
    node_trace_user(node, RAFT_TRACE_MEMBERSHIP_REQUESTED,
                    (uint16_t)node->requested_membership_count);
#endif
    return 0;
}
