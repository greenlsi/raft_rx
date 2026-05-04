#include "raft/raft.h"

#include <stddef.h>
#include <string.h>

/* ── Memory transport vtable adapter ─────────────────────────────────────── */

static int _mem_send(void *ctx, const raft_message_t *msg) {
    raft_node_t *n = (raft_node_t *)ctx;
    return raft_memory_transport_send(&n->cluster->transport, msg);
}

static size_t _mem_recv(void *ctx, raft_message_t *out, size_t capacity) {
    raft_node_t *n = (raft_node_t *)ctx;
    return raft_memory_transport_recv_for(&n->cluster->transport, n->config.node_id, out, capacity);
}

static int _mem_submit_command(void *ctx, const raft_command_t *cmd) {
    raft_node_t *n = (raft_node_t *)ctx;
    return raft_memory_transport_submit_client_command(&n->cluster->transport,
                                                       n->config.node_id, cmd);
}

static size_t _mem_recv_commands(void *ctx, raft_command_t *out, size_t capacity) {
    raft_node_t *n = (raft_node_t *)ctx;
    return raft_memory_transport_recv_client_commands(&n->cluster->transport,
                                                      n->config.node_id, out, capacity);
}

raft_transport_t raft_mem_transport_make(raft_node_t *node) {
    raft_transport_t t;
    t.send           = _mem_send;
    t.recv           = _mem_recv;
    t.submit_command = _mem_submit_command;
    t.recv_commands  = _mem_recv_commands;
    t.set_enabled    = NULL;
    t.ctx            = node;
    return t;
}

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
