#include "raft/raft.h"

#include <string.h>

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
