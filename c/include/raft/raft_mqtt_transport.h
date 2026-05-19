// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "raft/raft.h"
#include "raft/raft_mqtt_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Payload of a join announcement (plain node_id string). */
typedef struct {
    char node_id[RAFT_MAX_ID];
} raft_mqtt_join_t;

typedef struct {
    raft_mqtt_hal_t    hal;
    raft_mqtt_config_t config;
    char               inbox_topic[128];  /* raft/<cluster>/<own_node_id>  */

    /* Join support — populated by raft_mqtt_join.c when first referenced.  */
    /* When join_topic[0] == '\0', join routing is inactive.                */
    char               join_topic[128];   /* raft/<cluster>/join           */
    char               join_queue[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t             join_count;

    raft_command_t     cmd_queue[RAFT_MAX_QUEUE];
    size_t             cmd_count;

    int                enabled;
    int                started;
} raft_mqtt_transport_t;

/*
 * Lifecycle
 *
 *  init    Zero-initialise, copy config, wire HAL, pre-compute inbox_topic.
 *          Does NOT connect to the broker.
 *
 *  start   Connect to the broker and subscribe to inbox_topic. Returns 0 on
 *          success, -1 on error.
 *
 *  stop    Unsubscribe and disconnect from the broker.
 *
 *  make    Return a raft_transport_t vtable backed by this mqtt state.
 *          Assign the result to node->transport.
 */
int              raft_mqtt_transport_init (raft_mqtt_transport_t    *mqtt,
                                           const raft_mqtt_config_t *cfg,
                                           const raft_mqtt_hal_t    *hal);
int              raft_mqtt_transport_start(raft_mqtt_transport_t *mqtt);
void             raft_mqtt_transport_stop (raft_mqtt_transport_t *mqtt);
raft_transport_t raft_mqtt_transport_make (raft_mqtt_transport_t *mqtt);

/*
 * Dynamic peer discovery — defined in raft_mqtt_join.c.
 * Linked only when this symbol is referenced.
 *
 * On the first call: subscribes to raft/<cluster>/join and publishes own
 * node_id there so existing nodes learn about this node.
 * Subsequent calls: drain the join announcement queue built by recv().
 */
size_t raft_mqtt_transport_recv_joins(raft_mqtt_transport_t *mqtt,
                                      raft_mqtt_join_t      *out,
                                      size_t                 capacity);

#ifdef __cplusplus
}
#endif
