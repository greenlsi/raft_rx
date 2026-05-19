// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

/*
 * Optional dynamic peer discovery for the MQTT transport.
 * Linked only when raft_mqtt_transport_recv_joins() is referenced.
 */

#include "raft/raft_mqtt_transport.h"
#include <string.h>
#include <stdio.h>

size_t raft_mqtt_transport_recv_joins(raft_mqtt_transport_t *mqtt,
                                       raft_mqtt_join_t      *out,
                                       size_t                 capacity) {
    /* First call: subscribe to join topic and announce own presence. */
    if (!mqtt->join_topic[0]) {
        snprintf(mqtt->join_topic, sizeof(mqtt->join_topic),
                 "raft/%s/join", mqtt->config.cluster_id);
        mqtt->hal.subscribe(mqtt->hal.ctx, mqtt->join_topic, mqtt->config.qos);
        mqtt->hal.publish(mqtt->hal.ctx, mqtt->join_topic,
                          mqtt->config.node_id,
                          strlen(mqtt->config.node_id),
                          mqtt->config.qos);
    }

    /* Drain the join queue populated by recv(). */
    size_t count = mqtt->join_count < capacity ? mqtt->join_count : capacity;
    size_t i;
    for (i = 0; i < count; ++i)
        strncpy(out[i].node_id, mqtt->join_queue[i], RAFT_MAX_ID - 1);
    mqtt->join_count = 0;
    return count;
}
