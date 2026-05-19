// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "raft/raft_mqtt_transport.h"

#include <string.h>
#include <stdio.h>

/* ── vtable functions ─────────────────────────────────────────────────────── */

static int mqtt_send(void *ctx, const raft_message_t *msg) {
    raft_mqtt_transport_t *mqtt = (raft_mqtt_transport_t *)ctx;
    if (!mqtt->enabled) return 0;

    char topic[128];
    snprintf(topic, sizeof(topic), "raft/%s/%s",
             mqtt->config.cluster_id, msg->target);
    return mqtt->hal.publish(mqtt->hal.ctx, topic,
                             msg, sizeof(raft_message_t), mqtt->config.qos);
}

static size_t mqtt_recv(void *ctx, raft_message_t *out, size_t capacity) {
    raft_mqtt_transport_t *mqtt = (raft_mqtt_transport_t *)ctx;
    char    topic[128];
    uint8_t payload[sizeof(raft_message_t)];
    size_t  payload_len;
    size_t  count = 0;

    mqtt->hal.poll(mqtt->hal.ctx);

    while (count < capacity) {
        if (!mqtt->hal.recv_raw(mqtt->hal.ctx,
                                topic, sizeof(topic),
                                payload, sizeof(payload), &payload_len))
            break;

        /* Route: inbox topic → raft message */
        if (payload_len == sizeof(raft_message_t) &&
            strcmp(topic, mqtt->inbox_topic) == 0) {
            if (mqtt->enabled)
                memcpy(&out[count++], payload, sizeof(raft_message_t));
            continue;
        }

        /* Route: join topic → join queue */
        if (mqtt->join_topic[0] &&
            strcmp(topic, mqtt->join_topic) == 0 &&
            payload_len > 0 && payload_len < RAFT_MAX_ID &&
            mqtt->join_count < RAFT_MAX_NODES) {
            memcpy(mqtt->join_queue[mqtt->join_count], payload, payload_len);
            mqtt->join_queue[mqtt->join_count][RAFT_MAX_ID - 1] = '\0';
            mqtt->join_count++;
        }
        /* Unknown topics are discarded. */
    }
    return count;
}

static int mqtt_submit_command(void *ctx, const raft_command_t *cmd) {
    raft_mqtt_transport_t *mqtt = (raft_mqtt_transport_t *)ctx;
    if (mqtt->cmd_count >= RAFT_MAX_QUEUE) return -1;
    mqtt->cmd_queue[mqtt->cmd_count++] = *cmd;
    return 0;
}

static size_t mqtt_recv_commands(void *ctx, raft_command_t *out, size_t capacity) {
    raft_mqtt_transport_t *mqtt = (raft_mqtt_transport_t *)ctx;
    size_t count = mqtt->cmd_count < capacity ? mqtt->cmd_count : capacity;
    size_t i;
    for (i = 0; i < count; ++i) out[i] = mqtt->cmd_queue[i];
    mqtt->cmd_count = 0;
    return count;
}

static void mqtt_set_enabled(void *ctx, int enabled) {
    raft_mqtt_transport_t *mqtt = (raft_mqtt_transport_t *)ctx;
    mqtt->enabled = enabled;
    if (!enabled) {
        /* Drain and discard the HAL queue */
        char    topic[128];
        uint8_t payload[sizeof(raft_message_t)];
        size_t  pl;
        while (mqtt->hal.recv_raw(mqtt->hal.ctx,
                                  topic, sizeof(topic),
                                  payload, sizeof(payload), &pl))
            ; /* discard */
        mqtt->cmd_count = 0;
    }
}

/* ── Public API ──────────────────────────────────────────────────────────── */

int raft_mqtt_transport_init(raft_mqtt_transport_t    *mqtt,
                              const raft_mqtt_config_t *cfg,
                              const raft_mqtt_hal_t    *hal) {
    memset(mqtt, 0, sizeof(*mqtt));
    mqtt->config  = *cfg;
    mqtt->hal     = *hal;
    mqtt->enabled = 1;
    snprintf(mqtt->inbox_topic, sizeof(mqtt->inbox_topic),
             "raft/%s/%s", cfg->cluster_id, cfg->node_id);
    return 0;
}

int raft_mqtt_transport_start(raft_mqtt_transport_t *mqtt) {
    if (mqtt->started) return 0;
    if (mqtt->hal.connect(mqtt->hal.ctx, &mqtt->config) != 0) return -1;
    if (mqtt->hal.subscribe(mqtt->hal.ctx, mqtt->inbox_topic,
                            mqtt->config.qos) != 0) {
        mqtt->hal.disconnect(mqtt->hal.ctx);
        return -1;
    }
    mqtt->started = 1;
    return 0;
}

void raft_mqtt_transport_stop(raft_mqtt_transport_t *mqtt) {
    if (!mqtt->started) return;
    mqtt->hal.disconnect(mqtt->hal.ctx);
    mqtt->started = 0;
}

raft_transport_t raft_mqtt_transport_make(raft_mqtt_transport_t *mqtt) {
    raft_transport_t t;
    t.send           = mqtt_send;
    t.recv           = mqtt_recv;
    t.submit_command = mqtt_submit_command;
    t.recv_commands  = mqtt_recv_commands;
    t.set_enabled    = mqtt_set_enabled;
    t.ctx            = mqtt;
    return t;
}
