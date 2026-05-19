// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include "raft/raft.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char        cluster_id[64];
    char        node_id[RAFT_MAX_ID];
    char        endpoint[256];
    int         port;
    const char *ca_cert;       /* PEM, NULL-terminated */
    const char *client_cert;   /* PEM, NULL-terminated */
    const char *client_key;    /* PEM, NULL-terminated */
    int         qos;           /* 0 or 1 */
    int         keepalive_s;
} raft_mqtt_config_t;

/*
 * HAL vtable — platform transport abstraction.
 *
 *  connect     Open TLS connection to cfg->endpoint:cfg->port, perform MQTT
 *              CONNECT. Returns 0 on success, -1 on error.
 *
 *  publish     Publish payload to topic with given QoS.
 *              Returns 0 on success, -1 on error.
 *
 *  subscribe   Subscribe to topic with given QoS.
 *              Returns 0 on success, -1 on error.
 *
 *  poll        Drive the MQTT receive loop for one iteration. No-op on
 *              implementations that use a background thread or callbacks.
 *
 *  recv_raw    Drain one message from the HAL's internal incoming queue.
 *              Writes topic and payload into caller-supplied buffers.
 *              Returns 1 if a message was available, 0 if the queue is empty.
 *              Thread-safe: safe to call from the application thread while the
 *              HAL processes incoming data in a background thread or callback.
 *
 *  disconnect  Perform MQTT DISCONNECT, close TLS connection, stop any
 *              background thread.
 */
typedef struct {
    int    (*connect)   (void *ctx, const raft_mqtt_config_t *cfg);
    int    (*publish)   (void *ctx, const char *topic,
                         const void *payload, size_t len, int qos);
    int    (*subscribe) (void *ctx, const char *topic, int qos);
    void   (*poll)      (void *ctx);
    int    (*recv_raw)  (void *ctx,
                         char *topic_out,   size_t topic_max,
                         void *payload_out, size_t payload_max,
                         size_t *payload_len_out);
    void   (*disconnect)(void *ctx);
    void  *ctx;
} raft_mqtt_hal_t;

#ifdef __cplusplus
}
#endif
