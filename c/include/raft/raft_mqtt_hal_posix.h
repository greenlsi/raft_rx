// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#if !defined(ESP_PLATFORM)

#include "raft/raft_mqtt_hal.h"
#include "core_mqtt.h"
#include <openssl/ssl.h>
#include <pthread.h>
#include <stdint.h>

#define RAFT_MQTT_POSIX_BUF_SIZE   4096
#define RAFT_MQTT_POSIX_QUEUE_SIZE RAFT_MAX_QUEUE

struct NetworkContext {
    SSL *ssl;
    int  fd;
};

typedef struct {
    MQTTContext_t        mqtt;
    struct NetworkContext net;
    uint8_t              mqtt_buf[RAFT_MQTT_POSIX_BUF_SIZE];
    struct {
        char    topic[128];
        uint8_t payload[sizeof(raft_message_t) + 4];
        size_t  payload_len;
    } ring[RAFT_MQTT_POSIX_QUEUE_SIZE];
    size_t               r_head, r_tail;
    pthread_mutex_t      lock;
    pthread_t            thread;
    volatile int         shutdown;
    int                  thread_started;
} raft_mqtt_hal_posix_ctx_t;

/*
 * Zero-initialise ctx and return a raft_mqtt_hal_t wired to it.
 * ctx must remain valid for the lifetime of the transport.
 */
raft_mqtt_hal_t raft_mqtt_hal_posix_make(raft_mqtt_hal_posix_ctx_t *ctx);

#endif /* !ESP_PLATFORM */
