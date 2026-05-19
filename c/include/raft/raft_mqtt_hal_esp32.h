// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#if defined(ESP_PLATFORM)

#include "raft/raft_mqtt_hal.h"
#include "mqtt_client.h"     /* ESP-IDF 6.x */
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"

#define RAFT_MQTT_ESP32_QUEUE_SIZE RAFT_MAX_QUEUE

typedef struct {
    char    topic[128];
    uint8_t payload[sizeof(raft_message_t) + 4];
    size_t  payload_len;
} raft_mqtt_esp32_msg_t;

typedef struct {
    esp_mqtt_client_handle_t client;
    QueueHandle_t            queue;
    raft_mqtt_esp32_msg_t    pool[RAFT_MQTT_ESP32_QUEUE_SIZE];
    int                      connected;
} raft_mqtt_hal_esp32_ctx_t;

raft_mqtt_hal_t raft_mqtt_hal_esp32_make(raft_mqtt_hal_esp32_ctx_t *ctx);

#endif /* ESP_PLATFORM */
