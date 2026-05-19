// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#if defined(ESP_PLATFORM)

#include "raft/raft_mqtt_hal_esp32.h"
#include <string.h>

static void esp_mqtt_event_handler(void *arg, esp_event_base_t base,
                                    int32_t event_id, void *event_data) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;
    esp_mqtt_event_handle_t event  = (esp_mqtt_event_handle_t)event_data;
    (void)base;

    switch (event_id) {
    case MQTT_EVENT_CONNECTED:
        ctx->connected = 1;
        break;
    case MQTT_EVENT_DISCONNECTED:
        ctx->connected = 0;
        break;
    case MQTT_EVENT_DATA: {
        raft_mqtt_esp32_msg_t msg;
        size_t tl = (size_t)event->topic_len < 127
                    ? (size_t)event->topic_len : 127;
        memcpy(msg.topic, event->topic, tl);
        msg.topic[tl] = '\0';
        size_t pl = (size_t)event->data_len;
        if (pl > sizeof(msg.payload)) pl = sizeof(msg.payload);
        memcpy(msg.payload, event->data, pl);
        msg.payload_len = pl;
        xQueueSend(ctx->queue, &msg, 0);
        break;
    }
    default:
        break;
    }
}

static int esp_hal_connect(void *arg, const raft_mqtt_config_t *cfg) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;

    ctx->queue = xQueueCreate(RAFT_MQTT_ESP32_QUEUE_SIZE,
                               sizeof(raft_mqtt_esp32_msg_t));
    if (!ctx->queue) return -1;

    esp_mqtt_client_config_t mcfg = {0};
    mcfg.broker.address.hostname  = cfg->endpoint;
    mcfg.broker.address.port      = (uint32_t)cfg->port;
    mcfg.broker.address.transport = MQTT_TRANSPORT_OVER_SSL;
    mcfg.broker.verification.certificate        = cfg->ca_cert;
    mcfg.credentials.authentication.certificate = cfg->client_cert;
    mcfg.credentials.authentication.key         = cfg->client_key;
    mcfg.credentials.client_id                  = cfg->node_id;
    mcfg.session.keepalive = cfg->keepalive_s > 0 ? cfg->keepalive_s : 60;

    ctx->client = esp_mqtt_client_init(&mcfg);
    if (!ctx->client) return -1;

    esp_mqtt_client_register_event(ctx->client, ESP_EVENT_ANY_ID,
                                    esp_mqtt_event_handler, ctx);
    esp_mqtt_client_start(ctx->client);

    /* Simple busy-wait for CONNECTED; real app should use an event group */
    int retries = 100;
    while (!ctx->connected && retries-- > 0)
        vTaskDelay(pdMS_TO_TICKS(100));
    return ctx->connected ? 0 : -1;
}

static int esp_hal_publish(void *arg, const char *topic,
                            const void *payload, size_t len, int qos) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;
    int msg_id = esp_mqtt_client_publish(ctx->client, topic,
                                          (const char *)payload,
                                          (int)len, qos, 0);
    return (msg_id >= 0) ? 0 : -1;
}

static int esp_hal_subscribe(void *arg, const char *topic, int qos) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;
    int msg_id = esp_mqtt_client_subscribe(ctx->client, topic, qos);
    return (msg_id >= 0) ? 0 : -1;
}

static void esp_hal_poll(void *arg) { (void)arg; /* event-driven */ }

static int esp_hal_recv_raw(void *arg,
                             char *topic_out, size_t topic_max,
                             void *payload_out, size_t payload_max,
                             size_t *payload_len_out) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;
    raft_mqtt_esp32_msg_t msg;
    if (xQueueReceive(ctx->queue, &msg, 0) != pdTRUE) return 0;
    strncpy(topic_out, msg.topic, topic_max - 1);
    topic_out[topic_max - 1] = '\0';
    size_t pl = msg.payload_len < payload_max ? msg.payload_len : payload_max;
    memcpy(payload_out, msg.payload, pl);
    *payload_len_out = pl;
    return 1;
}

static void esp_hal_disconnect(void *arg) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;
    if (ctx->client) {
        esp_mqtt_client_stop(ctx->client);
        esp_mqtt_client_destroy(ctx->client);
        ctx->client = NULL;
    }
    if (ctx->queue) {
        vQueueDelete(ctx->queue);
        ctx->queue = NULL;
    }
    ctx->connected = 0;
}

raft_mqtt_hal_t raft_mqtt_hal_esp32_make(raft_mqtt_hal_esp32_ctx_t *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    raft_mqtt_hal_t h;
    h.connect    = esp_hal_connect;
    h.publish    = esp_hal_publish;
    h.subscribe  = esp_hal_subscribe;
    h.poll       = esp_hal_poll;
    h.recv_raw   = esp_hal_recv_raw;
    h.disconnect = esp_hal_disconnect;
    h.ctx        = ctx;
    return h;
}

#endif /* ESP_PLATFORM */
