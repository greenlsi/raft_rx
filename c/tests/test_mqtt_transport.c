// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "raft/raft_mqtt_transport.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

/* ── Mock HAL ─────────────────────────────────────────────────────────────── */

#define MOCK_Q 64

typedef struct {
    struct {
        char    topic[128];
        uint8_t payload[sizeof(raft_message_t)];
        size_t  payload_len;
        int     qos;
    } published[MOCK_Q];
    size_t pub_count;

    struct {
        char    topic[128];
        uint8_t payload[sizeof(raft_message_t)];
        size_t  payload_len;
    } injected[MOCK_Q];
    size_t inj_head, inj_tail;

    char   subscribed[8][128];
    size_t sub_count;
    int    connected;
} mock_ctx_t;

static int mock_connect(void *ctx, const raft_mqtt_config_t *cfg) {
    (void)cfg;
    ((mock_ctx_t *)ctx)->connected = 1;
    return 0;
}
static int mock_publish(void *ctx, const char *topic,
                        const void *payload, size_t len, int qos) {
    mock_ctx_t *m = ctx;
    if (m->pub_count >= MOCK_Q) return -1;
    strncpy(m->published[m->pub_count].topic, topic, 127);
    memcpy(m->published[m->pub_count].payload, payload, len);
    m->published[m->pub_count].payload_len = len;
    m->published[m->pub_count].qos = qos;
    m->pub_count++;
    return 0;
}
static int mock_subscribe(void *ctx, const char *topic, int qos) {
    mock_ctx_t *m = ctx;
    (void)qos;
    if (m->sub_count < 8)
        strncpy(m->subscribed[m->sub_count++], topic, 127);
    return 0;
}
static void mock_poll(void *ctx) { (void)ctx; }
static int mock_recv_raw(void *ctx,
                         char *topic_out, size_t topic_max,
                         void *payload_out, size_t payload_max,
                         size_t *payload_len_out) {
    mock_ctx_t *m = ctx;
    if (m->inj_head == m->inj_tail) return 0;
    size_t idx = m->inj_head % MOCK_Q;
    strncpy(topic_out, m->injected[idx].topic, topic_max - 1);
    topic_out[topic_max - 1] = '\0';
    size_t len = m->injected[idx].payload_len;
    if (len > payload_max) len = payload_max;
    memcpy(payload_out, m->injected[idx].payload, len);
    *payload_len_out = len;
    m->inj_head++;
    return 1;
}
static void mock_disconnect(void *ctx) { ((mock_ctx_t *)ctx)->connected = 0; }

static raft_mqtt_hal_t mock_hal_make(mock_ctx_t *m) {
    raft_mqtt_hal_t h;
    h.connect    = mock_connect;
    h.publish    = mock_publish;
    h.subscribe  = mock_subscribe;
    h.poll       = mock_poll;
    h.recv_raw   = mock_recv_raw;
    h.disconnect = mock_disconnect;
    h.ctx        = m;
    return h;
}

static void mock_inject(mock_ctx_t *m, const char *topic,
                        const void *payload, size_t len) {
    size_t idx = m->inj_tail % MOCK_Q;
    strncpy(m->injected[idx].topic, topic, 127);
    memcpy(m->injected[idx].payload, payload, len);
    m->injected[idx].payload_len = len;
    m->inj_tail++;
}

/* ── Helpers ─────────────────────────────────────────────────────────────── */

static raft_mqtt_config_t make_cfg(const char *cluster, const char *node) {
    raft_mqtt_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.cluster_id, cluster, 63);
    strncpy(cfg.node_id,    node,    RAFT_MAX_ID - 1);
    strncpy(cfg.endpoint,   "test.example.com", 255);
    cfg.port        = 8883;
    cfg.ca_cert     = "CA";
    cfg.client_cert = "CERT";
    cfg.client_key  = "KEY";
    cfg.qos         = 1;
    cfg.keepalive_s = 60;
    return cfg;
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

static void test_init_subscribes_to_inbox(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    assert(m.connected == 1);
    assert(m.sub_count == 1);
    assert(strcmp(m.subscribed[0], "raft/mycluster/n1") == 0);
    printf("PASS test_init_subscribes_to_inbox\n");
}

static void test_send_publishes_to_peer_topic(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_transport_t t = raft_mqtt_transport_make(&mqtt);

    raft_message_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.kind = RAFT_MSG_REQUEST_VOTE;
    strncpy(msg.source, "n1", RAFT_MAX_ID - 1);
    strncpy(msg.target, "n2", RAFT_MAX_ID - 1);

    assert(t.send(t.ctx, &msg) == 0);
    assert(m.pub_count == 1);
    assert(strcmp(m.published[0].topic, "raft/mycluster/n2") == 0);
    assert(m.published[0].payload_len == sizeof(raft_message_t));
    printf("PASS test_send_publishes_to_peer_topic\n");
}

static void test_recv_deserialises_raft_message(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_transport_t t = raft_mqtt_transport_make(&mqtt);

    raft_message_t in_msg;
    memset(&in_msg, 0, sizeof(in_msg));
    in_msg.kind = RAFT_MSG_APPEND_ENTRIES;
    strncpy(in_msg.source, "n2", RAFT_MAX_ID - 1);
    strncpy(in_msg.target, "n1", RAFT_MAX_ID - 1);
    mock_inject(&m, "raft/mycluster/n1", &in_msg, sizeof(in_msg));

    raft_message_t out[4];
    size_t count = t.recv(t.ctx, out, 4);
    assert(count == 1);
    assert(out[0].kind == RAFT_MSG_APPEND_ENTRIES);
    assert(strcmp(out[0].source, "n2") == 0);
    printf("PASS test_recv_deserialises_raft_message\n");
}

static void test_recv_ignores_wrong_size_payload(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_transport_t t = raft_mqtt_transport_make(&mqtt);

    uint8_t garbage[16] = {0};
    mock_inject(&m, "raft/mycluster/n1", garbage, sizeof(garbage));

    raft_message_t out[4];
    size_t count = t.recv(t.ctx, out, 4);
    assert(count == 0);
    printf("PASS test_recv_ignores_wrong_size_payload\n");
}

static void test_recv_ignores_unknown_topic(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_transport_t t = raft_mqtt_transport_make(&mqtt);

    raft_message_t msg; memset(&msg, 0, sizeof(msg));
    mock_inject(&m, "raft/mycluster/n99", &msg, sizeof(msg));

    raft_message_t out[4];
    size_t count = t.recv(t.ctx, out, 4);
    assert(count == 0);
    printf("PASS test_recv_ignores_unknown_topic\n");
}

static void test_submit_recv_commands_local(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_transport_t t = raft_mqtt_transport_make(&mqtt);

    raft_command_t cmd;
    memset(&cmd, 0, sizeof(cmd));
    strncpy(cmd.op, "set", 31);
    strncpy(cmd.key, "x", RAFT_MAX_KEY - 1);

    assert(t.submit_command(t.ctx, &cmd) == 0);
    assert(m.pub_count == 0);  /* no MQTT publish for local command */

    raft_command_t out[4];
    size_t count = t.recv_commands(t.ctx, out, 4);
    assert(count == 1);
    assert(strcmp(out[0].op, "set") == 0);
    printf("PASS test_submit_recv_commands_local\n");
}

static void test_send_disabled_no_publish(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_transport_t t = raft_mqtt_transport_make(&mqtt);
    t.set_enabled(t.ctx, 0);

    raft_message_t msg; memset(&msg, 0, sizeof(msg));
    strncpy(msg.target, "n2", RAFT_MAX_ID - 1);
    t.send(t.ctx, &msg);
    assert(m.pub_count == 0);
    printf("PASS test_send_disabled_no_publish\n");
}

int main(void) {
    test_init_subscribes_to_inbox();
    test_send_publishes_to_peer_topic();
    test_recv_deserialises_raft_message();
    test_recv_ignores_wrong_size_payload();
    test_recv_ignores_unknown_topic();
    test_submit_recv_commands_local();
    test_send_disabled_no_publish();
    printf("All MQTT transport tests passed.\n");
    return 0;
}
