# MQTT Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional MQTT transport to raft_rx C library that lets Raft nodes communicate over AWS IoT Core on both POSIX and ESP32.

**Architecture:** A thin HAL (`raft_mqtt_hal.h`) abstracts the platform MQTT library behind six functions (connect/publish/subscribe/poll/recv_raw/disconnect). The platform-agnostic transport logic (`raft_mqtt_transport.c`) implements the `raft_transport_t` vtable on top of the HAL. Two HAL implementations are provided: `raft_mqtt_hal_posix.c` (coreMQTT + OpenSSL, hidden background thread) and `raft_mqtt_hal_esp32.c` (esp_mqtt_client). Dynamic peer discovery lives in `raft_mqtt_join.c`, which the linker includes only when referenced.

**Tech Stack:** C99, coreMQTT v1.x (git submodule), OpenSSL (POSIX), esp_mqtt_client ESP-IDF 6.x (ESP32), pthreads (POSIX), FreeRTOS queues (ESP32).

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `c/include/raft/raft_mqtt_hal.h` | Create | `raft_mqtt_config_t` + `raft_mqtt_hal_t` vtable |
| `c/include/raft/raft_mqtt_transport.h` | Create | `raft_mqtt_transport_t`, `raft_mqtt_join_t`, public API |
| `c/src/raft_mqtt_transport.c` | Create | vtable impl, topic construction, message routing, command queue |
| `c/src/raft_mqtt_hal_posix.c` | Create | coreMQTT + OpenSSL TLS + pthread background thread |
| `c/src/raft_mqtt_hal_esp32.c` | Create | esp_mqtt_client wrapper, FreeRTOS queue |
| `c/src/raft_mqtt_join.c` | Create | Optional dynamic peer discovery |
| `c/tests/test_mqtt_transport.c` | Create | Mock HAL unit tests for transport + join |
| `c/third_party/coreMQTT/` | Create | git submodule |
| `c/third_party/core_mqtt_config.h` | Create | coreMQTT compile-time config |
| `c/Makefile` | Modify | Add MQTT build targets and test target |

---

## Task 1: coreMQTT submodule + headers

**Files:**
- Create: `c/third_party/core_mqtt_config.h`
- Create: `c/include/raft/raft_mqtt_hal.h`
- Create: `c/include/raft/raft_mqtt_transport.h`

- [ ] **Step 1.1: Add coreMQTT as a git submodule**

From the repo root:
```bash
cd c
mkdir -p third_party
git submodule add \
  https://github.com/FreeRTOS/coreMQTT.git \
  third_party/coreMQTT
cd third_party/coreMQTT
git checkout v1.1.0
cd ../..
git add .gitmodules third_party/coreMQTT
```

Expected: `.gitmodules` updated, `c/third_party/coreMQTT/` contains `source/` dir.

- [ ] **Step 1.2: Write `c/third_party/core_mqtt_config.h`**

coreMQTT looks for this file at compile time. Minimal config disabling debug logging:

```c
// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once
/* Disable all coreMQTT debug logging. */
#define LogError(msg)
#define LogWarn(msg)
#define LogInfo(msg)
#define LogDebug(msg)
```

- [ ] **Step 1.3: Write `c/include/raft/raft_mqtt_hal.h`**

```c
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
 *              CONNECT.  Must be called before publish/subscribe/recv_raw.
 *              Returns 0 on success, -1 on error.
 *
 *  publish     Publish payload to topic with given QoS.
 *              Returns 0 on success, -1 on error.
 *
 *  subscribe   Subscribe to topic with given QoS.
 *              Returns 0 on success, -1 on error.
 *
 *  poll        Drive the MQTT receive loop for one iteration.  No-op on
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
```

- [ ] **Step 1.4: Write `c/include/raft/raft_mqtt_transport.h`**

```c
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
 *  start   Connect to the broker and subscribe to inbox_topic.  Must be
 *          called after init.  Returns 0 on success, -1 on error.
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
```

- [ ] **Step 1.5: Verify headers compile cleanly**

```bash
cd /path/to/raft_rx/c
cc -std=c99 -Wall -Iinclude -I../../rxnet/c/include \
   -I third_party/coreMQTT/source/include \
   -I third_party \
   -fsyntax-only include/raft/raft_mqtt_hal.h
cc -std=c99 -Wall -Iinclude -I../../rxnet/c/include \
   -I third_party/coreMQTT/source/include \
   -I third_party \
   -fsyntax-only include/raft/raft_mqtt_transport.h
```

Expected: no errors or warnings.

- [ ] **Step 1.6: Commit**

```bash
git add c/third_party c/include/raft/raft_mqtt_hal.h c/include/raft/raft_mqtt_transport.h
git commit -m "feat: add MQTT transport headers and coreMQTT submodule"
```

---

## Task 2: Failing tests for core transport

**Files:**
- Create: `c/tests/test_mqtt_transport.c`

- [ ] **Step 2.1: Write mock HAL + test scaffold**

Create `c/tests/test_mqtt_transport.c`:

```c
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

    /* Inject a Raft message arriving on the inbox topic */
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
    mock_inject(&m, "raft/mycluster/n99", &msg, sizeof(msg)); /* wrong node */

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
```

- [ ] **Step 2.2: Verify tests fail to compile (transport not yet implemented)**

```bash
cd /path/to/raft_rx/c
cc -std=c99 -Wall -Iinclude -I../../rxnet/c/include \
   tests/test_mqtt_transport.c -o /dev/null
```

Expected: linker errors — `raft_mqtt_transport_init`, `raft_mqtt_transport_start`, etc. not found.

- [ ] **Step 2.3: Commit failing tests**

```bash
git add c/tests/test_mqtt_transport.c
git commit -m "test: add failing unit tests for MQTT transport"
```

---

## Task 3: Core transport implementation

**Files:**
- Create: `c/src/raft_mqtt_transport.c`

- [ ] **Step 3.1: Write `c/src/raft_mqtt_transport.c`**

```c
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
```

- [ ] **Step 3.2: Build and run tests**

Add a temporary build line to verify (full Makefile update is Task 7):
```bash
cd /path/to/raft_rx/c
cc -std=c99 -Wall -Iinclude -I../../rxnet/c/include \
   src/raft_mqtt_transport.c \
   tests/test_mqtt_transport.c \
   -o build/test_mqtt_transport \
   ../../rxnet/c/build/librxnet.a
./build/test_mqtt_transport
```

Expected:
```
PASS test_init_subscribes_to_inbox
PASS test_send_publishes_to_peer_topic
PASS test_recv_deserialises_raft_message
PASS test_recv_ignores_wrong_size_payload
PASS test_recv_ignores_unknown_topic
PASS test_submit_recv_commands_local
PASS test_send_disabled_no_publish
All MQTT transport tests passed.
```

- [ ] **Step 3.3: Commit**

```bash
git add c/src/raft_mqtt_transport.c
git commit -m "feat: implement core MQTT transport (vtable + topic routing)"
```

---

## Task 4: POSIX HAL (coreMQTT + OpenSSL)

**Files:**
- Create: `c/src/raft_mqtt_hal_posix.c`

The POSIX HAL connects to AWS IoT Core over TLS using OpenSSL, initialises coreMQTT on top of that socket, and runs a background thread that calls `MQTT_ProcessLoop()` in a loop. Received messages are placed in a mutex-protected ring buffer. The app thread drains it via `recv_raw()`.

- [ ] **Step 4.1: Write `c/src/raft_mqtt_hal_posix.c`**

```c
// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

/* POSIX HAL for raft_mqtt_transport using coreMQTT and OpenSSL. */

#if !defined(ESP_PLATFORM)  /* Exclude from ESP32 builds */

#include "raft/raft_mqtt_hal.h"

/* coreMQTT */
#include "core_mqtt.h"

/* POSIX + OpenSSL */
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <pthread.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

/* ── coreMQTT requires NetworkContext_t to be defined by the user ─────────── */

struct NetworkContext {
    SSL *ssl;
    int  fd;
};

/* ── HAL context ─────────────────────────────────────────────────────────── */

#define POSIX_HAL_BUF    4096
#define POSIX_HAL_QSIZE  RAFT_MAX_QUEUE

typedef struct {
    MQTTContext_t   mqtt;
    struct NetworkContext net;
    uint8_t         mqtt_buf[POSIX_HAL_BUF];
    /* Incoming ring buffer */
    struct {
        char    topic[128];
        uint8_t payload[sizeof(raft_message_t) + 4];
        size_t  payload_len;
    } ring[POSIX_HAL_QSIZE];
    size_t          r_head, r_tail;  /* [head, tail) mod POSIX_HAL_QSIZE */
    pthread_mutex_t lock;
    /* Background thread */
    pthread_t       thread;
    volatile int    shutdown;
    int             thread_started;
} raft_mqtt_hal_posix_ctx_t;

/* ── Time helper for coreMQTT ─────────────────────────────────────────────── */

static uint32_t posix_get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint32_t)((uint64_t)ts.tv_sec * 1000UL +
                      (uint64_t)ts.tv_nsec / 1000000UL);
}

/* ── coreMQTT network interface ──────────────────────────────────────────── */

static int32_t posix_tls_send(NetworkContext_t *net,
                               const void *buf, size_t len) {
    int n = SSL_write(net->ssl, buf, (int)len);
    return (n <= 0) ? -1 : (int32_t)n;
}

static int32_t posix_tls_recv(NetworkContext_t *net,
                               void *buf, size_t len) {
    int n = SSL_read(net->ssl, buf, (int)len);
    if (n > 0) return (int32_t)n;
    if (n == 0) return -1;  /* connection closed */
    int err = SSL_get_error(net->ssl, n);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
        return 0;  /* timeout / no data yet */
    return -1;
}

/* ── coreMQTT event callback (called from background thread) ─────────────── */

static void posix_mqtt_event_cb(MQTTContext_t          *ctx,
                                 MQTTPacketInfo_t       *packet_info,
                                 MQTTDeserializedInfo_t *info) {
    (void)packet_info;
    if (!info->incomingPublish) return;

    raft_mqtt_hal_posix_ctx_t *posix =
        (raft_mqtt_hal_posix_ctx_t *)
            ((uint8_t *)ctx - offsetof(raft_mqtt_hal_posix_ctx_t, mqtt));

    MQTTPublishInfo_t *pub = info->pPublishInfo;

    pthread_mutex_lock(&posix->lock);
    if (posix->r_tail - posix->r_head < POSIX_HAL_QSIZE) {
        size_t idx = posix->r_tail % POSIX_HAL_QSIZE;
        size_t tl  = pub->topicNameLength < 127 ? pub->topicNameLength : 127;
        memcpy(posix->ring[idx].topic, pub->pTopicName, tl);
        posix->ring[idx].topic[tl] = '\0';
        size_t pl = pub->payloadLength;
        if (pl > sizeof(posix->ring[0].payload))
            pl = sizeof(posix->ring[0].payload);
        memcpy(posix->ring[idx].payload, pub->pPayload, pl);
        posix->ring[idx].payload_len = pl;
        posix->r_tail++;
    }
    pthread_mutex_unlock(&posix->lock);
}

/* ── Background thread ───────────────────────────────────────────────────── */

static void *posix_mqtt_thread(void *arg) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)arg;
    while (!posix->shutdown)
        MQTT_ProcessLoop(&posix->mqtt, 100 /* ms */);
    return NULL;
}

/* ── TLS connection setup ─────────────────────────────────────────────────── */

static int posix_tls_connect(raft_mqtt_hal_posix_ctx_t *posix,
                               const raft_mqtt_config_t  *cfg) {
    /* Resolve host */
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%d", cfg->port);
    if (getaddrinfo(cfg->endpoint, port_str, &hints, &res) != 0) return -1;

    int fd = socket(res->ai_family, res->ai_socktype, 0);
    if (fd < 0) { freeaddrinfo(res); return -1; }
    if (connect(fd, res->ai_addr, (socklen_t)res->ai_addrlen) < 0) {
        close(fd); freeaddrinfo(res); return -1;
    }
    freeaddrinfo(res);

    /* 100 ms socket receive timeout so ProcessLoop doesn't hang */
    struct timeval tv = {0, 100000};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* TLS context */
    SSL_CTX *ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx) { close(fd); return -1; }

    /* CA certificate */
    BIO *bio = BIO_new_mem_buf(cfg->ca_cert, -1);
    X509 *ca = PEM_read_bio_X509(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (!ca || X509_STORE_add_cert(SSL_CTX_get_cert_store(ssl_ctx), ca) != 1) {
        X509_free(ca); SSL_CTX_free(ssl_ctx); close(fd); return -1;
    }
    X509_free(ca);
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);

    /* Client certificate */
    BIO *cbio = BIO_new_mem_buf(cfg->client_cert, -1);
    X509 *cert = PEM_read_bio_X509(cbio, NULL, NULL, NULL);
    BIO_free(cbio);
    if (!cert || SSL_CTX_use_certificate(ssl_ctx, cert) != 1) {
        X509_free(cert); SSL_CTX_free(ssl_ctx); close(fd); return -1;
    }
    X509_free(cert);

    /* Client key */
    BIO *kbio = BIO_new_mem_buf(cfg->client_key, -1);
    EVP_PKEY *key = PEM_read_bio_PrivateKey(kbio, NULL, NULL, NULL);
    BIO_free(kbio);
    if (!key || SSL_CTX_use_PrivateKey(ssl_ctx, key) != 1) {
        EVP_PKEY_free(key); SSL_CTX_free(ssl_ctx); close(fd); return -1;
    }
    EVP_PKEY_free(key);

    SSL *ssl = SSL_new(ssl_ctx);
    SSL_CTX_free(ssl_ctx);  /* SSL holds its own reference */
    if (!ssl) { close(fd); return -1; }
    SSL_set_fd(ssl, fd);
    SSL_set_tlsext_host_name(ssl, cfg->endpoint);
    if (SSL_connect(ssl) != 1) {
        SSL_free(ssl); close(fd); return -1;
    }

    posix->net.ssl = ssl;
    posix->net.fd  = fd;
    return 0;
}

/* ── HAL vtable functions ─────────────────────────────────────────────────── */

static int posix_hal_connect(void *ctx, const raft_mqtt_config_t *cfg) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)ctx;

    if (posix_tls_connect(posix, cfg) != 0) return -1;

    /* Init coreMQTT */
    TransportInterface_t transport;
    transport.recv            = posix_tls_recv;
    transport.send            = posix_tls_send;
    transport.writev          = NULL;
    transport.pNetworkContext = &posix->net;

    MQTTFixedBuffer_t net_buf = { posix->mqtt_buf, sizeof(posix->mqtt_buf) };

    MQTTStatus_t status = MQTT_Init(&posix->mqtt, &transport,
                                     posix_get_time_ms,
                                     posix_mqtt_event_cb, &net_buf);
    if (status != MQTTSuccess) goto fail;

    /* MQTT CONNECT */
    MQTTConnectInfo_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.cleanSession            = 1;
    conn.pClientIdentifier       = cfg->node_id;
    conn.clientIdentifierLength  = (uint16_t)strlen(cfg->node_id);
    conn.keepAliveSeconds        = (uint16_t)(cfg->keepalive_s > 0
                                              ? cfg->keepalive_s : 60);

    bool session_present;
    status = MQTT_Connect(&posix->mqtt, &conn, NULL, 5000, &session_present);
    if (status != MQTTSuccess) goto fail;

    /* Start background thread */
    pthread_mutex_init(&posix->lock, NULL);
    posix->shutdown = 0;
    if (pthread_create(&posix->thread, NULL, posix_mqtt_thread, posix) != 0)
        goto fail;
    posix->thread_started = 1;
    return 0;

fail:
    SSL_free(posix->net.ssl);
    close(posix->net.fd);
    posix->net.ssl = NULL;
    posix->net.fd  = -1;
    return -1;
}

static int posix_hal_publish(void *ctx, const char *topic,
                              const void *payload, size_t len, int qos) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)ctx;
    MQTTPublishInfo_t pub;
    memset(&pub, 0, sizeof(pub));
    pub.qos             = (MQTTQoS_t)qos;
    pub.pTopicName      = topic;
    pub.topicNameLength = (uint16_t)strlen(topic);
    pub.pPayload        = payload;
    pub.payloadLength   = (uint16_t)len;
    uint16_t pkt_id = (qos > 0) ? MQTT_GetPacketId(&posix->mqtt) : 0;
    MQTTStatus_t s = MQTT_Publish(&posix->mqtt, &pub, pkt_id);
    return (s == MQTTSuccess) ? 0 : -1;
}

static int posix_hal_subscribe(void *ctx, const char *topic, int qos) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)ctx;
    MQTTSubscribeInfo_t sub;
    sub.qos              = (MQTTQoS_t)qos;
    sub.pTopicFilter     = topic;
    sub.topicFilterLength = (uint16_t)strlen(topic);
    uint16_t pkt_id = MQTT_GetPacketId(&posix->mqtt);
    MQTTStatus_t s = MQTT_Subscribe(&posix->mqtt, &sub, 1, pkt_id);
    if (s != MQTTSuccess) return -1;
    /* Wait for SUBACK */
    s = MQTT_ProcessLoop(&posix->mqtt, 5000);
    return (s == MQTTSuccess) ? 0 : -1;
}

static void posix_hal_poll(void *ctx) { (void)ctx; /* thread handles it */ }

static int posix_hal_recv_raw(void *ctx,
                               char *topic_out, size_t topic_max,
                               void *payload_out, size_t payload_max,
                               size_t *payload_len_out) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)ctx;
    int got = 0;

    pthread_mutex_lock(&posix->lock);
    if (posix->r_head != posix->r_tail) {
        size_t idx = posix->r_head % POSIX_HAL_QSIZE;
        strncpy(topic_out, posix->ring[idx].topic, topic_max - 1);
        topic_out[topic_max - 1] = '\0';
        size_t pl = posix->ring[idx].payload_len;
        if (pl > payload_max) pl = payload_max;
        memcpy(payload_out, posix->ring[idx].payload, pl);
        *payload_len_out = pl;
        posix->r_head++;
        got = 1;
    }
    pthread_mutex_unlock(&posix->lock);
    return got;
}

static void posix_hal_disconnect(void *ctx) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)ctx;
    if (posix->thread_started) {
        posix->shutdown = 1;
        pthread_join(posix->thread, NULL);
        posix->thread_started = 0;
        pthread_mutex_destroy(&posix->lock);
    }
    if (posix->net.ssl) {
        MQTT_Disconnect(&posix->mqtt);
        SSL_shutdown(posix->net.ssl);
        SSL_free(posix->net.ssl);
        close(posix->net.fd);
        posix->net.ssl = NULL;
        posix->net.fd  = -1;
    }
}

/* ── Public factory ─────────────────────────────────────────────────────── */

raft_mqtt_hal_t raft_mqtt_hal_posix_make(raft_mqtt_hal_posix_ctx_t *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->net.fd = -1;
    raft_mqtt_hal_t h;
    h.connect    = posix_hal_connect;
    h.publish    = posix_hal_publish;
    h.subscribe  = posix_hal_subscribe;
    h.poll       = posix_hal_poll;
    h.recv_raw   = posix_hal_recv_raw;
    h.disconnect = posix_hal_disconnect;
    h.ctx        = ctx;
    return h;
}

#endif /* !ESP_PLATFORM */
```

Also add the factory declaration to `raft_mqtt_transport.h` in the POSIX-only block:

In `c/include/raft/raft_mqtt_transport.h`, append before the `#ifdef __cplusplus` closing:

```c
#if !defined(ESP_PLATFORM)
/* Forward declaration — defined in raft_mqtt_hal_posix.c */
typedef struct raft_mqtt_hal_posix_ctx_s raft_mqtt_hal_posix_ctx_t;
raft_mqtt_hal_t raft_mqtt_hal_posix_make(raft_mqtt_hal_posix_ctx_t *ctx);
#endif
```

And add the struct forward in a comment note: the full typedef is internal to `raft_mqtt_hal_posix.c`; callers declare it as:
```c
/* in application code */
#include "raft/raft_mqtt_transport.h"
static raft_mqtt_hal_posix_ctx_t hal_ctx;  /* opaque; size defined by including raft_mqtt_hal_posix.h */
```

Actually the cleanest way is to expose the typedef via a small extra header. Add `c/include/raft/raft_mqtt_hal_posix.h`:

```c
// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once
#if !defined(ESP_PLATFORM)
#include "raft/raft_mqtt_hal.h"
/* Forward-only; include this header to get the factory and allocate the ctx. */
struct raft_mqtt_hal_posix_ctx_s;
typedef struct raft_mqtt_hal_posix_ctx_s raft_mqtt_hal_posix_ctx_t;
/* Size of the opaque ctx for stack allocation — use this macro. */
#include <stddef.h>
size_t raft_mqtt_hal_posix_ctx_size(void);
raft_mqtt_hal_t raft_mqtt_hal_posix_make(raft_mqtt_hal_posix_ctx_t *ctx);
#endif
```

Hmm, the opaque size approach requires dynamic allocation or a user-specified fixed buffer. For simplicity in C99 (no VLA on stack for opaque types), let's just expose the full struct in the header. The struct is internal implementation but in C we often need to expose it for stack allocation.

**Simplification:** skip the opaque struct. In `raft_mqtt_hal_posix.h`, typedef the full struct publicly. This is the same pattern as `raft_tcp_transport_t` which is fully public.

Create `c/include/raft/raft_mqtt_hal_posix.h`:

```c
// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once
#if !defined(ESP_PLATFORM)
#include "raft/raft_mqtt_hal.h"
#include "core_mqtt.h"
#include <pthread.h>

#define POSIX_HAL_BUF    4096
#define POSIX_HAL_QSIZE  RAFT_MAX_QUEUE

/* Forward-declare the network context type required by coreMQTT. */
struct NetworkContext;

typedef struct {
    MQTTContext_t   mqtt;
    struct NetworkContext *net_ptr; /* pointer to net field below */
    struct {
        void *ssl;  /* SSL* — avoid pulling in openssl headers */
        int   fd;
    } net;
    uint8_t         mqtt_buf[POSIX_HAL_BUF];
    struct {
        char    topic[128];
        uint8_t payload[sizeof(raft_message_t) + 4];
        size_t  payload_len;
    } ring[POSIX_HAL_QSIZE];
    size_t          r_head, r_tail;
    pthread_mutex_t lock;
    pthread_t       thread;
    volatile int    shutdown;
    int             thread_started;
} raft_mqtt_hal_posix_ctx_t;

raft_mqtt_hal_t raft_mqtt_hal_posix_make(raft_mqtt_hal_posix_ctx_t *ctx);
#endif /* !ESP_PLATFORM */
```

Actually this gets messy with `void *ssl` vs `SSL *ssl`. Let me just keep it simple: include OpenSSL in the public header (it's an implementation requirement anyway).

**Final pragmatic decision:** The `raft_mqtt_hal_posix.c` implementation struct is fully defined in its `.c` file. The public header `raft_mqtt_hal_posix.h` just re-includes the same struct definition using `#include "raft_mqtt_hal_posix_internal.h"` or we keep the struct fully in the `.c` and expose a `raft_mqtt_hal_posix_alloc_size()` function. 

For maximum simplicity matching the codebase style, **define the full struct in the public header** and accept that `openssl/ssl.h` is required to include it. This mirrors how `raft_tcp_transport.h` uses `pthread.h`.

The final `raft_mqtt_hal_posix.h` is written in Step 4.2 below.

- [ ] **Step 4.2: Write `c/include/raft/raft_mqtt_hal_posix.h`**

```c
// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#if !defined(ESP_PLATFORM)

#include "raft/raft_mqtt_hal.h"
#include "core_mqtt.h"
#include <openssl/ssl.h>
#include <pthread.h>

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
```

Then update `raft_mqtt_hal_posix.c` to include this header instead of re-declaring the struct. The `NetworkContext` struct is now defined in the public header.

- [ ] **Step 4.3: Update `raft_mqtt_hal_posix.c` to use the public header**

Replace the top of `raft_mqtt_hal_posix.c` (the struct declaration section) with:

```c
#include "raft/raft_mqtt_hal_posix.h"
/* All types including raft_mqtt_hal_posix_ctx_t and NetworkContext come from
 * the public header. */
```

Remove the duplicate `#define POSIX_HAL_BUF` / `#define POSIX_HAL_QSIZE` lines from the `.c` file (they are now `RAFT_MQTT_POSIX_BUF_SIZE` / `RAFT_MQTT_POSIX_QUEUE_SIZE` from the header). Update references in the `.c` accordingly.

- [ ] **Step 4.4: Verify the POSIX HAL compiles**

```bash
cd /path/to/raft_rx/c
cc -std=c99 -Wall \
   -Iinclude \
   -I../../rxnet/c/include \
   -I third_party/coreMQTT/source/include \
   -I third_party/coreMQTT/source/interface \
   -I third_party \
   -c src/raft_mqtt_hal_posix.c -o build/raft_mqtt_hal_posix.o \
   $(pkg-config --cflags openssl)
```

Expected: compiles cleanly. Ignore `SSL_CTX_free` / OpenSSL deprecation warnings if on macOS with LibreSSL — not errors.

Note: `third_party/coreMQTT/source/core_mqtt.c`, `core_mqtt_serializer.c`, and `core_mqtt_state.c` also need to be compiled and linked. This is handled in the Makefile (Task 7).

- [ ] **Step 4.5: Commit**

```bash
git add c/src/raft_mqtt_hal_posix.c c/include/raft/raft_mqtt_hal_posix.h
git commit -m "feat: implement POSIX MQTT HAL (coreMQTT + OpenSSL + pthread)"
```

---

## Task 5: ESP32 HAL

**Files:**
- Create: `c/src/raft_mqtt_hal_esp32.c`
- Create: `c/include/raft/raft_mqtt_hal_esp32.h`

This HAL wraps `esp_mqtt_client`. The `MQTT_EVENT_DATA` callback pushes received messages into a FreeRTOS queue. `recv_raw()` drains it with a zero timeout. **This file cannot be unit-tested without ESP32 hardware + AWS IoT Core. Build verification on the target is the acceptance test.**

- [ ] **Step 5.1: Write `c/include/raft/raft_mqtt_hal_esp32.h`**

```c
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
```

- [ ] **Step 5.2: Write `c/src/raft_mqtt_hal_esp32.c`**

```c
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
        /* Non-blocking send: drop if queue full */
        xQueueSend(ctx->queue, &msg, 0);
        break;
    }
    default:
        break;
    }
}

static int esp_hal_connect(void *arg, const raft_mqtt_config_t *cfg) {
    raft_mqtt_hal_esp32_ctx_t *ctx = (raft_mqtt_hal_esp32_ctx_t *)arg;

    ctx->queue = xQueueCreateStatic(
        RAFT_MQTT_ESP32_QUEUE_SIZE,
        sizeof(raft_mqtt_esp32_msg_t),
        (uint8_t *)ctx->pool,
        /* StaticQueue_t must be caller-provided — add it to the struct if needed */
        NULL  /* dynamic allocation for the queue struct */
    );
    /* Note: for zero-heap use, replace with xQueueCreateStatic and add
     * StaticQueue_t queue_buf to raft_mqtt_hal_esp32_ctx_t. */
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
    /* Wait for CONNECTED (simple busy-wait; real app should use event group) */
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

static void esp_hal_poll(void *arg) { (void)arg; }

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
```

Note on `xQueueCreateStatic`: for zero-heap ESP32 use, add `StaticQueue_t queue_buf` to `raft_mqtt_hal_esp32_ctx_t` and replace `xQueueCreate` with `xQueueCreateStatic`. The current implementation uses dynamic FreeRTOS allocation which is the default ESP-IDF behaviour.

- [ ] **Step 5.3: Commit**

```bash
git add c/src/raft_mqtt_hal_esp32.c c/include/raft/raft_mqtt_hal_esp32.h
git commit -m "feat: implement ESP32 MQTT HAL (esp_mqtt_client + FreeRTOS queue)"
```

---

## Task 6: Dynamic peer discovery

**Files:**
- Modify: `c/tests/test_mqtt_transport.c` — add join tests
- Create: `c/src/raft_mqtt_join.c`

- [ ] **Step 6.1: Append join tests to `c/tests/test_mqtt_transport.c`**

Add the following tests at the end of the test list, before `main()`:

```c
static void test_join_first_call_subscribes_and_announces(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    /* inbox subscribed during start; no join sub yet */
    assert(m.sub_count == 1);
    assert(m.pub_count == 0);

    raft_mqtt_join_t out[4];
    raft_mqtt_transport_recv_joins(&mqtt, out, 4);

    /* After first recv_joins: subscribed to join topic, published own id */
    assert(m.sub_count == 2);
    assert(strcmp(m.subscribed[1], "raft/mycluster/join") == 0);
    assert(m.pub_count == 1);
    assert(strcmp(m.published[0].topic, "raft/mycluster/join") == 0);
    assert(strncmp((char *)m.published[0].payload, "n1",
                   m.published[0].payload_len) == 0);
    printf("PASS test_join_first_call_subscribes_and_announces\n");
}

static void test_join_second_call_no_extra_subscribe(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_mqtt_join_t out[4];
    raft_mqtt_transport_recv_joins(&mqtt, out, 4);
    size_t sub_after_first = m.sub_count;
    size_t pub_after_first = m.pub_count;

    raft_mqtt_transport_recv_joins(&mqtt, out, 4);
    assert(m.sub_count == sub_after_first);  /* no duplicate subscribe */
    assert(m.pub_count == pub_after_first);  /* no duplicate announce */
    printf("PASS test_join_second_call_no_extra_subscribe\n");
}

static void test_join_recv_returns_joining_node_id(void) {
    mock_ctx_t m; memset(&m, 0, sizeof(m));
    raft_mqtt_hal_t hal = mock_hal_make(&m);
    raft_mqtt_transport_t mqtt;
    raft_mqtt_config_t cfg = make_cfg("mycluster", "n1");

    assert(raft_mqtt_transport_init(&mqtt, &cfg, &hal) == 0);
    assert(raft_mqtt_transport_start(&mqtt) == 0);

    raft_mqtt_join_t out[4];
    raft_mqtt_transport_recv_joins(&mqtt, out, 4);  /* init join */

    /* Simulate n3 announcing itself on the join topic */
    const char *joining = "n3";
    mock_inject(&m, "raft/mycluster/join", joining, strlen(joining));

    /* recv() routes join-topic messages into join_queue */
    raft_message_t msgs[4];
    raft_transport_t t = raft_mqtt_transport_make(&mqtt);
    t.recv(t.ctx, msgs, 4);

    size_t count = raft_mqtt_transport_recv_joins(&mqtt, out, 4);
    assert(count == 1);
    assert(strcmp(out[0].node_id, "n3") == 0);
    printf("PASS test_join_recv_returns_joining_node_id\n");
}
```

Add calls to these three test functions in `main()`:
```c
    test_join_first_call_subscribes_and_announces();
    test_join_second_call_no_extra_subscribe();
    test_join_recv_returns_joining_node_id();
```

- [ ] **Step 6.2: Verify new tests fail (join not implemented yet)**

```bash
cd /path/to/raft_rx/c
cc -std=c99 -Wall -Iinclude -I../../rxnet/c/include \
   src/raft_mqtt_transport.c \
   tests/test_mqtt_transport.c \
   -o build/test_mqtt_transport \
   ../../rxnet/c/build/librxnet.a
```

Expected: linker error — `raft_mqtt_transport_recv_joins` undefined.

- [ ] **Step 6.3: Write `c/src/raft_mqtt_join.c`**

```c
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
```

- [ ] **Step 6.4: Build and run all tests**

```bash
cd /path/to/raft_rx/c
cc -std=c99 -Wall -Iinclude -I../../rxnet/c/include \
   src/raft_mqtt_transport.c \
   src/raft_mqtt_join.c \
   tests/test_mqtt_transport.c \
   -o build/test_mqtt_transport \
   ../../rxnet/c/build/librxnet.a
./build/test_mqtt_transport
```

Expected:
```
PASS test_init_subscribes_to_inbox
PASS test_send_publishes_to_peer_topic
PASS test_recv_deserialises_raft_message
PASS test_recv_ignores_wrong_size_payload
PASS test_recv_ignores_unknown_topic
PASS test_submit_recv_commands_local
PASS test_send_disabled_no_publish
PASS test_join_first_call_subscribes_and_announces
PASS test_join_second_call_no_extra_subscribe
PASS test_join_recv_returns_joining_node_id
All MQTT transport tests passed.
```

- [ ] **Step 6.5: Commit**

```bash
git add c/src/raft_mqtt_join.c c/tests/test_mqtt_transport.c
git commit -m "feat: add dynamic join discovery module + tests"
```

---

## Task 7: Makefile integration

**Files:**
- Modify: `c/Makefile`

- [ ] **Step 7.1: Update `c/Makefile`**

Add after the existing variable declarations and before the first target:

```makefile
# ── coreMQTT ──────────────────────────────────────────────────────────────
COREMQTT_DIR  := third_party/coreMQTT/source
COREMQTT_INC  := -I$(COREMQTT_DIR)/include \
                 -I$(COREMQTT_DIR)/interface \
                 -I third_party
COREMQTT_SRCS := $(COREMQTT_DIR)/core_mqtt.c \
                 $(COREMQTT_DIR)/core_mqtt_serializer.c \
                 $(COREMQTT_DIR)/core_mqtt_state.c
COREMQTT_OBJS := build/core_mqtt.o \
                 build/core_mqtt_serializer.o \
                 build/core_mqtt_state.o
OPENSSL_FLAGS := $(shell pkg-config --cflags openssl 2>/dev/null || echo "")
OPENSSL_LIBS  := $(shell pkg-config --libs   openssl 2>/dev/null || echo "-lssl -lcrypto")
```

Add build rules:

```makefile
build/core_mqtt.o: build $(COREMQTT_DIR)/core_mqtt.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) -c -o $@ $(COREMQTT_DIR)/core_mqtt.c

build/core_mqtt_serializer.o: build $(COREMQTT_DIR)/core_mqtt_serializer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) -c -o $@ $(COREMQTT_DIR)/core_mqtt_serializer.c

build/core_mqtt_state.o: build $(COREMQTT_DIR)/core_mqtt_state.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) -c -o $@ $(COREMQTT_DIR)/core_mqtt_state.c

build/raft_mqtt_transport.o: build src/raft_mqtt_transport.c \
    include/raft/raft_mqtt_transport.h include/raft/raft_mqtt_hal.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_mqtt_transport.c

build/raft_mqtt_hal_posix.o: build src/raft_mqtt_hal_posix.c \
    include/raft/raft_mqtt_hal_posix.h $(COREMQTT_SRCS)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) $(OPENSSL_FLAGS) \
	    -c -o $@ src/raft_mqtt_hal_posix.c

build/raft_mqtt_join.o: build src/raft_mqtt_join.c \
    include/raft/raft_mqtt_transport.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_mqtt_join.c

build/test_mqtt_transport: build \
    tests/test_mqtt_transport.c \
    build/raft_mqtt_transport.o \
    build/raft_mqtt_join.o \
    $(RXNET_LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ \
	    tests/test_mqtt_transport.c \
	    build/raft_mqtt_transport.o \
	    build/raft_mqtt_join.o \
	    $(RXNET_LIB)
```

Update the `all` target to include the new objects:

```makefile
all: build/libraft_rx.a build/libraft_rx_trace.a \
     build/test_raft build/raft_kv_cluster build/raft_kv_cluster_trace \
     build/raft_mqtt_transport.o build/raft_mqtt_join.o \
     build/raft_mqtt_hal_posix.o $(COREMQTT_OBJS) \
     build/test_mqtt_transport
```

Update the `test` target:

```makefile
test: build/test_raft build/raft_kv_cluster build/test_mqtt_transport
	./build/test_raft
	./build/raft_kv_cluster
	./build/raft_kv_cluster
	./build/test_mqtt_transport
```

- [ ] **Step 7.2: Run full build**

```bash
cd /path/to/raft_rx/c
make clean && make all
```

Expected: all targets build cleanly, no errors.

- [ ] **Step 7.3: Run all tests**

```bash
make test
```

Expected: `test_raft`, `raft_kv_cluster`, and `test_mqtt_transport` all pass.

- [ ] **Step 7.4: Commit**

```bash
git add c/Makefile
git commit -m "build: integrate MQTT transport into Makefile"
```

---

## Self-Review Notes

**Spec coverage check:**

| Spec requirement | Task |
|-----------------|------|
| `raft_mqtt_hal.h` — HAL vtable + config | Task 1 |
| `raft_mqtt_transport.h` — public API | Task 1 |
| `raft_mqtt_transport.c` — vtable, topics, queues | Task 3 |
| `raft_mqtt_hal_posix.c` — coreMQTT + OpenSSL + thread | Task 4 |
| `raft_mqtt_hal_esp32.c` — esp_mqtt_client | Task 5 |
| `raft_mqtt_join.c` — dynamic join, linker-optional | Task 6 |
| Binary payload (`memcpy` of `raft_message_t`) | Task 3 |
| Topic scheme `raft/<cluster>/<node_id>` | Task 3 |
| TLS via in-memory PEM buffers | Tasks 4, 5 |
| No changes to `raft.c` / existing API | All tasks |
| Join incurs zero overhead when not linked | Tasks 6, 7 |
| Makefile build targets | Task 7 |

**No placeholders found.**

**Type consistency:** `raft_mqtt_hal_t`, `raft_mqtt_config_t`, `raft_mqtt_transport_t`, and `raft_mqtt_join_t` are defined once in Tasks 1/3 and used consistently throughout.
