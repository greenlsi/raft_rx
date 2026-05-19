# MQTT Transport for raft_rx (C)

**Date:** 2026-05-19  
**Status:** Approved  
**Scope:** C implementation only; POSIX (Linux/macOS) and ESP32 (ESP-IDF 6.x) platforms

---

## Overview

Add an optional MQTT transport to raft_rx that allows Raft cluster nodes to communicate over AWS IoT Core. The transport implements the existing `raft_transport_t` vtable so it is a drop-in replacement for `raft_tcp_transport` with no changes to the consensus engine.

---

## Goals

- Support AWS IoT Core (mTLS, port 8883) on both POSIX and ESP32.
- Zero changes to `raft.c`, `raft_transport.c`, or any existing public API.
- Dynamic peer support is available but incurs no link-time or run-time cost when unused.
- All platform-specific code is encapsulated inside its HAL implementation file.

---

## File Structure

```
c/
├── include/raft/
│   ├── raft_mqtt_transport.h      # Public API and config struct
│   └── raft_mqtt_hal.h            # HAL interface (internal; not part of public API)
└── src/
    ├── raft_mqtt_transport.c      # Raft↔MQTT logic, vtable, topic construction, queues
    ├── raft_mqtt_hal_posix.c      # coreMQTT + OpenSSL/mbedTLS (hidden thread if needed)
    ├── raft_mqtt_hal_esp32.c      # esp_mqtt_client + FreeRTOS queue
    └── raft_mqtt_join.c           # Optional dynamic peer discovery
```

`raft_mqtt_join.c` is linked only when the application references
`raft_mqtt_transport_recv_joins` — no `#ifdef` required.

---

## MQTT Libraries

| Platform | Library | Notes |
|----------|---------|-------|
| POSIX    | coreMQTT (AWS IoT Device SDK for Embedded C) | C99, no heap, OpenSSL/mbedTLS for TLS. Uses a hidden background thread (implementation detail of the HAL, not visible to raft_rx). |
| ESP32    | `esp_mqtt_client` (ESP-IDF 6.x) | Event-driven; internal FreeRTOS task hidden behind HAL callbacks. |

---

## Topic Scheme

| Direction | Topic |
|-----------|-------|
| Raft message to node X | `raft/<cluster_id>/<node_id_X>` |
| Own inbox (subscribe) | `raft/<cluster_id>/<own_node_id>` |
| Join announcement (optional) | `raft/<cluster_id>/join` |

- `cluster_id` is a configurable string. Multiple clusters can share the same AWS IoT Core account without collision.
- Topic paths are constructed at init time and stored in the transport state — no runtime heap allocation.

---

## Configuration Struct

```c
typedef struct {
    char    cluster_id[64];
    char    node_id[RAFT_MAX_ID];
    char    endpoint[256];      /* e.g. xxxxx.iot.eu-west-1.amazonaws.com */
    int     port;               /* 8883 for TLS */

    /* TLS certificates — in-memory PEM buffers (NULL-terminated strings) */
    const char *ca_cert;        /* Root CA certificate */
    const char *client_cert;    /* Client certificate */
    const char *client_key;     /* Client private key */

    int     qos;                /* 0 or 1 */
    int     keepalive_s;        /* MQTT keepalive interval in seconds */
} raft_mqtt_config_t;
```

Certificates are passed as in-memory PEM buffers so the same API works on POSIX
(loaded from files by the application) and ESP32 (loaded from NVS or embedded flash).

---

## HAL Interface

```c
/* Callback fired by the HAL when a message arrives on any subscribed topic. */
typedef void (*raft_mqtt_hal_cb_t)(void       *user,
                                   const char *topic,  size_t topic_len,
                                   const void *payload, size_t payload_len);

typedef struct {
    int  (*connect)   (void *ctx, const raft_mqtt_config_t *cfg,
                       raft_mqtt_hal_cb_t on_message, void *user);
    int  (*publish)   (void *ctx, const char *topic,
                       const void *payload, size_t len, int qos);
    int  (*subscribe) (void *ctx, const char *topic, int qos);
    void (*poll)      (void *ctx);   /* no-op on both platforms when thread/callbacks used */
    void (*disconnect)(void *ctx);
    void *ctx;
} raft_mqtt_hal_t;
```

`raft_mqtt_transport.c` depends only on this interface, never on platform headers.

### POSIX HAL (`raft_mqtt_hal_posix.c`)

- Initialises coreMQTT with an OpenSSL/mbedTLS network context for mTLS.
- Runs a background thread that calls `MQTT_ProcessLoop()` in a loop; messages
  received in the thread are placed in a mutex-protected queue.
- `on_message_cb` is called from the thread; the transport reads the queue from
  `recv()` on the application thread.
- `poll()` is a no-op.

### ESP32 HAL (`raft_mqtt_hal_esp32.c`)

- Configures and starts `esp_mqtt_client` with the provided PEM buffers and endpoint.
- The `MQTT_EVENT_DATA` handler copies the payload into a FreeRTOS queue and fires
  `on_message_cb`.
- `poll()` is a no-op; the internal task drives all I/O.

---

## send / recv Flow

### send

1. Look up `msg->target` in the peer list to build the topic
   `raft/<cluster_id>/<msg->target>`.
2. `memcpy` the `raft_message_t` into a stack buffer.
3. `hal->publish(topic, buf, sizeof(raft_message_t), qos)`.

### recv

1. `hal->poll(ctx)` — no-op on both platforms.
2. Lock queue mutex; copy up to `capacity` `raft_message_t` entries out; unlock.
3. Return count.

### submit_command / recv_commands

Local queue only — no network involvement. Identical behaviour to the TCP transport.

---

## Dynamic Peer Discovery (`raft_mqtt_join.c`)

When linked, this module:

1. Subscribes to `raft/<cluster_id>/join` at connect time.
2. Publishes the own node ID to `raft/<cluster_id>/join` (retained = false) after
   connecting, so existing nodes learn about the newcomer.
3. Exposes:

```c
size_t raft_mqtt_transport_recv_joins(raft_mqtt_transport_t *mqtt,
                                      raft_mqtt_join_t      *out,
                                      size_t                 capacity);
```

The application drains join announcements and calls
`raft_node_request_membership_change()` to admit a new node. The transport itself
does not modify cluster membership.

**No static-peer code is affected.** A node that never calls
`raft_mqtt_transport_recv_joins` (and never links `raft_mqtt_join.o`) incurs zero
overhead.

---

## Payload Format

Raw binary: `memcpy` of `raft_message_t` directly into the MQTT payload. No
serialisation library required. AWS IoT Core accepts arbitrary binary payloads.
Message size is fixed (`sizeof(raft_message_t)` ≈ 2 KB); the receiver validates
length before deserialising.

---

## Build Integration

### POSIX Makefile

```makefile
build/raft_mqtt_hal_posix.o: src/raft_mqtt_hal_posix.c
    $(CC) $(CPPFLAGS) $(CFLAGS) -I$(COREMQTT_INCLUDE) -c -o $@ $<

build/raft_mqtt_transport.o: src/raft_mqtt_transport.c
    $(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

# raft_mqtt_join.o is built but only linked when referenced
build/raft_mqtt_join.o: src/raft_mqtt_join.c
    $(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<
```

### ESP32 (CMake / ESP-IDF component)

`CMakeLists.txt` selects `raft_mqtt_hal_esp32.c` via the existing `IDF_TARGET`
variable; `raft_mqtt_hal_posix.c` is excluded. `raft_mqtt_join.c` is always
compiled into the component.

---

## What Is Not In Scope

- Command forwarding from follower to leader over MQTT (application concern, same
  as with TCP).
- MQTT 5.0 features (AWS IoT Core supports MQTT 3.1.1 and 5.0; this design targets
  3.1.1 which is the common subset).
- Automatic reconnect logic beyond what `esp_mqtt_client` and coreMQTT provide
  natively.
- Python implementation (separate effort).
