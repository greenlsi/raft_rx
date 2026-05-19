// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

/* POSIX HAL for raft_mqtt_transport using coreMQTT and OpenSSL. */

#if !defined(ESP_PLATFORM)

#include "raft/raft_mqtt_hal_posix.h"

#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>

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
    if (n == 0) return -1;
    int err = SSL_get_error(net->ssl, n);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
        return 0;
    return -1;
}

/* ── coreMQTT event callback (called from background thread) ─────────────── */

static void posix_mqtt_event_cb(MQTTContext_t          *ctx,
                                 MQTTPacketInfo_t       *packet_info,
                                 MQTTDeserializedInfo_t *info) {
    (void)packet_info;
    if (!info->pPublishInfo) return;

    raft_mqtt_hal_posix_ctx_t *posix =
        (raft_mqtt_hal_posix_ctx_t *)
            ((uint8_t *)ctx - offsetof(raft_mqtt_hal_posix_ctx_t, mqtt));

    MQTTPublishInfo_t *pub = info->pPublishInfo;

    pthread_mutex_lock(&posix->lock);
    if (posix->r_tail - posix->r_head < RAFT_MQTT_POSIX_QUEUE_SIZE) {
        size_t idx = posix->r_tail % RAFT_MQTT_POSIX_QUEUE_SIZE;
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

    /* 100 ms socket receive timeout so ProcessLoop doesn't block */
    struct timeval tv = {0, 100000};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    SSL_CTX *ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx) { close(fd); return -1; }

    BIO *bio = BIO_new_mem_buf(cfg->ca_cert, -1);
    X509 *ca = PEM_read_bio_X509(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (!ca || X509_STORE_add_cert(SSL_CTX_get_cert_store(ssl_ctx), ca) != 1) {
        X509_free(ca); SSL_CTX_free(ssl_ctx); close(fd); return -1;
    }
    X509_free(ca);
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);

    BIO *cbio = BIO_new_mem_buf(cfg->client_cert, -1);
    X509 *cert = PEM_read_bio_X509(cbio, NULL, NULL, NULL);
    BIO_free(cbio);
    if (!cert || SSL_CTX_use_certificate(ssl_ctx, cert) != 1) {
        X509_free(cert); SSL_CTX_free(ssl_ctx); close(fd); return -1;
    }
    X509_free(cert);

    BIO *kbio = BIO_new_mem_buf(cfg->client_key, -1);
    EVP_PKEY *key = PEM_read_bio_PrivateKey(kbio, NULL, NULL, NULL);
    BIO_free(kbio);
    if (!key || SSL_CTX_use_PrivateKey(ssl_ctx, key) != 1) {
        EVP_PKEY_free(key); SSL_CTX_free(ssl_ctx); close(fd); return -1;
    }
    EVP_PKEY_free(key);

    SSL *ssl = SSL_new(ssl_ctx);
    SSL_CTX_free(ssl_ctx);
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

    TransportInterface_t transport;
    transport.recv            = posix_tls_recv;
    transport.send            = posix_tls_send;
    transport.pNetworkContext = &posix->net;

    MQTTFixedBuffer_t net_buf = { posix->mqtt_buf, sizeof(posix->mqtt_buf) };

    MQTTStatus_t status = MQTT_Init(&posix->mqtt, &transport,
                                     posix_get_time_ms,
                                     posix_mqtt_event_cb, &net_buf);
    if (status != MQTTSuccess) goto fail;

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
    sub.qos               = (MQTTQoS_t)qos;
    sub.pTopicFilter      = topic;
    sub.topicFilterLength = (uint16_t)strlen(topic);
    uint16_t pkt_id = MQTT_GetPacketId(&posix->mqtt);
    MQTTStatus_t s = MQTT_Subscribe(&posix->mqtt, &sub, 1, pkt_id);
    if (s != MQTTSuccess) return -1;
    s = MQTT_ProcessLoop(&posix->mqtt, 5000);
    return (s == MQTTSuccess) ? 0 : -1;
}

static void posix_hal_poll(void *ctx) { (void)ctx; /* background thread handles it */ }

static int posix_hal_recv_raw(void *ctx,
                               char *topic_out, size_t topic_max,
                               void *payload_out, size_t payload_max,
                               size_t *payload_len_out) {
    raft_mqtt_hal_posix_ctx_t *posix = (raft_mqtt_hal_posix_ctx_t *)ctx;
    int got = 0;

    pthread_mutex_lock(&posix->lock);
    if (posix->r_head != posix->r_tail) {
        size_t idx = posix->r_head % RAFT_MQTT_POSIX_QUEUE_SIZE;
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

/* ── Public factory ──────────────────────────────────────────────────────── */

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
