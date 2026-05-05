// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: MIT

#include "raft/raft_tcp_transport.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define TCP_MAGIC 0x52414654u  /* 'RAFT' */

typedef struct {
    uint32_t magic;
    uint32_t type;   /* TCP_TYPE_RAFT or TCP_TYPE_JOIN */
    uint32_t size;
} raft_tcp_frame_t;

/* ── I/O helpers ─────────────────────────────────────────────────────────── */

static int recv_full(int fd, void *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = recv(fd, (char *)buf + done, len - done, 0);
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

static int send_full(int fd, const void *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = send(fd, (const char *)buf + done, len - done, 0);
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

/* Send a join-request frame to a specific host:port. */
static int tcp_send_join(const char *host, int port,
                          const raft_tcp_join_req_t *jr) {
    raft_tcp_frame_t frame;
    int fd;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(fd); return -1; }

    frame.magic = TCP_MAGIC;
    frame.type  = TCP_TYPE_JOIN;
    frame.size  = (uint32_t)sizeof(*jr);

    if (send_full(fd, &frame, sizeof(frame)) < 0 ||
        send_full(fd, jr,    sizeof(*jr))    < 0) {
        close(fd); return -1;
    }
    close(fd);
    return 0;
}

int raft_tcp_transport_send_cluster_join(raft_tcp_transport_t *tcp,
                                          const raft_tcp_peer_t *members,
                                          size_t member_count,
                                          const char *target_host,
                                          int target_port,
                                          int forwarded);

/* ── Listener thread ────────────────────────────────────────────────────── */

static void *tcp_listener_thread(void *arg) {
    raft_tcp_transport_t *tcp = (raft_tcp_transport_t *)arg;

    while (!tcp->shutdown) {
        fd_set rfds;
        struct timeval tv;
        FD_ZERO(&rfds);
        FD_SET(tcp->listen_fd, &rfds);
        tv.tv_sec  = 0;
        tv.tv_usec = 100000;  /* 100 ms */

        if (select(tcp->listen_fd + 1, &rfds, NULL, NULL, &tv) <= 0) continue;

        int client_fd = accept(tcp->listen_fd, NULL, NULL);
        if (client_fd < 0) continue;

        raft_tcp_frame_t header;
        if (recv_full(client_fd, &header, sizeof(header)) < 0 ||
            header.magic != TCP_MAGIC) {
            close(client_fd);
            continue;
        }

        if (header.type == TCP_TYPE_JOIN) {
            if (header.size != (uint32_t)sizeof(raft_tcp_join_req_t)) {
                close(client_fd); continue;
            }
            raft_tcp_join_req_t jr;
            if (recv_full(client_fd, &jr, sizeof(jr)) < 0) {
                close(client_fd); continue;
            }
            close(client_fd);
            pthread_mutex_lock(&tcp->lock);
            if (tcp->join_count < RAFT_MAX_NODES)
                tcp->join_queue[tcp->join_count++] = jr;
            pthread_mutex_unlock(&tcp->lock);
            continue;
        }

        if (header.type == TCP_TYPE_CMD) {
            if (header.size != (uint32_t)sizeof(raft_command_t)) {
                close(client_fd); continue;
            }
            raft_command_t cmd;
            if (recv_full(client_fd, &cmd, sizeof(cmd)) < 0) {
                close(client_fd); continue;
            }
            close(client_fd);
            pthread_mutex_lock(&tcp->lock);
            if (tcp->fwd_cmd_count < RAFT_MAX_QUEUE)
                tcp->fwd_cmd_queue[tcp->fwd_cmd_count++] = cmd;
            pthread_mutex_unlock(&tcp->lock);
            continue;
        }

        /* TCP_TYPE_RAFT */
        if (header.type != TCP_TYPE_RAFT ||
            header.size != (uint32_t)sizeof(raft_message_t)) {
            close(client_fd); continue;
        }

        raft_message_t msg;
        if (recv_full(client_fd, &msg, sizeof(msg)) < 0) {
            close(client_fd);
            continue;
        }
        close(client_fd);

        if (!tcp->enabled) continue;

        pthread_mutex_lock(&tcp->lock);
        if (tcp->in_count < RAFT_MAX_QUEUE)
            tcp->in_queue[tcp->in_count++] = msg;
        pthread_mutex_unlock(&tcp->lock);
    }
    return NULL;
}

/* ── Vtable functions ────────────────────────────────────────────────────── */

static int tcp_send(void *ctx, const raft_message_t *msg) {
    raft_tcp_transport_t *tcp = (raft_tcp_transport_t *)ctx;
    if (!tcp->enabled) return 0;

    const char *host = NULL;
    int port = -1;
    size_t i;
    for (i = 0; i < tcp->peer_count; ++i) {
        if (strcmp(tcp->peers[i].id, msg->target) == 0) {
            host = tcp->peers[i].host;
            port = tcp->peers[i].port;
            break;
        }
    }
    if (!host) return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(fd); return -1; }

    raft_tcp_frame_t frame;
    frame.magic = TCP_MAGIC;
    frame.type  = TCP_TYPE_RAFT;
    frame.size  = (uint32_t)sizeof(raft_message_t);

    if (send_full(fd, &frame, sizeof(frame)) < 0 ||
        send_full(fd, msg,   sizeof(*msg))   < 0) {
        close(fd); return -1;
    }
    close(fd);
    return 0;
}

static size_t tcp_recv(void *ctx, raft_message_t *out, size_t capacity) {
    raft_tcp_transport_t *tcp = (raft_tcp_transport_t *)ctx;
    size_t count;

    pthread_mutex_lock(&tcp->lock);
    count = tcp->in_count < capacity ? tcp->in_count : capacity;
    if (count > 0) {
        size_t i;
        for (i = 0; i < count; ++i) out[i] = tcp->in_queue[i];
        tcp->in_count = 0;
    }
    pthread_mutex_unlock(&tcp->lock);
    return count;
}

static int tcp_submit_command(void *ctx, const raft_command_t *cmd) {
    raft_tcp_transport_t *tcp = (raft_tcp_transport_t *)ctx;
    if (tcp->cmd_count >= RAFT_MAX_QUEUE) return -1;
    tcp->cmd_queue[tcp->cmd_count++] = *cmd;
    return 0;
}

static size_t tcp_recv_commands(void *ctx, raft_command_t *out, size_t capacity) {
    raft_tcp_transport_t *tcp = (raft_tcp_transport_t *)ctx;
    size_t count = tcp->cmd_count < capacity ? tcp->cmd_count : capacity;
    size_t i;
    for (i = 0; i < count; ++i) out[i] = tcp->cmd_queue[i];
    tcp->cmd_count = 0;
    return count;
}

static void tcp_set_enabled(void *ctx, int enabled) {
    raft_tcp_transport_t *tcp = (raft_tcp_transport_t *)ctx;
    tcp->enabled = enabled;
    if (!enabled) {
        pthread_mutex_lock(&tcp->lock);
        tcp->in_count = 0;
        pthread_mutex_unlock(&tcp->lock);
        tcp->cmd_count = 0;
    }
}

/* ── Peer persistence ───────────────────────────────────────────────────── */

static void tcp_mkdir_p(const char *path) {
    char tmp[512];
    char *p;
    strncpy(tmp, path, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0777);
            *p = '/';
        }
    }
    mkdir(tmp, 0777);
}

static void tcp_peers_path(const raft_tcp_transport_t *tcp, char *out, size_t sz) {
    snprintf(out, sz, "%s/peers.txt", tcp->data_dir);
}

static void tcp_save_self(const raft_tcp_transport_t *tcp) {
    char path[576];
    FILE *f;
    if (!tcp->data_dir[0] || !tcp->own_host[0] || tcp->listen_port <= 0) return;
    tcp_mkdir_p(tcp->data_dir);
    snprintf(path, sizeof(path), "%s/self.txt", tcp->data_dir);
    f = fopen(path, "w");
    if (!f) return;
    fprintf(f, "%s %d\n", tcp->own_host, tcp->listen_port);
    fclose(f);
}

static void tcp_load_self(raft_tcp_transport_t *tcp) {
    char path[576];
    FILE *f;
    char host[256];
    int  port;
    if (!tcp->data_dir[0]) return;
    snprintf(path, sizeof(path), "%s/self.txt", tcp->data_dir);
    f = fopen(path, "r");
    if (!f) return;
    if (fscanf(f, "%255s %d", host, &port) == 2) {
        if (tcp->listen_port <= 0) tcp->listen_port = port;
        if (!tcp->own_host[0]) strncpy(tcp->own_host, host, sizeof(tcp->own_host) - 1);
    }
    fclose(f);
}

static void tcp_save_peers(const raft_tcp_transport_t *tcp) {
    char path[576];
    FILE *f;
    size_t i;
    if (!tcp->data_dir[0]) return;
    tcp_mkdir_p(tcp->data_dir);
    tcp_peers_path(tcp, path, sizeof(path));
    f = fopen(path, "w");
    if (!f) return;
    for (i = 0; i < tcp->peer_count; ++i)
        fprintf(f, "%s %s %d\n",
                tcp->peers[i].id, tcp->peers[i].host, tcp->peers[i].port);
    fclose(f);
}

static void tcp_load_peers(raft_tcp_transport_t *tcp) {
    char path[576];
    FILE *f;
    char id[RAFT_MAX_ID], host[256];
    int  port;
    if (!tcp->data_dir[0]) return;
    tcp_peers_path(tcp, path, sizeof(path));
    f = fopen(path, "r");
    if (!f) return;
    while (fscanf(f, "%15s %255s %d", id, host, &port) == 3)
        raft_tcp_transport_add_peer(tcp, id, host, port);
    fclose(f);
}

/* ── Public API ─────────────────────────────────────────────────────────── */

void raft_tcp_transport_init(raft_tcp_transport_t *tcp, int port) {
    memset(tcp, 0, sizeof(*tcp));
    tcp->listen_port = port;
    tcp->listen_fd   = -1;
    tcp->enabled     = 1;
    pthread_mutex_init(&tcp->lock, NULL);
}

void raft_tcp_transport_add_peer(raft_tcp_transport_t *tcp,
                                  const char *id, const char *host, int port) {
    raft_tcp_peer_t *p;
    size_t i;
    for (i = 0; i < tcp->peer_count; ++i) {
        if (strcmp(tcp->peers[i].id, id) == 0) {
            p = &tcp->peers[i];
            strncpy(p->host, host, sizeof(p->host) - 1);
            p->port = port;
            tcp_save_peers(tcp);
            return;
        }
    }
    if (tcp->peer_count >= RAFT_MAX_PEERS) return;
    p = &tcp->peers[tcp->peer_count++];
    strncpy(p->id,   id,   sizeof(p->id)   - 1);
    strncpy(p->host, host, sizeof(p->host) - 1);
    p->port = port;
    tcp_save_peers(tcp);
}

void raft_tcp_transport_set_data_dir(raft_tcp_transport_t *tcp, const char *dir) {
    strncpy(tcp->data_dir, dir, sizeof(tcp->data_dir) - 1);
    tcp->data_dir[sizeof(tcp->data_dir) - 1] = '\0';
    tcp_load_self(tcp);
    tcp_load_peers(tcp);
}

int raft_tcp_transport_start(raft_tcp_transport_t *tcp) {
    int opt = 1;
    struct sockaddr_in addr;
    if (tcp->listener_started) return 0;
    if (tcp->listen_port <= 0) return -1;
    tcp_save_self(tcp);

    tcp->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp->listen_fd < 0) return -1;

    setsockopt(tcp->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)tcp->listen_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(tcp->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(tcp->listen_fd); tcp->listen_fd = -1; return -1;
    }
    if (listen(tcp->listen_fd, 8) < 0) {
        close(tcp->listen_fd); tcp->listen_fd = -1; return -1;
    }
    tcp->shutdown = 0;
    if (pthread_create(&tcp->listener_thread, NULL, tcp_listener_thread, tcp) != 0) {
        close(tcp->listen_fd); tcp->listen_fd = -1; return -1;
    }
    tcp->listener_started = 1;
    return 0;
}

int raft_tcp_transport_listen(raft_tcp_transport_t *tcp, int port) {
    if (port <= 0) return -1;
    if (tcp->listener_started) return -1;
    tcp->listen_port = port;
    return raft_tcp_transport_start(tcp);
}

int raft_tcp_transport_is_listening(const raft_tcp_transport_t *tcp) {
    return tcp->listener_started;
}

void raft_tcp_transport_stop(raft_tcp_transport_t *tcp) {
    tcp->shutdown = 1;
    if (tcp->listen_fd >= 0) { close(tcp->listen_fd); tcp->listen_fd = -1; }
    if (tcp->listener_started) {
        pthread_join(tcp->listener_thread, NULL);
        tcp->listener_started = 0;
    }
    pthread_mutex_destroy(&tcp->lock);
}

raft_transport_t raft_tcp_transport_make(raft_tcp_transport_t *tcp) {
    raft_transport_t t;
    t.send           = tcp_send;
    t.recv           = tcp_recv;
    t.submit_command = tcp_submit_command;
    t.recv_commands  = tcp_recv_commands;
    t.set_enabled    = tcp_set_enabled;
    t.ctx            = tcp;
    return t;
}

int raft_tcp_transport_request_join(raft_tcp_transport_t *tcp,
                                     const char *my_id,
                                     const char *my_host, int my_port,
                                     const char *target_host, int target_port) {
    raft_tcp_join_req_t jr;
    (void)tcp;
    memset(&jr, 0, sizeof(jr));
    strncpy(jr.node_id, my_id,   RAFT_MAX_ID - 1);
    strncpy(jr.host,    my_host, sizeof(jr.host) - 1);
    jr.port      = my_port;
    jr.forwarded = 0;
    return tcp_send_join(target_host, target_port, &jr);
}

int raft_tcp_transport_request_cluster_join(raft_tcp_transport_t *tcp,
                                             const raft_tcp_peer_t *members,
                                             size_t member_count,
                                             const char *target_host,
                                             int target_port) {
    return raft_tcp_transport_send_cluster_join(tcp, members, member_count,
                                                target_host, target_port,
                                                RAFT_JOIN_ORIGINAL);
}

int raft_tcp_transport_send_cluster_join(raft_tcp_transport_t *tcp,
                                          const raft_tcp_peer_t *members,
                                          size_t member_count,
                                          const char *target_host,
                                          int target_port,
                                          int forwarded) {
    raft_tcp_join_req_t jr;
    size_t count = member_count < RAFT_MAX_NODES ? member_count : RAFT_MAX_NODES;
    memset(&jr, 0, sizeof(jr));
    if (!tcp->own_id[0] || !tcp->own_host[0] || tcp->listen_port <= 0) return -1;
    strncpy(jr.node_id, tcp->own_id,   RAFT_MAX_ID - 1);
    strncpy(jr.host,    tcp->own_host, sizeof(jr.host) - 1);
    jr.port         = tcp->listen_port;
    jr.forwarded    = forwarded;
    jr.cluster_join = 1;
    jr.member_count = count;
    if (count > 0) memcpy(jr.members, members, count * sizeof(jr.members[0]));
    return tcp_send_join(target_host, target_port, &jr);
}

void raft_tcp_transport_flood_join(raft_tcp_transport_t *tcp,
                                    const raft_tcp_join_req_t *jr) {
    raft_tcp_join_req_t fwd = *jr;
    size_t i;
    fwd.forwarded = 1;
    for (i = 0; i < tcp->peer_count; ++i)
        tcp_send_join(tcp->peers[i].host, tcp->peers[i].port, &fwd);
}

size_t raft_tcp_transport_recv_joins(raft_tcp_transport_t *tcp,
                                      raft_tcp_join_req_t  *out,
                                      size_t capacity) {
    size_t count;
    pthread_mutex_lock(&tcp->lock);
    count = tcp->join_count < capacity ? tcp->join_count : capacity;
    if (count > 0) {
        size_t i;
        for (i = 0; i < count; ++i) out[i] = tcp->join_queue[i];
        tcp->join_count = 0;
    }
    pthread_mutex_unlock(&tcp->lock);
    return count;
}

int raft_tcp_transport_forward_cmd(raft_tcp_transport_t *tcp,
                                    const char *target_host, int target_port,
                                    const raft_command_t *cmd) {
    raft_tcp_frame_t frame;
    int fd;
    struct sockaddr_in addr;
    (void)tcp;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)target_port);
    if (inet_pton(AF_INET, target_host, &addr.sin_addr) <= 0) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(fd); return -1; }

    frame.magic = TCP_MAGIC;
    frame.type  = TCP_TYPE_CMD;
    frame.size  = (uint32_t)sizeof(raft_command_t);

    if (send_full(fd, &frame, sizeof(frame)) < 0 ||
        send_full(fd, cmd,   sizeof(*cmd))   < 0) {
        close(fd); return -1;
    }
    close(fd);
    return 0;
}

size_t raft_tcp_transport_recv_fwd_cmds(raft_tcp_transport_t *tcp,
                                         raft_command_t *out, size_t capacity) {
    size_t count;
    pthread_mutex_lock(&tcp->lock);
    count = tcp->fwd_cmd_count < capacity ? tcp->fwd_cmd_count : capacity;
    if (count > 0) {
        size_t i;
        for (i = 0; i < count; ++i) out[i] = tcp->fwd_cmd_queue[i];
        tcp->fwd_cmd_count = 0;
    }
    pthread_mutex_unlock(&tcp->lock);
    return count;
}

void raft_tcp_transport_set_self(raft_tcp_transport_t *tcp,
                                  const char *node_id, const char *host) {
    strncpy(tcp->own_id,   node_id, RAFT_MAX_ID - 1);
    strncpy(tcp->own_host, host,    sizeof(tcp->own_host) - 1);
}

int raft_tcp_transport_announce_self(raft_tcp_transport_t *tcp,
                                      const char *target_host, int target_port) {
    raft_tcp_join_req_t jr;
    if (!tcp->own_id[0]) return 0;
    memset(&jr, 0, sizeof(jr));
    strncpy(jr.node_id, tcp->own_id,   RAFT_MAX_ID - 1);
    strncpy(jr.host,    tcp->own_host, sizeof(jr.host) - 1);
    jr.port      = tcp->listen_port;
    jr.forwarded = RAFT_JOIN_ANNOUNCE;
    return tcp_send_join(target_host, target_port, &jr);
}
