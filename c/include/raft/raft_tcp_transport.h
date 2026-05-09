// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-only

#pragma once

#include "raft/raft.h"

#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TCP_TYPE_RAFT 0u
#define TCP_TYPE_JOIN 1u
#define TCP_TYPE_CMD  3u  /* forwarded CLI command (follower → leader) */

/*
 * Values for raft_tcp_join_req_t.forwarded (on-wire values: 0–2 only).
 *
 *  0 ORIGINAL  : sent by the joining node to one introducer.
 *                Recipient must flood to all its peers (1) and announce
 *                itself back to the joiner (2).
 *  1 FORWARDED : flooded by the introducer to every existing member.
 *                Recipient must announce itself to the joiner (2) and
 *                add the joiner as a TCP peer.
 *  2 ANNOUNCE  : self-announcement from an existing member back to the
 *                joiner.  Recipient just adds the sender as a TCP peer.
 *
 * RAFT_JOIN_PENDING (99) is a local-only sentinel stored in the pending
 * queue after one-time steps (flood/announce) are done.  It is never
 * sent over the wire.
 */
#define RAFT_JOIN_ORIGINAL   0
#define RAFT_JOIN_FORWARDED  1
#define RAFT_JOIN_ANNOUNCE   2
#define RAFT_JOIN_PENDING   99

typedef struct {
    char id[RAFT_MAX_ID];
    char host[256];
    int  port;
} raft_tcp_peer_t;

typedef struct {
    char node_id[RAFT_MAX_ID];
    char host[256];
    int  port;
    int  forwarded;
    int  cluster_join;
    size_t member_count;
    raft_tcp_peer_t members[RAFT_MAX_NODES];
} raft_tcp_join_req_t;

typedef struct {
    int  listen_port;
    int  listen_fd;
    char own_id[RAFT_MAX_ID]; /* this node's ID, used for self-announcements */
    char own_host[256];       /* this node's advertised host */
    char data_dir[512];       /* node-specific data dir; empty = no persistence */

    raft_tcp_peer_t peers[RAFT_MAX_PEERS];
    size_t          peer_count;

    pthread_mutex_t lock;
    raft_message_t  in_queue[RAFT_MAX_QUEUE];
    size_t          in_count;

    raft_tcp_join_req_t join_queue[RAFT_MAX_NODES];
    size_t              join_count;

    raft_command_t cmd_queue[RAFT_MAX_QUEUE];
    size_t         cmd_count;

    raft_command_t fwd_cmd_queue[RAFT_MAX_QUEUE]; /* forwarded from followers */
    size_t         fwd_cmd_count;

    pthread_t       listener_thread;
    int             listener_started;
    int             enabled;
    volatile int    shutdown;
} raft_tcp_transport_t;

/*
 * Lifecycle
 *
 *  init          Zero-initialise and set the listen port (may be 0 if the
 *                port will be loaded from self.txt via set_data_dir).
 *  add_peer      Register a peer's address.  Deduplicates by id: if the same
 *                id already exists the address is updated.  Saves peers.txt.
 *  start         Start the listener thread and write self.txt.  Must be called
 *                after set_self and set_data_dir.
 *  listen        Start listening on a specific port (alternative to start).
 *  is_listening  Returns 1 if the listener thread is running.
 *  stop          Signal the listener thread to exit and join it.
 *  make          Build the raft_transport_t vtable backed by this tcp state.
 *                Assign the result to node->transport.
 */
void             raft_tcp_transport_init        (raft_tcp_transport_t *tcp, int port);
void             raft_tcp_transport_add_peer    (raft_tcp_transport_t *tcp,
                                                 const char *id, const char *host, int port);
int              raft_tcp_transport_start       (raft_tcp_transport_t *tcp);
int              raft_tcp_transport_listen      (raft_tcp_transport_t *tcp, int port);
int              raft_tcp_transport_is_listening(const raft_tcp_transport_t *tcp);
void             raft_tcp_transport_stop        (raft_tcp_transport_t *tcp);
raft_transport_t raft_tcp_transport_make        (raft_tcp_transport_t *tcp);

/*
 * Send a join-request frame to target_host:target_port announcing this node.
 * Used by the joining node once at startup (forwarded=0).
 */
int raft_tcp_transport_request_join(raft_tcp_transport_t *tcp,
                                     const char *my_id,
                                     const char *my_host, int my_port,
                                     const char *target_host, int target_port);
int raft_tcp_transport_request_cluster_join(raft_tcp_transport_t *tcp,
                                             const raft_tcp_peer_t *members,
                                             size_t member_count,
                                             const char *target_host,
                                             int target_port);
int raft_tcp_transport_send_cluster_join(raft_tcp_transport_t *tcp,
                                          const raft_tcp_peer_t *members,
                                          size_t member_count,
                                          const char *target_host,
                                          int target_port,
                                          int forwarded);

/*
 * Flood a join request to all configured peers (with forwarded=1 so they
 * don't re-forward it).  Used by the introducer node.
 */
void raft_tcp_transport_flood_join(raft_tcp_transport_t *tcp,
                                    const raft_tcp_join_req_t *jr);

/* Drain incoming join requests (call from main thread only). */
size_t raft_tcp_transport_recv_joins(raft_tcp_transport_t *tcp,
                                      raft_tcp_join_req_t  *out,
                                      size_t capacity);

/*
 * Forward a CLI command to target_host:target_port (typically the leader).
 * The recipient queues it in fwd_cmd_queue for its CLI FSM to process.
 */
int raft_tcp_transport_forward_cmd(raft_tcp_transport_t *tcp,
                                    const char *target_host, int target_port,
                                    const raft_command_t *cmd);

/* Drain the incoming forwarded-command queue (call from main thread only). */
size_t raft_tcp_transport_recv_fwd_cmds(raft_tcp_transport_t *tcp,
                                         raft_command_t *out, size_t capacity);

/*
 * set_self      Record this node's own id and advertised host so that
 *               self-announcement frames (sent to joining nodes) carry the
 *               correct address.  Must be called before start.
 *
 * set_data_dir  Set the node-specific data directory and immediately load:
 *                 {dir}/peers.txt — known peer addresses (id host port per line)
 *                 {dir}/self.txt  — own host and listen port
 *               Values from these files fill in fields that are still zero/empty
 *               (explicit --port / --host arguments take precedence).
 *               Call before start.  Subsequent add_peer calls auto-save peers.txt.
 */
void raft_tcp_transport_set_self    (raft_tcp_transport_t *tcp,
                                     const char *node_id, const char *host);
void raft_tcp_transport_set_data_dir(raft_tcp_transport_t *tcp, const char *dir);

/*
 * Send a RAFT_JOIN_ANNOUNCE frame to target_host:target_port so the
 * remote node learns this node's address (needed for reply routing).
 * No-op if set_self was never called.
 */
int raft_tcp_transport_announce_self(raft_tcp_transport_t *tcp,
                                      const char *target_host, int target_port);

#ifdef __cplusplus
}
#endif
