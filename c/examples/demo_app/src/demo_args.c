// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "demo_args.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(const char *argv0) {
    fprintf(stderr,
        "usage: %s --id NAME [--port PORT] [--host HOST] [--data DIR]\n"
        "          [--member ID] ... [--peer ID=HOST:PORT] ...\n"
        "          [--join HOST:PORT]\n",
        argv0);
    exit(1);
}

static int parse_host_port(const char *arg, char *host, size_t hostsz, int *port) {
    const char *colon = strrchr(arg, ':');
    if (!colon) return -1;
    {
        size_t hlen = (size_t)(colon - arg);
        if (hlen == 0 || hlen >= hostsz) return -1;
        strncpy(host, arg, hlen);
        host[hlen] = '\0';
    }
    *port = atoi(colon + 1);
    return (*port > 0) ? 0 : -1;
}

static int parse_peer(const char *arg, char *id, char *host, int *port) {
    const char *eq = strchr(arg, '=');
    if (!eq) return -1;
    {
        size_t id_len = (size_t)(eq - arg);
        if (id_len == 0 || id_len >= RAFT_MAX_ID) return -1;
        strncpy(id, arg, id_len);
        id[id_len] = '\0';
    }
    return parse_host_port(eq + 1, host, 256, port);
}

demo_args_t demo_args_parse(int argc, char **argv) {
    demo_args_t a;
    int i;
    memset(&a, 0, sizeof(a));
    strncpy(a.data_dir, DEMO_DEFAULT_DATA_DIR, sizeof(a.data_dir) - 1);
    strncpy(a.host,     DEMO_DEFAULT_HOST,     sizeof(a.host) - 1);

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            strncpy(a.node_id, argv[++i], RAFT_MAX_ID - 1);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            a.port = atoi(argv[++i]);
            a.port_supplied = 1;
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            strncpy(a.host, argv[++i], sizeof(a.host) - 1);
        } else if (strcmp(argv[i], "--data") == 0 && i + 1 < argc) {
            strncpy(a.data_dir, argv[++i], sizeof(a.data_dir) - 1);
        } else if (strcmp(argv[i], "--member") == 0 && i + 1 < argc) {
            if (a.member_count < RAFT_MAX_NODES)
                strncpy(a.initial_members[a.member_count++], argv[++i], RAFT_MAX_ID - 1);
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            if (a.peer_count < RAFT_MAX_PEERS) {
                int idx = a.peer_count;
                if (parse_peer(argv[++i], a.peer_ids[idx],
                               a.peer_hosts[idx], &a.peer_ports[idx]) == 0)
                    a.peer_count++;
                else {
                    fprintf(stderr, "bad --peer: %s\n", argv[i]);
                    exit(1);
                }
            }
        } else if (strcmp(argv[i], "--join") == 0 && i + 1 < argc) {
            if (parse_host_port(argv[++i], a.join_host, sizeof(a.join_host),
                                &a.join_port) < 0) {
                fprintf(stderr, "bad --join: %s (expected HOST:PORT)\n", argv[i]);
                exit(1);
            }
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            usage(argv[0]);
        }
    }

    if (!a.node_id[0]) {
        fprintf(stderr, "--id is required\n");
        usage(argv[0]);
    }
    if (a.join_port > 0 && (!a.port_supplied || a.port <= 0)) {
        fprintf(stderr, "--join requires --port so peers can reach this node\n");
        usage(argv[0]);
    }
    return a;
}
