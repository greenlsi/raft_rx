// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include "raft/raft.h"

#define DEMO_DEFAULT_DATA_DIR "var/raft"
#define DEMO_DEFAULT_HOST     "127.0.0.1"

typedef struct {
    char node_id[RAFT_MAX_ID];
    int  port;
    int  port_supplied;
    char host[256];
    char data_dir[256];

    char initial_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    int  member_count;

    char peer_ids  [RAFT_MAX_PEERS][RAFT_MAX_ID];
    char peer_hosts[RAFT_MAX_PEERS][256];
    int  peer_ports[RAFT_MAX_PEERS];
    int  peer_count;

    char join_host[256];
    int  join_port;      /* 0 = no --join */
} demo_args_t;

demo_args_t demo_args_parse(int argc, char **argv);
