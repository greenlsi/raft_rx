// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include "raft/raft.h"
#include "raft/raft_tcp_transport.h"
#include "rxnet/coop.h"
#include "rxnet/fsm.h"

typedef struct {
    int value;
} counter_state_t;

typedef struct {
    char                  node_id[RAFT_MAX_ID];
    char                  line[256];
    int                   has_line;
    int                   prompt_needed;
    int                   quit_requested;
    int                   last_role;
    int                   last_term;
    raft_tcp_join_req_t   pending_joins[RAFT_MAX_NODES];
    size_t                pending_join_count;
    raft_command_t        fwd_cmds[RAFT_MAX_QUEUE];
    size_t                fwd_cmd_count;
    raft_node_t          *node;
    raft_tcp_transport_t *tcp;
    counter_state_t      *counter;
    rx_coop_exec         *coop;
} counter_cli_t;

void counter_cli_machine_init(rx_fsm_machine *machine, counter_cli_t *cli);
