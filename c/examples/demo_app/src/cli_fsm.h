#pragma once

#include "raft/raft.h"
#include "raft/raft_kv_app.h"
#include "raft/raft_tcp_transport.h"
#include "rxnet/coop.h"
#include "rxnet/fsm.h"

#define CLI_READY 0

/*
 * All state needed by the CLI FSM.  Back-pointer fields (node, tcp, kv,
 * coop) are set once by the caller before the machine starts running.
 */
typedef struct {
    char                  node_id[RAFT_MAX_ID];
    char                  line[512];
    int                   has_line;
    int                   prompt_needed;
    int                   quit_requested;
    int                   leave_pending;
    int                   last_role;
    int                   last_term;
    char                  last_voted_for[RAFT_MAX_ID];
    int                   last_vote_count;
    raft_tcp_join_req_t   pending_joins[RAFT_MAX_NODES];
    size_t                pending_join_count;
    raft_command_t        fwd_cmds[RAFT_MAX_QUEUE];
    size_t                fwd_cmd_count;
    /* back-references — set once at startup */
    raft_node_t          *node;
    raft_tcp_transport_t *tcp;
    raft_kv_state_t      *kv;
    rx_coop_exec         *coop;
} cli_user_t;

/*
 * Initialise *machine using *user as the user-data context.
 * All back-pointer fields in *user must already be set before calling this.
 */
void cli_machine_init(rx_fsm_machine *machine, cli_user_t *user);
