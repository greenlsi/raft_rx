#include "cli_fsm.h"

#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <unistd.h>

/* ── Helpers ─────────────────────────────────────────────────────────────── */

static int find_leader_address(const cli_user_t *cli, char *host, int *port) {
    size_t i;
    const char *lid = cli->node->leader_id;
    if (!lid[0]) return -1;
    for (i = 0; i < cli->tcp->peer_count; ++i)
        if (strcmp(cli->tcp->peers[i].id, lid) == 0) {
            strncpy(host, cli->tcp->peers[i].host, 255);
            host[255] = '\0';
            *port = cli->tcp->peers[i].port;
            return 0;
        }
    return -1;
}

static void forward_to_leader(cli_user_t *cli, const raft_command_t *cmd) {
    char host[256];
    int  port;
    if (find_leader_address(cli, host, &port) < 0) {
        printf("leader unknown — cannot forward\n");
        return;
    }
    if (raft_tcp_transport_forward_cmd(cli->tcp, host, port, cmd) < 0)
        printf("forward failed\n");
}

static const char *role_name(int state) {
    switch (state) {
        case RAFT_ROLE_LEADER:    return "LEADER";
        case RAFT_ROLE_CANDIDATE: return "CANDIDATE";
        default:                  return "FOLLOWER";
    }
}

static void print_status(const cli_user_t *cli) {
    const raft_node_t *n = cli->node;
    int is_leader = (n->machine.state == RAFT_ROLE_LEADER);
    size_t i;

    printf("id=%-4s  running=%-3s  role=%-9s  term=%-3d  commit=%-3d  log=%-3d  leader=%s\n",
           n->config.node_id,
           n->running ? "yes" : "no",
           role_name(n->machine.state),
           n->current_term,
           n->commit_index,
           (int)n->log_count,
           n->leader_id[0] ? n->leader_id : "none");

    if (n->config.peer_count == 0) return;

    for (i = 0; i < n->config.peer_count; ++i) {
        if (is_leader) {
            int match = n->match_index[i];
            int behind = n->commit_index - match;
            printf("  %-4s  match=%-3d  %s\n",
                   n->config.peers[i],
                   match,
                   behind <= 0 ? "in sync" : "behind");
        } else {
            printf("  %-4s\n", n->config.peers[i]);
        }
    }
}

/* ── CLI commands ────────────────────────────────────────────────────────── */

static void cmd_leader(const cli_user_t *cli) {
    if (cli->node->machine.state == RAFT_ROLE_LEADER)
        printf("%s (this node)\n", cli->node->config.node_id);
    else if (cli->node->leader_id[0])
        puts(cli->node->leader_id);
    else
        puts("unknown");
}

static void cmd_set(cli_user_t *cli, const char *key, const char *value) {
    raft_command_t cmd;
    memset(&cmd, 0, sizeof(cmd));
    strncpy(cmd.op,    "set",  sizeof(cmd.op)    - 1);
    strncpy(cmd.key,   key,    sizeof(cmd.key)   - 1);
    strncpy(cmd.value, value,  sizeof(cmd.value) - 1);
    if (cli->node->machine.state == RAFT_ROLE_LEADER) {
        raft_node_submit_command(cli->node, &cmd);
        puts("queued");
    } else {
        forward_to_leader(cli, &cmd);
    }
}

static void cmd_get(const cli_user_t *cli, const char *key) {
    const char *v = raft_kv_get(cli->kv, key);
    printf("%s\n", v ? v : "(nil)");
}

static void cmd_delete(cli_user_t *cli, const char *key) {
    raft_command_t cmd;
    memset(&cmd, 0, sizeof(cmd));
    strncpy(cmd.op,  "delete", sizeof(cmd.op)  - 1);
    strncpy(cmd.key, key,      sizeof(cmd.key) - 1);
    if (cli->node->machine.state == RAFT_ROLE_LEADER) {
        raft_node_submit_command(cli->node, &cmd);
        puts("queued");
    } else {
        forward_to_leader(cli, &cmd);
    }
}

static void cmd_stop(cli_user_t *cli) {
    raft_node_stop(cli->node); puts("stopped"); print_status(cli);
}

static void cmd_start(cli_user_t *cli) {
    raft_node_start(cli->node); puts("started"); print_status(cli);
}

static void cmd_members(const cli_user_t *cli) {
    const raft_node_t *n = cli->node;
    size_t j;
    printf("mode=%s old=[", n->config_state.new_count ? "joint" : "stable");
    for (j = 0; j < n->config_state.old_count; ++j)
        printf("%s%s", n->config_state.old_members[j],
               j + 1 < n->config_state.old_count ? "," : "");
    if (n->config_state.new_count) {
        printf("] new=[");
        for (j = 0; j < n->config_state.new_count; ++j)
            printf("%s%s", n->config_state.new_members[j],
                   j + 1 < n->config_state.new_count ? "," : "");
    }
    puts("]");
}

static void cmd_log(const cli_user_t *cli) {
    const raft_node_t *n = cli->node;
    size_t i;
    printf("term=%d role=%s log=%zu snapshot=%d commit=%d applied=%d\n",
           n->current_term, role_name(n->machine.state),
           n->log_count, n->snapshot_last_included_index,
           n->commit_index, n->last_applied);
    if (n->log_count == 0) { puts("(empty)"); return; }
    for (i = 0; i < n->log_count; ++i) {
        const raft_log_entry_t *e = &n->log[i];
        printf("[%d] %s term=%d op=%s key=%s value=%s\n",
               e->index,
               e->index <= n->commit_index ? "COMMITTED  " : "UNCOMMITTED",
               e->term, e->command.op, e->command.key, e->command.value);
    }
}

static void cmd_addnode(cli_user_t *cli, const char *node_id) {
    char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t count = 0, i;
    if (cli->node->machine.state != RAFT_ROLE_LEADER) {
        raft_command_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        strncpy(cmd.op,  "cluster.fwd_add", sizeof(cmd.op)  - 1);
        strncpy(cmd.key, node_id,           sizeof(cmd.key) - 1);
        forward_to_leader(cli, &cmd);
        return;
    }
    for (i = 0; i < cli->node->config_state.old_count; ++i)
        strncpy(new_members[count++], cli->node->config_state.old_members[i], RAFT_MAX_ID - 1);
    for (i = 0; i < count; ++i)
        if (strcmp(new_members[i], node_id) == 0) {
            printf("already a member: %s\n", node_id); return;
        }
    if (count >= RAFT_MAX_NODES) { puts("cluster full"); return; }
    strncpy(new_members[count++], node_id, RAFT_MAX_ID - 1);
    raft_node_request_membership_change(cli->node, new_members, count);
    printf("requested addnode=%s\n", node_id);
}

static void cmd_rmnode(cli_user_t *cli, const char *node_id) {
    char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t count = 0, i;
    if (cli->node->machine.state != RAFT_ROLE_LEADER) {
        raft_command_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        strncpy(cmd.op,  "cluster.fwd_rm", sizeof(cmd.op)  - 1);
        strncpy(cmd.key, node_id,          sizeof(cmd.key) - 1);
        forward_to_leader(cli, &cmd);
        return;
    }
    for (i = 0; i < cli->node->config_state.old_count; ++i)
        if (strcmp(cli->node->config_state.old_members[i], node_id) != 0)
            strncpy(new_members[count++], cli->node->config_state.old_members[i], RAFT_MAX_ID - 1);
    if (count == cli->node->config_state.old_count) {
        printf("not a member: %s\n", node_id); return;
    }
    raft_node_request_membership_change(cli->node, new_members, count);
    printf("requested rmnode=%s\n", node_id);
}

static void cmd_leave(cli_user_t *cli) {
    char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t count = 0, i;
    if (cli->node->machine.state != RAFT_ROLE_LEADER) {
        raft_command_t cmd;
        memset(&cmd, 0, sizeof(cmd));
        strncpy(cmd.op,  "cluster.fwd_leave",        sizeof(cmd.op)  - 1);
        strncpy(cmd.key, cli->node->config.node_id,  sizeof(cmd.key) - 1);
        forward_to_leader(cli, &cmd);
        cli->leave_pending = 1;
        return;
    }
    for (i = 0; i < cli->node->config_state.old_count; ++i)
        if (strcmp(cli->node->config_state.old_members[i], cli->node->config.node_id) != 0)
            strncpy(new_members[count++], cli->node->config_state.old_members[i], RAFT_MAX_ID - 1);
    raft_node_request_membership_change(cli->node, new_members, count);
    cli->leave_pending = 1;
    puts("leaving cluster...");
}

static void cmd_help(void) {
    puts("Commands:");
    puts("  set    KEY VALUE   enqueue set(KEY,VALUE) — leader only");
    puts("  get    KEY         read KEY from local KV state");
    puts("  delete KEY         enqueue delete(KEY) — leader only");
    puts("  status             show local node status");
    puts("  leader             show who is leader");
    puts("  stop               simulate local node crash");
    puts("  start              simulate local node restart");
    puts("  members            show membership config");
    puts("  log                show Raft log");
    puts("  addnode NODE       add NODE to cluster — leader only");
    puts("  rmnode  NODE       remove NODE from cluster — leader only");
    puts("  leave              remove this node from cluster — leader only");
    puts("  help               this message");
    puts("  quit               exit");
}

static void dispatch(cli_user_t *cli, char *line) {
    char *nl = strchr(line, '\n');
    if (nl) *nl = '\0';

    char *tok[4] = {NULL, NULL, NULL, NULL};
    int   n = 0;
    char *p = line;
    while (n < 4 && (tok[n] = strtok(p, " \t")) != NULL) { p = NULL; ++n; }
    if (n == 0) return;

    const char *cmd = tok[0];
    if      (strcmp(cmd, "set")     == 0) { if (n<3) puts("usage: set KEY VALUE"); else cmd_set(cli, tok[1], tok[2]);
    } else if (strcmp(cmd, "get")     == 0) { if (n<2) puts("usage: get KEY");       else cmd_get(cli, tok[1]);
    } else if (strcmp(cmd, "delete")  == 0) { if (n<2) puts("usage: delete KEY");    else cmd_delete(cli, tok[1]);
    } else if (strcmp(cmd, "status")  == 0) { print_status(cli);
    } else if (strcmp(cmd, "leader")  == 0) { cmd_leader(cli);
    } else if (strcmp(cmd, "stop")    == 0) { cmd_stop(cli);
    } else if (strcmp(cmd, "start")   == 0) { cmd_start(cli);
    } else if (strcmp(cmd, "members") == 0) { cmd_members(cli);
    } else if (strcmp(cmd, "log")     == 0) { cmd_log(cli);
    } else if (strcmp(cmd, "addnode") == 0) { if (n<2) puts("usage: addnode NODE");  else cmd_addnode(cli, tok[1]);
    } else if (strcmp(cmd, "rmnode")  == 0) { if (n<2) puts("usage: rmnode NODE");   else cmd_rmnode(cli, tok[1]);
    } else if (strcmp(cmd, "leave")   == 0) { cmd_leave(cli);
    } else if (strcmp(cmd, "help")    == 0) { cmd_help();
    } else if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        cli->quit_requested = 1;
    } else {
        printf("unknown: %s  (type 'help')\n", cmd);
    }
}

/* ── FSM phase callbacks ─────────────────────────────────────────────────── */

void cli_latch_inputs(rx_fsm_context *ctx, void *user) {
    cli_user_t *cli = (cli_user_t *)user;
    (void)ctx;

    cli->has_line = 0;
    {
        fd_set rfds;
        struct timeval tv = {0, 0};
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        if (select(STDIN_FILENO + 1, &rfds, NULL, NULL, &tv) > 0) {
            if (fgets(cli->line, sizeof(cli->line), stdin))
                cli->has_line = 1;
            else
                cli->quit_requested = 1;
        }
    }

    /* Drain forwarded commands from followers */
    {
        size_t n = raft_tcp_transport_recv_fwd_cmds(cli->tcp,
                       cli->fwd_cmds + cli->fwd_cmd_count,
                       RAFT_MAX_QUEUE - cli->fwd_cmd_count);
        cli->fwd_cmd_count += n;
    }

    /* Accumulate incoming join frames (deduplicate by node_id) */
    {
        raft_tcp_join_req_t incoming[RAFT_MAX_NODES];
        size_t i, j;
        size_t n = raft_tcp_transport_recv_joins(cli->tcp, incoming, RAFT_MAX_NODES);
        for (i = 0; i < n; ++i) {
            int dup = 0;
            for (j = 0; j < cli->pending_join_count; ++j)
                if (strcmp(cli->pending_joins[j].node_id, incoming[i].node_id) == 0 &&
                    cli->pending_joins[j].forwarded == incoming[i].forwarded) {
                    dup = 1; break;
                }
            if (!dup && cli->pending_join_count < RAFT_MAX_NODES)
                cli->pending_joins[cli->pending_join_count++] = incoming[i];
        }
    }
}

static int cli_guard_has_line(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    return ((cli_user_t *)user)->has_line;
}

static void cli_action_execute(rx_fsm_context *ctx, void *user) {
    cli_user_t *cli = (cli_user_t *)user;
    (void)ctx;
    dispatch(cli, cli->line);
    cli->prompt_needed = 1;
}

/*
 * dump_outputs — output / side-effect phase.
 *
 * Join-request processing (three cases by forwarded value):
 *
 *  ANNOUNCE (2)  — an existing node introduced itself; just add it as a
 *                  TCP peer so we can route responses to it, then discard.
 *
 *  ORIGINAL (0)  — a new node wants to join; flood the request to all our
 *                  peers (so they add the new node as TCP peer too), and
 *                  announce ourselves back to the new node.
 *
 *  FORWARDED (1) — we received the flooded copy; announce ourselves to the
 *                  new node so it can reach us.
 *
 *  PENDING (99)  — one-time steps already done; keep in queue until we are
 *                  the leader and can apply the membership change.
 */
void cli_dump_outputs(rx_fsm_context *ctx, void *user) {
    cli_user_t *cli = (cli_user_t *)user;
    size_t i = 0;
    (void)ctx;

    if (cli->leave_pending && !cli->node->running)
        cli->quit_requested = 1;

    if (cli->quit_requested) {
        rx_coop_exec_stop(cli->coop);
        return;
    }

    /* Log role / term / vote transitions */
    {
        int         role       = cli->node->machine.state;
        int         term       = cli->node->current_term;
        const char *voted_for  = cli->node->voted_for;
        int         vote_count = cli->node->vote_count;

        if (role != cli->last_role || term != cli->last_term) {
            fprintf(stderr, "\n[%s] %s→%s  term=%d  commit=%d\n",
                    cli->node_id,
                    role_name(cli->last_role),
                    role_name(role),
                    term,
                    cli->node->commit_index);
            cli->last_role = role;
            cli->last_term = term;
            cli->prompt_needed = 1;
        }
        if (strcmp(voted_for, cli->last_voted_for) != 0) {
            fprintf(stderr, "[%s] voted_for: \"%s\" → \"%s\"  term=%d\n",
                    cli->node_id,
                    cli->last_voted_for[0] ? cli->last_voted_for : "(none)",
                    voted_for[0]           ? voted_for           : "(none)",
                    term);
            strncpy(cli->last_voted_for, voted_for, RAFT_MAX_ID - 1);
            cli->last_voted_for[RAFT_MAX_ID - 1] = '\0';
            cli->prompt_needed = 1;
        }
        if (role == RAFT_ROLE_CANDIDATE && vote_count != cli->last_vote_count) {
            fprintf(stderr, "[%s] vote_count: %d → %d  term=%d\n",
                    cli->node_id,
                    cli->last_vote_count,
                    vote_count,
                    term);
            cli->last_vote_count = vote_count;
            cli->prompt_needed = 1;
        } else if (role != RAFT_ROLE_CANDIDATE) {
            cli->last_vote_count = 0;
        }
    }

    while (i < cli->pending_join_count) {
        raft_tcp_join_req_t *jr = &cli->pending_joins[i];

        if (jr->forwarded == RAFT_JOIN_ANNOUNCE) {
            /* Peer self-announcement: add as TCP peer and discard */
            size_t k;
            int known = 0;
            for (k = 0; k < cli->tcp->peer_count; ++k)
                if (strcmp(cli->tcp->peers[k].id, jr->node_id) == 0) { known = 1; break; }
            if (!known)
                raft_tcp_transport_add_peer(cli->tcp, jr->node_id, jr->host, jr->port);
            cli->pending_joins[i] = cli->pending_joins[--cli->pending_join_count];
            continue;
        }

        /* join request (ORIGINAL, FORWARDED, or PENDING) */

        if (jr->forwarded == RAFT_JOIN_ORIGINAL) {
            /* Flood to all existing peers so they learn the joining node */
            jr->forwarded = RAFT_JOIN_FORWARDED;
            raft_tcp_transport_flood_join(cli->tcp, jr);
            /* Announce ourselves to the joining node so it can reply to us */
            raft_tcp_transport_announce_self(cli->tcp, jr->host, jr->port);
            jr->forwarded = RAFT_JOIN_PENDING;
        } else if (jr->forwarded == RAFT_JOIN_FORWARDED) {
            /* Announce ourselves to the joining node */
            raft_tcp_transport_announce_self(cli->tcp, jr->host, jr->port);
            jr->forwarded = RAFT_JOIN_PENDING;
        }

        /* Idempotently register the joining node as a TCP peer */
        {
            size_t k;
            int known = 0;
            for (k = 0; k < cli->tcp->peer_count; ++k)
                if (strcmp(cli->tcp->peers[k].id, jr->node_id) == 0) { known = 1; break; }
            if (!known)
                raft_tcp_transport_add_peer(cli->tcp, jr->node_id, jr->host, jr->port);
        }

        /* Discard if this node is already in the cluster (stable or joint) */
        {
            size_t k;
            int already = 0;
            for (k = 0; k < cli->node->config_state.old_count && !already; ++k)
                if (strcmp(cli->node->config_state.old_members[k], jr->node_id) == 0)
                    already = 1;
            for (k = 0; k < cli->node->config_state.new_count && !already; ++k)
                if (strcmp(cli->node->config_state.new_members[k], jr->node_id) == 0)
                    already = 1;
            if (already) {
                cli->pending_joins[i] = cli->pending_joins[--cli->pending_join_count];
                continue;
            }
        }

        /* Only the leader applies the membership change */
        if (cli->node->machine.state == RAFT_ROLE_LEADER) {
            char new_members[RAFT_MAX_NODES][RAFT_MAX_ID];
            size_t count = 0, k;
            int already = 0;
            for (k = 0; k < cli->node->config_state.old_count; ++k) {
                strncpy(new_members[count++], cli->node->config_state.old_members[k], RAFT_MAX_ID - 1);
                if (strcmp(cli->node->config_state.old_members[k], jr->node_id) == 0) already = 1;
            }
            if (!already && count < RAFT_MAX_NODES) {
                strncpy(new_members[count++], jr->node_id, RAFT_MAX_ID - 1);
                raft_node_request_membership_change(cli->node, new_members, count);
                printf("\n[cluster] join: %s\n", jr->node_id);
                cli->prompt_needed = 1;
            }
            cli->pending_joins[i] = cli->pending_joins[--cli->pending_join_count];
        } else {
            ++i;  /* keep pending until leader is elected */
        }
    }

    /* Process commands forwarded to us by followers */
    if (cli->fwd_cmd_count > 0) {
        if (cli->node->machine.state == RAFT_ROLE_LEADER) {
            size_t j;
            for (j = 0; j < cli->fwd_cmd_count; ++j) {
                const raft_command_t *c = &cli->fwd_cmds[j];
                if (strcmp(c->op, "cluster.fwd_add") == 0) {
                    char nm[RAFT_MAX_NODES][RAFT_MAX_ID];
                    size_t cnt = 0, k;
                    int already = 0;
                    for (k = 0; k < cli->node->config_state.old_count; ++k) {
                        strncpy(nm[cnt++], cli->node->config_state.old_members[k], RAFT_MAX_ID - 1);
                        if (strcmp(cli->node->config_state.old_members[k], c->key) == 0) already = 1;
                    }
                    if (!already && cnt < RAFT_MAX_NODES) {
                        strncpy(nm[cnt++], c->key, RAFT_MAX_ID - 1);
                        raft_node_request_membership_change(cli->node, nm, cnt);
                        printf("\n[fwd] addnode=%s\n", c->key);
                        cli->prompt_needed = 1;
                    }
                } else if (strcmp(c->op, "cluster.fwd_rm")    == 0 ||
                           strcmp(c->op, "cluster.fwd_leave") == 0) {
                    char nm[RAFT_MAX_NODES][RAFT_MAX_ID];
                    size_t cnt = 0, k;
                    for (k = 0; k < cli->node->config_state.old_count; ++k)
                        if (strcmp(cli->node->config_state.old_members[k], c->key) != 0)
                            strncpy(nm[cnt++], cli->node->config_state.old_members[k], RAFT_MAX_ID - 1);
                    if (cnt < cli->node->config_state.old_count) {
                        raft_node_request_membership_change(cli->node, nm, cnt);
                        printf("\n[fwd] rmnode=%s\n", c->key);
                        cli->prompt_needed = 1;
                    }
                } else {
                    raft_node_submit_command(cli->node, c);
                }
            }
        }
        cli->fwd_cmd_count = 0;
    }

    if (cli->prompt_needed) {
        printf("%s> ", cli->node_id);
        fflush(stdout);
        cli->prompt_needed = 0;
    }
}

/* ── Machine wiring ──────────────────────────────────────────────────────── */

static const rx_fsm_transition cli_transitions[] = {
    {CLI_READY, CLI_READY, cli_guard_has_line, cli_action_execute},
};

void cli_machine_init(rx_fsm_machine *machine, cli_user_t *user) {
    rx_fsm_machine_init(machine, "cli", CLI_READY,
                        cli_transitions,
                        sizeof(cli_transitions) / sizeof(cli_transitions[0]),
                        user,
                        cli_latch_inputs,
                        cli_dump_outputs);
}
