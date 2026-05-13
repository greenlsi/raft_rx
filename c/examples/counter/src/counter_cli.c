// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "counter_cli.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <unistd.h>

#define CLI_READY 0

static const char *role_name(int state) {
    switch (state) {
        case RAFT_ROLE_LEADER:    return "LEADER";
        case RAFT_ROLE_CANDIDATE: return "CANDIDATE";
        default:                  return "FOLLOWER";
    }
}

static int parse_host_port(const char *arg, char *host, size_t hostsz, int *port) {
    const char *colon = strrchr(arg, ':');
    size_t hlen;
    if (!colon) return -1;
    hlen = (size_t)(colon - arg);
    if (hlen == 0 || hlen >= hostsz) return -1;
    strncpy(host, arg, hlen);
    host[hlen] = '\0';
    *port = atoi(colon + 1);
    return *port > 0 ? 0 : -1;
}

static int find_peer_address(const counter_cli_t *cli, const char *id, char *host, int *port) {
    size_t i;
    if (strcmp(id, cli->node->config.node_id) == 0) {
        strncpy(host, cli->tcp->own_host, 255);
        host[255] = '\0';
        *port = cli->tcp->listen_port;
        return 0;
    }
    for (i = 0; i < cli->tcp->peer_count; ++i) {
        if (strcmp(cli->tcp->peers[i].id, id) == 0) {
            strncpy(host, cli->tcp->peers[i].host, 255);
            host[255] = '\0';
            *port = cli->tcp->peers[i].port;
            return 0;
        }
    }
    return -1;
}

static int find_leader_address(const counter_cli_t *cli, char *host, int *port) {
    if (!cli->node->leader_id[0]) return -1;
    return find_peer_address(cli, cli->node->leader_id, host, port);
}

static int forward_to_leader(counter_cli_t *cli, const raft_command_t *cmd) {
    char host[256];
    int port;
    if (find_leader_address(cli, host, &port) != 0) {
        puts("leader unknown; retry later");
        return -1;
    }
    if (raft_tcp_transport_forward_cmd(cli->tcp, host, port, cmd) != 0) {
        puts("forward failed");
        return -1;
    }
    puts("forwarded");
    return 0;
}

static void submit_counter_command(counter_cli_t *cli, const char *op) {
    raft_command_t cmd;
    memset(&cmd, 0, sizeof(cmd));
    strncpy(cmd.op, op, sizeof(cmd.op) - 1);
    if (cli->node->machine.state == RAFT_ROLE_LEADER) {
        if (raft_node_submit_command(cli->node, &cmd) == 0) puts("queued");
        else puts("not a cluster member");
    } else {
        forward_to_leader(cli, &cmd);
    }
}

static void cmd_join(counter_cli_t *cli, const char *addr) {
    char host[256];
    int port;
    if (parse_host_port(addr, host, sizeof(host), &port) != 0) {
        puts("usage: join HOST:PORT");
        return;
    }
    raft_node_reset_for_join(cli->node);
    cli->counter->value = 0;
    if (raft_tcp_transport_request_join(cli->tcp,
                                        cli->node->config.node_id,
                                        cli->tcp->own_host,
                                        cli->tcp->listen_port,
                                        host, port) != 0) {
        puts("join failed");
        return;
    }
    puts("join requested");
}

static void print_status(const counter_cli_t *cli) {
    const raft_node_t *n = cli->node;
    size_t i;
    printf("id=%s role=%s term=%d commit=%d leader=%s value=%d members=[",
           n->config.node_id,
           role_name(n->machine.state),
           n->current_term,
           n->commit_index,
           n->leader_id[0] ? n->leader_id : "unknown",
           cli->counter->value);
    for (i = 0; i < n->config_state.old_count; ++i)
        printf("%s%s", i ? "," : "", n->config_state.old_members[i]);
    puts("]");
}

static void cmd_help(void) {
    puts("Commands:");
    puts("  inc              synchronized: value += 1");
    puts("  reset            synchronized: value = 0");
    puts("  get              local unsynchronized read");
    puts("  join HOST:PORT   join the cluster reachable at HOST:PORT");
    puts("  leader           show known leader");
    puts("  status           show local Raft status");
    puts("  help             this message");
    puts("  quit             exit");
}

static void dispatch(counter_cli_t *cli, char *line) {
    char *tok[3] = {NULL, NULL, NULL};
    char *nl = strchr(line, '\n');
    char *p = line;
    int n = 0;
    if (nl) *nl = '\0';
    while (n < 3 && (tok[n] = strtok(p, " \t")) != NULL) {
        p = NULL;
        ++n;
    }
    if (n == 0) return;

    if (strcmp(tok[0], "inc") == 0) {
        submit_counter_command(cli, "inc");
    } else if (strcmp(tok[0], "reset") == 0) {
        submit_counter_command(cli, "reset");
    } else if (strcmp(tok[0], "get") == 0) {
        printf("%d\n", cli->counter->value);
    } else if (strcmp(tok[0], "join") == 0) {
        if (n < 2) puts("usage: join HOST:PORT");
        else cmd_join(cli, tok[1]);
    } else if (strcmp(tok[0], "leader") == 0) {
        puts(cli->node->leader_id[0] ? cli->node->leader_id : "unknown");
    } else if (strcmp(tok[0], "status") == 0) {
        print_status(cli);
    } else if (strcmp(tok[0], "help") == 0) {
        cmd_help();
    } else if (strcmp(tok[0], "quit") == 0 || strcmp(tok[0], "exit") == 0) {
        cli->quit_requested = 1;
    } else {
        printf("unknown: %s (type 'help')\n", tok[0]);
    }
}

static void cli_latch_inputs(rx_fsm_context *ctx, void *user) {
    counter_cli_t *cli = (counter_cli_t *)user;
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

    cli->fwd_cmd_count += raft_tcp_transport_recv_fwd_cmds(
        cli->tcp,
        cli->fwd_cmds + cli->fwd_cmd_count,
        RAFT_MAX_QUEUE - cli->fwd_cmd_count);

    {
        raft_tcp_join_req_t incoming[RAFT_MAX_NODES];
        size_t i, j;
        size_t count = raft_tcp_transport_recv_joins(cli->tcp, incoming, RAFT_MAX_NODES);
        for (i = 0; i < count; ++i) {
            int duplicate = 0;
            for (j = 0; j < cli->pending_join_count; ++j) {
                if (strcmp(cli->pending_joins[j].node_id, incoming[i].node_id) == 0 &&
                    cli->pending_joins[j].forwarded == incoming[i].forwarded) {
                    duplicate = 1;
                    break;
                }
            }
            if (!duplicate && cli->pending_join_count < RAFT_MAX_NODES)
                cli->pending_joins[cli->pending_join_count++] = incoming[i];
        }
    }
}

static int cli_guard_has_line(const rx_fsm_context *ctx, void *user) {
    (void)ctx;
    return ((counter_cli_t *)user)->has_line;
}

static void cli_action_execute(rx_fsm_context *ctx, void *user) {
    counter_cli_t *cli = (counter_cli_t *)user;
    (void)ctx;
    dispatch(cli, cli->line);
    cli->prompt_needed = 1;
}

static int member_contains(const char members[][RAFT_MAX_ID], size_t count, const char *id) {
    size_t i;
    for (i = 0; i < count; ++i)
        if (strcmp(members[i], id) == 0) return 1;
    return 0;
}

static void request_membership_add(counter_cli_t *cli, const char *node_id) {
    char members[RAFT_MAX_NODES][RAFT_MAX_ID];
    size_t count = 0, i;
    if (cli->node->machine.state != RAFT_ROLE_LEADER) return;
    for (i = 0; i < cli->node->config_state.old_count && count < RAFT_MAX_NODES; ++i)
        strncpy(members[count++], cli->node->config_state.old_members[i], RAFT_MAX_ID - 1);
    if (!member_contains(members, count, node_id) && count < RAFT_MAX_NODES) {
        strncpy(members[count++], node_id, RAFT_MAX_ID - 1);
        raft_node_request_membership_change(cli->node, members, count);
        printf("\n[cluster] join: %s\n", node_id);
        cli->prompt_needed = 1;
    }
}

static void handle_pending_joins(counter_cli_t *cli) {
    size_t i = 0;
    while (i < cli->pending_join_count) {
        raft_tcp_join_req_t *jr = &cli->pending_joins[i];
        size_t k;
        int known = 0;
        int already = 0;

        if (jr->forwarded == RAFT_JOIN_ANNOUNCE) {
            raft_tcp_transport_add_peer(cli->tcp, jr->node_id, jr->host, jr->port);
            cli->pending_joins[i] = cli->pending_joins[--cli->pending_join_count];
            continue;
        }

        if (jr->forwarded == RAFT_JOIN_ORIGINAL) {
            jr->forwarded = RAFT_JOIN_FORWARDED;
            raft_tcp_transport_flood_join(cli->tcp, jr);
            raft_tcp_transport_announce_self(cli->tcp, jr->host, jr->port);
            jr->forwarded = RAFT_JOIN_PENDING;
        } else if (jr->forwarded == RAFT_JOIN_FORWARDED) {
            raft_tcp_transport_announce_self(cli->tcp, jr->host, jr->port);
            jr->forwarded = RAFT_JOIN_PENDING;
        }

        for (k = 0; k < cli->tcp->peer_count; ++k)
            if (strcmp(cli->tcp->peers[k].id, jr->node_id) == 0) known = 1;
        if (!known)
            raft_tcp_transport_add_peer(cli->tcp, jr->node_id, jr->host, jr->port);

        for (k = 0; k < cli->node->config_state.old_count; ++k)
            if (strcmp(cli->node->config_state.old_members[k], jr->node_id) == 0) already = 1;
        for (k = 0; k < cli->node->config_state.new_count; ++k)
            if (strcmp(cli->node->config_state.new_members[k], jr->node_id) == 0) already = 1;
        if (already) {
            cli->pending_joins[i] = cli->pending_joins[--cli->pending_join_count];
            continue;
        }

        if (cli->node->machine.state == RAFT_ROLE_LEADER) {
            request_membership_add(cli, jr->node_id);
            cli->pending_joins[i] = cli->pending_joins[--cli->pending_join_count];
        } else {
            ++i;
        }
    }
}

static void handle_forwarded_commands(counter_cli_t *cli) {
    size_t i;
    if (cli->fwd_cmd_count == 0) return;
    if (cli->node->machine.state == RAFT_ROLE_LEADER) {
        for (i = 0; i < cli->fwd_cmd_count; ++i)
            raft_node_submit_command(cli->node, &cli->fwd_cmds[i]);
    }
    cli->fwd_cmd_count = 0;
}

static void cli_dump_outputs(rx_fsm_context *ctx, void *user) {
    counter_cli_t *cli = (counter_cli_t *)user;
    int role = cli->node->machine.state;
    int term = cli->node->current_term;
    (void)ctx;

    if (cli->quit_requested) {
        rx_coop_exec_stop(cli->coop);
        return;
    }

    if (role != cli->last_role || term != cli->last_term) {
        fprintf(stderr, "\n[%s] %s -> %s term=%d commit=%d\n",
                cli->node_id,
                role_name(cli->last_role),
                role_name(role),
                term,
                cli->node->commit_index);
        cli->last_role = role;
        cli->last_term = term;
        cli->prompt_needed = 1;
    }

    handle_pending_joins(cli);
    handle_forwarded_commands(cli);

    if (cli->prompt_needed) {
        printf("%s> ", cli->node_id);
        fflush(stdout);
        cli->prompt_needed = 0;
    }
}

static const rx_fsm_transition cli_transitions[] = {
    {CLI_READY, CLI_READY, cli_guard_has_line, cli_action_execute},
};

void counter_cli_machine_init(rx_fsm_machine *machine, counter_cli_t *cli) {
    rx_fsm_machine_init(machine, "counter-cli", CLI_READY,
                        cli_transitions,
                        sizeof(cli_transitions) / sizeof(cli_transitions[0]),
                        cli,
                        cli_latch_inputs,
                        cli_dump_outputs);
}
