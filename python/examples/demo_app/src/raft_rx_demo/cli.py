# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: GPL-3.0-only

"""Interactive rxnet CLI for the demo node."""
from __future__ import annotations

import select
import sys
import threading
from typing import Any

from rxnet import fsm

from raft_rx.messages import Command

CLI_READY = 0
CLI_PERIOD_US = 10_000


class DemoCli:
    def __init__(self, service: Any) -> None:
        self.service = service
        self.line: str | None = None
        self.prompt_needed = True
        self.quit_requested = False
        self.executive: Any = None

    def latch_inputs(self) -> None:
        self.line = None
        readable, _, _ = select.select([sys.stdin], [], [], 0)
        if not readable:
            return
        line = sys.stdin.readline()
        if line == "":
            self.quit_requested = True
        else:
            self.line = line.strip()

    def has_line(self) -> bool:
        return self.line is not None or self.quit_requested

    def execute(self) -> None:
        if self.quit_requested:
            self.stop()
            return
        if self.line is not None:
            self.dispatch(self.line)
        self.prompt_needed = True

    def dump_outputs(self) -> None:
        if not self.prompt_needed:
            return
        print(self.prompt(), end="", flush=True)
        self.prompt_needed = False

    def stop(self) -> None:
        self.quit_requested = True
        if self.executive is not None:
            self.executive.stop()

    def prompt(self) -> str:
        try:
            status = self.service.status()
            leader = status["leader_id"] or "-"
            return (
                f"[{status['display_role']} {status['membership_mode']} term={status['term']} "
                f"leader={leader} log={status['log_len']} commit={status['commit_index']}] demo> "
            )
        except Exception:
            return "[unknown] demo> "

    def dispatch(self, line: str) -> None:
        if not line:
            return
        parts = line.split()
        cmd_name = parts[0]
        arg = line[len(cmd_name):].strip()
        commands = {
            "status": self.cmd_status,
            "members": self.cmd_members,
            "log": self.cmd_log,
            "set": self.cmd_set,
            "get": self.cmd_get,
            "delete": self.cmd_delete,
            "addnode": self.cmd_addnode,
            "rmnode": self.cmd_rmnode,
            "join": self.cmd_join,
            "merge": self.cmd_merge,
            "stop": self.cmd_stop,
            "start": self.cmd_start,
            "help": self.cmd_help,
            "quit": self.cmd_quit,
            "exit": self.cmd_quit,
        }
        handler = commands.get(cmd_name)
        if handler is None:
            print(f"unknown: {cmd_name}  (type 'help')")
            return
        handler(arg)

    def cmd_status(self, arg: str) -> None:
        del arg
        status = self.service.status()
        last = status.get("last_committed_command")
        print(
            f"node={status['node_id']} running={status['running']} role={status['display_role']} "
            f"term={status['term']} log={status['log_len']} commit={status['commit_index']} "
            f"last_committed={last}"
        )

    def cmd_members(self, arg: str) -> None:
        del arg
        config = self.service.cluster_config()
        print(
            f"node={config['node_id']} role={config['display_role']} term={config['term']} "
            f"leader={config['leader_id']} mode={config['membership_mode']} "
            f"config_index={config['configuration_index']}"
        )
        print(f"members={config['members']}")
        print(f"old_members={config['old_members']}")
        print(f"new_members={config['new_members']}")

    def cmd_log(self, arg: str) -> None:
        limit = None
        if arg:
            try:
                limit = int(arg)
            except ValueError:
                print("usage: log [LIMIT]")
                return
        payload = self.service.log_entries(limit=limit)
        print(
            f"node={payload['node_id']} role={payload['display_role']} "
            f"snapshot_index={payload['snapshot_index']} commit={payload['commit_index']} "
            f"applied={payload['last_applied']}"
        )
        for item in payload["entries"]:
            state = "PENDING"
            if item["committed"] and item["applied"]:
                state = "COMMITTED,APPLIED"
            elif item["committed"]:
                state = "COMMITTED"
            command = item["command"]
            print(
                f"#{item['index']} term={item['term']} state={state} "
                f"op={command['op']} key={command['key']!r} value={command.get('value')!r}"
            )

    def cmd_set(self, arg: str) -> None:
        parts = arg.split(maxsplit=1)
        if len(parts) != 2:
            print("usage: set KEY VALUE")
            return
        print(self.service.queue_client_command(Command(op="set", key=parts[0], value=parts[1])))

    def cmd_get(self, arg: str) -> None:
        key = arg.strip()
        if not key:
            print("usage: get KEY")
            return
        print(self.service.get(key))

    def cmd_delete(self, arg: str) -> None:
        key = arg.strip()
        if not key:
            print("usage: delete KEY")
            return
        print(self.service.queue_client_command(Command(op="delete", key=key)))

    def cmd_addnode(self, arg: str) -> None:
        if not arg:
            print("usage: addnode HOST:PORT")
            return
        print(self.service.queue_add_node(arg))

    def cmd_rmnode(self, arg: str) -> None:
        if not arg:
            print("usage: rmnode HOST:PORT")
            return
        print(self.service.queue_remove_node(arg))

    def cmd_join(self, arg: str) -> None:
        if not arg:
            print("usage: join HOST:PORT")
            return
        threading.Thread(
            target=lambda: print(self.service.join_cluster(arg)),
            daemon=True,
            name="raft-rx-cli-join",
        ).start()
        print("join requested")

    def cmd_merge(self, arg: str) -> None:
        if not arg:
            print("usage: merge HOST:PORT")
            return
        threading.Thread(
            target=lambda: print(f"merged: {self.service.merge_cluster(arg)['merged_members']}"),
            daemon=True,
            name="raft-rx-cli-merge",
        ).start()
        print("merge requested")

    def cmd_stop(self, arg: str) -> None:
        del arg
        print(self.service.stop_node())

    def cmd_start(self, arg: str) -> None:
        del arg
        print(self.service.start_node())

    def cmd_help(self, arg: str) -> None:
        del arg
        print("Commands:")
        print("  set KEY VALUE")
        print("  get KEY")
        print("  delete KEY")
        print("  status | members | log [LIMIT]")
        print("  addnode HOST:PORT | rmnode HOST:PORT | join HOST:PORT | merge HOST:PORT")
        print("  stop | start | quit")

    def cmd_quit(self, arg: str) -> None:
        del arg
        self.stop()


def create_cli_fsm(cli: DemoCli) -> fsm.Machine:
    return fsm.Machine(
        name="cli",
        state=CLI_READY,
        transitions=[
            fsm.Transition(
                CLI_READY,
                CLI_READY,
                guard=lambda ctx, user: user.has_line(),
                action=lambda ctx, user: user.execute(),
                label="line",
            )
        ],
        user=cli,
        latch_inputs_cb=lambda ctx, user: user.latch_inputs(),
        dump_outputs_cb=lambda ctx, user: user.dump_outputs(),
        state_names={CLI_READY: "READY"},
    )
