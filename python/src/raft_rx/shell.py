# Copyright 2026 Jose M. Moya <jm.moya@upm.es>
# SPDX-License-Identifier: MIT

from __future__ import annotations

import cmd
import json
import shlex
import threading
from contextlib import contextmanager
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .messages import Command
from .cluster import RaftCluster

ShellHandler = Callable[["RaftShell", list[str]], None]


@dataclass(slots=True)
class ShellCommand:
    name: str
    handler: ShellHandler
    help_text: str
    usage: str


class RaftShell(cmd.Cmd):
    intro = "raft_rx shell. Type 'help' to list commands."
    prompt = "raft> "

    def __init__(self, cluster: RaftCluster, root: Path, auto_tick_ms: int = 300) -> None:
        super().__init__()
        self.cluster = cluster
        self.root = root
        self._commands: dict[str, ShellCommand] = {}
        self._cluster_lock = threading.RLock()
        self._auto_tick_ms = 0
        self._tick_quantum_ms = 10
        self._auto_tick_thread: threading.Thread | None = None
        self._auto_tick_stop = threading.Event()
        self._set_auto_tick(auto_tick_ms)

    def register_command(
        self,
        name: str,
        handler: ShellHandler,
        *,
        help_text: str,
        usage: str,
    ) -> None:
        self._commands[name] = ShellCommand(
            name=name,
            handler=handler,
            help_text=help_text,
            usage=usage,
        )

    @contextmanager
    def cluster_locked(self):
        with self._cluster_lock:
            yield self.cluster

    @property
    def auto_tick_ms(self) -> int:
        return self._auto_tick_ms

    def do_status(self, arg: str) -> None:
        del arg
        with self.cluster_locked() as cluster:
            summaries = cluster.summaries()
        print(f"autotick={self._auto_tick_ms}ms tick_quantum={self._tick_quantum_ms}ms")
        for summary in summaries:
            print(
                f"{summary['node_id']:>3} running={str(summary['running']).lower():<5} "
                f"role={summary['role']:<9} cfg={summary['membership_mode']:<6} "
                f"cfg_idx={summary['configuration_index']:<3} term={summary['term']:<3} "
                f"leader={summary['leader_id']!s:<4} "
                f"log={summary['log_len']:<3} snapshot={summary['snapshot_index']:<3} "
                f"maxlog={summary['compaction_threshold']:<3} commit={summary['commit_index']:<3} "
                f"applied={summary['last_applied']:<3}"
            )

    def do_leader(self, arg: str) -> None:
        del arg
        with self.cluster_locked() as cluster:
            leader = cluster.leader()
        print(leader.node_id if leader is not None else "no leader")

    def do_tick(self, arg: str) -> None:
        parts = shlex.split(arg)
        ticks = int(parts[0]) if parts else 1
        advance_ms = int(parts[1]) if len(parts) > 1 else 10
        with self.cluster_locked() as cluster:
            cluster.run(ticks, advance_ms=advance_ms)
        self.do_status("")

    def do_autotick(self, arg: str) -> None:
        parts = shlex.split(arg)
        if not parts:
            print(self._auto_tick_ms)
            return
        interval_ms = int(parts[0])
        self._set_auto_tick(interval_ms)
        if interval_ms == 0:
            print("autotick disabled")
        else:
            print(f"autotick={interval_ms}ms")

    def do_stop(self, arg: str) -> None:
        parts = shlex.split(arg)
        if len(parts) != 1:
            print("usage: stop NODE")
            return
        with self.cluster_locked() as cluster:
            cluster.stop_node(parts[0])
        self.do_status("")

    def do_start(self, arg: str) -> None:
        parts = shlex.split(arg)
        if len(parts) != 1:
            print("usage: start NODE")
            return
        with self.cluster_locked() as cluster:
            cluster.start_node(parts[0])
        self.do_status("")

    def do_restart(self, arg: str) -> None:
        parts = shlex.split(arg)
        if len(parts) != 1:
            print("usage: restart NODE")
            return
        with self.cluster_locked() as cluster:
            cluster.restart_node(parts[0])
        self.do_status("")

    def do_events(self, arg: str) -> None:
        parts = shlex.split(arg)
        with self.cluster_locked() as cluster:
            node_ids = parts if parts else cluster.node_ids()
        for node_id in node_ids:
            self._print_events(node_id)

    def do_members(self, arg: str) -> None:
        parts = shlex.split(arg)
        with self.cluster_locked() as cluster:
            if len(parts) == 1:
                node = cluster.nodes.get(parts[0])
                if node is None:
                    print(f"unknown node: {parts[0]}")
                    return
                print(
                    json.dumps(
                        {
                            "mode": node.config_state.mode(),
                            "old_members": list(node.config_state.old_members),
                            "new_members": list(node.config_state.new_members) if node.config_state.new_members is not None else None,
                            "index": node.config_state.index,
                        },
                        indent=2,
                    )
                )
                return
            view = {
                node_id: {
                    "mode": node.config_state.mode(),
                    "old_members": list(node.config_state.old_members),
                    "new_members": list(node.config_state.new_members) if node.config_state.new_members is not None else None,
                    "index": node.config_state.index,
                }
                for node_id, node in cluster.nodes.items()
            }
        print(json.dumps(view, indent=2, sort_keys=True))

    def do_log(self, arg: str) -> None:
        parts = shlex.split(arg)
        if len(parts) != 1:
            print("usage: log NODE")
            return
        node_id = parts[0]
        with self.cluster_locked() as cluster:
            node = cluster.nodes.get(node_id)
            if node is None:
                print(f"unknown node: {node_id}")
                return
            print(
                f"{node_id}: term={node.current_term} role={node.role.name} "
                f"log_len={len(node.log)} snapshot_index={node.snapshot_last_included_index} "
                f"commit_index={node.commit_index} last_applied={node.last_applied}"
            )
            if not node.log:
                print("(empty log)")
                return
            for entry in node.log:
                state = "COMMITTED" if entry.index <= node.commit_index else "UNCOMMITTED"
                print(
                    f"[{entry.index}] {state:<11} term={entry.term} op={entry.command.op} "
                    f"key={entry.command.key!r} value={entry.command.value!r}"
                )

    def do_dump(self, arg: str) -> None:
        del arg
        with self.cluster_locked() as cluster:
            print(json.dumps(cluster.summaries(), indent=2))

    def do_trace(self, arg: str) -> None:
        parts = shlex.split(arg)
        path = Path(parts[0]) if parts else self.root / "trace.bin"
        with self.cluster_locked() as cluster:
            if not cluster.export_trace(path):
                print("trace disabled")
                return
        print(path)

    def do_trace_report(self, arg: str) -> None:
        parts = shlex.split(arg)
        path = Path(parts[0]) if parts else self.root / "trace.html"
        with self.cluster_locked() as cluster:
            if not cluster.report_trace(path):
                print("trace disabled")
                return
        print(path)

    def do_maxlog(self, arg: str) -> None:
        parts = shlex.split(arg)
        if not parts:
            with self.cluster_locked() as cluster:
                values = {node_id: node.compaction_threshold for node_id, node in cluster.nodes.items()}
            print(json.dumps(values, indent=2, sort_keys=True))
            return
        if len(parts) != 1:
            print("usage: maxlog [ENTRIES]")
            return
        threshold = int(parts[0])
        if threshold <= 0:
            print("maxlog must be > 0")
            return
        with self.cluster_locked() as cluster:
            leader = cluster.leader()
            if leader is None:
                print("no leader")
                return
            leader.submit_command(Command(op="cluster.set_maxlog", key="maxlog", value=str(threshold)))
            print(f"queued maxlog={threshold} on {leader.node_id}")

    def do_addnode(self, arg: str) -> None:
        parts = shlex.split(arg)
        if len(parts) != 1:
            print("usage: addnode NODE")
            return
        node_id = parts[0]
        with self.cluster_locked() as cluster:
            leader = cluster.leader()
            if leader is None:
                print("no leader")
                return
            members = list(leader.config_state.all_members())
            if node_id in members:
                print(f"node already exists: {node_id}")
                return
            try:
                cluster.provision_node(node_id, members=[*members, node_id], running=False)
            except RuntimeError as exc:
                print(str(exc))
                return
            leader.request_membership_change([*members, node_id])
            print(f"requested addnode={node_id} on {leader.node_id}")

    def do_rmnode(self, arg: str) -> None:
        parts = shlex.split(arg)
        if len(parts) != 1:
            print("usage: rmnode NODE")
            return
        node_id = parts[0]
        with self.cluster_locked() as cluster:
            leader = cluster.leader()
            if leader is None:
                print("no leader")
                return
            members = list(leader.config_state.all_members())
            if node_id not in members:
                print(f"unknown node: {node_id}")
                return
            leader.request_membership_change([member for member in members if member != node_id])
            print(f"requested rmnode={node_id} on {leader.node_id}")

    def do_help(self, arg: str) -> None:
        target = arg.strip()
        if target and target in self._commands:
            command = self._commands[target]
            print(f"{command.name}: {command.help_text}")
            print(f"usage: {command.usage}")
            return
        super().do_help(arg)
        if not target and self._commands:
            print("\nregistered commands:")
            for name in sorted(self._commands):
                command = self._commands[name]
                print(f"  {name:<10} {command.help_text}")

    def default(self, line: str) -> None:
        parts = shlex.split(line)
        if not parts:
            return
        command = self._commands.get(parts[0])
        if command is None:
            return super().default(line)
        command.handler(self, parts[1:])

    def completenames(self, text: str, *ignored: object) -> list[str]:
        builtin = super().completenames(text, *ignored)
        dynamic = [name for name in self._commands if name.startswith(text)]
        return sorted(set(builtin + dynamic))

    def completedefault(self, text: str, line: str, begidx: int, endidx: int) -> list[str]:
        del endidx
        if begidx == 0:
            return self.completenames(text)
        parts = shlex.split(line[:begidx])
        if len(parts) == 1:
            return self.completenames(text)
        return []

    def do_quit(self, arg: str) -> bool:
        del arg
        self._set_auto_tick(0)
        return True

    def do_exit(self, arg: str) -> bool:
        return self.do_quit(arg)

    def do_EOF(self, arg: str) -> bool:
        print()
        return self.do_quit(arg)

    def _print_events(self, node_id: str) -> None:
        path = self.root / "telemetry" / f"{node_id}.jsonl"
        if not path.exists():
            print(f"== {node_id} ==\nno events")
            return
        print(f"== {node_id} ==")
        for line in path.read_text(encoding="utf-8").splitlines()[-10:]:
            event = json.loads(line)
            print(
                f"t={event['time_ms']:<5} running={str(event.get('running')).lower():<5} "
                f"role={event['role']:<9} event={event['event']:<16} payload={event['payload']}"
            )

    def _set_auto_tick(self, interval_ms: int) -> None:
        self._auto_tick_ms = interval_ms
        self._auto_tick_stop.set()
        if self._auto_tick_thread is not None:
            self._auto_tick_thread.join(timeout=1.0)
            self._auto_tick_thread = None
        self._auto_tick_stop = threading.Event()
        if interval_ms <= 0:
            self._auto_tick_ms = 0
            return
        self._auto_tick_thread = threading.Thread(target=self._auto_tick_loop, daemon=True)
        self._auto_tick_thread.start()

    def _auto_tick_loop(self) -> None:
        while not self._auto_tick_stop.wait(self._auto_tick_ms / 1000.0):
            with self.cluster_locked() as cluster:
                self._run_interval_ticks(cluster, self._auto_tick_ms)

    def _run_interval_ticks(self, cluster: RaftCluster, interval_ms: int) -> None:
        remaining_ms = interval_ms
        while remaining_ms > 0:
            step_ms = min(self._tick_quantum_ms, remaining_ms)
            cluster.tick(advance_ms=step_ms)
            remaining_ms -= step_ms
